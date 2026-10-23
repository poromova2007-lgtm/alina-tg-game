const express = require('express');
const http = require('http');
const path = require('path');
const multer = require('multer');
const WebSocket = require('ws');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const PORT = process.env.PORT || 3000;

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
pool.query(`CREATE TABLE IF NOT EXISTS game_data (id SERIAL PRIMARY KEY, data JSONB)`).catch(console.error);

// Увеличиваем лимит загрузки файлов
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json({ limit: '50mb' }));

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/phone', (req, res) => res.sendFile(path.join(__dirname, 'public', 'phone.html')));

app.post('/api/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'Нет файла' });
  const base64 = req.file.buffer.toString('base64');
  res.json({ ok: true, url: `data:${req.file.mimetype};base64,${base64}` });
});

app.get('/api/game', async (req, res) => {
  try {
    const result = await pool.query('SELECT data FROM game_data ORDER BY id DESC LIMIT 1');
    res.json(result.rows[0]?.data || null);
  } catch(e) { res.json(null); }
});

app.post('/api/game', async (req, res) => {
  try {
    const { gameData } = req.body;
    const exists = await pool.query('SELECT id FROM game_data LIMIT 1');
    if (exists.rows.length > 0) {
      await pool.query('UPDATE game_data SET data = $1 WHERE id = $2', [gameData, exists.rows[0].id]);
    } else {
      await pool.query('INSERT INTO game_data (data) VALUES ($1)', [gameData]);
    }
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ============ СОСТОЯНИЕ ИГРЫ ============
let gameState = { players: [], teams: [], currentQuestion: null, buzzQueue: [], answeredWrong: [], locked: false, votes: {}, timer: 0 };
let nextPlayerId = 1;
let voteTimer = null;

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'state', state: gameState }));

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch(e) { return; }

    if (msg.type === 'join') {
      const name = String(msg.name || '').trim().slice(0, 20) || 'Игрок';
      const teamId = parseInt(msg.teamId) || 0;
      const id = nextPlayerId++;
      gameState.players.push({ id, name, teamId, score: 0, connected: true });
      ws.playerId = id;
      ws.send(JSON.stringify({ type: 'joined', id, name, teamId }));
      broadcastState(); return;
    }

    if (msg.type === 'buzz') {
      if (!ws.playerId || gameState.locked || !gameState.currentQuestion || gameState.currentQuestion.isVote) return;
      if (gameState.buzzQueue.some(b => b.playerId === ws.playerId) || gameState.answeredWrong.includes(ws.playerId)) return;
      const player = gameState.players.find(p => p.id === ws.playerId);
      if (!player) return;
      gameState.buzzQueue.push({ playerId: player.id, playerName: player.name, answer: '' });
      broadcastState(); ws.send(JSON.stringify({ type: 'buzzed', position: gameState.buzzQueue.length })); return;
    }

    if (msg.type === 'answer') {
      if (!ws.playerId) return;
      const idx = gameState.buzzQueue.findIndex(b => b.playerId === ws.playerId);
      if (idx === -1) return;
      gameState.buzzQueue[idx].answer = String(msg.text || '').slice(0, 200);
      broadcastState(); return;
    }

    if (msg.type === 'vote') {
      if (!ws.playerId || !gameState.currentQuestion || !gameState.currentQuestion.isVote) return;
      if (gameState.votes[ws.playerId] !== undefined) return;
      gameState.votes[ws.playerId] = parseInt(msg.choiceIndex);
      broadcastState(); return;
    }

    if (msg.type === 'openQuestion') {
      gameState.currentQuestion = msg.question;
      gameState.buzzQueue = []; gameState.answeredWrong = []; gameState.locked = false; gameState.votes = {};
      
      if (gameState.currentQuestion.isVote) {
        gameState.timer = 20;
        if (voteTimer) clearInterval(voteTimer);
        voteTimer = setInterval(() => {
          gameState.timer--;
          broadcastState();
          if (gameState.timer <= 0) {
            clearInterval(voteTimer);
            let counts = {}; Object.values(gameState.votes).forEach(v => counts[v] = (counts[v] || 0) + 1);
            let maxVotes = 0, winningChoice = -1;
            for (let key in counts) { if (counts[key] > maxVotes) { maxVotes = counts[key]; winningChoice = parseInt(key); } }
            gameState.currentQuestion.winningChoice = winningChoice;
            gameState.currentQuestion.isVotingFinished = true;
            
            gameState.players.forEach(p => {
              if (gameState.votes[p.id] === winningChoice) {
                if (p.teamId === 0) p.score += (gameState.currentQuestion.price || 100);
                else { const team = gameState.teams.find(t => t.id === p.teamId); if (team) team.score += (gameState.currentQuestion.price || 100); }
              }
            });
            broadcastState();
          }
        }, 1000);
      }
      broadcastState(); return;
    }

    if (msg.type === 'closeQuestion') {
      gameState.currentQuestion = null; gameState.buzzQueue = []; gameState.answeredWrong = []; gameState.locked = false; gameState.votes = {};
      if (voteTimer) clearInterval(voteTimer);
      broadcastState(); return;
    }

    if (msg.type === 'correct') {
      const player = gameState.players.find(x => x.id === msg.playerId);
      if (player) {
        if (player.teamId === 0) player.score += msg.delta;
        else { const team = gameState.teams.find(t => t.id === player.teamId); if (team) team.score += msg.delta; }
      }
      broadcastState(); return;
    }

    if (msg.type === 'wrong') {
      if (!gameState.answeredWrong.includes(msg.playerId)) gameState.answeredWrong.push(msg.playerId);
      gameState.buzzQueue = gameState.buzzQueue.filter(b => b.playerId !== msg.playerId);
      broadcastState(); return;
    }

    if (msg.type === 'updateTeams') { gameState.teams = msg.teams; broadcastState(); return; }
    if (msg.type === 'startGame') { gameState.teams = msg.teams || []; gameState.players = []; broadcastState(); return; }
  });

  ws.on('close', () => { if (ws.playerId) { const p = gameState.players.find(x => x.id === ws.playerId); if (p) p.connected = false; broadcastState(); } });
});

function broadcastState() {
  const data = JSON.stringify({ type: 'state', state: gameState });
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
}

server.listen(PORT, () => { console.log('🎂 Сервер запущен!'); });
