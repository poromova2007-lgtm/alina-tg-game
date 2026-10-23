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

// Подключение к базе
const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL, 
  ssl: { rejectUnauthorized: false } 
});
pool.query(`CREATE TABLE IF NOT EXISTS game_data (id SERIAL PRIMARY KEY, data JSONB)`).catch(console.error);

// Загрузка медиа
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json({ limit: '15mb' }));

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

// ============ ИГРОВОЕ СОСТОЯНИЕ ============
let players = [];
let teams = []; // Список команд, который задаст ведущая
let currentQuestion = null;
let buzzQueue = [], answeredWrong = [], locked = false;
let nextPlayerId = 1;
let votes = {}; 
let voteTimer = null;

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'state', state: getPublicState() }));

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch(e) { return; }

    // Игрок заходит с телефона
    if (msg.type === 'join') {
      const name = String(msg.name || '').trim().slice(0, 20) || 'Игрок';
      const teamId = parseInt(msg.teamId) || 1;
      const id = nextPlayerId++;
      players.push({ id, name, teamId, connected: true });
      ws.playerId = id;
      ws.send(JSON.stringify({ type: 'joined', id, name, teamId }));
      broadcastState(); return;
    }

    // Обычное нажатие "Я ЗНАЮ"
    if (msg.type === 'buzz') {
      if (!ws.playerId || locked || !currentQuestion || currentQuestion.isVote) return;
      if (buzzQueue.some(b => b.playerId === ws.playerId) || answeredWrong.includes(ws.playerId)) return;
      const player = players.find(p => p.id === ws.playerId);
      if (!player) return;
      buzzQueue.push({ playerId: player.id, playerName: player.name, answer: '' });
      broadcastState(); ws.send(JSON.stringify({ type: 'buzzed', position: buzzQueue.length })); return;
    }

    // Ввод ответа
    if (msg.type === 'answer') {
      if (!ws.playerId) return;
      const idx = buzzQueue.findIndex(b => b.playerId === ws.playerId);
      if (idx === -1) return;
      buzzQueue[idx].answer = String(msg.text || '').slice(0, 200);
      broadcastState(); return;
    }

    // Голосование
    if (msg.type === 'vote') {
      if (!ws.playerId || !currentQuestion || !currentQuestion.isVote) return;
      if (votes[ws.playerId] !== undefined) return;
      votes[ws.playerId] = parseInt(msg.choiceIndex);
      broadcastState(); return;
    }

    // Ведущий открывает вопрос
    if (msg.type === 'openQuestion') {
      currentQuestion = msg.question;
      buzzQueue = []; answeredWrong = []; locked = false; votes = {};
      
      if (currentQuestion.isVote) {
        let timeLeft = 20;
        broadcastState();
        if(voteTimer) clearInterval(voteTimer);
        voteTimer = setInterval(() => {
          timeLeft--;
          // Рассылаем всем таймер (обновление раз в секунду)
          wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(JSON.stringify({ type: 'tick', timeLeft })); });

          if (timeLeft <= 0) {
            clearInterval(voteTimer);
            let counts = {};
            Object.values(votes).forEach(v => counts[v] = (counts[v] || 0) + 1);
            let maxVotes = 0, winningChoice = -1;
            for (let key in counts) {
              if (counts[key] > maxVotes) { maxVotes = counts[key]; winningChoice = parseInt(key); }
            }
            currentQuestion.winningChoice = winningChoice;
            currentQuestion.isVotingFinished = true;
            
            // Начисляем очки командам за правильный выбор
            players.forEach(p => {
              if (votes[p.id] === winningChoice) {
                const team = teams.find(t => t.id === p.teamId);
                if (team) team.score += (currentQuestion.price || 100);
              }
            });
            broadcastState();
          }
        }, 1000);
      } else {
        broadcastState();
      }
      return;
    }

    if (msg.type === 'closeQuestion') {
      currentQuestion = null; buzzQueue = []; answeredWrong = []; locked = false; votes = {};
      if (voteTimer) clearInterval(voteTimer);
      broadcastState(); return;
    }

    // Ведущий нажимает "Верно" (начисляет очки команде)
    if (msg.type === 'correct') {
      const player = players.find(x => x.id === msg.playerId);
      if (player) {
        const team = teams.find(t => t.id === player.teamId);
        if (team) team.score += msg.delta;
      }
      broadcastState(); return;
    }

    if (msg.type === 'wrong') {
      if (!answeredWrong.includes(msg.playerId)) answeredWrong.push(msg.playerId);
      buzzQueue = buzzQueue.filter(b => b.playerId !== msg.playerId);
      broadcastState(); return;
    }

    // Ведущий задает количество и названия команд
    if (msg.type === 'updateTeams') {
      teams = msg.teams;
      broadcastState(); return;
    }
  });

  ws.on('close', () => {
    if (ws.playerId) { const p = players.find(x => x.id === ws.playerId); if (p) p.connected = false; broadcastState(); }
  });
});

function getPublicState() {
  return { players, teams, buzzQueue, currentQuestion, answeredWrong, locked, votes };
}
function broadcastState() {
  const data = JSON.stringify({ type: 'state', state: getPublicState() });
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
}

server.listen(PORT, () => { console.log('🎂 Сервер запущен с командами и голосованием!'); });