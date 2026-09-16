const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');

/* ---------- HTTP: раздача статики ---------- */
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'text/javascript; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png':  'image/png',
  '.svg':  'image/svg+xml',
  '.ico':  'image/x-icon',
};

const server = http.createServer((req, res) => {
  let urlPath = req.url.split('?')[0];
  if (urlPath === '/') urlPath = '/index.html';

  const filePath = path.join(PUBLIC_DIR, urlPath);
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403); return res.end('Forbidden');
  }

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

/* ---------- WebSocket: сигналинг ---------- */
const wss = new WebSocketServer({ server });
const rooms = new Map(); // roomId -> Map<clientId, {ws, name, mic, cam, screen}>

function getRoom(id) {
  if (!rooms.has(id)) rooms.set(id, new Map());
  return rooms.get(id);
}

function broadcast(roomId, msg, exceptId = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const payload = JSON.stringify(msg);
  for (const [id, c] of room) {
    if (id === exceptId) continue;
    if (c.ws.readyState === 1) c.ws.send(payload);
  }
}

wss.on('connection', (ws) => {
  let clientId = null;
  let roomId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'join': {
        clientId = msg.id || ('u-' + Math.random().toString(36).slice(2, 8));
        roomId = msg.room || 'default';
        const room = getRoom(roomId);

        const participants = [...room.entries()].map(([id, c]) => ({
          id, name: c.name, mic: c.mic, cam: c.cam, screen: c.screen || false,
        }));

        ws.send(JSON.stringify({ type: 'joined', id: clientId, participants }));

        room.set(clientId, {
          ws,
          name: msg.name || 'Гость',
          mic: msg.mic ?? true,
          cam: msg.cam ?? false,
          screen: false,
        });

        broadcast(roomId, {
          type: 'participant-joined',
          id: clientId,
          name: msg.name || 'Гость',
          mic: msg.mic ?? true,
          cam: msg.cam ?? false,
        }, clientId);

        console.log(`[${roomId}] + ${clientId} (${msg.name}) — всего: ${room.size}`);
        break;
      }

      case 'signal': {
        const room = rooms.get(roomId);
        if (!room) return;
        const target = room.get(msg.to);
        if (target && target.ws.readyState === 1) {
          target.ws.send(JSON.stringify({
            type: 'signal', from: clientId, data: msg.data,
          }));
        }
        break;
      }

      case 'update': {
        const room = rooms.get(roomId);
        if (!room) return;
        const c = room.get(clientId);
        if (!c) return;
        if (msg.mic !== undefined) c.mic = msg.mic;
        if (msg.cam !== undefined) c.cam = msg.cam;
        if (msg.screen !== undefined) c.screen = msg.screen;

        broadcast(roomId, {
          type: 'participant-updated',
          id: clientId,
          mic: msg.mic, cam: msg.cam, screen: msg.screen,
        }, clientId);
        break;
      }

      case 'leave': handleLeave(); break;
    }
  });

  ws.on('close', handleLeave);

  function handleLeave() {
    if (!roomId || !clientId) return;
    const room = rooms.get(roomId);
    if (room && room.has(clientId)) {
      room.delete(clientId);
      broadcast(roomId, { type: 'participant-left', id: clientId });
      console.log(`[${roomId}] − ${clientId} — осталось: ${room.size}`);
      if (room.size === 0) rooms.delete(roomId);
    }
    clientId = null;
    roomId = null;
  }
});

server.listen(PORT, () => {
  console.log('');
  console.log('  🎬  Netflix Call server');
  console.log(`  →  http://localhost:${PORT}`);
  console.log('');
});