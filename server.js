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
const rooms = new Map();
const chatHistory = new Map(); // roomId -> []

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

function sendTo(roomId, clientId, msg) {
  const room = rooms.get(roomId);
  if (!room) return;
  const c = room.get(clientId);
  if (c && c.ws.readyState === 1) c.ws.send(JSON.stringify(msg));
}

wss.on('connection', (ws) => {
  let clientId = null;
  let roomId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      /* -------- Вход в комнату -------- */
      case 'join': {
        clientId = msg.id || ('u-' + Math.random().toString(36).slice(2, 8));
        roomId = msg.room || 'default';
        const room = getRoom(roomId);

        const participants = [...room.entries()].map(([id, c]) => ({
          id, name: c.name, mic: c.mic, cam: c.cam, screen: c.screen || false,
          isAdmin: c.isAdmin || false,
          micLocked: c.micLocked || false,
          camLocked: c.camLocked || false,
        }));

        ws.send(JSON.stringify({
          type: 'joined',
          id: clientId,
          participants,
          history: chatHistory.get(roomId) || [],
        }));

        room.set(clientId, {
          ws,
          name: msg.name || 'Гость',
          mic: msg.mic ?? true,
          cam: msg.cam ?? false,
          screen: false,
          isAdmin: false,
          micLocked: false,
          camLocked: false,
        });

        broadcast(roomId, {
          type: 'participant-joined',
          id: clientId,
          name: msg.name || 'Гость',
          mic: msg.mic ?? true,
          cam: msg.cam ?? false,
          isAdmin: false,
          micLocked: false,
          camLocked: false,
        }, clientId);

        console.log(`[${roomId}] + ${clientId} (${msg.name}) — всего: ${room.size}`);
        break;
      }

      /* -------- WebRTC сигналинг -------- */
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

      /* -------- Обновление статуса -------- */
      case 'update': {
        const room = rooms.get(roomId);
        if (!room) return;
        const c = room.get(clientId);
        if (!c) return;

        // Проверка локов — если включение заблокировано, не разрешаем
        if (msg.mic !== undefined && c.micLocked && msg.mic === true) {
          sendTo(roomId, clientId, { type: 'mic-locked-notice' });
          return;
        }
        if (msg.cam !== undefined && c.camLocked && msg.cam === true) {
          sendTo(roomId, clientId, { type: 'cam-locked-notice' });
          return;
        }

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

      /* -------- Чат -------- */
      case 'chat': {
        const room = rooms.get(roomId);
        if (!room) return;
        const c = room.get(clientId);
        if (!c) return;

        const text = String(msg.text || '').slice(0, 500).trim();
        if (!text) return;

        const chatMsg = {
          id: 'm-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6),
          from: clientId,
          name: c.name,
          text,
          time: Date.now(),
        };

        if (!chatHistory.has(roomId)) chatHistory.set(roomId, []);
        const hist = chatHistory.get(roomId);
        hist.push(chatMsg);
        if (hist.length > 200) hist.shift();

        broadcast(roomId, { type: 'chat', msg: chatMsg });
        break;
      }

      /* -------- Выход -------- */
      case 'leave': handleLeave(); break;

      /* ==================== АДМИН ==================== */
      case 'admin-login': {
        const room = rooms.get(roomId);
        if (!room) return;
        const c = room.get(clientId);
        if (!c) return;
        if (msg.password === 'admadm') {
          c.isAdmin = true;
          ws.send(JSON.stringify({ type: 'admin-granted' }));
          broadcast(roomId, {
            type: 'participant-updated',
            id: clientId, isAdmin: true,
          }, clientId);
          console.log(`[${roomId}] 🔑 ${clientId} became ADMIN`);
        } else {
          ws.send(JSON.stringify({ type: 'admin-denied' }));
        }
        break;
      }

      case 'admin-kick': {
        const room = rooms.get(roomId);
        if (!room) return;
        const admin = room.get(clientId);
        if (!admin || !admin.isAdmin) return;
        const target = room.get(msg.targetId);
        if (target && target.ws.readyState === 1) {
          target.ws.send(JSON.stringify({
            type: 'kicked',
            reason: msg.reason || 'Вас исключили из звонка',
          }));
          setTimeout(() => { try { target.ws.close(); } catch {} }, 200);
        }
        break;
      }

      case 'admin-mute': {
        const room = rooms.get(roomId);
        if (!room) return;
        const admin = room.get(clientId);
        if (!admin || !admin.isAdmin) return;
        const target = room.get(msg.targetId);
        if (target && target.ws.readyState === 1) {
          target.ws.send(JSON.stringify({ type: 'force-mute', value: msg.value }));
        }
        break;
      }

      case 'admin-cam': {
        const room = rooms.get(roomId);
        if (!room) return;
        const admin = room.get(clientId);
        if (!admin || !admin.isAdmin) return;
        const target = room.get(msg.targetId);
        if (target && target.ws.readyState === 1) {
          target.ws.send(JSON.stringify({ type: 'force-cam', value: msg.value }));
        }
        break;
      }

      case 'admin-lock-mic': {
        const room = rooms.get(roomId);
        if (!room) return;
        const admin = room.get(clientId);
        if (!admin || !admin.isAdmin) return;
        const target = room.get(msg.targetId);
        if (!target) return;
        target.micLocked = !!msg.locked;
        if (target.micLocked) target.mic = false;
        // Уведомить цель
        if (target.ws.readyState === 1) {
          target.ws.send(JSON.stringify({
            type: 'force-mute', value: true,
          }));
          target.ws.send(JSON.stringify({
            type: 'lock-update', micLocked: target.micLocked, camLocked: target.camLocked,
          }));
        }
        // Уведомить всех об обновлении статуса
        broadcast(roomId, {
          type: 'participant-updated',
          id: msg.targetId,
          mic: target.mic, micLocked: target.micLocked,
        }, msg.targetId);
        break;
      }

      case 'admin-lock-cam': {
        const room = rooms.get(roomId);
        if (!room) return;
        const admin = room.get(clientId);
        if (!admin || !admin.isAdmin) return;
        const target = room.get(msg.targetId);
        if (!target) return;
        target.camLocked = !!msg.locked;
        if (target.camLocked) target.cam = false;
        if (target.ws.readyState === 1) {
          target.ws.send(JSON.stringify({
            type: 'force-cam', value: false,
          }));
          target.ws.send(JSON.stringify({
            type: 'lock-update', micLocked: target.micLocked, camLocked: target.camLocked,
          }));
        }
        broadcast(roomId, {
          type: 'participant-updated',
          id: msg.targetId,
          cam: target.cam, camLocked: target.camLocked,
        }, msg.targetId);
        break;
      }

      case 'admin-unlock-all': {
        const room = rooms.get(roomId);
        if (!room) return;
        const admin = room.get(clientId);
        if (!admin || !admin.isAdmin) return;
        room.forEach((c, id) => {
          c.micLocked = false;
          c.camLocked = false;
          if (c.ws.readyState === 1) {
            c.ws.send(JSON.stringify({
              type: 'lock-update', micLocked: false, camLocked: false,
            }));
          }
          broadcast(roomId, {
            type: 'participant-updated',
            id, micLocked: false, camLocked: false,
          }, id);
        });
        break;
      }

      case 'admin-mute-all': {
        const room = rooms.get(roomId);
        if (!room) return;
        const admin = room.get(clientId);
        if (!admin || !admin.isAdmin) return;
        room.forEach((c, id) => {
          if (id === clientId) return;
          c.mic = false;
          if (c.ws.readyState === 1) {
            c.ws.send(JSON.stringify({ type: 'force-mute', value: true }));
          }
          broadcast(roomId, {
            type: 'participant-updated', id, mic: false,
          }, id);
        });
        break;
      }

      case 'admin-end-all': {
        const room = rooms.get(roomId);
        if (!room) return;
        const admin = room.get(clientId);
        if (!admin || !admin.isAdmin) return;
        broadcast(roomId, {
          type: 'call-ended',
          reason: msg.reason || 'Звонок завершён администратором',
        }, clientId);
        break;
      }
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
      if (room.size === 0) {
        rooms.delete(roomId);
        chatHistory.delete(roomId);
      }
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
