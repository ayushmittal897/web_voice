const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_CLIENTS = 5;

// HTTP server for health check (Render sleep waking)
const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end('ok');
  } else {
    res.writeHead(404);
    res.end();
  }
});

const wss = new WebSocketServer({ server });

// rooms = Map<roomCode, Map<wsId, ws>>
const rooms = new Map();

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function leaveRoom(ws) {
  if (!ws.roomId) return;
  const room = rooms.get(ws.roomId);
  if (room) {
    room.delete(ws.id);
    log(`Client ${ws.id} left room ${ws.roomId}. Clients left: ${room.size}`);
    
    // Notify remaining clients
    for (const [id, client] of room) {
      if (client.readyState === 1) {
        client.send(JSON.stringify({ type: 'peer-left', peerId: ws.id }));
      }
    }

    if (room.size === 0) {
      rooms.delete(ws.roomId);
      log(`Room ${ws.roomId} deleted (empty).`);
    }
  }
  ws.roomId = null;
}

wss.on('connection', (ws) => {
  ws.id = crypto.randomUUID();
  ws.isAlive = true;
  log(`New connection: ${ws.id}`);

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (messageAsString) => {
    let msg;
    try {
      msg = JSON.parse(messageAsString);
    } catch (e) {
      log(`Invalid JSON from ${ws.id}`);
      return;
    }

    if (msg.type === 'join') {
      let roomCode = msg.room;
      if (!roomCode || roomCode.trim() === '') {
        roomCode = 'rand-' + crypto.randomBytes(4).toString('hex');
      }

      if (!rooms.has(roomCode)) {
        rooms.set(roomCode, new Map());
      }

      const room = rooms.get(roomCode);

      if (room.size >= MAX_CLIENTS) {
        log(`Client ${ws.id} tried to join full room: ${roomCode}`);
        ws.send(JSON.stringify({ type: 'full' }));
        return;
      }

      ws.roomId = roomCode;
      
      // Get existing peers before adding this one
      const existingPeers = Array.from(room.keys());
      
      room.set(ws.id, ws);
      log(`Client ${ws.id} joined room ${roomCode}. Total: ${room.size}`);

      // Tell the new client who is already in the room
      ws.send(JSON.stringify({ 
        type: 'room-joined', 
        room: roomCode,
        peers: existingPeers,
        yourId: ws.id
      }));

      // Tell existing clients about the new client
      for (const peerId of existingPeers) {
        const client = room.get(peerId);
        if (client && client.readyState === 1) {
          client.send(JSON.stringify({ type: 'peer-joined', peerId: ws.id }));
        }
      }
    } 
    else if (msg.type === 'signal') {
      if (!ws.roomId) return;
      const room = rooms.get(ws.roomId);
      if (room) {
        const targetClient = room.get(msg.targetId);
        if (targetClient && targetClient.readyState === 1) {
          targetClient.send(JSON.stringify({ 
            type: 'signal', 
            senderId: ws.id, 
            data: msg.data 
          }));
        }
      }
    }
    else if (msg.type === 'leave') {
      leaveRoom(ws);
    }
  });

  ws.on('close', () => {
    log(`Connection closed: ${ws.id}`);
    leaveRoom(ws);
  });
});

const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      log(`Terminating dead socket: ${ws.id}`);
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 20000);

wss.on('close', () => {
  clearInterval(interval);
});

server.listen(PORT, () => {
  console.log(`WebRTC Signaling server running on port ${PORT} (Mesh, Max ${MAX_CLIENTS})`);
});
