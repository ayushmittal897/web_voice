const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;

// HTTP server for health check (Render sleep waking)
const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 
      'Content-Type': 'text/plain', 
      'Access-Control-Allow-Origin': '*' 
    });
    res.end('ok');
  } else {
    res.writeHead(404);
    res.end();
  }
});

const wss = new WebSocketServer({ server });

// Room state map: roomCode -> { clients: [ws1, ws2] }
const rooms = new Map();

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function leaveRoom(ws) {
  if (!ws.roomId) return;
  const room = rooms.get(ws.roomId);
  if (room) {
    // Remove client from room
    room.clients = room.clients.filter(client => client !== ws);
    log(`Client ${ws.id} left room ${ws.roomId}. Clients left: ${room.clients.length}`);
    
    // Notify the remaining client (if any)
    room.clients.forEach(client => {
      if (client.readyState === 1) { // WebSocket.OPEN
        client.send(JSON.stringify({ type: 'peer-left' }));
      }
    });

    // Clean up empty room
    if (room.clients.length === 0) {
      rooms.delete(ws.roomId);
      log(`Room ${ws.roomId} deleted (empty).`);
    }
  }
  ws.roomId = null;
}

wss.on('connection', (ws) => {
  ws.id = crypto.randomUUID();
  ws.isAlive = true;
  log(`New connection established: ${ws.id}`);

  // Heartbeat pong
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (messageAsString) => {
    let msg;
    try {
      msg = JSON.parse(messageAsString);
    } catch (e) {
      log(`Invalid JSON received from ${ws.id}`);
      return;
    }

    if (msg.type === 'join') {
      let roomCode = msg.room;
      // Empty room -> Random private default (not 'main' unless explicitly requested)
      if (!roomCode || roomCode.trim() === '') {
        roomCode = 'rand-' + crypto.randomBytes(4).toString('hex');
      }

      if (!rooms.has(roomCode)) {
        rooms.set(roomCode, { clients: [] });
      }

      const room = rooms.get(roomCode);

      // Max 2 clients
      if (room.clients.length >= 2) {
        log(`Client ${ws.id} tried to join full room: ${roomCode}`);
        ws.send(JSON.stringify({ type: 'full' }));
        return;
      }

      ws.roomId = roomCode;
      room.clients.push(ws);
      log(`Client ${ws.id} joined room ${roomCode}. Total clients in room: ${room.clients.length}`);

      if (room.clients.length === 2) {
        // Two clients are now in the room. Notify them to start WebRTC handshake.
        const firstClient = room.clients[0];
        const secondClient = room.clients[1];
        
        log(`Room ${roomCode} is full and ready for signaling.`);
        firstClient.send(JSON.stringify({ type: 'peer-joined' }));
        secondClient.send(JSON.stringify({ type: 'wait-for-offer' }));
      }
    } 
    else if (msg.type === 'signal') {
      if (!ws.roomId) return;
      const room = rooms.get(ws.roomId);
      if (room) {
        log(`Signal event relayed from ${ws.id} in room ${ws.roomId}`);
        room.clients.forEach(client => {
          if (client !== ws && client.readyState === 1) {
            client.send(JSON.stringify({ type: 'signal', data: msg.data }));
          }
        });
      }
    }
    else if (msg.type === 'leave') {
      log(`Client ${ws.id} requested to leave`);
      leaveRoom(ws);
    }
  });

  ws.on('close', () => {
    log(`Connection closed for client: ${ws.id}`);
    leaveRoom(ws);
  });
});

// Heartbeat ping every 20 seconds
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      log(`Terminating dead socket: ${ws.id}`);
      return ws.terminate(); // Will trigger 'close' event and cleanup
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 20000);

wss.on('close', () => {
  clearInterval(interval);
});

server.listen(PORT, () => {
  console.log(`WebRTC Signaling server running on port ${PORT}`);
});
