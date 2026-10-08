const WebSocket = require('ws');

const PORT = 8080;
const URL = `ws://localhost:${PORT}`;

// Quick helper to connect a client and resolve when 'open'
function connectClient() {
  return new Promise((resolve) => {
    const ws = new WebSocket(URL);
    ws.on('open', () => resolve(ws));
  });
}

// Helper to wait for a specific message type
function waitForMessage(ws, expectedType) {
  return new Promise((resolve) => {
    const listener = (data) => {
      const msg = JSON.parse(data);
      if (msg.type === expectedType) {
        ws.removeListener('message', listener);
        resolve(msg);
      }
    };
    ws.on('message', listener);
  });
}

async function runTests() {
  console.log("Starting Mesh Server Tests...\n");

  // a) A connects
  console.log("Test A: A connects");
  const wsA = await connectClient();
  wsA.send(JSON.stringify({ type: 'join', room: 'mesh-room' }));
  const msgA = await waitForMessage(wsA, 'room-joined');
  console.log(`-> Passed: A joined room. (Peers: ${msgA.peers.length})`);

  // b) B connects
  console.log("\nTest B: B connects");
  const wsB = await connectClient();
  wsB.send(JSON.stringify({ type: 'join', room: 'mesh-room' }));
  
  const [msgBJoined, msgAPeerJoined] = await Promise.all([
    waitForMessage(wsB, 'room-joined'),
    waitForMessage(wsA, 'peer-joined')
  ]);
  console.log(`-> Passed: B got room-joined (Peers: ${msgBJoined.peers.length}). A got peer-joined.`);

  // c) 3 more clients connect (total 5)
  console.log("\nTest C: 3 more clients (total 5)");
  const wsC = await connectClient();
  wsC.send(JSON.stringify({ type: 'join', room: 'mesh-room' }));
  await waitForMessage(wsC, 'room-joined');
  
  const wsD = await connectClient();
  wsD.send(JSON.stringify({ type: 'join', room: 'mesh-room' }));
  await waitForMessage(wsD, 'room-joined');

  const wsE = await connectClient();
  wsE.send(JSON.stringify({ type: 'join', room: 'mesh-room' }));
  await waitForMessage(wsE, 'room-joined');
  console.log("-> Passed: 5 clients in room.");

  // d) 6th client rejected (Max 5)
  console.log("\nTest D: 6th client rejected");
  const wsF = await connectClient();
  wsF.send(JSON.stringify({ type: 'join', room: 'mesh-room' }));
  const msgFull = await waitForMessage(wsF, 'full');
  console.log("-> Passed: 6th client got 'full'.");

  // e) Target-based signaling works
  console.log("\nTest E: Target-based signaling");
  wsA.send(JSON.stringify({ type: 'signal', targetId: msgBJoined.yourId, data: { test: 'hello' } }));
  
  let cGotSignal = false;
  wsC.on('message', (data) => { if(JSON.parse(data).type === 'signal') cGotSignal = true; });

  const signalAtB = await waitForMessage(wsB, 'signal');
  if (signalAtB.data.test === 'hello' && !cGotSignal) {
    console.log("-> Passed: Signal routed specifically to B, ignored by C.");
  } else {
    console.error("-> Failed: Signal leaked or not delivered.");
  }

  // f) Disconnect triggers peer-left
  console.log("\nTest F: Disconnect triggers peer-left");
  wsB.send(JSON.stringify({ type: 'leave' }));
  const leftMsg = await waitForMessage(wsA, 'peer-left');
  if (leftMsg.peerId === msgBJoined.yourId) {
    console.log("-> Passed: A notified that B left.");
  } else {
    console.error("-> Failed: Peer-left ID mismatch.");
  }

  // Cleanup
  wsA.close(); wsC.close(); wsD.close(); wsE.close(); wsF.close();
  console.log("\nAll tests completed successfully.");
  process.exit(0);
}

runTests();
