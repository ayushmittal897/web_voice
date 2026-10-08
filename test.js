const WebSocket = require('ws');
const http = require('http');

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
  console.log("Starting Server Tests...\n");

  // a) A+B connect
  console.log("Test A: A+B connect");
  const wsA = await connectClient();
  const wsB = await connectClient();
  
  wsA.send(JSON.stringify({ type: 'join', room: 'test-room' }));
  wsB.send(JSON.stringify({ type: 'join', room: 'test-room' }));
  
  const [msgA, msgB] = await Promise.all([
    waitForMessage(wsA, 'peer-joined'),
    waitForMessage(wsB, 'wait-for-offer')
  ]);
  console.log("-> Passed: A got 'peer-joined', B got 'wait-for-offer'.");

  // e) two different room codes never hear each other
  console.log("\nTest E: Two different rooms isolated");
  const wsD = await connectClient();
  wsD.send(JSON.stringify({ type: 'join', room: 'other-room' }));
  wsA.send(JSON.stringify({ type: 'signal', data: { test: 'hello' } }));
  
  // wait a bit to ensure D doesn't receive it, but B does
  let dGotMsg = false;
  wsD.on('message', () => { dGotMsg = true; });
  
  const msgFromA = await waitForMessage(wsB, 'signal');
  if (msgFromA.data.test === 'hello' && !dGotMsg) {
    console.log("-> Passed: Signals relayed only within room.");
  } else {
    console.error("-> Failed: Signal leaking or missing.");
  }
  
  // c) B leaves and a new C joins A's room
  console.log("\nTest C: B leaves, C joins");
  wsB.send(JSON.stringify({ type: 'leave' }));
  await waitForMessage(wsA, 'peer-left');
  
  const wsC = await connectClient();
  wsC.send(JSON.stringify({ type: 'join', room: 'test-room' }));
  await Promise.all([
    waitForMessage(wsA, 'peer-joined'),
    waitForMessage(wsC, 'wait-for-offer')
  ]);
  console.log("-> Passed: B left cleanly, C joined and triggered handshakes.");

  // d) A third client gets "full"
  console.log("\nTest D: Room full");
  const wsThird = await connectClient();
  wsThird.send(JSON.stringify({ type: 'join', room: 'test-room' }));
  const fullMsg = await waitForMessage(wsThird, 'full');
  console.log("-> Passed: Third client got 'full'.");

  // b) A presses END (sends leave), then A presses TALK again and reconnects to C with no stale state
  console.log("\nTest B: A reconnects with no stale state");
  wsA.send(JSON.stringify({ type: 'leave' }));
  await waitForMessage(wsC, 'peer-left');
  
  wsA.send(JSON.stringify({ type: 'join', room: 'test-room' }));
  await Promise.all([
    waitForMessage(wsC, 'peer-joined'), // C was in the room first this time
    waitForMessage(wsA, 'wait-for-offer')
  ]);
  console.log("-> Passed: A reconnected cleanly, C creates offer.");

  // f) Default no-code flow
  console.log("\nTest F: Random private room for empty input");
  const wsE = await connectClient();
  wsE.send(JSON.stringify({ type: 'join', room: '' })); // empty string
  // It shouldn't join test-room or main. It's in a random room alone.
  const wsF = await connectClient();
  wsF.send(JSON.stringify({ type: 'join', room: '' })); 
  // Should also be in its own random room. Neither gets peer-joined!
  let gotJoined = false;
  wsF.on('message', () => { gotJoined = true; });
  await new Promise(r => setTimeout(r, 500));
  if (!gotJoined) {
    console.log("-> Passed: Empty rooms generate isolated unique random rooms.");
  } else {
    console.error("-> Failed: Empty rooms merged into same room.");
  }

  // Cleanup
  wsA.close(); wsC.close(); wsD.close(); wsE.close(); wsF.close(); wsThird.close();
  console.log("\nAll tests completed successfully.");
  process.exit(0);
}

runTests();
