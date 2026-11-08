import assert from 'node:assert/strict';

// ─────────────────────────────────────────────────────────────
// Minimal browser stubs, installed before importing app modules.
// ─────────────────────────────────────────────────────────────
const lsBacking = new Map();
globalThis.localStorage = {
  getItem: (k) => (lsBacking.has(k) ? lsBacking.get(k) : null),
  setItem: (k, v) => lsBacking.set(k, String(v)),
  removeItem: (k) => lsBacking.delete(k),
};

function makeEl(tagName = 'div') {
  const classes = new Set();
  return {
    tagName: tagName.toUpperCase(),
    children: [],
    attributes: {},
    className: '',
    textContent: '',
    classList: {
      add: (...names) => names.forEach((n) => classes.add(n)),
      contains: (name) => classes.has(name),
    },
    setAttribute(name, value) { this.attributes[name] = String(value); },
    addEventListener() {},
    append(...nodes) { this.children.push(...nodes); },
    appendChild(node) { this.children.push(node); return node; },
    remove() {},
  };
}

const toastContainer = makeEl('div');
globalThis.document = {
  documentElement: { lang: '', dataset: {} },
  getElementById: (id) => (id === 'toast-container' ? toastContainer : null),
  createElement: (tag) => makeEl(tag),
  querySelectorAll: () => [],
  createTreeWalker: () => ({ currentNode: null, nextNode: () => null }),
};
globalThis.NodeFilter = { SHOW_TEXT: 4, FILTER_ACCEPT: 1, FILTER_REJECT: 2, FILTER_SKIP: 3 };
globalThis.CustomEvent = class CustomEvent {
  constructor(type, init) { this.type = type; this.detail = init?.detail; }
};

let reloads = 0;
globalThis.window = {
  location: { protocol: 'http:', host: '127.0.0.1:8000', reload: () => { reloads += 1; } },
  dispatchEvent() {},
  addEventListener() {},
};

// Reconnect timers are captured, not run, so each step is explicit.
const timers = [];
const realSetImmediate = globalThis.setImmediate;
globalThis.setTimeout = (fn, ms) => { timers.push({ fn, ms }); return timers.length; };
globalThis.clearTimeout = () => {};

const sockets = [];
class FakeWebSocket {
  constructor(url) {
    this.url = url;
    this.readyState = FakeWebSocket.CONNECTING;
    sockets.push(this);
  }
  serverOpen() {
    this.readyState = FakeWebSocket.OPEN;
    this.onopen?.();
  }
  serverClose(code) {
    this.readyState = FakeWebSocket.CLOSED;
    this.onclose?.({ code });
  }
}
FakeWebSocket.CONNECTING = 0;
FakeWebSocket.OPEN = 1;
FakeWebSocket.CLOSING = 2;
FakeWebSocket.CLOSED = 3;
globalThis.WebSocket = FakeWebSocket;

const statusCalls = [];
let statusReply = null; // object → JSON body; Error → network failure
globalThis.fetch = async (url) => {
  statusCalls.push(url);
  if (statusReply instanceof Error) throw statusReply;
  return { ok: true, status: 200, json: async () => statusReply };
};

const flush = () => new Promise((resolve) => realSetImmediate(resolve));

const quiet = (fn) => async (...args) => {
  const { log, warn, error } = console;
  console.log = () => {};
  console.warn = () => {};
  console.error = () => {};
  try { return await fn(...args); } finally { Object.assign(console, { log, warn, error }); }
};

const state = {};
const store = { set: (key, value) => { state[key] = value; }, get: (key) => state[key] };

const { initWebSocket, getWebSocketStatus } = await import('./websocket.js');

await quiet(async () => {
  // ───────────────────────────────────────────────────────────
  // 1. A live connection that drops (network / server restart) keeps
  //    the original backoff reconnect, without any auth check.
  // ───────────────────────────────────────────────────────────
  initWebSocket(store);
  assert.equal(sockets.length, 1);
  assert.equal(sockets[0].url, 'ws://127.0.0.1:8000/api/ws/tasks');
  sockets[0].serverOpen();
  assert.equal(getWebSocketStatus(), 'connected');
  sockets[0].serverClose(1006);
  await flush();
  assert.equal(statusCalls.length, 0, 'dropping an open connection must not query auth status');
  assert.equal(timers.length, 1, 'dropped connection → reconnect scheduled');
  timers.shift().fn();
  assert.equal(sockets.length, 2, 'reconnect timer opens a new socket');

  // ───────────────────────────────────────────────────────────
  // 2. Handshake fails (never opened) while still signed in, e.g. a
  //    proxy hiccup: check status, then keep reconnecting.
  // ───────────────────────────────────────────────────────────
  statusReply = { auth_required: true, authenticated: true };
  sockets[1].serverClose(1006);
  await flush();
  assert.equal(statusCalls.length, 1, 'never-opened close → auth status check');
  assert.equal(timers.length, 1, 'signed in → keep reconnecting');
  timers.shift().fn();
  assert.equal(sockets.length, 3);

  // ───────────────────────────────────────────────────────────
  // 3. Server down: status endpoint unreachable too → keep reconnecting.
  // ───────────────────────────────────────────────────────────
  statusReply = new TypeError('Failed to fetch');
  sockets[2].serverClose(1006);
  await flush();
  assert.equal(timers.length, 1, 'network outage → keep the original reconnect behaviour');
  timers.shift().fn();
  assert.equal(sockets.length, 4);

  // ───────────────────────────────────────────────────────────
  // 4. Key-less server, remote browser: handshake rejected and no way
  //    to log in → stop reconnecting, show the notice once.
  //    Regression: it retried every ~30s forever.
  // ───────────────────────────────────────────────────────────
  statusReply = { auth_required: false, authenticated: false };
  sockets[3].serverClose(1006);
  await flush();
  assert.equal(timers.length, 0, 'remote access disabled → no reconnect');
  assert.equal(reloads, 0);
  assert.equal(toastContainer.children.length, 1, 'remote access notice shown');
  assert.equal(getWebSocketStatus(), 'disconnected');

  // ───────────────────────────────────────────────────────────
  // 5. Session expired on an open connection: server closes with 1008
  //    → reload into the login page, no reconnect loop.
  // ───────────────────────────────────────────────────────────
  initWebSocket(store);
  assert.equal(sockets.length, 5);
  sockets[4].serverOpen();
  statusReply = { auth_required: true, authenticated: false };
  sockets[4].serverClose(1008);
  await flush();
  assert.equal(reloads, 1, 'login required → reload to the server login page');
  assert.equal(timers.length, 0, 'login required → no reconnect');
  assert.equal(sockets.length, 5);
})();

console.log('WEBSOCKET_SELFTEST_OK');
