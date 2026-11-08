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
    listeners: {},
    className: '',
    textContent: '',
    classList: {
      add: (...names) => names.forEach((n) => classes.add(n)),
      contains: (name) => classes.has(name),
    },
    setAttribute(name, value) { this.attributes[name] = String(value); },
    getAttribute(name) { return this.attributes[name] ?? null; },
    addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); },
    append(...nodes) { this.children.push(...nodes); },
    appendChild(node) { this.children.push(node); node.parentNode = this; return node; },
    remove() {
      if (this.parentNode) this.parentNode.children = this.parentNode.children.filter((c) => c !== this);
    },
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
  location: { reload: () => { reloads += 1; } },
  dispatchEvent() {},
  addEventListener() {},
};

// Toasts schedule auto-dismiss timers; capture them so node exits immediately.
globalThis.setTimeout = () => 0;

// fetch stub: each test sets `respond` to decide the reply.
const fetchCalls = [];
let respond = () => { throw new Error('unexpected fetch'); };
globalThis.fetch = async (url, init = {}) => {
  fetchCalls.push({ url, method: init.method || 'GET' });
  return respond(url, init);
};

function jsonResponse(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  };
}

function reset() {
  fetchCalls.length = 0;
  reloads = 0;
  toastContainer.children.length = 0;
}

let instance = 0;
async function freshAuth() {
  // Query string → a new module instance with clean module-level state.
  instance += 1;
  return import(`./auth.js?case=${instance}`);
}

const { messages, setLanguage, t } = await import('./i18n.js');
setLanguage('zh-CN', { force: true });

// ─────────────────────────────────────────────────────────────
// 1. Signed in: a 401 from elsewhere must not reload or toast.
// ─────────────────────────────────────────────────────────────
{
  reset();
  const auth = await freshAuth();
  respond = () => jsonResponse(200, { auth_required: true, authenticated: true });
  assert.equal(await auth.handleUnauthorized(), 'authenticated');
  assert.equal(reloads, 0, 'authenticated → no reload');
  assert.equal(toastContainer.children.length, 0, 'authenticated → no toast');
}

// ─────────────────────────────────────────────────────────────
// 2. Login required (cookie expired): reload once so the server
//    serves the login page. Concurrent 401s share one status check.
// ─────────────────────────────────────────────────────────────
{
  reset();
  const auth = await freshAuth();
  respond = () => jsonResponse(200, { auth_required: true, authenticated: false });
  const results = await Promise.all([
    auth.handleUnauthorized(),
    auth.handleUnauthorized(),
    auth.handleUnauthorized(),
  ]);
  assert.deepEqual(results, ['login_required', 'login_required', 'login_required']);
  assert.equal(fetchCalls.length, 1, 'concurrent 401s must share one /api/auth/status request');
  assert.equal(fetchCalls[0].url, '/api/auth/status');
  assert.equal(reloads, 1, 'login required → exactly one reload');

  // Reload already under way: later 401s do nothing more.
  assert.equal(await auth.handleUnauthorized(), 'login_required');
  assert.equal(fetchCalls.length, 1);
  assert.equal(reloads, 1);
  assert.equal(toastContainer.children.length, 0, 'login required must not show the remote-access notice');
}

// ─────────────────────────────────────────────────────────────
// 3. Key-less server, remote/proxied browser: no login is possible,
//    so show a dismissible notice exactly once and never reload.
//    Regression: a dead, undismissable login modal blocked the page.
// ─────────────────────────────────────────────────────────────
{
  reset();
  const auth = await freshAuth();
  respond = () => jsonResponse(200, { auth_required: false, authenticated: false });
  assert.equal(await auth.handleUnauthorized(), 'remote_disabled');
  assert.equal(await auth.handleUnauthorized(), 'remote_disabled');
  assert.equal(reloads, 0, 'key-less remote access must not reload');
  assert.equal(toastContainer.children.length, 1, 'remote-access notice is shown only once');

  const notice = toastContainer.children[0];
  assert.ok(notice.classList.contains('toast-dismissible'), 'notice must be dismissible');
  const message = notice.children.find((c) => c.className === 'toast-message');
  assert.equal(message.textContent, t('auth.remoteAccessNotice'));
  assert.ok(message.textContent.includes('server.api_key'));
  const close = notice.children.find((c) => c.className === 'toast-close');
  assert.ok(close, 'notice must have a close button');
  close.listeners.click[0]();
  assert.equal(toastContainer.children.length, 0, 'close button removes the notice');
  assert.ok(auth.isSignedOut('remote_disabled'));
  assert.ok(auth.isSignedOut('login_required'));
  assert.ok(!auth.isSignedOut('authenticated'));
  assert.ok(!auth.isSignedOut('unknown'));
}

// ─────────────────────────────────────────────────────────────
// 4. Status endpoint unreachable (network down): 'unknown', no side effects.
// ─────────────────────────────────────────────────────────────
{
  reset();
  const auth = await freshAuth();
  const originalError = console.error;
  console.error = () => {};
  respond = () => { throw new TypeError('Failed to fetch'); };
  assert.equal(await auth.handleUnauthorized(), 'unknown');
  respond = () => jsonResponse(503, { detail: 'unavailable' });
  assert.equal(await auth.handleUnauthorized(), 'unknown', 'non-2xx status response → unknown');
  console.error = originalError;
  assert.equal(reloads, 0);
  assert.equal(toastContainer.children.length, 0);
}

// ─────────────────────────────────────────────────────────────
// 5. logout(): reload only after the server confirmed; otherwise show
//    the reason. Regression: errors were swallowed and resp.ok ignored.
// ─────────────────────────────────────────────────────────────
{
  reset();
  const auth = await freshAuth();
  respond = () => jsonResponse(200, { authenticated: false });
  assert.equal(await auth.logout(), true);
  assert.deepEqual(fetchCalls.map((c) => [c.url, c.method]), [['/api/auth/logout', 'POST']]);
  assert.equal(reloads, 1, 'successful logout reloads into the login page');

  reset();
  respond = () => jsonResponse(403, { detail: 'Cross-site requests are not allowed.' });
  assert.equal(await auth.logout(), false);
  assert.equal(reloads, 0, 'failed logout must not reload');
  assert.equal(toastContainer.children.length, 1);
  assert.equal(
    toastContainer.children[0].textContent,
    t('auth.logoutFailed', { error: 'Cross-site requests are not allowed.' }),
  );
  assert.equal(toastContainer.children[0].className, 'toast toast-error');

  reset();
  respond = () => ({ ok: false, status: 500, json: async () => { throw new SyntaxError('not json'); } });
  assert.equal(await auth.logout(), false);
  assert.equal(toastContainer.children[0].textContent, t('auth.logoutFailed', { error: 'HTTP 500' }));

  reset();
  respond = () => jsonResponse(422, { detail: [{ loc: ['body'], msg: 'bad' }] });
  assert.equal(await auth.logout(), false);
  assert.equal(
    toastContainer.children[0].textContent,
    t('auth.logoutFailed', { error: 'HTTP 422' }),
    'array detail must not render as [object Object]',
  );

  reset();
  respond = () => { throw new TypeError('Failed to fetch'); };
  assert.equal(await auth.logout(), false);
  assert.equal(reloads, 0);
  assert.equal(toastContainer.children[0].textContent, t('auth.logoutFailed', { error: 'Failed to fetch' }));
}

// ─────────────────────────────────────────────────────────────
// 6. i18n: new keys exist in both languages; the removed login-modal
//    keys stay gone; no global logout hook leaks onto window.
// ─────────────────────────────────────────────────────────────
for (const lang of ['zh-CN', 'en-US']) {
  for (const key of ['auth.logout', 'auth.logoutFailed', 'auth.remoteAccessNotice', 'common.close']) {
    assert.ok(messages[lang]?.[key], `${lang} must define ${key}`);
  }
  for (const key of ['auth.title', 'auth.description', 'auth.apiKey', 'auth.login', 'auth.invalid', 'auth.tooMany']) {
    assert.equal(messages[lang]?.[key], undefined, `${lang} still defines unused key ${key}`);
  }
}
assert.equal(globalThis.window.autofluxLogout, undefined, 'auth.js must not expose window.autofluxLogout');

console.log('AUTH_SELFTEST_OK');
