/** Node shims so formatError/i18n can load without a browser. */
if (typeof globalThis.localStorage === 'undefined') {
  const store = new Map();
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => { store.set(k, String(v)); },
    removeItem: (k) => { store.delete(k); },
  };
}
if (typeof globalThis.document === 'undefined') {
  globalThis.document = {
    documentElement: { lang: '' },
    querySelectorAll: () => [],
    createTreeWalker: () => ({ currentNode: null, nextNode: () => false }),
  };
}
if (typeof globalThis.window === 'undefined') globalThis.window = globalThis;
if (typeof globalThis.window.dispatchEvent !== 'function') {
  globalThis.window.dispatchEvent = () => true;
}
if (typeof globalThis.window.addEventListener !== 'function') {
  globalThis.window.addEventListener = () => {};
}
if (typeof globalThis.CustomEvent === 'undefined') {
  globalThis.CustomEvent = class CustomEvent {
    constructor(type, init = {}) {
      this.type = type;
      this.detail = init.detail;
    }
  };
}
if (typeof globalThis.NodeFilter === 'undefined') {
  globalThis.NodeFilter = { SHOW_TEXT: 4, FILTER_ACCEPT: 1, FILTER_REJECT: 2 };
}

const {
  extractErrorCode,
  summarizeTaskFailure,
  collectHealthAttentionItems,
} = await import('./taskFailure.js');

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(extractErrorCode('rate_limited after 3 retries') === 'rate_limited', 'extract plain code');
assert(extractErrorCode('error_code=login_required') === 'login_required', 'extract structured');
assert(extractErrorCode('something broke') === null, 'no code');

const ok = summarizeTaskFailure({ status: 'success', error: null });
assert(ok === null, 'success null');

const failedKnown = summarizeTaskFailure({
  status: 'failed',
  error: 'Collector aborted: rate_limited',
});
assert(failedKnown && failedKnown.known === true, 'known failed');
assert(failedKnown.code === 'rate_limited', 'code rate_limited');
assert(failedKnown.title && failedKnown.title.length > 0, 'has title');

const fromBackend = summarizeTaskFailure({
  status: 'failed',
  error: 'raw detail',
  error_code: 'login_required',
  error_title: '需要登录',
  error_suggestion: '重新登录',
});
assert(fromBackend && fromBackend.code === 'login_required', 'backend code');
assert(fromBackend.title === '需要登录', 'backend title preferred');
assert(fromBackend.suggestion === '重新登录', 'backend suggestion preferred');

const failedPlain = summarizeTaskFailure({
  status: 'failed',
  error: '连接被对端重置，请稍后重试',
});
assert(failedPlain && failedPlain.known === false, 'plain unknown');
assert(failedPlain.title.includes('连接'), 'plain title keeps text');

const failedEmpty = summarizeTaskFailure({ status: 'failed', error: '' });
assert(failedEmpty && failedEmpty.known === false, 'empty error');

const cancelledNoErr = summarizeTaskFailure({ status: 'cancelled', error: null });
assert(cancelledNoErr === null, 'cancelled no error');

const cancelledErr = summarizeTaskFailure({ status: 'cancelled', error: 'user abort login_required' });
assert(cancelledErr && cancelledErr.code === 'login_required', 'cancelled with code');

const healthItems = collectHealthAttentionItems(
  { status: 'ok', checks: [] },
  {
    status: 'error',
    checks: [
      { id: 'db', name: 'Database', status: 'error', message: 'down' },
      { id: 'cache', name: 'Cache', status: 'ok', message: 'fine' },
      { id: 'steam', name: 'SteamDB', status: 'warning', message: 'stale session' },
    ],
  },
);
assert(healthItems.length >= 2, 'health attention collects non-ok');
assert(healthItems.every((i) => i.severity !== 'ok'), 'no ok items');

console.log('TASK_FAILURE_SELFTEST_OK');
