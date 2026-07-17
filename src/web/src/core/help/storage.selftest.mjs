if (typeof globalThis.localStorage === 'undefined') {
  const store = new Map();
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => { store.set(k, String(v)); },
    removeItem: (k) => { store.delete(k); },
  };
}

const { isTourCompleted, markTourCompleted, clearTourCompleted, TOUR_STORAGE_PREFIX } =
  await import('./storage.js');

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(TOUR_STORAGE_PREFIX.startsWith('gamedata-autoflux.help.'), 'prefix');
assert(isTourCompleted('platform-overview') === false, 'fresh false');
markTourCompleted('platform-overview');
assert(isTourCompleted('platform-overview') === true, 'marked true');
clearTourCompleted('platform-overview');
assert(isTourCompleted('platform-overview') === false, 'cleared');
assert(isTourCompleted('') === false, 'empty id');
markTourCompleted('');
assert(isTourCompleted('') === false, 'empty mark no-op');

console.log('HELP_STORAGE_SELFTEST_OK');
