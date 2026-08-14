if (typeof globalThis.localStorage === 'undefined') {
  const store = new Map();
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => { store.set(k, String(v)); },
    removeItem: (k) => { store.delete(k); },
  };
}

const {
  isTourCompleted,
  markTourCompleted,
  clearTourCompleted,
  isInlineGuideCollapsed,
  setInlineGuideCollapsed,
  TOUR_STORAGE_PREFIX,
  INLINE_GUIDE_STORAGE_PREFIX,
} = await import('./storage.js');

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

assert(INLINE_GUIDE_STORAGE_PREFIX === 'gamedata-autoflux.help.inline.', 'inline prefix');
assert(isInlineGuideCollapsed('tasks', 'v1') === false, 'inline expands initially');
setInlineGuideCollapsed('tasks', true, 'v1');
assert(isInlineGuideCollapsed('tasks', 'v1') === true, 'inline close remembered');
assert(isInlineGuideCollapsed('tasks', 'v2') === false, 'version upgrade expands');
setInlineGuideCollapsed('tasks', false, 'v1');
assert(isInlineGuideCollapsed('tasks', 'v1') === false, 'inline can reopen');

const originalLocalStorage = globalThis.localStorage;
globalThis.localStorage = {
  getItem() { throw new Error('disabled'); },
  setItem() { throw new Error('disabled'); },
  removeItem() { throw new Error('disabled'); },
};
assert(isInlineGuideCollapsed('tasks', 'v1') === false, 'inline unavailable safe read');
setInlineGuideCollapsed('tasks', true, 'v1');
markTourCompleted('unavailable');
globalThis.localStorage = originalLocalStorage;

console.log('HELP_STORAGE_SELFTEST_OK');
