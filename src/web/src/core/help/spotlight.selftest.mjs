/**
 * Lightweight smoke for createSpotlight without a browser.
 * Stubs document/window/localStorage enough for import + start/stop/isActive.
 */

if (typeof globalThis.localStorage === 'undefined') {
  const store = new Map();
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => { store.set(k, String(v)); },
    removeItem: (k) => { store.delete(k); },
  };
}

const listeners = { document: [], window: [] };
const bodyChildren = [];

function makeEl(tag = 'div') {
  const attrs = {};
  const style = {};
  const children = [];
  const el = {
    tagName: String(tag).toUpperCase(),
    className: '',
    id: '',
    style,
    children,
    innerHTML: '',
    textContent: '',
    classList: {
      _set: new Set(),
      add(c) { this._set.add(c); el.className = [...this._set].join(' '); },
      remove(c) { this._set.delete(c); el.className = [...this._set].join(' '); },
      contains(c) { return this._set.has(c); },
      toggle(c, force) {
        if (force === true) this.add(c);
        else if (force === false) this.remove(c);
        else if (this.contains(c)) this.remove(c);
        else this.add(c);
      },
    },
    setAttribute(k, v) { attrs[k] = String(v); },
    getAttribute(k) { return attrs[k] ?? null; },
    removeAttribute(k) { delete attrs[k]; },
    appendChild(child) { children.push(child); return child; },
    addEventListener() {},
    removeEventListener() {},
    querySelector() { return null; },
    closest() { return null; },
    focus() {},
    getBoundingClientRect() {
      return { top: 10, left: 10, width: 100, height: 40, bottom: 50, right: 110 };
    },
    scrollIntoView() {},
    offsetWidth: 320,
    offsetHeight: 120,
    contains(node) { return children.includes(node) || node === el; },
  };
  return el;
}

const body = makeEl('body');
body.appendChild = (child) => {
  bodyChildren.push(child);
  body.children.push(child);
  return child;
};

globalThis.document = {
  body,
  documentElement: { lang: 'zh-CN' },
  createElement: (tag) => makeEl(tag),
  getElementById: (id) => bodyChildren.find((c) => c.id === id) || null,
  querySelector: () => null, // missing targets → toast + skip all
  querySelectorAll: () => [],
  addEventListener: (type, fn) => listeners.document.push({ type, fn }),
  removeEventListener: () => {},
};

globalThis.window = {
  innerWidth: 1200,
  innerHeight: 800,
  addEventListener: (type, fn) => listeners.window.push({ type, fn }),
  removeEventListener: () => {},
  dispatchEvent: () => true,
};

globalThis.requestAnimationFrame = (cb) => setTimeout(() => cb(0), 0);

// toast container absent → toast no-ops
const toasts = [];
const realToastContainer = null;

const { createSpotlight } = await import('./spotlight.js');
const { isTourCompleted, clearTourCompleted, markTourCompleted } = await import('./storage.js');
const { getTour } = await import('./content.js');

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

// API shape
const ensured = [];
const activated = [];
let completed = 0;
const spot = createSpotlight({
  ensurePage: async (tab) => { ensured.push(tab); },
  activateTab: (tab) => { activated.push(tab); },
  onComplete: () => { completed += 1; },
});

assert(typeof spot.start === 'function', 'start');
assert(typeof spot.stop === 'function', 'stop');
assert(typeof spot.isActive === 'function', 'isActive');
assert(spot.isActive() === false, 'inactive initially');

// Unknown tour no-op
spot.start('__no_such_tour__');
assert(spot.isActive() === false, 'unknown tour inactive');

// Real tour: all targets missing → skip all → mark completed
const tour = getTour('platform-overview');
assert(tour && tour.steps.length >= 2, 'platform-overview exists');
clearTourCompleted('platform-overview');
assert(isTourCompleted('platform-overview') === false, 'not completed before');

spot.start('platform-overview');
assert(spot.isActive() === true, 'active after start');

// Wait for async showStep chain (missing targets advance until complete)
await new Promise((r) => setTimeout(r, 80));

assert(spot.isActive() === false, 'inactive after auto-complete (all missing)');
assert(isTourCompleted('platform-overview') === true, 'marked completed after full pass');
assert(completed === 1, 'onComplete once');
// ensure-tab:dashboard should have run for step 2
assert(ensured.includes('dashboard') || activated.includes('dashboard'), 'ensure-tab:dashboard ran');

// Skip mid-way without mark
clearTourCompleted('page-dashboard');
const spot2 = createSpotlight({
  ensurePage: async () => {},
  activateTab: () => {},
});
// Inject a resolvable target for first step only by patching querySelector
let call = 0;
const realQS = document.querySelector;
document.querySelector = (sel) => {
  call += 1;
  // first successful match once
  if (call === 1 && sel.includes('dashboard-stats')) {
    return makeEl('div');
  }
  return null;
};

spot2.start('page-dashboard');
assert(spot2.isActive() === true, 'spot2 active');
// stop mid-way (like Esc / skip)
spot2.stop();
assert(spot2.isActive() === false, 'spot2 stopped');
assert(isTourCompleted('page-dashboard') === false, 'mid-stop does not mark');

// Esc handler registered on document
assert(
  listeners.document.some((l) => l.type === 'keydown'),
  'Esc keydown listener registered',
);

// Root has is-open contract for drawer Esc deferral
const root = document.getElementById('help-spotlight-root');
assert(root, 'spotlight root exists after start');
// after stop, not open
assert(root.classList.contains('is-open') === false, 'root not is-open after stop');

document.querySelector = realQS;
void realToastContainer;
void toasts;
void markTourCompleted;

console.log('HELP_SPOTLIGHT_SELFTEST_OK');
