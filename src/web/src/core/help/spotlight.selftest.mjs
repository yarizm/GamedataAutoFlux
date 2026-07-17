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
  const elListeners = [];
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
    appendChild(child) { children.push(child); child.parentNode = el; return child; },
    addEventListener(type, fn) { elListeners.push({ type, fn }); },
    removeEventListener(type, fn) {
      const i = elListeners.findIndex((l) => l.type === type && l.fn === fn);
      if (i >= 0) elListeners.splice(i, 1);
    },
    dispatchEvent(event) {
      for (const l of elListeners) {
        if (l.type === event.type) l.fn(event);
      }
      return true;
    },
    querySelector() { return null; },
    closest(sel) {
      if (typeof sel === 'string' && sel.includes('data-spot-action') && attrs['data-spot-action'] != null) {
        return el;
      }
      return null;
    },
    focus() {},
    getBoundingClientRect() {
      return { top: 10, left: 10, width: 100, height: 40, bottom: 50, right: 110 };
    },
    scrollIntoView() {},
    offsetWidth: 320,
    offsetHeight: 120,
    contains(node) {
      if (node === el) return true;
      const walk = (list) => {
        for (const c of list) {
          if (c === node) return true;
          if (c.children && walk(c.children)) return true;
        }
        return false;
      };
      return walk(children);
    },
    _listeners: elListeners,
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

/** Active spotlight root (last is-open; each createSpotlight may append its own). */
function activeSpotlightRoot() {
  const roots = bodyChildren.filter((c) => c.id === 'help-spotlight-root');
  const open = roots.filter((r) => r.classList.contains('is-open'));
  return open[open.length - 1] || roots[roots.length - 1] || null;
}

/** Simulate Next/Done on the active bubble (stub DOM). */
function clickSpotAction(action) {
  const root = activeSpotlightRoot();
  assert(root, 'spotlight root for click');
  const bubble = root.children.find((c) => (c.className || '').includes('help-spot-bubble'))
    || root.children[1];
  assert(bubble, 'bubble for click');
  const btn = makeEl('button');
  btn.setAttribute('data-spot-action', action);
  bubble.appendChild(btn);
  bubble.dispatchEvent({ type: 'click', target: btn, preventDefault() {} });
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

// Real tour: all targets missing → skip all → finish WITHOUT mark
const tour = getTour('platform-overview');
assert(tour && tour.steps.length >= 2, 'platform-overview exists');
clearTourCompleted('platform-overview');
assert(isTourCompleted('platform-overview') === false, 'not completed before');

spot.start('platform-overview');
assert(spot.isActive() === true, 'active after start');

// Wait for async showStep chain (missing targets advance until complete)
await new Promise((r) => setTimeout(r, 80));

assert(spot.isActive() === false, 'inactive after auto-finish (all missing)');
assert(isTourCompleted('platform-overview') === false, 'all-missing does NOT mark completed');
assert(completed === 1, 'onComplete once even when unmarked');
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
const rootAfterStop = activeSpotlightRoot() || document.getElementById('help-spotlight-root');
assert(rootAfterStop, 'spotlight root exists after start');
// after stop, not open
assert(rootAfterStop.classList.contains('is-open') === false, 'root not is-open after stop');

// One real target then missing → mark completed if at least one shown
clearTourCompleted('page-dashboard');
let completed3 = 0;
const spot3 = createSpotlight({
  ensurePage: async () => {},
  activateTab: () => {},
  onComplete: () => { completed3 += 1; },
});

// Inject fake DOM target with data-tour-id for first step only
const fakeStats = makeEl('div');
fakeStats.setAttribute('data-tour-id', 'dashboard-stats');
document.querySelector = (sel) => {
  if (typeof sel === 'string' && sel.includes('dashboard-stats')) {
    return fakeStats;
  }
  return null;
};

spot3.start('page-dashboard');
assert(spot3.isActive() === true, 'spot3 active after start');
// Wait for first step to render hole/bubble
await new Promise((r) => setTimeout(r, 50));
assert(spot3.isActive() === true, 'spot3 still active after first shown step');
assert(isTourCompleted('page-dashboard') === false, 'not marked before finish');

// Advance past first (shown) step; remaining targets missing → auto-skip → complete with mark
clickSpotAction('next');
await new Promise((r) => setTimeout(r, 80));

assert(spot3.isActive() === false, 'spot3 inactive after finish');
assert(isTourCompleted('page-dashboard') === true, 'at least one shown → marked completed');
assert(completed3 === 1, 'spot3 onComplete once');

document.querySelector = realQS;
void realToastContainer;
void toasts;
void markTourCompleted;

console.log('HELP_SPOTLIGHT_SELFTEST_OK');
