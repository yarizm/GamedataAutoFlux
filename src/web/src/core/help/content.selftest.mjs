const {
  MAIN_TABS, mapCards, pageHelp, tours, getPageHelp, getTour,
} = await import('./content.js');

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(MAIN_TABS.length === 9, '9 tabs');
assert(mapCards.length === 9, '9 map cards');
const mapTabs = mapCards.map((c) => c.tab);
for (const tab of MAIN_TABS) {
  assert(mapTabs.includes(tab), `map missing ${tab}`);
  assert(getPageHelp(tab), `pageHelp missing ${tab}`);
  const ph = getPageHelp(tab);
  assert(ph.summaryKey && Array.isArray(ph.points) && ph.points.length >= 2, `pageHelp shape ${tab}`);
  assert(Array.isArray(ph.nextSteps), `nextSteps ${tab}`);
}
assert(mapCards.find((c) => c.tab === 'dag')?.badge === 'advanced', 'dag advanced');
// narrative order: dashboard first, system last
assert(mapCards[0].tab === 'dashboard', 'map starts dashboard');
assert(mapCards[mapCards.length - 1].tab === 'system', 'map ends system');

const requiredTours = ['platform-overview', 'page-dashboard', 'page-tasks', 'page-agent'];
for (const id of requiredTours) {
  const tour = getTour(id);
  assert(tour, `tour ${id}`);
  assert(tour.steps.length >= 2, `tour steps ${id}`);
  for (const step of tour.steps) {
    assert(step.target && step.titleKey && step.bodyKey, `step fields ${id}`);
  }
}
for (const tab of ['dashboard', 'tasks', 'agent']) {
  const tid = pageHelp[tab].tourId;
  assert(tid && getTour(tid), `page tour link ${tab}`);
}

// collect all keys for later i18n check
const keys = new Set();
function add(k) { if (k) keys.add(k); }
for (const c of mapCards) { add(c.titleKey); add(c.blurbKey); }
for (const tab of MAIN_TABS) {
  const ph = pageHelp[tab];
  add(ph.summaryKey);
  ph.points.forEach(add);
  ph.nextSteps.forEach((s) => add(s.labelKey));
}
for (const tour of Object.values(tours)) {
  add(tour.titleKey);
  tour.steps.forEach((s) => { add(s.titleKey); add(s.bodyKey); });
}
add('help.entry');
add('help.badge.advanced');
add('help.empty.page');
add('help.section.map');
add('help.section.page');
add('help.section.tours');
add('help.openPage');
add('help.startPageTour');
add('help.tour.start');
add('help.tour.back');
add('help.tour.next');
add('help.tour.skip');
add('help.tour.done');
add('help.tour.replay');
add('help.tour.completed');
add('help.tour.missingTarget');

// DOM stubs for i18n module side effects under Node
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
    constructor(type, init = {}) { this.type = type; this.detail = init.detail; }
  };
}
if (typeof globalThis.NodeFilter === 'undefined') {
  globalThis.NodeFilter = { SHOW_TEXT: 4, FILTER_ACCEPT: 1, FILTER_REJECT: 2 };
}

const { messages } = await import('../i18n.js');
const missing = { 'zh-CN': [], 'en-US': [] };
for (const lang of ['zh-CN', 'en-US']) {
  const bag = messages[lang] || {};
  for (const k of keys) {
    if (!(k in bag)) missing[lang].push(k);
  }
}
if (missing['zh-CN'].length || missing['en-US'].length) {
  const zh = missing['zh-CN'].join(', ') || '(none)';
  const en = missing['en-US'].join(', ') || '(none)';
  throw new Error(
    `Missing i18n keys\nzh-CN (${missing['zh-CN'].length}): ${zh}\nen-US (${missing['en-US'].length}): ${en}`,
  );
}

console.log('HELP_CONTENT_SELFTEST_OK');
