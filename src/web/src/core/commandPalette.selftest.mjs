import assert from 'node:assert/strict';

// ─────────────────────────────────────────────────────────────
// Minimal browser stubs. Must be installed *before* importing any
// app module, so the imports below are dynamic (top-level await).
// ─────────────────────────────────────────────────────────────
class FakeClassList {
  constructor() { this._set = new Set(); }
  add(...names) { names.forEach((n) => this._set.add(n)); }
  remove(...names) { names.forEach((n) => this._set.delete(n)); }
  contains(name) { return this._set.has(name); }
  toggle(name, force) {
    const next = force === undefined ? !this._set.has(name) : Boolean(force);
    if (next) this._set.add(name); else this._set.delete(name);
    return next;
  }
}

function makeEl(tagName = 'DIV', extra = {}) {
  return {
    tagName,
    dataset: {},
    style: {},
    classList: new FakeClassList(),
    isContentEditable: false,
    closest: () => null,
    querySelector: () => null,
    querySelectorAll: () => [],
    setAttribute() {},
    getAttribute() { return null; },
    addEventListener() {},
    scrollIntoView() {},
    ...extra,
  };
}

const lsBacking = new Map();
globalThis.localStorage = {
  getItem: (k) => (lsBacking.has(k) ? lsBacking.get(k) : null),
  setItem: (k, v) => lsBacking.set(k, String(v)),
  removeItem: (k) => lsBacking.delete(k),
};

globalThis.NodeFilter = {
  SHOW_TEXT: 4,
  FILTER_ACCEPT: 1,
  FILTER_REJECT: 2,
  FILTER_SKIP: 3,
};

const docEl = makeEl('HTML');
globalThis.document = {
  documentElement: docEl,
  body: makeEl('BODY'),
  activeElement: makeEl('BODY'),
  getElementById: () => null,
  querySelector: () => null,
  querySelectorAll: () => [],
  createTreeWalker: () => ({ currentNode: null, nextNode: () => null }),
  addEventListener() {},
};

globalThis.CustomEvent = class CustomEvent {
  constructor(type, init) { this.type = type; this.detail = init?.detail; }
};

globalThis.window = {
  location: { hash: '' },
  history: {},
  matchMedia: () => ({ matches: false, addEventListener() {}, removeEventListener() {} }),
  dispatchEvent() {},
  addEventListener() {},
};

const palette = await import('./commandPalette.js');
const { filterCommands, getStandardCommands } = palette;
const { messages, setLanguage } = await import('./i18n.js');

const mockStore = {
  get(key) { return key === 'activeTab' ? 'tasks' : undefined; },
  set() {},
};

// ─────────────────────────────────────────────────────────────
// 1. Standard commands return navigation and actions
// ─────────────────────────────────────────────────────────────
const commands = getStandardCommands(mockStore);
assert.ok(commands.length >= 15);
assert.ok(commands.some((c) => c.id === 'nav-dashboard'));
assert.ok(commands.some((c) => c.id === 'act-create-task'));
assert.ok(commands.some((c) => c.id === 'act-theme-dark'));

// ─────────────────────────────────────────────────────────────
// 2. Fuzzy search matching
// ─────────────────────────────────────────────────────────────
const taskResults = filterCommands('task', commands);
assert.ok(taskResults.length >= 2);
assert.ok(taskResults.some((c) => c.id === 'nav-tasks'));
assert.ok(taskResults.some((c) => c.id === 'act-create-task'));

const chineseResults = filterCommands('深色', commands);
assert.ok(chineseResults.some((c) => c.id === 'act-theme-dark'));

const emptyResults = filterCommands('nonexistent_command_xyz123', commands);
assert.equal(emptyResults.length, 0);

// ─────────────────────────────────────────────────────────────
// 3. Theme commands must actually change the theme.
//    Regression: they called `window.setTheme`, which is defined
//    nowhere, and `?.()` swallowed the failure silently.
// ─────────────────────────────────────────────────────────────
delete docEl.dataset.theme;

commands.find((c) => c.id === 'act-theme-dark').action();
assert.equal(
  docEl.dataset.theme,
  'dark',
  'act-theme-dark must apply the dark theme, not call an undefined global',
);

commands.find((c) => c.id === 'act-theme-light').action();
assert.equal(
  docEl.dataset.theme,
  'light',
  'act-theme-light must apply the light theme',
);

commands.find((c) => c.id === 'act-theme-system').action();
assert.equal(
  docEl.dataset.themePreference,
  'system',
  'act-theme-system must persist the "system" preference',
);

// ─────────────────────────────────────────────────────────────
// 4. The palette's empty-state i18n key must exist in every language.
//    Regression: `t()` falls back to the key itself, which is truthy,
//    so `t(missing) || '兜底'` renders the raw key to the user.
// ─────────────────────────────────────────────────────────────
for (const lang of ['zh-CN', 'en-US']) {
  assert.ok(
    messages[lang]?.['common.empty.noResults'],
    `${lang} must define common.empty.noResults`,
  );
  assert.ok(
    messages[lang]?.['commandPalette.badge.navigation'],
    `${lang} must define commandPalette.badge.navigation`,
  );
  assert.ok(
    messages[lang]?.['commandPalette.badge.action'],
    `${lang} must define commandPalette.badge.action`,
  );
}

// ─────────────────────────────────────────────────────────────
// 5. Category badges must be translated, not hardcoded Chinese.
// ─────────────────────────────────────────────────────────────
const { getCategoryBadgeLabel } = palette;
assert.equal(
  typeof getCategoryBadgeLabel,
  'function',
  'commandPalette must export getCategoryBadgeLabel',
);

setLanguage('zh-CN', { force: true });
assert.equal(getCategoryBadgeLabel('navigation'), '页面');
assert.equal(getCategoryBadgeLabel('action'), '动作');

setLanguage('en-US');
assert.equal(getCategoryBadgeLabel('navigation'), 'Page', 'badge must follow the active language');
assert.equal(getCategoryBadgeLabel('action'), 'Action', 'badge must follow the active language');

setLanguage('zh-CN');

// ─────────────────────────────────────────────────────────────
// 6. Ctrl+K must not be hijacked inside text inputs / editors.
//    Regression: `activeTag` was computed then ignored, and
//    preventDefault() ran unconditionally.
// ─────────────────────────────────────────────────────────────
const { shouldInterceptPaletteShortcut } = palette;
assert.equal(
  typeof shouldInterceptPaletteShortcut,
  'function',
  'commandPalette must export shouldInterceptPaletteShortcut',
);

assert.equal(shouldInterceptPaletteShortcut(makeEl('BODY')), true, 'plain body → intercept');
assert.equal(shouldInterceptPaletteShortcut(null), true, 'no target → intercept');
assert.equal(shouldInterceptPaletteShortcut(makeEl('INPUT')), false, 'input → do not hijack');
assert.equal(shouldInterceptPaletteShortcut(makeEl('TEXTAREA')), false, 'textarea → do not hijack');
assert.equal(shouldInterceptPaletteShortcut(makeEl('SELECT')), false, 'select → do not hijack');
assert.equal(
  shouldInterceptPaletteShortcut(makeEl('DIV', { isContentEditable: true })),
  false,
  'contenteditable → do not hijack',
);
assert.equal(
  shouldInterceptPaletteShortcut(
    makeEl('DIV', { closest: (sel) => (String(sel).includes('cm-editor') ? makeEl('DIV') : null) }),
  ),
  false,
  'CodeMirror editor → do not hijack',
);
// The palette's own search box is the one input where Ctrl+K should still toggle.
assert.equal(
  shouldInterceptPaletteShortcut(makeEl('INPUT', { id: 'command-palette-input' })),
  true,
  'palette input → Ctrl+K still toggles the palette',
);

// ─────────────────────────────────────────────────────────────
// 7. Commands must activate the tab through the store, so they work
//    even when the target hash equals the current hash.
//    Regression: navigate() was called without { store }, making any
//    command whose hash already matched a silent no-op.
// ─────────────────────────────────────────────────────────────
globalThis.window.location.hash = '#/tasks?modal=create';
const activated = [];
const trackingStore = {
  get: () => 'tasks',
  set: (key, value) => activated.push([key, value]),
};

getStandardCommands(trackingStore).find((c) => c.id === 'act-create-task').action();
assert.ok(
  activated.some(([k, v]) => k === 'activeTab' && v === 'tasks'),
  'act-create-task must activate the tab even when the hash is unchanged',
);

globalThis.window.location.hash = '#/reports';
activated.length = 0;
getStandardCommands(trackingStore).find((c) => c.id === 'nav-reports').action();
assert.ok(
  activated.some(([k, v]) => k === 'activeTab' && v === 'reports'),
  'nav-reports must activate the tab even when the hash is unchanged',
);

console.log('COMMAND_PALETTE_SELFTEST_OK');
