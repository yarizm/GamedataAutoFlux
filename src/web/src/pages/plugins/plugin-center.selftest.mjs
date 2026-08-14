import {
  inventoryRequiresRestart,
  operationIsActive,
  pluginActivationPending,
  pluginMatchesFilters,
  pluginStateTranslationKey,
} from './model.js';

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

const sample = {
  display_name: 'YouTube',
  distribution: 'autoflux-plugin-youtube',
  description: 'Video and comment collector',
  collectors: ['youtube_profiles', 'youtube_comments'],
  source_type: 'development',
  runtime_state: 'active',
};

assert(pluginMatchesFilters(sample, { search: 'comment' }), 'searches description/collectors');
assert(pluginMatchesFilters(sample, { source: 'development' }), 'matches source');
assert(pluginMatchesFilters(sample, { state: 'active' }), 'matches runtime state');
assert(!pluginMatchesFilters(sample, { source: 'external' }), 'rejects another source');
assert(!pluginMatchesFilters(sample, { search: 'steam' }), 'rejects another plugin');
assert(pluginStateTranslationKey('not_loaded') === 'plugins.state.notLoaded', 'normalizes not_loaded');
assert(operationIsActive({ state: 'queued' }), 'queued operation is active');
assert(operationIsActive({ state: 'running' }), 'running operation is active');
assert(!operationIsActive({ state: 'succeeded' }), 'finished operation is inactive');
assert(pluginActivationPending({
  restart_required: true,
  desired_state: 'enabled',
  runtime_state: 'not_loaded',
}), 'enabled plugin awaiting activation is pending restart');
assert(!pluginActivationPending({
  restart_required: true,
  desired_state: 'disabled',
  runtime_state: 'active',
}), 'disable pending is not presented as activation pending');
assert(inventoryRequiresRestart({ restart_required: 1 }), 'inventory can require a restart');
assert(!inventoryRequiresRestart({ restart_required: 0 }), 'reconciled inventory clears restart requirement');

if (typeof globalThis.localStorage === 'undefined') {
  const values = new Map();
  globalThis.localStorage = {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, String(value)),
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
if (typeof globalThis.window.dispatchEvent !== 'function') globalThis.window.dispatchEvent = () => true;
if (typeof globalThis.window.addEventListener !== 'function') globalThis.window.addEventListener = () => {};
if (typeof globalThis.CustomEvent === 'undefined') {
  globalThis.CustomEvent = class CustomEvent {
    constructor(type, init = {}) { this.type = type; this.detail = init.detail; }
  };
}
if (typeof globalThis.NodeFilter === 'undefined') {
  globalThis.NodeFilter = { SHOW_TEXT: 4, FILTER_ACCEPT: 1, FILTER_REJECT: 2 };
}

const { messages } = await import('../../core/i18n.js');
const requiredKeys = [
  'nav.plugins',
  'plugins.title',
  'plugins.managed.title',
  'plugins.source.managed',
  'plugins.source.external',
  'plugins.source.development',
  'plugins.state.active',
  'plugins.state.failed',
  'plugins.state.notLoaded',
  'plugins.state.inactive',
  'plugins.environment.title',
  'plugins.mode.read_only',
  'plugins.mode.mutable',
  'plugins.catalog.title',
  'plugins.catalog.install',
  'plugins.catalog.upgrade',
  'plugins.action.enable',
  'plugins.action.disable',
  'plugins.action.upgrade',
  'plugins.action.uninstall',
  'plugins.uninstall.referenced',
  'plugins.rollback.action',
  'plugins.upload.confirm',
  'plugins.operations.title',
  'plugins.operations.state.running',
  'plugins.operations.delete',
  'plugins.operations.deleteConfirm',
  'help.map.plugins.title',
  'help.page.plugins.summary',
];
for (const language of ['zh-CN', 'en-US']) {
  for (const key of requiredKeys) {
    assert(key in messages[language], `${language} missing ${key}`);
  }
}

console.log('PLUGIN_CENTER_SELFTEST_OK');
