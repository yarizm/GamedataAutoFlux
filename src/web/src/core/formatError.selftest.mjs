/** Node shims so i18n.js can load without a browser. */
if (typeof globalThis.localStorage === 'undefined') {
  const store = new Map();
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => {
      store.set(k, String(v));
    },
    removeItem: (k) => {
      store.delete(k);
    },
  };
}

if (typeof globalThis.document === 'undefined') {
  globalThis.document = {
    documentElement: { lang: '' },
    querySelectorAll: () => [],
    createTreeWalker: () => ({ currentNode: null, nextNode: () => false }),
  };
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
if (typeof globalThis.window === 'undefined') {
  globalThis.window = globalThis;
}
if (typeof globalThis.window.dispatchEvent !== 'function') {
  globalThis.window.dispatchEvent = () => true;
}
if (typeof globalThis.window.addEventListener !== 'function') {
  globalThis.window.addEventListener = () => {};
}

const {
  formatErrorCode,
  formatApiError,
  formatPrecheckIssue,
  formatPrecheckCategory,
  KNOWN_ERROR_CODES,
} = await import('./formatError.js');
const { setLanguage, t } = await import('./i18n.js');

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

// Default language is zh-CN
setLanguage('zh-CN', { force: true });

const rate = formatErrorCode('rate_limited');
assert(rate.known === true, 'rate_limited should be known');
assert(rate.title === '频率限制', `rate_limited zh title: ${rate.title}`);
assert(
  rate.suggestion && rate.suggestion.includes('降低采集频率'),
  `rate_limited zh suggestion: ${rate.suggestion}`,
);

const missing = formatErrorCode('missing_credentials');
assert(missing.title === '凭证缺失', `missing_credentials title: ${missing.title}`);
assert(
  missing.suggestion && missing.suggestion.includes('.env'),
  `missing_credentials suggestion: ${missing.suggestion}`,
);

const empty = formatErrorCode('');
assert(empty.known === false, 'empty code known=false');
assert(empty.title === '未知错误', `empty code title: ${empty.title}`);

const unknown = formatErrorCode('not_a_real_code_xyz');
assert(unknown.known === false, 'unknown should not be known');
assert(unknown.title === 'not_a_real_code_xyz', `unknown title: ${unknown.title}`);
assert(unknown.suggestion === null, 'unknown suggestion should be null');

assert(KNOWN_ERROR_CODES.includes('unknown'), 'KNOWN_ERROR_CODES has unknown');
assert(KNOWN_ERROR_CODES.length === 9, `KNOWN_ERROR_CODES length: ${KNOWN_ERROR_CODES.length}`);

const apiKnown = formatApiError({ error_code: 'login_required' });
assert(apiKnown === '需要登录', `formatApiError known: ${apiKnown}`);

const apiMsg = formatApiError({ message: 'boom' });
assert(apiMsg === 'boom', `formatApiError message: ${apiMsg}`);

const apiNull = formatApiError(null);
assert(apiNull === t('error.code.unknown'), `formatApiError null: ${apiNull}`);

const issue = formatPrecheckIssue({
  code: 'missing_steam_app_id',
  message: 'Steam app_id is recommended',
  suggested_action: 'Provide app_id',
});
assert(issue.title === '缺少 Steam App ID', `precheck title: ${issue.title}`);
assert(issue.message === 'Steam app_id is recommended', `precheck message: ${issue.message}`);
assert(issue.suggestion === 'Provide app_id', `precheck suggestion: ${issue.suggestion}`);

assert(formatPrecheckCategory('credential') === '凭证', 'category credential zh');
assert(formatPrecheckCategory('nope_cat') === 'nope_cat', 'unknown category passthrough');

setLanguage('en-US', { force: true });
const enRate = formatErrorCode('rate_limited');
assert(enRate.title === 'Rate limited', `rate_limited en title: ${enRate.title}`);
assert(enRate.known === true, 'rate_limited en known');
const enApi = formatApiError({ code: 'anti_bot_blocked' });
assert(enApi === 'Anti-bot blocked', `formatApiError en: ${enApi}`);
assert(formatPrecheckCategory('session') === 'Session', 'category session en');

// Restore default for any subsequent imports in same process
setLanguage('zh-CN', { force: true });

console.log('FORMAT_ERROR_SELFTEST_OK');
