import { resolveTheme, normalizePreference } from './theme.js';

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(normalizePreference('nope') === 'dark', 'default pref');
assert(resolveTheme('dark', false) === 'dark', 'force dark');
assert(resolveTheme('light', true) === 'light', 'force light');
assert(resolveTheme('system', true) === 'dark', 'system dark');
assert(resolveTheme('system', false) === 'light', 'system light');
assert(resolveTheme('invalid', true) === 'dark', 'invalid falls back to dark pref → dark');

console.log('THEME_SELFTEST_OK');
