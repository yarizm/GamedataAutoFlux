import { t } from './i18n.js';

export const KNOWN_ERROR_CODES = [
  'missing_credentials',
  'login_required',
  'anti_bot_blocked',
  'network_unreachable',
  'site_structure_changed',
  'empty_data',
  'rate_limited',
  'invalid_params',
  'unknown',
];

/**
 * @param {string|null|undefined} code
 * @returns {{ title: string, suggestion: string|null, known: boolean }}
 */
export function formatErrorCode(code) {
  const c = String(code || '').trim();
  if (!c) {
    return {
      title: t('error.code.unknown'),
      suggestion: t('error.suggestion.unknown'),
      known: false,
    };
  }
  const titleKey = `error.code.${c}`;
  const sugKey = `error.suggestion.${c}`;
  const title = t(titleKey);
  const hasTitle = title !== titleKey;
  const known = KNOWN_ERROR_CODES.includes(c) && hasTitle;
  if (!known && !hasTitle) {
    return { title: c, suggestion: null, known: false };
  }
  const suggestionRaw = t(sugKey);
  return {
    title: hasTitle ? title : c,
    suggestion: suggestionRaw === sugKey ? null : suggestionRaw,
    known,
  };
}

/**
 * @param {object} issue
 * @returns {{ title: string, message: string, suggestion: string|null }}
 */
export function formatPrecheckIssue(issue) {
  const code = issue?.code || '';
  const codeKey = code ? `precheck.code.${code}` : '';
  const mapped = codeKey ? t(codeKey) : '';
  const title = mapped && mapped !== codeKey ? mapped : code || t('common.warning');
  const message = issue?.message || '';
  const suggestion = issue?.suggested_action || null;
  return { title, message, suggestion };
}

/**
 * @param {string} cat
 * @returns {string}
 */
export function formatPrecheckCategory(cat) {
  const key = `precheck.category.${cat}`;
  const label = t(key);
  return label === key ? cat : label;
}

/**
 * @param {any} err
 * @returns {string}
 */
export function formatApiError(err) {
  if (!err) return t('error.code.unknown');
  const code = err.error_code || err.code || err?.detail?.error_code;
  if (code) {
    const { title, known } = formatErrorCode(code);
    if (known) return title;
  }
  if (typeof err === 'string') return err;
  if (typeof err.detail === 'string') return err.detail;
  return err.message || (err.detail != null ? String(err.detail) : String(err));
}
