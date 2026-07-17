import { KNOWN_ERROR_CODES, formatErrorCode } from './formatError.js';
import { t } from './i18n.js';

const TITLE_MAX = 100;

/**
 * Extract a known ErrorCode token from free-form error text.
 * @param {string|null|undefined} text
 * @returns {string|null}
 */
export function extractErrorCode(text) {
  const raw = String(text || '');
  if (!raw) return null;

  const structured = raw.match(
    /(?:error_code|errorCode)\s*[=:]\s*["']?([a-z][a-z0-9_]*)/i,
  );
  if (structured?.[1] && KNOWN_ERROR_CODES.includes(structured[1])) {
    return structured[1];
  }

  // Longer codes first so "unknown" does not win over more specific tokens incorrectly
  // when both appear; still require boundary-ish match.
  const ordered = [...KNOWN_ERROR_CODES].sort((a, b) => b.length - a.length);
  for (const code of ordered) {
    const re = new RegExp(`(?:^|[^a-z0-9_])${code}(?:[^a-z0-9_]|$)`, 'i');
    if (re.test(raw)) return code;
  }
  return null;
}

function truncate(text, max = TITLE_MAX) {
  const s = String(text || '').replace(/\s+/g, ' ').trim();
  if (!s) return '';
  if (s.length <= max) return s;
  return `${s.slice(0, max - 1)}…`;
}

/**
 * @param {{
 *   status?: string,
 *   error?: string|null,
 *   error_code?: string|null,
 *   error_title?: string|null,
 *   error_suggestion?: string|null,
 * }} task
 * @returns {null | { title: string, suggestion: string|null, raw: string|null, known: boolean, code: string|null }}
 */
export function summarizeTaskFailure(task) {
  if (!task || typeof task !== 'object') return null;
  const status = String(task.status || '').toLowerCase();
  const rawError = task.error != null ? String(task.error) : '';
  const hasError = Boolean(rawError.trim());
  const backendCode = String(task.error_code || '').trim();
  const backendTitle = String(task.error_title || '').trim();
  const backendSuggestion = task.error_suggestion != null
    ? String(task.error_suggestion).trim()
    : '';

  if (status === 'failed') {
    // always summarize
  } else if (status === 'cancelled' && (hasError || backendCode)) {
    // cancelled-with-error
  } else {
    return null;
  }

  // Prefer backend structured fields (API/WS contract)
  if (backendCode || backendTitle) {
    const fromCode = backendCode ? formatErrorCode(backendCode) : null;
    const known = Boolean(fromCode?.known) || Boolean(backendTitle);
    return {
      title: backendTitle || (fromCode?.known ? fromCode.title : null) || t('tasks.failure.unknown'),
      suggestion: backendSuggestion || fromCode?.suggestion || null,
      raw: hasError ? rawError : null,
      known,
      code: backendCode || null,
    };
  }

  if (!hasError) {
    return {
      title: t('tasks.failure.unknown'),
      suggestion: t('error.suggestion.unknown'),
      raw: null,
      known: false,
      code: null,
    };
  }

  // Fallback: heuristic extract from free-text error (legacy tasks)
  const code = extractErrorCode(rawError);
  if (code) {
    const { title, suggestion, known } = formatErrorCode(code);
    return {
      title: known ? title : truncate(rawError),
      suggestion: known ? suggestion : null,
      raw: rawError,
      known,
      code,
    };
  }

  return {
    title: truncate(rawError),
    suggestion: t('error.suggestion.unknown'),
    raw: rawError,
    known: false,
    code: null,
  };
}

/**
 * Inline HTML for table status cell (badge already rendered separately).
 * @param {ReturnType<typeof summarizeTaskFailure>} summary
 * @param {(s: string) => string} escapeHtml
 */
export function renderFailureLinesHtml(summary, escapeHtml) {
  if (!summary) return '';
  const esc = typeof escapeHtml === 'function' ? escapeHtml : (s) => String(s);
  const title = esc(summary.title);
  const full = esc(summary.raw || summary.title);
  const sug = summary.suggestion
    ? `<div class="task-failure-suggestion text-[10px] text-muted truncate" title="${esc(summary.suggestion)}">${esc(summary.suggestion)}</div>`
    : '';
  return `<div class="task-failure-line mt-1 max-w-[220px]">
    <div class="text-[11px] text-rose-400/90 truncate leading-snug" title="${full}">${title}</div>
    ${sug}
  </div>`;
}

/**
 * Detail panel failure diagnosis block.
 * @param {ReturnType<typeof summarizeTaskFailure>} summary
 * @param {(s: string) => string} escapeHtml
 */
export function renderFailureDetailHtml(summary, escapeHtml) {
  if (!summary) return '';
  const esc = typeof escapeHtml === 'function' ? escapeHtml : (s) => String(s);
  const suggestion = summary.suggestion
    ? `<div class="mt-2">
        <div class="text-[10px] uppercase tracking-widest text-muted font-bold">${esc(t('tasks.failure.suggestion'))}</div>
        <div class="text-sm text-amber-200/90 mt-0.5">${esc(summary.suggestion)}</div>
      </div>`
    : '';
  const raw = summary.raw
    ? `<div class="mt-2">
        <div class="text-[10px] uppercase tracking-widest text-muted font-bold">${esc(t('tasks.failure.raw'))}</div>
        <pre class="task-failure-raw mt-1 text-[11px] text-zinc-400 font-mono whitespace-pre-wrap break-words max-h-40 overflow-y-auto rounded-lg bg-black/20 border border-white/5 p-2">${esc(summary.raw)}</pre>
      </div>`
    : '';
  return `<div class="task-failure-panel rounded-lg border border-rose-500/25 bg-rose-950/20 p-3 mt-1">
    <div class="text-[10px] uppercase tracking-widest text-rose-300/80 font-bold">${esc(t('tasks.failure.section'))}</div>
    <div class="text-sm text-rose-200 font-medium mt-1">${esc(summary.title)}</div>
    ${suggestion}
    ${raw}
  </div>`;
}

/**
 * @param {object|null|undefined} health
 * @param {object|null|undefined} diagnostics
 * @returns {Array<{ id: string, name: string, status: string, message: string, severity: 'error'|'warning' }>}
 */
export function collectHealthAttentionItems(health, diagnostics) {
  const items = [];
  const seen = new Set();

  const push = (id, name, status, message, severity) => {
    const key = `${id}|${status}|${message}`;
    if (seen.has(key)) return;
    seen.add(key);
    items.push({ id, name, status, message, severity });
  };

  const overall = String(diagnostics?.status || health?.status || '').toLowerCase();
  if (overall && overall !== 'ok' && overall !== 'healthy' && overall !== 'up') {
    push(
      'overall',
      t('dashboard.attention.overallHealth'),
      overall,
      t('dashboard.attention.overallHealthMsg', { status: overall }),
      overall === 'warning' || overall === 'degraded' ? 'warning' : 'error',
    );
  }

  const checks = [
    ...(Array.isArray(diagnostics?.checks) ? diagnostics.checks : []),
    ...(Array.isArray(health?.checks) ? health.checks : []),
  ];

  for (const check of checks) {
    const st = String(check?.status || '').toLowerCase();
    if (!st || st === 'ok' || st === 'pass' || st === 'passed' || st === 'skipped') continue;
    const severity = st === 'warning' || st === 'warn' ? 'warning' : 'error';
    const name = check?.name || check?.id || check?.check_id || 'check';
    const message = check?.message || check?.error || st;
    push(String(check?.id || name), String(name), st, String(message), severity);
  }

  return items;
}
