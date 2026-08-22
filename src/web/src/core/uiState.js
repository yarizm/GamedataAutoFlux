/**
 * Shared empty / loading / error UI fragments for list pages.
 * Keep markup minimal and theme-token friendly.
 */

/**
 * @param {object} opts
 * @param {string} opts.title
 * @param {string} [opts.hint]
 * @param {string} [opts.actionHtml] raw HTML for action buttons (already escaped labels)
 * @param {'default'|'compact'|'table'} [opts.variant]
 * @param {number} [opts.colspan] for table variant
 * @param {(s: string) => string} [opts.escapeHtml]
 */
export function renderEmptyState(opts = {}) {
  const escapeHtml = opts.escapeHtml || ((s) => String(s ?? ''));
  const title = escapeHtml(opts.title || '');
  const hint = opts.hint ? `<p class="ui-empty-hint">${escapeHtml(opts.hint)}</p>` : '';
  const action = opts.actionHtml ? `<div class="ui-empty-actions">${opts.actionHtml}</div>` : '';
  const variant = opts.variant || 'default';

  const body = `
    <div class="ui-empty ui-empty--${variant}">
      <div class="ui-empty-icon" aria-hidden="true">
        <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 7h16M4 12h10M4 17h7" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M16 14l4 4m0-4l-4 4" opacity="0.35" />
        </svg>
      </div>
      <div class="ui-empty-title">${title}</div>
      ${hint}
      ${action}
    </div>`;

  if (variant === 'table') {
    const cols = Number(opts.colspan) || 6;
    return `<tr class="ui-empty-row"><td colspan="${cols}">${body}</td></tr>`;
  }
  return body;
}

/**
 * @param {object} opts
 * @param {string} [opts.label]
 * @param {'default'|'compact'|'table'|'inline'} [opts.variant]
 * @param {number} [opts.colspan]
 * @param {(s: string) => string} [opts.escapeHtml]
 */
export function renderLoadingState(opts = {}) {
  const escapeHtml = opts.escapeHtml || ((s) => String(s ?? ''));
  const label = escapeHtml(opts.label || '…');
  const variant = opts.variant || 'default';
  const body = `
    <div class="ui-loading ui-loading--${variant}" role="status" aria-live="polite">
      <span class="ui-spinner" aria-hidden="true"></span>
      <span class="ui-loading-label">${label}</span>
    </div>`;
  if (variant === 'table') {
    const cols = Number(opts.colspan) || 6;
    return `<tr class="ui-loading-row"><td colspan="${cols}">${body}</td></tr>`;
  }
  return body;
}

/**
 * @param {object} opts
 * @param {string} opts.message
 * @param {string} [opts.detail]
 * @param {'default'|'compact'|'table'|'inline'} [opts.variant]
 * @param {number} [opts.colspan]
 * @param {string} [opts.actionHtml]
 * @param {(s: string) => string} [opts.escapeHtml]
 */
export function renderErrorState(opts = {}) {
  const escapeHtml = opts.escapeHtml || ((s) => String(s ?? ''));
  const message = escapeHtml(opts.message || '');
  const detail = opts.detail
    ? `<p class="ui-error-detail">${escapeHtml(opts.detail)}</p>`
    : '';
  const action = opts.actionHtml ? `<div class="ui-empty-actions">${opts.actionHtml}</div>` : '';
  const variant = opts.variant || 'default';
  const body = `
    <div class="ui-error ui-error--${variant}" role="alert">
      <div class="ui-error-title">${message}</div>
      ${detail}
      ${action}
    </div>`;
  if (variant === 'table') {
    const cols = Number(opts.colspan) || 6;
    return `<tr class="ui-error-row"><td colspan="${cols}">${body}</td></tr>`;
  }
  return body;
}

/**
 * Render a table skeleton with shimmer effect rows.
 * @param {object} [opts]
 * @param {number} [opts.cols=6]
 * @param {number} [opts.rows=5]
 * @returns {string} HTML <tr> tags with skeleton cells
 */
export function renderTableSkeleton(opts = {}) {
  const cols = Number(opts.cols || opts.colspan) || 6;
  const rows = Number(opts.rows) || 5;
  const rowList = [];
  for (let r = 0; r < rows; r++) {
    const cells = [];
    for (let c = 0; c < cols; c++) {
      const widthPct = c === 0 ? 35 : c === 1 ? 65 : c === cols - 1 ? 40 : 50;
      cells.push(`<td class="py-3 px-4"><div class="skeleton-shimmer h-4 rounded" style="width:${widthPct}%;"></div></td>`);
    }
    rowList.push(`<tr class="border-b border-theme-subtle animate-pulse">${cells.join('')}</tr>`);
  }
  return rowList.join('');
}

/**
 * Render card skeleton placeholders.
 * @param {object} [opts]
 * @param {number} [opts.count=4]
 * @returns {string} HTML markup for skeleton cards
 */
export function renderCardSkeleton(opts = {}) {
  const count = Number(opts.count) || 4;
  const cards = [];
  for (let i = 0; i < count; i++) {
    cards.push(`
      <div class="rounded-xl border border-theme-subtle bg-theme-surface p-4 flex flex-col gap-3">
        <div class="skeleton-shimmer h-4 w-1/3 rounded"></div>
        <div class="skeleton-shimmer h-8 w-2/3 rounded"></div>
        <div class="skeleton-shimmer h-3 w-1/2 rounded"></div>
      </div>`);
  }
  return cards.join('');
}

/**
 * Render detail panel skeleton.
 * @returns {string} HTML markup for detail skeleton
 */
export function renderDetailSkeleton() {
  return `
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div class="rounded-xl border border-theme-subtle bg-theme-surface p-4 flex flex-col gap-3">
        <div class="skeleton-shimmer h-5 w-1/4 rounded mb-2"></div>
        <div class="skeleton-shimmer h-4 w-full rounded"></div>
        <div class="skeleton-shimmer h-4 w-3/4 rounded"></div>
        <div class="skeleton-shimmer h-4 w-5/6 rounded"></div>
      </div>
      <div class="rounded-xl border border-theme-subtle bg-theme-surface p-4 flex flex-col gap-3">
        <div class="skeleton-shimmer h-5 w-1/4 rounded mb-2"></div>
        <div class="skeleton-shimmer h-24 w-full rounded"></div>
      </div>
    </div>`;
}

