import { api, escapeHtml } from '../../core/api.js';
import { t } from '../../core/i18n.js';

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this._unsub = store.subscribe((key) => {
      if (key === 'refresh' && store.get('activeTab') === 'system') this.refresh(true);
    });
    this.refresh();
    return this;
  },

  destroy() { if (this._unsub) this._unsub(); },

  async refresh(silent) {
    try {
      const [health, diagnostics] = await Promise.all([
        api('/health'),
        api('/diagnostics/config'),
      ]);
      this._renderStatus(health, diagnostics);
      this._renderChecks(diagnostics.checks || []);
      this._renderPaths(diagnostics.paths || {});
    } catch (err) {
      if (!silent) {
        const list = this.container.querySelector('#system-checks-list');
        if (list) list.innerHTML = `<p class="text-muted">${escapeHtml(t('message.loadFailed', { error: err.message }))}</p>`;
      }
    }
  },

  _renderStatus(health, diagnostics) {
    const checks = diagnostics.checks || health.checks || [];
    const counts = checks.reduce((acc, check) => {
      acc[check.status] = (acc[check.status] || 0) + 1;
      return acc;
    }, {});
    const status = diagnostics.status || health.status || 'unknown';

    setText('system-overall-status', status.toUpperCase());
    setText('system-error-count', counts.error || 0);
    setText('system-warning-count', counts.warning || 0);
    setText('system-ok-count', counts.ok || 0);

    const statusEl = document.getElementById('system-overall-status');
    if (statusEl) statusEl.className = `stat-value system-status-${status}`;
  },

  _renderChecks(checks) {
    const list = this.container.querySelector('#system-checks-list');
    if (!list) return;
    if (!checks.length) {
      list.innerHTML = `<p class="text-muted">${t('system.empty.checks')}</p>`;
      return;
    }
    list.innerHTML = checks.map((check) => {
      const details = check.details && Object.keys(check.details).length
        ? `<pre class="system-check-details">${escapeHtml(JSON.stringify(check.details, null, 2))}</pre>`
        : '';
      return `<div class="system-check-row system-check-${escapeHtml(check.status)}">
        <div class="system-check-main">
          <span class="system-check-status">${escapeHtml(check.status)}</span>
          <div>
            <div class="system-check-name">${escapeHtml(check.name)}</div>
            <div class="system-check-message">${escapeHtml(check.message)}</div>
          </div>
        </div>
        ${details}
      </div>`;
    }).join('');
  },

  _renderPaths(paths) {
    const list = this.container.querySelector('#system-paths-list');
    if (!list) return;
    const entries = Object.entries(paths);
    if (!entries.length) {
      list.innerHTML = `<p class="text-muted">${t('system.empty.paths')}</p>`;
      return;
    }
    list.innerHTML = entries.map(([key, value]) =>
      `<div class="system-path-row"><span>${escapeHtml(key)}</span><code>${escapeHtml(value)}</code></div>`
    ).join('');
  },
};

window.loadSystemDiagnostics = function (options) {
  if (window._systemPage) window._systemPage.refresh(!(options && !options.silent));
};
