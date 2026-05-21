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
      list.innerHTML = `<p class="text-zinc-600 text-sm px-4">${t('system.empty.checks')}</p>`;
      return;
    }
    list.innerHTML = checks.map((check) => {
      let actionBtn = "";
      if (check.details && check.details.action === "open_steamdb_browser") {
        actionBtn = `<div class="mt-2"><button class="px-3 py-1 bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold rounded shadow transition-colors" onclick="window._launchSteamDBBrowser()">一键启动浏览器</button></div>`;
      }
      const details = check.details && Object.keys(check.details).length
        ? `<pre class="system-check-details terminal-console mt-3 p-3 bg-zinc-950 border border-white/5 rounded-lg text-[12px] shadow-[inset_0_0_15px_rgba(0,0,0,0.5)]">${escapeHtml(JSON.stringify(check.details, null, 2))}</pre>`
        : '';
      const statusIcon = check.status === 'ok' ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.8)]' : 
                         check.status === 'error' ? 'bg-rose-500 shadow-[0_0_8px_rgba(244,63,94,0.8)]' : 'bg-amber-500 shadow-[0_0_8px_rgba(245,158,11,0.8)]';

      return `<div class="system-check-row group p-4 rounded-xl border border-transparent transition-all duration-300 hover:bg-white/5 hover:border-white/5 mb-1">
        <div class="flex items-start gap-4">
          <div class="mt-1.5 w-2 h-2 rounded-full ${statusIcon} shrink-0"></div>
          <div class="flex-1 min-w-0">
            <div class="flex items-center justify-between mb-1">
              <span class="text-sm font-bold text-zinc-100 tracking-tight">${escapeHtml(check.name)}</span>
              <span class="text-[10px] font-mono uppercase tracking-widest ${check.status === 'ok' ? 'text-emerald-400' : 'text-rose-400'}">${escapeHtml(check.status)}</span>
            </div>
            <div class="text-xs text-zinc-500 leading-relaxed">${escapeHtml(check.message)}</div>
            ${actionBtn}
            ${details}
          </div>
        </div>
      </div>`;
    }).join('');
  },

  _renderPaths(paths) {
    const list = this.container.querySelector('#system-paths-list');
    if (!list) return;
    const entries = Object.entries(paths);
    if (!entries.length) {
      list.innerHTML = `<p class="text-zinc-600 text-sm px-4">${t('system.empty.paths')}</p>`;
      return;
    }
    list.innerHTML = `<div class="space-y-3 px-2">` + entries.map(([key, value]) =>
      `<div class="flex flex-col gap-1.5">
        <span class="text-[10px] font-bold tracking-widest text-zinc-500 uppercase ml-1">${escapeHtml(key)}</span>
        <code class="block p-3 bg-zinc-800 border border-white/5 rounded-lg text-[13px] text-zinc-300 font-mono break-all shadow-[inset_0_0_10px_rgba(0,0,0,0.3)]">${escapeHtml(value)}</code>
      </div>`
    ).join('') + `</div>`;
  },
};

let _spaSteamdbLaunching = false;
window._launchSteamDBBrowser = async function() {
    if (_spaSteamdbLaunching) return;
    _spaSteamdbLaunching = true;
    try {
        await api("/diagnostics/steamdb/launch", { method: "POST" });
        setTimeout(() => {
            window.loadSystemDiagnostics();
            _spaSteamdbLaunching = false;
        }, 5000);
    } catch (err) {
        console.error("启动浏览器失败:", err);
        _spaSteamdbLaunching = false;
    }
};

window.loadSystemDiagnostics = function (options) {
  if (window._systemPage) window._systemPage.refresh(!(options && !options.silent));
};
