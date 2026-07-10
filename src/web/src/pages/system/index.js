import { api, escapeHtml, formatTime, toast } from '../../core/api.js';
import { formatErrorCode } from '../../core/formatError.js';
import { t } from '../../core/i18n.js';

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

/** Map error_code/code via formatErrorCode for probe/check rows. */
function formatItemError(item) {
  const rawCode = String(item?.error_code || item?.code || '').trim();
  const rawMessage = String(item?.message || '');
  if (!rawCode) {
    return { message: rawMessage, codeHtml: '', suggestionHtml: '' };
  }
  const { title, suggestion, known } = formatErrorCode(rawCode);
  const message = known ? title : (rawMessage || title);
  const codeHtml = `<div class="text-[10px] text-muted mt-1">code: ${escapeHtml(rawCode)}</div>`;
  const suggestionHtml = known && suggestion
    ? `<div class="text-[11px] text-muted mt-1">${escapeHtml(suggestion)}</div>`
    : '';
  return { message, codeHtml, suggestionHtml };
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

  async refresh(silent = false) {
    try {
      const [health, diagnostics, workers, sessionDiagnostics, sessionInventory] = await Promise.all([
        api('/health'),
        api('/diagnostics/config'),
        api('/workers?stale_after_seconds=120').catch(() => []),
        api('/diagnostics/sessions').catch(() => ({ collectors: [], summary: {} })),
        api('/diagnostics/sessions-inventory?sync=true').catch(() => ({ items: [], summary: {} })),
      ]);
      this._renderStatus(health, diagnostics);
      this._renderChecks(diagnostics.checks || []);
      this._renderPaths(diagnostics.paths || {});
      this._renderWorkers(workers || []);
      this._renderSessions(sessionDiagnostics || {});
      this._renderSessionInventory(sessionInventory || {});
      window._runDeepProbes = () => this._runDeepProbes();
    } catch (err) {
      if (!silent) {
        const list = this.container.querySelector('#system-checks-list');
        if (list) list.innerHTML = `<p class="text-muted">${escapeHtml(t('message.loadFailed', { error: err.message }))}</p>`;
      }
    }
  },

  async _runDeepProbes() {
    const list = this.container.querySelector('#system-probes-list');
    if (list) {
      list.innerHTML = `<p class="text-muted text-sm px-2">${escapeHtml(t('system.probes.running'))}</p>`;
    }
    try {
      const report = await api('/diagnostics/probes', { method: 'POST' });
      this._renderProbes(report || {});
      toast(t('system.probes.done', { status: report.status || 'ok' }), report.status === 'error' ? 'error' : 'success');
    } catch (err) {
      const failed = err.message || t('system.probes.failed');
      if (list) {
        list.innerHTML = `<p class="text-rose-400 text-sm px-2">${escapeHtml(failed)}</p>`;
      }
      toast(failed, 'error');
    }
  },

  _renderProbes(report) {
    const list = this.container.querySelector('#system-probes-list');
    if (!list) return;
    const probes = report.probes || [];
    const summary = report.summary || {};
    if (!probes.length) {
      list.innerHTML = `<p class="text-muted text-sm px-2">${escapeHtml(t('system.empty.probes'))}</p>`;
      return;
    }
    const summaryHtml = `<div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
      ${['total','ok','warning','error','skipped'].map((key) => `
        <div class="rounded-lg bg-theme-elevated border border-theme-subtle px-3 py-2">
          <div class="text-[10px] uppercase tracking-widest text-muted font-bold">${key}</div>
          <div class="text-lg font-bold text-theme-primary mt-1">${summary[key] ?? 0}</div>
        </div>`).join('')}
    </div>`;
    list.innerHTML = summaryHtml + probes.map((probe) => {
      const status = probe.status || 'unknown';
      const tone = status === 'ok' ? 'text-emerald-400' :
        status === 'error' ? 'text-rose-400' :
        status === 'skipped' ? 'text-muted' : 'text-amber-400';
      const { message, codeHtml, suggestionHtml } = formatItemError(probe);
      const details = probe.details && Object.keys(probe.details).length
        ? `<pre class="mt-2 text-[11px] text-muted overflow-x-auto">${escapeHtml(JSON.stringify(probe.details))}</pre>`
        : '';
      return `<div class="rounded-xl border border-theme-subtle bg-theme-elevated px-4 py-3 mb-2">
        <div class="flex items-center justify-between gap-3">
          <div class="text-sm font-bold text-theme-primary">${escapeHtml(probe.collector_id || '')} · ${escapeHtml(probe.name || '')}</div>
          <span class="text-[10px] font-mono uppercase ${tone}">${escapeHtml(status)} · ${probe.latency_ms || 0}ms</span>
        </div>
        <div class="text-xs text-muted mt-1">${escapeHtml(message)}</div>
        ${codeHtml}
        ${suggestionHtml}
        ${details}
      </div>`;
    }).join('');
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
      list.innerHTML = `<p class="text-muted text-sm px-4">${t('system.empty.checks')}</p>`;
      return;
    }
    list.innerHTML = checks.map((check) => {
      let actionBtn = "";
      if (check.details && check.details.action === "open_steamdb_browser") {
        actionBtn = `<div class="mt-2"><button class="px-3 py-1 bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold rounded shadow transition-colors" onclick="window._launchSteamDBBrowser()">${escapeHtml(t('system.launchBrowser'))}</button></div>`;
      }
      const details = check.details && Object.keys(check.details).length
        ? `<pre class="system-check-details terminal-console mt-3 p-3 bg-theme-elevated border border-theme-subtle rounded-lg text-[12px] shadow-[inset_0_0_15px_rgba(0,0,0,0.5)]">${escapeHtml(JSON.stringify(check.details, null, 2))}</pre>`
        : '';
      const statusIcon = check.status === 'ok' ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.8)]' :
        check.status === 'error' ? 'bg-rose-500 shadow-[0_0_8px_rgba(244,63,94,0.8)]' : 'bg-amber-500 shadow-[0_0_8px_rgba(245,158,11,0.8)]';
      const statusTextTone = check.status === 'ok' ? 'text-emerald-400' :
        check.status === 'error' ? 'text-rose-400' : 'text-amber-400';
      const { message, codeHtml, suggestionHtml } = formatItemError(check);

      return `<div class="system-check-row group p-4 rounded-xl border border-transparent transition-all duration-300 hover:bg-white/5 hover:border-theme-subtle mb-1">
        <div class="flex items-start gap-4">
          <div class="mt-1.5 w-2 h-2 rounded-full ${statusIcon} shrink-0"></div>
          <div class="flex-1 min-w-0">
            <div class="flex items-center justify-between mb-1">
              <span class="text-sm font-bold text-theme-primary tracking-tight">${escapeHtml(check.name)}</span>
              <span class="text-[10px] font-mono uppercase tracking-widest ${statusTextTone}">${escapeHtml(check.status)}</span>
            </div>
            <div class="text-xs text-muted leading-relaxed">${escapeHtml(message)}</div>
            ${codeHtml}
            ${suggestionHtml}
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
      list.innerHTML = `<p class="text-muted text-sm px-4">${t('system.empty.paths')}</p>`;
      return;
    }
    list.innerHTML = `<div class="space-y-3 px-2">` + entries.map(([key, value]) =>
      `<div class="flex flex-col gap-1.5">
        <span class="text-[10px] font-bold tracking-widest text-muted uppercase ml-1">${escapeHtml(key)}</span>
        <code class="block p-3 bg-zinc-800 border border-theme-subtle rounded-lg text-[13px] text-zinc-300 font-mono break-all shadow-[inset_0_0_10px_rgba(0,0,0,0.3)]">${escapeHtml(value)}</code>
      </div>`
    ).join('') + `</div>`;
  },

  _renderWorkers(workers) {
    const list = this.container.querySelector('#system-workers-list');
    if (!list) return;
    if (!workers.length) {
      list.innerHTML = `<p class="text-muted text-sm px-2">${escapeHtml(t('system.empty.workers'))}</p>`;
      return;
    }
    list.innerHTML = `<div class="grid grid-cols-1 xl:grid-cols-2 gap-4">` + workers.map((worker) => {
      const status = worker.status || 'unknown';
      const tone = status === 'online' || status === 'idle'
        ? 'text-emerald-300 bg-emerald-500/10 border-emerald-500/20'
        : status === 'busy'
          ? 'text-cyan-300 bg-cyan-500/10 border-cyan-500/20'
          : status === 'offline'
            ? 'text-rose-300 bg-rose-500/10 border-rose-500/20'
            : 'text-amber-300 bg-amber-500/10 border-amber-500/20';
      const capabilities = (worker.capabilities || []).slice(0, 8);
      const taskIds = worker.current_task_ids || [];
      const claim = this._workerClaimStatus(worker);
      const claimHtml = this._renderWorkerClaimStatus(claim);
      return `<div class="rounded-xl bg-theme-elevated border border-theme-subtle p-4">
        <div class="flex items-start justify-between gap-4">
          <div class="min-w-0">
            <div class="text-sm font-bold text-theme-primary truncate">${escapeHtml(worker.worker_id || '-')}</div>
            <div class="text-xs text-muted truncate mt-1">${escapeHtml(worker.hostname || '-')}</div>
          </div>
          <span class="shrink-0 rounded border px-2 py-1 text-[10px] font-bold uppercase ${tone}">${escapeHtml(status)}</span>
        </div>
        <div class="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">
          <div>
            <div class="text-muted uppercase font-bold tracking-widest mb-1">${escapeHtml(t('system.label.heartbeat'))}</div>
            <div class="text-zinc-300">${escapeHtml(formatTime(worker.last_heartbeat_at))}</div>
          </div>
          <div>
            <div class="text-muted uppercase font-bold tracking-widest mb-1">${escapeHtml(t('system.label.tasks'))}</div>
            <div class="text-zinc-300">${taskIds.length ? taskIds.map(id => `<code class="mr-1">${escapeHtml(id)}</code>`).join('') : '-'}</div>
          </div>
        </div>
        ${claimHtml}
        <div class="mt-4 flex flex-wrap gap-1.5">
          ${capabilities.length ? capabilities.map(capability => `<span class="rounded bg-white/5 border border-theme-subtle px-2 py-1 text-[11px] text-zinc-400">${escapeHtml(capability)}</span>`).join('') : `<span class="text-xs text-muted">${escapeHtml(t('system.noCapabilities'))}</span>`}
        </div>
      </div>`;
    }).join('') + `</div>`;
  },

  _renderSessions(payload) {
    const list = this.container.querySelector('#system-session-list');
    if (!list) return;
    const collectors = payload.collectors || [];
    const summary = payload.summary || {};
    if (!collectors.length) {
      list.innerHTML = `<p class="text-muted text-sm px-2">${escapeHtml(t('system.empty.sessions'))}</p>`;
      return;
    }

    const summaryHtml = `<div class="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
      ${this._renderSessionStat(t('system.stat.collectors'), summary.collectors ?? collectors.length)}
      ${this._renderSessionStat(t('system.stat.required'), summary.requires_session ?? 0)}
      ${this._renderSessionStat(t('system.stat.warnings'), summary.warnings ?? 0)}
      ${this._renderSessionStat(t('system.stat.errors'), summary.errors ?? 0)}
    </div>`;

    list.innerHTML = summaryHtml + `<div class="grid grid-cols-1 xl:grid-cols-2 gap-4">` + collectors.map((collector) => {
      const status = collector.status || 'unknown';
      const tone = status === 'ok'
        ? 'text-emerald-300 bg-emerald-500/10 border-emerald-500/20'
        : status === 'error'
          ? 'text-rose-300 bg-rose-500/10 border-rose-500/20'
          : 'text-amber-300 bg-amber-500/10 border-amber-500/20';
      const state = collector.session_state || {};
      const requiredCapabilities = collector.required_worker_capabilities || [];
      const checks = collector.checks || [];
      const stateLabel = this._sessionStateLabel(state, collector.session_mode);
      return `<div class="rounded-xl bg-theme-elevated border border-theme-subtle p-4">
        <div class="flex items-start justify-between gap-4">
          <div class="min-w-0">
            <div class="text-sm font-bold text-theme-primary truncate">${escapeHtml(collector.display_name || collector.collector_id || '-')}</div>
            <div class="text-xs text-muted truncate mt-1">${escapeHtml(collector.collector_id || '-')}</div>
          </div>
          <span class="shrink-0 rounded border px-2 py-1 text-[10px] font-bold uppercase ${tone}">${escapeHtml(status)}</span>
        </div>
        <div class="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">
          <div>
            <div class="text-muted uppercase font-bold tracking-widest mb-1">${escapeHtml(t('system.label.session'))}</div>
            <div class="text-zinc-300">${escapeHtml(collector.session_mode || '-')} / ${escapeHtml(collector.worker_binding || '-')}</div>
          </div>
          <div>
            <div class="text-muted uppercase font-bold tracking-widest mb-1">${escapeHtml(t('system.label.state'))}</div>
            <div class="text-zinc-300">${escapeHtml(stateLabel)}${state.cdp_status && state.cdp_status !== 'not_configured' ? ` / ${escapeHtml(state.cdp_status)}` : ''}</div>
          </div>
        </div>
        <div class="mt-4 flex flex-wrap gap-1.5">
          ${requiredCapabilities.length
            ? requiredCapabilities.map(capability => `<span class="rounded bg-white/5 border border-theme-subtle px-2 py-1 text-[11px] text-zinc-400">${escapeHtml(capability)}</span>`).join('')
            : `<span class="text-xs text-muted">${escapeHtml(t('system.noExtraWorkerCapability'))}</span>`}
        </div>
        ${checks.length ? `<div class="mt-4 space-y-2">${checks.map(check => this._renderSessionCheck(check)).join('')}</div>` : ''}
      </div>`;
    }).join('') + `</div>`;
  },

  _renderSessionInventory(payload) {
    const list = this.container.querySelector('#system-session-inventory-list');
    if (!list) return;
    const items = payload.items || [];
    const summary = payload.summary || {};
    if (!items.length) {
      list.innerHTML = `<p class="text-muted text-sm px-2">${escapeHtml(t('system.empty.inventory'))}</p>`;
      return;
    }

    const summaryHtml = `<div class="grid grid-cols-2 md:grid-cols-7 gap-3 mb-4">
      ${this._renderSessionStat(t('system.stat.items'), summary.items ?? items.length)}
      ${this._renderSessionStat(t('system.stat.collectors'), summary.collectors ?? 0)}
      ${this._renderSessionStat(t('system.stat.ready'), summary.ready ?? 0)}
      ${this._renderSessionStat(t('system.stat.claimed'), summary.claimed ?? 0)}
      ${this._renderSessionStat(t('system.stat.stale'), summary.stale ?? 0)}
      ${this._renderSessionStat(t('system.stat.warnings'), summary.warnings ?? 0)}
      ${this._renderSessionStat(t('system.stat.errors'), summary.errors ?? 0)}
    </div>`;

    const observedHint = summary.latest_observed_at
      ? `<div class="mb-4 text-xs text-muted">${escapeHtml(t('system.latestObserved', { time: formatTime(summary.latest_observed_at) }))}</div>`
      : '';

    const leaseHint = this._renderLeaseSummary(summary.lease_statuses || {});

    list.innerHTML = summaryHtml + observedHint + leaseHint + `<div class="grid grid-cols-1 xl:grid-cols-2 gap-4">` + items.map((item) => {
      const tone = this._statusTone(item.diagnostics_status || item.health || 'unknown');
      const stateLabel = this._sessionInventoryStateLabel(item);
      const capabilityBadges = item.required_worker_capabilities || [];
      const modeSource = item.session_mode_source || 'metadata';
      const overrideStatus = item.session_mode_override_status || 'default';
      const locator = item.locator || '-';
      const locatorLabel = item.locator_label || 'locator';
      const leaseTone = this._leaseTone(item.lease_status);
      const leaseStatus = item.lease_status || 'unbound';
      const leaseWorker = item.lease_worker_id || item.last_worker_id || '-';
      const leaseTask = item.lease_task_id || item.last_task_id || '-';
      const leaseTime = item.lease_status === 'claimed'
        ? item.lease_acquired_at
        : item.lease_released_at;
      const leaseTimeLabel = item.lease_status === 'claimed' ? t('system.lease.acquired') : t('system.lease.released');
      const staleBadge = item.is_stale
        ? '<span class="shrink-0 rounded border px-2 py-1 text-[10px] font-bold uppercase text-amber-300 bg-amber-500/10 border-amber-500/20">stale</span>'
        : '';

      return `<div class="rounded-xl bg-theme-elevated border border-theme-subtle p-4">
        <div class="flex items-start justify-between gap-4">
          <div class="min-w-0">
            <div class="text-sm font-bold text-theme-primary truncate">${escapeHtml(item.display_name || item.collector_id || '-')}</div>
            <div class="text-xs text-muted truncate mt-1">${escapeHtml(item.collector_id || '-')} / ${escapeHtml(item.session_id || '-')}</div>
          </div>
          <div class="flex shrink-0 items-center gap-2">
            ${staleBadge}
            <span class="rounded border px-2 py-1 text-[10px] font-bold uppercase ${tone}">${escapeHtml(item.diagnostics_status || item.health || 'unknown')}</span>
          </div>
        </div>
        <div class="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">
          <div>
            <div class="text-muted uppercase font-bold tracking-widest mb-1">${escapeHtml(t('system.label.session'))}</div>
            <div class="text-zinc-300">${escapeHtml(item.session_mode || '-')} / ${escapeHtml(item.worker_binding || '-')}</div>
            <div class="text-muted mt-1">${escapeHtml(modeSource)} / ${escapeHtml(overrideStatus)}</div>
          </div>
          <div>
            <div class="text-muted uppercase font-bold tracking-widest mb-1">${escapeHtml(t('system.label.observed'))}</div>
            <div class="text-zinc-300">${escapeHtml(formatTime(item.observed_at))}</div>
            <div class="text-muted mt-1">${escapeHtml(stateLabel)}</div>
          </div>
          <div>
            <div class="text-muted uppercase font-bold tracking-widest mb-1">${escapeHtml(t('system.label.account'))}</div>
            <div class="text-zinc-300">${escapeHtml(item.account_kind || '-')}</div>
            <div class="text-muted mt-1 break-all">${escapeHtml(item.account_id || '-')}</div>
          </div>
          <div>
            <div class="text-muted uppercase font-bold tracking-widest mb-1">${escapeHtml(t('system.label.lease'))}</div>
            <div class="flex items-center gap-2 text-zinc-300">
              <span class="rounded border px-2 py-1 text-[10px] font-bold uppercase ${leaseTone}">${escapeHtml(leaseStatus)}</span>
            </div>
            <div class="text-muted mt-1 break-all">${escapeHtml(t('system.label.worker'))}: ${escapeHtml(leaseWorker)}</div>
            <div class="text-muted mt-1 break-all">${escapeHtml(t('system.label.task'))}: ${escapeHtml(leaseTask)}</div>
            <div class="text-muted mt-1">${escapeHtml(leaseTimeLabel)}: ${escapeHtml(formatTime(leaseTime))}</div>
          </div>
          <div>
            <div class="text-muted uppercase font-bold tracking-widest mb-1">${escapeHtml(locatorLabel)}</div>
            <div class="text-zinc-300 break-all">${escapeHtml(locator)}</div>
          </div>
        </div>
        <div class="mt-4 flex flex-wrap gap-1.5">
          ${capabilityBadges.length
            ? capabilityBadges.map((capability) => `<span class="rounded bg-white/5 border border-theme-subtle px-2 py-1 text-[11px] text-zinc-400">${escapeHtml(capability)}</span>`).join('')
            : `<span class="text-xs text-muted">${escapeHtml(t('system.noExtraWorkerCapability'))}</span>`}
        </div>
      </div>`;
    }).join('') + `</div>`;
  },

  _renderSessionCheck(check) {
    if (!check || typeof check !== 'object') {
      return '';
    }

    const status = String(check.status || 'unknown');
    const name = String(check.name || 'session');
    const { message, codeHtml, suggestionHtml } = formatItemError(check);
    const displayMessage = message || '-';
    const tone = this._statusTone(status);
    const hasDetails = check.details !== undefined
      && check.details !== null
      && (typeof check.details !== 'object' || Object.keys(check.details).length > 0);
    const details = hasDetails
      ? `<pre class="mt-2 rounded-lg border border-theme-subtle bg-theme-elevated p-3 text-[11px] text-zinc-400 shadow-[inset_0_0_12px_rgba(0,0,0,0.35)]">${escapeHtml(JSON.stringify(check.details, null, 2))}</pre>`
      : '';

    return `<div class="rounded-lg border border-theme-subtle bg-white/[0.03] p-3 text-xs">
      <div class="flex items-center justify-between gap-3">
        <div class="min-w-0">
          <div class="text-[11px] font-bold tracking-widest text-zinc-300 uppercase">${escapeHtml(name)}</div>
          <div class="mt-1 text-zinc-400 leading-relaxed">${escapeHtml(displayMessage)}</div>
          ${codeHtml}
          ${suggestionHtml}
        </div>
        <span class="shrink-0 rounded border px-2 py-1 text-[10px] font-bold uppercase ${tone}">${escapeHtml(status)}</span>
      </div>
      ${details}
    </div>`;
  },

  _renderSessionStat(label, value) {
    return `<div class="rounded-lg bg-theme-elevated border border-theme-subtle px-3 py-3">
      <div class="text-[10px] uppercase tracking-widest text-muted font-bold">${escapeHtml(label)}</div>
      <div class="mt-1 text-lg font-bold text-theme-primary">${escapeHtml(String(value ?? '-'))}</div>
    </div>`;
  },

  _statusTone(status) {
    if (status === 'ok' || status === 'ready') {
      return 'text-emerald-300 bg-emerald-500/10 border-emerald-500/20';
    }
    if (status === 'error') {
      return 'text-rose-300 bg-rose-500/10 border-rose-500/20';
    }
    return 'text-amber-300 bg-amber-500/10 border-amber-500/20';
  },

  _workerClaimStatus(worker) {
    const metadata = worker.metadata || {};
    const claim = metadata.worker_claim || {};
    if (!claim || typeof claim !== 'object') {
      return null;
    }
    return {
      status: claim.status || 'unknown',
      reason: claim.reason || '',
      taskId: claim.task_id || '',
      blockedSessions: Array.isArray(claim.blocked_sessions) ? claim.blocked_sessions : [],
    };
  },

  _renderWorkerClaimStatus(claim) {
    if (!claim) {
      return '';
    }
    const tone = this._claimTone(claim.status);
    const reason = claim.reason
      ? escapeHtml(t('system.claim.reason', { reason: claim.reason }))
      : escapeHtml(t('system.claim.ready'));
    const taskHint = claim.taskId
      ? `<div class="text-muted mt-1 break-all">${escapeHtml(t('system.label.task'))}: ${escapeHtml(claim.taskId)}</div>`
      : '';
    const blocked = claim.blockedSessions.slice(0, 2);
    const blockedHtml = blocked.length
      ? `<div class="mt-2 space-y-1">` + blocked.map((item) => {
        const collector = item.collector_id || '-';
        const owner = item.lease_worker_id || '-';
        const task = item.lease_task_id || '-';
        return `<div class="rounded border border-theme-subtle bg-white/[0.03] px-2 py-1 text-[11px] text-zinc-400">
          <span class="text-zinc-300">${escapeHtml(collector)}</span>
          <span class="text-muted"> ${escapeHtml(t('system.label.owner'))} </span>${escapeHtml(owner)}
          <span class="text-muted"> ${escapeHtml(t('system.label.task'))} </span>${escapeHtml(task)}
        </div>`;
      }).join('') + `</div>`
      : '';
    return `<div class="mt-4 rounded-lg border border-theme-subtle bg-white/[0.02] p-3 text-xs">
      <div class="flex items-center justify-between gap-3">
        <div class="text-muted uppercase font-bold tracking-widest">${escapeHtml(t('system.label.claim'))}</div>
        <span class="rounded border px-2 py-1 text-[10px] font-bold uppercase ${tone}">${escapeHtml(claim.status || 'unknown')}</span>
      </div>
      <div class="mt-2 text-zinc-400">${reason}</div>
      ${taskHint}
      ${blockedHtml}
    </div>`;
  },

  _claimTone(status) {
    if (status === 'claimed') {
      return 'text-cyan-300 bg-cyan-500/10 border-cyan-500/20';
    }
    if (status === 'blocked') {
      return 'text-amber-300 bg-amber-500/10 border-amber-500/20';
    }
    if (status === 'no_task') {
      return 'text-zinc-300 bg-white/5 border-theme-strong';
    }
    if (status === 'invalid_response') {
      return 'text-rose-300 bg-rose-500/10 border-rose-500/20';
    }
    return 'text-zinc-300 bg-white/5 border-theme-strong';
  },

  _leaseTone(status) {
    if (status === 'claimed') {
      return 'text-cyan-300 bg-cyan-500/10 border-cyan-500/20';
    }
    if (status === 'released') {
      return 'text-emerald-300 bg-emerald-500/10 border-emerald-500/20';
    }
    if (status === 'interrupted') {
      return 'text-rose-300 bg-rose-500/10 border-rose-500/20';
    }
    if (status === 'stale') {
      return 'text-amber-300 bg-amber-500/10 border-amber-500/20';
    }
    return 'text-zinc-300 bg-white/5 border-theme-strong';
  },

  _renderLeaseSummary(leaseStatuses) {
    const entries = Object.entries(leaseStatuses || {}).filter(([, count]) => Number(count) > 0);
    if (!entries.length) {
      return '';
    }
    return `<div class="mb-4 flex flex-wrap gap-2">` + entries.map(([status, count]) => (
      `<span class="rounded border px-2.5 py-1 text-[11px] font-bold uppercase ${this._leaseTone(status)}">${escapeHtml(status)} ${escapeHtml(String(count))}</span>`
    )).join('') + `</div>`;
  },

  _sessionStateLabel(state, sessionMode) {
    if (sessionMode === 'managed_state') {
      return state.storage_state_ready ? 'storage_state_ready' : 'storage_state_missing';
    }
    if (sessionMode === 'local_profile') {
      return state.local_profile_ready ? 'profile_ready' : 'profile_missing';
    }
    return state.health || 'ready';
  },

  _sessionInventoryStateLabel(item) {
    const state = item.session_state || {};
    return this._sessionStateLabel(state, item.session_mode) || item.health || 'unknown';
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

window.loadSystemDiagnostics = function (options = {}) {
  if (window._systemPage) {
    window._systemPage.refresh(Boolean(options.silent));
  }
};

let _reconcileWorkersRunning = false;
window._reconcileStaleWorkerTasks = async function() {
  if (_reconcileWorkersRunning) return;
  _reconcileWorkersRunning = true;
  try {
    const result = await api('/workers/reconcile-stale-tasks', { method: 'POST' });
    const interrupted = result.interrupted_tasks?.length || 0;
    const recoveredRetry = result.recovered_retry_tasks?.length || 0;
    const updatedWorkers = result.updated_worker_ids?.length || 0;
    const totalRecovered = interrupted + recoveredRetry;
    toast(
      t('system.reconcile.ok', {
        total: totalRecovered,
        interrupted,
        retrying: recoveredRetry,
        workers: updatedWorkers,
      }),
      totalRecovered || updatedWorkers ? 'warning' : 'success'
    );
    window.loadSystemDiagnostics();
  } catch (err) {
    toast(t('system.reconcile.failed', { error: err.message }), 'error');
  } finally {
    _reconcileWorkersRunning = false;
  }
};
