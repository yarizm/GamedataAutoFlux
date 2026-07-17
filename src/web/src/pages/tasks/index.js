import { api, toast, escapeHtml, escapeJs, formatTime } from '../../core/api.js';
import { renderBadge, renderProgress, setValue } from '../../core/api.js';
import { getLanguage, t } from '../../core/i18n.js';
import {
  formatApiError,
  formatPrecheckCategory,
  formatPrecheckIssue,
} from '../../core/formatError.js';
import {
  renderFailureDetailHtml,
  renderFailureLinesHtml,
  summarizeTaskFailure,
} from '../../core/taskFailure.js';
import {
  renderEmptyState,
  renderErrorState,
  renderLoadingState,
} from '../../core/uiState.js';
import {
  getCollectorForPipeline,
  hasStorageStep,
  loadAvailablePipelines,
  loadPipelineTemplates,
  populatePipelineSelect,
} from '../../core/pipelines.js';
import {
  buildTargets as buildTargetsShared,
  parseAdvancedTargetsJson,
  updateTargetFieldPanels,
} from '../../core/targetForm.js';

function safeArtifactDownloadUrl(value) {
  const url = String(value || '').trim();
  if (!url) return '';
  if (url.startsWith('/api/')) return url;
  if (/^https?:\/\//i.test(url)) {
    try {
      const parsed = new URL(url);
      return parsed.protocol === 'http:' || parsed.protocol === 'https:' ? parsed.href : '';
    } catch {
      return '';
    }
  }
  return '';
}

let currentWizardStep = 1;
let lastCreatedTask = null; // { id, name }

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this._submitting = false;
    this._unsub = store.subscribe((key, value) => {
      if (key === 'refresh' && value === 'tasks') this.refresh();
    });
    this.refresh();
    this._renderPathBanner();
    return this;
  },

  destroy() { if (this._unsub) this._unsub(); },

  async refresh() { await this._load(); },

  async _load() {
    const tbody = document.getElementById('tasks-body');
    if (tbody) {
      tbody.innerHTML = renderLoadingState({
        label: t('common.loading'),
        variant: 'table',
        colspan: 8,
        escapeHtml,
      });
    }
    try {
      const filter = document.getElementById('task-status-filter')?.value || '';
      const query = filter ? `?status=${encodeURIComponent(filter)}` : '';
      const tasks = await api(`/tasks${query}`);
      const ordered = [...tasks].sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
      this._renderTable(ordered);
    } catch (err) {
      toast(t('message.loadFailed', { error: err.message }), 'error');
      if (tbody) {
        tbody.innerHTML = renderErrorState({
          message: t('message.loadFailed', { error: err.message }),
          variant: 'table',
          colspan: 8,
          escapeHtml,
          actionHtml: `<button type="button" class="btn btn-ghost btn-sm" onclick="loadTasks()">${escapeHtml(t('common.refresh'))}</button>`,
        });
      }
    }
  },

  _renderTable(tasks) {
    const tbody = document.getElementById('tasks-body');
    if (!tbody) return;
    if (!tasks.length) {
      tbody.innerHTML = renderEmptyState({
        title: t('common.empty.tasks'),
        hint: t('ui.empty.tasksHint'),
        variant: 'table',
        colspan: 8,
        escapeHtml,
        actionHtml: `<button type="button" class="btn btn-primary btn-sm" onclick="showCreateTaskModal()">${escapeHtml(t('tasks.create'))}</button>`,
      });
      return;
    }
    tbody.innerHTML = tasks.map((task) => {
      const failure = summarizeTaskFailure(task);
      const failureHtml = renderFailureLinesHtml(failure, escapeHtml);
      return `
      <tr class="group">
        <td><code>${task.id}</code></td>
        <td class="max-w-[200px] truncate" title="${escapeHtml(task.name)}">${escapeHtml(task.name)}</td>
        <td class="max-w-[150px] truncate" title="${escapeHtml(task.pipeline_name || '-')}">${escapeHtml(task.pipeline_name || '-')}</td>
        <td>
          <div class="flex flex-col items-start gap-0.5">
            ${renderBadge(task.status)}
            ${failureHtml}
          </div>
        </td>
        <td>
          <div class="flex flex-col items-start gap-0.5">
            ${renderProgress(task.progress)}
            ${task.phase || task.current_step
              ? `<div class="text-[10px] text-muted truncate max-w-[140px]" title="${escapeHtml(task.current_step || task.phase || '')}">${escapeHtml(task.current_step || task.phase || '')}</div>`
              : ''}
          </div>
        </td>
        <td>${task.targets_count}</td>
        <td>${task.duration ? `${task.duration.toFixed(1)}s` : '-'}</td>
        <td>${this._renderActions(task)}</td>
      </tr>`;
    }).join('');
  },

  _renderActions(task) {
    const actions = [
      `<button class="btn btn-ghost btn-sm" onclick="viewTaskDetail('${escapeJs(task.id)}')">${t('common.details')}</button>`,
      `<button class="btn btn-ghost btn-sm" onclick="viewTaskLogs('${escapeJs(task.id)}')">${t('common.logs')}</button>`,
    ];
    if (task.status === 'running' || task.status === 'pending') {
      actions.push(`<button class="btn btn-danger btn-sm" onclick="cancelTask('${escapeJs(task.id)}')">${t('common.cancel')}</button>`);
    } else {
      actions.push(`<button class="btn btn-danger btn-sm" onclick="deleteTask('${escapeJs(task.id)}')">${t('common.delete')}</button>`);
    }
    return `<div class="action-buttons flex gap-2 opacity-30 group-hover:opacity-100 transition-opacity duration-300">${actions.join(' ')}</div>`;
  },

  // ── Create Task Wizard ──

  _showCreateModal() {
    Promise.all([loadPipelineTemplates(), populatePipelineSelect('task-pipeline')])
      .then(() => this._updateTargetFields());
    window.openModal('modal-create-task');
    currentWizardStep = 1;
    this._submitting = false;
    this._setSubmitLoading(false);
    const precheckEl = document.getElementById('task-precheck');
    if (precheckEl) {
      precheckEl.style.display = 'none';
      precheckEl.innerHTML = '';
    }
    this._updateWizardUI();
  },

  _getCollector(pipelineName) {
    return getCollectorForPipeline(pipelineName);
  },

  _updateTargetFields() {
    const pipelineName = document.getElementById('task-pipeline')?.value || '';
    const collector = this._getCollector(pipelineName);
    updateTargetFieldPanels('task', collector);

    const autoReport = document.getElementById('task-enable-report');
    if (autoReport && ['steam_full_report', 'taptap_full_report', 'steam_discussions_full_report'].includes(pipelineName)) {
      autoReport.checked = true;
    }
  },

  _buildTargets(formState) {
    return buildTargetsShared(formState);
  },

  /**
   * Build create/precheck payload from the wizard form.
   * @returns {Promise<{ payload: object, meta: object } | null>}
   */
  async _buildCreatePayload() {
    const getVal = (id) => document.getElementById(id)?.value.trim() || '';
    const getNum = (id, fallback) => document.getElementById(id)?.value || fallback;
    const getChecked = (id) => document.getElementById(id)?.checked || false;

    const name = getVal('task-name');
    const dataGroup = getVal('task-data-group');
    const pipelineName = getVal('task-pipeline');
    const cmEditor = document.querySelector('#task-targets + .CodeMirror')?.CodeMirror;
    const targetsRaw = cmEditor ? cmEditor.getValue().trim() : (document.getElementById('task-targets')?.value.trim() || '');
    const description = getVal('task-desc');
    const targetName = getVal('task-target-name');
    const steamAppId = getVal('task-app-id');
    const steamDiscussionsAppId = getVal('task-steam-discussions-app-id');

    if (!name || !pipelineName) {
      toast(t('message.taskNamePipelineRequired'), 'error');
      return null;
    }

    let collector = '';
    try {
      await loadAvailablePipelines();
      collector = this._getCollector(pipelineName);
    } catch (err) {
      toast(t('message.pipelineLoadFailed', { error: err.message }), 'error');
      return null;
    }

    let targets = this._buildTargets({
      collector, targetName,
      appId: collector === 'taptap' ? getVal('task-taptap-app-id') : collector === 'steam_discussions' ? steamDiscussionsAppId : steamAppId,
      ...(collector === 'monitor' ? { appId: getVal('task-monitor-app-id') } : {}),
      skipSteamdb: getChecked('task-skip-steamdb'),
      steamdbTimeSlice: getNum('task-steamdb-time-slice', 'monthly_peak_1y'),
      steamDiscussionsForumUrl: getVal('task-steam-discussions-forum-url'),
      steamDiscussionsStart: document.getElementById('task-steam-discussions-start')?.value || '',
      steamDiscussionsEnd: document.getElementById('task-steam-discussions-end')?.value || '',
      steamDiscussionsMaxPages: getNum('task-steam-discussions-max-pages', '50'),
      steamDiscussionsMaxTopics: getNum('task-steam-discussions-max-topics', '1000'),
      steamDiscussionsIncludeReplies: document.getElementById('task-steam-discussions-include-replies')?.checked ?? true,
      taptapUrl: getVal('task-taptap-url'),
      taptapReviewsPages: getNum('task-taptap-reviews-pages', '1'),
      taptapReviewsLimit: getNum('task-taptap-reviews-limit', '20'),
      monitorDays: getNum('task-monitor-days', '30'),
      monitorTwitchName: getVal('task-monitor-twitch-name'),
      monitorSiteurl: getVal('task-monitor-siteurl'),
      qimaiAppId: getVal('task-qimai-app-id'),
      officialSiteUrl: getVal('task-official-site-url'),
    });

    if (targetsRaw) {
      try {
        const parsed = parseAdvancedTargetsJson(targetsRaw, targetName || 'Target');
        if (parsed) targets = parsed;
      } catch {
        toast(t('message.targetsJsonInvalid'), 'error');
        return null;
      }
    }
    if (!targets.length) {
      toast(t('message.targetRequired'), 'error');
      return null;
    }

    const enableReport = getChecked('task-enable-report');
    const reportPromptRaw = getVal('task-report-prompt');
    const reportTemplate = document.getElementById('task-report-template')?.value || 'default';
    const primarySubject = targetName
      || (collector === 'taptap' ? getVal('task-taptap-app-id') : collector === 'steam_discussions' ? steamDiscussionsAppId : steamAppId)
      || (collector === 'official_site' ? getVal('task-official-site-url') : '')
      || name;
    const reportPrompt = reportPromptRaw || (getLanguage?.() === 'en-US'
      ? `Based on this collection result, summarize ${primarySubject}'s core performance, version updates, review feedback, and key events.`
      : `基于本次采集结果，总结${primarySubject}的核心表现、版本更新、评论反馈和关键事件。`);

    const config = enableReport ? {
      report: { enabled: true, prompt: reportPrompt, template: reportTemplate, data_source: collector || pipelineName, params: { use_vector: hasStorageStep(pipelineName, 'vector') } },
    } : {};
    if (dataGroup) config.data_group = { id: dataGroup, name: dataGroup };

    return {
      payload: { name, pipeline_name: pipelineName, targets, description, config },
      meta: {
        name,
        pipelineName,
        collector: collector || '-',
        targetsCount: targets.length,
        enableReport,
        primarySubject,
      },
    };
  },

  _renderSubmitSummary(meta) {
    const el = document.getElementById('task-submit-summary');
    if (!el || !meta) return;
    el.innerHTML = `
      <div class="task-submit-summary-inner">
        <div class="text-[11px] uppercase tracking-widest text-muted font-bold mb-2">${escapeHtml(t('tasks.submitSummary'))}</div>
        <div class="task-submit-summary-grid">
          <span>${escapeHtml(t('common.name'))}</span><strong>${escapeHtml(meta.name || '-')}</strong>
          <span>${escapeHtml(t('cron.pipeline'))}</span><strong>${escapeHtml(meta.pipelineName || '-')}</strong>
          <span>${escapeHtml(t('tasks.collector'))}</span><strong>${escapeHtml(meta.collector || '-')}</strong>
          <span>${escapeHtml(t('tasks.targetCount'))}</span><strong>${escapeHtml(String(meta.targetsCount ?? 0))}</strong>
          <span>${escapeHtml(t('tasks.enableReport'))}</span><strong>${meta.enableReport ? escapeHtml(t('common.ok')) : escapeHtml(t('common.none'))}</strong>
        </div>
        <p class="text-[11px] text-muted mt-2 mb-0">${escapeHtml(t('tasks.submitHint'))}</p>
      </div>`;
  },

  _renderPathBanner() {
    const el = document.getElementById('tasks-path-banner');
    if (!el) return;
    if (!lastCreatedTask?.id) {
      el.hidden = true;
      el.classList.add('hidden');
      el.innerHTML = '';
      return;
    }
    el.hidden = false;
    el.classList.remove('hidden');
    el.innerHTML = `
      <div class="tasks-path-banner-inner">
        <div class="min-w-0 flex-1">
          <div class="text-sm font-bold text-theme-primary">${escapeHtml(t('tasks.path.createdTitle'))}</div>
          <div class="text-xs text-muted truncate">${escapeHtml(lastCreatedTask.name || lastCreatedTask.id)} · <code>${escapeHtml(lastCreatedTask.id)}</code></div>
        </div>
        <div class="flex items-center gap-2 shrink-0">
          <button type="button" class="btn btn-ghost btn-sm" onclick="viewTaskDetail('${escapeJs(lastCreatedTask.id)}')">${escapeHtml(t('common.details'))}</button>
          <button type="button" class="btn btn-ghost btn-sm" onclick="viewTaskLogs('${escapeJs(lastCreatedTask.id)}')">${escapeHtml(t('common.logs'))}</button>
          <button type="button" class="btn btn-primary btn-sm" onclick="activateTab('data')">${escapeHtml(t('tasks.path.openData'))}</button>
          <button type="button" class="btn btn-ghost btn-sm" onclick="dismissTaskPathBanner()" aria-label="${escapeHtml(t('common.cancel'))}">×</button>
        </div>
      </div>`;
  },

  _dismissPathBanner() {
    lastCreatedTask = null;
    this._renderPathBanner();
  },

  // ── Wizard UI ──

  _updateWizardUI() {
    document.querySelectorAll('#task-wizard-steps .wizard-step').forEach((el) => {
      const s = parseInt(el.dataset.step, 10);
      el.classList.remove('active', 'done');
      if (s < currentWizardStep) el.classList.add('done');
      if (s === currentWizardStep) el.classList.add('active');
    });
    document.querySelectorAll('.wizard-panel').forEach((el) => {
      el.style.display = parseInt(el.dataset.panel, 10) === currentWizardStep ? '' : 'none';
    });
    const back = document.getElementById('btn-wizard-back');
    const next = document.getElementById('btn-wizard-next');
    const submit = document.getElementById('btn-submit-task');
    if (back) back.style.display = currentWizardStep > 1 ? '' : 'none';
    if (next) next.style.display = currentWizardStep < 3 ? '' : 'none';
    if (submit) submit.style.display = currentWizardStep === 3 ? '' : 'none';
    if (currentWizardStep === 3) {
      this._buildCreatePayload().then((built) => {
        if (built) this._renderSubmitSummary(built.meta);
      }).catch(() => {});
    }
  },

  async _wizardNext() {
    if (currentWizardStep === 1) {
      const name = document.getElementById('task-name')?.value.trim();
      const pipeline = document.getElementById('task-pipeline')?.value;
      if (!name) { toast(t('message.taskNameRequired'), 'error'); return; }
      if (!pipeline) { toast(t('message.selectPipeline'), 'error'); return; }
      this._updateTargetFields();
    }
    // Step 2 → 3: do not hard-block on targets (advanced JSON lives on step 3).
    // Submit/precheck still require at least one target.
    if (currentWizardStep < 3) {
      currentWizardStep += 1;
      this._updateWizardUI();
    }
  },

  _wizardPrev() {
    if (currentWizardStep > 1) {
      currentWizardStep -= 1;
      this._updateWizardUI();
    }
  },

  _setSubmitLoading(loading) {
    this._submitting = Boolean(loading);
    const submit = document.getElementById('btn-submit-task');
    const precheckBtn = document.getElementById('btn-task-precheck');
    if (submit) {
      submit.disabled = this._submitting;
      submit.textContent = this._submitting ? t('tasks.submitting') : t('common.submit');
    }
    if (precheckBtn) precheckBtn.disabled = this._submitting;
  },

  async _runPrecheckOnly() {
    const built = await this._buildCreatePayload();
    if (!built) return null;
    this._renderSubmitSummary(built.meta);
    try {
      const precheck = await api('/tasks/precheck', { method: 'POST', body: JSON.stringify(built.payload) });
      this._renderPrecheck(precheck);
      if (!precheck.can_submit) {
        toast(t('message.taskPrecheckFailed'), 'error');
      } else if (precheck.status === 'warning') {
        toast(t('tasks.precheckWarningToast'), 'info');
      } else {
        toast(t('tasks.precheckOkToast'), 'success');
      }
      return precheck;
    } catch (err) {
      toast(t('message.createFailed', { error: formatApiError(err) }), 'error');
      return null;
    }
  },

  // ── Create / Cancel / Delete ──

  async _createTask() {
    if (this._submitting) return;
    const built = await this._buildCreatePayload();
    if (!built) return;

    this._renderSubmitSummary(built.meta);
    this._setSubmitLoading(true);
    try {
      const precheck = await api('/tasks/precheck', { method: 'POST', body: JSON.stringify(built.payload) });
      this._renderPrecheck(precheck);
      if (!precheck.can_submit) {
        toast(t('message.taskPrecheckFailed'), 'error');
        return;
      }
      if (precheck.status === 'warning') {
        const warningText = (precheck.issues || []).filter((i) => i.level === 'warning').map((i) => i.message).join('\n');
        if (!confirm(t('confirm.taskWarnings', { warnings: warningText }))) return;
      }
      const created = await api('/tasks', { method: 'POST', body: JSON.stringify(built.payload) });
      const taskId = created?.id || '';
      lastCreatedTask = { id: taskId, name: created?.name || built.meta.name };
      toast(t('message.taskCreatedWithId', { id: taskId || '-' }), 'success');
      window.closeModal('modal-create-task');
      window.refreshDashboard && window.refreshDashboard();
      await this.refresh();
      this._renderPathBanner();
      if (taskId) {
        // Open detail so demo path is immediate
        await this._viewDetail(taskId);
      }
    } catch (err) {
      toast(t('message.createFailed', { error: formatApiError(err) }), 'error');
    } finally {
      this._setSubmitLoading(false);
    }
  },

  _renderPrecheck(precheck) {
    const container = document.getElementById('task-precheck');
    if (!container || !precheck) return;
    const issues = precheck.issues || [];
    const credentials = precheck.credential_status || {};
    const dataSources = precheck.data_source_status || {};
    const required = precheck.required_fields || [];
    const session = precheck.session_readiness || {};
    const collectors = precheck.collectors || (precheck.collector_name ? [precheck.collector_name] : []);
    const readiness = precheck.collectors_readiness || [];
    container.style.display = 'block';
    container.className = `task-precheck task-precheck-${precheck.status || 'ok'}`;
    container.innerHTML = `
      <div class="task-precheck-title">${t('tasks.precheck')}: ${escapeHtml(precheck.status || 'ok')}</div>
      <div class="task-precheck-grid">
        <span>${t('tasks.collector')}</span><strong>${escapeHtml(collectors.join(', ') || precheck.collector_name || '-')}</strong>
        <span>${t('tasks.required')}</span><strong>${escapeHtml(required.join(' / ') || '-')}</strong>
        <span>${t('tasks.credentials')}</span><strong>${escapeHtml(Object.entries(credentials).map(([k,v]) => `${k}: ${v}`).join(' / ') || '-')}</strong>
        <span>${t('tasks.dataSource')}</span><strong>${escapeHtml(Object.entries(dataSources).map(([k,v]) => `${k}: ${v}`).join(' / ') || '-')}</strong>
      </div>
      ${this._renderCollectorsReadiness(readiness)}
      ${this._renderPrecheckSessionReadiness(session)}
      ${this._renderPrecheckIssues(issues)}
      ${precheck.deep && precheck.probe_report ? this._renderProbeSummary(precheck.probe_report) : ''}`;
  },

  _renderPrecheckIssues(issues) {
    if (!issues.length) return '';
    const groups = {};
    for (const issue of issues) {
      const cat = issue.category || 'other';
      if (!groups[cat]) groups[cat] = [];
      groups[cat].push(issue);
    }
    const order = ['config', 'target', 'credential', 'session', 'graph', 'probe', 'runtime', 'other'];
    const cats = [...order.filter((c) => groups[c]), ...Object.keys(groups).filter((c) => !order.includes(c))];
    return cats.map((cat) => `
      <div class="mt-3">
        <div class="text-[10px] uppercase tracking-widest text-zinc-500 font-bold mb-1">${escapeHtml(formatPrecheckCategory(cat))}</div>
        <ul class="space-y-1">${groups[cat].map((i) => {
          const formatted = formatPrecheckIssue(i);
          const cid = i.collector_id ? `${escapeHtml(i.collector_id)} · ` : '';
          return `<li class="task-precheck-${escapeHtml(i.level)} text-sm">
  ${cid}<strong>${escapeHtml(formatted.title)}</strong>
  ${i.field ? ` <code class="text-[11px]">${escapeHtml(i.field)}</code>` : ''}:
  ${escapeHtml(formatted.message)}
  ${formatted.suggestion ? `<div class="text-[11px] text-zinc-500 mt-0.5">${escapeHtml(formatted.suggestion)}</div>` : ''}
</li>`;
        }).join('')}</ul>
      </div>`).join('');
  },

  _renderProbeSummary(report) {
    if (!report || !report.summary) return '';
    const s = report.summary;
    const summaryText = t('tasks.deepProbeSummary', {
      total: s.total ?? 0,
      ok: s.ok ?? 0,
      warn: s.warning ?? 0,
      err: s.error ?? 0,
      skip: s.skipped ?? 0,
    });
    return `<div class="mt-3 rounded-lg border border-theme-subtle bg-theme-elevated px-3 py-2 text-xs text-zinc-400">
      ${escapeHtml(summaryText)}
    </div>`;
  },

  _renderCollectorsReadiness(readiness) {
    if (!Array.isArray(readiness) || !readiness.length) return '';
    if (readiness.length === 1 && !readiness[0].from_upstream) return '';
    return `
      <div class="mt-3 flex flex-wrap gap-2">
        ${readiness.map((item) => {
          const tone = this._precheckTone(item.status || 'ok');
          const role = item.from_upstream ? 'upstream' : 'root';
          return `<span class="rounded border px-2 py-1 text-[10px] font-bold uppercase ${tone}" title="errors=${item.error_count || 0} warnings=${item.warning_count || 0}">
            ${escapeHtml(item.collector_id || '?')} · ${escapeHtml(role)} · ${escapeHtml(item.status || 'ok')}
          </span>`;
        }).join('')}
      </div>`;
  },

  _renderPrecheckSessionReadiness(session) {
    if (!session || !Object.keys(session).length) return '';
    const mode = session.mode || 'api_only';
    const binding = session.binding || '-';
    const status = session.status || 'unknown';
    const precheckStatus = session.precheck_status || 'ok';
    const summary = session.summary || '-';
    const locator = session.locator || '-';
    const locatorLabel = session.locator_label || 'locator';
    const accountKind = session.account_kind || '-';
    const leaseStrategy = session.lease_strategy || '-';
    const capabilities = session.required_worker_capabilities || [];
    const reasons = [
      ...(session.blocking_reasons || []),
      ...(session.attention_reasons || []),
    ];
    const tone = this._precheckTone(precheckStatus);

    return `
      <div class="mt-4 rounded-xl bg-theme-elevated border border-theme-subtle p-4">
        <div class="flex items-start justify-between gap-3">
          <div class="text-[10px] uppercase tracking-widest text-zinc-500 font-bold">${escapeHtml(t('tasks.sessionReadiness'))}</div>
          <span class="shrink-0 rounded border px-2 py-1 text-[10px] font-bold uppercase ${tone}">${escapeHtml(precheckStatus)} / ${escapeHtml(status)}</span>
        </div>
        <div class="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">
          <div>
            <div class="text-zinc-600 uppercase font-bold tracking-widest mb-1">${escapeHtml(t('tasks.sessionMode'))}</div>
            <div class="text-zinc-300">${escapeHtml(mode)} / ${escapeHtml(binding)}</div>
            <div class="text-zinc-500 mt-1">${escapeHtml(leaseStrategy)}</div>
          </div>
          <div>
            <div class="text-zinc-600 uppercase font-bold tracking-widest mb-1">${escapeHtml(t('tasks.sessionAccount'))}</div>
            <div class="text-zinc-300">${escapeHtml(accountKind)}</div>
            <div class="text-zinc-500 mt-1 break-all">${escapeHtml(locatorLabel)}: ${escapeHtml(locator)}</div>
          </div>
        </div>
        <div class="mt-3 text-sm text-zinc-300 leading-relaxed">${escapeHtml(summary)}</div>
        <div class="mt-3 flex flex-wrap gap-1.5">
          ${capabilities.length
            ? capabilities.map((capability) => `<span class="rounded bg-white/5 border border-theme-subtle px-2 py-1 text-[11px] text-zinc-400">${escapeHtml(capability)}</span>`).join('')
            : `<span class="text-xs text-zinc-600">${escapeHtml(t('tasks.noExtraWorkerCapability'))}</span>`}
        </div>
        ${reasons.length ? `<div class="mt-3 space-y-2">${reasons.map((reason) => `
          <div class="rounded-lg bg-white/5 border border-theme-subtle px-3 py-2">
            <div class="text-xs font-semibold text-zinc-200">${escapeHtml(reason.name || 'session')}</div>
            <div class="text-xs text-zinc-500 mt-1">${escapeHtml(reason.message || '')}</div>
          </div>
        `).join('')}</div>` : ''}
      </div>`;
  },

  _precheckTone(status) {
    if (status === 'ok') return 'text-emerald-300 bg-emerald-500/10 border-emerald-500/20';
    if (status === 'error') return 'text-rose-300 bg-rose-500/10 border-rose-500/20';
    return 'text-amber-300 bg-amber-500/10 border-amber-500/20';
  },

  async _cancelTask(id) {
    try { await api(`/tasks/${id}/cancel`, { method: 'POST' }); toast(t('message.taskCancelled'), 'success'); window.refreshDashboard(); this.refresh(); }
    catch (err) { toast(t('message.cancelFailed', { error: err.message }), 'error'); }
  },

  async _deleteTask(id) {
    if (!confirm(t('confirm.deleteTask', { id }))) return;
    try { await api(`/tasks/${encodeURIComponent(id)}?confirm=true`, { method: 'DELETE' }); toast(t('message.taskDeleted'), 'success'); window.refreshDashboard(); this.refresh(); }
    catch (err) { toast(t('message.deleteFailed', { error: err.message }), 'error'); }
  },

  _renderFailedTaskActions(taskId, latestCheckpoint) {
    const id = String(taskId || '');
    const checkpointSummary = latestCheckpoint ? (() => {
      const stage = latestCheckpoint.recovery_level
        || latestCheckpoint.cursor?.stage
        || latestCheckpoint.cursor?.phase
        || (latestCheckpoint.seq != null ? `#${latestCheckpoint.seq}` : '-');
      const collector = latestCheckpoint.collector_name || '-';
      const time = formatTime(latestCheckpoint.created_at);
      return `<div class="text-xs text-zinc-400 mt-1">${escapeHtml(t('tasks.checkpointSummary', {
        stage: String(stage),
        collector: String(collector),
        time: String(time),
      }))}</div>`;
    })() : '';
    return `
      <div class="task-resume-actions border-t border-theme-subtle pt-3 mt-1">
        ${checkpointSummary}
        <div class="flex gap-2 mt-3">
          <button type="button" class="btn btn-primary" data-action="resume-task" data-id="${escapeHtml(id)}" onclick="resumeTask('${escapeJs(id)}')">
            ${escapeHtml(t('tasks.resume'))}
          </button>
          <button type="button" class="btn btn-secondary" data-action="rerun-task" data-id="${escapeHtml(id)}" onclick="rerunTask('${escapeJs(id)}')">
            ${escapeHtml(t('tasks.rerun'))}
          </button>
        </div>
      </div>`;
  },

  async _resumeTask(id) {
    try {
      await api(`/tasks/${encodeURIComponent(id)}/resume`, { method: 'POST', body: JSON.stringify({}) });
      toast(t('tasks.resume.ok'), 'success');
      window.refreshDashboard && window.refreshDashboard();
      await this.refresh();
      await this._viewDetail(id);
    } catch (err) {
      toast(t('message.createFailed', { error: formatApiError(err) }), 'error');
    }
  },

  async _rerunTask(id) {
    try {
      await api(`/tasks/${encodeURIComponent(id)}/rerun`, { method: 'POST', body: JSON.stringify({}) });
      toast(t('tasks.rerun.ok'), 'success');
      window.refreshDashboard && window.refreshDashboard();
      await this.refresh();
      await this._viewDetail(id);
    } catch (err) {
      toast(t('message.createFailed', { error: formatApiError(err) }), 'error');
    }
  },

  async _viewLogs(id) {
    window.openModal('modal-task-logs');
    const modalLogs = document.getElementById('modal-task-logs');
    if (modalLogs) modalLogs.dataset.taskId = id;
    const container = document.getElementById('task-logs-content');
    if (!container) return;
    container.innerHTML = renderLoadingState({ label: t('common.loading'), escapeHtml });
    try {
      const data = await api(`/tasks/${id}/logs`);
      if (!data.logs.length) {
        container.innerHTML = renderEmptyState({
          title: t('common.empty.logs'),
          hint: t('ui.empty.logsHint'),
          variant: 'compact',
          escapeHtml,
        });
        return;
      }
      container.innerHTML = `<div class="terminal-console bg-theme-elevated border border-theme-subtle p-4 rounded-xl min-h-[300px] font-mono text-sm leading-relaxed tracking-wide">` + data.logs.map((log) => {
        const statusClass = log.status === 'success' ? 'log-success' : log.status === 'failed' ? 'log-failed' : 'log-running';
        return `<div class="terminal-line log-entry ${statusClass}">
          <span class="log-time text-zinc-600 mr-2">${log.started_at ? formatTime(log.started_at) : ''}</span>
          <span class="log-step text-violet-400 mr-2">[${escapeHtml(log.step)}]</span>
          <span class="log-message text-zinc-300">${escapeHtml(log.message || '')}</span>
          ${log.error ? `<div class="text-rose-400 mt-1 pl-6">-> ${escapeHtml(log.error)}</div>` : ''}
        </div>`;
      }).join('') + `</div>`;
    } catch (err) {
      container.innerHTML = renderErrorState({
        message: t('message.loadFailed', { error: err.message }),
        variant: 'compact',
        escapeHtml,
      });
    }
  },

  async _viewDetail(id) {
    window.openModal('modal-task-detail');
    const container = document.getElementById('task-detail-content');
    if (!container) return;
    container.innerHTML = renderLoadingState({ label: t('common.loading'), escapeHtml });
    try {
      const [task, eventsPayload, artifactsPayload, checkpointsPayload] = await Promise.all([
        api(`/tasks/${id}`),
        api(`/tasks/${id}/events?limit=8&order=desc`).catch(() => ({ events: [] })),
        api(`/tasks/${id}/artifacts?limit=8`).catch(() => ({ artifacts: [] })),
        api(`/tasks/${id}/checkpoints?limit=8`).catch(() => ({ checkpoints: [], latest: null })),
      ]);
      const targets = task.targets?.length ? `<pre class="report-output">${escapeHtml(JSON.stringify(task.targets, null, 2))}</pre>` : `<p class="text-muted">${t('common.empty.targets')}</p>`;
      const config = Object.keys(task.config || {}).length ? `<pre class="report-output">${escapeHtml(JSON.stringify(task.config, null, 2))}</pre>` : `<p class="text-muted">${t('common.empty.config')}</p>`;
      const resultSummary = task.result_summary ? `<pre class="report-output">${escapeHtml(JSON.stringify(task.result_summary, null, 2))}</pre>` : `<p class="text-muted">${t('common.empty.summary')}</p>`;
      const observability = this._renderTaskObservability({
        task,
        events: eventsPayload.events || [],
        artifacts: artifactsPayload.artifacts || [],
        checkpoints: checkpointsPayload.checkpoints || [],
        latestCheckpoint: checkpointsPayload.latest || null,
      });
      const autoReportLink = task.result_summary?.generated_report_id
        ? `<div style="margin-top:0.75rem"><button class="btn btn-primary btn-sm" onclick="viewReport('${escapeJs(task.result_summary.generated_report_id)}')">${t('tasks.generatedReport')}</button></div>` : '';
      const latestLogs = task.step_logs?.length
        ? `<div class="terminal-console bg-theme-elevated border border-theme-subtle p-3 rounded-lg max-h-48 overflow-y-auto">` + task.step_logs.slice(-8).map(log => `<div class="terminal-line log-entry ${log.status==='success'?'log-success':log.status==='failed'?'log-failed':'log-running'}">
            <span class="log-time text-zinc-600">${log.started_at ? formatTime(log.started_at) : ''}</span>
            <span class="log-step text-violet-400">[${escapeHtml(log.step)}]</span>
            <span class="log-message text-zinc-300">${escapeHtml(log.message||'')}</span>
            ${log.error?`<div class="text-rose-400 mt-1 pl-4">-> ${escapeHtml(log.error)}</div>`:''}
          </div>`).join('') + `</div>`
        : `<p class="text-muted">${t('common.empty.logs')}</p>`;
      const latestCheckpoint = checkpointsPayload.latest || null;
      const failedActionsHtml = task.status === 'failed' ? this._renderFailedTaskActions(task.id, latestCheckpoint) : '';

      container.innerHTML = `
        <div class="detail-grid grid grid-cols-1 md:grid-cols-2 gap-4">
          <div class="detail-card bg-theme-elevated border border-theme-subtle p-4 rounded-xl flex flex-col gap-3">
            <h3 class="text-theme-primary font-bold border-b border-theme-strong pb-2 mb-1">${t('tasks.basic')}</h3>
            <div class="detail-kv flex items-center justify-between text-sm"><span class="text-zinc-500">ID</span><code class="text-violet-300 bg-violet-500/10 px-1.5 py-0.5 rounded">${task.id}</code></div>
            <div class="detail-kv flex items-center justify-between text-sm"><span class="text-zinc-500">${t('common.name')}</span><span class="text-zinc-200 font-medium">${escapeHtml(task.name)}</span></div>
            <div class="detail-kv flex items-center justify-between text-sm"><span class="text-zinc-500">${t('common.status')}</span><span>${renderBadge(task.status)}</span></div>
            <div class="detail-kv flex items-center justify-between text-sm"><span class="text-zinc-500">Pipeline</span><span class="text-zinc-200">${escapeHtml(task.pipeline_name||'-')}</span></div>
            <div class="detail-kv flex items-center justify-between text-sm"><span class="text-zinc-500">${t('common.progress')}</span><span class="text-zinc-200">${Math.round(task.progress*100)}%</span></div>
            <div class="detail-kv flex items-center justify-between text-sm"><span class="text-zinc-500">Retry</span><span class="text-zinc-200">${task.retry_count}/${task.max_retries}</span></div>
            ${(() => {
              const failure = summarizeTaskFailure(task);
              if (failure) {
                return `<div class="detail-kv flex flex-col gap-1 text-sm"><span class="text-zinc-500">${escapeHtml(t('common.error'))}</span>${renderFailureDetailHtml(failure, escapeHtml)}</div>`;
              }
              return `<div class="detail-kv flex items-center justify-between text-sm"><span class="text-zinc-500">${t('common.error')}</span><span class="text-zinc-400">-</span></div>`;
            })()}
            ${failedActionsHtml}
          </div>
          <div class="detail-card bg-theme-elevated border border-theme-subtle p-4 rounded-xl flex flex-col gap-3">
            <h3 class="text-theme-primary font-bold border-b border-theme-strong pb-2 mb-1">${t('common.description')}</h3>
            <p class="text-sm text-zinc-400">${escapeHtml(task.description||t('common.none'))}</p>
            <h3 class="text-theme-primary font-bold border-b border-theme-strong pb-2 mt-2 mb-1">${t('tasks.recentLogs')}</h3>
            ${latestLogs}
          </div>
        </div>
        <div class="mt-6 flex flex-col gap-4">
          ${observability}
          <div><h3 class="text-theme-primary font-bold mb-2">${t('tasks.targets')}</h3>${targets}</div>
          <div><h3 class="text-theme-primary font-bold mb-2">${t('tasks.runtimeConfig')}</h3>${config}</div>
          <div><h3 class="text-theme-primary font-bold mb-2">${t('tasks.resultSummary')}</h3>${resultSummary}${autoReportLink}</div>
        </div>`;
    } catch (err) { container.innerHTML = `<p style="color:var(--danger)">${escapeHtml(t('message.loadFailed', { error: err.message }))}</p>`; }
  },

  _renderTaskObservability({ task, events, artifacts, checkpoints, latestCheckpoint }) {
    const metadata = task.collector_metadata || {};
    const recovery = task.recovery || {};
    const session = task.session_diagnostics || {};
    const sessionAccount = session.session_account || {};
    const sessionState = session.session_state || {};
    const sessionLease = session.session_lease || {};
    const sessionLabel = this._taskSessionStateLabel(sessionState, session.session_mode || metadata.session_mode);
    const latestEvents = (events || []).slice(0, 8);
    const latestCheckpoints = (checkpoints || []).slice(0, 4);
    return `
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="detail-card bg-theme-elevated border border-theme-subtle p-4 rounded-xl">
          <h3 class="text-theme-primary font-bold border-b border-theme-strong pb-2 mb-3">${escapeHtml(t('tasks.collectorRecovery'))}</h3>
          <div class="task-observe-grid">
            ${this._renderObserveKv('Collector', metadata.collector_id || task.collector_name || '-')}
            ${this._renderObserveKv('Session', `${metadata.session_mode || session.session_mode || '-'}${metadata.requires_session ? ' / required' : ''}`)}
            ${this._renderObserveKv('Binding', session.worker_binding || '-')}
            ${this._renderObserveKv('Checkpoint', `${recovery.recovery_level || metadata.recovery_level || 'L0'} / ${recovery.supports_checkpoint || metadata.supports_checkpoint ? 'supported' : 'not supported'}`)}
            ${this._renderObserveKv('Action', recovery.recommended_action || '-')}
          </div>
          ${recovery.guidance ? `<p class="mt-3 text-xs text-zinc-500 leading-relaxed">${escapeHtml(recovery.guidance)}</p>` : ''}
          ${(sessionAccount.account_kind || sessionState.health || sessionLease.strategy) ? `
            <div class="mt-3 rounded-lg bg-theme-elevated border border-theme-subtle p-3">
              <div class="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs">
                <div>
                  <div class="text-zinc-600 uppercase font-bold tracking-widest mb-1">Account</div>
                  <div class="text-zinc-300">${escapeHtml(sessionAccount.account_kind || '-')}</div>
                  <div class="text-zinc-500 mt-1 break-all">${escapeHtml(sessionAccount.account_id || '-')}</div>
                </div>
                <div>
                  <div class="text-zinc-600 uppercase font-bold tracking-widest mb-1">State</div>
                  <div class="text-zinc-300">${escapeHtml(sessionLabel)}</div>
                  <div class="text-zinc-500 mt-1">${escapeHtml(sessionState.cdp_status || '-')}</div>
                </div>
                <div>
                  <div class="text-zinc-600 uppercase font-bold tracking-widest mb-1">Lease</div>
                  <div class="text-zinc-300">${escapeHtml(sessionLease.strategy || '-')}</div>
                  <div class="text-zinc-500 mt-1">${escapeHtml(String(sessionLease.transferable ?? '-'))}</div>
                </div>
              </div>
            </div>
          ` : ''}
          ${session.checks?.length ? `<div class="mt-3 space-y-2">${session.checks.map(check => this._renderSessionCheck(check)).join('')}</div>` : ''}
        </div>
        <div class="detail-card bg-theme-elevated border border-theme-subtle p-4 rounded-xl">
          <h3 class="text-theme-primary font-bold border-b border-theme-strong pb-2 mb-3">${escapeHtml(t('tasks.events'))}</h3>
          ${latestEvents.length ? `<div class="space-y-2 max-h-64 overflow-y-auto pr-1">${latestEvents.map(event => this._renderTaskEvent(event)).join('')}</div>` : `<p class="text-muted">${t('common.empty.logs')}</p>`}
        </div>
      </div>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="detail-card bg-theme-elevated border border-theme-subtle p-4 rounded-xl">
          <h3 class="text-theme-primary font-bold border-b border-theme-strong pb-2 mb-3">${escapeHtml(t('tasks.artifacts'))}</h3>
          ${artifacts.length ? `<div class="space-y-2">${artifacts.map(artifact => this._renderArtifact(artifact)).join('')}</div>` : `<p class="text-muted">${escapeHtml(t('tasks.noArtifacts'))}</p>`}
        </div>
        <div class="detail-card bg-theme-elevated border border-theme-subtle p-4 rounded-xl">
          <h3 class="text-theme-primary font-bold border-b border-theme-strong pb-2 mb-3">${escapeHtml(t('tasks.checkpoints'))}</h3>
          ${latestCheckpoint ? `<div class="mb-3 p-3 rounded-lg bg-emerald-500/5 border border-emerald-500/20 text-xs text-emerald-300">Latest: #${escapeHtml(String(latestCheckpoint.seq))} ${escapeHtml(latestCheckpoint.recovery_level || 'L0')} at ${escapeHtml(formatTime(latestCheckpoint.created_at))}</div>` : ''}
          ${latestCheckpoints.length ? `<div class="space-y-2">${latestCheckpoints.map(checkpoint => this._renderCheckpoint(checkpoint)).join('')}</div>` : `<p class="text-muted">${escapeHtml(t('tasks.noCheckpoints'))}</p>`}
        </div>
      </div>`;
  },

  _renderObserveKv(label, value) {
    return `<div class="flex items-center justify-between gap-4 text-sm py-1.5 border-b border-theme-subtle last:border-b-0">
      <span class="text-zinc-500">${escapeHtml(label)}</span>
      <span class="text-zinc-200 text-right">${escapeHtml(String(value || '-'))}</span>
    </div>`;
  },

  _renderSessionCheck(check) {
    const status = check.status || 'unknown';
    const tone = status === 'ok' ? 'text-emerald-300 bg-emerald-500/10' : status === 'error' ? 'text-rose-300 bg-rose-500/10' : 'text-amber-300 bg-amber-500/10';
    return `<div class="rounded-lg bg-theme-elevated border border-theme-subtle p-3">
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0">
          <div class="text-xs font-semibold text-zinc-200 truncate">${escapeHtml(check.name || 'session')}</div>
          <div class="text-xs text-zinc-500 mt-1">${escapeHtml(check.message || '')}</div>
        </div>
        <span class="shrink-0 rounded px-2 py-0.5 text-[10px] font-bold uppercase ${tone}">${escapeHtml(status)}</span>
      </div>
    </div>`;
  },

  _renderTaskEvent(event) {
    const tone = event.level === 'error' ? 'text-rose-300 border-rose-500/20 bg-rose-500/5' : event.level === 'warning' ? 'text-amber-300 border-amber-500/20 bg-amber-500/5' : 'text-zinc-300 border-theme-subtle bg-theme-elevated';
    const status = event.payload?.status || event.payload?.task_status || '';
    return `<div class="rounded-lg border p-3 ${tone}">
      <div class="flex items-center justify-between gap-3 text-xs">
        <span class="font-bold">${escapeHtml(event.type || 'event')}${status ? ` · ${escapeHtml(status)}` : ''}</span>
        <span class="text-zinc-600">${escapeHtml(formatTime(event.created_at))}</span>
      </div>
      <div class="mt-1 text-xs text-zinc-400">${escapeHtml(event.message || '')}</div>
    </div>`;
  },

  _renderArtifact(artifact) {
    const label = `${artifact.type || 'file'} #${artifact.seq || '-'}`;
    const size = typeof artifact.size === 'number' ? ` · ${Math.round(artifact.size / 1024)} KB` : '';
    const downloadUrl = safeArtifactDownloadUrl(artifact.download_url);
    const link = downloadUrl ? `<a class="btn btn-ghost btn-sm" href="${escapeHtml(downloadUrl)}" target="_blank" rel="noopener">Open</a>` : '';
    return `<div class="flex items-center justify-between gap-3 rounded-lg bg-theme-elevated border border-theme-subtle p-3">
      <div class="min-w-0">
        <div class="text-sm text-zinc-200 truncate">${escapeHtml(artifact.name || label)}</div>
        <div class="text-xs text-zinc-600">${escapeHtml(label + size)}</div>
      </div>
      ${link}
    </div>`;
  },

  _renderCheckpoint(checkpoint) {
    const cursor = checkpoint.cursor && Object.keys(checkpoint.cursor).length ? ` · ${JSON.stringify(checkpoint.cursor)}` : '';
    return `<div class="rounded-lg bg-theme-elevated border border-theme-subtle p-3">
      <div class="flex items-center justify-between gap-3 text-xs">
        <span class="font-bold text-zinc-200">#${escapeHtml(String(checkpoint.seq || '-'))} ${escapeHtml(checkpoint.recovery_level || 'L0')}</span>
        <span class="text-zinc-600">${escapeHtml(formatTime(checkpoint.created_at))}</span>
      </div>
      <div class="mt-1 text-xs text-zinc-500 truncate">${escapeHtml(`${checkpoint.collector_name || '-'}${cursor}`)}</div>
    </div>`;
  },

  _taskSessionStateLabel(sessionState, sessionMode) {
    if (sessionMode === 'managed_state') {
      if (sessionState.storage_state_ready) return 'ready / storage_state';
      return 'blocked / storage_state_missing';
    }
    if (sessionMode === 'local_profile') {
      if (sessionState.local_profile_ready) return 'ready / local_profile';
      return sessionState.health || 'blocked / local_profile_missing';
    }
    return sessionState.health || '-';
  },
};

// Global exports for onclick handlers in HTML
window.loadTasks = function () { if (window._tasksPage) window._tasksPage.refresh(); };
window.showCreateTaskModal = function () { if (window._tasksPage) window._tasksPage._showCreateModal(); };
window.updateTaskTargetFields = function () { if (window._tasksPage) window._tasksPage._updateTargetFields(); };
window.buildTaskTargetsFromForm = function (fs) { if (window._tasksPage) return window._tasksPage._buildTargets(fs); };
window.renderTaskPrecheck = function (p) { if (window._tasksPage) window._tasksPage._renderPrecheck(p); };
window.wizardNext = function () { if (window._tasksPage) window._tasksPage._wizardNext(); };
window.wizardPrev = function () { if (window._tasksPage) window._tasksPage._wizardPrev(); };
window.createTask = function () { if (window._tasksPage) window._tasksPage._createTask(); };
window.cancelTask = function (id) { if (window._tasksPage) window._tasksPage._cancelTask(id); };
window.deleteTask = function (id) { if (window._tasksPage) window._tasksPage._deleteTask(id); };
window.viewTaskLogs = function (id) { if (window._tasksPage) window._tasksPage._viewLogs(id); };
window.viewTaskDetail = function (id) { if (window._tasksPage) window._tasksPage._viewDetail(id); };
window.resumeTask = function (id) { if (window._tasksPage) window._tasksPage._resumeTask(id); };
window.rerunTask = function (id) { if (window._tasksPage) window._tasksPage._rerunTask(id); };
window.getCollectorForPipeline = function (n) { if (window._tasksPage) return window._tasksPage._getCollector(n); };
window.hasStorageStep = function () { return false; };

// ── YouTube TXT import ──
window._importedYouTubeTargetsByCollector = window._importedYouTubeTargetsByCollector || {};
window._importedYouTubeTargets = window._importedYouTubeTargets || [];
window.importYouTubeTargets = async function (collector, targetType, prefix = 'task') {
  const p = prefix || 'task';
  const inputId = collector === 'youtube_profiles' ? `${p}-yt-profiles-txt` : `${p}-yt-comments-txt`;
  const previewId = collector === 'youtube_profiles' ? `${p}-yt-profiles-preview` : `${p}-yt-comments-preview`;
  const input = document.getElementById(inputId);
  const preview = document.getElementById(previewId);
  if (!input?.files?.length) return;
  const file = input.files[0];
  const formData = new FormData();
  formData.append('file', file);
  formData.append('collector_name', collector);
  formData.append('target_type', targetType);
  try {
    const resp = await api('/tasks/import-targets', { method: 'POST', body: formData, isFormData: true });
    const targets = resp.targets || [];
    window._importedYouTubeTargetsByCollector[collector] = targets;
    window._importedYouTubeTargets = targets;
    const skipped = resp.skipped > 0 ? `, 跳过 ${resp.skipped} 行` : '';
    preview.style.display = 'block';
    preview.className = 'mt-3 rounded-lg bg-emerald-500/10 border border-emerald-500/20 p-3 text-xs text-emerald-300';
    preview.innerHTML = `已解析 <strong>${resp.total}</strong> 个目标${skipped}<br>` +
      (resp.skipped_reasons?.length ? `<span class="text-amber-400">${resp.skipped_reasons.slice(0, 3).join('<br>')}</span>` : '');
    if (typeof toast === 'function') toast(`已导入 ${resp.total} 个采集目标`, 'success');
  } catch (err) {
    window._importedYouTubeTargetsByCollector[collector] = [];
    window._importedYouTubeTargets = [];
    preview.style.display = 'block';
    preview.className = 'mt-3 rounded-lg bg-rose-500/10 border border-rose-500/20 p-3 text-xs text-rose-300';
    preview.innerHTML = `导入失败：${err.message || '未知错误'}`;
  }
};
