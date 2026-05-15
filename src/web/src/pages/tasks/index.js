import { api, toast, escapeHtml, escapeJs, formatTime } from '../../core/api.js';
import { renderBadge, renderProgress, setValue } from '../../core/api.js';

let currentWizardStep = 1;

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this._unsub = store.subscribe((key, value) => {
      if (key === 'refresh' && value === 'tasks') this.refresh();
    });
    this.refresh();
    return this;
  },

  destroy() { if (this._unsub) this._unsub(); },

  async refresh() { await this._load(); },

  async _load() {
    try {
      const filter = document.getElementById('task-status-filter')?.value || '';
      const query = filter ? `?status=${encodeURIComponent(filter)}` : '';
      const tasks = await api(`/tasks${query}`);
      const ordered = [...tasks].sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
      this._renderTable(ordered);
    } catch (err) {
      toast(`Load failed: ${err.message}`, 'error');
    }
  },

  _renderTable(tasks) {
    const tbody = document.getElementById('tasks-body');
    if (!tbody) return;
    if (!tasks.length) {
      tbody.innerHTML = '<tr><td colspan="8" class="text-muted">No tasks</td></tr>';
      return;
    }
    tbody.innerHTML = tasks.map((task) => `
      <tr>
        <td><code>${task.id}</code></td>
        <td>${escapeHtml(task.name)}</td>
        <td>${escapeHtml(task.pipeline_name || '-')}</td>
        <td>${renderBadge(task.status)}</td>
        <td>${renderProgress(task.progress)}</td>
        <td>${task.targets_count}</td>
        <td>${task.duration ? `${task.duration.toFixed(1)}s` : '-'}</td>
        <td>${this._renderActions(task)}</td>
      </tr>
    `).join('');
  },

  _renderActions(task) {
    const actions = [
      `<button class="btn btn-ghost btn-sm" onclick="viewTaskDetail('${escapeJs(task.id)}')">Details</button>`,
      `<button class="btn btn-ghost btn-sm" onclick="viewTaskLogs('${escapeJs(task.id)}')">Logs</button>`,
    ];
    if (task.status === 'running' || task.status === 'pending') {
      actions.push(`<button class="btn btn-danger btn-sm" onclick="cancelTask('${escapeJs(task.id)}')">Cancel</button>`);
    } else {
      actions.push(`<button class="btn btn-danger btn-sm" onclick="deleteTask('${escapeJs(task.id)}')">Delete</button>`);
    }
    return `<div class="action-buttons">${actions.join(' ')}</div>`;
  },

  // ── Create Task Wizard ──

  _showCreateModal() {
    Promise.all([window.loadPipelineTemplates(), window.loadPipelineSelect('task-pipeline')])
      .then(() => this._updateTargetFields());
    window.openModal('modal-create-task');
    currentWizardStep = 1;
    this._updateWizardUI();
  },

  _getCollector(pipelineName) {
    const pipeline = window.availablePipelines?.[pipelineName]
      || (window._pipelinesPage ? null : null);
    const templates = window._pipelinesPage ? null : null;
    // Rely on availablePipelines being populated by loadPipelineSelect
    const p = (window.availablePipelines || {})[pipelineName];
    const collectorStep = p?.steps?.find((step) => step.type === 'collector');
    return collectorStep?.name || collectorStep?.component_name || '';
  },

  _updateTargetFields() {
    const pipelineName = document.getElementById('task-pipeline')?.value || '';
    const collector = this._getCollector(pipelineName);

    const fields = {
      steam: 'task-steam-fields',
      steam_discussions: 'task-steam-discussions-fields',
      taptap: 'task-taptap-fields',
      monitor: 'task-monitor-fields',
      qimai: 'task-qimai-fields',
      official_site: 'task-official-site-fields',
    };
    Object.entries(fields).forEach(([c, id]) => {
      const el = document.getElementById(id);
      if (el) el.style.display = collector === c ? 'block' : 'none';
    });

    const helper = document.getElementById('task-target-helper');
    if (helper) {
      const tips = {
        taptap: 'TapTap v1 expects a public mainland page URL or app ID.',
        steam_discussions: 'Steam Community tasks use app id or forum URL plus optional start/end dates.',
        monitor: 'Monitor tasks use app id and optional Twitch/SullyGnome hints.',
        qimai: 'Qimai tasks use qimai_app_id (App Store ID or Package Name).',
        official_site: 'Official site tasks use target name plus official_url, or advanced JSON targets.',
      };
      helper.textContent = tips[collector] || 'Steam tasks use target name + app id, or advanced JSON targets.';
    }

    const autoReport = document.getElementById('task-enable-report');
    if (autoReport && ['steam_full_report', 'taptap_full_report', 'steam_discussions_full_report'].includes(pipelineName)) {
      autoReport.checked = true;
    }
  },

  _buildTargets(formState) {
    const { collector, targetName, appId, skipSteamdb, steamdbTimeSlice,
      steamDiscussionsForumUrl, steamDiscussionsStart, steamDiscussionsEnd,
      steamDiscussionsMaxPages, steamDiscussionsMaxTopics, steamDiscussionsIncludeReplies,
      taptapUrl, taptapReviewsPages, taptapReviewsLimit, monitorDays, monitorTwitchName,
      monitorSiteurl, qimaiAppId, officialSiteUrl } = formState;

    if (collector === 'steam_discussions') {
      if (!targetName && !appId && !steamDiscussionsForumUrl) return [];
      return [{
        name: targetName || appId || steamDiscussionsForumUrl, target_type: 'game',
        params: {
          ...(appId ? { app_id: appId } : {}),
          ...(steamDiscussionsForumUrl ? { forum_url: steamDiscussionsForumUrl } : {}),
          ...(steamDiscussionsStart ? { start_time: steamDiscussionsStart } : {}),
          ...(steamDiscussionsEnd ? { end_time: steamDiscussionsEnd } : {}),
          max_pages: Number(steamDiscussionsMaxPages || 50),
          max_topics: Number(steamDiscussionsMaxTopics || 1000),
          include_replies: Boolean(steamDiscussionsIncludeReplies),
        },
      }];
    }
    if (collector === 'taptap') {
      if (!targetName && !taptapUrl && !appId) return [];
      return [{
        name: targetName || appId || taptapUrl, target_type: 'game',
        params: {
          region: 'cn', metrics: ['details', 'reviews', 'updates'],
          reviews_pages: Number(taptapReviewsPages || 1),
          reviews_limit: Number(taptapReviewsLimit || 20),
          use_playwright: 'auto',
          ...(taptapUrl ? { page_url: taptapUrl } : {}),
          ...(appId ? { app_id: appId } : {}),
        },
      }];
    }
    if (collector === 'monitor') {
      if (!targetName && !appId) return [];
      return [{
        name: targetName || appId, target_type: 'game',
        params: {
          app_id: appId, days: Number(monitorDays || 30),
          metrics: ['twitch_viewer_trend'],
          ...(monitorTwitchName ? { twitch_name: monitorTwitchName } : {}),
          ...(monitorSiteurl ? { siteurl: monitorSiteurl } : {}),
        },
      }];
    }
    if (collector === 'qimai') {
      if (!targetName && !qimaiAppId) return [];
      return [{ name: targetName || qimaiAppId, target_type: 'game', params: { qimai_app_id: qimaiAppId } }];
    }
    if (collector === 'official_site') {
      if (!officialSiteUrl) return [];
      return [{
        name: targetName || officialSiteUrl, target_type: 'game',
        params: { official_url: officialSiteUrl, use_playwright: 'auto' },
      }];
    }
    if (!targetName && !appId) return [];
    return [{
      name: targetName || appId, target_type: 'game',
      params: { ...(appId ? { app_id: appId } : {}), ...(!skipSteamdb && steamdbTimeSlice ? { steamdb_time_slice: steamdbTimeSlice } : {}), ...(skipSteamdb ? { skip_steamdb: true } : {}) },
    }];
  },

  // ── Wizard UI ──

  _updateWizardUI() {
    document.querySelectorAll('#task-wizard-steps .wizard-step').forEach((el) => {
      const s = parseInt(el.dataset.step);
      el.classList.remove('active', 'done');
      if (s < currentWizardStep) el.classList.add('done');
      if (s === currentWizardStep) el.classList.add('active');
    });
    document.querySelectorAll('.wizard-panel').forEach((el) => {
      el.style.display = parseInt(el.dataset.panel) === currentWizardStep ? '' : 'none';
    });
    const back = document.getElementById('btn-wizard-back');
    const next = document.getElementById('btn-wizard-next');
    const submit = document.getElementById('btn-submit-task');
    if (back) back.style.display = currentWizardStep > 1 ? '' : 'none';
    if (next) next.style.display = currentWizardStep < 3 ? '' : 'none';
    if (submit) submit.style.display = currentWizardStep === 3 ? '' : 'none';
  },

  _wizardNext() {
    if (currentWizardStep === 1) {
      const pipeline = document.getElementById('task-pipeline')?.value;
      if (!pipeline) { toast('请选择 Pipeline', 'error'); return; }
      this._updateTargetFields();
    }
    if (currentWizardStep < 3) { currentWizardStep++; this._updateWizardUI(); }
  },

  _wizardPrev() { if (currentWizardStep > 1) { currentWizardStep--; this._updateWizardUI(); } },

  // ── Create / Cancel / Delete ──

  async _createTask() {
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
    const collector = this._getCollector(pipelineName);

    if (!name || !pipelineName) { toast('Task name and pipeline are required', 'error'); return; }

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

    if (targetsRaw) { try { targets = JSON.parse(targetsRaw); } catch { toast('Targets JSON is invalid', 'error'); return; } }
    if (!targets.length) { toast('At least one target is required', 'error'); return; }

    const enableReport = getChecked('task-enable-report');
    const reportPromptRaw = getVal('task-report-prompt');
    const reportTemplate = document.getElementById('task-report-template')?.value || 'default';
    const primarySubject = targetName
      || (collector === 'taptap' ? getVal('task-taptap-app-id') : collector === 'steam_discussions' ? steamDiscussionsAppId : steamAppId)
      || (collector === 'official_site' ? getVal('task-official-site-url') : '')
      || name;
    const reportPrompt = reportPromptRaw || `基于本次采集结果，总结${primarySubject}的核心表现、版本更新、评论反馈和关键事件。`;

    const hasStorage = (pipeline, storageName) => {
      const p = (window.availablePipelines || {})[pipeline];
      return Boolean(p?.steps?.some(s => s.type === 'storage' && (s.name || s.component_name) === storageName));
    };
    const config = enableReport ? {
      report: { enabled: true, prompt: reportPrompt, template: reportTemplate, data_source: collector || pipelineName, params: { use_vector: hasStorage(pipelineName, 'vector') } },
    } : {};
    if (dataGroup) config.data_group = { id: dataGroup, name: dataGroup };

    const payload = { name, pipeline_name: pipelineName, targets, description, config };

    try {
      const precheck = await api('/tasks/precheck', { method: 'POST', body: JSON.stringify(payload) });
      this._renderPrecheck(precheck);
      if (!precheck.can_submit) { toast('Task precheck failed', 'error'); return; }
      if (precheck.status === 'warning') {
        const warningText = (precheck.issues || []).filter(i => i.level === 'warning').map(i => i.message).join('\n');
        if (!confirm(`Task precheck has warnings:\n${warningText}\n\nSubmit anyway?`)) return;
      }
      await api('/tasks', { method: 'POST', body: JSON.stringify(payload) });
      toast('Task created', 'success');
      window.closeModal('modal-create-task');
      window.refreshDashboard && window.refreshDashboard();
      this.refresh();
    } catch (err) { toast(`Create failed: ${err.message}`, 'error'); }
  },

  _renderPrecheck(precheck) {
    const container = document.getElementById('task-precheck');
    if (!container || !precheck) return;
    const issues = precheck.issues || [];
    const credentials = precheck.credential_status || {};
    const dataSources = precheck.data_source_status || {};
    const required = precheck.required_fields || [];
    container.style.display = 'block';
    container.className = `task-precheck task-precheck-${precheck.status || 'ok'}`;
    container.innerHTML = `
      <div class="task-precheck-title">Precheck: ${escapeHtml(precheck.status || 'ok')}</div>
      <div class="task-precheck-grid">
        <span>Collector</span><strong>${escapeHtml(precheck.collector_name || '-')}</strong>
        <span>Required</span><strong>${escapeHtml(required.join(' / ') || '-')}</strong>
        <span>Credentials</span><strong>${escapeHtml(Object.entries(credentials).map(([k,v]) => `${k}: ${v}`).join(' / ') || '-')}</strong>
        <span>Data source</span><strong>${escapeHtml(Object.entries(dataSources).map(([k,v]) => `${k}: ${v}`).join(' / ') || '-')}</strong>
      </div>
      ${issues.length ? `<ul>${issues.map(i => `<li class="task-precheck-${escapeHtml(i.level)}">${escapeHtml(i.field)}: ${escapeHtml(i.message)}</li>`).join('')}</ul>` : ''}`;
  },

  async _cancelTask(id) {
    try { await api(`/tasks/${id}/cancel`, { method: 'POST' }); toast('Task cancelled', 'success'); window.refreshDashboard(); this.refresh(); }
    catch (err) { toast(`Cancel failed: ${err.message}`, 'error'); }
  },

  async _deleteTask(id) {
    if (!confirm(`Delete task "${id}"?`)) return;
    try { await api(`/tasks/${encodeURIComponent(id)}?confirm=true`, { method: 'DELETE' }); toast('Task deleted', 'success'); window.refreshDashboard(); this.refresh(); }
    catch (err) { toast(`Delete failed: ${err.message}`, 'error'); }
  },

  async _viewLogs(id) {
    window.openModal('modal-task-logs');
    const modalLogs = document.getElementById('modal-task-logs');
    if (modalLogs) modalLogs.dataset.taskId = id;
    const container = document.getElementById('task-logs-content');
    if (!container) return;
    container.innerHTML = '<p class="text-muted">Loading...</p>';
    try {
      const data = await api(`/tasks/${id}/logs`);
      if (!data.logs.length) { container.innerHTML = '<p class="text-muted">No logs</p>'; return; }
      container.innerHTML = data.logs.map((log) => {
        const statusClass = log.status === 'success' ? 'log-success' : log.status === 'failed' ? 'log-failed' : 'log-running';
        return `<div class="log-entry ${statusClass}">
          <span class="log-step">${escapeHtml(log.step)}</span>
          <span class="log-message">${escapeHtml(log.message || '')}</span>
          ${log.error ? `<div style="color:var(--danger);margin-top:0.25rem">${escapeHtml(log.error)}</div>` : ''}
          ${log.started_at ? `<span class="log-time">${formatTime(log.started_at)}</span>` : ''}
        </div>`;
      }).join('');
    } catch (err) { container.innerHTML = `<p style="color:var(--danger)">Load failed: ${escapeHtml(err.message)}</p>`; }
  },

  async _viewDetail(id) {
    window.openModal('modal-task-detail');
    const container = document.getElementById('task-detail-content');
    if (!container) return;
    container.innerHTML = '<p class="text-muted">Loading...</p>';
    try {
      const task = await api(`/tasks/${id}`);
      const targets = task.targets?.length ? `<pre class="report-output">${escapeHtml(JSON.stringify(task.targets, null, 2))}</pre>` : '<p class="text-muted">No targets</p>';
      const config = Object.keys(task.config || {}).length ? `<pre class="report-output">${escapeHtml(JSON.stringify(task.config, null, 2))}</pre>` : '<p class="text-muted">No runtime config</p>';
      const resultSummary = task.result_summary ? `<pre class="report-output">${escapeHtml(JSON.stringify(task.result_summary, null, 2))}</pre>` : '<p class="text-muted">No result summary</p>';
      const autoReportLink = task.result_summary?.generated_report_id
        ? `<div style="margin-top:0.75rem"><button class="btn btn-primary btn-sm" onclick="viewReport('${escapeJs(task.result_summary.generated_report_id)}')">Open Generated Report</button></div>` : '';
      const latestLogs = task.step_logs?.length
        ? task.step_logs.slice(-8).map(log => `<div class="log-entry ${log.status==='success'?'log-success':log.status==='failed'?'log-failed':'log-running'}">
            <span class="log-step">${escapeHtml(log.step)}</span>
            <span class="log-message">${escapeHtml(log.message||'')}</span>
            ${log.error?`<div style="color:var(--danger);margin-top:0.25rem">${escapeHtml(log.error)}</div>`:''}
          </div>`).join('')
        : '<p class="text-muted">No logs</p>';

      container.innerHTML = `
        <div class="detail-grid">
          <div class="detail-card">
            <h3>Basic</h3>
            <div class="detail-kv"><span>ID</span><code>${task.id}</code></div>
            <div class="detail-kv"><span>Name</span><span>${escapeHtml(task.name)}</span></div>
            <div class="detail-kv"><span>Status</span><span>${renderBadge(task.status)}</span></div>
            <div class="detail-kv"><span>Pipeline</span><span>${escapeHtml(task.pipeline_name||'-')}</span></div>
            <div class="detail-kv"><span>Progress</span><span>${Math.round(task.progress*100)}%</span></div>
            <div class="detail-kv"><span>Retry</span><span>${task.retry_count}/${task.max_retries}</span></div>
            <div class="detail-kv"><span>Error</span><span>${escapeHtml(task.error||'-')}</span></div>
          </div>
          <div class="detail-card">
            <h3>Description</h3><p>${escapeHtml(task.description||'No description')}</p>
            <h3 style="margin-top:1rem">Recent Logs</h3>${latestLogs}
          </div>
        </div>
        <h3 style="margin-top:1rem">Targets</h3>${targets}
        <h3 style="margin-top:1rem">Runtime Config</h3>${config}
        <h3 style="margin-top:1rem">Result Summary</h3>${resultSummary}${autoReportLink}`;
    } catch (err) { container.innerHTML = `<p style="color:var(--danger)">Load failed: ${escapeHtml(err.message)}</p>`; }
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
window.getCollectorForPipeline = function (n) { if (window._tasksPage) return window._tasksPage._getCollector(n); };
window.hasStorageStep = function () { return false; };
