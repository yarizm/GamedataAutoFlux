import { api, toast, escapeHtml, formatTime } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { renderEmptyState } from '../../core/uiState.js';
import {
  getCollectorForPipeline,
  loadAvailablePipelines,
  populatePipelineSelect,
} from '../../core/pipelines.js';
import {
  applyTargetToForm,
  buildTargets,
  parseAdvancedTargetsJson,
  readTargetFormState,
  updateTargetFieldPanels,
} from '../../core/targetForm.js';

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this._scheduleMode = 'preset';
    this._editName = null;
    this._bindModalOnce();
    this.refresh();
    return this;
  },

  destroy() {},

  async refresh() { await this._load(); },

  _bindModalOnce() {
    if (window._cronModalBound) return;
    window._cronModalBound = true;

    document.getElementById('cron-mode-preset')?.addEventListener('click', () => {
      this._setScheduleMode('preset');
    });
    document.getElementById('cron-mode-cron')?.addEventListener('click', () => {
      this._setScheduleMode('cron');
    });
    document.getElementById('cron-preset-type')?.addEventListener('change', () => {
      this._updatePresetFields();
      this._previewSchedule();
    });
    ['cron-preset-interval', 'cron-preset-time', 'cron-preset-day', 'cron-preset-minute', 'cron-timezone', 'cron-expr']
      .forEach((id) => {
        document.getElementById(id)?.addEventListener('change', () => this._previewSchedule());
        document.getElementById(id)?.addEventListener('input', () => this._previewSchedule());
      });
    document.getElementById('cron-preset-weekdays')?.addEventListener('change', () => this._previewSchedule());
    document.getElementById('btn-cron-preview')?.addEventListener('click', () => this._previewSchedule(true));
  },

  async _load() {
    try {
      const jobs = await api('/cron-jobs');
      const list = this.container.querySelector('#cron-list');
      if (!list) return;

      if (!jobs.length) {
        list.innerHTML = renderEmptyState({
          title: t('cron.empty'),
          hint: t('ui.empty.cronHint'),
          variant: 'compact',
          escapeHtml,
          actionHtml: `<button type="button" class="btn btn-primary btn-sm" onclick="showCreateCronModal()">${escapeHtml(t('cron.create'))}</button>`,
        });
        return;
      }
      list.innerHTML = jobs.map((job) => {
        const enabled = job.enabled !== false;
        const tone = enabled
          ? 'text-emerald-300 bg-emerald-500/10 border-emerald-500/20'
          : 'text-zinc-400 bg-zinc-500/10 border-zinc-500/20';
        const targets = job.targets_count ?? (job.task_template?.targets?.length || 0);
        const label = job.human_label || job.cron_expr || job.trigger || '-';
        return `
        <div class="cron-item group flex items-center justify-between p-4 rounded-xl bg-theme-elevated border border-theme-subtle transition-all duration-300 hover:bg-white/5 hover:border-theme-strong mb-3 relative overflow-hidden">
          <div class="flex items-center gap-4 flex-1 min-w-0">
            <div class="w-10 h-10 rounded-lg bg-amber-500/10 border border-amber-500/20 flex items-center justify-center text-amber-400 shrink-0">
              <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
            </div>
            <div class="flex-1 min-w-0">
              <div class="flex items-center gap-2 mb-1">
                <div class="font-bold text-theme-primary text-sm tracking-tight truncate">${escapeHtml(job.name)}</div>
                <span class="shrink-0 rounded border px-2 py-0.5 text-[10px] font-bold uppercase ${tone}">${enabled ? t('cron.on') : t('cron.off')}</span>
              </div>
              <div class="text-xs text-zinc-400 mb-1 truncate">${escapeHtml(job.description || job.pipeline_name || '')}</div>
              <div class="flex flex-wrap items-center gap-x-4 gap-y-1">
                <div class="flex items-center gap-1.5">
                  <span class="text-[10px] font-bold text-zinc-600 uppercase tracking-widest">${escapeHtml(t('cron.schedule'))}</span>
                  <span class="text-[11px] text-zinc-200">${escapeHtml(label)}</span>
                </div>
                <div class="flex items-center gap-1.5">
                  <span class="text-[10px] font-bold text-zinc-600 uppercase tracking-widest">${escapeHtml(t('cron.pipeline'))}</span>
                  <code class="text-[11px] text-zinc-300 font-mono">${escapeHtml(job.pipeline_name || '-')}</code>
                </div>
                <div class="flex items-center gap-1.5">
                  <span class="text-[10px] font-bold text-zinc-600 uppercase tracking-widest">${escapeHtml(t('cron.targets'))}</span>
                  <span class="text-[11px] text-zinc-300">${targets}</span>
                </div>
                <div class="flex items-center gap-1.5">
                  <span class="text-[10px] font-bold text-zinc-600 uppercase tracking-widest">${escapeHtml(t('cron.nextShort'))}</span>
                  <span class="text-[11px] text-amber-400/80 font-mono tabular-nums">${job.next_run ? escapeHtml(formatTime(job.next_run)) : '-'}</span>
                </div>
                ${job.cron_expr ? `<code class="text-[10px] text-zinc-500 font-mono">${escapeHtml(job.cron_expr)}</code>` : ''}
              </div>
            </div>
          </div>
          <div class="flex items-center gap-2 shrink-0 opacity-0 group-hover:opacity-100 transition-opacity">
            <button class="btn btn-ghost h-8 px-2 text-xs" data-run="${escapeHtml(job.id)}">${t('cron.runNow')}</button>
            <button class="btn btn-ghost h-8 px-2 text-xs" data-toggle="${escapeHtml(job.id)}" data-enabled="${enabled ? '1' : '0'}">${enabled ? t('cron.pause') : t('cron.enable')}</button>
            <button class="btn btn-ghost h-8 px-2 text-xs" data-edit="${escapeHtml(job.id)}">${t('common.edit')}</button>
            <button class="btn btn-danger h-8 px-3 text-xs" data-delete="${escapeHtml(job.id)}">${t('common.delete')}</button>
          </div>
          <div class="absolute left-0 top-0 bottom-0 w-[3px] bg-amber-500 opacity-0 group-hover:opacity-100 transition-opacity"></div>
        </div>`;
      }).join('');

      list.querySelectorAll('[data-delete]').forEach((btn) => {
        btn.addEventListener('click', () => this._deleteJob(btn.dataset.delete));
      });
      list.querySelectorAll('[data-edit]').forEach((btn) => {
        btn.addEventListener('click', () => this._editJob(btn.dataset.edit));
      });
      list.querySelectorAll('[data-run]').forEach((btn) => {
        btn.addEventListener('click', () => this._runJob(btn.dataset.run));
      });
      list.querySelectorAll('[data-toggle]').forEach((btn) => {
        btn.addEventListener('click', () => {
          const enabled = btn.dataset.enabled === '1';
          this._toggleJob(btn.dataset.toggle, !enabled);
        });
      });
    } catch (err) {
      console.error('Load cron jobs failed:', err);
    }
  },

  _setScheduleMode(mode) {
    this._scheduleMode = mode;
    const presetPanel = document.getElementById('cron-preset-panel');
    const advancedPanel = document.getElementById('cron-advanced-panel');
    const presetBtn = document.getElementById('cron-mode-preset');
    const cronBtn = document.getElementById('cron-mode-cron');
    if (presetPanel) presetPanel.style.display = mode === 'preset' ? '' : 'none';
    if (advancedPanel) advancedPanel.style.display = mode === 'cron' ? '' : 'none';
    presetBtn?.classList.toggle('active', mode === 'preset');
    cronBtn?.classList.toggle('active', mode === 'cron');
    this._updatePresetFields();
    this._previewSchedule();
  },

  _updatePresetFields() {
    const type = document.getElementById('cron-preset-type')?.value || 'daily';
    const show = {
      interval: type === 'every_minutes',
      time: type === 'daily' || type === 'weekly' || type === 'monthly',
      weekdays: type === 'weekly',
      day: type === 'monthly',
      minute: type === 'hourly',
    };
    document.querySelectorAll('[data-preset-field]').forEach((el) => {
      const key = el.getAttribute('data-preset-field');
      el.style.display = show[key] ? '' : 'none';
    });
  },

  _buildSchedulePayload() {
    const timezone = document.getElementById('cron-timezone')?.value.trim() || 'Asia/Shanghai';
    if (this._scheduleMode === 'cron') {
      return {
        mode: 'cron',
        cron_expr: document.getElementById('cron-expr')?.value.trim() || '',
        timezone,
      };
    }
    const type = document.getElementById('cron-preset-type')?.value || 'daily';
    const preset = { type };
    if (type === 'every_minutes') {
      preset.interval = parseInt(document.getElementById('cron-preset-interval')?.value || '15', 10);
    } else if (type === 'hourly') {
      preset.minute = parseInt(document.getElementById('cron-preset-minute')?.value || '0', 10);
    } else if (type === 'daily' || type === 'weekly' || type === 'monthly') {
      preset.time = document.getElementById('cron-preset-time')?.value || '08:00';
    }
    if (type === 'weekly') {
      const days = [...document.querySelectorAll('#cron-preset-weekdays input:checked')].map((el) => el.value);
      preset.weekdays = days;
    }
    if (type === 'monthly') {
      preset.day_of_month = parseInt(document.getElementById('cron-preset-day')?.value || '1', 10);
    }
    return { mode: 'preset', preset, timezone };
  },

  async _updateTargetFields() {
    const pipelineName = document.getElementById('cron-pipeline')?.value || '';
    try {
      await loadAvailablePipelines();
    } catch {
      /* ignore — badge still updates */
    }
    const collector = getCollectorForPipeline(pipelineName);
    updateTargetFieldPanels('cron', collector);
    return collector;
  },

  _buildTaskTemplate() {
    const pipelineName = document.getElementById('cron-pipeline')?.value || '';
    const collector = getCollectorForPipeline(pipelineName);
    const formState = readTargetFormState('cron', collector);
    let targets = buildTargets(formState);

    const targetsRaw = document.getElementById('cron-targets')?.value.trim() || '';
    if (targetsRaw) {
      try {
        const parsed = parseAdvancedTargetsJson(
          targetsRaw,
          formState.targetName || t('tasks.targetName'),
        );
        if (parsed) targets = parsed;
      } catch {
        throw new Error(t('cron.advancedJsonInvalid'));
      }
    }

    const config = {};
    if (document.getElementById('cron-rolling-window')?.checked) {
      config.refresh = { rolling_window: true };
    }
    if (document.getElementById('cron-report-enabled')?.checked) {
      config.report = { enabled: true };
    }
    const dataGroup = document.getElementById('cron-data-group')?.value.trim() || '';
    if (dataGroup) {
      config.data_group = { id: dataGroup, name: dataGroup };
    }
    const template = {};
    if (collector) template.collector_name = collector;
    if (targets.length) template.targets = targets;
    if (Object.keys(config).length) template.config = config;
    const description = document.getElementById('cron-description')?.value.trim() || '';
    if (description) template.description = description;
    return template;
  },

  _clearTargetFields() {
    [
      'cron-target-name', 'cron-app-id', 'cron-taptap-url', 'cron-taptap-app-id',
      'cron-steam-discussions-app-id', 'cron-steam-discussions-forum-url',
      'cron-steam-discussions-start', 'cron-steam-discussions-end',
      'cron-monitor-app-id', 'cron-monitor-twitch-name', 'cron-monitor-siteurl',
      'cron-qimai-app-id', 'cron-official-site-url', 'cron-targets',
    ].forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.value = '';
    });
    const skip = document.getElementById('cron-skip-steamdb');
    if (skip) skip.checked = true;
  },

  async _previewSchedule(forceToast = false) {
    const box = document.getElementById('cron-preview-box');
    try {
      const schedule = this._buildSchedulePayload();
      const body = {
        schedule,
        timezone: schedule.timezone,
        cron_expr: schedule.cron_expr || '',
        count: 5,
      };
      const result = await api('/cron-jobs/preview', { method: 'POST', body: JSON.stringify(body) });
      const runs = (result.next_runs || []).map((x) => formatTime(x)).join('\n');
      if (box) {
        box.textContent = `${result.human_label || ''}\n${result.cron_expr || ''}\n${t('cron.previewNext')}\n${runs || '-'}`;
      }
      if (forceToast) toast(result.human_label || t('cron.previewOk'), 'success');
    } catch (err) {
      if (box) box.textContent = err.message || t('cron.previewFail');
      if (forceToast) toast(err.message || t('cron.previewFail'), 'error');
    }
  },

  async _showCreateModal() {
    this._editName = null;
    document.getElementById('cron-edit-mode').value = 'create';
    const title = document.getElementById('cron-modal-title');
    if (title) title.textContent = t('cron.modal.create');
    const submit = document.getElementById('btn-submit-cron');
    if (submit) submit.textContent = t('common.create');
    document.getElementById('cron-name').value = '';
    document.getElementById('cron-name').disabled = false;
    document.getElementById('cron-description').value = '';
    document.getElementById('cron-data-group').value = '';
    document.getElementById('cron-rolling-window').checked = false;
    document.getElementById('cron-report-enabled').checked = false;
    document.getElementById('cron-enabled').checked = true;
    document.getElementById('cron-timezone').value = 'Asia/Shanghai';
    document.getElementById('cron-expr').value = '0 8 * * *';
    document.getElementById('cron-preset-type').value = 'daily';
    document.getElementById('cron-preset-time').value = '08:00';
    this._clearTargetFields();
    await populatePipelineSelect('cron-pipeline');
    await this._updateTargetFields();
    this._setScheduleMode('preset');
    window.openModal && window.openModal('modal-create-cron');
    this._previewSchedule();
  },

  async _editJob(name) {
    try {
      const job = await api(`/cron-jobs/${encodeURIComponent(name)}`);
      this._editName = name;
      document.getElementById('cron-edit-mode').value = 'edit';
      const title = document.getElementById('cron-modal-title');
      if (title) title.textContent = t('cron.modal.edit', { name });
      const submit = document.getElementById('btn-submit-cron');
      if (submit) submit.textContent = t('common.save');
      await populatePipelineSelect('cron-pipeline');
      document.getElementById('cron-name').value = job.name || name;
      document.getElementById('cron-name').disabled = true;
      document.getElementById('cron-pipeline').value = job.pipeline_name || '';
      document.getElementById('cron-description').value = job.description || '';
      document.getElementById('cron-timezone').value = job.timezone || 'Asia/Shanghai';
      document.getElementById('cron-enabled').checked = job.enabled !== false;
      document.getElementById('cron-expr').value = job.cron_expr || '';
      const template = job.task_template || {};
      this._clearTargetFields();
      document.getElementById('cron-rolling-window').checked = !!(template.config?.refresh?.rolling_window);
      document.getElementById('cron-report-enabled').checked = !!(template.config?.report?.enabled);
      document.getElementById('cron-data-group').value = template.config?.data_group?.id || template.config?.data_group?.name || '';

      const collector = await this._updateTargetFields();
      const targets = Array.isArray(template.targets) ? template.targets : [];
      if (collector === 'youtube_profiles' || collector === 'youtube_comments') {
        window._importedYouTubeTargetsByCollector = window._importedYouTubeTargetsByCollector || {};
        window._importedYouTubeTargetsByCollector[collector] = targets;
        if (targets.length > 1) {
          document.getElementById('cron-targets').value = JSON.stringify(targets, null, 2);
        }
        const previewId = collector === 'youtube_profiles' ? 'cron-yt-profiles-preview' : 'cron-yt-comments-preview';
        const preview = document.getElementById(previewId);
        if (preview && targets.length) {
          preview.style.display = 'block';
          preview.className = 'mt-2 rounded-lg bg-emerald-500/10 border border-emerald-500/20 p-3 text-xs text-emerald-300';
          preview.innerHTML = t('cron.loadedTargets', { count: targets.length });
        }
      } else if (targets.length === 1) {
        applyTargetToForm('cron', collector, targets[0]);
      } else if (targets.length > 1) {
        // multi-target: keep JSON advanced override so nothing is lost
        document.getElementById('cron-targets').value = JSON.stringify(targets, null, 2);
      }

      const meta = job.schedule_meta || {};
      if (meta.mode === 'preset' && meta.preset) {
        this._applyPresetToForm(meta.preset);
        this._setScheduleMode('preset');
      } else {
        this._setScheduleMode('cron');
      }
      window.openModal && window.openModal('modal-create-cron');
      this._previewSchedule();
    } catch (err) {
      toast(err.message || t('cron.loadFailed'), 'error');
    }
  },

  _applyPresetToForm(preset) {
    const type = preset.type || 'daily';
    document.getElementById('cron-preset-type').value = type;
    if (preset.interval != null) document.getElementById('cron-preset-interval').value = String(preset.interval);
    if (preset.time) document.getElementById('cron-preset-time').value = preset.time;
    if (preset.hour != null && preset.minute != null) {
      document.getElementById('cron-preset-time').value =
        `${String(preset.hour).padStart(2, '0')}:${String(preset.minute).padStart(2, '0')}`;
    }
    if (preset.day_of_month != null) document.getElementById('cron-preset-day').value = String(preset.day_of_month);
    if (preset.minute != null && type === 'hourly') {
      document.getElementById('cron-preset-minute').value = String(preset.minute);
    }
    if (Array.isArray(preset.weekdays)) {
      document.querySelectorAll('#cron-preset-weekdays input').forEach((el) => {
        el.checked = preset.weekdays.includes(el.value);
      });
    }
    this._updatePresetFields();
  },

  async _createJob() {
    const name = document.getElementById('cron-name')?.value.trim() || '';
    const pipelineName = document.getElementById('cron-pipeline')?.value || '';
    const editMode = document.getElementById('cron-edit-mode')?.value === 'edit';
    if (!name || !pipelineName) {
      toast(t('message.cronRequired'), 'error');
      return;
    }
    let taskTemplate;
    try {
      taskTemplate = this._buildTaskTemplate();
    } catch (err) {
      toast(err.message, 'error');
      return;
    }
    const schedule = this._buildSchedulePayload();
    const payload = {
      name,
      pipeline_name: pipelineName,
      schedule,
      cron_expr: schedule.cron_expr || '',
      task_template: taskTemplate,
      enabled: document.getElementById('cron-enabled')?.checked ?? true,
      timezone: schedule.timezone || 'Asia/Shanghai',
      description: document.getElementById('cron-description')?.value.trim() || '',
    };
    try {
      if (editMode) {
        await api(`/cron-jobs/${encodeURIComponent(this._editName || name)}`, {
          method: 'PUT',
          body: JSON.stringify(payload),
        });
        toast(t('message.cronUpdated'), 'success');
      } else {
        await api('/cron-jobs', {
          method: 'POST',
          body: JSON.stringify(payload),
        });
        toast(t('message.cronCreated'), 'success');
      }
      window.closeModal && window.closeModal('modal-create-cron');
      this.refresh();
    } catch (err) {
      toast(t('message.createFailed', { error: err.message }), 'error');
    }
  },

  async _deleteJob(name) {
    if (!confirm(t('confirm.deleteCron', { name }))) return;
    try {
      await api(`/cron-jobs/${encodeURIComponent(name)}?confirm=true`, { method: 'DELETE' });
      toast(t('message.cronDeleted'), 'success');
      this.refresh();
    } catch (err) {
      toast(t('message.deleteFailed', { error: err.message }), 'error');
    }
  },

  async _runJob(name) {
    try {
      const result = await api(`/cron-jobs/${encodeURIComponent(name)}/run`, { method: 'POST' });
      toast(t('cron.triggered', { id: result.task_id || name }), 'success');
    } catch (err) {
      toast(err.message || t('cron.runFailed'), 'error');
    }
  },

  async _toggleJob(name, enabled) {
    try {
      await api(`/cron-jobs/${encodeURIComponent(name)}/enabled`, {
        method: 'PATCH',
        body: JSON.stringify({ enabled }),
      });
      toast(enabled ? t('cron.enabledToast') : t('cron.pausedToast'), 'success');
      this.refresh();
    } catch (err) {
      toast(err.message || t('cron.actionFailed'), 'error');
    }
  },
};

window.loadCronJobs = function () { if (window._cronPage) window._cronPage.refresh(); };
window.showCreateCronModal = function () { if (window._cronPage) window._cronPage._showCreateModal(); };
window.createCronJob = function () { if (window._cronPage) window._cronPage._createJob(); };
window.deleteCronJob = function (name) { if (window._cronPage) window._cronPage._deleteJob(name); };
window.updateCronTargetFields = function () {
  if (window._cronPage) return window._cronPage._updateTargetFields();
};
