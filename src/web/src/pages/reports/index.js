import { api, toast, escapeHtml, escapeJs, formatTime, setValue, setText } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { marked } from 'marked';
import DOMPurify from 'dompurify';

function renderSafeMarkdown(content) {
  const text = String(content || '');
  try {
    return DOMPurify.sanitize(marked.parse(text));
  } catch { return escapeHtml(text); }
}

let reportTemplates = [];
let selectedReportRecordKeys = [];
let selectedReportRecordMeta = {};
let currentReportProgressId = null;

function normalizeCollector(value) {
  const normalized = String(value || '').toLowerCase();
  const aliases = { google_trends: 'gtrends', pytrends: 'gtrends', steam_api: 'steam', steamdb: 'steam', firecrawl: 'steam' };
  return aliases[normalized] || normalized;
}

function labelCollector(value) {
  const labels = { steam: 'Steam', taptap: 'TapTap', gtrends: 'Google Trends', monitor: 'Monitor', events: '事件数据', steam_discussions: 'Steam Community Discussions', official_site: '官方网站', qimai: '七麦数据' };
  return labels[value] || value;
}

function setReportProgress(progress, stage, message) {
  const wrapper = document.getElementById('report-progress');
  const fill = document.getElementById('report-progress-fill');
  const percent = document.getElementById('report-progress-percent');
  const stageEl = document.getElementById('report-progress-stage');
  const messageEl = document.getElementById('report-progress-message');
  const value = Math.max(0, Math.min(1, Number(progress) || 0));
  if (wrapper) wrapper.style.display = 'block';
  if (fill) fill.style.width = `${Math.round(value * 100)}%`;
  if (percent) percent.textContent = `${Math.round(value * 100)}%`;
  if (stageEl) stageEl.textContent = stage;
  if (messageEl) messageEl.textContent = message || stage;
}

function resetReportProgress() { setReportProgress(0, 'queued', t('reports.waiting')); }

function formatQualityStatus(status) {
  const labels = { complete: 'Complete', partial: 'Partial', empty: 'Empty', unchecked: 'Unchecked', unknown: 'Unknown' };
  return labels[status] || status;
}

function qualityBadgeClass(status) {
  const classes = {
    complete: 'bg-emerald-500/10 text-emerald-300 border-emerald-500/30',
    partial: 'bg-amber-500/10 text-amber-300 border-amber-500/30',
    empty: 'bg-rose-500/10 text-rose-300 border-rose-500/30',
    unchecked: 'bg-zinc-500/10 text-zinc-300 border-zinc-500/30',
  };
  return classes[status] || 'bg-zinc-500/10 text-zinc-300 border-zinc-500/30';
}

function renderReportQuality(quality, compact = false) {
  if (!quality || typeof quality !== 'object') return '';
  const status = String(quality.quality_status || quality.template_status || '').toLowerCase();
  if (!status && quality.regeneration_recommended == null) return '';
  const label = formatQualityStatus(status || 'unknown');
  const risks = Array.isArray(quality.coverage_risks) ? quality.coverage_risks : [];
  const missing = Array.isArray(quality.missing_collectors) ? quality.missing_collectors : [];
  const summary = String(quality.quality_summary || risks[0] || '');
  const detail = compact
    ? (missing.length ? `${t('reports.missing')}: ${missing.map(labelCollector).join(' / ')}` : summary)
    : summary;
  const action = quality.regeneration_recommended ? t('reports.regenerateRecommended') : '';
  return `
    <div class="mt-2 flex flex-wrap items-center gap-2 text-[11px]" title="${escapeHtml(summary)}">
      <span class="inline-flex items-center h-5 px-2 border ${qualityBadgeClass(status)} font-medium">${escapeHtml(label)}</span>
      ${detail ? `<span class="text-muted truncate max-w-full">${escapeHtml(detail)}</span>` : ''}
      ${action ? `<span class="text-amber-300/90">${escapeHtml(action)}</span>` : ''}
    </div>`;
}

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this._unsub = store.subscribe((key, value) => {
      if (key === 'reportProgress') this._handleReportProgress(value);
    });
    this._initReportProviderSelector();
    this.refresh();
    return this;
  },

  destroy() { if (this._unsub) this._unsub(); },

  async refresh() {
    await Promise.all([this._loadTemplates(), this._loadReports(), this._loadGroups()]);
  },

  _initReportProviderSelector() {
    const select = document.getElementById('report-provider');
    if (!select) return;

    const providers = window._reportProviders || [];

    const providerLabel = (p) => {
      const labels = { qwen: 'Qwen', deepseek: 'DeepSeek', openai: 'OpenAI', local: 'Local', sense: 'SenseNova' };
      return labels[p] || p;
    };

    select.innerHTML = '<option value=\"\">' + t('reports.useDefault', '使用默认') + '</option>' +
      providers.map(p => `<option value=\"${escapeHtml(p.key)}\">${escapeHtml(providerLabel(p.key))} — ${escapeHtml(p.model)}</option>`).join('');

    const saved = localStorage.getItem('report_provider');
    if (saved && [...select.options].some(o => o.value === saved)) {
      select.value = saved;
    }

    select.addEventListener('change', () => {
      localStorage.setItem('report_provider', select.value);
    });
  },

  async _loadTemplates() {
    try {
      reportTemplates = await api('/reports/templates');
      const select = document.getElementById('report-template');
      if (!select) return;
      const current = select.value;
      select.innerHTML = reportTemplates.map(t => `<option value="${escapeHtml(t.id)}">${escapeHtml(t.name)}</option>`).join('');
      if ([...select.options].some(o => o.value === current)) select.value = current;
      this._updateTemplateHelp();
    } catch (err) { console.error('Load report templates failed:', err); }
  },

  async _loadReports() {
    try {
      const reports = await api('/reports');
      const container = document.getElementById('reports-list');
      if (!container) return;
      if (!reports.length) { container.innerHTML = `<p class="text-zinc-600 text-sm">暂无历史记录</p>`; return; }
      container.innerHTML = reports.map((report) => `
        <div class="report-item group flex flex-col p-3 rounded-xl bg-transparent border border-transparent cursor-pointer transition-all duration-300 ease-[cubic-bezier(0.4,0,0.2,1)] relative min-w-0 overflow-hidden mb-1 hover:bg-white/5">
          <div class="flex items-center justify-between">
            <button class="flex-1 text-left min-w-0 pr-4 outline-none" data-view="${report.id}">
              <div class="font-semibold text-theme-primary text-sm mb-1 truncate tracking-tight group-hover:text-violet-400 transition-colors">${escapeHtml(report.title)}</div>
              <div class="text-xs text-zinc-500 truncate mb-0.5 tabular-nums">${formatTime(report.generated_at)} | ${escapeHtml(report.template)} | ${t('reports.records', { count: report.matched_records })}</div>
              ${renderReportQuality(report.quality, true)}
            </button>
            <div class="inline-actions flex gap-2 opacity-0 group-hover:opacity-100 transition-opacity duration-300 shrink-0">
              <button class="btn btn-ghost px-2 h-7 text-xs border border-theme-strong" data-edit="${report.id}">编辑</button>
              <button class="btn btn-danger px-2 h-7 text-xs" data-delete="${report.id}">删除</button>
            </div>
          </div>
          <div class="absolute left-0 top-0 bottom-0 w-[3px] bg-violet-500 shadow-[0_0_10px_rgba(139,92,246,0.8)] opacity-0 group-hover:opacity-100 transition-opacity duration-300"></div>
        </div>`).join('');

      container.querySelectorAll('[data-view]').forEach(b => b.addEventListener('click', () => this._view(b.dataset.view)));
      container.querySelectorAll('[data-edit]').forEach(b => b.addEventListener('click', () => this._edit(b.dataset.edit)));
      container.querySelectorAll('[data-delete]').forEach(b => b.addEventListener('click', () => this._deleteReport(b.dataset.delete)));
    } catch (err) { toast(t('message.loadFailed', { error: err.message }), 'error'); }
  },

  async _loadGroups() {
    try {
      const groups = await api('/data/groups');
      const select = document.getElementById('report-group-select');
      if (select) {
        const current = select.value;
        select.innerHTML = `<option value="">${t('reports.importByGroup')}</option>`;
        for (const g of groups) {
          select.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(g.group_id)}">${escapeHtml(g.group_name || g.group_id)} (${g.count})</option>`);
        }
        if ([...select.options].some(o => o.value === current)) select.value = current;
      }
    } catch (err) { /* ignore */ }
  },

  // ── Record selection ──

  _addRecordSelection(key, meta) {
    if (!selectedReportRecordKeys.includes(key)) selectedReportRecordKeys.push(key);
    if (meta) selectedReportRecordMeta[key] = { key, collector: meta.collector || '', data_source: meta.data_source || meta.collector || '', game_name: meta.game_name || '', app_id: meta.app_id || '' };
  },

  _removeRecordSelection(key) {
    selectedReportRecordKeys = selectedReportRecordKeys.filter(k => k !== key);
    delete selectedReportRecordMeta[key];
    this._syncRecordKeys();
  },

  _clearRecordSelections() {
    selectedReportRecordKeys = [];
    selectedReportRecordMeta = {};
    this._syncRecordKeys();
  },

  _syncRecordKeys() {
    const el = document.getElementById('report-record-keys');
    if (el) el.value = selectedReportRecordKeys.join('\n');
    this._renderSelectedRecords();
    this._updateTemplateHelp();
  },

  _syncFromTextarea() {
    const raw = document.getElementById('report-record-keys')?.value.trim() || '';
    const keys = raw ? raw.split(/\s+/).map(s => s.trim()).filter(Boolean) : [];
    selectedReportRecordKeys = [...new Set(keys)];
    for (const key of Object.keys(selectedReportRecordMeta)) { if (!selectedReportRecordKeys.includes(key)) delete selectedReportRecordMeta[key]; }
    this._renderSelectedRecords();
    this._updateTemplateHelp();
  },

  _renderSelectedRecords() {
    const container = document.getElementById('report-selected-records');
    if (!container) return;
    if (!selectedReportRecordKeys.length) { container.innerHTML = `<p class="text-zinc-600 text-xs italic">Awaiting input data...</p>`; return; }
    container.innerHTML = selectedReportRecordKeys.map((key) => {
      const meta = selectedReportRecordMeta[key] || {};
      const label = meta.data_source || meta.collector || t('reports.manualInput');
      const title = meta.game_name ? `${meta.game_name} / ${label}` : label;
      return `<div class="inline-flex items-center gap-2 px-2.5 py-1 bg-violet-500/10 border border-violet-500/30 rounded-md shadow-[0_0_8px_rgba(139,92,246,0.1)] group transition-all duration-300 hover:bg-violet-500/20 hover:border-violet-400">
        <div class="flex flex-col">
          <span class="text-[10px] font-bold tracking-wider text-violet-300 uppercase">${escapeHtml(title)}</span>
          <code class="text-[10px] text-zinc-400 font-mono">${escapeHtml(key)}</code>
        </div>
        <button class="text-violet-400 hover:text-rose-400 opacity-50 group-hover:opacity-100 transition-all p-0.5 rounded cursor-pointer" type="button" data-remove="${escapeHtml(key)}">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
        </button>
      </div>`;
    }).join('');
    container.querySelectorAll('[data-remove]').forEach(b => b.addEventListener('click', () => this._removeRecordSelection(b.dataset.remove)));
  },

  _updateTemplateHelp() {
    const templateId = document.getElementById('report-template')?.value || '';
    const help = document.getElementById('report-template-help');
    if (!help) return;
    const template = reportTemplates.find(t => t.id === templateId);
    if (!template) { help.textContent = ''; return; }
    const knownCollectors = new Set(Object.values(selectedReportRecordMeta).map(m => normalizeCollector(m.collector)).filter(Boolean));
    const missing = (template.required_collectors || []).filter(c => !knownCollectors.has(c));
    const requirements = (template.required_collectors || []).map(labelCollector).join(' / ');
    const manualCount = selectedReportRecordKeys.filter(k => !selectedReportRecordMeta[k]).length;
    const status = missing.length ? `${t('reports.missing')}: ${missing.map(labelCollector).join(' / ')}` : t('common.ok');
    help.innerHTML = `<span>${escapeHtml(template.description)}</span><br><span>${t('reports.available')}: ${escapeHtml(requirements || '-')} | ${escapeHtml(status)}</span>${manualCount ? `<br><span>${t('reports.records', { count: manualCount })}</span>` : ''}`;
  },

  // ── Precheck ──

  _renderPrecheck(precheck) {
    const container = document.getElementById('report-precheck');
    if (!container || !precheck) return;
    const status = precheck.status || 'unchecked';
    const missing = precheck.missing_collectors || [];
    const available = precheck.available_collectors || [];
    const sourceCounts = precheck.source_counts || {};
    const recommendations = precheck.recommendations || [];
    const missingText = missing.length ? missing.map(labelCollector).join(' / ') : t('common.none');
    const availableText = available.length ? available.map(c => `${labelCollector(c)}${sourceCounts[c] ? ` (${sourceCounts[c]})` : ''}`).join(' / ') : t('common.none');
    container.style.display = 'block';
    container.className = `report-precheck report-precheck-${status}`;
    const fillBtns = missing.length ? missing.map(c => `<button class="btn btn-primary btn-sm" data-fill="${c}" style="margin:2px">${escapeHtml(t('reports.fill', { collector: labelCollector(c) }))}</button>`).join('') : '';
    container.innerHTML = `
      <div class="report-precheck-title">${escapeHtml(precheck.message || t('reports.precheckFinished'))}</div>
      <div class="report-precheck-grid">
        <span>${escapeHtml(t('data.records'))}</span><strong>${precheck.usable_records || 0}/${precheck.selected_records || 0}</strong>
        <span>${escapeHtml(t('reports.available'))}</span><strong>${escapeHtml(availableText)}</strong>
        <span>${escapeHtml(t('reports.missing'))}</span><strong>${escapeHtml(missingText)}</strong>
      </div>
      ${fillBtns ? `<div class="report-precheck-actions">${fillBtns}</div>` : ''}
      ${recommendations.length ? `<ul>${recommendations.map(r => `<li>${escapeHtml(r)}</li>`).join('')}</ul>` : ''}`;
    container.querySelectorAll('[data-fill]').forEach(b => b.addEventListener('click', () => this._createFillTask(b.dataset.fill)));
  },

  async _createFillTask(collector) {
    const gameName = window.selectedDataGame?.game_name || '';
    const pipelineMap = { steam: 'steam_steamdb', steam_discussions: 'steam_discussions', taptap: 'taptap_basic', gtrends: 'gtrends_weekly', monitor: 'monitor_basic', events: 'events', official_site: 'official_site', qimai: 'qimai' };
    await window.showCreateTaskModal?.();
    setValue('task-name', gameName ? `${gameName} - ${labelCollector(collector)} ${t('reports.fill', { collector: '' }).trim()}` : t('reports.fill', { collector: labelCollector(collector) }));
    setValue('task-target-name', gameName || '');
    await window.loadPipelineSelect?.('task-pipeline');
    setValue('task-pipeline', pipelineMap[collector] || collector);
    if (collector === 'steam' || collector === 'steam_discussions') { const appId = window._dataPage?._state?.selectedGame?.app_id || ''; setValue('task-app-id', appId); if (collector === 'steam_discussions') setValue('task-steam-discussions-app-id', appId); }
    if (collector === 'taptap' || collector === 'monitor' || collector === 'qimai') setValue('task-app-id', window._dataPage?._state?.selectedGame?.app_id || '');
    await window.updateTaskTargetFields?.();
  },

  // ── Upload / Import ──

  async _uploadJson() {
    const input = document.getElementById('report-json-files');
    const files = [...(input?.files || [])];
    if (!files.length) { toast(t('message.selectJsonFiles'), 'error'); return; }
    const formData = new FormData();
    for (const file of files) formData.append('files', file);
    try {
        // ... (rest is same, but I'll replace the exact _uploadJson to append my new function too)

      const resp = await fetch('/api/reports/upload-json', { method: 'POST', body: formData });
      if (!resp.ok) { const err = await resp.json().catch(() => ({ detail: resp.statusText })); throw new Error(err.detail || `HTTP ${resp.status}`); }
      const uploaded = await resp.json();
      for (const item of uploaded) this._addRecordSelection(item.key, { collector: item.collector, data_source: labelCollector(normalizeCollector(item.collector)), game_name: item.game_name, app_id: item.app_id });
      this._syncRecordKeys();
      if (input) input.value = '';
      window.loadDataGames && window.loadDataGames();
      toast(t('message.jsonImported', { count: uploaded.length }), 'success');
    } catch (err) { toast(t('message.uploadFailed', { error: err.message }), 'error'); }
  },

  async _importGroup() {
    const groupId = document.getElementById('report-group-select')?.value || '';
    if (!groupId) { toast(t('message.chooseDataGroup'), 'error'); return; }
    try {
      const records = await api(`/reports/group-records?group_id=${encodeURIComponent(groupId)}`);
      for (const record of records) this._addRecordSelection(record.key, record);
      this._syncRecordKeys();
      toast(t('message.recordsImported', { count: records.length }), 'success');
    } catch (err) { toast(t('message.importFailed', { error: err.message }), 'error'); }
  },

  // ── Template Upload / Editor ──

  _uploadTemplate() {
    this._showTemplateModal();
  },

  _showTemplateModal() {
    this._renderCollectorCheckboxes();
    setValue('tmpl-name', '');
    setValue('tmpl-desc', '');
    setValue('tmpl-prompt', '');
    const fileInput = document.getElementById('tmpl-file-input');
    if (fileInput) fileInput.value = '';
    document.querySelectorAll('#tmpl-required-cols input[type="checkbox"]').forEach(cb => cb.checked = false);
    document.querySelectorAll('#tmpl-optional-cols input[type="checkbox"]').forEach(cb => cb.checked = false);
    this._switchTemplateTab('visual');
    openModal('modal-report-template');
  },

  _switchTemplateTab(tab) {
    document.querySelectorAll('[data-tmpl-tab]').forEach(btn => {
      const isActive = btn.dataset.tmplTab === tab;
      btn.classList.toggle('text-violet-400', isActive);
      btn.classList.toggle('border-violet-500', isActive);
      btn.classList.toggle('text-zinc-400', !isActive);
      btn.classList.toggle('border-transparent', !isActive);
    });
    document.getElementById('tmpl-tab-visual').style.display = tab === 'visual' ? 'block' : 'none';
    document.getElementById('tmpl-tab-upload').style.display = tab === 'upload' ? 'block' : 'none';
    const btn = document.getElementById('btn-submit-template');
    if (btn) btn.textContent = t(tab === 'visual' ? 'reports.template.saveTemplate' : 'reports.template.uploadYaml');
  },

  _renderCollectorCheckboxes() {
    const collectorKeys = ['steam', 'taptap', 'gtrends', 'monitor', 'events', 'steam_discussions', 'official_site', 'qimai'];
    const html = collectorKeys.map(val =>
      `<label class="flex items-center gap-2 px-3 py-2 bg-zinc-800 rounded-lg border border-theme-subtle cursor-pointer hover:bg-zinc-700/50 transition-colors">
        <input type="checkbox" value="${escapeHtml(val)}" class="w-4 h-4 rounded border-theme-strong text-violet-500 focus:ring-violet-500/40 bg-theme-elevated">
        <span class="text-xs text-zinc-300">${escapeHtml(labelCollector(val))}</span>
      </label>`
    ).join('');
    document.getElementById('tmpl-required-cols').innerHTML = html;
    document.getElementById('tmpl-optional-cols').innerHTML = html;

    const mutualExclude = (changedGroup, otherGroupId) => {
      document.querySelectorAll(`#${changedGroup} input[type="checkbox"]`).forEach(cb => {
        cb.addEventListener('change', () => {
          if (cb.checked) {
            const other = document.querySelector(`#${otherGroupId} input[value="${escapeJs(cb.value)}"]`);
            if (other) other.checked = false;
          }
        });
      });
    };
    mutualExclude('tmpl-required-cols', 'tmpl-optional-cols');
    mutualExclude('tmpl-optional-cols', 'tmpl-required-cols');
  },

  _submitVisualTemplate() {
    const name = document.getElementById('tmpl-name')?.value.trim();
    if (!name) { toast(t('message.templateEmptyName'), 'error'); return; }

    const id = name.toLowerCase()
      .replace(/[^a-z0-9一-鿿]+/g, '_')
      .replace(/^_|_$/g, '')
      .replace(/_+/g, '_');
    if (!id) { toast(t('message.templateInvalidId'), 'error'); return; }

    const required = [...document.querySelectorAll('#tmpl-required-cols input:checked')].map(cb => cb.value);
    const optional = [...document.querySelectorAll('#tmpl-optional-cols input:checked')].map(cb => cb.value);
    const description = document.getElementById('tmpl-desc')?.value.trim() || '';
    const promptInstruction = document.getElementById('tmpl-prompt')?.value.trim() || '';

    const payload = {
      name,
      description,
      required_collectors: required,
      optional_collectors: optional,
      prompt_instruction: promptInstruction,
    };

    api(`/reports/templates/${encodeURIComponent(id)}`, {
      method: 'POST',
      body: JSON.stringify(payload),
    }).then(() => {
      toast(t('message.templateSaved', { name }), 'success');
      closeModal('modal-report-template');
      this._loadTemplates();
    }).catch(err => {
      toast(t('message.templateSaveFailed', { error: err.message }), 'error');
    });
  },

  _submitFileTemplate() {
    const input = document.getElementById('tmpl-file-input');
    const file = input?.files?.[0];
    if (!file) { toast(t('message.templateNoFile'), 'error'); return; }

    const formData = new FormData();
    formData.append('file', file);

    api('/reports/templates/upload', {
      method: 'POST',
      body: formData,
    }).then(result => {
      toast(t('message.templateUploaded', { name: result.name || result.id }), 'success');
      closeModal('modal-report-template');
      this._loadTemplates();
    }).catch(err => {
      toast(t('message.templateUploadFailed', { error: err.message }), 'error');
    });
  },

  _useCurrentData() {
    // Delegated to window functions (set from data page)
    if (window._dataPage && window._dataPage._batchAddToReport) {
      window._dataPage._batchAddToReport();
    }
  },

  // ── Generate ──

  async _generate() {
    this._syncFromTextarea();
    const prompt = document.getElementById('report-prompt')?.value.trim() || '';
    const dataSource = document.getElementById('report-data-source')?.value.trim() || '';
    const template = document.getElementById('report-template')?.value || 'default';
    const provider = document.getElementById('report-provider')?.value || '';
    const recordKeysRaw = document.getElementById('report-record-keys')?.value.trim() || '';
    const recordKeys = recordKeysRaw ? recordKeysRaw.split(/\s+/).map(s => s.trim()).filter(Boolean) : [];

    if (!prompt) { toast(t('message.promptRequired'), 'error'); return; }

    const payload = { prompt, data_source: dataSource, template, provider, record_keys: recordKeys, params: {} };
    currentReportProgressId = `report_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    resetReportProgress();
    const button = document.getElementById('btn-generate-report');
    if (button) { button.disabled = true; button.textContent = t('reports.generate') + '...'; }

    try {
      setReportProgress(0.04, 'precheck', t('reports.precheckFinished'));
      const precheck = await api('/reports/precheck', { method: 'POST', body: JSON.stringify(payload) });
      this._renderPrecheck(precheck);
      if (precheck.status === 'empty') throw new Error(precheck.message || 'No usable report data');
      if (precheck.status === 'partial') {
        const missing = (precheck.missing_collectors || []).map(labelCollector).join(' / ');
        if (!confirm(t('confirm.missingSources', { missing }))) {
          setReportProgress(0, 'cancelled', t('common.cancel'));
          return;
        }
      }
      setReportProgress(0.08, 'requesting', t('reports.generate'));
      payload.params = { progress_id: currentReportProgressId };
      const report = await api('/reports/generate-excel', { method: 'POST', body: JSON.stringify(payload) });
      setReportProgress(1, 'completed', t('message.reportGenerated'));
      this._renderReport(report);
      this._loadReports();
      toast(t('message.reportGenerated'), 'success');
    } catch (err) {
      setReportProgress(1, 'failed', err.message);
      toast(t('message.generateFailed', { error: err.message }), 'error');
    } finally {
      if (button) { button.disabled = false; button.textContent = t('reports.generate'); }
    }
  },

  _handleReportProgress(event) {
    if (!event || event.progress_id !== currentReportProgressId) return;
    setReportProgress(event.progress || 0, event.stage || 'running', event.message || '');
  },

  _renderReport(report) {
    const container = document.getElementById('report-content');
    if (!container) return;
    let html = `${renderReportQuality(report.quality)}<div class="markdown-body">${renderSafeMarkdown(report.content || '')}</div>`;
    const isExcel = report.metadata?.format === 'excel' || report.metadata?.excel_path;
    if (isExcel) {
      html = `<div style="margin-bottom:1rem;padding:1rem;background:var(--bg-card);border-radius:4px;border:1px solid var(--border)">
        <h4 style="margin:0 0 0.5rem 0;color:var(--success)">📊 ${t('reports.excelGenerated')}</h4>
        <p style="margin:0 0 1rem 0;color:var(--text-muted)">${t('reports.excelHelp')}</p>
        <a href="/api/reports/${report.id}/download" class="btn btn-primary" target="_blank" download>${t('reports.downloadExcel')}</a>
      </div>` + html;
    }
    container.innerHTML = html;
  },

  // ── CRUD ──

  async _view(id) {
    try { const report = await api(`/reports/${id}`); this._renderReport(report); }
    catch (err) { toast(t('message.loadFailed', { error: err.message }), 'error'); }
  },

  async _edit(id) {
    try {
      const report = await api(`/reports/${id}`);
      const title = prompt(t('prompt.reportTitle'), report.title || '');
      if (title === null) return;
      const notes = prompt(t('prompt.notes'), report.metadata?.notes || '');
      if (notes === null) return;
      const updated = await api(`/reports/${id}`, { method: 'PATCH', body: JSON.stringify({ title: title.trim(), notes: notes.trim() }) });
      this._renderReport(updated);
      this._loadReports();
      toast(t('message.reportUpdated'), 'success');
    } catch (err) { toast(t('message.editFailed', { error: err.message }), 'error'); }
  },

  async _deleteReport(id) {
    if (!confirm(t('confirm.deleteReport', { id }))) return;
    try {
      await api(`/reports/${encodeURIComponent(id)}?confirm=true`, { method: 'DELETE' });
      toast(t('message.reportDeleted'), 'success');
      this._loadReports();
      const container = document.getElementById('report-content');
      if (container) container.textContent = t('common.noSelection.report');
    } catch (err) { toast(t('message.deleteFailed', { error: err.message }), 'error'); }
  },
};

window.loadReportTemplates = function () { if (window._reportsPage) window._reportsPage._loadTemplates(); };
window.loadReports = function () { if (window._reportsPage) window._reportsPage._loadReports(); };
window.updateReportTemplateHelp = function () { if (window._reportsPage) window._reportsPage._updateTemplateHelp(); };
window.addReportRecordSelection = function (k, m) { if (window._reportsPage) window._reportsPage._addRecordSelection(k, m); };
window.removeReportRecordSelection = function (k) { if (window._reportsPage) window._reportsPage._removeRecordSelection(k); };
window.clearSelectedReportRecords = function () { if (window._reportsPage) window._reportsPage._clearRecordSelections(); };
window.syncSelectedReportRecordKeys = function () { if (window._reportsPage) window._reportsPage._syncRecordKeys(); };
window.syncReportRecordKeysFromTextarea = function () { if (window._reportsPage) window._reportsPage._syncFromTextarea(); };
window.renderSelectedReportRecords = function () { if (window._reportsPage) window._reportsPage._renderSelectedRecords(); };
window.renderReportPrecheck = function (p) { if (window._reportsPage) window._reportsPage._renderPrecheck(p); };
window.createFillTaskFromPrecheck = function (c) { if (window._reportsPage) window._reportsPage._createFillTask(c); };
window.uploadReportJsonFiles = function () { if (window._reportsPage) window._reportsPage._uploadJson(); };
window.uploadReportTemplate = function () { if (window._reportsPage) window._reportsPage._uploadTemplate(); };
window.switchReportTemplateTab = function (tab) { if (window._reportsPage) window._reportsPage._switchTemplateTab(tab); };
window.submitReportTemplate = function () { if (window._reportsPage) { const activeTab = document.getElementById('tmpl-tab-upload').style.display === 'none' ? 'visual' : 'upload'; if (activeTab === 'visual') window._reportsPage._submitVisualTemplate(); else window._reportsPage._submitFileTemplate(); } };
window.importReportGroupRecords = function () { if (window._reportsPage) window._reportsPage._importGroup(); };
window.useCurrentDataForReport = function () { if (window._reportsPage) window._reportsPage._useCurrentData(); };
window.generateReport = function () { if (window._reportsPage) window._reportsPage._generate(); };
window.renderReport = function (r) { if (window._reportsPage) window._reportsPage._renderReport(r); };
window.viewReport = function (id) { if (window._reportsPage) window._reportsPage._view(id); };
window.editReport = function (id) { if (window._reportsPage) window._reportsPage._edit(id); };
window.deleteReport = function (id) { if (window._reportsPage) window._reportsPage._deleteReport(id); };
window.normalizeCollector = normalizeCollector;
window.labelCollector = labelCollector;
