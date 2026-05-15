import { api, toast, escapeHtml, formatTime, setValue, setText } from '../../core/api.js';

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

function resetReportProgress() { setReportProgress(0, 'queued', 'Report generation queued'); }

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this.refresh();
    return this;
  },

  destroy() {},

  async refresh() {
    await Promise.all([this._loadTemplates(), this._loadReports(), this._loadGroups()]);
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
      if (!reports.length) { container.innerHTML = '<p class="text-muted">No reports</p>'; return; }
      container.innerHTML = reports.map((report) => `
        <div class="report-item">
          <button class="report-item-main" data-view="${report.id}">
            <span class="report-item-title">${escapeHtml(report.title)}</span>
            <span class="report-item-meta">${formatTime(report.generated_at)} | ${escapeHtml(report.template)} | ${report.matched_records} records</span>
          </button>
          <div class="inline-actions">
            <button class="btn btn-ghost btn-sm" data-edit="${report.id}">Edit</button>
            <button class="btn btn-danger btn-sm" data-delete="${report.id}">Delete</button>
          </div>
        </div>`).join('');

      container.querySelectorAll('[data-view]').forEach(b => b.addEventListener('click', () => this._view(b.dataset.view)));
      container.querySelectorAll('[data-edit]').forEach(b => b.addEventListener('click', () => this._edit(b.dataset.edit)));
      container.querySelectorAll('[data-delete]').forEach(b => b.addEventListener('click', () => this._deleteReport(b.dataset.delete)));
    } catch (err) { toast(`Load failed: ${err.message}`, 'error'); }
  },

  async _loadGroups() {
    try {
      const groups = await api('/data/groups');
      const select = document.getElementById('report-group-select');
      if (select) {
        const current = select.value;
        select.innerHTML = '<option value="">-- Select group --</option>';
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
    if (!selectedReportRecordKeys.length) { container.innerHTML = '<p class="text-muted">尚未添加 JSON 数据源</p>'; return; }
    container.innerHTML = selectedReportRecordKeys.map((key) => {
      const meta = selectedReportRecordMeta[key] || {};
      const label = meta.data_source || meta.collector || '手工输入';
      const title = meta.game_name ? `${meta.game_name} / ${label}` : label;
      return `<div class="selected-source-chip">
        <span><strong>${escapeHtml(title)}</strong> <code>${escapeHtml(key)}</code></span>
        <button class="btn btn-ghost btn-sm" type="button" data-remove="${escapeHtml(key)}">移除</button>
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
    const status = missing.length ? `缺少：${missing.map(labelCollector).join(' / ')}` : '已满足已知数据源要求';
    help.innerHTML = `<span>${escapeHtml(template.description)}</span><br><span>必需数据源：${escapeHtml(requirements || '-')}；${escapeHtml(status)}</span>${manualCount ? `<br><span>包含 ${manualCount} 个手工 key，前端无法识别来源，后端生成时会再次校验。</span>` : ''}`;
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
    const missingText = missing.length ? missing.map(labelCollector).join(' / ') : 'None';
    const availableText = available.length ? available.map(c => `${labelCollector(c)}${sourceCounts[c] ? ` (${sourceCounts[c]})` : ''}`).join(' / ') : 'None';
    container.style.display = 'block';
    container.className = `report-precheck report-precheck-${status}`;
    const fillBtns = missing.length ? missing.map(c => `<button class="btn btn-primary btn-sm" data-fill="${c}" style="margin:2px">补采 ${escapeHtml(labelCollector(c))}</button>`).join('') : '';
    container.innerHTML = `
      <div class="report-precheck-title">${escapeHtml(precheck.message || 'Report precheck finished')}</div>
      <div class="report-precheck-grid">
        <span>Records</span><strong>${precheck.usable_records || 0}/${precheck.selected_records || 0}</strong>
        <span>Available</span><strong>${escapeHtml(availableText)}</strong>
        <span>Missing</span><strong>${escapeHtml(missingText)}</strong>
      </div>
      ${fillBtns ? `<div class="report-precheck-actions">${fillBtns}</div>` : ''}
      ${recommendations.length ? `<ul>${recommendations.map(r => `<li>${escapeHtml(r)}</li>`).join('')}</ul>` : ''}`;
    container.querySelectorAll('[data-fill]').forEach(b => b.addEventListener('click', () => this._createFillTask(b.dataset.fill)));
  },

  _createFillTask(collector) {
    const gameName = window.selectedDataGame?.game_name || '';
    const pipelineMap = { steam: 'steam_steamdb', steam_discussions: 'steam_discussions', taptap: 'taptap_basic', gtrends: 'gtrends_weekly', monitor: 'monitor_basic', events: 'events', official_site: 'official_site', qimai: 'qimai' };
    setValue('task-name', gameName ? `${gameName} - ${labelCollector(collector)} 补采` : `${labelCollector(collector)} 补采`);
    setValue('task-target-name', gameName || '');
    setValue('task-pipeline', pipelineMap[collector] || collector);
    if (collector === 'steam' || collector === 'steam_discussions') { const appId = window._dataPage?._state?.selectedGame?.app_id || ''; setValue('task-app-id', appId); if (collector === 'steam_discussions') setValue('task-steam-discussions-app-id', appId); }
    if (collector === 'taptap' || collector === 'monitor' || collector === 'qimai') setValue('task-app-id', window._dataPage?._state?.selectedGame?.app_id || '');
    window.updateTaskTargetFields && window.updateTaskTargetFields();
    window.openModal && window.openModal('modal-create-task');
    window._tasksPage && (window._tasksPage._wizardPrev && window._tasksPage._wizardNext());
  },

  // ── Upload / Import ──

  async _uploadJson() {
    const input = document.getElementById('report-json-files');
    const files = [...(input?.files || [])];
    if (!files.length) { toast('请选择 JSON 文件', 'error'); return; }
    const formData = new FormData();
    for (const file of files) formData.append('files', file);
    try {
      const resp = await fetch('/api/reports/upload-json', { method: 'POST', body: formData });
      if (!resp.ok) { const err = await resp.json().catch(() => ({ detail: resp.statusText })); throw new Error(err.detail || `HTTP ${resp.status}`); }
      const uploaded = await resp.json();
      for (const item of uploaded) this._addRecordSelection(item.key, { collector: item.collector, data_source: labelCollector(normalizeCollector(item.collector)), game_name: item.game_name, app_id: item.app_id });
      this._syncRecordKeys();
      if (input) input.value = '';
      window.loadDataGames && window.loadDataGames();
      toast(`已导入 ${uploaded.length} 个 JSON 数据源`, 'success');
    } catch (err) { toast(`Upload failed: ${err.message}`, 'error'); }
  },

  async _importGroup() {
    const groupId = document.getElementById('report-group-select')?.value || '';
    if (!groupId) { toast('Choose a data group', 'error'); return; }
    try {
      const records = await api(`/reports/group-records?group_id=${encodeURIComponent(groupId)}`);
      for (const record of records) this._addRecordSelection(record.key, record);
      this._syncRecordKeys();
      toast(`Imported ${records.length} records`, 'success');
    } catch (err) { toast(`Import failed: ${err.message}`, 'error'); }
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
    const recordKeysRaw = document.getElementById('report-record-keys')?.value.trim() || '';
    const recordKeys = recordKeysRaw ? recordKeysRaw.split(/\s+/).map(s => s.trim()).filter(Boolean) : [];

    if (!prompt) { toast('Prompt is required', 'error'); return; }

    const payload = { prompt, data_source: dataSource, template, record_keys: recordKeys, params: {} };
    currentReportProgressId = `report_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    resetReportProgress();
    const button = document.getElementById('btn-generate-report');
    if (button) { button.disabled = true; button.textContent = 'Generating...'; }

    try {
      setReportProgress(0.04, 'precheck', 'Checking report data coverage');
      const precheck = await api('/reports/precheck', { method: 'POST', body: JSON.stringify(payload) });
      this._renderPrecheck(precheck);
      if (precheck.status === 'empty') throw new Error(precheck.message || 'No usable report data');
      if (precheck.status === 'partial') {
        const missing = (precheck.missing_collectors || []).map(labelCollector).join(' / ');
        if (!confirm(`Missing data sources: ${missing}. Generate report anyway?`)) {
          setReportProgress(0, 'cancelled', 'Report generation cancelled');
          return;
        }
      }
      setReportProgress(0.08, 'requesting', 'Sending report request');
      payload.params = { progress_id: currentReportProgressId };
      const report = await api('/reports/generate-excel', { method: 'POST', body: JSON.stringify(payload) });
      setReportProgress(1, 'completed', 'Report generated');
      this._renderReport(report);
      this._loadReports();
      toast('Report generated', 'success');
    } catch (err) {
      setReportProgress(1, 'failed', err.message);
      toast(`Generate failed: ${err.message}`, 'error');
    } finally {
      if (button) { button.disabled = false; button.textContent = '生成报告'; }
    }
  },

  _renderReport(report) {
    const container = document.getElementById('report-content');
    if (!container) return;
    let html = `<pre>${escapeHtml(report.content || '')}</pre>`;
    const isExcel = report.metadata?.format === 'excel' || report.metadata?.excel_path;
    if (isExcel) {
      html = `<div style="margin-bottom:1rem;padding:1rem;background:var(--bg-card);border-radius:4px;border:1px solid var(--border)">
        <h4 style="margin:0 0 0.5rem 0;color:var(--success)">📊 Excel 报告已生成</h4>
        <p style="margin:0 0 1rem 0;color:var(--text-muted)">该报告包含了清洗好的表格行、多个工作表以及统计图表。</p>
        <a href="/api/reports/${report.id}/download" class="btn btn-primary" target="_blank" download>⬇️ 下载 Excel 文件</a>
      </div>` + html;
    }
    container.innerHTML = html;
  },

  // ── CRUD ──

  async _view(id) {
    try { const report = await api(`/reports/${id}`); this._renderReport(report); }
    catch (err) { toast(`Load failed: ${err.message}`, 'error'); }
  },

  async _edit(id) {
    try {
      const report = await api(`/reports/${id}`);
      const title = prompt('Report title', report.title || '');
      if (title === null) return;
      const notes = prompt('Notes', report.metadata?.notes || '');
      if (notes === null) return;
      const updated = await api(`/reports/${id}`, { method: 'PATCH', body: JSON.stringify({ title: title.trim(), notes: notes.trim() }) });
      this._renderReport(updated);
      this._loadReports();
      toast('Report updated', 'success');
    } catch (err) { toast(`Edit failed: ${err.message}`, 'error'); }
  },

  async _deleteReport(id) {
    if (!confirm(`Delete report ${id}?`)) return;
    try {
      await api(`/reports/${encodeURIComponent(id)}?confirm=true`, { method: 'DELETE' });
      toast('Report deleted', 'success');
      this._loadReports();
      const container = document.getElementById('report-content');
      if (container) container.textContent = 'No report selected';
    } catch (err) { toast(`Delete failed: ${err.message}`, 'error'); }
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
window.importReportGroupRecords = function () { if (window._reportsPage) window._reportsPage._importGroup(); };
window.useCurrentDataForReport = function () { if (window._reportsPage) window._reportsPage._useCurrentData(); };
window.generateReport = function () { if (window._reportsPage) window._reportsPage._generate(); };
window.renderReport = function (r) { if (window._reportsPage) window._reportsPage._renderReport(r); };
window.viewReport = function (id) { if (window._reportsPage) window._reportsPage._view(id); };
window.editReport = function (id) { if (window._reportsPage) window._reportsPage._edit(id); };
window.deleteReport = function (id) { if (window._reportsPage) window._reportsPage._deleteReport(id); };
window.normalizeCollector = normalizeCollector;
window.labelCollector = labelCollector;
