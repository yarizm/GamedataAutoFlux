import { api, toast, escapeHtml, formatTime, setValue } from '../../core/api.js';
import { t } from '../../core/i18n.js';

// Page-local state
let dataGames = [];
let dataGroups = [];
let selectedDataGame = null;
window.selectedDataGame = null;
let currentDataRecords = [];
let currentDataPage = 1;
let currentDataPageSize = 20;
let currentDataTotal = 0;
let currentDataSourceFilter = '';
let currentDataSortOrder = 'desc';
let selectedDataRecordKeys = new Set();

function formatDataSummary(summary) {
  const entries = Object.entries(summary || {}).filter(([, v]) => v !== null && v !== undefined && v !== '');
  if (!entries.length) return '-';
  return entries.slice(0, 4).map(([k, v]) => `${k}: ${v}`).join(' | ');
}

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this._state = { selectedGame: selectedDataGame };
    this.refresh();
    return this;
  },

  destroy() {},

  async refresh() {
    await Promise.all([this._loadGames(), this._loadGroups()]);
  },

  async _loadGames() {
    try {
      dataGames = await api('/data/games');
      this._renderGames(dataGames);
      if (selectedDataGame && dataGames.some(g => g.game_key === selectedDataGame.game_key)) {
        await this._selectGame(selectedDataGame.game_key);
      }
    } catch (err) { toast(t('message.loadFailed', { error: err.message }), 'error'); }
  },

  async _loadGroups() {
    try {
      dataGroups = await api('/data/groups');
      const select = document.getElementById('report-group-select');
      if (select) {
        const current = select.value;
        select.innerHTML = `<option value="">${t('reports.importByGroup')}</option>`;
        for (const group of dataGroups) {
          select.insertAdjacentHTML('beforeend',
            `<option value="${escapeHtml(group.group_id)}">${escapeHtml(group.group_name || group.group_id)} (${group.count})</option>`);
        }
        if ([...select.options].some(o => o.value === current)) select.value = current;
      }
    } catch (err) { console.error('Load groups failed:', err); }
  },

  async _search() {
    const q = document.getElementById('data-search-query')?.value.trim() || '';
    if (!q) { this._loadGames(); return; }
    try {
      selectedDataGame = null;
      window.selectedDataGame = null;
      selectedDataRecordKeys.clear();
      const params = new URLSearchParams({ q, page: '1', page_size: String(currentDataPageSize) });
      const result = await api(`/data/records?${params.toString()}`);
      currentDataRecords = result.items;
      currentDataTotal = result.total;
      currentDataPage = result.page;
      const title = document.getElementById('data-records-title');
      const summary = document.getElementById('data-selected-summary');
      if (title) title.textContent = t('data.search');
      if (summary) summary.textContent = t('reports.records', { count: result.total });
      this._renderRecords(currentDataRecords);
      this._renderPagination(result);
    } catch (err) { toast(t('message.loadFailed', { error: err.message }), 'error'); }
  },

  _renderGames(games) {
    const container = document.getElementById('data-games-list');
    if (!container) return;
    if (!games.length) { container.innerHTML = '<p class="text-muted">暂无已落库数据</p>'; return; }

    container.innerHTML = games.map((game) => {
      const activeClass = selectedDataGame?.game_key === game.game_key ? 'active' : '';
      const sourceText = (game.sources || []).map(s => `${s.name} ${s.count}`).join(' / ');
      return `<div class="data-game-item ${activeClass}" role="button" tabindex="0" data-game="${escapeHtml(game.game_key)}">
        <button class="data-game-delete" type="button" title="${t('common.delete')}" data-delete="${escapeHtml(game.game_key)}">${t('common.delete')}</button>
        <span class="data-game-name">${escapeHtml(game.game_name)}</span>
        <span class="data-game-meta">App ID: ${escapeHtml(game.app_id || '-')} | Group: ${escapeHtml(game.group_name || '-')} | ${t('reports.records', { count: game.total_records })}</span>
        <span class="data-game-sources">${escapeHtml(sourceText || t('common.none'))}</span>
      </div>`;
    }).join('');

    container.querySelectorAll('.data-game-item').forEach(el => {
      el.addEventListener('click', () => this._selectGame(el.dataset.game));
      el.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); this._selectGame(el.dataset.game); } });
    });
    container.querySelectorAll('[data-delete]').forEach(btn => {
      btn.addEventListener('click', (e) => { e.stopPropagation(); this._deleteGame(e, btn.dataset.delete); });
    });
  },

  async _selectGame(gameKey) {
    selectedDataGame = dataGames.find(g => g.game_key === gameKey) || null;
    window.selectedDataGame = selectedDataGame;
    this._state = { selectedGame: selectedDataGame };
    currentDataPage = 1;
    currentDataSourceFilter = '';
    selectedDataRecordKeys.clear();
    this._renderGames(dataGames);

    const title = document.getElementById('data-records-title');
    const summary = document.getElementById('data-selected-summary');
    const sourceFilter = document.getElementById('data-source-filter');

    if (!selectedDataGame) {
      if (title) title.textContent = '选择一个游戏';
      if (summary) summary.textContent = '按 App ID 或游戏名聚合已落库 JSON';
      return;
    }
    if (title) title.textContent = selectedDataGame.game_name;
    if (summary) {
      summary._baseText = summary._baseText || summary.textContent;
      const text = `App ID: ${selectedDataGame.app_id || '-'} | ${selectedDataGame.total_records} 条记录 | 最新 ${formatTime(selectedDataGame.latest_stored_at)}`;
      summary._baseText = text;
      summary.textContent = text;
    }
    if (sourceFilter) {
      const current = sourceFilter.value;
      sourceFilter.innerHTML = '<option value="">全部数据源</option>';
      for (const src of selectedDataGame.sources || []) {
        sourceFilter.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(src.name)}">${escapeHtml(src.name)} (${src.count})</option>`);
      }
      sourceFilter.value = [...sourceFilter.options].some(o => o.value === current) ? current : '';
    }
    await this._loadRecords(1);
  },

  async _loadRecords(page) {
    if (!selectedDataGame) return;
    const source = currentDataSourceFilter || document.getElementById('data-source-filter')?.value || '';
    const sortOrder = document.getElementById('data-sort-order')?.value || currentDataSortOrder;
    const pageSize = currentDataPageSize;
    currentDataPage = page;

    const params = new URLSearchParams();
    params.set('page', String(page));
    params.set('page_size', String(pageSize));
    params.set('sort_order', sortOrder);
    if (source) params.set('source', source);
    if (selectedDataGame.app_id) params.set('app_id', selectedDataGame.app_id);

    try {
      const result = await api(`/data/records?${params.toString()}`);
      currentDataRecords = result.items;
      currentDataTotal = result.total;
      currentDataPage = result.page;
      this._renderRecords(currentDataRecords);
      this._renderPagination(result);
    } catch (err) { toast(t('message.loadFailed', { error: err.message }), 'error'); }
  },

  _renderRecords(records) {
    const tbody = document.getElementById('data-records-body');
    if (!tbody) return;
    if (!records.length) { tbody.innerHTML = '<tr><td colspan="6" class="text-muted">该分类下暂无记录</td></tr>'; return; }

    const allChecked = records.length > 0 && records.every(r => selectedDataRecordKeys.has(r.key));
    tbody.innerHTML = records.map((record) => {
      const checked = selectedDataRecordKeys.has(record.key) ? 'checked' : '';
      const comp = record.completeness || 'full';
      const compLabel = { full: '完整', partial: '部分', empty: '空' }[comp] || comp;
      return `<tr>
        <td class="cell-checkbox"><input type="checkbox" class="record-checkbox" data-key="${escapeHtml(record.key)}" ${checked}></td>
        <td><code>${escapeHtml(record.key)}</code></td>
        <td>${escapeHtml(record.data_source)}</td>
        <td><span class="completeness-badge completeness-${comp}" title="数据完整度: ${compLabel}">${compLabel}</span> ${escapeHtml(formatDataSummary(record.summary || {}))}</td>
        <td>${formatTime(record.stored_at)}</td>
        <td><div class="action-buttons">
          <button class="btn btn-ghost btn-sm" data-preview="${escapeHtml(record.key)}">预览</button>
          <button class="btn btn-ghost btn-sm" data-download="${escapeHtml(record.key)}">导出</button>
          <button class="btn btn-ghost btn-sm" data-edit="${escapeHtml(record.key)}">${t('common.edit')}</button>
          <button class="btn btn-ghost btn-sm" data-refresh="${escapeHtml(record.key)}">Update</button>
          <button class="btn btn-ghost btn-sm" data-schedule="${escapeHtml(record.key)}">Schedule</button>
          <button class="btn btn-primary btn-sm" data-report="${escapeHtml(record.key)}">报告</button>
          <button class="btn btn-danger btn-sm" data-delete="${escapeHtml(record.key)}">${t('common.delete')}</button>
        </div></td>
      </tr>`;
    }).join('');

    const selectAll = document.getElementById('data-select-all');
    if (selectAll) { selectAll.checked = allChecked; selectAll.indeterminate = !allChecked && selectedDataRecordKeys.size > 0; }
    this._updateBatchBar();

    tbody.querySelectorAll('.record-checkbox').forEach(cb => {
      cb.addEventListener('change', () => { this._toggleSelect(cb); });
    });
    tbody.querySelectorAll('[data-preview]').forEach(b => b.addEventListener('click', () => this._preview(b.dataset.preview)));
    tbody.querySelectorAll('[data-download]').forEach(b => b.addEventListener('click', () => this._download(b.dataset.download)));
    tbody.querySelectorAll('[data-edit]').forEach(b => b.addEventListener('click', () => this._edit(b.dataset.edit)));
    tbody.querySelectorAll('[data-refresh]').forEach(b => b.addEventListener('click', () => this._refresh(b.dataset.refresh)));
    tbody.querySelectorAll('[data-schedule]').forEach(b => b.addEventListener('click', () => this._schedule(b.dataset.schedule)));
    tbody.querySelectorAll('[data-report]').forEach(b => b.addEventListener('click', () => this._useForReport(b.dataset.report)));
    tbody.querySelectorAll('[data-delete]').forEach(b => b.addEventListener('click', () => this._deleteRecord(b.dataset.delete)));
  },

  _toggleSelect(el) {
    if (el.checked) selectedDataRecordKeys.add(el.dataset.key);
    else selectedDataRecordKeys.delete(el.dataset.key);
    this._updateBatchBar();
  },

  _updateBatchBar() {
    const bar = document.getElementById('data-batch-bar');
    const countEl = document.getElementById('data-batch-count');
    if (!bar) return;
    if (selectedDataRecordKeys.size > 0) { bar.style.display = 'flex'; if (countEl) countEl.textContent = `已选 ${selectedDataRecordKeys.size} 条`; }
    else { bar.style.display = 'none'; }
  },

  async _toggleSelectAll(el) {
    if (el.checked) currentDataRecords.forEach(r => selectedDataRecordKeys.add(r.key));
    else currentDataRecords.forEach(r => selectedDataRecordKeys.delete(r.key));
    this._renderRecords(currentDataRecords);
  },

  _renderPagination(result) {
    let container = document.getElementById('data-pagination');
    if (!container) {
      const table = document.getElementById('data-records-table');
      if (!table) return;
      container = document.createElement('div');
      container.id = 'data-pagination';
      container.className = 'pagination-bar';
      table.parentNode.insertBefore(container, table.nextSibling);
    }
    const totalPages = Math.ceil(result.total / result.page_size);
    if (totalPages <= 1 && result.page_size <= 50) {
      container.innerHTML = `<span class="text-muted">共 ${result.total} 条</span>`;
      return;
    }
    let pageBtns = '';
    const maxVisible = 5;
    let startPage = Math.max(1, result.page - Math.floor(maxVisible / 2));
    let endPage = Math.min(totalPages, startPage + maxVisible - 1);
    if (endPage - startPage < maxVisible - 1) startPage = Math.max(1, endPage - maxVisible + 1);
    for (let i = startPage; i <= endPage; i++) {
      pageBtns += `<button class="btn btn-ghost btn-sm ${i === result.page ? 'active' : ''}" data-goto="${i}">${i}</button>`;
    }
    container.innerHTML = `<div class="pagination-controls">
      <button class="btn btn-ghost btn-sm" data-goto="${result.page - 1}" ${result.page <= 1 ? 'disabled' : ''}>上一页</button>
      ${pageBtns}
      <button class="btn btn-ghost btn-sm" data-goto="${result.page + 1}" ${!result.has_more ? 'disabled' : ''}>下一页</button>
      <span class="text-muted">共 ${result.total} 条</span>
      <select class="page-size-select" id="page-size-select">
        <option value="20" ${result.page_size === 20 ? 'selected' : ''}>20条/页</option>
        <option value="50" ${result.page_size === 50 ? 'selected' : ''}>50条/页</option>
        <option value="100" ${result.page_size === 100 ? 'selected' : ''}>100条/页</option>
        <option value="200" ${result.page_size === 200 ? 'selected' : ''}>200条/页</option>
      </select>
    </div>`;

    container.querySelectorAll('[data-goto]').forEach(btn => {
      btn.addEventListener('click', () => this._goToPage(parseInt(btn.dataset.goto)));
    });
    const psSelect = container.querySelector('#page-size-select');
    if (psSelect) psSelect.addEventListener('change', () => this._changePageSize(psSelect.value));
  },

  _goToPage(page) {
    if (selectedDataGame) { this._loadRecords(page); return; }
    const q = document.getElementById('data-search-query')?.value.trim() || '';
    if (q) {
      const params = new URLSearchParams({ q, page: String(page), page_size: String(currentDataPageSize) });
      api(`/data/records?${params.toString()}`).then(result => {
        currentDataRecords = result.items; currentDataTotal = result.total; currentDataPage = result.page;
        this._renderRecords(currentDataRecords); this._renderPagination(result);
      }).catch(err => toast(`Load failed: ${err.message}`, 'error'));
    }
  },

  _changePageSize(size) { currentDataPageSize = parseInt(size); currentDataPage = 1; this._goToPage(1); },

  // ── Per-record actions ──

  async _preview(key) {
    const preview = document.getElementById('data-preview');
    if (preview) preview.textContent = '加载中...';
    try {
      const detail = await api(`/data/records/${encodeURIComponent(key)}`);
      if (preview) preview.textContent = JSON.stringify(detail, null, 2);
    } catch (err) { if (preview) preview.textContent = `Load failed: ${err.message}`; }
  },

  _download(key) { window.open(`/api/data/records/${encodeURIComponent(key)}/download`, '_blank'); },

  async _edit(key) {
    const record = currentDataRecords.find(r => r.key === key) || {};
    const groupName = prompt('Data group', record.group_name || record.group_id || '');
    if (groupName === null) return;
    const displayName = prompt('Display name', record.display_name || record.game_name || '');
    if (displayName === null) return;
    try {
      const updated = await api(`/data/records/${encodeURIComponent(key)}`, {
        method: 'PATCH', body: JSON.stringify({ group_id: groupName.trim(), group_name: groupName.trim(), display_name: displayName.trim() }),
      });
      toast('Record updated', 'success');
      await this._loadGames();
      if (selectedDataGame) await this._loadRecords(currentDataPage);
      else await this._search();
      this._preview(updated.key);
    } catch (err) { toast(`Update failed: ${err.message}`, 'error'); }
  },

  async _deleteRecord(key) {
    if (!confirm(`Delete data record ${key}?`)) return;
    try {
      await api(`/data/records/${encodeURIComponent(key)}?confirm=true`, { method: 'DELETE' });
      toast('Record deleted', 'success');
      selectedDataRecordKeys.delete(key);
      await this._loadGames();
      this._loadRecords(currentDataPage);
    } catch (err) { toast(`Delete failed: ${err.message}`, 'error'); }
  },

  async _deleteGame(e, gameKey) {
    e?.stopPropagation();
    const game = dataGames.find(g => g.game_key === gameKey);
    if (!game) return;
    const name = game.group_name || game.game_name || game.game_key;
    if (!confirm(`Delete category "${name}" and all related records, vector data, tasks, schedules, and reports?`)) return;
    try {
      const resp = await api(`/data/games/${encodeURIComponent(gameKey)}?confirm=true`, { method: 'DELETE' });
      toast(`Category deleted: ${resp.records_deleted} records`, 'success');
      if (selectedDataGame?.game_key === gameKey) {
        selectedDataGame = null;
        window.selectedDataGame = null;
        this._state = { selectedGame: selectedDataGame };
        currentDataRecords = [];
        this._renderRecords([]);
        const title = document.getElementById('data-records-title');
        const summary = document.getElementById('data-selected-summary');
        if (title) title.textContent = 'Choose a game category';
        if (summary) summary.textContent = '';
      }
      await this._loadGames();
      await this._loadGroups();
      window.loadTasks && window.loadTasks();
      window.loadReports && window.loadReports();
    } catch (err) { toast(`Delete category failed: ${err.message}`, 'error'); }
  },

  async _refresh(key) {
    try {
      const resp = await api(`/data/records/${encodeURIComponent(key)}/refresh`, { method: 'POST', body: JSON.stringify({ rolling_window: true }) });
      toast(`Refresh task submitted: ${resp.task_id}`, 'success');
      window.activateTab && window.activateTab('tasks', this.store);
      window.loadTasks && window.loadTasks();
    } catch (err) { toast(`Refresh failed: ${err.message}`, 'error'); }
  },

  async _schedule(key) {
    const cronExpr = prompt('Cron expression', '0 8 * * *');
    if (!cronExpr) return;
    const name = prompt('Schedule name', `refresh_${key.replace(/[^a-zA-Z0-9_-]+/g, '_').slice(0, 48)}`);
    if (!name) return;
    try {
      await api(`/data/records/${encodeURIComponent(key)}/refresh-schedules`, { method: 'POST', body: JSON.stringify({ name, cron_expr: cronExpr, rolling_window: true }) });
      toast('Refresh schedule created', 'success');
      window.loadCronJobs && window.loadCronJobs();
    } catch (err) { toast(`Schedule failed: ${err.message}`, 'error'); }
  },

  async _useForReport(key) {
    const record = currentDataRecords.find(r => r.key === key);
    await window.addReportRecordSelection?.(key, record);
    await window.syncSelectedReportRecordKeys?.();
    setValue('report-data-source', record?.data_source || '');
    setValue('report-prompt', `基于所选原始 JSON 数据，生成${record?.game_name || '该游戏'}的数据分析报告。`);
    window.activateTab && window.activateTab('reports', this.store);
    toast('已添加 1 条原始 JSON 用于报告', 'success');
  },

  async _batchDelete() {
    if (selectedDataRecordKeys.size === 0) return;
    const keys = Array.from(selectedDataRecordKeys);
    if (!confirm(`确定删除 ${keys.length} 条记录？此操作不可撤销。`)) return;
    try {
      const result = await api('/data/records/batch-delete', { method: 'POST', body: JSON.stringify({ keys, confirm: true }) });
      toast(result.message, 'success');
      selectedDataRecordKeys.clear();
      this._loadRecords(currentDataPage);
    } catch (err) { toast(`Batch delete failed: ${err.message}`, 'error'); }
  },

  async _batchAddToReport() {
    if (selectedDataRecordKeys.size === 0) return;
    let added = 0;
    for (const key of selectedDataRecordKeys) {
      const record = currentDataRecords.find(r => r.key === key);
      if (record) { await window.addReportRecordSelection?.(key, record); added++; }
    }
    if (added > 0) {
      await window.syncSelectedReportRecordKeys?.();
      setValue('report-data-source', selectedDataGame?.game_name || '');
      setValue('report-prompt', `基于 ${selectedDataGame?.game_name || '所选游戏'} 的 ${added} 条数据生成综合分析报告。`);
      window.activateTab && window.activateTab('reports', this.store);
      toast(`已添加 ${added} 条记录用于报告`, 'success');
    }
  },

  async _batchExport() {
    if (selectedDataRecordKeys.size === 0) return;
    try {
      const result = await api('/data/records/batch-export', { method: 'POST', body: JSON.stringify({ keys: Array.from(selectedDataRecordKeys), confirm: false }) });
      const blob = new Blob([JSON.stringify(result, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = `batch_export_${new Date().toISOString().slice(0, 10)}.json`;
      a.click(); URL.revokeObjectURL(url);
      toast(`Exported ${result.count} records`, 'success');
    } catch (err) { toast(`Batch export failed: ${err.message}`, 'error'); }
  },
};

// Global exports
window.loadDataGames = function () { if (window._dataPage) window._dataPage.refresh(); };
window.loadDataGroups = function () { if (window._dataPage) window._dataPage._loadGroups(); };
window.searchDataRecords = function () { if (window._dataPage) window._dataPage._search(); };
window.selectDataGame = function (k) { if (window._dataPage) window._dataPage._selectGame(k); };
window.loadSelectedGameRecords = function (p) { if (window._dataPage) window._dataPage._loadRecords(p); };
window.renderDataRecords = function (r) { if (window._dataPage) window._dataPage._renderRecords(r); };
window.toggleRecordSelect = function (el) { if (window._dataPage) window._dataPage._toggleSelect(el); };
window.toggleSelectAll = function (el) { if (window._dataPage) window._dataPage._toggleSelectAll(el); };
window.previewDataRecord = function (k) { if (window._dataPage) window._dataPage._preview(k); };
window.downloadDataRecord = function (k) { if (window._dataPage) window._dataPage._download(k); };
window.editDataRecord = function (k) { if (window._dataPage) window._dataPage._edit(k); };
window.deleteDataRecord = function (k) { if (window._dataPage) window._dataPage._deleteRecord(k); };
window.deleteDataGame = function (e, k) { if (window._dataPage) window._dataPage._deleteGame(e, k); };
window.refreshDataRecord = function (k) { if (window._dataPage) window._dataPage._refresh(k); };
window.scheduleDataRecordRefresh = function (k) { if (window._dataPage) window._dataPage._schedule(k); };
window.useDataRecordForReport = function (k) { if (window._dataPage) window._dataPage._useForReport(k); };
window.goToPage = function (p) { if (window._dataPage) window._dataPage._goToPage(p); };
window.changePageSize = function (s) { if (window._dataPage) window._dataPage._changePageSize(s); };
window.batchDeleteSelected = function () { if (window._dataPage) window._dataPage._batchDelete(); };
window.batchAddToReport = function () { if (window._dataPage) window._dataPage._batchAddToReport(); };
window.batchExportSelected = function () { if (window._dataPage) window._dataPage._batchExport(); };
