import './style.css';
import { api, toast, escapeHtml, formatTime, setValue } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { renderEmptyState } from '../../core/uiState.js';

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
    if (!games.length) {
      container.innerHTML = renderEmptyState({
        title: t('data.emptyStored'),
        hint: t('ui.empty.dataHint'),
        variant: 'compact',
        escapeHtml,
        actionHtml: `<button type="button" class="btn btn-primary btn-sm" onclick="showCreateTaskModal()">${escapeHtml(t('tasks.create'))}</button>`,
      });
      return;
    }

    container.innerHTML = games.map((game) => {
      const activeClass = selectedDataGame?.game_key === game.game_key ? 'active' : '';
      const sourceText = (game.sources || []).map(s => `${s.name} ${s.count}`).join(' / ');
      const metaText = `App ID: ${game.app_id || '-'} · ${t('data.group')}: ${game.group_name || '-'} · ${t('reports.records', { count: game.total_records })}`;
      const sources = sourceText || t('common.none');
      return `<div class="data-game-item ${activeClass}" role="button" tabindex="0" data-game="${escapeHtml(game.game_key)}">
        <div class="data-game-body">
          <div class="data-game-name" title="${escapeHtml(game.game_name)}">${escapeHtml(game.game_name)}</div>
          <div class="data-game-meta" title="${escapeHtml(metaText)}">${escapeHtml(metaText)}</div>
          <div class="data-game-sources" title="${escapeHtml(sources)}">${escapeHtml(sources)}</div>
        </div>
        <button class="data-game-delete" type="button" title="${t('common.delete')}" aria-label="${t('common.delete')}" data-delete="${escapeHtml(game.game_key)}">×</button>
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
      if (title) title.textContent = t('data.chooseGame');
      if (summary) summary.textContent = t('data.summary');
      return;
    }
    if (title) title.textContent = selectedDataGame.game_name;
    if (summary) {
      const text = `App ID: ${selectedDataGame.app_id || '-'} | ${t('reports.records', { count: selectedDataGame.total_records })} | ${t('data.latest')} ${formatTime(selectedDataGame.latest_stored_at)}`;
      summary._baseText = text;
      summary.textContent = text;
    }
    if (sourceFilter) {
      const current = sourceFilter.value;
      sourceFilter.innerHTML = `<option value="">${escapeHtml(t('data.allSources'))}</option>`;
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

    try {
      // Use game_key endpoint — filters correctly by computed identity, not stale DB columns
      const gk = encodeURIComponent(selectedDataGame.game_key);
      const params = new URLSearchParams();
      params.set('page', String(page));
      params.set('page_size', String(pageSize));
      params.set('sort_order', sortOrder);
      if (source) params.set('source', source);
      const result = await api(`/data/games/${gk}/records?${params.toString()}`);
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
    if (!records.length) {
      tbody.innerHTML = renderEmptyState({
        title: t('data.noRecordsInCategory'),
        hint: t('ui.empty.dataHint'),
        variant: 'table',
        colspan: 6,
        escapeHtml,
      });
      return;
    }

    const allChecked = records.length > 0 && records.every(r => selectedDataRecordKeys.has(r.key));
    tbody.innerHTML = records.map((record) => {
      const checked = selectedDataRecordKeys.has(record.key) ? 'checked' : '';
      const comp = record.completeness || 'full';
      const compLabel = { full: t('data.completeness.full'), partial: t('data.completeness.partial'), empty: t('data.completeness.empty') }[comp] || comp;
      return `<tr class="group">
        <td class="cell-checkbox"><input type="checkbox" class="record-checkbox cyber-checkbox" data-key="${escapeHtml(record.key)}" ${checked}></td>
        <td class="max-w-[150px] truncate" title="${escapeHtml(record.key)}"><code>${escapeHtml(record.key)}</code></td>
        <td>${escapeHtml(record.data_source)}</td>
        <td class="max-w-[300px] truncate" title="${escapeHtml(formatDataSummary(record.summary || {}))}"><span class="completeness-badge completeness-${comp}" title="${escapeHtml(t('data.completenessTitle'))}: ${escapeHtml(compLabel)}">${escapeHtml(compLabel)}</span> ${escapeHtml(formatDataSummary(record.summary || {}))}</td>
        <td>${formatTime(record.stored_at)}</td>
        <td><div class="action-buttons flex gap-2 opacity-30 group-hover:opacity-100 transition-opacity duration-300">
          <button class="btn btn-ghost btn-sm" data-preview="${escapeHtml(record.key)}">${escapeHtml(t('data.preview'))}</button>
          <button class="btn btn-ghost btn-sm" data-download="${escapeHtml(record.key)}">${escapeHtml(t('data.export'))}</button>
          <button class="btn btn-ghost btn-sm" data-edit="${escapeHtml(record.key)}">${escapeHtml(t('common.edit'))}</button>
          <button class="btn btn-ghost btn-sm" data-refresh="${escapeHtml(record.key)}">${escapeHtml(t('common.update'))}</button>
          <button class="btn btn-ghost btn-sm" data-schedule="${escapeHtml(record.key)}">${escapeHtml(t('common.schedule'))}</button>
          <button class="btn btn-primary btn-sm" data-report="${escapeHtml(record.key)}">${escapeHtml(t('data.report'))}</button>
          <button class="btn btn-danger btn-sm" data-delete="${escapeHtml(record.key)}">${escapeHtml(t('common.delete'))}</button>
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
    if (selectedDataRecordKeys.size > 0) {
      bar.style.display = 'flex';
      if (countEl) countEl.textContent = t('data.selectedCount', { count: selectedDataRecordKeys.size });
    } else {
      bar.style.display = 'none';
    }
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
      container.innerHTML = `<span class="text-muted">${escapeHtml(t('data.totalCount', { count: result.total }))}</span>`;
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
    const perPage = (n) => escapeHtml(t('data.perPage', { size: n }));
    container.innerHTML = `<div class="pagination-controls">
      <button class="btn btn-ghost btn-sm" data-goto="${result.page - 1}" ${result.page <= 1 ? 'disabled' : ''}>${escapeHtml(t('data.pagePrev'))}</button>
      ${pageBtns}
      <button class="btn btn-ghost btn-sm" data-goto="${result.page + 1}" ${!result.has_more ? 'disabled' : ''}>${escapeHtml(t('data.pageNext'))}</button>
      <span class="text-muted">${escapeHtml(t('data.totalCount', { count: result.total }))}</span>
      <select class="page-size-select" id="page-size-select">
        <option value="20" ${result.page_size === 20 ? 'selected' : ''}>${perPage(20)}</option>
        <option value="50" ${result.page_size === 50 ? 'selected' : ''}>${perPage(50)}</option>
        <option value="100" ${result.page_size === 100 ? 'selected' : ''}>${perPage(100)}</option>
        <option value="200" ${result.page_size === 200 ? 'selected' : ''}>${perPage(200)}</option>
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
      }).catch(err => toast(t('message.loadFailed', { error: err.message }), 'error'));
    }
  },

  _changePageSize(size) { currentDataPageSize = parseInt(size); currentDataPage = 1; this._goToPage(1); },

  // ── Per-record actions ──

  async _preview(key) {
    const preview = document.getElementById('data-preview');
    if (preview) preview.textContent = t('common.loading');
    try {
      const detail = await api(`/data/records/${encodeURIComponent(key)}`);
      if (preview) preview.textContent = JSON.stringify(detail, null, 2);
    } catch (err) { if (preview) preview.textContent = t('message.loadFailed', { error: err.message }); }
  },

  _download(key) { window.open(`/api/data/records/download?key=${encodeURIComponent(key)}`, '_blank'); },

  async _edit(key) {
    const record = currentDataRecords.find(r => r.key === key) || {};
    const groupName = prompt(t('prompt.dataGroup'), record.group_name || record.group_id || '');
    if (groupName === null) return;
    const displayName = prompt(t('prompt.displayName'), record.display_name || record.game_name || '');
    if (displayName === null) return;
    try {
      const updated = await api(`/data/records/${encodeURIComponent(key)}`, {
        method: 'PATCH', body: JSON.stringify({ group_id: groupName.trim(), group_name: groupName.trim(), display_name: displayName.trim() }),
      });
      toast(t('message.recordUpdated'), 'success');
      await this._loadGames();
      if (selectedDataGame) await this._loadRecords(currentDataPage);
      else await this._search();
      this._preview(updated.key);
    } catch (err) { toast(t('message.editFailed', { error: err.message }), 'error'); }
  },

  async _deleteRecord(key) {
    if (!confirm(t('confirm.deleteRecord', { key }))) return;
    try {
      await api(`/data/records/${encodeURIComponent(key)}?confirm=true`, { method: 'DELETE' });
      toast(t('message.recordDeleted'), 'success');
      selectedDataRecordKeys.delete(key);
      await this._loadGames();
      this._loadRecords(currentDataPage);
    } catch (err) { toast(t('message.deleteFailed', { error: err.message }), 'error'); }
  },

  async _deleteGame(e, gameKey) {
    e?.stopPropagation();
    const game = dataGames.find(g => g.game_key === gameKey);
    if (!game) return;
    const name = game.group_name || game.game_name || game.game_key;
    if (!confirm(t('confirm.deleteCategory', { name }))) return;
    try {
      const resp = await api(`/data/games/${encodeURIComponent(gameKey)}?confirm=true`, { method: 'DELETE' });
      toast(t('message.categoryDeleted', { count: resp.records_deleted }), 'success');
      if (selectedDataGame?.game_key === gameKey) {
        selectedDataGame = null;
        window.selectedDataGame = null;
        this._state = { selectedGame: selectedDataGame };
        currentDataRecords = [];
        this._renderRecords([]);
        const title = document.getElementById('data-records-title');
        const summary = document.getElementById('data-selected-summary');
        if (title) title.textContent = t('data.chooseCategory');
        if (summary) summary.textContent = '';
      }
      await this._loadGames();
      await this._loadGroups();
      window.loadTasks && window.loadTasks();
      window.loadReports && window.loadReports();
    } catch (err) { toast(t('message.deleteCategoryFailed', { error: err.message }), 'error'); }
  },

  async _refresh(key) {
    try {
      const resp = await api(`/data/records/refresh?key=${encodeURIComponent(key)}`, { method: 'POST', body: JSON.stringify({ rolling_window: true }) });
      toast(t('message.refreshSubmitted', { taskId: resp.task_id }), 'success');
      window.activateTab && window.activateTab('tasks', this.store);
      window.loadTasks && window.loadTasks();
    } catch (err) { toast(t('message.refreshFailed', { error: err.message }), 'error'); }
  },

  async _schedule(key) {
    const cronExpr = prompt(t('prompt.cronExpression'), '0 8 * * *');
    if (!cronExpr) return;
    const name = prompt(t('prompt.scheduleName'), `refresh_${key.replace(/[^a-zA-Z0-9_-]+/g, '_').slice(0, 48)}`);
    if (!name) return;
    try {
      await api(`/data/records/refresh-schedules?key=${encodeURIComponent(key)}`, { method: 'POST', body: JSON.stringify({ name, cron_expr: cronExpr, rolling_window: true }) });
      toast(t('message.scheduleCreated'), 'success');
      window.loadCronJobs && window.loadCronJobs();
    } catch (err) { toast(t('message.scheduleFailed', { error: err.message }), 'error'); }
  },

  async _useForReport(key) {
    const record = currentDataRecords.find(r => r.key === key);
    await window.addReportRecordSelection?.(key, record);
    await window.syncSelectedReportRecordKeys?.();
    setValue('report-data-source', record?.data_source || '');
    setValue('report-prompt', t('data.reportPromptSingle', { game: record?.game_name || t('data.thisGame') }));
    window.activateTab && window.activateTab('reports', this.store);
    toast(t('message.addedToReport', { count: 1 }), 'success');
  },

  async _batchDelete() {
    if (selectedDataRecordKeys.size === 0) return;
    const keys = Array.from(selectedDataRecordKeys);
    if (!confirm(t('confirm.batchDeleteRecords', { count: keys.length }))) return;
    try {
      const result = await api('/data/records/batch-delete', { method: 'POST', body: JSON.stringify({ keys, confirm: true }) });
      toast(result.message, 'success');
      selectedDataRecordKeys.clear();
      this._loadRecords(currentDataPage);
    } catch (err) { toast(t('message.batchDeleteFailed', { error: err.message }), 'error'); }
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
      setValue('report-prompt', t('data.reportPromptBatch', {
        game: selectedDataGame?.game_name || t('data.selectedGame'),
        count: added,
      }));
      window.activateTab && window.activateTab('reports', this.store);
      toast(t('message.addedToReport', { count: added }), 'success');
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
      toast(t('message.exportedRecords', { count: result.count }), 'success');
    } catch (err) { toast(t('message.batchExportFailed', { error: err.message }), 'error'); }
  },

  async _batchExportXlsx() {
    if (selectedDataRecordKeys.size === 0) return;
    const taskIds = new Set();
    const collectors = new Set();
    for (const key of selectedDataRecordKeys) {
      const record = currentDataRecords.find(r => r.key === key);
      if (record?.task_id) taskIds.add(record.task_id);
      else {
        // Fallback for legacy records: key format is task_id:source:index.
        const parts = key.split(':');
        if (parts.length >= 1) taskIds.add(parts[0]);
      }
      if (record?.collector) collectors.add(record.collector);
    }
    if (collectors.size !== 1) { toast(t('message.youtubeSingleSource'), 'error'); return; }
    const collector = Array.from(collectors)[0] || '';
    if (!['youtube_profiles', 'youtube_comments'].includes(collector)) {
      toast(t('message.youtubeXlsxOnly'), 'error');
      return;
    }
    try {
      const resp = await api('/data/export/youtube', {
        method: 'POST',
        body: JSON.stringify({
          collector: collector,
          task_ids: Array.from(taskIds),
          format: 'xlsx',
        }),
      });
      if (resp.download_url) {
        window.open(resp.download_url, '_blank');
        toast(t('message.exportedRecords', { count: resp.record_count }), 'success');
      }
    } catch (err) { toast(t('message.xlsxExportFailed', { error: err.message }), 'error'); }
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
window.batchExportXlsx = function () { if (window._dataPage) window._dataPage._batchExportXlsx(); };
