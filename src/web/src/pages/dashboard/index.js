import { api, toast, escapeHtml, escapeJs, formatTime, setText } from '../../core/api.js';
import { renderBadge, renderProgress } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import {
  collectHealthAttentionItems,
  renderFailureLinesHtml,
  summarizeTaskFailure,
} from '../../core/taskFailure.js';
import { renderEmptyState, renderLoadingState } from '../../core/uiState.js';

let dashboardChart = null;
let echartsModulePromise = null;
let dashboardResizeHandler = null;

function chartThemeColors() {
  const light = document.documentElement.dataset.theme === 'light';
  return {
    text: light ? '#64748b' : '#a1a1aa',
    tooltipBg: light ? '#ffffff' : 'rgba(10,10,10,0.9)',
    tooltipBorder: light ? '#d0d7e2' : 'rgba(255,255,255,0.1)',
    tooltipText: light ? '#0f172a' : '#d4d4d8',
    pieBorder: light ? '#ffffff' : '#050505',
    emphasisLabel: light ? '#0f172a' : '#e4e4e7',
  };
}

function renderTaskActions(task) {
  return `<div class="btn-group">
    <button class="btn btn-sm" onclick="viewTaskDetail('${escapeJs(task.id)}')">${t('common.details')}</button>
    <button class="btn btn-sm" onclick="viewTaskLogs('${escapeJs(task.id)}')">${t('common.logs')}</button>
    ${task.status === 'running' ? `<button class="btn btn-sm btn-danger" onclick="cancelTask('${escapeJs(task.id)}')">${t('common.cancel')}</button>` : ''}
    <button class="btn btn-sm btn-danger" onclick="deleteTask('${escapeJs(task.id)}')">${t('common.delete')}</button>
  </div>`;
}

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this._unsub = store.subscribe((key) => {
      if (key === 'refresh' && store.get('activeTab') === 'dashboard') this.refresh();
    });
    this._onTheme = () => { if (dashboardChart) this.refresh(); };
    window.addEventListener('themechange', this._onTheme);
    this.refresh();
    return this;
  },

  destroy() {
    if (this._unsub) this._unsub();
    if (this._onTheme) {
      window.removeEventListener('themechange', this._onTheme);
      this._onTheme = null;
    }
    if (dashboardResizeHandler) {
      window.removeEventListener('resize', dashboardResizeHandler);
      dashboardResizeHandler = null;
    }
    if (dashboardChart) { dashboardChart.dispose(); dashboardChart = null; }
  },

  async refresh() {
    try {
      const [stats, components, tasks, health, diagnostics] = await Promise.all([
        api('/tasks/stats/summary'),
        api('/components'),
        api('/tasks'),
        api('/health').catch(() => null),
        api('/diagnostics/config').catch(() => null),
      ]);

      const counts = stats.status_counts || {};
      setText('stat-total', stats.total_tasks || 0);
      setText('stat-running', counts.running || 0);
      setText('stat-success', counts.success || 0);
      setText('stat-failed', counts.failed || 0);
      setText('stat-cron', stats.cron_jobs || 0);

      const componentCount = Object.values(components).reduce((sum, names) => sum + names.length, 0);
      setText('stat-components', componentCount);

      this._renderChart(stats);

      const orderedTasks = [...tasks]
        .sort((left, right) => new Date(right.created_at) - new Date(left.created_at));
      this._renderRecentTasks(orderedTasks.slice(0, 5));
      this._renderAttention(orderedTasks, health, diagnostics);
    } catch (err) {
      console.error('Dashboard refresh failed:', err);
    }
  },

  _renderAttention(tasks, health, diagnostics) {
    const root = document.getElementById('dashboard-attention');
    if (!root) return;

    const failed = (tasks || [])
      .filter((task) => String(task.status || '').toLowerCase() === 'failed')
      .slice(0, 5)
      .map((task) => ({
        task,
        failure: summarizeTaskFailure(task),
      }));

    const healthItems = collectHealthAttentionItems(health, diagnostics).slice(0, 5);
    if (!failed.length && !healthItems.length) {
      root.classList.add('hidden');
      root.hidden = true;
      root.innerHTML = '';
      return;
    }

    root.hidden = false;
    root.classList.remove('hidden');
    root.setAttribute('aria-label', t('dashboard.attention.title'));

    const failureBlock = failed.length
      ? `<div class="attention-section">
          <div class="attention-section-title">${escapeHtml(t('dashboard.attention.failures'))}</div>
          <ul class="attention-list">
            ${failed.map(({ task, failure }) => {
              const title = failure?.title || t('tasks.failure.unknown');
              return `<li class="attention-item">
                <div class="min-w-0 flex-1">
                  <div class="text-sm font-medium text-theme-primary truncate">${escapeHtml(task.name || task.id)}</div>
                  <div class="text-[11px] text-rose-400/90 truncate" title="${escapeHtml(failure?.raw || title)}">${escapeHtml(title)}</div>
                </div>
                <button type="button" class="btn btn-ghost btn-sm shrink-0" onclick="viewTaskDetail('${escapeJs(task.id)}')">${escapeHtml(t('common.details'))}</button>
              </li>`;
            }).join('')}
          </ul>
        </div>`
      : '';

    const healthBlock = healthItems.length
      ? `<div class="attention-section">
          <div class="attention-section-title flex items-center justify-between gap-2">
            <span>${escapeHtml(t('dashboard.attention.health'))}</span>
            <button type="button" class="btn btn-ghost btn-sm" onclick="activateTab('system')">${escapeHtml(t('dashboard.attention.openSystem'))}</button>
          </div>
          <ul class="attention-list">
            ${healthItems.map((item) => {
              const tone = item.severity === 'warning' ? 'text-amber-400' : 'text-rose-400';
              return `<li class="attention-item">
                <div class="min-w-0 flex-1">
                  <div class="text-sm font-medium text-theme-primary truncate">${escapeHtml(item.name)}
                    <span class="text-[10px] font-mono uppercase ${tone} ml-1">${escapeHtml(item.status)}</span>
                  </div>
                  <div class="text-[11px] text-muted truncate" title="${escapeHtml(item.message)}">${escapeHtml(item.message)}</div>
                </div>
              </li>`;
            }).join('')}
          </ul>
        </div>`
      : '';

    root.innerHTML = `
      <div class="attention-banner-inner">
        <div class="attention-banner-header">
          <span class="attention-dot" aria-hidden="true"></span>
          <h2 class="attention-banner-title">${escapeHtml(t('dashboard.attention.title'))}</h2>
        </div>
        <div class="attention-banner-body">
          ${failureBlock}
          ${healthBlock}
        </div>
      </div>`;
  },

  _renderChart(stats) {
    const chartDom = document.getElementById('dashboard-chart');
    if (!chartDom) return;
    if (!dashboardChart) {
      echartsModulePromise ||= import('../../core/echarts.js');
      echartsModulePromise.then(({ echarts }) => {
        if (!dashboardChart) {
          dashboardChart = echarts.init(chartDom);
          if (!dashboardResizeHandler) {
            dashboardResizeHandler = () => {
              if (dashboardChart) dashboardChart.resize();
            };
            window.addEventListener('resize', dashboardResizeHandler);
          }
          this._setChartOption(stats);
        }
      });
    } else {
      this._setChartOption(stats);
    }
  },

  _setChartOption(stats) {
    if (!dashboardChart) return;
    const counts = stats.status_counts || {};
    const theme = chartThemeColors();
    dashboardChart.setOption({
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'item',
        backgroundColor: theme.tooltipBg,
        borderColor: theme.tooltipBorder,
        textStyle: { color: theme.tooltipText },
      },
      legend: { top: 'bottom', textStyle: { color: theme.text } },
      series: [{
        name: t('dashboard.taskDistribution'),
        type: 'pie',
        radius: ['70%', '85%'],
        avoidLabelOverlap: false,
        itemStyle: { borderRadius: 8, borderColor: theme.pieBorder, borderWidth: 3 },
        label: { show: false, position: 'center' },
        emphasis: {
          label: { show: true, fontSize: 18, fontWeight: 'bold', color: theme.emphasisLabel },
          itemStyle: { shadowBlur: 15, shadowOffsetX: 0, shadowColor: 'rgba(0, 0, 0, 0.5)' },
        },
        labelLine: { show: false },
        data: [
          { value: counts.success || 0, name: t('status.success'), itemStyle: { color: '#10b981', shadowBlur: 10, shadowColor: 'rgba(16, 185, 129, 0.4)' } },
          { value: counts.running || 0, name: t('status.running'), itemStyle: { color: '#06b6d4', shadowBlur: 10, shadowColor: 'rgba(6, 182, 212, 0.4)' } },
          { value: counts.failed || 0, name: t('status.failed'), itemStyle: { color: '#f43f5e', shadowBlur: 10, shadowColor: 'rgba(244, 63, 94, 0.4)' } },
          { value: counts.pending || 0, name: t('status.pending'), itemStyle: { color: '#8b5cf6', shadowBlur: 10, shadowColor: 'rgba(139, 92, 246, 0.4)' } },
        ].filter(item => item.value > 0),
      }],
    });
  },

  _renderRecentTasks(tasks) {
    const tbody = document.getElementById('recent-tasks-body');
    if (!tbody) return;

    if (tasks.length === 0) {
      tbody.innerHTML = renderEmptyState({
        title: t('common.empty.tasks'),
        hint: t('ui.empty.tasksHint'),
        variant: 'table',
        colspan: 6,
        escapeHtml,
        actionHtml: `<button type="button" class="btn btn-primary btn-sm" onclick="showCreateTaskModal()">${escapeHtml(t('tasks.create'))}</button>`,
      });
      return;
    }
    tbody.innerHTML = tasks.map((task) => {
      const failureHtml = renderFailureLinesHtml(summarizeTaskFailure(task), escapeHtml);
      return `
      <tr>
        <td><code>${task.id}</code></td>
        <td>${escapeHtml(task.name)}</td>
        <td>
          <div class="flex flex-col items-start gap-0.5">
            ${renderBadge(task.status)}
            ${failureHtml}
          </div>
        </td>
        <td>${renderProgress(task.progress)}</td>
        <td>${formatTime(task.created_at)}</td>
        <td>${renderTaskActions(task)}</td>
      </tr>`;
    }).join('');
  },
};

window.refreshDashboard = function () { if (window._dashboardPage) window._dashboardPage.refresh(); };
