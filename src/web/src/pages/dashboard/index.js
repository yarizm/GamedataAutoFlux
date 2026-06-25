import { api, toast, escapeHtml, escapeJs, formatTime, setText } from '../../core/api.js';
import { renderBadge, renderProgress } from '../../core/api.js';
import { t } from '../../core/i18n.js';

let dashboardChart = null;
let echartsModulePromise = null;
let dashboardResizeHandler = null;

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
    this.refresh();
    return this;
  },

  destroy() {
    if (this._unsub) this._unsub();
    if (dashboardResizeHandler) {
      window.removeEventListener('resize', dashboardResizeHandler);
      dashboardResizeHandler = null;
    }
    if (dashboardChart) { dashboardChart.dispose(); dashboardChart = null; }
  },

  async refresh() {
    try {
      const [stats, components, tasks] = await Promise.all([
        api('/tasks/stats/summary'),
        api('/components'),
        api('/tasks'),
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

      const recentTasks = [...tasks]
        .sort((left, right) => new Date(right.created_at) - new Date(left.created_at))
        .slice(0, 5);
      this._renderRecentTasks(recentTasks);
    } catch (err) {
      console.error('Dashboard refresh failed:', err);
    }
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
    dashboardChart.setOption({
      backgroundColor: 'transparent',
      tooltip: { 
        trigger: 'item',
        backgroundColor: 'rgba(10, 10, 10, 0.9)',
        borderColor: 'rgba(255, 255, 255, 0.1)',
        textStyle: { color: '#d4d4d8' }
      },
      legend: { top: 'bottom', textStyle: { color: '#a1a1aa' } },
      series: [{
        name: t('dashboard.taskDistribution'),
        type: 'pie',
        radius: ['70%', '85%'],
        avoidLabelOverlap: false,
        itemStyle: { borderRadius: 8, borderColor: '#050505', borderWidth: 3 },
        label: { show: false, position: 'center' },
        emphasis: { 
          label: { show: true, fontSize: 18, fontWeight: 'bold', color: '#e4e4e7' },
          itemStyle: { shadowBlur: 15, shadowOffsetX: 0, shadowColor: 'rgba(0, 0, 0, 0.5)' }
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
      tbody.innerHTML = `<tr><td colspan="6" class="text-muted">${t('common.empty.tasks')}</td></tr>`;
      return;
    }
    tbody.innerHTML = tasks.map((task) => `
      <tr>
        <td><code>${task.id}</code></td>
        <td>${escapeHtml(task.name)}</td>
        <td>${renderBadge(task.status)}</td>
        <td>${renderProgress(task.progress)}</td>
        <td>${formatTime(task.created_at)}</td>
        <td>${renderTaskActions(task)}</td>
      </tr>
    `).join('');
  },
};

window.refreshDashboard = function () { if (window._dashboardPage) window._dashboardPage.refresh(); };
