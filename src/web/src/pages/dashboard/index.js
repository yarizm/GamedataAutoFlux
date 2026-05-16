import { api, toast, escapeHtml, escapeJs, formatTime, setText } from '../../core/api.js';
import { renderBadge, renderProgress } from '../../core/api.js';
import { t } from '../../core/i18n.js';

let dashboardChart = null;

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
      import('echarts').then((echarts) => {
        if (!dashboardChart) {
          dashboardChart = echarts.init(chartDom);
          window.addEventListener('resize', () => dashboardChart && dashboardChart.resize());
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
      tooltip: { trigger: 'item' },
      legend: { top: 'bottom' },
      series: [{
        name: t('dashboard.taskDistribution'),
        type: 'pie',
        radius: ['40%', '70%'],
        avoidLabelOverlap: false,
        itemStyle: { borderRadius: 10, borderColor: '#fff', borderWidth: 2 },
        label: { show: false, position: 'center' },
        emphasis: { label: { show: true, fontSize: 20, fontWeight: 'bold' } },
        labelLine: { show: false },
        data: [
          { value: counts.success || 0, name: t('status.success'), itemStyle: { color: '#10b981' } },
          { value: counts.running || 0, name: t('status.running'), itemStyle: { color: '#3b82f6' } },
          { value: counts.failed || 0, name: t('status.failed'), itemStyle: { color: '#ef4444' } },
          { value: counts.pending || 0, name: t('status.pending'), itemStyle: { color: '#f59e0b' } },
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
