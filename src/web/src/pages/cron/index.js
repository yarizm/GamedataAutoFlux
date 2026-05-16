import { api, toast, escapeHtml } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { populatePipelineSelect } from '../../core/pipelines.js';

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this.refresh();
    return this;
  },

  destroy() {},

  async refresh() { await this._load(); },

  async _load() {
    try {
      const jobs = await api('/cron-jobs');
      const list = this.container.querySelector('#cron-list');
      if (!list) return;

      if (!jobs.length) {
        list.innerHTML = `<p class="text-muted">${t('cron.empty')}</p>`;
        return;
      }
      list.innerHTML = jobs.map((job) => `
        <div class="cron-item">
          <div class="cron-info">
            <span class="cron-name">${escapeHtml(job.name)}</span>
            <span class="cron-detail">${t('cron.trigger')}: ${escapeHtml(job.trigger)} | ${t('cron.next')}: ${job.next_run || '-'}</span>
          </div>
          <button class="btn btn-danger btn-sm" data-delete="${escapeHtml(job.id)}">${t('common.delete')}</button>
        </div>
      `).join('');

      list.querySelectorAll('[data-delete]').forEach(btn => {
        btn.addEventListener('click', () => this._deleteJob(btn.dataset.delete));
      });
    } catch (err) {
      console.error('Load cron jobs failed:', err);
    }
  },

  _showCreateModal() {
    populatePipelineSelect('cron-pipeline');
    window.openModal && window.openModal('modal-create-cron');
  },

  async _createJob() {
    const name = document.getElementById('cron-name')?.value.trim() || '';
    const pipelineName = document.getElementById('cron-pipeline')?.value || '';
    const cronExpr = document.getElementById('cron-expr')?.value.trim() || '';

    if (!name || !pipelineName || !cronExpr) {
      toast(t('message.cronRequired'), 'error');
      return;
    }
    try {
      await api('/cron-jobs', {
        method: 'POST',
        body: JSON.stringify({ name, pipeline_name: pipelineName, cron_expr: cronExpr }),
      });
      toast(t('message.cronCreated'), 'success');
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
};

window.loadCronJobs = function () { if (window._cronPage) window._cronPage.refresh(); };
window.showCreateCronModal = function () { if (window._cronPage) window._cronPage._showCreateModal(); };
window.createCronJob = function () { if (window._cronPage) window._cronPage._createJob(); };
window.deleteCronJob = function (name) { if (window._cronPage) window._cronPage._deleteJob(name); };
