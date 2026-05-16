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
        <div class="cron-item group flex items-center justify-between p-4 rounded-xl bg-[#0a0a0a] border border-white/5 transition-all duration-300 hover:bg-white/5 hover:border-white/10 mb-3 relative overflow-hidden">
          <div class="flex items-center gap-4 flex-1 min-w-0">
            <div class="w-10 h-10 rounded-lg bg-amber-500/10 border border-amber-500/20 flex items-center justify-center text-amber-400 shrink-0 shadow-[0_0_10px_rgba(245,158,11,0.1)]">
              <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
            </div>
            <div class="flex-1 min-w-0">
              <div class="font-bold text-zinc-100 text-sm tracking-tight mb-1 truncate">${escapeHtml(job.name)}</div>
              <div class="flex flex-wrap items-center gap-x-4 gap-y-1">
                <div class="flex items-center gap-1.5">
                  <span class="text-[10px] font-bold text-zinc-600 uppercase tracking-widest">${t('cron.trigger')}</span>
                  <code class="text-[11px] text-zinc-300 font-mono bg-[#111] px-1.5 py-0.5 rounded border border-white/5">${escapeHtml(job.trigger)}</code>
                </div>
                <div class="flex items-center gap-1.5">
                  <span class="text-[10px] font-bold text-zinc-600 uppercase tracking-widest">${t('cron.next')}</span>
                  <span class="text-[11px] text-amber-400/80 font-mono tabular-nums">${job.next_run || '-'}</span>
                </div>
              </div>
            </div>
          </div>
          <div class="absolute left-0 top-0 bottom-0 w-[3px] bg-amber-500 shadow-[0_0_10px_rgba(245,158,11,0.8)] opacity-0 group-hover:opacity-100 transition-opacity duration-300"></div>
          <button class="btn btn-danger h-8 px-3 text-xs opacity-0 group-hover:opacity-100 transition-all duration-300 shadow-[0_0_10px_rgba(244,63,94,0.2)]" data-delete="${escapeHtml(job.id)}">${t('common.delete')}</button>
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
