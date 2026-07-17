import { api, toast, escapeHtml, setValue, setChecked } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { renderEmptyState } from '../../core/uiState.js';
import {
  getCachedPipelineTemplates,
  invalidatePipelineCache,
  loadAvailablePipelines,
  loadPipelineTemplates,
  loadPipelines,
  populatePipelineSelect,
} from '../../core/pipelines.js';

let pipelineTemplates = [];
let availableComponents = {};

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this.refresh();
    return this;
  },

  destroy() {},

  async refresh() {
    await Promise.all([this._loadComponents(), this._loadTemplates(), this._loadPipelines()]);
  },

  async _loadComponents() {
    try {
      const components = await api('/components');
      availableComponents = components;
      const list = this.container.querySelector('#components-list');
      if (!list) return;

      if (!Object.keys(components).length) {
        list.innerHTML = renderEmptyState({
          title: t('pipelines.empty.components'),
          hint: t('ui.empty.componentsHint'),
          variant: 'compact',
          escapeHtml,
        });
        return;
      }
      const typeLabels = {
        collector: t('pipelines.type.collector'),
        processor: t('pipelines.type.processor'),
        storage: t('pipelines.type.storage'),
      };
      list.innerHTML = Object.entries(components).map(([type, names]) => `
        <div class="component-group mb-6">
          <h3 class="text-[10px] font-bold tracking-widest uppercase text-zinc-500 mb-3 border-b border-theme-strong pb-1">${escapeHtml(typeLabels[type] || type)}</h3>
          <div class="flex flex-wrap gap-2">
            ${names.map((name) => `<span class="component-tag type-${escapeHtml(type)}">${escapeHtml(name)}</span>`).join('')}
          </div>
        </div>
      `).join('');
      this._populateFormComponents();
    } catch (err) { console.error('Load components failed:', err); }
  },

  async _loadTemplates() {
    try {
      pipelineTemplates = await loadPipelineTemplates();
      const select = document.getElementById('pipeline-template');
      if (!select) return;
      select.innerHTML = `<option value="">${escapeHtml(t('pipelines.custom'))}</option>`;
      for (const template of pipelineTemplates) {
        select.insertAdjacentHTML('beforeend',
          `<option value="${template.id}">${escapeHtml(template.name)}</option>`);
      }
    } catch (err) { console.error('Load pipeline templates failed:', err); }
  },

  async _loadPipelines() {
    try {
      const pipelines = await loadPipelines();
      const list = this.container.querySelector('#pipelines-list');
      if (!list) return;

      const entries = Object.entries(pipelines);
      if (!entries.length) {
        list.innerHTML = renderEmptyState({
          title: t('pipelines.empty.pipelines'),
          hint: t('ui.empty.pipelinesHint'),
          variant: 'compact',
          escapeHtml,
          actionHtml: `<button type="button" class="btn btn-primary btn-sm" onclick="showCreatePipelineModal()">${escapeHtml(t('pipelines.create'))}</button>`,
        });
        return;
      }
      list.innerHTML = entries.map(([name, config]) => `
        <div class="pipeline-item group bg-zinc-800 border border-theme-subtle rounded-xl p-4 mb-4 relative overflow-hidden transition-all duration-300 hover:border-theme-strong hover:shadow-[0_8px_30px_rgba(0,0,0,0.5)]">
          <div class="flex items-center justify-between mb-4">
            <span class="font-bold text-theme-primary text-sm tracking-tight">${escapeHtml(name)}</span>
            <button class="btn btn-danger h-7 px-2 text-xs opacity-0 group-hover:opacity-100 transition-opacity duration-300 shadow-[0_0_10px_rgba(244,63,94,0.2)]" data-delete="${escapeHtml(name)}">${t('common.delete')}</button>
          </div>
          <div class="pipeline-steps flex items-center flex-wrap gap-2">
            ${(config.steps || []).map((step, index) => `
              ${index > 0 ? '<svg class="w-4 h-4 text-zinc-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14 5l7 7m0 0l-7 7m7-7H3"></path></svg>' : ''}
              <span class="component-tag type-${escapeHtml(step.type)}">${escapeHtml(step.name)}</span>
            `).join('')}
          </div>
        </div>
      `).join('');

      list.querySelectorAll('[data-delete]').forEach(btn => {
        btn.addEventListener('click', () => this._deletePipeline(btn.dataset.delete));
      });
    } catch (err) { console.error('Load pipelines failed:', err); }
  },

  _showCreateModal() {
    this._loadComponents();
    this._loadTemplates();
    window.openModal && window.openModal('modal-create-pipeline');
  },

  _populateFormComponents() {
    const collectorSelect = document.getElementById('pipeline-collector');
    if (!collectorSelect) return;
    const current = collectorSelect.value;
    const collectors = availableComponents.collector || [];
    collectorSelect.innerHTML = collectors.map((name) =>
      `<option value="${name}" ${name === current ? 'selected' : ''}>${name}</option>`
    ).join('');
  },

  _applyTemplate() {
    const templateId = document.getElementById('pipeline-template')?.value;
    if (!templateId) return;
    const template = pipelineTemplates.find((item) => item.id === templateId);
    if (!template) return;

    setValue('pipeline-name', template.id);
    setValue('pipeline-collector', template.steps.find((step) => step.type === 'collector')?.name || '');
    setChecked('pipeline-processor-cleaner', template.steps.some((step) => step.type === 'processor' && step.name === 'cleaner'));
    setChecked('pipeline-processor-embedding', template.steps.some((step) => step.type === 'processor' && step.name === 'embedding'));
    setChecked(
      'pipeline-storage-sqlalchemy',
      template.steps.some((step) =>
        step.type === 'storage' && (step.name === 'sqlalchemy' || step.name === 'local')),
    );
    setChecked('pipeline-storage-vector', template.steps.some((step) => step.type === 'storage' && step.name === 'vector'));
    setValue('pipeline-steps', JSON.stringify(template.steps, null, 2));
  },

  _buildStepsFromForm() {
    const steps = [];
    const collector = document.getElementById('pipeline-collector')?.value || '';
    if (collector) steps.push({ type: 'collector', name: collector, config: {} });
    if (document.getElementById('pipeline-processor-cleaner')?.checked) steps.push({ type: 'processor', name: 'cleaner', config: {} });
    if (document.getElementById('pipeline-processor-embedding')?.checked) steps.push({ type: 'processor', name: 'embedding', config: {} });
    if (document.getElementById('pipeline-storage-sqlalchemy')?.checked) steps.push({ type: 'storage', name: 'sqlalchemy', config: {} });
    if (document.getElementById('pipeline-storage-vector')?.checked) steps.push({ type: 'storage', name: 'vector', config: {} });
    return steps;
  },

  async _createPipeline() {
    const name = document.getElementById('pipeline-name')?.value.trim() || '';
    const cmEditor = document.querySelector('#pipeline-steps + .CodeMirror')?.CodeMirror;
    const stepsRaw = cmEditor ? cmEditor.getValue().trim() : (document.getElementById('pipeline-steps')?.value.trim() || '');

    if (!name) { toast(t('message.pipelineNameRequired'), 'error'); return; }

    let steps = this._buildStepsFromForm();
    if (stepsRaw) {
      try { 
        const parsed = JSON.parse(stepsRaw); 
        if (Array.isArray(parsed)) {
          steps = parsed;
        } else if (typeof parsed === 'object' && parsed !== null) {
          // Assume it's config for the collector if it's an object
          if (steps.length > 0 && steps[0].type === 'collector') {
            steps[0].config = parsed;
          } else {
            steps = [parsed]; // Fallback to array replacement if no collector
          }
        }
      }
      catch { toast(t('message.pipelineJsonInvalid'), 'error'); return; }
    }
    if (!steps || !steps.length) { toast(t('message.pipelineStepsRequired'), 'error'); return; }

    try {
      await api('/pipelines', { method: 'POST', body: JSON.stringify({ name, steps }) });
      toast(t('message.pipelineCreated'), 'success');
      window.closeModal && window.closeModal('modal-create-pipeline');
      invalidatePipelineCache();
      await this.refresh();
      await populatePipelineSelect('task-pipeline');
      await populatePipelineSelect('cron-pipeline');
    } catch (err) { toast(t('message.createFailed', { error: err.message }), 'error'); }
  },

  async _deletePipeline(name) {
    if (!confirm(t('confirm.deletePipeline', { name }))) return;
    try {
      await api(`/pipelines/${encodeURIComponent(name)}?confirm=true`, { method: 'DELETE' });
      toast(t('message.pipelineDeleted'), 'success');
      invalidatePipelineCache();
      await this.refresh();
      await populatePipelineSelect('task-pipeline');
      await populatePipelineSelect('cron-pipeline');
    } catch (err) { toast(t('message.deleteFailed', { error: err.message }), 'error'); }
  },

  async _loadPipelineSelect(selectId) {
    try {
      await loadAvailablePipelines();
      pipelineTemplates = getCachedPipelineTemplates();
      await populatePipelineSelect(selectId);
    } catch (err) { console.error('Load pipeline select failed:', err); }
  },
};

window.loadComponents = function () { if (window._pipelinesPage) window._pipelinesPage._loadComponents(); };
window.loadPipelineTemplates = function () { if (window._pipelinesPage) window._pipelinesPage._loadTemplates(); };
window.loadPipelines = function () { if (window._pipelinesPage) window._pipelinesPage._loadPipelines(); };
window.showCreatePipelineModal = function () { if (window._pipelinesPage) window._pipelinesPage._showCreateModal(); };
window.applyPipelineTemplate = function () { if (window._pipelinesPage) window._pipelinesPage._applyTemplate(); };
window.buildPipelineStepsFromForm = function () { if (window._pipelinesPage) return window._pipelinesPage._buildStepsFromForm(); };
window.createPipeline = function () { if (window._pipelinesPage) window._pipelinesPage._createPipeline(); };
window.deletePipeline = function (name) { if (window._pipelinesPage) window._pipelinesPage._deletePipeline(name); };
window.loadPipelineSelect = function (id) { if (window._pipelinesPage) window._pipelinesPage._loadPipelineSelect(id); };
window.populatePipelineFormComponents = function () { if (window._pipelinesPage) window._pipelinesPage._populateFormComponents(); };
