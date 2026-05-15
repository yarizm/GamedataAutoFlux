import { api, toast, escapeHtml, setValue, setChecked } from '../../core/api.js';

let pipelineTemplates = [];
let availableComponents = {};
let pipelineStepsEditor = null;

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
        list.innerHTML = '<p class="text-muted">No components</p>';
        return;
      }
      list.innerHTML = Object.entries(components).map(([type, names]) => `
        <div class="component-group">
          <h3>${escapeHtml(type)}</h3>
          <div class="component-tags">
            ${names.map((name) => `<span class="component-tag">${escapeHtml(name)}</span>`).join('')}
          </div>
        </div>
      `).join('');
      this._populateFormComponents();
    } catch (err) { console.error('Load components failed:', err); }
  },

  async _loadTemplates() {
    try {
      pipelineTemplates = await api('/pipeline-templates');
      const select = document.getElementById('pipeline-template');
      if (!select) return;
      select.innerHTML = '<option value="">-- Custom --</option>';
      for (const template of pipelineTemplates) {
        select.insertAdjacentHTML('beforeend',
          `<option value="${template.id}">${escapeHtml(template.name)}</option>`);
      }
    } catch (err) { console.error('Load pipeline templates failed:', err); }
  },

  async _loadPipelines() {
    try {
      const pipelines = await api('/pipelines');
      const list = this.container.querySelector('#pipelines-list');
      if (!list) return;

      const entries = Object.entries(pipelines);
      if (!entries.length) {
        list.innerHTML = '<p class="text-muted">No pipelines</p>';
        return;
      }
      list.innerHTML = entries.map(([name, config]) => `
        <div class="pipeline-item">
          <div class="pipeline-item-header">
            <span class="pipeline-item-name">${escapeHtml(name)}</span>
            <button class="btn btn-danger btn-sm" data-delete="${escapeHtml(name)}">Delete</button>
          </div>
          <div class="pipeline-steps">
            ${(config.steps || []).map((step, index) => `
              ${index > 0 ? '<span class="pipeline-arrow">-></span>' : ''}
              <span class="pipeline-step-tag ${escapeHtml(step.type)}">${escapeHtml(step.type)}:${escapeHtml(step.name)}</span>
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
    setChecked('pipeline-storage-local', template.steps.some((step) => step.type === 'storage' && step.name === 'local'));
    setChecked('pipeline-storage-vector', template.steps.some((step) => step.type === 'storage' && step.name === 'vector'));
    setValue('pipeline-steps', JSON.stringify(template.steps, null, 2));
  },

  _buildStepsFromForm() {
    const steps = [];
    const collector = document.getElementById('pipeline-collector')?.value || '';
    if (collector) steps.push({ type: 'collector', name: collector, config: {} });
    if (document.getElementById('pipeline-processor-cleaner')?.checked) steps.push({ type: 'processor', name: 'cleaner', config: {} });
    if (document.getElementById('pipeline-processor-embedding')?.checked) steps.push({ type: 'processor', name: 'embedding', config: {} });
    if (document.getElementById('pipeline-storage-local')?.checked) steps.push({ type: 'storage', name: 'local', config: {} });
    if (document.getElementById('pipeline-storage-vector')?.checked) steps.push({ type: 'storage', name: 'vector', config: {} });
    return steps;
  },

  async _createPipeline() {
    const name = document.getElementById('pipeline-name')?.value.trim() || '';
    const cmEditor = document.querySelector('#pipeline-steps + .CodeMirror')?.CodeMirror;
    const stepsRaw = cmEditor ? cmEditor.getValue().trim() : (document.getElementById('pipeline-steps')?.value.trim() || '');

    if (!name) { toast('Pipeline name is required', 'error'); return; }

    let steps = this._buildStepsFromForm();
    if (stepsRaw) {
      try { steps = JSON.parse(stepsRaw); }
      catch { toast('Pipeline steps JSON is invalid', 'error'); return; }
    }
    if (!steps.length) { toast('Choose at least one collector and one storage step', 'error'); return; }

    try {
      await api('/pipelines', { method: 'POST', body: JSON.stringify({ name, steps }) });
      toast('Pipeline created', 'success');
      window.closeModal && window.closeModal('modal-create-pipeline');
      this.refresh();
      window.loadPipelineSelect && window.loadPipelineSelect('task-pipeline');
      window.loadPipelineSelect && window.loadPipelineSelect('cron-pipeline');
    } catch (err) { toast(`Create failed: ${err.message}`, 'error'); }
  },

  async _deletePipeline(name) {
    if (!confirm(`Delete pipeline "${name}"?`)) return;
    try {
      await api(`/pipelines/${encodeURIComponent(name)}?confirm=true`, { method: 'DELETE' });
      toast('Pipeline deleted', 'success');
      this.refresh();
      window.loadPipelineSelect && window.loadPipelineSelect('task-pipeline');
      window.loadPipelineSelect && window.loadPipelineSelect('cron-pipeline');
    } catch (err) { toast(`Delete failed: ${err.message}`, 'error'); }
  },

  async _loadPipelineSelect(selectId) {
    try {
      const [pipelines, templates] = await Promise.all([
        api('/pipelines'),
        pipelineTemplates.length ? Promise.resolve(pipelineTemplates) : api('/pipeline-templates'),
      ]);
      pipelineTemplates = templates;
      const allPipelines = { ...pipelines };
      for (const template of pipelineTemplates) {
        if (!allPipelines[template.id]) allPipelines[template.id] = template;
      }
      const select = document.getElementById(selectId);
      if (!select) return;
      select.innerHTML = Object.keys(allPipelines).length === 0
        ? '<option value="">-- No pipelines --</option>'
        : '<option value="">-- Select a pipeline --</option>';
      for (const [pName] of Object.entries(allPipelines)) {
        select.insertAdjacentHTML('beforeend', `<option value="${escapeHtml(pName)}">${escapeHtml(pName)}</option>`);
      }
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
