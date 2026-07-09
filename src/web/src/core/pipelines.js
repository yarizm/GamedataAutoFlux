import { api, escapeHtml } from './api.js';
import { t } from './i18n.js';

let pipelinesCache = null;
let templatesCache = null;
let availablePipelinesCache = null;
let pipelinesPromise = null;
let templatesPromise = null;
let availablePromise = null;

function publishAvailablePipelines(value) {
  availablePipelinesCache = value || {};
  window.availablePipelines = availablePipelinesCache;
  window.pipelineTemplates = templatesCache || [];
  return availablePipelinesCache;
}

export function invalidatePipelineCache() {
  pipelinesCache = null;
  templatesCache = null;
  availablePipelinesCache = null;
  pipelinesPromise = null;
  templatesPromise = null;
  availablePromise = null;
  publishAvailablePipelines({});
}

export async function loadPipelines({ force = false } = {}) {
  if (!force && pipelinesCache) return pipelinesCache;
  if (!force && pipelinesPromise) return pipelinesPromise;
  pipelinesPromise = api('/pipelines')
    .then((items) => {
      pipelinesCache = items || {};
      return pipelinesCache;
    })
    .finally(() => { pipelinesPromise = null; });
  return pipelinesPromise;
}

export async function loadPipelineTemplates({ force = false } = {}) {
  if (!force && templatesCache) return templatesCache;
  if (!force && templatesPromise) return templatesPromise;
  templatesPromise = api('/pipeline-templates')
    .then((items) => {
      templatesCache = items || [];
      window.pipelineTemplates = templatesCache;
      return templatesCache;
    })
    .finally(() => { templatesPromise = null; });
  return templatesPromise;
}

export async function loadAvailablePipelines({ force = false } = {}) {
  if (!force && availablePipelinesCache) return availablePipelinesCache;
  if (!force && availablePromise) return availablePromise;
  availablePromise = Promise.all([
    loadPipelines({ force }),
    loadPipelineTemplates({ force }),
  ]).then(([pipelines, templates]) => {
    const merged = { ...(pipelines || {}) };
    for (const template of templates || []) {
      if (template?.id && !merged[template.id]) merged[template.id] = template;
    }
    return publishAvailablePipelines(merged);
  }).finally(() => { availablePromise = null; });
  return availablePromise;
}

export function getCachedAvailablePipelines() {
  return availablePipelinesCache || window.availablePipelines || {};
}

export function getCachedPipelineTemplates() {
  return templatesCache || window.pipelineTemplates || [];
}

export function getPipelineConfig(name) {
  return getCachedAvailablePipelines()[name] || null;
}

function _isDagConfig(cfg) {
  if (!cfg || typeof cfg !== 'object') return false;
  if (cfg.kind === 'dag' || cfg.kind === 'pipeline_legacy') return true;
  return Array.isArray(cfg.nodes) && !Array.isArray(cfg.steps);
}

export function getCollectorForPipeline(name) {
  const pipeline = getPipelineConfig(name);
  if (!pipeline) return '';
  // 三段式 Pipeline
  const collectorStep = pipeline?.steps?.find((step) => step.type === 'collector');
  if (collectorStep) {
    return collectorStep.name || collectorStep.component_name || '';
  }
  // DAG：nodes[].type === collector
  const collectorNode = (pipeline.nodes || []).find((n) => n.type === 'collector');
  return collectorNode?.component || collectorNode?.name || '';
}

export function hasStorageStep(pipelineName, storageName) {
  const pipeline = getPipelineConfig(pipelineName);
  if (!pipeline) return false;
  if (Array.isArray(pipeline.steps)) {
    return Boolean(pipeline.steps.some((step) =>
      step.type === 'storage' && (step.name || step.component_name) === storageName));
  }
  return Boolean((pipeline.nodes || []).some((n) =>
    n.type === 'storage' && (n.component || n.name) === storageName));
}

export async function populatePipelineSelect(selectId) {
  const allPipelines = await loadAvailablePipelines();
  const select = document.getElementById(selectId);
  if (!select) return allPipelines;

  const current = select.value;
  const names = Object.keys(allPipelines).sort((a, b) => {
    const aDag = _isDagConfig(allPipelines[a]) ? 0 : 1;
    const bDag = _isDagConfig(allPipelines[b]) ? 0 : 1;
    if (aDag !== bDag) return aDag - bDag;
    return a.localeCompare(b);
  });
  select.innerHTML = names.length === 0
    ? `<option value="">${t('pipelines.empty.pipelines')}</option>`
    : `<option value="">${t('tasks.selectPipeline')}</option>`;

  for (const name of names) {
    const cfg = allPipelines[name];
    const label = _isDagConfig(cfg) ? `[DAG] ${name}` : name;
    select.insertAdjacentHTML(
      'beforeend',
      `<option value="${escapeHtml(name)}">${escapeHtml(label)}</option>`,
    );
  }
  if (names.includes(current)) select.value = current;
  return allPipelines;
}
