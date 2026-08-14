import { api, escapeHtml } from './api.js';
import { t } from './i18n.js';

let pipelinesCache = null;
let runnablePipelinesCache = null;
let templatesCache = null;
let componentMetadataCache = null;
let availablePipelinesCache = null;
let pipelinesPromise = null;
let runnablePipelinesPromise = null;
let templatesPromise = null;
let componentMetadataPromise = null;
let availablePromise = null;

function publishAvailablePipelines(value) {
  availablePipelinesCache = value || {};
  window.availablePipelines = availablePipelinesCache;
  window.pipelineTemplates = templatesCache || [];
  return availablePipelinesCache;
}

export function invalidatePipelineCache() {
  pipelinesCache = null;
  runnablePipelinesCache = null;
  templatesCache = null;
  componentMetadataCache = null;
  availablePipelinesCache = null;
  pipelinesPromise = null;
  runnablePipelinesPromise = null;
  templatesPromise = null;
  componentMetadataPromise = null;
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

async function loadRunnablePipelines({ force = false } = {}) {
  if (!force && runnablePipelinesCache) return runnablePipelinesCache;
  if (!force && runnablePipelinesPromise) return runnablePipelinesPromise;
  runnablePipelinesPromise = api('/pipelines?available_only=true')
    .then((items) => {
      runnablePipelinesCache = items || {};
      return runnablePipelinesCache;
    })
    .finally(() => { runnablePipelinesPromise = null; });
  return runnablePipelinesPromise;
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

export async function loadComponentMetadata({ force = false } = {}) {
  if (!force && componentMetadataCache) return componentMetadataCache;
  if (!force && componentMetadataPromise) return componentMetadataPromise;
  componentMetadataPromise = api('/components/metadata')
    .then((payload) => {
      componentMetadataCache = payload || { components: {}, collectors: {}, dag_nodes: [] };
      return componentMetadataCache;
    })
    .finally(() => { componentMetadataPromise = null; });
  return componentMetadataPromise;
}

export function getCachedComponentMetadata() {
  return componentMetadataCache || { components: {}, collectors: {}, dag_nodes: [] };
}

export async function loadAvailablePipelines({ force = false } = {}) {
  if (!force && availablePipelinesCache) return availablePipelinesCache;
  if (!force && availablePromise) return availablePromise;
  availablePromise = Promise.all([
    loadRunnablePipelines({ force }),
    loadPipelineTemplates({ force }),
    loadComponentMetadata({ force }),
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

export function isDagConfig(cfg) {
  if (!cfg || typeof cfg !== 'object') return false;
  if (cfg.kind === 'dag' || cfg.kind === 'pipeline_legacy') return true;
  return Array.isArray(cfg.nodes) && !Array.isArray(cfg.steps);
}

function planSteps(cfg) {
  if (!cfg || typeof cfg !== 'object') return [];
  if (Array.isArray(cfg.steps)) {
    return cfg.steps.map((step) => ({
      type: step.type,
      name: step.name || step.component_name || '',
      config: step.config || {},
    }));
  }
  return (cfg.nodes || []).map((node) => ({
    type: node.type,
    name: node.component || node.name || '',
    config: node.config || {},
  }));
}

export function describePipeline(name) {
  const config = getPipelineConfig(name);
  if (!config) return null;
  const template = getCachedPipelineTemplates().find((item) => item.id === name) || null;
  const metadataPayload = getCachedComponentMetadata();
  const steps = planSteps(config);
  const collectorSteps = steps.filter((step) => step.type === 'collector');
  const rootCollectorSteps = collectorSteps.filter(
    (step) => !step.config?.from_upstream,
  );
  const collectors = collectorSteps.map((step) => step.name).filter(Boolean);
  const targetCollectors = (rootCollectorSteps.length ? rootCollectorSteps : collectorSteps)
    .map((step) => step.name)
    .filter(Boolean);
  const collectorDetails = collectors.map((collectorId) => (
    metadataPayload.collectors?.[collectorId] || {
      collector_id: collectorId,
      display_name: collectorId,
      description: '',
      target_schema: {},
    }
  ));
  const targetDetails = targetCollectors.map((collectorId) => (
    metadataPayload.collectors?.[collectorId] || {
      collector_id: collectorId,
      display_name: collectorId,
      description: '',
      target_schema: {},
    }
  ));
  const description = String(
    config.description
    || collectorDetails.map((item) => item.description).filter(Boolean).join(' ')
    || template?.description
    || '',
  );
  return {
    name,
    displayName: String(config.display_name || template?.name || name),
    config,
    template,
    kind: isDagConfig(config) ? 'dag' : 'linear',
    steps,
    collectors,
    targetCollectors,
    collectorDetails,
    targetDetails,
    description,
  };
}

function targetFieldForPath(fields, path) {
  const text = String(path || '').trim();
  if (text === 'target.name') {
    return fields.find((field) => field.location === 'name');
  }
  const match = text.match(/^target\.params\.([a-zA-Z0-9_]+)$/);
  return match
    ? fields.find((field) => field.location !== 'name' && field.key === match[1])
    : null;
}

export function getTargetRequirementLabels(metadata) {
  const schema = metadata?.target_schema || {};
  const fields = Array.isArray(schema.fields) ? schema.fields : [];
  const directlyRequired = fields
    .filter((field) => field.required)
    .map((field) => field.label || field.key)
    .filter(Boolean);
  if (directlyRequired.length) return directlyRequired;

  const ruleLabels = [];
  for (const rule of schema.rules || []) {
    if (rule.level === 'warning' || (rule.check && rule.check !== 'presence')) continue;
    const labels = (rule.fields || [])
      .map((path) => targetFieldForPath(fields, path)?.label)
      .filter(Boolean);
    if (!labels.length) continue;
    ruleLabels.push(
      rule.mode === 'any' && labels.length > 1
        ? t('tasks.requireAny', { fields: labels.join(' / ') })
        : labels.join(' / '),
    );
  }
  if (ruleLabels.length) return ruleLabels;
  return Array.isArray(schema.required_fields) ? schema.required_fields : [];
}

export function getCollectorForPipeline(name) {
  return describePipeline(name)?.targetCollectors?.[0] || '';
}

export function getCollectorsForPipeline(name) {
  return describePipeline(name)?.collectors || [];
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
    const aDag = isDagConfig(allPipelines[a]) ? 0 : 1;
    const bDag = isDagConfig(allPipelines[b]) ? 0 : 1;
    if (aDag !== bDag) return aDag - bDag;
    return a.localeCompare(b);
  });
  select.innerHTML = names.length === 0
    ? `<option value="">${t('pipelines.empty.pipelines')}</option>`
    : `<option value="">${t('tasks.selectPipeline')}</option>`;

  for (const name of names) {
    const cfg = allPipelines[name];
    const descriptor = describePipeline(name);
    const kind = isDagConfig(cfg) ? 'DAG' : t('pipelines.plan.linearShort');
    const collectors = descriptor?.targetDetails
      ?.map((item) => item.display_name || item.collector_id)
      .filter(Boolean)
      .join(' + ');
    const displayName = descriptor?.displayName || name;
    const identity = displayName === name ? displayName : `${displayName} (${name})`;
    const label = `[${kind}] ${identity}${collectors ? ` — ${collectors}` : ''}`;
    select.insertAdjacentHTML(
      'beforeend',
      `<option value="${escapeHtml(name)}">${escapeHtml(label)}</option>`,
    );
  }
  if (names.includes(current)) select.value = current;
  return allPipelines;
}
