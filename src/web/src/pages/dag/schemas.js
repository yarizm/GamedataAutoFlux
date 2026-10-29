/**
 * Node I/O schema hints for DAG editor (Dify-style mapping UI).
 * Inputs: from collector_metadata API when available.
 * Outputs: collector-declared output_fields; logic/processor nodes pass their
 * upstream fields through (transform adds / drops fields).
 */

/** Fields used by from_upstream auto mode (must match dag_upstream.py). */
export const AUTO_UPSTREAM_FIELDS = [
  'channel_url',
  'channel_id',
  'handle',
  'video_url',
  'app_id',
  'url',
  'official_url',
];

/** Record roots every logic condition can reference (see dag_logic.record_view). */
export const RECORD_ROOT_FIELDS = ['success', 'error', 'error_code', 'target.name', 'source'];

/**
 * Parse collector_metadata payload → list of input param hints.
 * @param {object|null} meta - collectors[id] from /components/metadata
 */
export function inputParamsFromMetadata(meta, componentId) {
  if (!meta || typeof meta !== 'object') {
    return [];
  }
  const schema = meta.target_schema || {};
  const fields = new Map();

  for (const field of schema.fields || []) {
    if (!field || typeof field !== 'object') continue;
    const key = field.location === 'name' ? '__name__' : String(field.key || '');
    if (!key) continue;
    fields.set(key, {
      key,
      required: Boolean(field.required),
      label: field.label || field.key || key,
      description: field.description || '',
      inputType: field.input_type || 'text',
    });
  }
  if (fields.size) {
    return [...fields.values()];
  }
  for (const raw of schema.required_fields || []) {
    const text = String(raw);
    // "target.params.video_url" or free text
    const m = text.match(/target\.params\.([a-zA-Z0-9_]+)/);
    if (m) {
      const current = fields.get(m[1]) || {};
      fields.set(m[1], { key: m[1], label: m[1], ...current, required: true });
    } else if (text.includes('target.name')) {
      const current = fields.get('__name__') || {};
      fields.set('__name__', {
        key: '__name__',
        label: 'target.name',
        ...current,
        required: true,
      });
    }
  }
  for (const rule of schema.rules || []) {
    for (const f of rule.fields || []) {
      const m = String(f).match(/target\.params\.([a-zA-Z0-9_]+)/);
      if (m && !fields.has(m[1])) {
        fields.set(m[1], {
          key: m[1],
          required: rule.level !== 'warning',
          label: m[1],
        });
      }
    }
  }

  if (!fields.size) {
    return [];
  }
  return [...fields.values()];
}

export function outputFieldsForComponent(componentId) {
  const outputFields = _metaCache?.[componentId]?.output_fields;
  if (Array.isArray(outputFields) && outputFields.length) {
    return outputFields.map((item) => ({
      key: item.key,
      label: item.label || item.key,
      typeHint: item.type_hint || '',
      description: item.description || '',
    }));
  }
  return [{ key: 'records', label: 'records（整包）' }];
}

function declaredCollectorFields(component) {
  return outputFieldsForComponent(component).filter((f) => f.key !== 'records');
}

/**
 * Data fields a node's output records are known to carry.
 * @param {object} editor
 * @param {object} node
 * @param {Set<string>} seen - cycle guard
 */
function outputFieldsOfNode(editor, node, seen) {
  if (!node || seen.has(node.id)) return [];
  seen.add(node.id);
  if (node.type === 'collector') {
    return declaredCollectorFields(node.component).map((f) => ({ ...f, fromNode: node.id, fromComponent: node.component }));
  }
  if (node.type === 'input' || node.type === 'storage') return [];
  // processor / logic: pass upstream fields through
  const upstream = recordFieldsInto(editor, node.id, seen);
  if (node.type === 'logic' && node.component === 'transform') {
    const config = node.config || {};
    const dropped = new Set(Array.isArray(config.drop_fields) ? config.drop_fields : []);
    const base = config.keep_original === false ? [] : upstream.filter((f) => !dropped.has(f.key));
    const added = (Array.isArray(config.fields) ? config.fields : [])
      .filter((f) => f && f.key)
      .map((f) => ({ key: f.key, label: f.key, fromNode: node.id, fromComponent: 'transform' }));
    const byKey = new Map(base.map((f) => [f.key, f]));
    for (const f of added) byKey.set(f.key, f);
    return [...byKey.values()];
  }
  return upstream;
}

function recordFieldsInto(editor, nodeId, seen) {
  const byKey = new Map();
  for (const e of (editor?.edges || []).filter((edge) => edge.to === nodeId && (edge.in || 'records') === 'records')) {
    const src = (editor.nodes || []).find((n) => n.id === e.from);
    for (const f of outputFieldsOfNode(editor, src, new Set(seen))) {
      if (!byKey.has(f.key)) byKey.set(f.key, f);
    }
  }
  return [...byKey.values()];
}

/**
 * Collect upstream output fields for a node given graph edges (for from_upstream mapping).
 * @param {object} editor
 * @param {string} nodeId
 */
export function upstreamOutputFields(editor, nodeId) {
  const byKey = new Map(recordFieldsInto(editor, nodeId, new Set()).map((f) => [f.key, f]));
  // always include auto catalog keys as optional picks
  for (const key of AUTO_UPSTREAM_FIELDS) {
    if (!byKey.has(key)) {
      byKey.set(key, { key, label: key, fromNode: '', fromComponent: '' });
    }
  }
  return [...byKey.values()];
}

/**
 * Field paths offered by logic node editors: known upstream data fields first,
 * then the generic record roots.
 * @param {object} editor
 * @param {string} nodeId
 * @returns {string[]}
 */
export function recordFieldOptions(editor, nodeId) {
  const dataFields = recordFieldsInto(editor, nodeId, new Set()).map((f) => `data.${f.key}`);
  return [...new Set([...dataFields, ...RECORD_ROOT_FIELDS])];
}

let _metaCache = null;
let _dagNodeCache = null;

export async function loadCollectorMetaMap() {
  if (_metaCache) return _metaCache;
  try {
    const { api } = await import('../../core/api.js');
    const data = await api('/components/metadata');
    _metaCache = data?.collectors || {};
    _dagNodeCache = new Map(
      (Array.isArray(data?.dag_nodes) ? data.dag_nodes : []).map((definition) => [
        `${definition.type}:${definition.component}`,
        definition,
      ]),
    );
  } catch {
    _metaCache = {};
    _dagNodeCache = new Map();
  }
  return _metaCache;
}

/** Drop cached metadata so the next load reflects newly (de)activated plugins. */
export function invalidateCollectorMetaMap() {
  _metaCache = null;
  _dagNodeCache = null;
}

export function getCachedCollectorMeta(componentId) {
  return _metaCache?.[componentId] || null;
}

export function getCachedDagNodeDefinition(nodeType, componentId) {
  return _dagNodeCache?.get(`${nodeType}:${componentId}`) || null;
}
