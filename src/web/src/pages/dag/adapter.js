/** API ↔ editor model adapter for DAG page. */

export const TYPE_COLORS = {
  input: 'rose',
  collector: 'emerald',
  logic: 'violet',
  processor: 'sky',
  storage: 'amber',
  composite: 'violet',
};

export const ERROR_PORT = 'error';
const ELSE_PORT = 'else';

/** Coerce layout coords (API may return number or numeric string). */
export function layoutNum(value, fallback = 0) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const n = parseFloat(value);
    if (Number.isFinite(n)) return n;
  }
  return fallback;
}

export function defaultPortsForType(type) {
  if (type === 'input') {
    return {
      ports_in: [],
      ports_out: [{ name: 'params', required: true, type_hint: 'params' }],
    };
  }
  if (type === 'storage') {
    return {
      ports_in: [{ name: 'records', required: true }],
      ports_out: [],
    };
  }
  if (type === 'collector') {
    return {
      // params 接 input 节点（任务参数）；records 接上游记录（串联采集）
      ports_in: [
        { name: 'params', required: false, type_hint: 'params' },
        { name: 'records', required: false },
      ],
      ports_out: [{ name: 'records', required: true }],
    };
  }
  // processor / default
  return {
    ports_in: [{ name: 'records', required: true }],
    ports_out: [{ name: 'records', required: true }],
  };
}

/**
 * Logic node ports derive from config (mirror of src/core/dag_logic.py:logic_output_port_names).
 * @param {string} component
 * @param {object} config
 */
export function logicPorts(component, config) {
  const portsIn = [{ name: 'records', required: true, type_hint: 'records' }];
  let names;
  if (component === 'condition') {
    const cases = Array.isArray(config?.cases) ? config.cases : [];
    names = [...new Set(cases.map((c) => c?.id).filter((id) => typeof id === 'string' && id))];
    names.push(ELSE_PORT);
  } else if (component === 'filter') {
    names = ['records', 'rejected'];
  } else {
    names = ['records'];
  }
  return {
    ports_in: portsIn,
    ports_out: names.map((name) => ({ name, required: true, type_hint: 'records' })),
  };
}

export function usesErrorPort(policy) {
  return Boolean(policy && policy.on_error === 'error_branch');
}

/**
 * Recompute a node's ports from its type/config/policy; keeps ports_in from the
 * catalog for plugin nodes. Mutates and returns the node.
 * @param {object} node
 */
export function syncNodePorts(node) {
  if (node.type === 'logic') {
    const ports = logicPorts(node.component, node.config);
    node.ports_in = ports.ports_in;
    node.ports_out = ports.ports_out;
  }
  if (node.type === 'collector' && !(node.ports_in || []).some((p) => p.name === 'params')) {
    node.ports_in = [{ name: 'params', required: false, type_hint: 'params' }, ...(node.ports_in || [])];
  }
  const outs = (node.ports_out || []).filter((p) => p.name !== ERROR_PORT);
  if (usesErrorPort(node.policy)) {
    outs.push({ name: ERROR_PORT, required: false, type_hint: 'records' });
  }
  node.ports_out = outs;
  return node;
}

function normalizePort(p, { forceOptional = false } = {}) {
  const port = { name: p.name, required: forceOptional ? false : p.required !== false };
  if (p.type_hint) port.type_hint = p.type_hint;
  return port;
}

/**
 * Convert API DAG payload to editor state.
 * @param {object} payload
 * @returns {{ name: string, nodes: object[], edges: object[], ui: object }}
 */
export function apiToEditor(payload) {
  const nodesIn = payload?.nodes || [];
  const nodes = nodesIn.map((n) => {
    const ui = n.ui || {};
    const ports = defaultPortsForType(n.type);
    // Normalize legacy local storage alias for display
    let component = n.component || '';
    if (n.type === 'storage' && (component === 'local' || !component)) {
      component = 'sqlalchemy';
    }
    const node = {
      id: n.id,
      type: n.type,
      component,
      config: { ...(n.config || {}) },
      policy: { ...(n.policy || {}) },
      ports_in: (n.ports_in?.length ? n.ports_in : ports.ports_in).map((p) =>
        normalizePort(p, { forceOptional: n.type === 'collector' }),
      ),
      ports_out: (n.ports_out?.length ? n.ports_out : ports.ports_out).map((p) => normalizePort(p)),
      x: layoutNum(ui.x, Number.NaN),
      y: layoutNum(ui.y, Number.NaN),
      label: typeof ui.label === 'string' ? ui.label : '',
    };
    return syncNodePorts(node);
  }).map((n, i) => ({
    ...n,
    // Missing/invalid ui coords → stable grid (not 0,0 pile-up)
    x: Number.isFinite(n.x) ? n.x : 60 + (i % 4) * 220,
    y: Number.isFinite(n.y) ? n.y : 40 + Math.floor(i / 4) * 140,
  }));
  const edges = (payload?.edges || []).map((e) => ({
    from: e.from,
    out: e.out || e.from_port || 'records',
    to: e.to,
    in: e.in || e.to_port || 'records',
    condition: e.condition || null,
  }));
  const graphUi = payload?.ui && typeof payload.ui === 'object' ? payload.ui : {};
  const pan = graphUi.pan && typeof graphUi.pan === 'object' ? graphUi.pan : {};
  return {
    name: payload?.name || '',
    nodes,
    edges,
    ui: {
      ...graphUi,
      zoom: layoutNum(graphUi.zoom, 1) || 1,
      pan: {
        x: layoutNum(pan.x, 0),
        y: layoutNum(pan.y, 0),
      },
    },
  };
}

function cleanPolicy(policy) {
  const out = {};
  if (!policy || typeof policy !== 'object') return out;
  const retries = Number(policy.max_retries);
  if (Number.isInteger(retries) && retries > 0) out.max_retries = retries;
  const interval = Number(policy.retry_interval_seconds);
  if (out.max_retries && Number.isFinite(interval) && interval > 0) {
    out.retry_interval_seconds = interval;
  }
  const timeout = Number(policy.timeout_seconds);
  if (policy.timeout_seconds !== '' && policy.timeout_seconds != null && Number.isFinite(timeout) && timeout > 0) {
    out.timeout_seconds = timeout;
  }
  if (policy.on_error && policy.on_error !== 'fail') out.on_error = policy.on_error;
  return out;
}

/**
 * Convert editor state to POST /api/dags body.
 * @param {{ name: string, nodes: object[], edges: object[], ui?: object }} editor
 */
export function editorToApi(editor) {
  const graphUi = editor.ui && typeof editor.ui === 'object' ? editor.ui : {};
  const pan = graphUi.pan && typeof graphUi.pan === 'object' ? graphUi.pan : {};
  return {
    name: (editor.name || '').trim(),
    nodes: (editor.nodes || []).map((n) => ({
      id: n.id,
      type: n.type,
      component: n.component,
      config: n.config || {},
      policy: cleanPolicy(n.policy),
      ports_in: (n.ports_in || []).map((p) => ({
        name: p.name,
        required: n.type === 'collector' ? false : p.required !== false,
        ...(p.type_hint ? { type_hint: p.type_hint } : {}),
      })),
      ports_out: (n.ports_out || []).map((p) => ({
        name: p.name,
        required: p.required !== false,
        ...(p.type_hint ? { type_hint: p.type_hint } : {}),
      })),
      is_param_port: [],
      ui: {
        x: layoutNum(n.x, 0),
        y: layoutNum(n.y, 0),
        ...(n.label ? { label: n.label } : {}),
      },
    })),
    edges: (editor.edges || []).map((e) => ({
      from: e.from,
      out: e.out || 'records',
      to: e.to,
      in: e.in || 'records',
      condition: e.condition || null,
    })),
    conditions: [],
    ui: {
      ...graphUi,
      zoom: layoutNum(graphUi.zoom, 1) || 1,
      pan: {
        x: layoutNum(pan.x, 0),
        y: layoutNum(pan.y, 0),
      },
    },
  };
}

/** Stable identity of an edge (4-tuple, same as the engine). */
export function edgeKey(e) {
  return `${e.from}|${e.out || 'records'}|${e.to}|${e.in || 'records'}`;
}

/** Can an output port feed an input port? params only into params; data ports into records. */
export function portsCompatible(outPort, inPort) {
  const outIsParams = (outPort?.type_hint || '') === 'params' || outPort?.name === 'params';
  const inIsParams = (inPort?.type_hint || '') === 'params' || inPort?.name === 'params';
  return outIsParams === inIsParams;
}
