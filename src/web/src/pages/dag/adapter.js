/** API ↔ editor model adapter for DAG page. */

export const TYPE_COLORS = {
  collector: 'emerald',
  processor: 'sky',
  storage: 'amber',
  composite: 'violet',
};

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
  if (type === 'storage') {
    return {
      ports_in: [{ name: 'records', required: true }],
      ports_out: [],
    };
  }
  if (type === 'collector') {
    return {
      ports_in: [{ name: 'records', required: false }],
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
 * Convert API DAG payload to editor state.
 * @param {object} payload
 * @returns {{ name: string, nodes: object[], edges: object[], ui: object }}
 */
export function apiToEditor(payload) {
  const nodesIn = payload?.nodes || [];
  const nodes = nodesIn.map((n, i) => {
    const ui = n.ui || {};
    const ports = defaultPortsForType(n.type);
    // Normalize legacy local storage alias for display
    let component = n.component || '';
    if (n.type === 'storage' && (component === 'local' || !component)) {
      component = 'sqlalchemy';
    }
    return {
      id: n.id,
      type: n.type,
      component,
      config: { ...(n.config || {}) },
      ports_in: (n.ports_in?.length ? n.ports_in : ports.ports_in).map((p) => ({
        name: p.name,
        required: p.required !== false && n.type !== 'collector',
      })),
      ports_out: (n.ports_out?.length ? n.ports_out : ports.ports_out).map((p) => ({
        name: p.name,
        required: p.required !== false,
      })),
      x: layoutNum(ui.x, Number.NaN),
      y: layoutNum(ui.y, Number.NaN),
      label: typeof ui.label === 'string' ? ui.label : '',
    };
  }).map((n, i) => ({
    ...n,
    // Missing/invalid ui coords → stable grid (not 0,0 pile-up)
    x: Number.isFinite(n.x) ? n.x : 60 + (i % 4) * 200,
    y: Number.isFinite(n.y) ? n.y : 40 + Math.floor(i / 4) * 120,
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
      ports_in: (n.ports_in || []).map((p) => ({
        name: p.name,
        required: n.type === 'collector' ? false : p.required !== false,
      })),
      ports_out: (n.ports_out || []).map((p) => ({
        name: p.name,
        required: p.required !== false,
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
