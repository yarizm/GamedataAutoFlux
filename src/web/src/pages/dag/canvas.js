/**
 * Drawflow canvas wrapper for DAG editor.
 * Maps single "records" port to input_1 / output_1.
 */
import Drawflow from 'drawflow';
import { escapeHtml } from '../../core/api.js';

function nodeHtml(node) {
  const title = escapeHtml(node.label || node.component || node.id);
  const type = escapeHtml(node.type || '');
  const id = escapeHtml(node.id || '');
  const badge = node.config?.from_upstream
    ? '<span class="dag-node-badge">↑上游</span>'
    : '';
  return `
    <div class="dag-node-inner">
      <div class="dag-node-type">[${type}]</div>
      <div class="dag-node-title">${title}</div>
      <div class="dag-node-id">${id}</div>
      ${badge}
    </div>
  `;
}

function inputsCount(node) {
  if (node.type === 'storage' || node.type === 'processor' || node.type === 'collector') {
    return 1;
  }
  return (node.ports_in || []).length ? 1 : 0;
}

function outputsCount(node) {
  if (node.type === 'storage') return 0;
  return 1;
}

/**
 * @param {HTMLElement} el - #drawflow container
 * @param {{
 *   onSelectNode?: (id: string|null) => void,
 *   onSelectEdge?: (edge: object|null) => void,
 *   onChange?: () => void,
 *   getEditor?: () => object,
 * }} handlers
 */
export function createCanvas(el, handlers = {}) {
  if (!el) throw new Error('createCanvas: missing element');

  const editor = new Drawflow(el);
  editor.reroute = false;
  editor.force_first_input = false;
  editor.draggable_inputs = false;
  editor.zoom_max = 1.6;
  editor.zoom_min = 0.4;
  editor.zoom_value = 0.1;
  editor.start();

  /** businessId -> drawflow numeric id (string) */
  const bizToDf = new Map();
  /** drawflow id (string) -> businessId */
  const dfToBiz = new Map();

  let suppressEvents = false;
  let selectedEdgeKey = null;

  function mapDfId(dfId) {
    return dfToBiz.get(String(dfId)) || null;
  }

  function mapBizId(bizId) {
    return bizToDf.get(bizId) ?? null;
  }

  function edgeFromConn(conn) {
    const from = mapDfId(conn.output_id);
    const to = mapDfId(conn.input_id);
    if (!from || !to) return null;
    return {
      from,
      out: 'records',
      to,
      in: 'records',
      condition: null,
    };
  }

  function edgeKey(e) {
    return `${e.from}|${e.out || 'records'}|${e.to}|${e.in || 'records'}`;
  }

  function syncEdgesFromDrawflow() {
    const ed = handlers.getEditor?.();
    if (!ed) return;
    const prev = new Map((ed.edges || []).map((e) => [edgeKey(e), e]));
    const next = [];
    const data = editor.export();
    const home = data?.drawflow?.Home?.data || {};
    for (const [dfId, node] of Object.entries(home)) {
      const fromBiz = mapDfId(dfId);
      if (!fromBiz) continue;
      const outputs = node.outputs || {};
      for (const out of Object.values(outputs)) {
        for (const c of out.connections || []) {
          const toBiz = mapDfId(c.node);
          if (!toBiz) continue;
          const base = {
            from: fromBiz,
            out: 'records',
            to: toBiz,
            in: 'records',
            condition: null,
          };
          const key = edgeKey(base);
          const old = prev.get(key);
          next.push(old ? { ...base, condition: old.condition || null } : base);
        }
      }
    }
    ed.edges = next;
  }

  function syncPositionsFromDrawflow() {
    const ed = handlers.getEditor?.();
    if (!ed) return;
    for (const n of ed.nodes || []) {
      const dfId = mapBizId(n.id);
      if (dfId == null) continue;
      try {
        const info = editor.getNodeFromId(dfId);
        if (info) {
          n.x = typeof info.pos_x === 'number' ? info.pos_x : n.x;
          n.y = typeof info.pos_y === 'number' ? info.pos_y : n.y;
        }
      } catch {
        /* node may have been removed */
      }
    }
  }

  function applyConditionStyles() {
    const ed = handlers.getEditor?.();
    if (!ed) return;
    const conditioned = new Set(
      (ed.edges || [])
        .filter((e) => e.condition)
        .map((e) => edgeKey(e)),
    );
    // Drawflow connection SVG paths: class connection node_in_node-X node_out_node-Y ...
    const svg = el.querySelector('svg.drawflow');
    if (!svg) return;
    svg.querySelectorAll('.connection').forEach((connEl) => {
      connEl.classList.remove('dag-edge-condition');
      const cls = connEl.className?.baseVal || connEl.getAttribute('class') || '';
      // class pattern: connection node_in_node-{toDf} node_out_node-{fromDf}
      const mIn = cls.match(/node_in_node-(\d+)/);
      const mOut = cls.match(/node_out_node-(\d+)/);
      if (!mIn || !mOut) return;
      const fromBiz = mapDfId(mOut[1]);
      const toBiz = mapDfId(mIn[1]);
      if (!fromBiz || !toBiz) return;
      const key = `${fromBiz}|records|${toBiz}|records`;
      if (conditioned.has(key)) {
        connEl.classList.add('dag-edge-condition');
      }
    });
  }

  function rebuildNodeHtml(bizId) {
    const ed = handlers.getEditor?.();
    const node = ed?.nodes?.find((n) => n.id === bizId);
    const dfId = mapBizId(bizId);
    if (!node || dfId == null) return;
    const nodeEl = el.querySelector(`#node-${dfId}`);
    if (!nodeEl) return;
    const content = nodeEl.querySelector('.drawflow_content_node');
    if (content) {
      content.innerHTML = nodeHtml(node);
    }
    // refresh class for type
    nodeEl.classList.remove(
      'dag-type-collector',
      'dag-type-processor',
      'dag-type-storage',
      'dag-type-composite',
    );
    nodeEl.classList.add(`dag-type-${node.type || 'composite'}`);
  }

  function loadEditor(editorState) {
    suppressEvents = true;
    try {
      editor.clear();
      bizToDf.clear();
      dfToBiz.clear();
      selectedEdgeKey = null;

      const nodes = editorState?.nodes || [];
      const edges = editorState?.edges || [];

      for (const n of nodes) {
        const hasIn = inputsCount(n);
        const hasOut = outputsCount(n);
        const cls = `dag-type-${n.type || 'composite'}`;
        const dfId = editor.addNode(
          n.id,
          hasIn,
          hasOut,
          Number(n.x) || 0,
          Number(n.y) || 0,
          cls,
          {
            bizId: n.id,
            type: n.type,
            component: n.component,
          },
          nodeHtml(n),
        );
        const idStr = String(dfId);
        bizToDf.set(n.id, idStr);
        dfToBiz.set(idStr, n.id);
      }

      for (const e of edges) {
        const fromDf = mapBizId(e.from);
        const toDf = mapBizId(e.to);
        if (fromDf == null || toDf == null) continue;
        // storage has no output; collector/processor have output_1; all non-empty inputs are input_1
        editor.addConnection(fromDf, toDf, 'output_1', 'input_1');
      }

      // restore zoom if present
      if (editorState?.ui?.zoom && typeof editorState.ui.zoom === 'number') {
        editor.zoom = editorState.ui.zoom;
        editor.zoom_last_value = editorState.ui.zoom;
        editor.precanvas.style.transform =
          `translate(${editor.canvas_x}px, ${editor.canvas_y}px) scale(${editor.zoom})`;
      }

      // allow DOM to settle then style condition edges
      requestAnimationFrame(() => applyConditionStyles());
    } finally {
      suppressEvents = false;
    }
  }

  function addNodeToCanvas(node) {
    const hasIn = inputsCount(node);
    const hasOut = outputsCount(node);
    const cls = `dag-type-${node.type || 'composite'}`;
    const dfId = editor.addNode(
      node.id,
      hasIn,
      hasOut,
      Number(node.x) || 60,
      Number(node.y) || 40,
      cls,
      { bizId: node.id, type: node.type, component: node.component },
      nodeHtml(node),
    );
    const idStr = String(dfId);
    bizToDf.set(node.id, idStr);
    dfToBiz.set(idStr, node.id);
    return idStr;
  }

  function removeNodeFromCanvas(bizId) {
    const dfId = mapBizId(bizId);
    if (dfId == null) return;
    suppressEvents = true;
    try {
      editor.removeNodeId(`node-${dfId}`);
    } catch {
      /* ignore */
    } finally {
      suppressEvents = false;
    }
    bizToDf.delete(bizId);
    dfToBiz.delete(String(dfId));
  }

  function selectNode(bizId) {
    if (!bizId) {
      handlers.onSelectNode?.(null);
      return;
    }
    const dfId = mapBizId(bizId);
    if (dfId == null) return;
    const nodeEl = el.querySelector(`#node-${dfId}`);
    if (nodeEl) {
      // mimic Drawflow selection
      el.querySelectorAll('.drawflow-node.selected').forEach((n) => n.classList.remove('selected'));
      nodeEl.classList.add('selected');
    }
    handlers.onSelectNode?.(bizId);
  }

  function fitView() {
    const ed = handlers.getEditor?.();
    const nodes = ed?.nodes || [];
    if (!nodes.length) {
      editor.zoom = 1;
      editor.canvas_x = 0;
      editor.canvas_y = 0;
      editor.zoom_last_value = 1;
      editor.precanvas.style.transform = 'translate(0px, 0px) scale(1)';
      return;
    }
    syncPositionsFromDrawflow();
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    for (const n of nodes) {
      minX = Math.min(minX, n.x);
      minY = Math.min(minY, n.y);
      maxX = Math.max(maxX, n.x + 180);
      maxY = Math.max(maxY, n.y + 100);
    }
    const wrap = el.getBoundingClientRect();
    const pad = 40;
    const w = maxX - minX + pad * 2;
    const h = maxY - minY + pad * 2;
    const scale = Math.max(
      editor.zoom_min,
      Math.min(1, Math.min(wrap.width / w, wrap.height / h)),
    );
    editor.zoom = scale;
    editor.zoom_last_value = scale;
    editor.canvas_x = pad - minX * scale + (wrap.width - w * scale) / 2;
    editor.canvas_y = pad - minY * scale + (wrap.height - h * scale) / 2;
    editor.precanvas.style.transform =
      `translate(${editor.canvas_x}px, ${editor.canvas_y}px) scale(${scale})`;
  }

  function getZoomUi() {
    return {
      zoom: editor.zoom,
      pan: { x: editor.canvas_x || 0, y: editor.canvas_y || 0 },
    };
  }

  // --- Events ---
  editor.on('nodeSelected', (dfId) => {
    if (suppressEvents) return;
    selectedEdgeKey = null;
    handlers.onSelectEdge?.(null);
    handlers.onSelectNode?.(mapDfId(dfId));
  });

  editor.on('nodeUnselected', () => {
    if (suppressEvents) return;
    if (!selectedEdgeKey) {
      handlers.onSelectNode?.(null);
    }
  });

  editor.on('nodeMoved', (dfId) => {
    if (suppressEvents) return;
    const bizId = mapDfId(dfId);
    const ed = handlers.getEditor?.();
    if (!bizId || !ed) return;
    try {
      const info = editor.getNodeFromId(dfId);
      const node = ed.nodes.find((n) => n.id === bizId);
      if (node && info) {
        node.x = info.pos_x;
        node.y = info.pos_y;
      }
    } catch {
      /* ignore */
    }
    handlers.onChange?.();
  });

  editor.on('nodeRemoved', (dfId) => {
    if (suppressEvents) return;
    const bizId = mapDfId(dfId);
    const ed = handlers.getEditor?.();
    if (bizId && ed) {
      ed.nodes = (ed.nodes || []).filter((n) => n.id !== bizId);
      ed.edges = (ed.edges || []).filter((e) => e.from !== bizId && e.to !== bizId);
      bizToDf.delete(bizId);
      dfToBiz.delete(String(dfId));
      handlers.onSelectNode?.(null);
      handlers.onChange?.();
    }
  });

  editor.on('connectionCreated', (conn) => {
    if (suppressEvents) return;
    const edge = edgeFromConn(conn);
    const ed = handlers.getEditor?.();
    if (!edge || !ed) return;

    const exists = (ed.edges || []).some(
      (e) => e.from === edge.from && e.to === edge.to
        && (e.out || 'records') === edge.out
        && (e.in || 'records') === edge.in,
    );
    if (!exists) {
      ed.edges = [...(ed.edges || []), edge];
    }

    // collector → collector: auto from_upstream
    const fromNode = ed.nodes.find((n) => n.id === edge.from);
    const toNode = ed.nodes.find((n) => n.id === edge.to);
    if (fromNode?.type === 'collector' && toNode?.type === 'collector') {
      if (!toNode.config?.from_upstream) {
        toNode.config = {
          ...(toNode.config || {}),
          from_upstream: { auto: true },
        };
        rebuildNodeHtml(toNode.id);
      }
    }
    requestAnimationFrame(() => applyConditionStyles());
    handlers.onChange?.();
  });

  editor.on('connectionRemoved', (conn) => {
    if (suppressEvents) return;
    const edge = edgeFromConn(conn);
    const ed = handlers.getEditor?.();
    if (!edge || !ed) return;
    ed.edges = (ed.edges || []).filter(
      (e) => !(
        e.from === edge.from
        && e.to === edge.to
        && (e.out || 'records') === edge.out
        && (e.in || 'records') === edge.in
      ),
    );
    selectedEdgeKey = null;
    handlers.onSelectEdge?.(null);
    handlers.onChange?.();
  });

  editor.on('connectionSelected', (conn) => {
    if (suppressEvents) return;
    const edge = edgeFromConn(conn);
    const ed = handlers.getEditor?.();
    if (!edge || !ed) return;
    const full = (ed.edges || []).find(
      (e) => e.from === edge.from
        && e.to === edge.to
        && (e.out || 'records') === edge.out
        && (e.in || 'records') === edge.in,
    ) || edge;
    selectedEdgeKey = edgeKey(full);
    handlers.onSelectNode?.(null);
    handlers.onSelectEdge?.(full);
  });

  editor.on('connectionUnselected', () => {
    if (suppressEvents) return;
    selectedEdgeKey = null;
    handlers.onSelectEdge?.(null);
  });

  return {
    loadEditor,
    getEditorState() {
      syncPositionsFromDrawflow();
      syncEdgesFromDrawflow();
      return handlers.getEditor?.();
    },
    syncPositions: syncPositionsFromDrawflow,
    syncEdges: syncEdgesFromDrawflow,
    applyConditionStyles,
    rebuildNodeHtml,
    addNodeToCanvas,
    removeNodeFromCanvas,
    selectNode,
    fitView,
    getZoomUi,
    destroy() {
      try {
        editor.clear();
      } catch {
        /* ignore */
      }
      el.innerHTML = '';
      bizToDf.clear();
      dfToBiz.clear();
    },
    /** @internal expose for debugging */
    _df: editor,
    _bizToDf: bizToDf,
  };
}
