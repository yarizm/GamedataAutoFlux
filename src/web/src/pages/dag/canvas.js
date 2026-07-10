/**
 * Drawflow canvas wrapper for DAG editor.
 * Maps single "records" port to input_1 / output_1.
 * Supports space/middle-button pan for canvas navigation.
 */
import Drawflow from 'drawflow';
import { escapeHtml } from '../../core/api.js';
import {
  getCachedCollectorMeta,
  inputParamsFromMetadata,
  outputFieldsForComponent,
} from './schemas.js';

function nodeHtml(node) {
  const title = escapeHtml(node.label || node.component || node.id);
  const type = escapeHtml(node.type || '');
  const id = escapeHtml(node.id || '');
  const badge = node.config?.from_upstream
    ? '<span class="dag-node-badge">↑上游</span>'
    : '';

  let inHint = '';
  let outHint = '';
  if (node.type === 'collector') {
    const meta = getCachedCollectorMeta(node.component);
    const inputs = inputParamsFromMetadata(meta, node.component);
    if (inputs.length) {
      const keys = inputs.map((p) => p.key).filter((k) => k !== '__name__').slice(0, 4);
      if (keys.length) {
        inHint = `<div class="dag-node-ports"><span class="port-tag in">入参</span> ${escapeHtml(keys.join(', '))}</div>`;
      }
    }
    const outs = outputFieldsForComponent(node.component).slice(0, 4).map((f) => f.key);
    if (outs.length && outs[0] !== 'records') {
      outHint = `<div class="dag-node-ports"><span class="port-tag out">输出</span> ${escapeHtml(outs.join(', '))}</div>`;
    }
  } else if (node.type === 'processor' || node.type === 'storage') {
    inHint = '<div class="dag-node-ports"><span class="port-tag in">入</span> records</div>';
    if (node.type === 'processor') {
      outHint = '<div class="dag-node-ports"><span class="port-tag out">出</span> records</div>';
    }
  }

  return `
    <div class="dag-node-inner">
      <div class="dag-node-type">[${type}]</div>
      <div class="dag-node-title">${title}</div>
      <div class="dag-node-id">${id}</div>
      ${inHint}
      ${outHint}
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
  editor.zoom_max = 2.0;
  editor.zoom_min = 0.25;
  editor.zoom_value = 0.08;
  editor.start();

  /** businessId -> drawflow numeric id (string) */
  const bizToDf = new Map();
  /** drawflow id (string) -> businessId */
  const dfToBiz = new Map();

  let suppressEvents = false;
  let selectedEdgeKey = null;

  // --- Canvas pan: space+drag / middle-button / empty-area only ---
  // Must fully own pan events: dual pan with Drawflow causes canvas_x double-count
  // and nodes appear to jump when left-dragging.
  let spaceHeld = false;
  let panning = false;
  let panLast = { x: 0, y: 0 };

  function applyCanvasTransform() {
    if (!editor.precanvas) return;
    editor.precanvas.style.transform =
      `translate(${editor.canvas_x || 0}px, ${editor.canvas_y || 0}px) scale(${editor.zoom || 1})`;
  }

  function setPanCursor(on) {
    el.classList.toggle('dag-panning', on);
    el.style.cursor = on ? 'grabbing' : (spaceHeld ? 'grab' : '');
  }

  function stopPanning() {
    if (!panning) return;
    panning = false;
    setPanCursor(false);
    if (spaceHeld) el.style.cursor = 'grab';
  }

  /** True only for blank canvas hits — never nodes/ports/connections. */
  function isEmptyCanvasTarget(target) {
    if (!target || !el.contains(target)) return false;
    if (target.closest('.drawflow-node, .input, .output, .connection, .main-path, .point, .drawflow-delete')) {
      return false;
    }
    return (
      target === el
      || target === editor.precanvas
      || target.classList?.contains('parent-drawflow')
      || target.classList?.contains('drawflow')
      || target.classList?.contains('parent-node')
    );
  }

  function onKeyDown(ev) {
    if (ev.code === 'Space' && !ev.repeat && !ev.target.closest('input,textarea,select')) {
      spaceHeld = true;
      el.classList.add('dag-pan-ready');
      el.style.cursor = 'grab';
      ev.preventDefault();
    }
  }
  function onKeyUp(ev) {
    if (ev.code === 'Space') {
      spaceHeld = false;
      el.classList.remove('dag-pan-ready');
      if (!panning) el.style.cursor = '';
    }
  }
  window.addEventListener('keydown', onKeyDown);
  window.addEventListener('keyup', onKeyUp);

  el.addEventListener('mousedown', (ev) => {
    // Node / port / connection: never pan; cancel any pan so node drag stays stable
    const onInteractive = ev.target.closest(
      '.drawflow-node, .input, .output, .connection, .main-path, .point, .drawflow-delete',
    );
    if (onInteractive) {
      stopPanning();
      // Prevent Drawflow from also treating this as canvas pan
      try { editor.editor_selected = false; } catch { /* ignore */ }
      return;
    }

    const middle = ev.button === 1;
    const spacePan = spaceHeld && ev.button === 0;
    const emptyPan = ev.button === 0 && isEmptyCanvasTarget(ev.target);
    if (!(middle || spacePan || emptyPan)) return;

    panning = true;
    panLast = { x: ev.clientX, y: ev.clientY };
    // Kill Drawflow's own canvas drag so canvas_x is not applied twice on mouseup
    try {
      editor.editor_selected = false;
      editor.drag = false;
    } catch { /* ignore */ }
    setPanCursor(true);
    ev.preventDefault();
    ev.stopPropagation();
  }, true);

  window.addEventListener('mousemove', (ev) => {
    if (!panning) return;
    // If Drawflow started a node drag somehow, yield immediately
    if (editor.drag) {
      stopPanning();
      return;
    }
    const dx = ev.clientX - panLast.x;
    const dy = ev.clientY - panLast.y;
    if (dx === 0 && dy === 0) return;
    panLast = { x: ev.clientX, y: ev.clientY };
    editor.canvas_x = (editor.canvas_x || 0) + dx;
    editor.canvas_y = (editor.canvas_y || 0) + dy;
    applyCanvasTransform();
    ev.preventDefault();
  });

  window.addEventListener('mouseup', () => {
    stopPanning();
  });

  // Wheel zoom toward cursor (Drawflow has wheel zoom; ensure enabled)
  el.addEventListener(
    'wheel',
    (ev) => {
      // Drawflow handles wheel on precanvas; we just prevent page scroll
      if (ev.ctrlKey || ev.metaKey || el.matches(':hover')) {
        ev.preventDefault();
      }
    },
    { passive: false },
  );

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
        const nodeEl = el.querySelector(`#node-${dfId}`);
        let x = nodeEl ? parseFloat(nodeEl.style.left) : NaN;
        let y = nodeEl ? parseFloat(nodeEl.style.top) : NaN;
        if (!Number.isFinite(x) || !Number.isFinite(y)) {
          const info = editor.getNodeFromId(dfId);
          x = typeof info?.pos_x === 'number' ? info.pos_x : n.x;
          y = typeof info?.pos_y === 'number' ? info.pos_y : n.y;
        }
        if (Number.isFinite(x)) n.x = x;
        if (Number.isFinite(y)) n.y = y;
        const home = editor.drawflow?.drawflow?.[editor.module]?.data;
        if (home?.[dfId] && Number.isFinite(x) && Number.isFinite(y)) {
          home[dfId].pos_x = x;
          home[dfId].pos_y = y;
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
        const x = Number.isFinite(Number(n.x)) ? Number(n.x) : 0;
        const y = Number.isFinite(Number(n.y)) ? Number(n.y) : 0;
        const dfId = editor.addNode(
          n.id,
          hasIn,
          hasOut,
          x,
          y,
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

      // Re-apply left/top from editor state (Drawflow + parent-node CSS can drift)
      reapplyNodeLayout(nodes);

      // restore zoom + pan if present
      const z = Number(editorState?.ui?.zoom);
      const pan = editorState?.ui?.pan || {};
      if (Number.isFinite(z) && z > 0) {
        editor.zoom = z;
        editor.zoom_last_value = z;
      }
      const panX = Number(pan.x);
      const panY = Number(pan.y);
      if (Number.isFinite(panX) && Number.isFinite(panY)) {
        editor.canvas_x = panX;
        editor.canvas_y = panY;
      }
      applyCanvasTransform();

      // allow DOM to settle then style condition edges + layout once more
      requestAnimationFrame(() => {
        reapplyNodeLayout(nodes);
        applyConditionStyles();
      });
    } finally {
      suppressEvents = false;
    }
  }

  function reapplyNodeLayout(nodes) {
    for (const n of nodes || []) {
      const dfId = mapBizId(n.id);
      if (dfId == null) continue;
      const x = Number(n.x);
      const y = Number(n.y);
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
      const nodeEl = el.querySelector(`#node-${dfId}`);
      if (!nodeEl) continue;
      nodeEl.style.left = `${x}px`;
      nodeEl.style.top = `${y}px`;
      try {
        const home = editor.drawflow?.drawflow?.[editor.module]?.data;
        if (home?.[dfId]) {
          home[dfId].pos_x = x;
          home[dfId].pos_y = y;
        }
        editor.updateConnectionNodes(`node-${dfId}`);
      } catch {
        /* ignore */
      }
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
      // Prefer DOM left/top: Drawflow sometimes writes pos_x/pos_y with a
      // double-subtract against offsetLeft after style update (visible jump /
      // wrong save position under zoom/pan).
      const nodeEl = el.querySelector(`#node-${dfId}`);
      let x;
      let y;
      if (nodeEl) {
        x = parseFloat(nodeEl.style.left);
        y = parseFloat(nodeEl.style.top);
      }
      if (!Number.isFinite(x) || !Number.isFinite(y)) {
        const info = editor.getNodeFromId(dfId);
        x = info?.pos_x;
        y = info?.pos_y;
      }
      if (Number.isFinite(x) && Number.isFinite(y)) {
        const home = editor.drawflow?.drawflow?.[editor.module]?.data;
        if (home?.[dfId]) {
          home[dfId].pos_x = x;
          home[dfId].pos_y = y;
        }
        const node = ed.nodes.find((n) => n.id === bizId);
        if (node) {
          node.x = x;
          node.y = y;
        }
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
      window.removeEventListener('keydown', onKeyDown);
      window.removeEventListener('keyup', onKeyUp);
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
