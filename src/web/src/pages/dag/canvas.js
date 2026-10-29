/**
 * Drawflow canvas wrapper for DAG editor.
 *
 * Named ports map onto Drawflow's positional ports: node.ports_in[i] ↔ input_{i+1},
 * node.ports_out[i] ↔ output_{i+1}. Edges in editor state always carry port names.
 * Supports space/middle-button pan for canvas navigation.
 */
import Drawflow from 'drawflow';
import { escapeHtml } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { ERROR_PORT, edgeKey, portsCompatible } from './adapter.js';
import {
  getCachedCollectorMeta,
  getCachedDagNodeDefinition,
  inputParamsFromMetadata,
  outputFieldsForComponent,
} from './schemas.js';
import { describeRunState } from './runTrace.js';

const NODE_TYPE_CLASSES = [
  'dag-type-collector',
  'dag-type-processor',
  'dag-type-storage',
  'dag-type-composite',
  'dag-type-input',
  'dag-type-logic',
];
const RUN_CLASSES = [
  'dag-run-pending',
  'dag-run-running',
  'dag-run-retrying',
  'dag-run-succeeded',
  'dag-run-failed',
  'dag-run-exception',
  'dag-run-skipped',
  'dag-run-not_run',
];

export function nodeDisplayName(node) {
  if (node.label) return node.label;
  if (node.type === 'input') return t('dag.inputNode');
  if (node.type === 'logic') return t(`dag.logic.${node.component}`);
  const definition = getCachedDagNodeDefinition(node.type, node.component);
  return definition?.display_name || node.component || node.id;
}

function typeLabel(node) {
  if (node.type === 'logic') return t('dag.type.logic');
  return node.type || '';
}

function logicSummary(node) {
  const config = node.config || {};
  if (node.component === 'condition') {
    const count = Array.isArray(config.cases) ? config.cases.length : 0;
    return t('dag.logic.conditionSummary', { count });
  }
  if (node.component === 'filter') {
    const parts = [];
    const conditions = Array.isArray(config.conditions) ? config.conditions.length : 0;
    if (conditions) parts.push(t('dag.logic.filterConditions', { count: conditions }));
    if (Array.isArray(config.dedupe_by) && config.dedupe_by.length) parts.push(t('dag.logic.filterDedupe'));
    if (config.sort_by) parts.push(t('dag.logic.filterSort'));
    if (Number(config.limit) > 0) parts.push(t('dag.logic.filterLimit', { limit: config.limit }));
    return parts.join(' · ') || t('dag.logic.filterPassAll');
  }
  if (node.component === 'transform') {
    const count = Array.isArray(config.fields) ? config.fields.length : 0;
    return t('dag.logic.transformSummary', { count });
  }
  return '';
}

function policyBadges(node) {
  const policy = node.policy || {};
  const badges = [];
  if (Number(policy.max_retries) > 0) badges.push(t('dag.policy.badgeRetry', { count: policy.max_retries }));
  if (Number(policy.timeout_seconds) > 0) badges.push(t('dag.policy.badgeTimeout', { seconds: policy.timeout_seconds }));
  if (policy.on_error && policy.on_error !== 'fail') badges.push(t(`dag.policy.badge.${policy.on_error}`));
  return badges;
}

function nodeHtml(node, runState) {
  const title = escapeHtml(nodeDisplayName(node));
  const type = escapeHtml(typeLabel(node));
  const id = escapeHtml(node.id || '');
  const badges = [];
  if (node.config?.from_upstream) badges.push(t('dag.badgeUpstream'));
  badges.push(...policyBadges(node));

  let hint = '';
  if (node.type === 'collector') {
    const meta = getCachedCollectorMeta(node.component);
    const inputs = inputParamsFromMetadata(meta, node.component);
    const keys = inputs.map((p) => p.key).filter((k) => k !== '__name__').slice(0, 4);
    if (keys.length) {
      hint = `<div class="dag-node-ports"><span class="port-tag in">${escapeHtml(t('dag.portTagIn'))}</span> ${escapeHtml(keys.join(', '))}</div>`;
    }
    const outs = outputFieldsForComponent(node.component).slice(0, 4).map((f) => f.key);
    if (outs.length && outs[0] !== 'records') {
      hint += `<div class="dag-node-ports"><span class="port-tag out">${escapeHtml(t('dag.portTagOut'))}</span> ${escapeHtml(outs.join(', '))}</div>`;
    }
  } else if (node.type === 'logic') {
    const summary = logicSummary(node);
    if (summary) hint = `<div class="dag-node-ports">${escapeHtml(summary)}</div>`;
  }

  const run = runState ? describeRunState(runState) : null;
  const runRow = run
    ? `<div class="dag-node-run is-${escapeHtml(run.status)}" title="${escapeHtml(run.title)}"><span class="dag-node-run-icon">${run.icon}</span><span>${escapeHtml(run.text)}</span></div>`
    : '';

  return `
    <div class="dag-node-inner">
      <div class="dag-node-type">${type}</div>
      <div class="dag-node-title">${title}</div>
      <div class="dag-node-id">${id}</div>
      ${hint}
      ${badges.length ? `<div class="dag-node-badges">${badges.map((b) => `<span class="dag-node-badge">${escapeHtml(b)}</span>`).join('')}</div>` : ''}
      ${runRow}
    </div>
  `;
}

function portIndex(ports, name) {
  const idx = (ports || []).findIndex((p) => p.name === name);
  return idx >= 0 ? idx + 1 : null;
}

function portByClass(ports, cls, prefix) {
  const m = String(cls || '').match(new RegExp(`^${prefix}_(\\d+)$`));
  if (!m) return null;
  return (ports || [])[Number(m[1]) - 1] || null;
}

/** Only label ports when a side has several ports or a non-default name. */
function shouldLabelPorts(ports) {
  return (ports || []).length > 1 || (ports || []).some((p) => p.name !== 'records');
}

function portLabel(node, port) {
  if (port.name === ERROR_PORT) return t('dag.port.error');
  if (node.type === 'logic' && node.component === 'condition') {
    if (port.name === 'else') return t('dag.port.else');
    const found = (node.config?.cases || []).find((c) => c?.id === port.name);
    return found?.label || port.name;
  }
  if (port.name === 'rejected') return t('dag.port.rejected');
  if (port.name === 'params') return t('dag.port.params');
  if (port.name === 'records') return t('dag.port.records');
  return port.name;
}

/**
 * @param {HTMLElement} el - #drawflow container
 * @param {{
 *   onSelectNode?: (id: string|null) => void,
 *   onSelectEdge?: (edge: object|null) => void,
 *   onChange?: (change: { kind: string }) => void,
 *   onReject?: (message: string) => void,
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
  /** businessId -> run trace (transient, never saved) */
  const runStates = new Map();

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

  function onWindowMouseMove(ev) {
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
  }
  function onWindowMouseUp() {
    stopPanning();
  }
  window.addEventListener('mousemove', onWindowMouseMove);
  window.addEventListener('mouseup', onWindowMouseUp);

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

  function findNode(bizId) {
    return handlers.getEditor?.()?.nodes?.find((n) => n.id === bizId) || null;
  }

  function edgeFromConn(conn) {
    const from = mapDfId(conn.output_id);
    const to = mapDfId(conn.input_id);
    if (!from || !to) return null;
    const outPort = portByClass(findNode(from)?.ports_out, conn.output_class, 'output');
    const inPort = portByClass(findNode(to)?.ports_in, conn.input_class, 'input');
    if (!outPort || !inPort) return null;
    return {
      from,
      out: outPort.name,
      to,
      in: inPort.name,
      condition: null,
    };
  }

  function findEdge(edge) {
    const ed = handlers.getEditor?.();
    const key = edgeKey(edge);
    return (ed?.edges || []).find((e) => edgeKey(e) === key) || null;
  }

  function syncEdgesFromDrawflow() {
    const ed = handlers.getEditor?.();
    if (!ed) return;
    const prev = new Map((ed.edges || []).map((e) => [edgeKey(e), e]));
    const order = new Map((ed.edges || []).map((e, i) => [edgeKey(e), i]));
    const next = [];
    const data = editor.export();
    const home = data?.drawflow?.Home?.data || {};
    for (const [dfId, dfNode] of Object.entries(home)) {
      const fromBiz = mapDfId(dfId);
      const fromNode = fromBiz ? findNode(fromBiz) : null;
      if (!fromNode) continue;
      for (const [outCls, out] of Object.entries(dfNode.outputs || {})) {
        const outPort = portByClass(fromNode.ports_out, outCls, 'output');
        if (!outPort) continue;
        for (const c of out.connections || []) {
          const toBiz = mapDfId(c.node);
          const toNode = toBiz ? findNode(toBiz) : null;
          const inPort = toNode ? portByClass(toNode.ports_in, c.output, 'input') : null;
          if (!inPort) continue;
          const base = {
            from: fromBiz,
            out: outPort.name,
            to: toBiz,
            in: inPort.name,
            condition: null,
          };
          const old = prev.get(edgeKey(base));
          next.push(old ? { ...base, condition: old.condition || null } : base);
        }
      }
    }
    // 保持既有顺序：上面按 drawflow 节点 id 重建，顺序与数组不一致，
    // 会让快照比较误判“有变化”，撤销/重做被空提交吞掉
    next.sort((a, b) => (
      (order.get(edgeKey(a)) ?? Number.MAX_SAFE_INTEGER)
      - (order.get(edgeKey(b)) ?? Number.MAX_SAFE_INTEGER)
    ));
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

  /** Style connection paths: condition (dashed), error branch, run state. */
  function applyConditionStyles() {
    const ed = handlers.getEditor?.();
    if (!ed) return;
    const edgesByKey = new Map((ed.edges || []).map((e) => [edgeKey(e), e]));
    el.querySelectorAll('.connection').forEach((connEl) => {
      connEl.classList.remove('dag-edge-condition', 'dag-edge-error', 'dag-edge-inactive', 'dag-edge-active');
      const cls = connEl.className?.baseVal || connEl.getAttribute('class') || '';
      // class pattern: connection node_in_node-{toDf} node_out_node-{fromDf} output_k input_j
      const mIn = cls.match(/node_in_node-(\d+)/);
      const mOut = cls.match(/node_out_node-(\d+)/);
      const mOutCls = cls.match(/\b(output_\d+)\b/);
      const mInCls = cls.match(/\b(input_\d+)\b/);
      if (!mIn || !mOut || !mOutCls || !mInCls) return;
      const edge = edgeFromConn({
        output_id: mOut[1],
        input_id: mIn[1],
        output_class: mOutCls[1],
        input_class: mInCls[1],
      });
      if (!edge) return;
      const stored = edgesByKey.get(edgeKey(edge));
      if (stored?.condition) connEl.classList.add('dag-edge-condition');
      if (edge.out === ERROR_PORT) connEl.classList.add('dag-edge-error');
      if (runStates.size) {
        const fromRun = runStates.get(edge.from);
        const toRun = runStates.get(edge.to);
        const toActive = toRun && !['skipped', 'not_run', 'pending'].includes(toRun.status);
        const fromDone = fromRun && ['succeeded', 'exception', 'failed'].includes(fromRun.status);
        const branchOk = !fromRun?.branch || fromRun.branch === edge.out
          || (fromRun.node_type !== 'logic' && edge.out !== ERROR_PORT);
        if (fromDone && toActive && branchOk) connEl.classList.add('dag-edge-active');
        else if (fromDone) connEl.classList.add('dag-edge-inactive');
      }
    });
  }

  function labelPorts(bizId) {
    const node = findNode(bizId);
    const dfId = mapBizId(bizId);
    if (!node || dfId == null) return;
    const nodeEl = el.querySelector(`#node-${dfId}`);
    if (!nodeEl) return;
    const sides = [
      ['inputs', 'input', node.ports_in],
      ['outputs', 'output', node.ports_out],
    ];
    for (const [container, prefix, ports] of sides) {
      const label = shouldLabelPorts(ports);
      (ports || []).forEach((port, idx) => {
        const portEl = nodeEl.querySelector(`.${container} .${prefix}_${idx + 1}`);
        if (!portEl) return;
        const text = portLabel(node, port);
        portEl.title = text;
        portEl.classList.toggle('dag-port-error', port.name === ERROR_PORT);
        portEl.classList.toggle('dag-port-params', port.name === 'params');
        if (label) portEl.dataset.portLabel = text;
        else delete portEl.dataset.portLabel;
      });
    }
  }

  function rebuildNodeHtml(bizId) {
    const node = findNode(bizId);
    const dfId = mapBizId(bizId);
    if (!node || dfId == null) return;
    const nodeEl = el.querySelector(`#node-${dfId}`);
    if (!nodeEl) return;
    const content = nodeEl.querySelector('.drawflow_content_node');
    if (content) {
      content.innerHTML = nodeHtml(node, runStates.get(bizId));
    }
    nodeEl.classList.remove(...NODE_TYPE_CLASSES, ...RUN_CLASSES);
    nodeEl.classList.add(`dag-type-${node.type || 'composite'}`);
    const run = runStates.get(bizId);
    if (run?.status) nodeEl.classList.add(`dag-run-${run.status}`);
    labelPorts(bizId);
    try {
      editor.updateConnectionNodes(`node-${dfId}`);
    } catch {
      /* ignore */
    }
  }

  function createDfNode(n) {
    const cls = `dag-type-${n.type || 'composite'}`;
    const x = Number.isFinite(Number(n.x)) ? Number(n.x) : 60;
    const y = Number.isFinite(Number(n.y)) ? Number(n.y) : 40;
    const dfId = editor.addNode(
      n.id,
      (n.ports_in || []).length,
      (n.ports_out || []).length,
      x,
      y,
      cls,
      { bizId: n.id, type: n.type, component: n.component },
      nodeHtml(n, runStates.get(n.id)),
    );
    const idStr = String(dfId);
    bizToDf.set(n.id, idStr);
    dfToBiz.set(idStr, n.id);
    return idStr;
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
        createDfNode(n);
      }

      const byId = new Map(nodes.map((n) => [n.id, n]));
      for (const e of edges) {
        const fromDf = mapBizId(e.from);
        const toDf = mapBizId(e.to);
        if (fromDf == null || toDf == null) continue;
        const outIdx = portIndex(byId.get(e.from)?.ports_out, e.out || 'records');
        const inIdx = portIndex(byId.get(e.to)?.ports_in, e.in || 'records');
        if (outIdx == null || inIdx == null) continue;
        editor.addConnection(fromDf, toDf, `output_${outIdx}`, `input_${inIdx}`);
      }

      // Re-apply left/top from editor state (Drawflow + parent-node CSS can drift)
      reapplyNodeLayout(nodes);
      for (const n of nodes) labelPorts(n.id);

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

  /** Rebuild the whole canvas from editor state (e.g. after ports changed). */
  function reload() {
    syncPositionsFromDrawflow();
    const ed = handlers.getEditor?.();
    if (!ed) return;
    const zoomUi = getZoomUi();
    ed.ui = { ...(ed.ui || {}), ...zoomUi };
    loadEditor(ed);
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
    suppressEvents = true;
    try {
      const idStr = createDfNode(node);
      labelPorts(node.id);
      return idStr;
    } finally {
      suppressEvents = false;
    }
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

  /**
   * Drawflow scales the precanvas around its own center (default
   * transform-origin), so screen = c + (p - c) * zoom + translate.
   */
  function precanvasCenter() {
    const pre = editor.precanvas;
    return {
      x: (pre?.clientWidth || el.clientWidth || 0) / 2,
      y: (pre?.clientHeight || el.clientHeight || 0) / 2,
    };
  }

  /** Unscaled node box sizes (offset sizes ignore the canvas zoom transform). */
  function nodeSizes() {
    const sizes = {};
    for (const [bizId, dfId] of bizToDf.entries()) {
      const nodeEl = el.querySelector(`#node-${dfId}`);
      if (nodeEl) sizes[bizId] = { w: nodeEl.offsetWidth, h: nodeEl.offsetHeight };
    }
    return sizes;
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
      const dfId = mapBizId(n.id);
      const nodeEl = dfId != null ? el.querySelector(`#node-${dfId}`) : null;
      const w = nodeEl?.offsetWidth || 200;
      const h = nodeEl?.offsetHeight || 110;
      minX = Math.min(minX, n.x);
      minY = Math.min(minY, n.y);
      maxX = Math.max(maxX, n.x + w);
      maxY = Math.max(maxY, n.y + h);
    }
    const wrap = el.getBoundingClientRect();
    const pad = 60;
    const w = maxX - minX + pad * 2;
    const h = maxY - minY + pad * 2;
    const scale = Math.max(
      editor.zoom_min,
      Math.min(1, Math.min(wrap.width / w, wrap.height / h)),
    );
    const c = precanvasCenter();
    const graphCenter = { x: (minX + maxX) / 2, y: (minY + maxY) / 2 };
    editor.zoom = scale;
    editor.zoom_last_value = scale;
    // 把图的中心放到视口中心：translate = W - c - (G - c) * zoom
    editor.canvas_x = wrap.width / 2 - c.x - (graphCenter.x - c.x) * scale;
    editor.canvas_y = wrap.height / 2 - c.y - (graphCenter.y - c.y) * scale;
    applyCanvasTransform();
  }

  function getZoomUi() {
    return {
      zoom: editor.zoom,
      pan: { x: editor.canvas_x || 0, y: editor.canvas_y || 0 },
    };
  }

  /** Canvas-space point under the center of the visible viewport (for placing new nodes). */
  function viewportCenter() {
    const wrap = el.getBoundingClientRect();
    const zoom = editor.zoom || 1;
    const c = precanvasCenter();
    return {
      x: c.x + (wrap.width / 2 - c.x - (editor.canvas_x || 0)) / zoom,
      y: c.y + (wrap.height / 2 - c.y - (editor.canvas_y || 0)) / zoom,
    };
  }

  function setRunState(states) {
    runStates.clear();
    for (const [nodeId, state] of Object.entries(states || {})) {
      if (state) runStates.set(nodeId, state);
    }
    for (const n of handlers.getEditor?.()?.nodes || []) rebuildNodeHtml(n.id);
    applyConditionStyles();
  }

  function clearRunState() {
    setRunState({});
  }

  function rejectConnection(conn, message) {
    suppressEvents = true;
    try {
      editor.removeSingleConnection(conn.output_id, conn.input_id, conn.output_class, conn.input_class);
    } catch {
      /* ignore */
    } finally {
      suppressEvents = false;
    }
    if (message) handlers.onReject?.(message);
  }

  function wouldCreateCycle(edges, from, to) {
    // adding from→to creates a cycle iff `from` is reachable from `to`
    const adj = new Map();
    for (const e of edges) {
      if (!adj.has(e.from)) adj.set(e.from, []);
      adj.get(e.from).push(e.to);
    }
    const stack = [to];
    const seen = new Set();
    while (stack.length) {
      const cur = stack.pop();
      if (cur === from) return true;
      if (seen.has(cur)) continue;
      seen.add(cur);
      stack.push(...(adj.get(cur) || []));
    }
    return false;
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
    handlers.onChange?.({ kind: 'move' });
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
      runStates.delete(bizId);
      handlers.onSelectNode?.(null);
      handlers.onChange?.({ kind: 'remove-node' });
    }
  });

  editor.on('connectionCreated', (conn) => {
    if (suppressEvents) return;
    const ed = handlers.getEditor?.();
    const edge = edgeFromConn(conn);
    if (!edge || !ed) {
      rejectConnection(conn, t('dag.connect.unknownPort'));
      return;
    }
    if (edge.from === edge.to) {
      rejectConnection(conn, t('dag.connect.selfLoop'));
      return;
    }
    const fromNode = findNode(edge.from);
    const toNode = findNode(edge.to);
    const outPort = (fromNode?.ports_out || []).find((p) => p.name === edge.out);
    const inPort = (toNode?.ports_in || []).find((p) => p.name === edge.in);
    if (!portsCompatible(outPort, inPort)) {
      rejectConnection(conn, t('dag.connect.incompatible', { out: edge.out, in: edge.in }));
      return;
    }
    if (findEdge(edge)) {
      return; // Drawflow already dedupes identical connections; keep state as is
    }
    if (wouldCreateCycle(ed.edges || [], edge.from, edge.to)) {
      rejectConnection(conn, t('dag.connect.cycle'));
      return;
    }
    ed.edges = [...(ed.edges || []), edge];

    // collector → collector records edge: auto from_upstream
    // （error 端口送来的是错误信息不是数据：兜底采集器应继续用任务参数）
    if (
      fromNode?.type === 'collector'
      && toNode?.type === 'collector'
      && edge.in === 'records'
      && edge.out !== ERROR_PORT
      && !toNode.config?.from_upstream
    ) {
      toNode.config = {
        ...(toNode.config || {}),
        from_upstream: { auto: true },
      };
      rebuildNodeHtml(toNode.id);
    }
    requestAnimationFrame(() => applyConditionStyles());
    handlers.onChange?.({ kind: 'connect' });
  });

  editor.on('connectionRemoved', (conn) => {
    if (suppressEvents) return;
    const edge = edgeFromConn(conn);
    const ed = handlers.getEditor?.();
    if (!edge || !ed) return;
    const key = edgeKey(edge);
    ed.edges = (ed.edges || []).filter((e) => edgeKey(e) !== key);
    selectedEdgeKey = null;
    handlers.onSelectEdge?.(null);
    handlers.onChange?.({ kind: 'disconnect' });
  });

  editor.on('connectionSelected', (conn) => {
    if (suppressEvents) return;
    const edge = edgeFromConn(conn);
    if (!edge) return;
    const full = findEdge(edge) || edge;
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
    reload,
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
    viewportCenter,
    nodeSizes,
    setRunState,
    clearRunState,
    getSelectedEdgeKey: () => selectedEdgeKey,
    destroy() {
      window.removeEventListener('keydown', onKeyDown);
      window.removeEventListener('keyup', onKeyUp);
      window.removeEventListener('mousemove', onWindowMouseMove);
      window.removeEventListener('mouseup', onWindowMouseUp);
      try {
        editor.clear();
      } catch {
        /* ignore */
      }
      el.innerHTML = '';
      bizToDf.clear();
      dfToBiz.clear();
      runStates.clear();
    },
    /** @internal expose for debugging */
    _df: editor,
    _bizToDf: bizToDf,
  };
}
