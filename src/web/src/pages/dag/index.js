import 'drawflow/dist/drawflow.min.css';
import './style.css';

import { api, toast, escapeHtml } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { invalidatePipelineCache } from '../../core/pipelines.js';
import { apiToEditor, editorToApi, defaultPortsForType, syncNodePorts, edgeKey } from './adapter.js';
import { validateEditor } from './validate.js';
import { createCanvas } from './canvas.js';
import { mountPalette } from './palette.js';
import { mountSavedList } from './saved-list.js';
import { mountInspector } from './inspector.js';
import { loadCollectorMetaMap, invalidateCollectorMetaMap } from './schemas.js';
import { createHistory, snapshotOf } from './history.js';
import { autoLayout } from './layout.js';
import { applyNodeEvent, summarizeRun, TERMINAL_TASK_STATUSES } from './runTrace.js';
import { openRunDialog } from './runDialog.js';

function emptyEditor() {
  return {
    name: '',
    nodes: [],
    edges: [],
    ui: { zoom: 1, pan: { x: 0, y: 0 } },
  };
}

/** @type {{ name: string, nodes: object[], edges: object[], ui: object }} */
let editorState = emptyEditor();

let activeName = '';
/** name + graph snapshot at last load/save; used for the unsaved indicator */
let savedSnapshot = '';
let canvasApi = null;
let paletteApi = null;
let savedListApi = null;
let inspectorApi = null;
let selection = { kind: null };
let themeListener = null;
let languageListener = null;
let keyListener = null;
let pageContainer = null;
let storeRef = null;
let storeUnsub = null;
let clipboard = null;
const history = createHistory();

const RUN_POLL_MS = 3000;
/** live run being visualized on the canvas */
let run = {
  taskId: '',
  taskName: '',
  status: '',
  states: {},
  pollTimer: null,
  renderQueued: false,
  dagName: '',
};

const LAST_DAG_KEY = 'gamedata-autoflux.dag.active';
const LAST_RUN_PREFIX = 'gamedata-autoflux.dag.lastRun.';

function rememberActiveDag(name) {
  try {
    if (name) localStorage.setItem(LAST_DAG_KEY, name);
    else localStorage.removeItem(LAST_DAG_KEY);
  } catch {
    /* ignore */
  }
}

function readRememberedDag() {
  try {
    return localStorage.getItem(LAST_DAG_KEY) || '';
  } catch {
    return '';
  }
}

function rememberLastRun(name, taskId) {
  try {
    if (name && taskId) localStorage.setItem(`${LAST_RUN_PREFIX}${name}`, taskId);
  } catch {
    /* ignore */
  }
}

function readLastRun(name) {
  try {
    return (name && localStorage.getItem(`${LAST_RUN_PREFIX}${name}`)) || '';
  } catch {
    return '';
  }
}

function getEditor() {
  return editorState;
}

function setNameInput(name) {
  const el = document.getElementById('dag-name-input');
  if (el) el.value = name || '';
}

function readNameInput() {
  const el = document.getElementById('dag-name-input');
  return (el?.value || '').trim();
}

function applyDagTheme() {
  const wrap = document.getElementById('dag-canvas-wrap');
  if (!wrap) return;
  const theme = document.documentElement.dataset.theme || 'dark';
  wrap.dataset.theme = theme;
}

function isPageActive() {
  if (storeRef?.get?.('activeTab')) return storeRef.get('activeTab') === 'dag';
  return Boolean(pageContainer && pageContainer.offsetParent !== null);
}

function refreshInspector() {
  if (!inspectorApi) return;
  if (selection.kind === 'node' && selection.id) {
    const node = editorState.nodes.find((n) => n.id === selection.id);
    if (node) {
      inspectorApi.render({ kind: 'node', node });
      return;
    }
  }
  if (selection.kind === 'edge' && selection.edge) {
    const key = edgeKey(selection.edge);
    const edge = editorState.edges.find((e) => edgeKey(e) === key) || selection.edge;
    inspectorApi.render({ kind: 'edge', edge });
    return;
  }
  inspectorApi.render({ kind: null });
}

function showValidateBar(result) {
  const bar = document.getElementById('dag-validate-bar');
  if (!bar) return;
  if (!result) {
    bar.textContent = '';
    bar.className = 'text-xs text-muted mb-2 px-1';
    return;
  }
  const warnings = result.warnings || [];
  if (result.ok && !warnings.length) {
    bar.className = 'text-xs mb-2 px-1 is-ok';
    bar.textContent = t('dag.validateOk');
    return;
  }
  bar.className = `text-xs mb-2 px-1 ${result.ok ? 'has-warnings' : 'has-errors'}`;
  const rows = [
    ...(result.issues || []).map((iss) => ({ ...iss, level: 'error' })),
    ...warnings.map((iss) => ({ ...iss, level: 'warning' })),
  ];
  bar.innerHTML = rows
    .map(
      (iss) =>
        `<span class="dag-issue is-${iss.level}" data-node-id="${escapeHtml(iss.nodeId || '')}">${iss.level === 'warning' ? '⚠ ' : ''}${escapeHtml(iss.message)}</span>`,
    )
    .join('');
  bar.querySelectorAll('.dag-issue').forEach((el) => {
    el.addEventListener('click', () => {
      const nid = el.dataset.nodeId;
      if (nid) {
        selection = { kind: 'node', id: nid };
        canvasApi?.selectNode(nid);
        refreshInspector();
      }
    });
  });
}

function currentSnapshot() {
  return `${readNameInput()}\n${snapshotOf(editorState)}`;
}

function markSaved() {
  savedSnapshot = currentSnapshot();
  updateDirtyIndicator();
}

function isDirty() {
  return currentSnapshot() !== savedSnapshot;
}

function updateDirtyIndicator() {
  const el = document.getElementById('dag-dirty');
  if (!el) return;
  const dirty = editorState.nodes.length > 0 && isDirty();
  el.hidden = !dirty;
  el.textContent = t('dag.unsaved');
}

function updateHistoryButtons() {
  const undoBtn = document.getElementById('btn-dag-undo');
  const redoBtn = document.getElementById('btn-dag-redo');
  if (undoBtn) undoBtn.disabled = !history.canUndo();
  if (redoBtn) redoBtn.disabled = !history.canRedo();
}

function commitHistory() {
  history.commit(editorState);
  updateHistoryButtons();
  updateDirtyIndicator();
}

function syncFromCanvas() {
  if (!canvasApi) return;
  canvasApi.syncPositions();
  canvasApi.syncEdges();
  const zoomUi = canvasApi.getZoomUi?.();
  if (zoomUi) {
    editorState.ui = { ...(editorState.ui || {}), ...zoomUi };
  }
}

function loadPayload(name, payload, options = {}) {
  const { quiet = false, restoreRun = true } = options;
  editorState = apiToEditor(payload);
  editorState.name = name || editorState.name;
  activeName = name || editorState.name;
  rememberActiveDag(activeName);
  setNameInput(editorState.name);
  selection = { kind: null };
  clearRun({ keepRemembered: true });
  canvasApi?.loadEditor(editorState);
  history.reset(editorState);
  updateHistoryButtons();
  markSaved();
  refreshInspector();
  showValidateBar(null);
  savedListApi?.refresh();
  if (!quiet) {
    toast(t('dag.loadedToast', { name: editorState.name }), 'success');
  }
  const lastRun = restoreRun ? readLastRun(activeName) : '';
  if (lastRun) {
    loadRunTrace(lastRun, { dagName: activeName, passive: true }).catch(() => { /* stale run id */ });
  }
}

function clearGraph() {
  editorState = emptyEditor();
  activeName = '';
  rememberActiveDag('');
  setNameInput('');
  selection = { kind: null };
  clearRun({ keepRemembered: true });
  canvasApi?.loadEditor(editorState);
  history.reset(editorState);
  updateHistoryButtons();
  markSaved();
  refreshInspector();
  showValidateBar(null);
  savedListApi?.refresh();
}

function nextNodeId(type) {
  const used = new Set(editorState.nodes.map((n) => n.id));
  let i = 1;
  while (used.has(`${type}_${i}`)) i += 1;
  return `${type}_${i}`;
}

function clone(value) {
  return JSON.parse(JSON.stringify(value ?? null));
}

function placeNewNode() {
  const center = canvasApi?.viewportCenter?.() || { x: 200, y: 120 };
  // 轻微错开，避免连续添加的节点完全重叠
  const offset = (editorState.nodes.length % 5) * 24;
  return { x: Math.round(center.x - 100 + offset), y: Math.round(center.y - 50 + offset) };
}

function addNode(type, component, definition = null) {
  const id = nextNodeId(type);
  const fallbackPorts = defaultPortsForType(type);
  const ports = {
    ports_in: Array.isArray(definition?.ports_in) ? definition.ports_in : fallbackPorts.ports_in,
    ports_out: Array.isArray(definition?.ports_out) ? definition.ports_out : fallbackPorts.ports_out,
  };
  const pos = placeNewNode();
  const node = syncNodePorts({
    id,
    type,
    component,
    config: type === 'logic' ? clone(definition?.default_config || {}) : {},
    policy: {},
    ports_in: ports.ports_in.map((p) => ({ ...p })),
    ports_out: ports.ports_out.map((p) => ({ ...p })),
    x: pos.x,
    y: pos.y,
    label: '',
  });
  editorState.nodes.push(node);
  canvasApi?.addNodeToCanvas(node);
  selection = { kind: 'node', id: node.id };
  canvasApi?.selectNode?.(node.id);
  refreshInspector();
  commitHistory();
}

async function deleteSaved(name) {
  if (!name) return;
  if (!window.confirm(t('dag.confirmDelete', { name }))) return;
  try {
    await api(`/dags/${encodeURIComponent(name)}?confirm=true`, { method: 'DELETE' });
    invalidatePipelineCache();
    if (activeName === name) {
      clearGraph();
    }
    toast(t('dag.deletedToast', { name }), 'success');
    await savedListApi?.refresh();
  } catch (e) {
    toast(t('message.deleteFailed', { error: e.message || e }), 'error');
  }
}

/** Server warnings with a stable meaning get localized; errors keep the precise server text. */
const SERVER_WARNING_KEYS = {
  error_branch_unconnected: 'dag.check.error_branch_unconnected',
  unreachable_node: 'dag.server.unreachable_node',
  conditional_required_input: 'dag.server.conditional_required_input',
};

function serverIssueEntry(item) {
  const key = SERVER_WARNING_KEYS[item.code];
  const message = key && item.node_id ? t(key, { node: item.node_id }) : item.message;
  return { code: item.code, message, nodeId: item.node_id || undefined };
}

function dedupeIssues(list) {
  const seen = new Set();
  return list.filter((item) => {
    const key = `${item.code}|${item.nodeId || ''}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function serverIssues(body) {
  const issues = [];
  const warnings = [];
  for (const item of body?.issues || []) {
    const entry = serverIssueEntry(item);
    if (item.severity === 'warning') warnings.push(entry);
    else issues.push(entry);
  }
  return { ok: issues.length === 0, issues, warnings };
}

/** Client checks first; when they pass, ask the server (authoritative) too. */
async function runValidate({ silent = false } = {}) {
  syncFromCanvas();
  editorState.name = readNameInput();
  const local = validateEditor(editorState, { t });
  if (!local.ok) {
    showValidateBar(local);
    if (!silent) toast(t('dag.validateFail', { count: local.issues.length }), 'error');
    return local;
  }
  let result = local;
  try {
    const body = await api('/dags/validate', { method: 'POST', body: JSON.stringify(editorToApi(editorState)) });
    const remote = serverIssues(body);
    result = {
      ok: remote.ok,
      issues: remote.issues,
      warnings: dedupeIssues([...local.warnings, ...remote.warnings]),
    };
  } catch (e) {
    result = { ok: false, issues: [{ code: 'server', message: e.message || String(e) }], warnings: local.warnings };
  }
  showValidateBar(result);
  if (!silent) {
    if (result.ok) toast(t('dag.validateOk'), 'success');
    else toast(t('dag.validateFail', { count: result.issues.length }), 'error');
  }
  return result;
}

/** @returns {Promise<boolean>} */
async function saveGraph({ quiet = false } = {}) {
  syncFromCanvas();
  editorState.name = readNameInput();
  const result = validateEditor(editorState, { t });
  showValidateBar(result);
  if (!result.ok) {
    toast(t('dag.saveBlocked', { count: result.issues.length }), 'error');
    return false;
  }

  const name = editorState.name;
  const saved = savedListApi?.getSaved?.() || {};
  if (saved[name] && name !== activeName) {
    if (!window.confirm(t('dag.confirmOverwrite', { name }))) {
      return false;
    }
  }

  // Sync twice: positions first, then edges/zoom after layout settles
  syncFromCanvas();
  const payload = editorToApi(editorState);
  try {
    const resp = await api('/dags', { method: 'POST', body: JSON.stringify(payload) });
    activeName = name;
    rememberActiveDag(name);
    invalidatePipelineCache();
    // Prefer server echo so ui coords match what was persisted
    const savedPayload = resp?.config && typeof resp.config === 'object'
      ? resp.config
      : await api(`/dags/${encodeURIComponent(name)}`).catch(() => payload);
    const keepSelection = selection;
    editorState = apiToEditor(savedPayload);
    editorState.name = name;
    canvasApi?.loadEditor(editorState);
    canvasApi?.setRunState(run.dagName === name ? run.states : {});
    selection = keepSelection;
    history.commit(editorState);
    updateHistoryButtons();
    markSaved();
    refreshInspector();
    const warningDetails = Array.isArray(resp?.warning_details) ? resp.warning_details : [];
    if (warningDetails.length) {
      showValidateBar({
        ok: true,
        issues: [],
        warnings: dedupeIssues(warningDetails.map(serverIssueEntry)),
      });
    }
    if (!quiet) toast(t('dag.savedToast', { name }), 'success');
    await savedListApi?.refresh();
    return true;
  } catch (e) {
    toast(t('dag.saveFail', { error: e.message || e }), 'error');
    return false;
  }
}

/**
 * @param {string} id
 * @param {{ label?: string, config?: object, policy?: object }} patch
 * @param {{ rerender?: boolean, structural?: boolean }} [meta]
 */
function onUpdateNode(id, patch, meta = {}) {
  const node = editorState.nodes.find((n) => n.id === id);
  if (!node) return;
  if (patch.label !== undefined) node.label = patch.label;
  if (patch.config !== undefined) node.config = patch.config;
  if (patch.policy !== undefined) node.policy = patch.policy;

  const before = JSON.stringify([node.ports_in, node.ports_out]);
  syncNodePorts(node);
  const portsChanged = before !== JSON.stringify([node.ports_in, node.ports_out]);
  if (portsChanged) {
    // 删掉指向已不存在端口的边，再整体重建画布（Drawflow 端口数随之变化）
    const outNames = new Set(node.ports_out.map((p) => p.name));
    const inNames = new Set(node.ports_in.map((p) => p.name));
    editorState.edges = editorState.edges.filter((e) => (
      (e.from !== id || outNames.has(e.out || 'records'))
      && (e.to !== id || inNames.has(e.in || 'records'))
    ));
    canvasApi?.reload();
    canvasApi?.setRunState(run.states);
    if (selection.kind === 'node' && selection.id === id) canvasApi?.selectNode(id);
  } else {
    canvasApi?.rebuildNodeHtml(id);
  }
  commitHistory();
  if (meta.rerender !== false && selection.kind === 'node' && selection.id === id) {
    refreshInspector();
  }
}

function onUpdateEdge(edgeRef, patch) {
  const key = edgeKey(edgeRef);
  const edge = editorState.edges.find((e) => edgeKey(e) === key);
  if (!edge) return;
  if (patch.condition !== undefined) {
    edge.condition = patch.condition || null;
  }
  selection = { kind: 'edge', edge: { ...edge } };
  canvasApi?.applyConditionStyles();
  commitHistory();
  refreshInspector();
}

function deleteNode(id) {
  const node = editorState.nodes.find((n) => n.id === id);
  if (!node) return;
  editorState.nodes = editorState.nodes.filter((n) => n.id !== id);
  editorState.edges = editorState.edges.filter((e) => e.from !== id && e.to !== id);
  selection = { kind: null };
  canvasApi?.removeNodeFromCanvas(id);
  canvasApi?.setRunState(run.states);
  commitHistory();
  refreshInspector();
}

function deleteEdge(edgeRef) {
  const key = edgeKey(edgeRef);
  const before = (editorState.edges || []).length;
  editorState.edges = editorState.edges.filter((e) => edgeKey(e) !== key);
  if (editorState.edges.length === before) return;
  selection = { kind: null };
  canvasApi?.reload();
  canvasApi?.setRunState(run.states);
  commitHistory();
  refreshInspector();
}

function restoreSnapshot(snapshot) {
  if (!snapshot) return;
  editorState.nodes = snapshot.nodes || [];
  editorState.edges = snapshot.edges || [];
  syncFromCanvasZoomOnly();
  canvasApi?.loadEditor(editorState);
  canvasApi?.setRunState(run.states);
  if (selection.kind === 'node' && !editorState.nodes.some((n) => n.id === selection.id)) {
    selection = { kind: null };
  }
  if (selection.kind === 'edge') selection = { kind: null };
  if (selection.kind === 'node') canvasApi?.selectNode(selection.id);
  refreshInspector();
  updateHistoryButtons();
  updateDirtyIndicator();
}

function syncFromCanvasZoomOnly() {
  const zoomUi = canvasApi?.getZoomUi?.();
  if (zoomUi) editorState.ui = { ...(editorState.ui || {}), ...zoomUi };
}

function undo() {
  syncFromCanvas();
  history.commit(editorState); // 未提交的拖动也先入栈
  restoreSnapshot(history.undo());
}

function redo() {
  restoreSnapshot(history.redo());
}

function copySelection() {
  if (selection.kind !== 'node') return false;
  const node = editorState.nodes.find((n) => n.id === selection.id);
  if (!node) return false;
  clipboard = clone(node);
  toast(t('dag.copied', { id: node.id }), 'info');
  return true;
}

function pasteClipboard() {
  if (!clipboard) return;
  syncFromCanvas();
  const node = clone(clipboard);
  node.id = nextNodeId(node.type);
  node.x = (Number(node.x) || 0) + 40;
  node.y = (Number(node.y) || 0) + 40;
  clipboard.x = node.x;
  clipboard.y = node.y;
  editorState.nodes.push(node);
  canvasApi?.addNodeToCanvas(node);
  selection = { kind: 'node', id: node.id };
  canvasApi?.selectNode(node.id);
  refreshInspector();
  commitHistory();
}

function duplicateSelection() {
  if (copySelection()) pasteClipboard();
}

function applyAutoLayout() {
  if (!editorState.nodes.length) return;
  syncFromCanvas();
  const positions = autoLayout(editorState.nodes, editorState.edges, {
    sizes: canvasApi?.nodeSizes?.() || {},
  });
  for (const n of editorState.nodes) {
    const pos = positions[n.id];
    if (pos) {
      n.x = pos.x;
      n.y = pos.y;
    }
  }
  canvasApi?.loadEditor(editorState);
  canvasApi?.setRunState(run.states);
  requestAnimationFrame(() => canvasApi?.fitView());
  commitHistory();
}

function exportGraph() {
  syncFromCanvas();
  editorState.name = readNameInput();
  const payload = editorToApi(editorState);
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${payload.name || 'dag'}.dag.json`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function importGraph(file) {
  if (!file) return;
  try {
    const payload = JSON.parse(await file.text());
    if (!payload || !Array.isArray(payload.nodes)) throw new Error(t('dag.importInvalid'));
    if (editorState.nodes.length && !window.confirm(t('dag.confirmImport'))) return;
    const name = String(payload.name || '').trim();
    editorState = apiToEditor(payload);
    editorState.name = name;
    activeName = '';
    setNameInput(name);
    selection = { kind: null };
    clearRun({ keepRemembered: true });
    canvasApi?.loadEditor(editorState);
    history.reset(editorState);
    updateHistoryButtons();
    savedSnapshot = '';
    updateDirtyIndicator();
    refreshInspector();
    requestAnimationFrame(() => canvasApi?.fitView());
    const check = validateEditor(editorState, { t });
    showValidateBar(check);
    toast(t('dag.imported', { name: name || '-' }), check.ok ? 'success' : 'info');
  } catch (e) {
    toast(t('dag.importFail', { error: e.message || e }), 'error');
  }
}

// ── Run & trace ─────────────────────────────────────────────────────

function stopPolling() {
  if (run.pollTimer) {
    clearInterval(run.pollTimer);
    run.pollTimer = null;
  }
}

function clearRun({ keepRemembered = false } = {}) {
  stopPolling();
  if (!keepRemembered && run.dagName && run.taskId) {
    try {
      localStorage.removeItem(`${LAST_RUN_PREFIX}${run.dagName}`);
    } catch {
      /* ignore */
    }
  }
  run = { taskId: '', taskName: '', status: '', states: {}, pollTimer: null, renderQueued: false, dagName: '' };
  canvasApi?.clearRunState();
  renderRunBar();
}

function renderRunBar() {
  const bar = document.getElementById('dag-run-bar');
  if (!bar) return;
  if (!run.taskId) {
    bar.hidden = true;
    bar.innerHTML = '';
    return;
  }
  const summary = summarizeRun(run.states);
  const done = (summary.succeeded || 0) + (summary.failed || 0) + (summary.exception || 0)
    + (summary.skipped || 0) + (summary.not_run || 0);
  const terminal = TERMINAL_TASK_STATUSES.has(run.status);
  const statusKey = terminal ? run.status : 'running';
  const chips = ['succeeded', 'running', 'retrying', 'failed', 'exception', 'skipped', 'not_run']
    .filter((s) => summary[s])
    .map((s) => `<span class="dag-run-pill is-${s}">${escapeHtml(t(`dag.run.status.${s}`))} ${summary[s]}</span>`)
    .join('');
  bar.hidden = false;
  bar.className = `dag-run-bar is-${statusKey}`;
  bar.innerHTML = `
    <span class="dag-run-bar-status">${escapeHtml(t(`dag.run.task.${statusKey}`))}</span>
    <span class="dag-run-bar-task" title="${escapeHtml(run.taskId)}">${escapeHtml(run.taskName || run.taskId)}</span>
    <span class="dag-run-bar-progress">${escapeHtml(t('dag.run.progress', { done, total: summary.total }))}</span>
    <span class="dag-run-bar-chips">${chips}</span>
    <span class="dag-run-bar-actions">
      <button type="button" class="btn btn-ghost btn-sm" data-run-open>${escapeHtml(t('dag.run.openTask'))}</button>
      <button type="button" class="btn btn-ghost btn-sm" data-run-clear>${escapeHtml(t('dag.run.clear'))}</button>
    </span>
  `;
  bar.querySelector('[data-run-open]')?.addEventListener('click', () => {
    window.navigate?.('tasks', {});
    setTimeout(() => window._tasksPage?._viewDetail?.(run.taskId), 300);
  });
  bar.querySelector('[data-run-clear]')?.addEventListener('click', () => clearRun());
}

function renderRunState() {
  if (run.renderQueued) return;
  run.renderQueued = true;
  requestAnimationFrame(() => {
    run.renderQueued = false;
    if (run.dagName && run.dagName !== activeName) return;
    canvasApi?.setRunState(run.states);
    renderRunBar();
    // 正在属性面板里输入时不打断；否则刷新选中节点的"最近运行"
    const insp = document.getElementById('dag-inspector');
    if (selection.kind === 'node' && !(insp && insp.contains(document.activeElement))) {
      refreshInspector();
    }
  });
}

async function loadRunTrace(taskId, { dagName = activeName, passive = false } = {}) {
  const data = await api(`/tasks/${encodeURIComponent(taskId)}/dag-trace`);
  if (passive && dagName !== activeName) return;
  // 节点基线：图里所有节点先占位为 pending，事件覆盖
  const states = {};
  for (const n of editorState.nodes) {
    if (n.type === 'input') continue;
    states[n.id] = { node_id: n.id, node_type: n.type, component: n.component, status: 'pending' };
  }
  for (const [nodeId, state] of Object.entries(data?.nodes || {})) {
    states[nodeId] = { ...(states[nodeId] || {}), ...state };
  }
  if (TERMINAL_TASK_STATUSES.has(data?.status)) {
    for (const state of Object.values(states)) {
      if (state.status === 'pending') state.status = 'not_run';
    }
  }
  stopPolling();
  run = {
    ...run,
    taskId,
    taskName: data?.task_name || taskId,
    status: data?.status || '',
    states,
    dagName,
  };
  renderRunState();
  if (!TERMINAL_TASK_STATUSES.has(run.status)) {
    run.pollTimer = setInterval(() => {
      refreshRunTrace().catch(() => { /* transient */ });
    }, RUN_POLL_MS);
  }
}

async function refreshRunTrace() {
  if (!run.taskId) return;
  const taskId = run.taskId;
  const data = await api(`/tasks/${encodeURIComponent(taskId)}/dag-trace`);
  if (run.taskId !== taskId) return;
  for (const [nodeId, state] of Object.entries(data?.nodes || {})) {
    run.states[nodeId] = { ...(run.states[nodeId] || {}), ...state };
  }
  const wasTerminal = TERMINAL_TASK_STATUSES.has(run.status);
  run.status = data?.status || run.status;
  if (TERMINAL_TASK_STATUSES.has(run.status)) {
    stopPolling();
    for (const state of Object.values(run.states)) {
      if (state.status === 'pending') state.status = 'not_run';
    }
    if (!wasTerminal) {
      toast(
        t(run.status === 'success' ? 'dag.run.finishedOk' : 'dag.run.finishedFail', { name: run.taskName }),
        run.status === 'success' ? 'success' : 'error',
      );
    }
  }
  renderRunState();
}

async function createRunTask({ name, targets }) {
  try {
    const created = await api('/tasks', {
      method: 'POST',
      body: JSON.stringify({ name, pipeline_name: activeName, targets, config: {} }),
    });
    if (!created?.id) throw new Error('missing task id');
    rememberLastRun(activeName, created.id);
    toast(t('dag.run.started', { name: created.name || name }), 'success');
    await loadRunTrace(created.id, { dagName: activeName });
    return true;
  } catch (e) {
    toast(t('dag.run.startFail', { error: e.message || e }), 'error');
    return false;
  }
}

async function startRun() {
  syncFromCanvas();
  const name = readNameInput();
  if (!name || name !== activeName || isDirty()) {
    // 运行的是已保存的版本：先保存当前改动
    const ok = await saveGraph({ quiet: true });
    if (!ok) return;
    toast(t('dag.run.savedBeforeRun', { name: activeName }), 'info');
  }
  await loadCollectorMetaMap();
  openRunDialog({ editor: editorState, dagName: activeName, onSubmit: createRunTask });
}

function onStoreChange(key, value) {
  if (!run.taskId) return;
  if (key === 'taskEvent' && value?.task_id === run.taskId) {
    applyNodeEvent(run.states, value);
    renderRunState();
  } else if (key === 'taskUpdate' && value?.id === run.taskId) {
    const status = value.status;
    if (status && status !== run.status) {
      if (TERMINAL_TASK_STATUSES.has(status)) {
        // 以服务端折叠结果为准收尾（事件可能在断线期间丢失）
        refreshRunTrace().catch(() => { /* ignore */ });
      } else {
        run.status = status;
        renderRunBar();
      }
    }
  }
}

// ── Toolbar & keyboard ─────────────────────────────────────────────

function bindToolbar(container) {
  const on = (id, handler) => container.querySelector(id)?.addEventListener('click', handler);
  on('#btn-dag-save', () => { saveGraph(); });
  on('#btn-dag-validate', () => { runValidate(); });
  on('#btn-dag-fit', () => { canvasApi?.fitView(); });
  on('#btn-dag-layout', () => { applyAutoLayout(); });
  on('#btn-dag-undo', () => { undo(); });
  on('#btn-dag-redo', () => { redo(); });
  on('#btn-dag-export', () => { exportGraph(); });
  on('#btn-dag-import', () => { container.querySelector('#dag-import-file')?.click(); });
  on('#btn-dag-run', () => { startRun(); });
  on('#btn-dag-clear', () => {
    if (editorState.nodes.length && !window.confirm(t('dag.confirmClear'))) return;
    clearGraph();
  });
  on('#btn-dag-refresh-list', () => { savedListApi?.refresh(); });
  container.querySelector('#dag-import-file')?.addEventListener('change', (ev) => {
    const file = ev.target.files?.[0];
    ev.target.value = '';
    importGraph(file);
  });
  container.querySelector('#dag-name-input')?.addEventListener('input', () => updateDirtyIndicator());
}

function onKeyDown(ev) {
  if (!isPageActive()) return;
  const mod = ev.ctrlKey || ev.metaKey;
  if (!mod) return;
  const key = ev.key.toLowerCase();
  if (key === 's') {
    ev.preventDefault();
    saveGraph();
    return;
  }
  const target = ev.target;
  if (target?.closest?.('input, textarea, select, [contenteditable="true"]')) return;
  if (key === 'z' && !ev.shiftKey) {
    ev.preventDefault();
    undo();
  } else if ((key === 'z' && ev.shiftKey) || key === 'y') {
    ev.preventDefault();
    redo();
  } else if (key === 'c') {
    if (copySelection()) ev.preventDefault();
  } else if (key === 'v') {
    if (clipboard) {
      ev.preventDefault();
      pasteClipboard();
    }
  } else if (key === 'd') {
    ev.preventDefault();
    duplicateSelection();
  }
}

// Global bridges for any leftover onclick handlers
window.dagSaveGraph = () => saveGraph();
window.dagClearGraph = () => clearGraph();
window.dagRefreshList = async () => {
  await savedListApi?.refresh();
};
window.dagLoadGraph = (name) => {
  const saved = savedListApi?.getSaved?.() || {};
  if (!saved[name]) {
    toast(t('dag.notFound', { name }), 'error');
    return;
  }
  loadPayload(name, saved[name]);
};
window.dagDeleteSaved = (name) => deleteSaved(name);
window.dagAddNode = (type, component) => addNode(type, component);

export default {
  init(container, store) {
    this.container = container;
    pageContainer = container;
    storeRef = store || null;

    const drawflowEl = container.querySelector('#drawflow');
    if (drawflowEl) {
      canvasApi = createCanvas(drawflowEl, {
        getEditor,
        onSelectNode(id) {
          if (id) {
            selection = { kind: 'node', id };
          } else if (selection.kind === 'node') {
            selection = { kind: null };
          }
          refreshInspector();
        },
        onSelectEdge(edge) {
          if (edge) {
            selection = { kind: 'edge', edge };
          } else if (selection.kind === 'edge') {
            selection = { kind: null };
          }
          refreshInspector();
        },
        onChange() {
          // keep selection if still valid
          if (selection.kind === 'node' && selection.id) {
            if (!editorState.nodes.some((n) => n.id === selection.id)) {
              selection = { kind: null };
              refreshInspector();
            }
          }
          commitHistory();
        },
        onReject(message) {
          toast(message, 'error');
        },
      });
    }

    inspectorApi = mountInspector(container.querySelector('#dag-inspector'), {
      onUpdateNode,
      onUpdateEdge,
      onDeleteNode: deleteNode,
      onDeleteEdge: deleteEdge,
      getEditor,
      getRunState: (nodeId) => (run.dagName === activeName ? run.states[nodeId] : null) || null,
    });

    paletteApi = mountPalette(container.querySelector('#dag-palette'), {
      onAdd: (type, component, definition) => addNode(type, component, definition),
    });

    savedListApi = mountSavedList(container.querySelector('#dag-saved-list'), {
      getActiveName: () => activeName,
      onLoad: (name, payload) => {
        if (isDirty() && editorState.nodes.length && name !== activeName
          && !window.confirm(t('dag.confirmDiscard'))) return;
        loadPayload(name, payload);
      },
      onDelete: (name) => deleteSaved(name),
    });

    history.reset(editorState);
    markSaved();
    updateHistoryButtons();

    // Restore last opened graph after list fetch (page reload / service restart UX)
    Promise.resolve(savedListApi.refresh?.()).then((saved) => {
      const last = readRememberedDag();
      if (!last || !saved?.[last]) return;
      if (editorState.nodes.length > 0) return;
      loadPayload(last, saved[last], { quiet: true });
    }).catch(() => { /* ignore restore errors */ });

    bindToolbar(container);
    applyDagTheme();
    if (!themeListener) {
      themeListener = () => applyDagTheme();
      window.addEventListener('themechange', themeListener);
    }
    if (!languageListener) {
      languageListener = () => {
        refreshInspector();
        paletteApi?.refresh?.();
        savedListApi?.refresh?.();
        for (const n of editorState.nodes) canvasApi?.rebuildNodeHtml?.(n.id);
        renderRunBar();
        updateDirtyIndicator();
      };
      window.addEventListener('languagechange', languageListener);
    }
    if (!keyListener) {
      keyListener = onKeyDown;
      window.addEventListener('keydown', keyListener);
    }
    if (store?.subscribe && !storeUnsub) {
      storeUnsub = store.subscribe(onStoreChange);
    }
    loadCollectorMetaMap().then(() => {
      // refresh node cards with input/output hints once metadata ready
      for (const n of editorState.nodes) {
        canvasApi?.rebuildNodeHtml?.(n.id);
      }
      refreshInspector();
    });
    refreshInspector();
    return this;
  },

  destroy() {
    stopPolling();
    canvasApi?.destroy();
    canvasApi = null;
    if (themeListener) {
      window.removeEventListener('themechange', themeListener);
      themeListener = null;
    }
    if (languageListener) {
      window.removeEventListener('languagechange', languageListener);
      languageListener = null;
    }
    if (keyListener) {
      window.removeEventListener('keydown', keyListener);
      keyListener = null;
    }
    if (storeUnsub) {
      storeUnsub();
      storeUnsub = null;
    }
  },

  async refresh() {
    // 插件可能在别的页面被启停：重新拉组件目录
    invalidateCollectorMetaMap();
    await loadCollectorMetaMap();
    await Promise.all([
      paletteApi?.refresh?.(),
      savedListApi?.refresh?.(),
    ]);
    for (const n of editorState.nodes) {
      canvasApi?.rebuildNodeHtml?.(n.id);
    }
    refreshInspector();
    applyDagTheme();
    if (run.taskId && !TERMINAL_TASK_STATUSES.has(run.status)) {
      refreshRunTrace().catch(() => { /* ignore */ });
    }
  },

  async handleRoute(params) {
    if (params?.name) {
      const name = params.name;
      if (savedListApi) {
        const saved = await Promise.resolve(savedListApi.refresh?.());
        if (saved?.[name]) {
          loadPayload(name, saved[name], { quiet: true });
        }
      }
    }
  },
};
