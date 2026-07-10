import 'drawflow/dist/drawflow.min.css';
import './style.css';

import { api, toast, escapeHtml } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { invalidatePipelineCache } from '../../core/pipelines.js';
import { apiToEditor, editorToApi, defaultPortsForType } from './adapter.js';
import { validateEditor } from './validate.js';
import { createCanvas } from './canvas.js';
import { mountPalette } from './palette.js';
import { mountSavedList } from './saved-list.js';
import { mountInspector } from './inspector.js';
import { loadCollectorMetaMap } from './schemas.js';

/** @type {{ name: string, nodes: object[], edges: object[], ui: object }} */
let editorState = {
  name: '',
  nodes: [],
  edges: [],
  ui: { zoom: 1, pan: { x: 0, y: 0 } },
};

let activeName = '';
let nodeCounter = {};
let canvasApi = null;
let paletteApi = null;
let savedListApi = null;
let inspectorApi = null;
let selection = { kind: null };
let themeListener = null;
let languageListener = null;

const LAST_DAG_KEY = 'gamedata-autoflux.dag.active';

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
    const edge = editorState.edges.find(
      (e) => e.from === selection.edge.from
        && e.to === selection.edge.to
        && (e.out || 'records') === (selection.edge.out || 'records')
        && (e.in || 'records') === (selection.edge.in || 'records'),
    ) || selection.edge;
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
  if (result.ok) {
    bar.className = 'text-xs mb-2 px-1 is-ok';
    bar.textContent = t('dag.validateOk');
    return;
  }
  bar.className = 'text-xs mb-2 px-1 has-errors';
  bar.innerHTML = result.issues
    .map(
      (iss, i) =>
        `<span class="dag-issue" data-issue-idx="${i}" data-node-id="${escapeHtml(iss.nodeId || '')}">${escapeHtml(iss.message)}</span>`,
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
  const { quiet = false } = options;
  editorState = apiToEditor(payload);
  editorState.name = name || editorState.name;
  activeName = name || editorState.name;
  rememberActiveDag(activeName);
  nodeCounter = {};
  for (const n of editorState.nodes) {
    nodeCounter[n.type] = (nodeCounter[n.type] || 0) + 1;
  }
  setNameInput(editorState.name);
  selection = { kind: null };
  canvasApi?.loadEditor(editorState);
  refreshInspector();
  showValidateBar(null);
  savedListApi?.refresh();
  if (!quiet) {
    toast(t('dag.loadedToast', { name: editorState.name }), 'success');
  }
}

function clearGraph() {
  editorState = {
    name: '',
    nodes: [],
    edges: [],
    ui: { zoom: 1, pan: { x: 0, y: 0 } },
  };
  activeName = '';
  rememberActiveDag('');
  nodeCounter = {};
  setNameInput('');
  selection = { kind: null };
  canvasApi?.loadEditor(editorState);
  refreshInspector();
  showValidateBar(null);
  savedListApi?.refresh();
}

function addNode(type, component) {
  nodeCounter[type] = (nodeCounter[type] || 0) + 1;
  const id = `${type}_${nodeCounter[type]}`;
  const ports = defaultPortsForType(type);
  const node = {
    id,
    type,
    component,
    config: {},
    ports_in: ports.ports_in.map((p) => ({ ...p })),
    ports_out: ports.ports_out.map((p) => ({ ...p })),
    x: 60 + (editorState.nodes.length % 4) * 200,
    y: 40 + Math.floor(editorState.nodes.length / 4) * 120,
    label: '',
  };
  editorState.nodes.push(node);
  canvasApi?.addNodeToCanvas(node);
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

function runValidate() {
  syncFromCanvas();
  editorState.name = readNameInput();
  const result = validateEditor(editorState);
  showValidateBar(result);
  if (result.ok) {
    toast(t('dag.validateOk'), 'success');
  } else {
    toast(t('dag.validateFail', { count: result.issues.length }), 'error');
  }
  return result;
}

async function saveGraph() {
  syncFromCanvas();
  editorState.name = readNameInput();
  const result = validateEditor(editorState);
  showValidateBar(result);
  if (!result.ok) {
    toast(t('dag.saveBlocked', { count: result.issues.length }), 'error');
    return;
  }

  const name = editorState.name;
  const saved = savedListApi?.getSaved?.() || {};
  if (saved[name] && name !== activeName) {
    if (!window.confirm(t('dag.confirmOverwrite', { name }))) {
      return;
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
    editorState = apiToEditor(savedPayload);
    editorState.name = name;
    canvasApi?.loadEditor(editorState);
    toast(t('dag.savedToast', { name }), 'success');
    await savedListApi?.refresh();
  } catch (e) {
    toast(t('dag.saveFail', { error: e.message || e }), 'error');
  }
}

function onUpdateNode(id, patch) {
  const node = editorState.nodes.find((n) => n.id === id);
  if (!node) return;
  if (patch.label !== undefined) node.label = patch.label;
  if (patch.config !== undefined) node.config = patch.config;
  canvasApi?.rebuildNodeHtml(id);
  if (selection.kind === 'node' && selection.id === id) {
    refreshInspector();
  }
}

function onUpdateEdge(edgeRef, patch) {
  const edge = editorState.edges.find(
    (e) => e.from === edgeRef.from
      && e.to === edgeRef.to
      && (e.out || 'records') === (edgeRef.out || 'records')
      && (e.in || 'records') === (edgeRef.in || 'records'),
  );
  if (!edge) return;
  if (patch.condition !== undefined) {
    edge.condition = patch.condition || null;
  }
  selection = { kind: 'edge', edge: { ...edge } };
  canvasApi?.applyConditionStyles();
  refreshInspector();
}

function bindToolbar(container) {
  container.querySelector('#btn-dag-save')?.addEventListener('click', () => {
    saveGraph();
  });
  container.querySelector('#btn-dag-validate')?.addEventListener('click', () => {
    runValidate();
  });
  container.querySelector('#btn-dag-fit')?.addEventListener('click', () => {
    canvasApi?.fitView();
  });
  container.querySelector('#btn-dag-clear')?.addEventListener('click', () => {
    if (editorState.nodes.length && !window.confirm(t('dag.confirmClear'))) return;
    clearGraph();
  });
  container.querySelector('#btn-dag-refresh-list')?.addEventListener('click', () => {
    savedListApi?.refresh();
  });
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
  init(container) {
    this.container = container;

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
            }
          }
        },
      });
    }

    inspectorApi = mountInspector(container.querySelector('#dag-inspector'), {
      onUpdateNode,
      onUpdateEdge,
      getEditor,
    });

    paletteApi = mountPalette(container.querySelector('#dag-palette'), {
      onAdd: (type, component) => addNode(type, component),
    });

    savedListApi = mountSavedList(container.querySelector('#dag-saved-list'), {
      getActiveName: () => activeName,
      onLoad: (name, payload) => loadPayload(name, payload),
      onDelete: (name) => deleteSaved(name),
    });

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
      };
      window.addEventListener('languagechange', languageListener);
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
  },

  async refresh() {
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
  },
};
