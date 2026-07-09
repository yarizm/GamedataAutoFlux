import { api, toast, escapeHtml } from '../../core/api.js';

// DAG 编辑器状态
const state = {
  nodes: [],      // [{id, type, component, x, y, ports_in:[], ports_out:[]}]
  edges: [],      // [{from, out, to, in, condition}]
  nextId: 1,
  dragging: null,
  connecting: null,  // {fromNode, fromPort} 开始连线
  nodeCounter: {},   // {type: count}
};

const TYPE_COLORS = {
  collector: 'emerald',
  processor: 'sky',
  storage: 'amber',
  composite: 'violet',
};

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this.refresh();
    return this;
  },

  destroy() {},

  async refresh() { await this._loadPalette(); },

  async _loadPalette() {
    try {
      const data = await api('/components/metadata');
      const palette = this.container.querySelector('#dag-palette');
      if (!palette) return;
      const components = data.components || {};
      const items = [];
      for (const [type, names] of Object.entries(components)) {
        for (const name of names) {
          items.push({ type, name });
        }
      }
      if (!items.length) {
        palette.innerHTML = '<p class="text-zinc-600 text-xs">无可用组件</p>';
        return;
      }
      palette.innerHTML = items.map((it) => {
        const color = TYPE_COLORS[it.type] || 'zinc';
        return `<button class="dag-palette-item w-full text-left px-3 py-2 rounded-lg bg-zinc-800/50 hover:bg-${color}-500/10 border border-white/5 hover:border-${color}-500/30 transition-all text-xs" data-add-type="${it.type}" data-add-name="${escapeHtml(it.name)}">
          <span class="font-mono text-${color}-400">[${it.type}]</span>
          <span class="text-zinc-200 ml-1">${escapeHtml(it.name)}</span>
        </button>`;
      }).join('');
      palette.querySelectorAll('[data-add-type]').forEach((btn) => {
        btn.addEventListener('click', () => {
          dagAddNode(btn.dataset.addType, btn.dataset.addName);
        });
      });
    } catch (e) {
      toast('加载节点库失败', 'error');
    }
  },
};

// 暴露给 window 供 onclick 调用
window.dagAddNode = function (type, component) {
  state.nodeCounter[type] = (state.nodeCounter[type] || 0) + 1;
  const id = `${type}_${state.nodeCounter[type]}`;
  const hasRecordsOut = type !== 'storage';
  const hasRecordsIn = type !== 'collector';
  state.nodes.push({
    id, type, component,
    x: 60 + (state.nodes.length % 4) * 200,
    y: 40 + Math.floor(state.nodes.length / 4) * 120,
    ports_in: hasRecordsIn ? [{ name: 'records' }] : [],
    ports_out: hasRecordsOut ? [{ name: 'records' }] : [],
  });
  dagRender();
};

window.dagSaveGraph = async function () {
  const nameEl = document.getElementById('dag-name-input');
  const name = (nameEl?.value || '').trim();
  if (!name) { toast('请输入 DAG 名称', 'error'); return; }
  if (!state.nodes.length) { toast('请添加至少一个节点', 'error'); return; }
  const payload = {
    name,
    nodes: state.nodes.map((n) => ({
      id: n.id, type: n.type, component: n.component,
      config: {},
      ports_in: n.ports_in.map((p) => ({ name: p.name })),
      ports_out: n.ports_out.map((p) => ({ name: p.name })),
      is_param_port: [],
    })),
    edges: state.edges.map((e) => ({
      from: e.from, out: e.out, to: e.to, in: e.in, condition: e.condition || null,
    })),
  };
  try {
    await api('/dags', { method: 'POST', body: JSON.stringify(payload) });
    toast(`DAG 已保存: ${name}`, 'success');
  } catch (e) {
    toast(`保存失败: ${e.message || e}`, 'error');
  }
};

window.dagDeleteNode = function (id) {
  state.nodes = state.nodes.filter((n) => n.id !== id);
  state.edges = state.edges.filter((e) => e.from !== id && e.to !== id);
  dagRender();
};

window.dagClearGraph = function () {
  state.nodes = []; state.edges = []; state.nodeCounter = {};
  dagRender();
};

function dagRender() {
  const canvas = document.getElementById('dag-canvas');
  const svg = document.getElementById('dag-edges-svg');
  if (!canvas || !svg) return;
  canvas.innerHTML = state.nodes.map((n) => {
    const color = TYPE_COLORS[n.type] || 'zinc';
    const inPorts = n.ports_in.map((p) =>
      `<div class="dag-port dag-port-in" data-node="${n.id}" data-port="${p.name}" data-dir="in"></div><span class="text-[10px] text-zinc-500 ml-1">${p.name}</span>`
    ).join('');
    const outPorts = n.ports_out.map((p) =>
      `<span class="text-[10px] text-zinc-500 mr-1">${p.name}</span><div class="dag-port dag-port-out" data-node="${n.id}" data-port="${p.name}" data-dir="out"></div>`
    ).join('');
    return `<div class="dag-node absolute" style="left:${n.x}px;top:${n.y}px;" data-node-id="${n.id}">
      <div class="dag-node-card rounded-xl bg-zinc-800 border border-${color}-500/30 shadow-lg w-44 cursor-move" data-drag="${n.id}">
        <div class="px-3 py-2 border-b border-white/5 flex items-center justify-between">
          <span class="font-mono text-[10px] text-${color}-400">[${n.type}]</span>
          <button class="text-zinc-600 hover:text-rose-400 text-xs" data-del="${n.id}">×</button>
        </div>
        <div class="px-3 py-2">
          <div class="text-zinc-100 text-xs font-bold mb-2 truncate">${escapeHtml(n.component)}</div>
          <div class="text-[10px] text-zinc-600 mb-1">${n.id}</div>
          <div class="flex flex-col gap-1 mb-1">${inPorts ? `<div class="flex items-center">${inPorts}</div>` : ''}</div>
          <div class="flex flex-col items-end gap-1">${outPorts ? `<div class="flex items-center">${outPorts}</div>` : ''}</div>
        </div>
      </div>
    </div>`;
  }).join('');

  // 连线
  svg.innerHTML = state.edges.map((e) => {
    const from = portCenter(e.from, e.out, 'out');
    const to = portCenter(e.to, e.in, 'in');
    if (!from || !to) return '';
    const mid = (from.x + to.x) / 2;
    return `<path d="M${from.x},${from.y} C${mid},${from.y} ${mid},${to.y} ${to.x},${to.y}" stroke="rgba(56,189,248,0.6)" stroke-width="2" fill="none" />`;
  }).join('');

  // 绑定拖拽
  canvas.querySelectorAll('[data-drag]').forEach((el) => {
    el.addEventListener('mousedown', (ev) => startDrag(ev, el.dataset.drag));
  });
  canvas.querySelectorAll('[data-del]').forEach((el) => {
    el.addEventListener('click', (ev) => { ev.stopPropagation(); window.dagDeleteNode(el.dataset.del); });
  });
  canvas.querySelectorAll('.dag-port-out').forEach((el) => {
    el.addEventListener('click', (ev) => { ev.stopPropagation(); startConnect(el.dataset.node, el.dataset.port); });
  });
  canvas.querySelectorAll('.dag-port-in').forEach((el) => {
    el.addEventListener('click', (ev) => { ev.stopPropagation(); finishConnect(el.dataset.node, el.dataset.port); });
  });
}

function portCenter(nodeId, port, dir) {
  const canvas = document.getElementById('dag-canvas');
  const wrap = document.getElementById('dag-canvas-wrap');
  if (!canvas || !wrap) return null;
  const sel = `.dag-port-${dir}[data-node="${nodeId}"][data-port="${port}"]`;
  const el = canvas.querySelector(sel);
  if (!el) return null;
  const cr = el.getBoundingClientRect();
  const wr = wrap.getBoundingClientRect();
  return { x: cr.left - wr.left + cr.width / 2, y: cr.top - wr.top + cr.height / 2 };
}

function startDrag(ev, nodeId) {
  const node = state.nodes.find((n) => n.id === nodeId);
  if (!node) return;
  const canvas = document.getElementById('dag-canvas');
  const wrap = document.getElementById('dag-canvas-wrap');
  const wr = wrap.getBoundingClientRect();
  state.dragging = {
    nodeId,
    offsetX: ev.clientX - wr.left - node.x,
    offsetY: ev.clientY - wr.top - node.y,
  };
  ev.preventDefault();
}

document.addEventListener('mousemove', (ev) => {
  if (!state.dragging) return;
  const wrap = document.getElementById('dag-canvas-wrap');
  if (!wrap) return;
  const wr = wrap.getBoundingClientRect();
  const node = state.nodes.find((n) => n.id === state.dragging.nodeId);
  if (!node) return;
  node.x = ev.clientX - wr.left - state.dragging.offsetX;
  node.y = ev.clientY - wr.top - state.dragging.offsetY;
  dagRender();
});

document.addEventListener('mouseup', () => { state.dragging = null; });

function startConnect(nodeId, port) {
  state.connecting = { fromNode: nodeId, fromPort: port };
  toast('已选择起点，点击目标节点的输入端口连线', 'info');
}

function finishConnect(nodeId, port) {
  if (!state.connecting) return;
  const { fromNode, fromPort } = state.connecting;
  if (fromNode === nodeId) { state.connecting = null; return; }
  const exists = state.edges.some((e) => e.from === fromNode && e.out === fromPort && e.to === nodeId && e.in === port);
  if (!exists) {
    state.edges.push({ from: fromNode, out: fromPort, to: nodeId, in: port, condition: null });
  }
  state.connecting = null;
  dagRender();
}
