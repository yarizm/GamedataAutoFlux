/**
 * Pure editor modules: adapter (ports / policy), auto layout, undo history.
 * Run: node src/web/src/pages/dag/editor.selftest.mjs
 */
import {
  apiToEditor,
  editorToApi,
  logicPorts,
  portsCompatible,
  syncNodePorts,
} from './adapter.js';
import { autoLayout } from './layout.js';
import { createHistory } from './history.js';

function assert(cond, msg) {
  if (!cond) {
    console.error('ASSERT_FAIL:', msg);
    process.exit(1);
  }
}

// --- logic ports follow config (mirror of dag_logic.logic_output_port_names) ---
{
  const cond = logicPorts('condition', { cases: [{ id: 'hot' }, { id: 'cold' }, { id: 'hot' }] });
  assert(JSON.stringify(cond.ports_out.map((p) => p.name)) === '["hot","cold","else"]', 'condition ports');
  assert(logicPorts('filter', {}).ports_out.map((p) => p.name).join() === 'records,rejected', 'filter ports');
  assert(logicPorts('transform', {}).ports_out.map((p) => p.name).join() === 'records', 'transform ports');
}

// --- error_branch policy adds / removes the error port; collectors get params ---
{
  const node = syncNodePorts({
    id: 'c', type: 'collector', component: 'steam', config: {},
    policy: { on_error: 'error_branch' },
    ports_in: [{ name: 'records', required: false }],
    ports_out: [{ name: 'records' }],
  });
  assert(node.ports_out.map((p) => p.name).join() === 'records,error', 'error port added');
  assert(node.ports_in[0].name === 'params', 'collector params port prepended');
  node.policy = {};
  syncNodePorts(node);
  assert(node.ports_out.map((p) => p.name).join() === 'records', 'error port removed');
}

// --- params ports only connect to params ---
{
  assert(portsCompatible({ name: 'params', type_hint: 'params' }, { name: 'params', type_hint: 'params' }), 'params→params');
  assert(!portsCompatible({ name: 'records' }, { name: 'params', type_hint: 'params' }), 'records→params rejected');
  assert(!portsCompatible({ name: 'params' }, { name: 'records' }), 'params→records rejected');
  assert(portsCompatible({ name: 'case_1' }, { name: 'records' }), 'branch→records');
}

// --- policy round-trip drops defaults, keeps meaningful values ---
{
  const api = editorToApi({
    name: 'g',
    nodes: [{
      id: 'c', type: 'collector', component: 'steam', x: 1, y: 2,
      policy: { max_retries: 2, retry_interval_seconds: 1.5, timeout_seconds: '', on_error: 'fail' },
      ports_in: [], ports_out: [{ name: 'records' }],
    }],
    edges: [{ from: 'c', out: 'records', to: 'x', in: 'records' }],
  });
  assert(JSON.stringify(api.nodes[0].policy) === '{"max_retries":2,"retry_interval_seconds":1.5}', `policy cleaned: ${JSON.stringify(api.nodes[0].policy)}`);
  const back = apiToEditor({ ...api, nodes: [{ ...api.nodes[0], policy: { on_error: 'error_branch' } }] });
  assert(back.nodes[0].ports_out.some((p) => p.name === 'error'), 'apiToEditor syncs error port');
}

// --- auto layout: layers by longest path, no overlaps ---
{
  const nodes = ['input', 'a', 'b', 'merge', 'store'].map((id) => ({ id }));
  const edges = [
    { from: 'input', to: 'a' },
    { from: 'input', to: 'b' },
    { from: 'a', to: 'merge' },
    { from: 'b', to: 'merge' },
    { from: 'merge', to: 'store' },
    { from: 'a', to: 'store' },
  ];
  const pos = autoLayout(nodes, edges, { gapX: 100, gapY: 50, originX: 0, originY: 0, defaultWidth: 100, defaultHeight: 50 });
  assert(pos.input.x === 0 && pos.a.x === 200 && pos.b.x === 200, 'layer 0/1');
  assert(pos.merge.x === 400 && pos.store.x === 600, 'longest path layering');
  const keys = new Set(Object.values(pos).map((p) => `${p.x},${p.y}`));
  assert(keys.size === nodes.length, 'no overlapping positions');
  // real node sizes: a tall node pushes its column neighbour down instead of overlapping
  const sized = autoLayout(
    [{ id: 'p' }, { id: 'q' }],
    [],
    { gapY: 20, originY: 0, sizes: { p: { w: 180, h: 300 }, q: { w: 180, h: 80 } } },
  );
  assert(sized.q.y >= sized.p.y + 300 + 20, `sized rows do not overlap: ${JSON.stringify(sized)}`);
}

// --- history: commit / undo / redo / no-op commits ---
{
  const h = createHistory();
  const ed = { nodes: [{ id: 'a' }], edges: [] };
  h.reset(ed);
  assert(!h.canUndo(), 'fresh history cannot undo');
  assert(h.commit(ed) === false, 'unchanged commit is a no-op');
  ed.nodes.push({ id: 'b' });
  assert(h.commit(ed) === true, 'changed commit recorded');
  const undone = h.undo();
  assert(undone.nodes.length === 1, 'undo restores previous');
  assert(h.canRedo(), 'redo available');
  const redone = h.redo();
  assert(redone.nodes.length === 2, 'redo reapplies');
  h.undo();
  ed.nodes = [{ id: 'z' }];
  h.commit(ed);
  assert(!h.canRedo(), 'new commit clears redo stack');
}

console.log('DAG_EDITOR_SELFTEST_OK');
