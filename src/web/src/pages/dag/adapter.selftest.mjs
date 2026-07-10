import { apiToEditor, editorToApi, layoutNum } from './adapter.js';

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(layoutNum('42.5', 0) === 42.5, 'string coord');
assert(layoutNum(10, 0) === 10, 'number coord');
assert(layoutNum(undefined, 7) === 7, 'fallback');

const ed = apiToEditor({
  name: 'g',
  nodes: [
    {
      id: 'a',
      type: 'collector',
      component: 'steam',
      ui: { x: '120', y: '40' },
    },
  ],
  edges: [],
  ui: { zoom: '0.9', pan: { x: '1', y: '2' } },
});
assert(ed.nodes[0].x === 120, 'apiToEditor x string');
assert(ed.nodes[0].y === 40, 'apiToEditor y string');
assert(ed.ui.zoom === 0.9, 'zoom string');
assert(ed.ui.pan.x === 1 && ed.ui.pan.y === 2, 'pan string');

const api = editorToApi({
  name: 'g',
  nodes: [{ id: 'a', type: 'collector', component: 'steam', x: 99, y: 88, ports_in: [], ports_out: [] }],
  edges: [],
  ui: { zoom: 1.1, pan: { x: 3, y: 4 } },
});
assert(api.nodes[0].ui.x === 99 && api.nodes[0].ui.y === 88, 'editorToApi ui');
assert(api.ui.zoom === 1.1, 'editorToApi zoom');

// round-trip positions
const back = apiToEditor(api);
assert(back.nodes[0].x === 99 && back.nodes[0].y === 88, 'roundtrip');

console.log('DAG_ADAPTER_SELFTEST_OK');
