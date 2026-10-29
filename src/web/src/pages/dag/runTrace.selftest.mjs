/**
 * Run trace folding must match src/core/dag_trace.py (live WS events and the
 * /dag-trace snapshot produce the same node states).
 * Run: node src/web/src/pages/dag/runTrace.selftest.mjs
 */
if (typeof globalThis.localStorage === 'undefined') {
  const store = new Map();
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => store.set(k, String(v)),
    removeItem: (k) => store.delete(k),
  };
}

const { applyNodeEvent, describeRunState, foldNodeEvents, formatElapsed, summarizeRun } = await import('./runTrace.js');

function assert(cond, msg) {
  if (!cond) {
    console.error('ASSERT_FAIL:', msg);
    process.exit(1);
  }
}

const events = [
  { payload: { node_id: 'c1', status: 'started', node: { node_id: 'c1', status: 'running', attempts: 1 } } },
  { payload: { node_id: 'c1', status: 'progress', checkpoint_cursor: { i: 1 } } },
  { payload: { node_id: 'c1', status: 'retrying', node: { node_id: 'c1', status: 'retrying', attempts: 1, max_attempts: 3 } } },
  { payload: { node_id: 'c1', status: 'succeeded', node: { node_id: 'c1', status: 'succeeded', attempts: 2, max_attempts: 3, elapsed_ms: 1500, node_type: 'collector', output_counts: { records: 12 } } } },
  { payload: { node_id: 'p1', status: 'skipped' } },
  { payload: { progress: 0.5 } },
];
const states = foldNodeEvents(events);
assert(states.c1.status === 'succeeded' && states.c1.attempts === 2, 'snapshot wins, progress ignored');
assert(states.p1.status === 'skipped', 'status-only fallback');
assert(Object.keys(states).length === 2, 'events without node_id ignored');

applyNodeEvent(states, { payload: { node_id: 'p1', status: 'failed', error: 'boom' } });
assert(states.p1.status === 'failed' && states.p1.error === 'boom', 'incremental apply');

const summary = summarizeRun(states);
assert(summary.total === 2 && summary.succeeded === 1 && summary.failed === 1, 'summary counts');

assert(formatElapsed(850) === '850ms', 'ms format');
assert(formatElapsed(1500) === '1.5s', 'seconds format');
assert(formatElapsed(125000) === '2m 05s', 'minutes format');

const desc = describeRunState(states.c1);
assert(desc.status === 'succeeded' && desc.icon === '✓', 'describe status/icon');
assert(desc.text.includes('12') && desc.text.includes('1.5s'), `describe text has counts + elapsed: ${desc.text}`);
const branch = describeRunState({ status: 'succeeded', node_type: 'logic', branch: 'hot', output_counts: { hot: 3, else: 0 } });
assert(branch.text.includes('hot'), 'condition branch shown');

console.log('DAG_RUN_TRACE_SELFTEST_OK');
