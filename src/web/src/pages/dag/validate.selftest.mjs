/**
 * Automated checks against the SHIPPED validateEditor (validate.js).
 * Run: node src/web/src/pages/dag/validate.selftest.mjs
 * Exit 0 on success; non-zero on failure.
 */
import { validateEditor } from './validate.js';

function assert(cond, msg) {
  if (!cond) {
    console.error('ASSERT_FAIL:', msg);
    process.exit(1);
  }
}

function codes(result) {
  return new Set((result.issues || []).map((i) => i.code));
}

// --- invalid: empty graph ---
{
  const r = validateEditor({ name: '', nodes: [], edges: [] });
  assert(r.ok === false, 'empty graph must fail');
  const c = codes(r);
  assert(c.has('missing_name'), 'empty graph needs missing_name');
  assert(c.has('no_collector'), 'empty graph needs no_collector');
  console.log('case empty_graph: FAIL_AS_EXPECTED', [...c].join(','));
}

// --- invalid: from_upstream without edge ---
{
  const r = validateEditor({
    name: 'bad_up',
    nodes: [
      {
        id: 'profiles',
        type: 'collector',
        component: 'youtube_profiles',
        config: { from_upstream: { auto: true } },
        ports_in: [{ name: 'records', required: false }],
        ports_out: [{ name: 'records' }],
      },
    ],
    edges: [],
  });
  assert(r.ok === false, 'from_upstream without edge must fail');
  assert(codes(r).has('upstream_no_edge'), 'expected upstream_no_edge');
  console.log('case upstream_no_edge: FAIL_AS_EXPECTED');
}

// --- invalid: dangling required port on storage ---
{
  const r = validateEditor({
    name: 'dangling',
    nodes: [
      {
        id: 'src',
        type: 'collector',
        component: 'steam',
        config: {},
        ports_in: [],
        ports_out: [{ name: 'records' }],
      },
      {
        id: 'store',
        type: 'storage',
        component: 'sqlalchemy',
        config: {},
        ports_in: [{ name: 'records', required: true }],
        ports_out: [],
      },
    ],
    edges: [],
  });
  assert(r.ok === false, 'unconnected storage required port must fail');
  assert(codes(r).has('dangling_port'), 'expected dangling_port');
  console.log('case dangling_port: FAIL_AS_EXPECTED');
}

// --- invalid: cycle ---
{
  const r = validateEditor({
    name: 'cyc',
    nodes: [
      {
        id: 'a',
        type: 'collector',
        component: 'steam',
        config: {},
        ports_in: [{ name: 'records', required: false }],
        ports_out: [{ name: 'records' }],
      },
      {
        id: 'b',
        type: 'collector',
        component: 'taptap',
        config: {},
        ports_in: [{ name: 'records', required: false }],
        ports_out: [{ name: 'records' }],
      },
    ],
    edges: [
      { from: 'a', out: 'records', to: 'b', in: 'records' },
      { from: 'b', out: 'records', to: 'a', in: 'records' },
    ],
  });
  assert(r.ok === false, 'cycle must fail');
  assert(codes(r).has('cycle'), 'expected cycle');
  console.log('case cycle: FAIL_AS_EXPECTED');
}

// --- valid minimal chain ---
{
  const r = validateEditor({
    name: 'ok_chain',
    nodes: [
      {
        id: 'src',
        type: 'collector',
        component: 'youtube_comments',
        config: {},
        ports_in: [{ name: 'records', required: false }],
        ports_out: [{ name: 'records' }],
      },
      {
        id: 'store',
        type: 'storage',
        component: 'sqlalchemy',
        config: {},
        ports_in: [{ name: 'records', required: true }],
        ports_out: [],
      },
    ],
    edges: [{ from: 'src', out: 'records', to: 'store', in: 'records' }],
  });
  assert(r.ok === true, `valid chain must pass, got ${JSON.stringify(r.issues)}`);
  console.log('case valid_chain: PASS_AS_EXPECTED');
}

const collector = (id, extra = {}) => ({
  id,
  type: 'collector',
  component: 'steam',
  config: {},
  ports_in: [{ name: 'params', required: false }, { name: 'records', required: false }],
  ports_out: [{ name: 'records' }],
  ...extra,
});

// --- invalid: edge uses a port the node does not have ---
{
  const r = validateEditor({
    name: 'bad_port',
    nodes: [
      collector('src'),
      {
        id: 'cond', type: 'logic', component: 'condition',
        config: { cases: [{ id: 'hot', conditions: [{ source: 'aggregate', field: 'count', operator: 'gt', value: 0 }] }] },
        ports_in: [{ name: 'records', required: true }],
        ports_out: [{ name: 'hot' }, { name: 'else' }],
      },
    ],
    edges: [
      { from: 'src', out: 'records', to: 'cond', in: 'records' },
      { from: 'cond', out: 'missing', to: 'src', in: 'records' },
    ],
  });
  assert(r.ok === false && codes(r).has('bad_port'), 'expected bad_port');
  console.log('case bad_port: FAIL_AS_EXPECTED');
}

// --- invalid: incomplete logic configs + bad policy ---
{
  const r = validateEditor({
    name: 'bad_logic',
    nodes: [
      collector('src', { policy: { max_retries: 99 } }),
      { id: 'cond', type: 'logic', component: 'condition', config: { cases: [] }, ports_in: [{ name: 'records', required: true }], ports_out: [{ name: 'else' }] },
      { id: 'flt', type: 'logic', component: 'filter', config: { conditions: [{ field: '', operator: 'eq', value: '' }], limit: -1 }, ports_in: [{ name: 'records', required: true }], ports_out: [{ name: 'records' }, { name: 'rejected' }] },
      { id: 'tf', type: 'logic', component: 'transform', config: { fields: [{ key: 'bad key', template: 'x' }] }, ports_in: [{ name: 'records', required: true }], ports_out: [{ name: 'records' }] },
    ],
    edges: [
      { from: 'src', out: 'records', to: 'cond', in: 'records' },
      { from: 'src', out: 'records', to: 'flt', in: 'records' },
      { from: 'src', out: 'records', to: 'tf', in: 'records' },
    ],
  });
  const c = codes(r);
  for (const code of ['logic_no_case', 'logic_condition', 'logic_limit', 'logic_transform_key', 'policy_invalid']) {
    assert(c.has(code), `expected ${code}, got ${[...c].join(',')}`);
  }
  console.log('case logic_and_policy: FAIL_AS_EXPECTED');
}

// --- warning only: error_branch without error edge; translator hook ---
{
  const r = validateEditor({
    name: 'warn',
    nodes: [collector('src', { policy: { on_error: 'error_branch' }, ports_out: [{ name: 'records' }, { name: 'error' }] })],
    edges: [],
  }, { t: (key, params) => (key === 'dag.check.error_branch_unconnected' ? `T:${params.node}` : key) });
  assert(r.ok === true, 'warnings do not block');
  assert(r.warnings.length === 1 && r.warnings[0].message === 'T:src', 'translated warning');
  console.log('case error_branch_warning: PASS_AS_EXPECTED');
}

console.log('VALIDATE_EDITOR_SELFTEST_OK');
