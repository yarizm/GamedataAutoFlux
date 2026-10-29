/**
 * Client-side DAG editor validation (aligned with engine intent; the server's
 * POST /api/dags/validate stays authoritative).
 *
 * Pure module (no DOM / i18n imports) so node selftests can load it. Pass
 * `{ t }` to localize messages; keys are `dag.check.<code>`.
 */

const FALLBACK_MESSAGES = {
  missing_name: '请填写 DAG 名称',
  no_collector: '至少需要一个 collector 节点',
  dup_id: '存在重复的节点 id',
  self_loop: '边不能自环: {from}',
  bad_edge: '边引用了不存在的节点: {from} → {to}',
  bad_port: '边使用了节点没有的端口: {from}.{out} → {to}.{in}',
  upstream_no_edge: '节点 {node} 配置了 from_upstream 但没有 records 入边',
  dangling_port: '节点 {node} 的必需输入端口 {port} 未连接',
  cycle: '图中存在环，DAG 不允许循环依赖',
  logic_no_case: '条件节点 {node} 至少需要一个分支',
  logic_case_id: '条件节点 {node} 的分支 id「{id}」无效或重复',
  logic_case_empty: '条件节点 {node} 的分支「{id}」没有条件',
  logic_condition: '节点 {node} 的第 {index} 个条件不完整（字段 / 运算符 / 值）',
  logic_transform_empty: '转换节点 {node} 至少需要一个输出字段或删除字段',
  logic_transform_key: '转换节点 {node} 的字段名「{key}」无效或重复',
  logic_limit: '过滤节点 {node} 的数量上限必须是 ≥0 的整数',
  policy_invalid: '节点 {node} 的异常处理配置无效：{detail}',
  error_branch_unconnected: '节点 {node} 选择了「走异常分支」，但 error 端口没有连线',
};

const UNARY_OPERATORS = new Set(['empty', 'not_empty']);
const OPERATORS = new Set([
  'eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'contains', 'not_contains',
  'starts_with', 'ends_with', 'in', 'not_in', 'empty', 'not_empty',
]);
const AGGREGATE_FIELDS = new Set(['count', 'total', 'failed', 'success_rate']);
const RESERVED_CASE_IDS = new Set(['else', 'error', 'records', 'rejected']);
const CASE_ID_RE = /^[A-Za-z0-9_]{1,40}$/;
const FIELD_KEY_RE = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;
const ON_ERROR_MODES = new Set(['fail', 'stop', 'continue', 'error_branch']);

function interpolate(text, params) {
  return String(text).replace(/\{(\w+)\}/g, (_, key) => (params?.[key] ?? `{${key}}`));
}

function conditionIncomplete(cond, { batch }) {
  if (!cond || typeof cond !== 'object') return true;
  const source = cond.source || 'record';
  if (source === 'aggregate') {
    if (!batch || !AGGREGATE_FIELDS.has(cond.field)) return true;
  } else if (typeof cond.field !== 'string' || !cond.field.trim()) {
    return true;
  }
  if (!OPERATORS.has(cond.operator)) return true;
  if (!UNARY_OPERATORS.has(cond.operator)) {
    if (cond.value === undefined || cond.value === null || cond.value === '') return true;
  }
  return false;
}

/** Mirror of src/core/dag_policy.validate_node_policy (subset used by the form). */
export function policyProblems(policy) {
  if (!policy || typeof policy !== 'object') return [];
  const problems = [];
  if (policy.max_retries !== undefined && policy.max_retries !== '') {
    const n = Number(policy.max_retries);
    if (!Number.isInteger(n) || n < 0 || n > 10) problems.push('max_retries 0-10');
  }
  if (policy.retry_interval_seconds !== undefined && policy.retry_interval_seconds !== '') {
    const n = Number(policy.retry_interval_seconds);
    if (!Number.isFinite(n) || n < 0 || n > 600) problems.push('retry_interval 0-600s');
  }
  if (policy.timeout_seconds !== undefined && policy.timeout_seconds !== '' && policy.timeout_seconds !== null) {
    const n = Number(policy.timeout_seconds);
    if (!Number.isFinite(n) || n <= 0 || n > 86400) problems.push('timeout 0-86400s');
  }
  if (policy.on_error !== undefined && !ON_ERROR_MODES.has(policy.on_error)) problems.push('on_error');
  return problems;
}

function logicIssues(node, push) {
  const config = node.config || {};
  if (node.component === 'condition') {
    const cases = Array.isArray(config.cases) ? config.cases : [];
    if (!cases.length) {
      push('logic_no_case', { node: node.id }, node.id);
      return;
    }
    const seen = new Set();
    cases.forEach((c) => {
      const id = c?.id;
      if (typeof id !== 'string' || !CASE_ID_RE.test(id) || RESERVED_CASE_IDS.has(id) || seen.has(id)) {
        push('logic_case_id', { node: node.id, id: id ?? '' }, node.id);
      }
      seen.add(id);
      const conditions = Array.isArray(c?.conditions) ? c.conditions : [];
      if (!conditions.length) push('logic_case_empty', { node: node.id, id: id ?? '' }, node.id);
      conditions.forEach((cond, idx) => {
        if (conditionIncomplete(cond, { batch: true })) {
          push('logic_condition', { node: node.id, index: idx + 1 }, node.id);
        }
      });
    });
  } else if (node.component === 'filter') {
    (Array.isArray(config.conditions) ? config.conditions : []).forEach((cond, idx) => {
      if (conditionIncomplete(cond, { batch: false })) {
        push('logic_condition', { node: node.id, index: idx + 1 }, node.id);
      }
    });
    const limit = config.limit ?? 0;
    if (!Number.isInteger(Number(limit)) || Number(limit) < 0) push('logic_limit', { node: node.id }, node.id);
  } else if (node.component === 'transform') {
    const fields = Array.isArray(config.fields) ? config.fields : [];
    const drops = Array.isArray(config.drop_fields) ? config.drop_fields : [];
    if (!fields.length && !drops.length) push('logic_transform_empty', { node: node.id }, node.id);
    const seen = new Set();
    fields.forEach((f) => {
      const key = f?.key;
      if (typeof key !== 'string' || !FIELD_KEY_RE.test(key) || seen.has(key)) {
        push('logic_transform_key', { node: node.id, key: key ?? '' }, node.id);
      }
      seen.add(key);
    });
  }
}

/**
 * @param {{ name?: string, nodes?: object[], edges?: object[] }} editor
 * @param {{ t?: (key: string, params?: object) => string }} [options]
 * @returns {{ ok: boolean, issues: Array<{ code: string, message: string, nodeId?: string, edgeKey?: string }>, warnings: Array<{ code: string, message: string, nodeId?: string }> }}
 */
export function validateEditor(editor, options = {}) {
  const translate = (code, params) => {
    const key = `dag.check.${code}`;
    const localized = options.t ? options.t(key, params) : key;
    return localized === key ? interpolate(FALLBACK_MESSAGES[code] || code, params) : localized;
  };
  const issues = [];
  const warnings = [];
  const push = (code, params = {}, nodeId, extra = {}) => {
    issues.push({ code, message: translate(code, params), ...(nodeId ? { nodeId } : {}), ...extra });
  };
  const warn = (code, params = {}, nodeId) => {
    warnings.push({ code, message: translate(code, params), ...(nodeId ? { nodeId } : {}) });
  };

  const nodes = editor?.nodes || [];
  const edges = editor?.edges || [];
  const name = (editor?.name || '').trim();
  if (!name) {
    push('missing_name');
  }
  if (!nodes.some((n) => n.type === 'collector')) {
    push('no_collector');
  }
  const ids = new Set(nodes.map((n) => n.id));
  if (ids.size !== nodes.length) {
    push('dup_id');
  }
  const byId = new Map(nodes.map((n) => [n.id, n]));
  for (const e of edges) {
    const edgeRef = { edgeKey: `${e.from}->${e.to}` };
    if (e.from === e.to) {
      push('self_loop', { from: e.from }, undefined, edgeRef);
    }
    if (!ids.has(e.from) || !ids.has(e.to)) {
      push('bad_edge', { from: e.from, to: e.to }, undefined, edgeRef);
      continue;
    }
    const out = e.out || 'records';
    const inp = e.in || 'records';
    const fromNode = byId.get(e.from);
    const toNode = byId.get(e.to);
    const hasOut = (fromNode.ports_out || []).some((p) => p.name === out);
    const hasIn = (toNode.ports_in || []).some((p) => p.name === inp);
    if (!hasOut || !hasIn) {
      push('bad_port', { from: e.from, out, to: e.to, in: inp }, hasOut ? e.to : e.from, edgeRef);
    }
  }
  for (const n of nodes) {
    if (n.type === 'collector' && n.config?.from_upstream) {
      const hasIn = edges.some((e) => e.to === n.id && (e.in || 'records') === 'records');
      if (!hasIn) {
        push('upstream_no_edge', { node: n.id }, n.id);
      }
    }
    for (const p of n.ports_in || []) {
      if (p.required === false) continue;
      if (n.type === 'collector') continue;
      const connected = edges.some((e) => e.to === n.id && (e.in || 'records') === p.name);
      if (!connected) {
        push('dangling_port', { node: n.id, port: p.name }, n.id);
      }
    }
    if (n.type === 'logic') logicIssues(n, push);
    const problems = policyProblems(n.policy);
    if (problems.length) push('policy_invalid', { node: n.id, detail: problems.join(', ') }, n.id);
    if (n.policy?.on_error === 'error_branch' && !edges.some((e) => e.from === n.id && e.out === 'error')) {
      warn('error_branch_unconnected', { node: n.id }, n.id);
    }
  }
  // Simple cycle detection (Kahn)
  const indeg = Object.fromEntries(nodes.map((n) => [n.id, 0]));
  const adj = Object.fromEntries(nodes.map((n) => [n.id, []]));
  for (const e of edges) {
    if (!ids.has(e.from) || !ids.has(e.to)) continue;
    adj[e.from].push(e.to);
    indeg[e.to] = (indeg[e.to] || 0) + 1;
  }
  const q = Object.keys(indeg).filter((id) => indeg[id] === 0);
  let seen = 0;
  while (q.length) {
    const id = q.shift();
    seen += 1;
    for (const nxt of adj[id] || []) {
      indeg[nxt] -= 1;
      if (indeg[nxt] === 0) q.push(nxt);
    }
  }
  if (nodes.length && seen !== nodes.length) {
    push('cycle');
  }
  return { ok: issues.length === 0, issues, warnings };
}
