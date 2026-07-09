/**
 * Client-side DAG editor validation (aligned with engine intent).
 * @param {{ name?: string, nodes?: object[], edges?: object[] }} editor
 * @returns {{ ok: boolean, issues: Array<{ code: string, message: string, nodeId?: string, edgeKey?: string }> }}
 */
export function validateEditor(editor) {
  const issues = [];
  const nodes = editor?.nodes || [];
  const edges = editor?.edges || [];
  const name = (editor?.name || '').trim();
  if (!name) {
    issues.push({ code: 'missing_name', message: '请填写 DAG 名称' });
  }
  if (!nodes.some((n) => n.type === 'collector')) {
    issues.push({ code: 'no_collector', message: '至少需要一个 collector 节点' });
  }
  const ids = new Set(nodes.map((n) => n.id));
  if (ids.size !== nodes.length) {
    issues.push({ code: 'dup_id', message: '存在重复的节点 id' });
  }
  for (const e of edges) {
    if (e.from === e.to) {
      issues.push({
        code: 'self_loop',
        message: `边不能自环: ${e.from}`,
        edgeKey: `${e.from}->${e.to}`,
      });
    }
    if (!ids.has(e.from) || !ids.has(e.to)) {
      issues.push({
        code: 'bad_edge',
        message: `边引用了不存在的节点: ${e.from} → ${e.to}`,
        edgeKey: `${e.from}->${e.to}`,
      });
    }
  }
  for (const n of nodes) {
    if (n.type === 'collector' && n.config?.from_upstream) {
      const hasIn = edges.some((e) => e.to === n.id && (e.in || 'records') === 'records');
      if (!hasIn) {
        issues.push({
          code: 'upstream_no_edge',
          message: `节点 ${n.id} 配置了 from_upstream 但没有 records 入边`,
          nodeId: n.id,
        });
      }
    }
    for (const p of n.ports_in || []) {
      if (p.required === false) continue;
      if (n.type === 'collector') continue;
      const connected = edges.some((e) => e.to === n.id && (e.in || 'records') === p.name);
      if (!connected) {
        issues.push({
          code: 'dangling_port',
          message: `节点 ${n.id} 的必需输入端口 ${p.name} 未连接`,
          nodeId: n.id,
        });
      }
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
    issues.push({ code: 'cycle', message: '图中存在环，DAG 不允许循环依赖' });
  }
  return { ok: issues.length === 0, issues };
}
