/**
 * Auto layout for the DAG canvas: longest-path layering from sources, then a
 * barycenter pass to reduce edge crossings. Columns and rows are packed with
 * the real node sizes when provided. Pure function, no DOM access.
 */

/**
 * @param {{ id: string }[]} nodes
 * @param {{ from: string, to: string }[]} edges
 * @param {{
 *   gapX?: number, gapY?: number, originX?: number, originY?: number,
 *   sizes?: Record<string, { w: number, h: number }>,
 *   defaultWidth?: number, defaultHeight?: number,
 * }} [options]
 * @returns {Record<string, { x: number, y: number }>}
 */
export function autoLayout(nodes, edges, options = {}) {
  const gapX = options.gapX ?? 90;
  const gapY = options.gapY ?? 40;
  const originX = options.originX ?? 60;
  const originY = options.originY ?? 60;
  const sizeOf = (id) => {
    const size = options.sizes?.[id];
    return {
      w: Number(size?.w) > 0 ? Number(size.w) : (options.defaultWidth ?? 190),
      h: Number(size?.h) > 0 ? Number(size.h) : (options.defaultHeight ?? 110),
    };
  };

  const ids = (nodes || []).map((n) => n.id);
  const idSet = new Set(ids);
  const preds = new Map(ids.map((id) => [id, []]));
  const succs = new Map(ids.map((id) => [id, []]));
  for (const e of edges || []) {
    if (!idSet.has(e.from) || !idSet.has(e.to) || e.from === e.to) continue;
    succs.get(e.from).push(e.to);
    preds.get(e.to).push(e.from);
  }

  // Kahn order; nodes left over (cycle) are appended in original order
  const indeg = new Map(ids.map((id) => [id, preds.get(id).length]));
  const queue = ids.filter((id) => indeg.get(id) === 0);
  const order = [];
  while (queue.length) {
    const id = queue.shift();
    order.push(id);
    for (const next of succs.get(id)) {
      indeg.set(next, indeg.get(next) - 1);
      if (indeg.get(next) === 0) queue.push(next);
    }
  }
  for (const id of ids) if (!order.includes(id)) order.push(id);

  const layer = new Map();
  for (const id of order) {
    const ps = preds.get(id).filter((p) => layer.has(p));
    layer.set(id, ps.length ? Math.max(...ps.map((p) => layer.get(p))) + 1 : 0);
  }

  const layers = [];
  for (const id of order) {
    const l = layer.get(id);
    (layers[l] ||= []).push(id);
  }

  // barycenter ordering: sort each layer by average position of predecessors
  const position = new Map();
  layers.forEach((members, l) => {
    if (l > 0) {
      const score = (id) => {
        const ps = preds.get(id).filter((p) => position.has(p));
        if (!ps.length) return Number.MAX_SAFE_INTEGER;
        return ps.reduce((sum, p) => sum + position.get(p), 0) / ps.length;
      };
      members.sort((a, b) => score(a) - score(b));
    }
    members.forEach((id, idx) => position.set(id, idx));
  });

  // columns sized by their widest node; each column centered on the tallest one
  const colX = [];
  let x = originX;
  layers.forEach((members, l) => {
    colX[l] = x;
    x += Math.max(...members.map((id) => sizeOf(id).w)) + gapX;
  });
  const colHeight = layers.map((members) =>
    members.reduce((sum, id) => sum + sizeOf(id).h, 0) + gapY * (members.length - 1));
  const tallest = Math.max(0, ...colHeight);

  const result = {};
  layers.forEach((members, l) => {
    let y = originY + (tallest - colHeight[l]) / 2;
    for (const id of members) {
      result[id] = { x: colX[l], y: Math.round(y) };
      y += sizeOf(id).h + gapY;
    }
  });
  return result;
}
