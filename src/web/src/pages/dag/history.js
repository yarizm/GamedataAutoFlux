/**
 * Undo/redo for the DAG editor: snapshot-based (graphs are small), so every
 * mutation path only needs to call commit() after it finished.
 */

/** @param {object} editor */
export function snapshotOf(editor) {
  return JSON.stringify({
    nodes: (editor?.nodes || []).map((n) => ({ ...n })),
    edges: (editor?.edges || []).map((e) => ({ ...e })),
  });
}

/**
 * @param {number} [limit]
 */
export function createHistory(limit = 60) {
  let past = [];
  let future = [];
  let present = null;

  return {
    /** Start fresh from a loaded graph (no undo across loads). */
    reset(editor) {
      past = [];
      future = [];
      present = snapshotOf(editor);
    },
    /** Record the state after a mutation; no-op when nothing changed. */
    commit(editor) {
      const next = snapshotOf(editor);
      if (present === null) {
        present = next;
        return false;
      }
      if (next === present) return false;
      past.push(present);
      if (past.length > limit) past.shift();
      present = next;
      future = [];
      return true;
    },
    /** @returns {{ nodes: object[], edges: object[] } | null} */
    undo() {
      if (!past.length) return null;
      future.push(present);
      present = past.pop();
      return JSON.parse(present);
    },
    /** @returns {{ nodes: object[], edges: object[] } | null} */
    redo() {
      if (!future.length) return null;
      past.push(present);
      present = future.pop();
      return JSON.parse(present);
    },
    canUndo: () => past.length > 0,
    canRedo: () => future.length > 0,
  };
}
