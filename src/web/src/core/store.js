export function createStore(initialState = {}) {
  const state = { ...initialState };
  const listeners = new Set();

  function getState() {
    return { ...state };
  }

  function get(key) {
    return state[key];
  }

  function set(key, value) {
    const old = state[key];
    state[key] = value;
    for (const fn of listeners) {
      try { fn(key, value, old); } catch (e) { console.error('store listener error:', e); }
    }
  }

  function subscribe(fn) {
    listeners.add(fn);
    return () => listeners.delete(fn);
  }

  return { getState, get, set, subscribe };
}
