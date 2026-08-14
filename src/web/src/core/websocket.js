let wsConnection = null;
let _store = null;
let reconnectTimer = null;
let reconnectAttempts = 0;
let connectionStatus = 'disconnected';

const BASE_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 30000;
const BACKOFF_FACTOR = 1.5;

function setStatus(status, store) {
  connectionStatus = status;
  if (store) {
    store.set('wsStatus', status);
  }
}

export function getWebSocket() {
  return wsConnection;
}

export function getWebSocketStatus() {
  return connectionStatus;
}

export function initWebSocket(store) {
  _store = store;
  if (wsConnection && (wsConnection.readyState === WebSocket.OPEN || wsConnection.readyState === WebSocket.CONNECTING)) {
    return;
  }

  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/api/ws/tasks`;

  setStatus('connecting', store);

  try {
    wsConnection = new WebSocket(wsUrl);
  } catch (err) {
    console.error('WebSocket creation error:', err);
    scheduleReconnect(store);
    return;
  }

  wsConnection.onopen = () => {
    const wasReconnecting = reconnectAttempts > 0;
    reconnectAttempts = 0;
    setStatus('connected', store);
    console.log('WebSocket connected');

    // If reconnected after a disruption, auto-resync state for active page
    if (wasReconnecting && store) {
      store.set('wsReconnected', Date.now());
      const activeTab = store.get('activeTab');
      if (activeTab && activeTab !== 'agent') {
        store.set('refresh', activeTab);
      }
    }
  };

  wsConnection.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      if (!data || !store) return;

      if (data.type === 'task_update' && data.task) {
        store.set('taskUpdate', data.task);
      } else if (data.type === 'stats_update' && data.stats) {
        store.set('statsUpdate', data.stats);
      } else if (data.type === 'report_progress') {
        store.set('reportProgress', data);
      }
    } catch (e) {
      console.error('WS message parse error:', e);
    }
  };

  wsConnection.onclose = () => {
    setStatus('disconnected', store);
    wsConnection = null;
    scheduleReconnect(store);
  };

  wsConnection.onerror = (err) => {
    console.error('WebSocket error:', err);
    // onclose will be triggered automatically following error
  };
}

function scheduleReconnect(store) {
  if (reconnectTimer) return;

  reconnectAttempts++;
  const backoff = Math.min(
    MAX_RECONNECT_DELAY_MS,
    BASE_RECONNECT_DELAY_MS * Math.pow(BACKOFF_FACTOR, reconnectAttempts - 1),
  );
  const jitter = Math.random() * 500;
  const delay = Math.round(backoff + jitter);

  console.log(`WebSocket disconnected. Reconnecting in ${delay}ms (attempt ${reconnectAttempts})...`);
  setStatus('connecting', store);

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    initWebSocket(store);
  }, delay);
}

// Backward compat
if (typeof window !== 'undefined') {
  window.initWebSocket = initWebSocket;
  window.getWebSocket = getWebSocket;
  window.getWebSocketStatus = getWebSocketStatus;
  Object.defineProperty(window, 'wsConnection', {
    get() {
      return wsConnection;
    },
    configurable: true,
  });
}
