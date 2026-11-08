import { handleUnauthorized, isSignedOut } from './auth.js';

let wsConnection = null;
let _store = null;
let reconnectTimer = null;
let reconnectAttempts = 0;
let connectionStatus = 'disconnected';

const BASE_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 30000;
const BACKOFF_FACTOR = 1.5;
// 服务端鉴权拒绝 WebSocket 时用 1008（策略违规）；握手阶段就被拒时浏览器只能看到 1006，
// 表现为连接从未 open 就关闭。
const WS_POLICY_VIOLATION = 1008;

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

  let socket;
  try {
    socket = new WebSocket(wsUrl);
  } catch (err) {
    console.error('WebSocket creation error:', err);
    scheduleReconnect(store);
    return;
  }
  wsConnection = socket;
  let opened = false;

  socket.onopen = () => {
    opened = true;
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

  socket.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      if (!data || !store) return;

      if (data.type === 'task_update' && data.task) {
        store.set('taskUpdate', data.task);
      } else if (data.type === 'task_event' && data.event) {
        store.set('taskEvent', data.event);
      } else if (data.type === 'stats_update' && data.stats) {
        store.set('statsUpdate', data.stats);
      } else if (data.type === 'report_progress') {
        store.set('reportProgress', data);
      }
    } catch (e) {
      console.error('WS message parse error:', e);
    }
  };

  socket.onclose = (event) => {
    if (wsConnection && wsConnection !== socket) return; // 已被更新的连接取代
    setStatus('disconnected', store);
    wsConnection = null;
    if (!opened || event?.code === WS_POLICY_VIOLATION) {
      // 可能是未登录 / 会话过期被拒：先确认登录状态，未登录就停止重连。
      void reconnectUnlessSignedOut(store);
    } else {
      scheduleReconnect(store);
    }
  };

  socket.onerror = (err) => {
    console.error('WebSocket error:', err);
    // onclose will be triggered automatically following error
  };
}

async function reconnectUnlessSignedOut(store) {
  let result = 'unknown';
  try {
    result = await handleUnauthorized();
  } catch (err) {
    console.error('WebSocket auth check failed:', err);
  }
  if (isSignedOut(result)) {
    console.warn('WebSocket rejected: not signed in, reconnect stopped.');
    return;
  }
  // 已登录或状态接口也不可达（断网、服务重启）：按原逻辑退避重连。
  scheduleReconnect(store);
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
