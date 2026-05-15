import { toast } from './api.js';

let wsConnection = null;
let _store = null;

export function initWebSocket(store) {
  _store = store;
  if (wsConnection) return;

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/api/ws/tasks`;

  wsConnection = new WebSocket(wsUrl);

  wsConnection.onopen = () => {
    console.log('WebSocket connected');
    toast('实时推送已连接', 'success');
  };

  wsConnection.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
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
    console.log('WebSocket disconnected, retrying in 5s...');
    wsConnection = null;
    setTimeout(() => initWebSocket(store), 5000);
  };

  wsConnection.onerror = (err) => {
    console.error('WebSocket error:', err);
  };
}

// Backward compat
window.initWebSocket = initWebSocket;
window.wsConnection = wsConnection;
