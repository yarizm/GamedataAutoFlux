import './style.css';
import { createStore } from './core/store.js';
import { bindNavigation, bindModalOverlayClose, restartAutoRefresh } from './core/router.js';
import { initWebSocket } from './core/websocket.js';

export const store = createStore({
  activeTab: 'dashboard',
});

const pages = {
  dashboard: () => import('./pages/dashboard/index.js'),
  tasks: () => import('./pages/tasks/index.js'),
  pipelines: () => import('./pages/pipelines/index.js'),
  data: () => import('./pages/data/index.js'),
  reports: () => import('./pages/reports/index.js'),
  cron: () => import('./pages/cron/index.js'),
  agent: () => import('./pages/agent/index.js'),
  system: () => import('./pages/system/index.js'),
};

let currentPage = null;

async function loadPage(tab) {
  if (currentPage && currentPage.destroy) {
    currentPage.destroy();
    currentPage = null;
  }

  const container = document.getElementById(`tab-${tab}`);
  if (!container) return;

  try {
    const mod = await pages[tab]();
    const page = mod.default || mod[Object.keys(mod)[0]];
    if (page) {
      currentPage = page;
      if (page.init) page.init(container, store);
      // Register on window for global function bridges
      window[`_${tab}Page`] = page;
    }
  } catch (err) {
    console.error(`Failed to load page "${tab}":`, err);
  }
}

document.addEventListener('DOMContentLoaded', () => {
  bindNavigation(store);
  bindModalOverlayClose();
  initWebSocket(store);
  restartAutoRefresh(store);

  store.subscribe((key, value) => {
    if (key === 'activeTab') loadPage(value);
  });

  loadPage('dashboard');
});
