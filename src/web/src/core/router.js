const AUTO_REFRESH_INTERVAL_MS = 30000;
let autoRefreshHandle = null;
let editorsModulePromise = null;
let isRouterInitialized = false;

export const VALID_TABS = [
  'dashboard',
  'tasks',
  'pipelines',
  'data',
  'reports',
  'cron',
  'dag',
  'agent',
  'plugins',
  'system',
];

/**
 * Parse raw hash string into route object { tab, params }.
 * Handles '#/tasks?id=123', '#dag?name=steam_basic', '/plugins', '', etc.
 */
export function parseRoute(rawHash = '') {
  let clean = String(rawHash || '').trim();
  if (clean.startsWith('#')) clean = clean.slice(1);
  if (clean.startsWith('/')) clean = clean.slice(1);

  const [pathPart, queryPart] = clean.split('?');
  const rawTab = (pathPart || '').split('/')[0].trim().toLowerCase();
  const tab = VALID_TABS.includes(rawTab) ? rawTab : 'dashboard';

  const params = {};
  if (queryPart) {
    const searchParams = new URLSearchParams(queryPart);
    for (const [key, value] of searchParams.entries()) {
      if (key) params[key] = value;
    }
  }

  return { tab, params };
}

/**
 * Build a canonical hash string from tab name and parameters.
 */
export function buildRoute(tab, params = {}) {
  const validTab = VALID_TABS.includes(tab) ? tab : 'dashboard';
  const searchParams = new URLSearchParams();

  if (params && typeof params === 'object') {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null && value !== '') {
        searchParams.append(key, String(value));
      }
    }
  }

  const queryString = searchParams.toString();
  return queryString ? `#/${validTab}?${queryString}` : `#/${validTab}`;
}

/**
 * Navigate to a specific tab and route parameters, updating URL hash.
 */
export function navigate(tab, params = {}, options = {}) {
  const { replace = false, store = null } = options;
  const targetHash = buildRoute(tab, params);

  if (typeof window !== 'undefined' && window.location) {
    const currentHash = window.location.hash || '';
    if (currentHash !== targetHash) {
      if (replace && window.history?.replaceState) {
        window.history.replaceState(null, '', targetHash);
      } else {
        window.location.hash = targetHash;
      }
    }
  }

  if (store) {
    activateTab(tab, store, params, { syncHash: false });
  }
}

/**
 * Activate a tab visually and update store state.
 */
export function activateTab(tab, store, params = {}, options = { syncHash: true, replace: false }) {
  const validTab = VALID_TABS.includes(tab) ? tab : 'dashboard';

  if (store) {
    store.set('routeParams', params || {});
    store.set('activeTab', validTab);
  }

  if (typeof document !== 'undefined') {
    document.querySelectorAll('.nav-link').forEach((item) => {
      item.classList.toggle('active', item.dataset.tab === validTab);
    });

    document.querySelectorAll('.tab-content').forEach((panel) => panel.classList.remove('active'));
    const target = document.getElementById(`tab-${validTab}`);
    if (target) target.classList.add('active');
  }

  if (options?.syncHash && typeof window !== 'undefined' && window.location) {
    const targetHash = buildRoute(validTab, params);
    if ((window.location.hash || '') !== targetHash) {
      if (options.replace && window.history?.replaceState) {
        window.history.replaceState(null, '', targetHash);
      } else {
        window.location.hash = targetHash;
      }
    }
  }
}

/**
 * Initialize Hash router and history change listeners.
 */
export function initRouter(store) {
  if (isRouterInitialized) return parseRoute(window.location.hash);
  isRouterInitialized = true;

  const onHashChange = () => {
    const { tab, params } = parseRoute(window.location.hash);
    activateTab(tab, store, params, { syncHash: false });
  };

  window.addEventListener('hashchange', onHashChange);
  window.addEventListener('popstate', onHashChange);

  const initialRoute = parseRoute(window.location.hash);
  const targetHash = buildRoute(initialRoute.tab, initialRoute.params);

  // Sync initial hash if empty or invalid
  if (window.location.hash !== targetHash) {
    if (window.history?.replaceState) {
      window.history.replaceState(null, '', targetHash);
    } else {
      window.location.hash = targetHash;
    }
  }

  activateTab(initialRoute.tab, store, initialRoute.params, { syncHash: false });
  return initialRoute;
}

export function bindNavigation(store) {
  if (typeof document === 'undefined') return;
  document.querySelectorAll('.nav-link').forEach((link) => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      const tab = link.dataset.tab;
      if (tab) {
        navigate(tab, {}, { store });
      }
    });
  });
}

export function bindModalOverlayClose() {
  if (typeof document === 'undefined') return;
  document.querySelectorAll('.modal-overlay').forEach((overlay) => {
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) {
        overlay.classList.remove('show');
      }
    });
  });
}

export function restartAutoRefresh(store) {
  if (autoRefreshHandle) {
    clearInterval(autoRefreshHandle);
    autoRefreshHandle = null;
  }

  autoRefreshHandle = setInterval(() => {
    const tab = store?.get?.('activeTab');
    if (tab && VALID_TABS.includes(tab) && tab !== 'agent') {
      store.set('refresh', tab);
    }
  }, AUTO_REFRESH_INTERVAL_MS);
}

async function ensureEditorsForModal(id) {
  if (id !== 'modal-create-task' && id !== 'modal-create-pipeline') {
    return null;
  }
  editorsModulePromise ||= import('./editors.js');
  const module = await editorsModulePromise;
  module.initEditors();
  return module;
}

export function openModal(id) {
  const modal = document.getElementById(id);
  if (modal) {
    modal.classList.add('show');
    setTimeout(async () => {
      const editors = await ensureEditorsForModal(id);
      editors?.refreshEditorForModal(id);
    }, 10);
  }
}

export function closeModal(id) {
  const modal = document.getElementById(id);
  if (modal) modal.classList.remove('show');
}

// Backward compatibility for legacy templates and global window functions
if (typeof window !== 'undefined') {
  window.activateTab = (tab, store, params, options) => activateTab(tab, store || window._globalStore, params, options);
  window.navigate = navigate;
  window.parseRoute = parseRoute;
  window.buildRoute = buildRoute;
  window.bindNavigation = bindNavigation;
  window.bindModalOverlayClose = bindModalOverlayClose;
  window.restartAutoRefresh = restartAutoRefresh;
  window.openModal = openModal;
  window.closeModal = closeModal;
}
