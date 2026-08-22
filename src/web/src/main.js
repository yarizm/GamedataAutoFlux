import './style.css';
import { createStore } from './core/store.js';
import {
  activateTab,
  bindNavigation,
  bindModalOverlayClose,
  initRouter,
  navigate,
  restartAutoRefresh,
} from './core/router.js';
import { initWebSocket } from './core/websocket.js';
import { applyTranslations, bindLanguageControls, configureI18n, getLanguage, setLanguage, t } from './core/i18n.js';
import { initTheme, bindThemeControls } from './core/theme.js';
import { initHelp } from './core/help/drawer.js';
import { initInlineGuides } from './core/help/inlineGuide.js';
import { createSpotlight } from './core/help/spotlight.js';
import { initCommandPalette } from './core/commandPalette.js';
import {
  getCollectorForPipeline,
  hasStorageStep,
  loadAvailablePipelines,
  loadPipelineTemplates,
  populatePipelineSelect,
} from './core/pipelines.js';

export const store = createStore({
  activeTab: 'dashboard',
  routeParams: {},
  wsStatus: 'disconnected',
  language: getLanguage(),
});

const pages = {
  dashboard: () => import('./pages/dashboard/index.js'),
  tasks: () => import('./pages/tasks/index.js'),
  pipelines: () => import('./pages/pipelines/index.js'),
  data: () => import('./pages/data/index.js'),
  reports: () => import('./pages/reports/index.js'),
  cron: () => import('./pages/cron/index.js'),
  dag: () => import('./pages/dag/index.js'),
  agent: () => import('./pages/agent/index.js'),
  plugins: () => import('./pages/plugins/index.js'),
  system: () => import('./pages/system/index.js'),
};

const pageInstances = {};
const pageLoadPromises = {};
let inlineGuides = null;

function updateHeader(tab) {
  const activeLink = document.querySelector(`.nav-link[data-tab="${tab}"]`);
  const headerTitle = document.getElementById('header-title');
  if (activeLink && headerTitle) {
    headerTitle.textContent = activeLink.textContent.trim();
  }
}

function updateWebSocketIndicator(status) {
  const dot = document.getElementById('ws-status-dot');
  const text = document.getElementById('ws-status-text');
  const container = document.getElementById('ws-status-container');
  if (!dot || !text) return;

  if (status === 'connected') {
    dot.className = 'w-2 h-2 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_8px_rgba(16,185,129,0.8)]';
    text.setAttribute('data-i18n', 'common.systemOnline');
    text.textContent = t('common.systemOnline') || 'System Online';
    if (container) container.title = `${t('common.systemOnline') || 'System Online'} (WebSocket)`;
  } else if (status === 'connecting') {
    dot.className = 'w-2 h-2 rounded-full bg-amber-500 animate-ping shadow-[0_0_8px_rgba(245,158,11,0.8)]';
    text.setAttribute('data-i18n', 'common.connecting');
    text.textContent = t('common.connecting') || 'Connecting...';
    if (container) container.title = `${t('common.connecting') || 'Connecting...'} (WebSocket)`;
  } else {
    dot.className = 'w-2 h-2 rounded-full bg-rose-500 shadow-[0_0_8px_rgba(244,63,94,0.8)]';
    text.setAttribute('data-i18n', 'common.offline');
    text.textContent = t('common.offline') || 'Offline';
    if (container) container.title = `${t('common.offline') || 'Offline'} (WebSocket)`;
  }
}

export async function ensurePage(tab) {
  if (pageInstances[tab]) return pageInstances[tab];
  if (pageLoadPromises[tab]) return pageLoadPromises[tab];
  const container = document.getElementById(`tab-${tab}`);
  if (!container || !pages[tab]) return null;

  pageLoadPromises[tab] = (async () => {
    const mod = await pages[tab]();
    const page = mod.default || mod[Object.keys(mod)[0]];
    if (page) {
      if (page.init) page.init(container, store);
      pageInstances[tab] = page;
      window[`_${tab}Page`] = page;
      installGlobalBridge();
    }
    return page || null;
  })().catch((err) => {
    console.error(`Failed to load page "${tab}":`, err);
    return null;
  }).finally(() => {
    pageLoadPromises[tab] = null;
  });

  return pageLoadPromises[tab];
}

async function loadPage(tab, params = {}) {
  updateHeader(tab);
  const alreadyLoaded = Boolean(pageInstances[tab]);
  const page = await ensurePage(tab);
  if (page) {
    if (typeof page.handleRoute === 'function') {
      try {
        await page.handleRoute(params);
      } catch (err) {
        console.error(`Page ${tab}.handleRoute error:`, err);
      }
    }
    if (alreadyLoaded && page.refresh) {
      page.refresh();
    }
  }
  applyTranslations(document);
}

export async function runPageAction(tab, method, args = [], options = {}) {
  const { activate = false } = options;
  if (activate && store.get('activeTab') !== tab) {
    navigate(tab, {}, { store });
  }
  const page = await ensurePage(tab);
  const fn = page?.[method];
  if (typeof fn !== 'function') {
    console.warn(`Page action not found: ${tab}.${method}`);
    return undefined;
  }
  return fn.apply(page, args);
}

function action(tab, method, options = {}) {
  return (...args) => runPageAction(tab, method, args, options);
}

function installGlobalBridge() {
  window._globalStore = store;
  window.ensurePage = ensurePage;
  window.runPageAction = runPageAction;
  window.activateTab = (tab, customStore, params, options) => activateTab(tab, customStore || store, params, options);
  window.navigate = (tab, params, options) => navigate(tab, params, { ...options, store });
  window.t = t;
  window.getLanguage = getLanguage;
  window.setLanguage = setLanguage;

  window.refreshDashboard = action('dashboard', 'refresh');

  window.loadTasks = action('tasks', 'refresh');
  window.showCreateTaskModal = action('tasks', '_showCreateModal', { activate: true });
  window.updateTaskTargetFields = action('tasks', '_updateTargetFields');
  window.buildTaskTargetsFromForm = action('tasks', '_buildTargets');
  window.renderTaskPrecheck = action('tasks', '_renderPrecheck');
  window.wizardNext = action('tasks', '_wizardNext', { activate: true });
  window.wizardPrev = action('tasks', '_wizardPrev', { activate: true });
  window.createTask = action('tasks', '_createTask', { activate: true });
  window.runTaskPrecheck = action('tasks', '_runPrecheckOnly', { activate: true });
  window.dismissTaskPathBanner = action('tasks', '_dismissPathBanner', { activate: true });
  window.cancelTask = action('tasks', '_cancelTask');
  window.deleteTask = action('tasks', '_deleteTask');
  window.viewTaskLogs = action('tasks', '_viewLogs', { activate: true });
  window.viewTaskDetail = action('tasks', '_viewDetail', { activate: true });
  window.getCollectorForPipeline = getCollectorForPipeline;
  window.hasStorageStep = hasStorageStep;

  window.loadComponents = action('pipelines', '_loadComponents');
  window.loadPipelines = action('pipelines', '_loadPipelines');
  window.showCreatePipelineModal = action('pipelines', '_showCreateModal', { activate: true });
  window.applyPipelineTemplate = action('pipelines', '_applyTemplate', { activate: true });
  window.buildPipelineStepsFromForm = action('pipelines', '_buildStepsFromForm');
  window.createPipeline = action('pipelines', '_createPipeline', { activate: true });
  window.deletePipeline = action('pipelines', '_deletePipeline');
  window.populatePipelineFormComponents = action('pipelines', '_populateFormComponents');
  window.loadPipelineTemplates = loadPipelineTemplates;
  window.loadPipelineSelect = populatePipelineSelect;

  window.loadDataGames = action('data', 'refresh');
  window.loadDataGroups = action('data', '_loadGroups');
  window.searchDataRecords = action('data', '_search', { activate: true });
  window.selectDataGame = action('data', '_selectGame', { activate: true });
  window.loadSelectedGameRecords = action('data', '_loadRecords', { activate: true });
  window.renderDataRecords = action('data', '_renderRecords');
  window.toggleRecordSelect = action('data', '_toggleSelect');
  window.toggleSelectAll = action('data', '_toggleSelectAll');
  window.previewDataRecord = action('data', '_preview', { activate: true });
  window.downloadDataRecord = action('data', '_download');
  window.editDataRecord = action('data', '_edit', { activate: true });
  window.deleteDataRecord = action('data', '_deleteRecord', { activate: true });
  window.deleteDataGame = action('data', '_deleteGame', { activate: true });
  window.refreshDataRecord = action('data', '_refresh');
  window.scheduleDataRecordRefresh = action('data', '_schedule');
  window.useDataRecordForReport = action('data', '_useForReport', { activate: true });
  window.goToPage = action('data', '_goToPage', { activate: true });
  window.changePageSize = action('data', '_changePageSize', { activate: true });
  window.batchDeleteSelected = action('data', '_batchDelete', { activate: true });
  window.batchAddToReport = action('data', '_batchAddToReport', { activate: true });
  window.batchExportSelected = action('data', '_batchExport', { activate: true });

  window.loadReportTemplates = action('reports', '_loadTemplates');
  window.loadReports = action('reports', '_loadReports');
  window.updateReportTemplateHelp = action('reports', '_updateTemplateHelp');
  window.addReportRecordSelection = action('reports', '_addRecordSelection');
  window.removeReportRecordSelection = action('reports', '_removeRecordSelection');
  window.clearSelectedReportRecords = action('reports', '_clearRecordSelections', { activate: true });
  window.syncSelectedReportRecordKeys = action('reports', '_syncRecordKeys');
  window.syncReportRecordKeysFromTextarea = action('reports', '_syncFromTextarea');
  window.renderSelectedReportRecords = action('reports', '_renderSelectedRecords');
  window.renderReportPrecheck = action('reports', '_renderPrecheck');
  window.createFillTaskFromPrecheck = action('reports', '_createFillTask', { activate: true });
  window.uploadReportJsonFiles = action('reports', '_uploadJson', { activate: true });
  window.importReportGroupRecords = action('reports', '_importGroup', { activate: true });
  window.useCurrentDataForReport = action('reports', '_useCurrentData', { activate: true });
  window.generateReport = action('reports', '_generate', { activate: true });
  window.renderReport = action('reports', '_renderReport');
  window.viewReport = action('reports', '_view', { activate: true });
  window.editReport = action('reports', '_edit', { activate: true });
  window.deleteReport = action('reports', '_deleteReport', { activate: true });

  window.loadCronJobs = action('cron', 'refresh');
  window.showCreateCronModal = action('cron', '_showCreateModal', { activate: true });
  window.createCronJob = action('cron', '_createJob', { activate: true });
  window.deleteCronJob = action('cron', '_deleteJob');

  window.sendAgentMessage = action('agent', '_send', { activate: true });
  window.stopAgentMessage = action('agent', '_stop', { activate: true });
  window.clearAgentHistory = action('agent', '_clearHistory', { activate: true });
  window.createAgentSession = action('agent', '_createSession', { activate: true });
  window.editAgentSession = action('agent', '_editSession', { activate: true });
  window.switchAgentSession = action('agent', '_switchSession', { activate: true });
  window.deleteAgentSession = action('agent', '_deleteSession', { activate: true });
  window.showProviderConfigModal = action('agent', '_showProviderConfig', { activate: true });
  window.addProviderConfigRow = action('agent', '_addConfigRow', { activate: true });
  window.refreshProviderDefaultSelect = action('agent', '_refreshDefaultSelect', { activate: true });
  window.saveProviderConfig = action('agent', '_saveProviderConfig', { activate: true });
  window.onAgentProviderChange = action('agent', '_onProviderChange', { activate: true });

  window.loadSystemDiagnostics = (options = {}) => {
    const silent = typeof options === 'boolean' ? options : Boolean(options?.silent);
    return runPageAction('system', 'refresh', [silent]);
  };
}

function isModalOpen(id) {
  return document.getElementById(id)?.classList.contains('show');
}

function handleTaskUpdate(task) {
  const activeTab = store.get('activeTab');
  if (activeTab === 'dashboard') runPageAction('dashboard', 'refresh');
  if (activeTab === 'tasks') runPageAction('tasks', 'refresh');

  if (isModalOpen('modal-task-detail')) {
    const currentIdEl = document.querySelector('#task-detail-content .detail-kv code');
    if (currentIdEl?.textContent === task.id) runPageAction('tasks', '_viewDetail', [task.id]);
  }
  if (isModalOpen('modal-task-logs')) {
    const modalLogs = document.getElementById('modal-task-logs');
    if (modalLogs?.dataset.taskId === task.id) runPageAction('tasks', '_viewLogs', [task.id]);
  }
}

function handleStatsUpdate() {
  if (store.get('activeTab') === 'dashboard') {
    runPageAction('dashboard', 'refresh');
  }
}

function handleLanguageChange() {
  applyTranslations(document);
  inlineGuides?.refresh();
  updateHeader(store.get('activeTab'));
  updateWebSocketIndicator(store.get('wsStatus'));
  for (const page of Object.values(pageInstances)) {
    if (typeof page?.refresh === 'function') page.refresh();
  }
  if (window.__help?.refresh) window.__help.refresh();
}

async function runSpotlightAction(actionName) {
  switch (actionName) {
    case 'task-tour-open':
      return runPageAction('tasks', '_beginGuidedCreate', [], { activate: true });
    case 'task-tour-step-1':
      return runPageAction('tasks', '_showGuidedWizardStep', [1], { activate: true });
    case 'task-tour-step-2':
      return runPageAction('tasks', '_showGuidedWizardStep', [2], { activate: true });
    case 'task-tour-step-3':
      return runPageAction('tasks', '_showGuidedWizardStep', [3], { activate: true });
    case 'task-tour-close':
      return runPageAction('tasks', '_endGuidedCreate');
    default:
      console.warn(`Unknown spotlight action: ${actionName}`);
      return undefined;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  installGlobalBridge();
  configureI18n(store);
  initTheme();
  bindThemeControls();
  bindLanguageControls();
  loadAvailablePipelines().catch((err) => console.error('Load pipeline cache failed:', err));
  bindNavigation(store);
  bindModalOverlayClose();
  inlineGuides = initInlineGuides();
  initWebSocket(store);
  restartAutoRefresh(store);
  initCommandPalette(store);

  // Mobile drawer wiring
  const sidebar = document.getElementById('navbar');
  const backdrop = document.getElementById('sidebar-backdrop');
  const mobileMenuBtn = document.getElementById('btn-mobile-menu');

  function openMobileMenu() {
    if (sidebar) sidebar.classList.add('open');
    if (backdrop) backdrop.classList.add('open');
  }
  function closeMobileMenu() {
    if (sidebar) sidebar.classList.remove('open');
    if (backdrop) backdrop.classList.remove('open');
  }

  mobileMenuBtn?.addEventListener('click', openMobileMenu);
  backdrop?.addEventListener('click', closeMobileMenu);

  // The drawer only exists below 768px. Widening past it (or rotating a
  // tablet to landscape) returns the sidebar to normal flow, so a left-open
  // backdrop would otherwise sit over the whole desktop layout and swallow
  // the first click.
  window.addEventListener('resize', () => {
    if (window.innerWidth > 768) closeMobileMenu();
  });

  window.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeMobileMenu();
  });

  sidebar?.querySelectorAll('.nav-link')?.forEach((link) => {
    link.addEventListener('click', () => {
      if (window.innerWidth <= 768) closeMobileMenu();
    });
  });

  // Command palette trigger button in header
  document.getElementById('btn-command-palette')?.addEventListener('click', () => {
    if (typeof window.openCommandPalette === 'function') {
      window.openCommandPalette();
    }
  });

  const help = initHelp({
    store,
    activateTab: (tab) => navigate(tab, {}, { store }),
    ensurePage,
  });
  const spotlight = createSpotlight({
    ensurePage,
    activateTab: (tab) => navigate(tab, {}, { store }),
    runAction: runSpotlightAction,
    onComplete: () => {
      help.refresh();
    },
  });
  help.setTourHandler((tourId) => {
    help.close();
    void spotlight.start(tourId);
  });
  window.__help = help; // optional debug
  window.__spotlight = spotlight;

  document.getElementById('btn-help')?.addEventListener('click', () => help.toggle());

  // optional shortcut: ? when not in input/textarea/contenteditable
  document.addEventListener('keydown', (e) => {
    if (e.key !== '?' && !(e.key === '/' && e.shiftKey)) return;
    const tag = (e.target && e.target.tagName) || '';
    if (tag === 'INPUT' || tag === 'TEXTAREA' || e.target?.isContentEditable) return;
    e.preventDefault();
    help.toggle();
  });

  store.subscribe((key, value) => {
    if (key === 'activeTab') {
      const params = store.get('routeParams') || {};
      loadPage(value, params);
    } else if (key === 'routeParams') {
      const activeTab = store.get('activeTab');
      const page = pageInstances[activeTab];
      if (page && typeof page.handleRoute === 'function') {
        try {
          page.handleRoute(value);
        } catch (err) {
          console.error(`Page ${activeTab}.handleRoute error:`, err);
        }
      }
    } else if (key === 'wsStatus') {
      updateWebSocketIndicator(value);
    } else if (key === 'taskUpdate') {
      handleTaskUpdate(value);
    } else if (key === 'statsUpdate') {
      handleStatsUpdate(value);
    } else if (key === 'language') {
      handleLanguageChange();
    }
  });

  // Initialize router from current window.location.hash
  const initialRoute = initRouter(store);
  loadPage(initialRoute.tab, initialRoute.params);
});
