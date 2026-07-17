import './style.css';
import { createStore } from './core/store.js';
import { activateTab, bindNavigation, bindModalOverlayClose, restartAutoRefresh } from './core/router.js';
import { initWebSocket } from './core/websocket.js';
import { applyTranslations, bindLanguageControls, configureI18n, getLanguage, setLanguage, t } from './core/i18n.js';
import { initTheme, bindThemeControls } from './core/theme.js';
import { initHelp } from './core/help/drawer.js';
import { createSpotlight } from './core/help/spotlight.js';
import {
  getCollectorForPipeline,
  hasStorageStep,
  loadAvailablePipelines,
  loadPipelineTemplates,
  populatePipelineSelect,
} from './core/pipelines.js';

export const store = createStore({
  activeTab: 'dashboard',
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
  system: () => import('./pages/system/index.js'),
};

const pageInstances = {};
const pageLoadPromises = {};

function updateHeader(tab) {
  const activeLink = document.querySelector(`.nav-link[data-tab="${tab}"]`);
  const headerTitle = document.getElementById('header-title');
  if (activeLink && headerTitle) {
    headerTitle.textContent = activeLink.textContent.trim();
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

async function loadPage(tab) {
  updateHeader(tab);
  const alreadyLoaded = Boolean(pageInstances[tab]);
  const page = await ensurePage(tab);
  if (alreadyLoaded && page?.refresh) page.refresh();
  applyTranslations(document);
}

export async function runPageAction(tab, method, args = [], options = {}) {
  const { activate = false } = options;
  if (activate && store.get('activeTab') !== tab) {
    activateTab(tab, store);
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
  window.ensurePage = ensurePage;
  window.runPageAction = runPageAction;
  window.activateTab = (tab) => activateTab(tab, store);
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
  updateHeader(store.get('activeTab'));
  for (const page of Object.values(pageInstances)) {
    if (typeof page?.refresh === 'function') page.refresh();
  }
  if (window.__help?.refresh) window.__help.refresh();
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
  initWebSocket(store);
  restartAutoRefresh(store);

  const help = initHelp({
    store,
    activateTab: (tab) => activateTab(tab, store),
    ensurePage,
  });
  const spotlight = createSpotlight({
    ensurePage,
    activateTab: (tab) => activateTab(tab, store),
    onComplete: () => {
      help.refresh();
    },
  });
  help.setTourHandler((tourId) => {
    help.close();
    spotlight.start(tourId);
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
    if (key === 'activeTab') loadPage(value);
    else if (key === 'taskUpdate') handleTaskUpdate(value);
    else if (key === 'statsUpdate') handleStatsUpdate(value);
    else if (key === 'language') handleLanguageChange();
  });

  loadPage('dashboard');
  // Never auto-open help on load
});
