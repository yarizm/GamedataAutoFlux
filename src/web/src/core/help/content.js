// Pure data registry for Platform Help (map + page help + tours).
// i18n keys are resolved by callers; this module has no UI.

export const MAIN_TABS = [
  'dashboard', 'tasks', 'pipelines', 'data', 'reports', 'cron', 'dag', 'agent', 'system',
];

export const mapCards = [
  { id: 'dashboard', tab: 'dashboard', titleKey: 'help.map.dashboard.title', blurbKey: 'help.map.dashboard.blurb' },
  { id: 'tasks', tab: 'tasks', titleKey: 'help.map.tasks.title', blurbKey: 'help.map.tasks.blurb' },
  { id: 'pipelines', tab: 'pipelines', titleKey: 'help.map.pipelines.title', blurbKey: 'help.map.pipelines.blurb' },
  { id: 'dag', tab: 'dag', titleKey: 'help.map.dag.title', blurbKey: 'help.map.dag.blurb', badge: 'advanced' },
  { id: 'data', tab: 'data', titleKey: 'help.map.data.title', blurbKey: 'help.map.data.blurb' },
  { id: 'reports', tab: 'reports', titleKey: 'help.map.reports.title', blurbKey: 'help.map.reports.blurb' },
  { id: 'cron', tab: 'cron', titleKey: 'help.map.cron.title', blurbKey: 'help.map.cron.blurb' },
  { id: 'agent', tab: 'agent', titleKey: 'help.map.agent.title', blurbKey: 'help.map.agent.blurb' },
  { id: 'system', tab: 'system', titleKey: 'help.map.system.title', blurbKey: 'help.map.system.blurb' },
];

export const pageHelp = {
  dashboard: {
    summaryKey: 'help.page.dashboard.summary',
    points: [
      'help.page.dashboard.p1',
      'help.page.dashboard.p2',
      'help.page.dashboard.p3',
    ],
    nextSteps: [
      { labelKey: 'help.page.dashboard.next.tasks', action: { type: 'tab', tab: 'tasks' } },
      { labelKey: 'help.page.dashboard.next.system', action: { type: 'tab', tab: 'system' } },
    ],
    tourId: 'page-dashboard',
  },
  tasks: {
    summaryKey: 'help.page.tasks.summary',
    points: ['help.page.tasks.p1', 'help.page.tasks.p2', 'help.page.tasks.p3'],
    nextSteps: [
      { labelKey: 'help.page.tasks.next.data', action: { type: 'tab', tab: 'data' } },
      { labelKey: 'help.page.tasks.next.agent', action: { type: 'tab', tab: 'agent' } },
    ],
    tourId: 'page-tasks',
  },
  pipelines: {
    summaryKey: 'help.page.pipelines.summary',
    points: ['help.page.pipelines.p1', 'help.page.pipelines.p2', 'help.page.pipelines.p3'],
    nextSteps: [
      { labelKey: 'help.page.pipelines.next.tasks', action: { type: 'tab', tab: 'tasks' } },
      { labelKey: 'help.page.pipelines.next.dag', action: { type: 'tab', tab: 'dag' } },
    ],
  },
  data: {
    summaryKey: 'help.page.data.summary',
    points: ['help.page.data.p1', 'help.page.data.p2', 'help.page.data.p3'],
    nextSteps: [
      { labelKey: 'help.page.data.next.reports', action: { type: 'tab', tab: 'reports' } },
    ],
  },
  reports: {
    summaryKey: 'help.page.reports.summary',
    points: ['help.page.reports.p1', 'help.page.reports.p2', 'help.page.reports.p3'],
    nextSteps: [
      { labelKey: 'help.page.reports.next.data', action: { type: 'tab', tab: 'data' } },
    ],
  },
  cron: {
    summaryKey: 'help.page.cron.summary',
    points: ['help.page.cron.p1', 'help.page.cron.p2', 'help.page.cron.p3'],
    nextSteps: [
      { labelKey: 'help.page.cron.next.tasks', action: { type: 'tab', tab: 'tasks' } },
    ],
  },
  dag: {
    summaryKey: 'help.page.dag.summary',
    points: ['help.page.dag.p1', 'help.page.dag.p2', 'help.page.dag.p3'],
    nextSteps: [
      { labelKey: 'help.page.dag.next.pipelines', action: { type: 'tab', tab: 'pipelines' } },
    ],
  },
  agent: {
    summaryKey: 'help.page.agent.summary',
    points: ['help.page.agent.p1', 'help.page.agent.p2', 'help.page.agent.p3'],
    nextSteps: [
      { labelKey: 'help.page.agent.next.system', action: { type: 'tab', tab: 'system' } },
      { labelKey: 'help.page.agent.next.tasks', action: { type: 'tab', tab: 'tasks' } },
    ],
    tourId: 'page-agent',
  },
  system: {
    summaryKey: 'help.page.system.summary',
    points: ['help.page.system.p1', 'help.page.system.p2', 'help.page.system.p3'],
    nextSteps: [
      { labelKey: 'help.page.system.next.dashboard', action: { type: 'tab', tab: 'dashboard' } },
    ],
  },
};

export const tours = {
  'platform-overview': {
    id: 'platform-overview',
    titleKey: 'help.tour.platformOverview.title',
    steps: [
      {
        target: '[data-tour-id="nav"]',
        titleKey: 'help.tour.platformOverview.s1.title',
        bodyKey: 'help.tour.platformOverview.s1.body',
      },
      {
        target: '[data-tour-id="dashboard-stats"]',
        titleKey: 'help.tour.platformOverview.s2.title',
        bodyKey: 'help.tour.platformOverview.s2.body',
        before: 'ensure-tab:dashboard',
      },
      {
        target: '[data-tour-id="nav-tasks"]',
        titleKey: 'help.tour.platformOverview.s3.title',
        bodyKey: 'help.tour.platformOverview.s3.body',
      },
      {
        target: '[data-tour-id="nav-agent"]',
        titleKey: 'help.tour.platformOverview.s4.title',
        bodyKey: 'help.tour.platformOverview.s4.body',
      },
      {
        target: '[data-tour-id="nav-system"]',
        titleKey: 'help.tour.platformOverview.s5.title',
        bodyKey: 'help.tour.platformOverview.s5.body',
      },
    ],
  },
  'page-dashboard': {
    id: 'page-dashboard',
    titleKey: 'help.tour.pageDashboard.title',
    steps: [
      {
        target: '[data-tour-id="dashboard-stats"]',
        titleKey: 'help.tour.pageDashboard.s1.title',
        bodyKey: 'help.tour.pageDashboard.s1.body',
        before: 'ensure-tab:dashboard',
      },
      {
        target: '[data-tour-id="dashboard-recent"]',
        titleKey: 'help.tour.pageDashboard.s2.title',
        bodyKey: 'help.tour.pageDashboard.s2.body',
        before: 'ensure-tab:dashboard',
      },
      {
        target: '[data-tour-id="btn-refresh-dashboard"]',
        titleKey: 'help.tour.pageDashboard.s3.title',
        bodyKey: 'help.tour.pageDashboard.s3.body',
        before: 'ensure-tab:dashboard',
      },
    ],
  },
  'page-tasks': {
    id: 'page-tasks',
    titleKey: 'help.tour.pageTasks.title',
    steps: [
      {
        target: '[data-tour-id="tasks-create"]',
        titleKey: 'help.tour.pageTasks.s1.title',
        bodyKey: 'help.tour.pageTasks.s1.body',
        before: 'ensure-tab:tasks',
      },
      {
        target: '[data-tour-id="tasks-list"]',
        titleKey: 'help.tour.pageTasks.s2.title',
        bodyKey: 'help.tour.pageTasks.s2.body',
        before: 'ensure-tab:tasks',
      },
      {
        target: '[data-tour-id="tasks-filters"]',
        titleKey: 'help.tour.pageTasks.s3.title',
        bodyKey: 'help.tour.pageTasks.s3.body',
        before: 'ensure-tab:tasks',
      },
    ],
  },
  'page-agent': {
    id: 'page-agent',
    titleKey: 'help.tour.pageAgent.title',
    steps: [
      {
        target: '[data-tour-id="agent-intent-chips"]',
        titleKey: 'help.tour.pageAgent.s1.title',
        bodyKey: 'help.tour.pageAgent.s1.body',
        before: 'ensure-tab:agent',
      },
      {
        target: '[data-tour-id="agent-chat"]',
        titleKey: 'help.tour.pageAgent.s2.title',
        bodyKey: 'help.tour.pageAgent.s2.body',
        before: 'ensure-tab:agent',
      },
      {
        target: '[data-tour-id="agent-sessions"]',
        titleKey: 'help.tour.pageAgent.s3.title',
        bodyKey: 'help.tour.pageAgent.s3.body',
        before: 'ensure-tab:agent',
      },
    ],
  },
};

export function getPageHelp(tab) {
  const key = String(tab || '').trim();
  return pageHelp[key] || null;
}

export function getTour(tourId) {
  const id = String(tourId || '').trim();
  return tours[id] || null;
}
