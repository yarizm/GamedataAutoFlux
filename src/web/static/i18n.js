(function () {
  const STORAGE_KEY = 'gamedata-autoflux.language';
  const DEFAULT_LANGUAGE = 'zh-CN';
  const messages = {
    'zh-CN': {
      'lang.zh': '中文', 'lang.en': 'EN', 'common.language': '语言', 'common.workspace': '工作区',
      'common.refresh': '刷新', 'common.create': '创建', 'common.delete': '删除', 'common.cancel': '取消',
      'common.submit': '提交', 'common.previous': '上一步', 'common.next': '下一步', 'common.save': '保存',
      'common.details': '详情', 'common.logs': '日志', 'common.loading': '加载中...', 'common.name': '名称',
      'common.status': '状态', 'common.progress': '进度', 'common.actions': '操作', 'common.time': '时间',
      'common.empty.tasks': '暂无任务', 'common.empty.report': '尚未生成报告',
      'status.pending': '等待中', 'status.running': '运行中', 'status.success': '成功', 'status.failed': '失败', 'status.cancelled': '已取消',
      'nav.dashboard': '仪表盘', 'nav.tasks': '任务管理', 'nav.pipelines': 'Pipeline', 'nav.data': '数据浏览',
      'nav.reports': '报告', 'nav.cron': '定时任务', 'nav.agent': 'AI 助手', 'nav.system': '系统检查',
      'dashboard.title': '系统概览', 'dashboard.totalTasks': '任务总数', 'dashboard.completed': '已完成',
      'dashboard.cronJobs': '定时任务', 'dashboard.components': '已注册组件', 'dashboard.chartTitle': '任务执行趋势',
      'dashboard.recentTasks': '最近任务', 'tasks.title': '任务管理', 'tasks.create': '+ 创建任务',
      'tasks.allStatus': '全部状态', 'tasks.autoRefreshHint': '运行中的任务会自动刷新', 'tasks.targetCount': '目标数',
      'tasks.duration': '耗时', 'tasks.modal.create': '创建任务', 'tasks.wizard.basic': '基础信息',
      'tasks.wizard.collect': '采集配置', 'tasks.wizard.report': '报告选项与提交', 'tasks.name': '任务名称',
      'tasks.selectPipeline': '-- 选择 Pipeline --', 'pipelines.title': 'Pipeline 配置', 'pipelines.create': '+ 创建 Pipeline',
      'pipelines.components': '可用组件', 'pipelines.configured': '已配置 Pipeline', 'pipelines.empty.pipelines': '暂无 Pipeline',
      'data.title': '数据浏览', 'data.search': '搜索', 'data.games': '游戏分类', 'data.chooseGame': '选择一个游戏',
      'data.summary': '按 App ID 或游戏名聚合已落库 JSON', 'data.allSources': '全部数据源', 'data.useForReport': '用于报告',
      'data.addToReport': '加入报告', 'data.exportSelected': '导出选中', 'data.deleteSelected': '删除选中',
      'data.newest': '最新优先', 'data.oldest': '最早优先', 'data.records': '记录', 'data.source': '数据源',
      'data.abstract': '摘要', 'data.chooseCategory': '请选择左侧游戏分类', 'data.previewTitle': '原始 JSON 预览',
      'data.previewEmpty': '选择一条记录后在这里预览', 'reports.title': '报告生成', 'reports.generate': '生成报告',
      'reports.params': '生成参数', 'reports.prompt': '提示词', 'reports.sourceFilter': '数据源过滤',
      'reports.recordKeys': '指定原始 JSON', 'reports.clearSelection': '清空选择', 'reports.uploadJson': '上传 JSON 数据源',
      'reports.importDataList': '导入数据列表', 'reports.addedSources': '已添加数据源', 'reports.template': '模板',
      'reports.history': '报告历史', 'reports.content': '报告内容', 'reports.empty': '暂无报告',
      'cron.title': '定时任务', 'cron.create': '+ 添加定时任务', 'cron.empty': '暂无定时任务',
      'agent.title': 'AI 助手', 'agent.config': '配置', 'agent.clear': '清空对话', 'agent.new': '+ 新建', 'agent.send': '发送',
      'system.title': '系统检查', 'system.overall': '总体状态', 'system.errors': '错误', 'system.warnings': '警告',
      'system.ok': '正常', 'system.checks': '诊断项目', 'system.paths': '运行路径',
    },
    'en-US': {
      'lang.zh': '中文', 'lang.en': 'EN', 'common.language': 'Language', 'common.workspace': 'Workspace',
      'common.refresh': 'Refresh', 'common.create': 'Create', 'common.delete': 'Delete', 'common.cancel': 'Cancel',
      'common.submit': 'Submit', 'common.previous': 'Previous', 'common.next': 'Next', 'common.save': 'Save',
      'common.details': 'Details', 'common.logs': 'Logs', 'common.loading': 'Loading...', 'common.name': 'Name',
      'common.status': 'Status', 'common.progress': 'Progress', 'common.actions': 'Actions', 'common.time': 'Time',
      'common.empty.tasks': 'No tasks', 'common.empty.report': 'No report generated',
      'status.pending': 'Pending', 'status.running': 'Running', 'status.success': 'Success', 'status.failed': 'Failed', 'status.cancelled': 'Cancelled',
      'nav.dashboard': 'Dashboard', 'nav.tasks': 'Tasks', 'nav.pipelines': 'Pipeline', 'nav.data': 'Data',
      'nav.reports': 'Reports', 'nav.cron': 'Cron Jobs', 'nav.agent': 'AI Assistant', 'nav.system': 'System',
      'dashboard.title': 'System Overview', 'dashboard.totalTasks': 'Total Tasks', 'dashboard.completed': 'Completed',
      'dashboard.cronJobs': 'Cron Jobs', 'dashboard.components': 'Registered Components', 'dashboard.chartTitle': 'Task Execution Trend',
      'dashboard.recentTasks': 'Recent Tasks', 'tasks.title': 'Task Management', 'tasks.create': '+ Create Task',
      'tasks.allStatus': 'All Statuses', 'tasks.autoRefreshHint': 'Running tasks refresh automatically', 'tasks.targetCount': 'Targets',
      'tasks.duration': 'Duration', 'tasks.modal.create': 'Create Task', 'tasks.wizard.basic': 'Basic Info',
      'tasks.wizard.collect': 'Collection Config', 'tasks.wizard.report': 'Report Options & Submit', 'tasks.name': 'Task Name',
      'tasks.selectPipeline': '-- Select Pipeline --', 'pipelines.title': 'Pipeline Config', 'pipelines.create': '+ Create Pipeline',
      'pipelines.components': 'Available Components', 'pipelines.configured': 'Configured Pipelines', 'pipelines.empty.pipelines': 'No pipelines',
      'data.title': 'Data Browser', 'data.search': 'Search', 'data.games': 'Game Categories', 'data.chooseGame': 'Choose a game',
      'data.summary': 'Aggregated stored JSON by App ID or game name', 'data.allSources': 'All Sources', 'data.useForReport': 'Use for Report',
      'data.addToReport': 'Add to Report', 'data.exportSelected': 'Export Selected', 'data.deleteSelected': 'Delete Selected',
      'data.newest': 'Newest First', 'data.oldest': 'Oldest First', 'data.records': 'Records', 'data.source': 'Data Source',
      'data.abstract': 'Summary', 'data.chooseCategory': 'Select a game category on the left', 'data.previewTitle': 'Raw JSON Preview',
      'data.previewEmpty': 'Select a record to preview it here', 'reports.title': 'Report Generation', 'reports.generate': 'Generate Report',
      'reports.params': 'Generation Parameters', 'reports.prompt': 'Prompt', 'reports.sourceFilter': 'Data Source Filter',
      'reports.recordKeys': 'Specific Raw JSON', 'reports.clearSelection': 'Clear Selection', 'reports.uploadJson': 'Upload JSON Data Source',
      'reports.importDataList': 'Import Data List', 'reports.addedSources': 'Added Data Sources', 'reports.template': 'Template',
      'reports.history': 'Report History', 'reports.content': 'Report Content', 'reports.empty': 'No reports',
      'cron.title': 'Cron Jobs', 'cron.create': '+ Add Cron Job', 'cron.empty': 'No cron jobs',
      'agent.title': 'AI Assistant', 'agent.config': 'Config', 'agent.clear': 'Clear Chat', 'agent.new': '+ New', 'agent.send': 'Send',
      'system.title': 'System Check', 'system.overall': 'Overall Status', 'system.errors': 'Errors', 'system.warnings': 'Warnings',
      'system.ok': 'OK', 'system.checks': 'Diagnostics', 'system.paths': 'Runtime Paths',
    },
  };

  const textToKey = new Map();
  Object.entries(messages).forEach(([, bundle]) => {
    Object.entries(bundle).forEach(([key, value]) => {
      if (value && !value.includes('{')) textToKey.set(value, key);
    });
  });

  let language = messages[localStorage.getItem(STORAGE_KEY)] ? localStorage.getItem(STORAGE_KEY) : DEFAULT_LANGUAGE;

  function t(key, params) {
    return String((messages[language] && messages[language][key]) || messages[DEFAULT_LANGUAGE][key] || key)
      .replace(/\{(\w+)\}/g, function (_, name) { return params && params[name] != null ? params[name] : ''; });
  }

  function refreshControls(root) {
    (root || document).querySelectorAll('[data-lang]').forEach(function (button) {
      const selected = button.dataset.lang === language;
      button.classList.toggle('active', selected);
      button.setAttribute('aria-pressed', selected ? 'true' : 'false');
    });
  }

  function translateMarked(root) {
    root.querySelectorAll('[data-i18n]').forEach(function (el) { el.textContent = t(el.dataset.i18n); });
    root.querySelectorAll('[data-i18n-placeholder]').forEach(function (el) { el.setAttribute('placeholder', t(el.dataset.i18nPlaceholder)); });
    root.querySelectorAll('[data-i18n-title]').forEach(function (el) { el.setAttribute('title', t(el.dataset.i18nTitle)); });
  }

  function applyTranslations(root) {
    root = root || document;
    document.documentElement.lang = language;
    translateMarked(root);
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    const nodes = [];
    while (walker.nextNode()) {
      const node = walker.currentNode;
      const parent = node.parentElement;
      if (!parent || parent.closest('script,style,textarea,input,code,pre,.CodeMirror,.report-output')) continue;
      const trimmed = node.nodeValue.trim();
      if (trimmed && textToKey.has(trimmed)) nodes.push(node);
    }
    nodes.forEach(function (node) {
      const trimmed = node.nodeValue.trim();
      node.nodeValue = node.nodeValue.replace(trimmed, t(textToKey.get(trimmed)));
    });
    refreshControls(root);
  }

  function setLanguage(lang) {
    if (!messages[lang]) lang = DEFAULT_LANGUAGE;
    language = lang;
    localStorage.setItem(STORAGE_KEY, language);
    applyTranslations(document);
    window.dispatchEvent(new CustomEvent('languagechange', { detail: { language: language } }));
    return language;
  }

  window.t = t;
  window.setLanguage = setLanguage;
  window.getLanguage = function () { return language; };
  window.applyTranslations = applyTranslations;
  document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('[data-lang]').forEach(function (button) {
      button.addEventListener('click', function () { setLanguage(button.dataset.lang); });
    });
    applyTranslations(document);
  });
})();
