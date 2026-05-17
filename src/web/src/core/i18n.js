const STORAGE_KEY = 'gamedata-autoflux.language';
const DEFAULT_LANGUAGE = 'zh-CN';
const SUPPORTED_LANGUAGES = new Set(['zh-CN', 'en-US']);

let activeLanguage = normalizeLanguage(localStorage.getItem(STORAGE_KEY));
let activeStore = null;

export const messages = {
  'zh-CN': {
    'lang.zh': '中文',
    'lang.en': 'EN',
    'common.language': '语言',
    'common.workspace': '工作区',
    'common.systemOnline': '系统在线',
    'common.search': '搜索',
    'common.import': '导入',
    'common.update': '更新',
    'common.schedule': '计划任务',
    'common.refresh': '刷新',
    'common.create': '创建',
    'common.delete': '删除',
    'common.cancel': '取消',
    'common.submit': '提交',
    'common.previous': '上一步',
    'common.next': '下一步',
    'common.save': '保存',
    'common.clear': '清空',
    'common.edit': '编辑',
    'common.details': '详情',
    'common.logs': '日志',
    'common.loading': '加载中...',
    'common.none': '无',
    'common.name': '名称',
    'common.status': '状态',
    'common.progress': '进度',
    'common.actions': '操作',
    'common.time': '时间',
    'common.description': '描述',
    'common.error': '错误',
    'common.warning': '警告',
    'common.ok': '正常',
    'common.empty.tasks': '暂无任务',
    'common.empty.logs': '暂无日志',
    'common.empty.targets': '暂无目标',
    'common.empty.config': '暂无运行配置',
    'common.empty.summary': '暂无结果摘要',
    'common.empty.report': '尚未生成报告',
    'common.noSelection.report': '未选择报告',
    'status.pending': '等待中',
    'status.running': '运行中',
    'status.success': '成功',
    'status.failed': '失败',
    'status.cancelled': '已取消',
    'status.retrying': '重试中',
    'nav.dashboard': '仪表盘',
    'nav.tasks': '任务管理',
    'nav.pipelines': 'Pipeline',
    'nav.data': '数据浏览',
    'nav.reports': '报告',
    'nav.cron': '定时任务',
    'nav.agent': 'AI 助手',
    'nav.system': '系统检查',
    'dashboard.title': '系统概览',
    'dashboard.totalTasks': '任务总数',
    'dashboard.completed': '已完成',
    'dashboard.cronJobs': '定时任务',
    'dashboard.components': '已注册组件',
    'dashboard.chartTitle': '任务执行趋势',
    'dashboard.recentTasks': '最近任务',
    'dashboard.taskDistribution': '任务分布',
    'tasks.title': '任务管理',
    'tasks.create': '+ 创建任务',
    'tasks.allStatus': '全部状态',
    'tasks.autoRefreshHint': '运行中的任务会自动刷新',
    'tasks.autoRefresh': '自动刷新',
    'tasks.targetCount': '目标数',
    'tasks.duration': '耗时',
    'tasks.modal.create': '创建任务',
    'tasks.wizard.basic': '基础信息',
    'tasks.wizard.collect': '采集配置',
    'tasks.wizard.report': '报告选项与提交',
    'tasks.name': '任务名称',
    'tasks.targetName': 'Target Name',
    'tasks.descPlaceholder': '可选描述',
    'tasks.enableReport': '采集完成后自动生成报告',
    'tasks.reportPrompt': '自动报告提示词',
    'tasks.reportTemplate': '自动报告模板',
    'tasks.selectPipeline': '-- 选择 Pipeline --',
    'tasks.precheck': 'Precheck',
    'tasks.collector': 'Collector',
    'tasks.required': 'Required',
    'tasks.credentials': 'Credentials',
    'tasks.dataSource': 'Data source',
    'tasks.basic': '基础信息',
    'tasks.recentLogs': '最近日志',
    'tasks.targets': '目标',
    'tasks.runtimeConfig': '运行配置',
    'tasks.resultSummary': '结果摘要',
    'tasks.generatedReport': '打开生成的报告',
    'pipelines.title': 'Pipeline 配置',
    'pipelines.create': '+ 创建 Pipeline',
    'pipelines.components': '可用组件',
    'pipelines.configured': '已配置 Pipeline',
    'pipelines.empty.components': '暂无组件',
    'pipelines.empty.pipelines': '暂无 Pipeline',
    'pipelines.modal.create': '创建 Pipeline',
    'pipelines.template': '预设模板',
    'pipelines.custom': '-- 自定义 --',
    'pipelines.name': 'Pipeline 名称',
    'pipelines.collector': '采集器',
    'pipelines.stepsJson': 'Advanced Steps JSON',
    'pipelines.dangerZone': '危险区域',
    'pipelines.advancedSteps': '高级 JSON 配置',
    'data.title': '数据浏览',
    'data.searchPlaceholder': '按任务名称、任务 ID、游戏、分组或记录 key 搜索',
    'data.search': '搜索',
    'data.games': '游戏分类',
    'data.records': '记录',
    'data.chooseGame': '选择一个游戏',
    'data.groups': '分组',
    'data.summary': '按 App ID 或游戏名聚合已落库 JSON',
    'data.allSources': '全部数据源',
    'data.useForReport': '用于报告',
    'data.addToReport': '加入报告',
    'data.exportSelected': '导出选中',
    'data.deleteSelected': '删除选中',
    'data.newest': '最新优先',
    'data.oldest': '最早优先',
    'data.source': '数据源',
    'data.abstract': '摘要',
    'data.chooseCategory': '请选择左侧游戏分类',
    'data.previewTitle': '原始 JSON 预览',
    'data.previewEmpty': '选择一条记录后在这里预览',
    'data.empty': '暂无数据',
    'data.selectedCount': '已选 {count} 条',
    'reports.title': '报告生成',
    'reports.generate': '生成报告',
    'reports.params': '生成参数',
    'reports.prompt': '提示词',
    'reports.promptPlaceholder': '例如：总结最近落库的移动端游戏表现',
    'reports.sourceFilter': '数据源过滤',
    'reports.sourcePlaceholder': '如 steam / taptap',
    'reports.recordKeys': '指定原始 JSON',
    'reports.recordKeysPlaceholder': '从数据浏览页选择记录后自动填充；留空则按数据源过滤',
    'reports.recordKeysHelp': '每行一个 storage key，用于精确选择报告输入数据。',
    'reports.clearSelection': '清空选择',
    'reports.uploadJson': '上传 JSON 数据源',
    'reports.importDataList': '导入数据列表',
    'reports.uploadHelp': '上传后会写入本地数据列表，并自动加入本次报告输入。',
    'reports.addedSources': '已添加数据源',
    'reports.importByGroup': '按数据组导入',
    'reports.importGroup': '导入分组',
    'reports.template': '模板',
    'reports.history': '报告历史',
    'reports.content': '报告内容',
    'reports.empty': '暂无报告',
    'reports.noSources': '尚未添加 JSON 数据源',
    'reports.waiting': '等待中',
    'reports.records': '{count} 条记录',
    'reports.edit': '编辑',
    'reports.remove': '移除',
    'reports.manualInput': '手工输入',
    'reports.precheckFinished': '报告预检完成',
    'reports.available': 'Available',
    'reports.missing': 'Missing',
    'reports.fill': '补采 {collector}',
    'reports.excelGenerated': 'Excel 报告已生成',
    'reports.excelHelp': '该报告包含了清洗好的表格行、多个工作表以及统计图表。',
    'reports.downloadExcel': '下载 Excel 文件',
    'reports.awaitingData': '等待输入数据...',
    'reports.waitingInit': '等待分析初始化...',
    'reports.preparing': '准备中',
    'reports.waitingMsg': '等待中...',
    'reports.promptLabel': '核心提示词',
    'reports.sourceLabel': '数据源过滤',
    'reports.recordKeysLabel': '指定原始记录',
    'reports.uploadLabel': '上传数据集',
    'reports.lockedLabel': '已锁定数据源',
    'reports.importGroupLabel': '通过组导入',
    'reports.templateLabel': '分析模板',
    'reports.templateGeneral': '通用游戏模板',
    'reports.templateTaptap': 'TapTap游戏模板',
    'reports.templateSteam': 'Steam游戏模板',
    'cron.title': '定时任务',
    'cron.create': '+ 添加定时任务',
    'cron.empty': '暂无定时任务',
    'cron.modal.create': '添加定时任务',
    'cron.name': '任务名称',
    'cron.expression': 'Cron 表达式',
    'cron.expressionHelp': '格式：分 时 日 月 周',
    'cron.trigger': '触发器',
    'cron.next': '下次运行',
    'agent.title': 'AI 助手',
    'agent.config': '配置',
    'agent.clear': '清空对话',
    'agent.new': '+ 新建',
    'agent.send': '发送',
    'agent.inputPlaceholder': '输入指令，如：帮我采集 Elden Ring 的 Steam 数据',
    'agent.welcome': '你好！我是游戏数据助手，可以帮你：',
    'agent.help.status': '查看任务状态和系统概览',
    'agent.help.create': '创建数据采集任务',
    'agent.help.pipeline': '配置 Pipeline 和定时任务',
    'agent.help.data': '浏览和搜索已采集数据',
    'agent.help.report': '生成数据分析报告',
    'agent.help.ask': '请告诉我你需要什么帮助？',
    'agent.thinking': '思考中...',
    'agent.thinkingProcess': '思考过程',
    'agent.running': '执行中...',
    'agent.cleared': '对话已清空。请告诉我你需要什么帮助？',
    'agent.providerConfig': 'LLM 模型配置',
    'agent.defaultProvider': '默认 Provider',
    'agent.addProvider': '+ 添加供应商',
    'system.title': '系统检查',
    'system.overall': '总体状态',
    'system.errors': '错误',
    'system.warnings': '警告',
    'system.ok': '正常',
    'system.checks': '诊断项目',
    'system.paths': '运行路径',
    'system.empty.checks': '暂无诊断项目',
    'system.empty.paths': '暂无路径信息',
    'system.notLoaded.checks': '尚未加载诊断结果',
    'system.notLoaded.paths': '尚未加载路径信息',
    'message.loadFailed': '加载失败：{error}',
    'message.createFailed': '创建失败：{error}',
    'message.deleteFailed': '删除失败：{error}',
    'message.cancelFailed': '取消失败：{error}',
    'message.editFailed': '编辑失败：{error}',
    'message.generateFailed': '生成失败：{error}',
    'message.uploadFailed': '上传失败：{error}',
    'message.importFailed': '导入失败：{error}',
    'message.pipelineNameRequired': 'Pipeline 名称必填',
    'message.pipelineJsonInvalid': 'Pipeline steps JSON 无效',
    'message.pipelineStepsRequired': '请至少选择一个采集器和一个存储步骤',
    'message.pipelineCreated': 'Pipeline 已创建',
    'message.pipelineDeleted': 'Pipeline 已删除',
    'message.taskNamePipelineRequired': '任务名称和 Pipeline 必填',
    'message.pipelineLoadFailed': '加载 Pipeline 失败：{error}',
    'message.targetsJsonInvalid': 'Targets JSON 无效',
    'message.targetRequired': '至少需要一个目标',
    'message.taskPrecheckFailed': '任务预检失败',
    'message.taskCreated': '任务已创建',
    'message.taskCancelled': '任务已取消',
    'message.taskDeleted': '任务已删除',
    'message.selectPipeline': '请选择 Pipeline',
    'message.cronRequired': '所有定时任务字段都必填',
    'message.cronCreated': '定时任务已创建',
    'message.cronDeleted': '定时任务已删除',
    'message.promptRequired': '提示词必填',
    'message.reportGenerated': '报告已生成',
    'message.reportDeleted': '报告已删除',
    'message.reportUpdated': '报告已更新',
    'message.selectJsonFiles': '请选择 JSON 文件',
    'message.jsonImported': '已导入 {count} 个 JSON 数据源',
    'message.chooseDataGroup': '请选择数据组',
    'message.recordsImported': '已导入 {count} 条记录',
    'confirm.deleteTask': '删除任务 "{id}"？',
    'confirm.deletePipeline': '删除 Pipeline "{name}"？',
    'confirm.deleteCron': '删除定时任务 "{name}"？',
    'confirm.deleteReport': '删除报告 {id}？',
    'confirm.taskWarnings': '任务预检存在警告：\n{warnings}\n\n仍要提交吗？',
    'confirm.missingSources': '缺少数据源：{missing}。仍要生成报告吗？',
    'prompt.reportTitle': '报告标题',
    'prompt.notes': '备注',
  },
  'en-US': {
    'lang.zh': '中文',
    'lang.en': 'EN',
    'common.language': 'Language',
    'common.workspace': 'Workspace',
    'common.systemOnline': 'System Online',
    'common.search': 'Search',
    'common.import': 'Import',
    'common.update': 'Update',
    'common.schedule': 'Schedule',
    'common.refresh': 'Refresh',
    'common.create': 'Create',
    'common.delete': 'Delete',
    'common.cancel': 'Cancel',
    'common.submit': 'Submit',
    'common.previous': 'Previous',
    'common.next': 'Next',
    'common.save': 'Save',
    'common.clear': 'Clear',
    'common.edit': 'Edit',
    'common.details': 'Details',
    'common.logs': 'Logs',
    'common.loading': 'Loading...',
    'common.none': 'None',
    'common.name': 'Name',
    'common.status': 'Status',
    'common.progress': 'Progress',
    'common.actions': 'Actions',
    'common.time': 'Time',
    'common.description': 'Description',
    'common.error': 'Error',
    'common.warning': 'Warning',
    'common.ok': 'OK',
    'common.empty.tasks': 'No tasks',
    'common.empty.logs': 'No logs',
    'common.empty.targets': 'No targets',
    'common.empty.config': 'No runtime config',
    'common.empty.summary': 'No result summary',
    'common.empty.report': 'No report generated',
    'common.noSelection.report': 'No report selected',
    'status.pending': 'Pending',
    'status.running': 'Running',
    'status.success': 'Success',
    'status.failed': 'Failed',
    'status.cancelled': 'Cancelled',
    'status.retrying': 'Retrying',
    'nav.dashboard': 'Dashboard',
    'nav.tasks': 'Tasks',
    'nav.pipelines': 'Pipeline',
    'nav.data': 'Data',
    'nav.reports': 'Reports',
    'nav.cron': 'Cron Jobs',
    'nav.agent': 'AI Assistant',
    'nav.system': 'System',
    'dashboard.title': 'System Overview',
    'dashboard.totalTasks': 'Total Tasks',
    'dashboard.completed': 'Completed',
    'dashboard.cronJobs': 'Cron Jobs',
    'dashboard.components': 'Registered Components',
    'dashboard.chartTitle': 'Task Execution Trend',
    'dashboard.recentTasks': 'Recent Tasks',
    'dashboard.taskDistribution': 'Task Distribution',
    'tasks.title': 'Task Management',
    'tasks.create': '+ Create Task',
    'tasks.allStatus': 'All Statuses',
    'tasks.autoRefreshHint': 'Running tasks refresh automatically',
    'tasks.autoRefresh': 'Auto Refresh',
    'tasks.targetCount': 'Targets',
    'tasks.duration': 'Duration',
    'tasks.modal.create': 'Create Task',
    'tasks.wizard.basic': 'Basic Info',
    'tasks.wizard.collect': 'Collection Config',
    'tasks.wizard.report': 'Report Options & Submit',
    'tasks.name': 'Task Name',
    'tasks.targetName': 'Target Name',
    'tasks.descPlaceholder': 'Optional description',
    'tasks.enableReport': 'Generate a report after collection completes',
    'tasks.reportPrompt': 'Auto Report Prompt',
    'tasks.reportTemplate': 'Auto Report Template',
    'tasks.selectPipeline': '-- Select Pipeline --',
    'tasks.precheck': 'Precheck',
    'tasks.collector': 'Collector',
    'tasks.required': 'Required',
    'tasks.credentials': 'Credentials',
    'tasks.dataSource': 'Data source',
    'tasks.basic': 'Basic',
    'tasks.recentLogs': 'Recent Logs',
    'tasks.targets': 'Targets',
    'tasks.runtimeConfig': 'Runtime Config',
    'tasks.resultSummary': 'Result Summary',
    'tasks.generatedReport': 'Open Generated Report',
    'pipelines.title': 'Pipeline Config',
    'pipelines.create': '+ Create Pipeline',
    'pipelines.components': 'Available Components',
    'pipelines.configured': 'Configured Pipelines',
    'pipelines.empty.components': 'No components',
    'pipelines.empty.pipelines': 'No pipelines',
    'pipelines.modal.create': 'Create Pipeline',
    'pipelines.template': 'Template',
    'pipelines.custom': '-- Custom --',
    'pipelines.name': 'Pipeline Name',
    'pipelines.collector': 'Collector',
    'pipelines.stepsJson': 'Advanced Steps JSON',
    'pipelines.dangerZone': 'Danger Zone',
    'pipelines.advancedSteps': 'ADVANCED_STEPS',
    'data.title': 'Data Browser',
    'data.searchPlaceholder': 'Search by task name, task id, game, group, or record key',
    'data.search': 'Search',
    'data.games': 'Game Categories',
    'data.records': 'Records',
    'data.chooseGame': 'Choose a game',
    'data.groups': 'Groups',
    'data.summary': 'Aggregated stored JSON by App ID or game name',
    'data.allSources': 'All Sources',
    'data.useForReport': 'Use for Report',
    'data.addToReport': 'Add to Report',
    'data.exportSelected': 'Export Selected',
    'data.deleteSelected': 'Delete Selected',
    'data.newest': 'Newest First',
    'data.oldest': 'Oldest First',
    'data.source': 'Data Source',
    'data.abstract': 'Summary',
    'data.chooseCategory': 'Select a game category on the left',
    'data.previewTitle': 'Raw JSON Preview',
    'data.previewEmpty': 'Select a record to preview it here',
    'data.empty': 'No data',
    'data.selectedCount': '{count} selected',
    'reports.title': 'Report Generation',
    'reports.generate': 'Generate Report',
    'reports.params': 'Generation Parameters',
    'reports.prompt': 'Prompt',
    'reports.promptPlaceholder': 'Example: summarize recent mobile game performance from stored data',
    'reports.sourceFilter': 'Data Source Filter',
    'reports.sourcePlaceholder': 'e.g. steam / taptap',
    'reports.recordKeys': 'Specific Raw JSON',
    'reports.recordKeysPlaceholder': 'Auto-filled from Data Browser selections; empty means filter by data source',
    'reports.recordKeysHelp': 'One storage key per line for exact report inputs.',
    'reports.clearSelection': 'Clear Selection',
    'reports.uploadJson': 'Upload JSON Data Source',
    'reports.importDataList': 'Import Data List',
    'reports.uploadHelp': 'Uploaded files are stored locally and added to this report input.',
    'reports.addedSources': 'Added Data Sources',
    'reports.importByGroup': 'Import by Data Group',
    'reports.importGroup': 'Import Group',
    'reports.template': 'Template',
    'reports.history': 'Report History',
    'reports.content': 'Report Content',
    'reports.empty': 'No reports',
    'reports.noSources': 'No JSON data sources added',
    'reports.waiting': 'Waiting',
    'reports.records': '{count} records',
    'reports.edit': 'Edit',
    'reports.remove': 'Remove',
    'reports.manualInput': 'Manual Input',
    'reports.precheckFinished': 'Report precheck finished',
    'reports.available': 'Available',
    'reports.missing': 'Missing',
    'reports.fill': 'Collect {collector}',
    'reports.excelGenerated': 'Excel Report Generated',
    'reports.excelHelp': 'This report includes cleaned table rows, multiple sheets, and charts.',
    'reports.downloadExcel': 'Download Excel File',
    'reports.awaitingData': 'Awaiting input data...',
    'reports.waitingInit': 'Waiting for analysis initialization...',
    'reports.preparing': 'Preparing',
    'reports.waitingMsg': 'Waiting...',
    'reports.promptLabel': 'PROMPT',
    'reports.sourceLabel': 'SOURCE_FILTER',
    'reports.recordKeysLabel': 'RECORD_KEYS',
    'reports.uploadLabel': 'UPLOAD_JSON',
    'reports.lockedLabel': 'LOCKED_SOURCES',
    'reports.importGroupLabel': 'IMPORT_GROUP',
    'reports.templateLabel': 'TEMPLATE',
    'reports.templateGeneral': 'General Game Template',
    'reports.templateTaptap': 'TapTap Game Template',
    'reports.templateSteam': 'Steam Game Template',
    'cron.title': 'Cron Jobs',
    'cron.create': '+ Add Cron Job',
    'cron.empty': 'No cron jobs',
    'cron.modal.create': 'Add Cron Job',
    'cron.name': 'Job Name',
    'cron.expression': 'Cron Expression',
    'cron.expressionHelp': 'Format: minute hour day month weekday',
    'cron.trigger': 'Trigger',
    'cron.next': 'Next',
    'agent.title': 'AI Assistant',
    'agent.config': 'Config',
    'agent.clear': 'Clear Chat',
    'agent.new': '+ New',
    'agent.send': 'Send',
    'agent.inputPlaceholder': 'Type an instruction, e.g. collect Steam data for Elden Ring',
    'agent.welcome': 'Hello! I am the game data assistant. I can help you:',
    'agent.help.status': 'View task status and system overview',
    'agent.help.create': 'Create data collection tasks',
    'agent.help.pipeline': 'Configure pipelines and cron jobs',
    'agent.help.data': 'Browse and search collected data',
    'agent.help.report': 'Generate data analysis reports',
    'agent.help.ask': 'Tell me what you need.',
    'agent.thinking': 'Thinking...',
    'agent.thinkingProcess': 'Thinking Process',
    'agent.running': 'Running...',
    'agent.cleared': 'Chat cleared. Tell me what you need.',
    'agent.providerConfig': 'LLM Provider Config',
    'agent.defaultProvider': 'Default Provider',
    'agent.addProvider': '+ Add Provider',
    'system.title': 'System Check',
    'system.overall': 'Overall Status',
    'system.errors': 'Errors',
    'system.warnings': 'Warnings',
    'system.ok': 'OK',
    'system.checks': 'Diagnostics',
    'system.paths': 'Runtime Paths',
    'system.empty.checks': 'No diagnostics',
    'system.empty.paths': 'No path information',
    'system.notLoaded.checks': 'Diagnostics not loaded',
    'system.notLoaded.paths': 'Path information not loaded',
    'message.loadFailed': 'Load failed: {error}',
    'message.createFailed': 'Create failed: {error}',
    'message.deleteFailed': 'Delete failed: {error}',
    'message.cancelFailed': 'Cancel failed: {error}',
    'message.editFailed': 'Edit failed: {error}',
    'message.generateFailed': 'Generate failed: {error}',
    'message.uploadFailed': 'Upload failed: {error}',
    'message.importFailed': 'Import failed: {error}',
    'message.pipelineNameRequired': 'Pipeline name is required',
    'message.pipelineJsonInvalid': 'Pipeline steps JSON is invalid',
    'message.pipelineStepsRequired': 'Choose at least one collector and one storage step',
    'message.pipelineCreated': 'Pipeline created',
    'message.pipelineDeleted': 'Pipeline deleted',
    'message.taskNamePipelineRequired': 'Task name and pipeline are required',
    'message.pipelineLoadFailed': 'Load pipelines failed: {error}',
    'message.targetsJsonInvalid': 'Targets JSON is invalid',
    'message.targetRequired': 'At least one target is required',
    'message.taskPrecheckFailed': 'Task precheck failed',
    'message.taskCreated': 'Task created',
    'message.taskCancelled': 'Task cancelled',
    'message.taskDeleted': 'Task deleted',
    'message.selectPipeline': 'Select a pipeline',
    'message.cronRequired': 'All cron fields are required',
    'message.cronCreated': 'Cron job created',
    'message.cronDeleted': 'Cron job deleted',
    'message.promptRequired': 'Prompt is required',
    'message.reportGenerated': 'Report generated',
    'message.reportDeleted': 'Report deleted',
    'message.reportUpdated': 'Report updated',
    'message.selectJsonFiles': 'Select JSON files',
    'message.jsonImported': 'Imported {count} JSON data sources',
    'message.chooseDataGroup': 'Choose a data group',
    'message.recordsImported': 'Imported {count} records',
    'confirm.deleteTask': 'Delete task "{id}"?',
    'confirm.deletePipeline': 'Delete pipeline "{name}"?',
    'confirm.deleteCron': 'Delete cron job "{name}"?',
    'confirm.deleteReport': 'Delete report {id}?',
    'confirm.taskWarnings': 'Task precheck has warnings:\n{warnings}\n\nSubmit anyway?',
    'confirm.missingSources': 'Missing data sources: {missing}. Generate report anyway?',
    'prompt.reportTitle': 'Report title',
    'prompt.notes': 'Notes',
  },
};

const textToKey = new Map();
const attrToKey = new Map();

const selectorTextKeys = [
  ['.nav-link[data-tab="dashboard"] span', 'nav.dashboard'],
  ['.nav-link[data-tab="tasks"] span', 'nav.tasks'],
  ['.nav-link[data-tab="pipelines"] span', 'nav.pipelines'],
  ['.nav-link[data-tab="data"] span', 'nav.data'],
  ['.nav-link[data-tab="reports"] span', 'nav.reports'],
  ['.nav-link[data-tab="cron"] span', 'nav.cron'],
  ['.nav-link[data-tab="agent"] span', 'nav.agent'],
  ['.nav-link[data-tab="system"] span', 'nav.system'],
  ['#tab-dashboard .section-header h1', 'dashboard.title'],
  ['#btn-refresh-dashboard', 'common.refresh'],
  ['#stat-total + .stat-label', 'dashboard.totalTasks'],
  ['#stat-running + .stat-label', 'status.running'],
  ['#stat-success + .stat-label', 'dashboard.completed'],
  ['#stat-failed + .stat-label', 'status.failed'],
  ['#stat-cron + .stat-label', 'dashboard.cronJobs'],
  ['#stat-components + .stat-label', 'dashboard.components'],
  ['#tab-dashboard .card:nth-of-type(2) .card-header h2', 'dashboard.recentTasks'],
  ['#tab-tasks .section-header h1', 'tasks.title'],
  ['#btn-create-task', 'tasks.create'],
  ['#tasks-refresh-hint', 'tasks.autoRefreshHint'],
  ['#tab-pipelines .section-header h1', 'pipelines.title'],
  ['#btn-create-pipeline', 'pipelines.create'],
  ['#tab-pipelines .card:nth-of-type(1) .card-header h2', 'pipelines.components'],
  ['#tab-pipelines .card:nth-of-type(2) .card-header h2', 'pipelines.configured'],
  ['#tab-data .section-header h1', 'data.title'],
  ['#btn-refresh-data', 'common.refresh'],
  ['#tab-data .data-game-panel .card-header h2', 'data.games'],
  ['#data-records-title', 'data.chooseGame'],
  ['#data-selected-summary', 'data.summary'],
  ['#data-batch-count', 'data.selectedCount'],
  ['#tab-reports .section-header h1', 'reports.title'],
  ['#btn-generate-report', 'reports.generate'],
  ['#tab-reports .reports-layout .card:nth-of-type(1) .card-header h2', 'reports.params'],
  ['#tab-cron .section-header h1', 'cron.title'],
  ['#btn-create-cron', 'cron.create'],
  ['#cron-list .text-muted', 'cron.empty'],
  ['#tab-agent .section-header h1', 'agent.title'],
  ['#btn-clear-agent', 'agent.clear'],
  ['#btn-send-agent', 'agent.send'],
  ['#tab-system .section-header h1', 'system.title'],
  ['#btn-refresh-system', 'common.refresh'],
  ['#system-overall-status + .stat-label', 'system.overall'],
  ['#system-error-count + .stat-label', 'system.errors'],
  ['#system-warning-count + .stat-label', 'system.warnings'],
  ['#system-ok-count + .stat-label', 'system.ok'],
  ['#modal-create-task .modal-header h2', 'tasks.modal.create'],
  ['#modal-create-pipeline .modal-header h2', 'pipelines.modal.create'],
  ['#modal-create-cron .modal-header h2', 'cron.modal.create'],
  ['#modal-task-logs .modal-header h2', 'common.logs'],
  ['#modal-task-detail .modal-header h2', 'common.details'],
  ['#modal-provider-config .modal-header h2', 'agent.providerConfig'],
];

const selectorAttrKeys = [
  ['#data-search-query', 'placeholder', 'data.searchPlaceholder'],
  ['#report-prompt', 'placeholder', 'reports.promptPlaceholder'],
  ['#report-data-source', 'placeholder', 'reports.sourcePlaceholder'],
  ['#report-record-keys', 'placeholder', 'reports.recordKeysPlaceholder'],
  ['#agent-input', 'placeholder', 'agent.inputPlaceholder'],
  ['#agent-provider-select', 'title', 'agent.providerConfig'],
  ['#btn-create-task', 'title', 'tasks.create'],
];

const staticTextKeys = [
  ['Search', 'common.search'],
  ['Import', 'common.import'],
  ['Update', 'common.update'],
  ['Schedule', 'common.schedule'],
  ['Groups', 'data.groups'],
  ['Danger Zone', 'pipelines.dangerZone'],
  ['Auto Refresh', 'tasks.autoRefresh'],
  ['System Online', 'common.systemOnline'],
  ['Awaiting input data...', 'reports.awaitingData'],
  ['Waiting for analysis initialization...', 'reports.waitingInit'],
  ['Preparing', 'reports.preparing'],
  ['Waiting...', 'reports.waitingMsg'],
  ['核心提示词 / PROMPT', 'reports.promptLabel'],
  ['数据源过滤 / SOURCE_FILTER', 'reports.sourceLabel'],
  ['指定原始记录 / RECORD_KEYS', 'reports.recordKeysLabel'],
  ['上传数据集 / UPLOAD_JSON', 'reports.uploadLabel'],
  ['已锁定数据源 / LOCKED_SOURCES', 'reports.lockedLabel'],
  ['通过组导入 / IMPORT_GROUP', 'reports.importGroupLabel'],
  ['分析模板 / TEMPLATE', 'reports.templateLabel'],
  ['通用游戏模板', 'reports.templateGeneral'],
  ['TapTap游戏模板', 'reports.templateTaptap'],
  ['Steam游戏模板', 'reports.templateSteam'],
  ['高级 JSON 配置 / ADVANCED_STEPS', 'pipelines.advancedSteps'],
  ['仪表盘', 'nav.dashboard'],
  ['任务管理', 'nav.tasks'],
  ['Pipeline', 'nav.pipelines'],
  ['数据浏览', 'nav.data'],
  ['报告', 'nav.reports'],
  ['定时任务', 'nav.cron'],
  ['AI 助手', 'nav.agent'],
  ['系统检查', 'nav.system'],
  ['工作区', 'common.workspace'],
  ['系统概览', 'dashboard.title'],
  ['刷新', 'common.refresh'],
  ['任务总数', 'dashboard.totalTasks'],
  ['运行中', 'status.running'],
  ['已完成', 'dashboard.completed'],
  ['失败', 'status.failed'],
  ['已注册组件', 'dashboard.components'],
  ['任务执行趋势', 'dashboard.chartTitle'],
  ['最近任务', 'dashboard.recentTasks'],
  ['名称', 'common.name'],
  ['状态', 'common.status'],
  ['进度', 'common.progress'],
  ['创建时间', 'common.time'],
  ['操作', 'common.actions'],
  ['暂无任务', 'common.empty.tasks'],
  ['+ 创建任务', 'tasks.create'],
  ['全部状态', 'tasks.allStatus'],
  ['等待中', 'status.pending'],
  ['已取消', 'status.cancelled'],
  ['运行中的任务会自动刷新', 'tasks.autoRefreshHint'],
  ['目标数', 'tasks.targetCount'],
  ['耗时', 'tasks.duration'],
  ['Pipeline 配置', 'pipelines.title'],
  ['+ 创建 Pipeline', 'pipelines.create'],
  ['可用组件', 'pipelines.components'],
  ['已配置 Pipeline', 'pipelines.configured'],
  ['暂无 Pipeline', 'pipelines.empty.pipelines'],
  ['加载中...', 'common.loading'],
  ['游戏分类', 'data.games'],
  ['暂无数据', 'data.empty'],
  ['选择一个游戏', 'data.chooseGame'],
  ['按 App ID 或游戏名聚合已落库 JSON', 'data.summary'],
  ['全部数据源', 'data.allSources'],
  ['用于报告', 'data.useForReport'],
  ['已选 0 条', 'data.selectedCount'],
  ['加入报告', 'data.addToReport'],
  ['导出选中', 'data.exportSelected'],
  ['删除选中', 'data.deleteSelected'],
  ['最新优先', 'data.newest'],
  ['最早优先', 'data.oldest'],
  ['记录', 'data.records'],
  ['数据源', 'data.source'],
  ['摘要', 'data.abstract'],
  ['时间', 'common.time'],
  ['请选择左侧游戏分类', 'data.chooseCategory'],
  ['原始 JSON 预览', 'data.previewTitle'],
  ['选择一条记录后在这里预览', 'data.previewEmpty'],
  ['报告生成', 'reports.title'],
  ['生成报告', 'reports.generate'],
  ['生成参数', 'reports.params'],
  ['提示词', 'reports.prompt'],
  ['数据源过滤', 'reports.sourceFilter'],
  ['指定原始 JSON', 'reports.recordKeys'],
  ['清空选择', 'reports.clearSelection'],
  ['上传 JSON 数据源', 'reports.uploadJson'],
  ['导入数据列表', 'reports.importDataList'],
  ['已添加数据源', 'reports.addedSources'],
  ['Import by data group', 'reports.importByGroup'],
  ['Import group', 'reports.importGroup'],
  ['模板', 'reports.template'],
  ['报告历史', 'reports.history'],
  ['暂无报告', 'reports.empty'],
  ['报告内容', 'reports.content'],
  ['尚未生成报告', 'common.empty.report'],
  ['+ 添加定时任务', 'cron.create'],
  ['暂无定时任务', 'cron.empty'],
  ['配置', 'agent.config'],
  ['清空对话', 'agent.clear'],
  ['+ 新建', 'agent.new'],
  ['发送', 'agent.send'],
  ['你好！我是游戏数据助手，可以帮你：', 'agent.welcome'],
  ['查看任务状态和系统概览', 'agent.help.status'],
  ['创建数据采集任务', 'agent.help.create'],
  ['配置 Pipeline 和定时任务', 'agent.help.pipeline'],
  ['浏览和搜索已采集数据', 'agent.help.data'],
  ['生成数据分析报告', 'agent.help.report'],
  ['请告诉我你需要什么帮助？', 'agent.help.ask'],
  ['总体状态', 'system.overall'],
  ['错误', 'system.errors'],
  ['警告', 'system.warnings'],
  ['正常', 'system.ok'],
  ['诊断项目', 'system.checks'],
  ['尚未加载诊断结果', 'system.notLoaded.checks'],
  ['运行路径', 'system.paths'],
  ['尚未加载路径信息', 'system.notLoaded.paths'],
  ['创建任务', 'tasks.modal.create'],
  ['基础信息', 'tasks.wizard.basic'],
  ['采集配置', 'tasks.wizard.collect'],
  ['报告选项与提交', 'tasks.wizard.report'],
  ['任务名称', 'tasks.name'],
  ['描述', 'common.description'],
  ['取消', 'common.cancel'],
  ['上一步', 'common.previous'],
  ['下一步', 'common.next'],
  ['提交', 'common.submit'],
  ['创建 Pipeline', 'pipelines.modal.create'],
  ['预设模板', 'pipelines.template'],
  ['Pipeline 名称', 'pipelines.name'],
  ['采集器', 'pipelines.collector'],
  ['添加定时任务', 'cron.modal.create'],
  ['Cron 表达式', 'cron.expression'],
  ['任务日志', 'common.logs'],
  ['任务详情', 'common.details'],
  ['LLM 模型配置', 'agent.providerConfig'],
  ['默认 Provider', 'agent.defaultProvider'],
  ['+ 添加供应商', 'agent.addProvider'],
  ['保存', 'common.save'],
];

const attrKeys = [
  ['Search by task name, task id, game, group, or record key', 'data.searchPlaceholder'],
  ['例如：总结最近落库的移动端游戏表现', 'reports.promptPlaceholder'],
  ['如 steam / taptap', 'reports.sourcePlaceholder'],
  ['从数据浏览页选择记录后自动填充；留空则按数据源过滤', 'reports.recordKeysPlaceholder'],
  ['输入指令，如：帮我采集 Elden Ring 的 Steam 数据', 'agent.inputPlaceholder'],
  ['选择 LLM 模型', 'agent.providerConfig'],
  ['配置模型供应商', 'agent.providerConfig'],
];

function registerStaticMaps() {
  for (const [key, value] of Object.entries(messages['zh-CN'])) {
    if (value && !value.includes('{') && !value.includes('\n')) textToKey.set(value, key);
  }
  for (const [key, value] of Object.entries(messages['en-US'])) {
    if (value && !value.includes('{') && !value.includes('\n')) textToKey.set(value, key);
  }
  for (const [text, key] of staticTextKeys) textToKey.set(text, key);
  for (const [text, key] of attrKeys) attrToKey.set(text, key);
}
registerStaticMaps();

function normalizeLanguage(lang) {
  return SUPPORTED_LANGUAGES.has(lang) ? lang : DEFAULT_LANGUAGE;
}

function interpolate(template, params) {
  return String(template).replace(/\{(\w+)\}/g, (_, key) => params?.[key] ?? '');
}

export function t(key, params = {}) {
  const value = messages[activeLanguage]?.[key] ?? messages[DEFAULT_LANGUAGE]?.[key] ?? key;
  return interpolate(value, params);
}

export function getLanguage() {
  return activeLanguage;
}

export function setLanguage(lang, options = {}) {
  const next = normalizeLanguage(lang);
  if (next === activeLanguage && !options.force) return activeLanguage;
  activeLanguage = next;
  localStorage.setItem(STORAGE_KEY, activeLanguage);
  document.documentElement.lang = activeLanguage;
  refreshLanguageControls();
  applyTranslations(document);
  if (activeStore && activeStore.get('language') !== activeLanguage) {
    activeStore.set('language', activeLanguage);
  }
  window.dispatchEvent(new CustomEvent('languagechange', { detail: { language: activeLanguage } }));
  return activeLanguage;
}

export function configureI18n(store) {
  activeStore = store || null;
  document.documentElement.lang = activeLanguage;
  if (activeStore && activeStore.get('language') !== activeLanguage) {
    activeStore.set('language', activeLanguage);
  }
  window.t = t;
  window.getLanguage = getLanguage;
  window.setLanguage = setLanguage;
  bindLanguageControls();
  applyTranslations(document);
  refreshLanguageControls();
}

export function bindLanguageControls(root = document) {
  root.querySelectorAll('[data-lang]').forEach((button) => {
    if (button.dataset.i18nBound === 'true') return;
    button.dataset.i18nBound = 'true';
    button.addEventListener('click', () => setLanguage(button.dataset.lang));
  });
}

function refreshLanguageControls(root = document) {
  root.querySelectorAll('[data-lang]').forEach((button) => {
    const selected = button.dataset.lang === activeLanguage;
    button.classList.toggle('active', selected);
    button.setAttribute('aria-pressed', selected ? 'true' : 'false');
  });
}

function translateMarkedElements(root) {
  root.querySelectorAll('[data-i18n]').forEach((el) => {
    el.textContent = t(el.dataset.i18n);
  });
  root.querySelectorAll('[data-i18n-html]').forEach((el) => {
    el.innerHTML = t(el.dataset.i18nHtml);
  });
  root.querySelectorAll('[data-i18n-placeholder]').forEach((el) => {
    el.setAttribute('placeholder', t(el.dataset.i18nPlaceholder));
  });
  root.querySelectorAll('[data-i18n-title]').forEach((el) => {
    el.setAttribute('title', t(el.dataset.i18nTitle));
  });
  root.querySelectorAll('[data-i18n-value]').forEach((el) => {
    el.setAttribute('value', t(el.dataset.i18nValue));
  });
}

function translateSelectorTargets(root) {
  for (const [selector, key] of selectorTextKeys) {
    root.querySelectorAll(selector).forEach((el) => {
      if (el.matches('input, textarea, select')) return;
      if (el.children.length && !el.matches('button, h1, h2, label, .stat-label, .text-muted')) return;
      if (key === 'data.selectedCount') {
        const match = el.textContent.match(/\d+/);
        el.textContent = t(key, { count: match ? match[0] : 0 });
      } else {
        el.textContent = t(key);
      }
    });
  }
  for (const [selector, attr, key] of selectorAttrKeys) {
    root.querySelectorAll(selector).forEach((el) => el.setAttribute(attr, t(key)));
  }
  root.querySelectorAll('button,h1,h2,h3,th,td,option,span,small,p,div.text-muted,label').forEach((el) => {
    if (el.closest('script,style,textarea,input,code,pre,.CodeMirror,.report-output')) return;
    if (el.children.length && !el.matches('button')) return;
    const text = el.textContent.trim();
    const key = text && textToKey.get(text);
    if (key) el.textContent = t(key);
  });
}

function shouldSkipTextNode(node) {
  const parent = node.parentElement;
  if (!parent) return true;
  return Boolean(parent.closest('script,style,textarea,input,code,pre,.CodeMirror,.report-output'));
}

function translateTextNodes(root) {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
    acceptNode(node) {
      if (shouldSkipTextNode(node)) return NodeFilter.FILTER_REJECT;
      const value = node.nodeValue.trim();
      return value && textToKey.has(value) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
    },
  });
  const nodes = [];
  while (walker.nextNode()) nodes.push(walker.currentNode);
  for (const node of nodes) {
    const original = node.nodeValue;
    const trimmed = original.trim();
    const key = textToKey.get(trimmed);
    if (!key) continue;
    node.nodeValue = original.replace(trimmed, t(key));
  }
}

function translateAttributes(root) {
  const attrs = ['placeholder', 'title', 'aria-label'];
  root.querySelectorAll(attrs.map((name) => `[${name}]`).join(',')).forEach((el) => {
    for (const attr of attrs) {
      const value = el.getAttribute(attr);
      const key = value && attrToKey.get(value);
      if (key) el.setAttribute(attr, t(key));
    }
  });
}

export function applyTranslations(root = document) {
  translateMarkedElements(root);
  translateSelectorTargets(root);
  translateTextNodes(root);
  translateAttributes(root);
  refreshLanguageControls(root);
}

export function languageLabel(key) {
  return t(key);
}
