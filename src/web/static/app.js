const AUTO_REFRESH_INTERVAL_MS = 30000; // 降低轮询频率，主要依赖 WebSocket

let activeTab = "dashboard";
let autoRefreshHandle = null;
let pipelineTemplates = [];
let availableComponents = {};
let availablePipelines = {};
let dataGames = [];
let dataGroups = [];
let selectedDataGame = null;
let currentDataRecords = [];
let selectedReportRecordKeys = [];
let selectedReportRecordMeta = {};
let reportTemplates = [];
let currentReportProgressId = "";

async function api(path, options = {}) {
    const resp = await fetch(`/api${path}`, {
        headers: { "Content-Type": "application/json", ...options.headers },
        ...options,
    });

    if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(err.detail || `HTTP ${resp.status}`);
    }

    return resp.json();
}

function toast(message, type = "info") {
    const container = document.getElementById("toast-container");
    if (!container) return;

    const el = document.createElement("div");
    el.className = `toast toast-${type}`;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(() => el.remove(), 3000);
}

let wsConnection = null;

function initWebSocket() {
    if (wsConnection) return;
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/api/ws/tasks`;
    
    wsConnection = new WebSocket(wsUrl);
    
    wsConnection.onopen = () => {
        console.log("WebSocket connected");
        toast("实时推送已连接", "success");
    };
    
    wsConnection.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.type === "task_update" && data.task) {
                handleTaskUpdate(data.task);
            } else if (data.type === "stats_update" && data.stats) {
                handleStatsUpdate(data.stats);
            } else if (data.type === "report_progress") {
                handleReportProgress(data);
            }
        } catch (e) {
            console.error("WS message parse error:", e);
        }
    };
    
    wsConnection.onclose = () => {
        console.log("WebSocket disconnected, retrying in 5s...");
        wsConnection = null;
        setTimeout(initWebSocket, 5000);
    };
    
    wsConnection.onerror = (err) => {
        console.error("WebSocket error:", err);
    };
}

function handleTaskUpdate(task) {
    // 1. 如果在大盘页，且任务是近期任务，刷新 Dashboard（简单粗暴点可以直接调 refreshDashboard）
    if (activeTab === "dashboard") {
        refreshDashboard();
    }
    
    // 2. 如果在任务列表页，更新对应行
    if (activeTab === "tasks") {
        loadTasks(); // 可以优化为 DOM 局部更新，这里先直接拉取保持简单稳定
    }
    
    // 3. 如果当前正打开此任务的详情页或日志页，刷新它们
    const modalDetail = document.getElementById("modal-task-detail");
    if (modalDetail && modalDetail.classList.contains("show")) {
        // 判断当前查看的是否是这个 task（可以通过读取当前 DOM 里的 ID 判断，这里简化为重新加载当前 ID）
        const currentIdEl = document.querySelector("#task-detail-content .detail-kv code");
        if (currentIdEl && currentIdEl.textContent === task.id) {
            viewTaskDetail(task.id);
        }
    }
    
    const modalLogs = document.getElementById("modal-task-logs");
    if (modalLogs && modalLogs.classList.contains("show")) {
        // 如果日志弹窗打开，且是当前任务，重新加载日志
        // (为了精准可以把 currentTaskId 存成全局变量，这里为了简便直接调用 API)
        // 简单实现：由于日志弹窗没有保存当前任务ID，如果想实时追加需要一点结构改动。
        // 这里我们在 viewTaskLogs 时把 ID 存在弹窗上
        if (modalLogs.dataset.taskId === task.id) {
            viewTaskLogs(task.id);
        }
    }
}

function handleStatsUpdate(stats) {
    if (activeTab === "dashboard") {
        refreshDashboard();
    }
}

function handleReportProgress(event) {
    if (!event || event.progress_id !== currentReportProgressId) return;
    setReportProgress(event.progress || 0, event.stage || "running", event.message || "");
}

function setReportProgress(progress, stage, message) {
    const wrapper = document.getElementById("report-progress");
    const fill = document.getElementById("report-progress-fill");
    const percent = document.getElementById("report-progress-percent");
    const stageEl = document.getElementById("report-progress-stage");
    const messageEl = document.getElementById("report-progress-message");
    const value = Math.max(0, Math.min(1, Number(progress) || 0));
    if (wrapper) wrapper.style.display = "block";
    if (fill) fill.style.width = `${Math.round(value * 100)}%`;
    if (percent) percent.textContent = `${Math.round(value * 100)}%`;
    if (stageEl) stageEl.textContent = stage;
    if (messageEl) messageEl.textContent = message || stage;
}

function resetReportProgress() {
    setReportProgress(0, "queued", "Report generation queued");
}

function openModal(id) {
    const modal = document.getElementById(id);
    if (modal) {
        modal.classList.add("show");
        // refresh CodeMirror to prevent UI bugs inside hidden elements
        setTimeout(() => {
            if (id === "modal-create-task" && taskTargetsEditor) {
                taskTargetsEditor.refresh();
            }
            if (id === "modal-create-pipeline" && pipelineStepsEditor) {
                pipelineStepsEditor.refresh();
            }
        }, 10);
    }
}

function closeModal(id) {
    const modal = document.getElementById(id);
    if (modal) {
        modal.classList.remove("show");
    }
}

function loadTabData(tab) {
    switch (tab) {
        case "dashboard":
            refreshDashboard();
            break;
        case "tasks":
            loadTasks();
            break;
        case "pipelines":
            loadComponents();
            loadPipelines();
            break;
        case "data":
            loadDataGames();
            break;
        case "reports":
            loadReportTemplates();
            loadDataGroups();
            loadReports();
            break;
        case "cron":
            loadCronJobs();
            break;
        default:
            break;
    }
}

let dashboardChart = null;
let taskTargetsEditor = null;
let pipelineStepsEditor = null;

function initEditors() {
    if (typeof CodeMirror === "undefined") return;

    const targetsEl = document.getElementById("task-targets");
    if (targetsEl && !taskTargetsEditor) {
        taskTargetsEditor = CodeMirror.fromTextArea(targetsEl, {
            mode: "javascript",
            theme: "dracula",
            lineNumbers: true,
            viewportMargin: Infinity
        });
        taskTargetsEditor.setSize(null, 150);
    }

    const stepsEl = document.getElementById("pipeline-steps");
    if (stepsEl && !pipelineStepsEditor) {
        pipelineStepsEditor = CodeMirror.fromTextArea(stepsEl, {
            mode: "javascript",
            theme: "dracula",
            lineNumbers: true,
            viewportMargin: Infinity
        });
        pipelineStepsEditor.setSize(null, 200);
    }
}

function renderDashboardChart(stats) {
    const chartDom = document.getElementById("dashboard-chart");
    if (!chartDom) return;
    if (!dashboardChart) {
        dashboardChart = echarts.init(chartDom);
        window.addEventListener('resize', () => dashboardChart.resize());
    }

    const counts = stats.status_counts || {};
    const option = {
        tooltip: { trigger: 'item' },
        legend: { top: 'bottom' },
        series: [
            {
                name: '任务分布',
                type: 'pie',
                radius: ['40%', '70%'],
                avoidLabelOverlap: false,
                itemStyle: {
                    borderRadius: 10,
                    borderColor: '#fff',
                    borderWidth: 2
                },
                label: { show: false, position: 'center' },
                emphasis: {
                    label: { show: true, fontSize: 20, fontWeight: 'bold' }
                },
                labelLine: { show: false },
                data: [
                    { value: counts.success || 0, name: '已完成', itemStyle: { color: '#10b981' } },
                    { value: counts.running || 0, name: '运行中', itemStyle: { color: '#3b82f6' } },
                    { value: counts.failed || 0, name: '失败', itemStyle: { color: '#ef4444' } },
                    { value: counts.pending || 0, name: '等待中', itemStyle: { color: '#f59e0b' } }
                ].filter(item => item.value > 0)
            }
        ]
    };
    dashboardChart.setOption(option);
}

async function refreshDashboard() {
    try {
        const [stats, components, tasks] = await Promise.all([
            api("/tasks/stats/summary"),
            api("/components"),
            api("/tasks"),
        ]);

        const counts = stats.status_counts || {};
        setText("stat-total", stats.total_tasks || 0);
        setText("stat-running", counts.running || 0);
        setText("stat-success", counts.success || 0);
        setText("stat-failed", counts.failed || 0);
        setText("stat-cron", stats.cron_jobs || 0);

        const componentCount = Object.values(components).reduce((sum, names) => sum + names.length, 0);
        setText("stat-components", componentCount);

        renderDashboardChart(stats);

        const recentTasks = [...tasks]
            .sort((left, right) => new Date(right.created_at) - new Date(left.created_at))
            .slice(0, 5);
        renderRecentTasks(recentTasks);
    } catch (err) {
        console.error("Dashboard refresh failed:", err);
    }
}

function renderRecentTasks(tasks) {
    const tbody = document.getElementById("recent-tasks-body");
    if (!tbody) return;

    if (tasks.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-muted">No tasks</td></tr>';
        return;
    }

    tbody.innerHTML = tasks.map((task) => `
        <tr>
            <td><code>${task.id}</code></td>
            <td>${escapeHtml(task.name)}</td>
            <td>${renderBadge(task.status)}</td>
            <td>${renderProgress(task.progress)}</td>
            <td>${formatTime(task.created_at)}</td>
            <td>${renderTaskActions(task)}</td>
        </tr>
    `).join("");
}

async function loadTasks() {
    try {
        const filter = document.getElementById("task-status-filter")?.value || "";
        const query = filter ? `?status=${encodeURIComponent(filter)}` : "";
        const tasks = await api(`/tasks${query}`);
        const orderedTasks = [...tasks]
            .sort((left, right) => new Date(right.created_at) - new Date(left.created_at));
        renderTasksTable(orderedTasks);
    } catch (err) {
        toast(`Load failed: ${err.message}`, "error");
    }
}

function renderTasksTable(tasks) {
    const tbody = document.getElementById("tasks-body");
    if (!tbody) return;

    if (tasks.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="text-muted">No tasks</td></tr>';
        return;
    }

    tbody.innerHTML = tasks.map((task) => `
        <tr>
            <td><code>${task.id}</code></td>
            <td>${escapeHtml(task.name)}</td>
            <td>${escapeHtml(task.pipeline_name || "-")}</td>
            <td>${renderBadge(task.status)}</td>
            <td>${renderProgress(task.progress)}</td>
            <td>${task.targets_count}</td>
            <td>${task.duration ? `${task.duration.toFixed(1)}s` : "-"}</td>
            <td>${renderTaskActions(task)}</td>
        </tr>
    `).join("");
}

function renderTaskActions(task) {
    const actions = [
        `<button class="btn btn-ghost btn-sm" onclick="viewTaskDetail('${task.id}')">Details</button>`,
        `<button class="btn btn-ghost btn-sm" onclick="viewTaskLogs('${task.id}')">Logs</button>`,
    ];

    if (task.status === "running" || task.status === "pending") {
        actions.push(`<button class="btn btn-danger btn-sm" onclick="cancelTask('${task.id}')">Cancel</button>`);
    } else {
        actions.push(`<button class="btn btn-danger btn-sm" onclick="deleteTask('${task.id}')">Delete</button>`);
    }

    return `<div class="action-buttons">${actions.join(" ")}</div>`;
}

function showCreateTaskModal() {
    Promise.all([loadPipelineTemplates(), loadPipelineSelect("task-pipeline")]).then(() => updateTaskTargetFields());
    openModal("modal-create-task");
}

function getCollectorForPipeline(pipelineName) {
    const pipeline = availablePipelines[pipelineName]
        || pipelineTemplates.find((template) => template.id === pipelineName);
    const collectorStep = pipeline?.steps?.find((step) => step.type === "collector");
    return collectorStep?.name || collectorStep?.component_name || "";
}

function hasStorageStep(pipelineName, storageName) {
    const pipeline = availablePipelines[pipelineName]
        || pipelineTemplates.find((template) => template.id === pipelineName);
    return Boolean(pipeline?.steps?.some((step) => step.type === "storage" && (step.name || step.component_name) === storageName));
}

function updateTaskTargetFields() {
    const pipelineName = document.getElementById("task-pipeline")?.value || "";
    const collector = getCollectorForPipeline(pipelineName);

    const steamFields = document.getElementById("task-steam-fields");
    const steamDiscussionsFields = document.getElementById("task-steam-discussions-fields");
    const taptapFields = document.getElementById("task-taptap-fields");
    const monitorFields = document.getElementById("task-monitor-fields");
    const qimaiFields = document.getElementById("task-qimai-fields");
    const officialSiteFields = document.getElementById("task-official-site-fields");
    const helper = document.getElementById("task-target-helper");

    if (steamFields) {
        steamFields.style.display = collector === "steam" ? "block" : "none";
    }
    if (steamDiscussionsFields) {
        steamDiscussionsFields.style.display = collector === "steam_discussions" ? "block" : "none";
    }
    if (taptapFields) {
        taptapFields.style.display = collector === "taptap" ? "block" : "none";
    }
    if (monitorFields) {
        monitorFields.style.display = collector === "monitor" ? "block" : "none";
    }
    if (qimaiFields) {
        qimaiFields.style.display = collector === "qimai" ? "block" : "none";
    }
    if (officialSiteFields) {
        officialSiteFields.style.display = collector === "official_site" ? "block" : "none";
    }
    if (helper) {
        if (collector === "taptap") {
            helper.textContent = "TapTap v1 expects a public mainland page URL or app ID.";
        } else if (collector === "steam_discussions") {
            helper.textContent = "Steam Community tasks use app id or forum URL plus optional start/end dates.";
        } else if (collector === "monitor") {
            helper.textContent = "Monitor tasks use app id and optional Twitch/SullyGnome hints.";
        } else if (collector === "qimai") {
            helper.textContent = "Qimai tasks use qimai_app_id (App Store ID or Package Name).";
        } else if (collector === "official_site") {
            helper.textContent = "Official site tasks use target name plus official_url, or advanced JSON targets.";
        } else {
            helper.textContent = "Steam tasks use target name + app id, or advanced JSON targets.";
        }
    }

    const autoReport = document.getElementById("task-enable-report");
    if (autoReport && (
        pipelineName === "steam_full_report"
        || pipelineName === "taptap_full_report"
        || pipelineName === "steam_discussions_full_report"
    )) {
        autoReport.checked = true;
    }
}

function buildTaskTargetsFromForm(formState) {
    const {
        collector,
        targetName,
        appId,
        skipSteamdb,
        steamdbTimeSlice,
        steamDiscussionsForumUrl,
        steamDiscussionsStart,
        steamDiscussionsEnd,
        steamDiscussionsMaxPages,
        steamDiscussionsMaxTopics,
        steamDiscussionsIncludeReplies,
        taptapUrl,
        taptapReviewsPages,
        taptapReviewsLimit,
        monitorDays,
        monitorTwitchName,
        monitorSiteurl,
        qimaiAppId,
        officialSiteUrl,
    } = formState;

    if (collector === "steam_discussions") {
        if (!targetName && !appId && !steamDiscussionsForumUrl) {
            return [];
        }

        const params = {
            ...(appId ? { app_id: appId } : {}),
            ...(steamDiscussionsForumUrl ? { forum_url: steamDiscussionsForumUrl } : {}),
            ...(steamDiscussionsStart ? { start_time: steamDiscussionsStart } : {}),
            ...(steamDiscussionsEnd ? { end_time: steamDiscussionsEnd } : {}),
            max_pages: Number(steamDiscussionsMaxPages || 50),
            max_topics: Number(steamDiscussionsMaxTopics || 1000),
            include_replies: Boolean(steamDiscussionsIncludeReplies),
        };

        return [
            {
                name: targetName || appId || steamDiscussionsForumUrl,
                target_type: "game",
                params,
            },
        ];
    }

    if (collector === "taptap") {
        if (!targetName && !taptapUrl && !appId) {
            return [];
        }

        const params = {
            region: "cn",
            metrics: ["details", "reviews", "updates"],
            reviews_pages: Number(taptapReviewsPages || 1),
            reviews_limit: Number(taptapReviewsLimit || 20),
            use_playwright: "auto",
            ...(taptapUrl ? { page_url: taptapUrl } : {}),
            ...(appId ? { app_id: appId } : {}),
        };

        return [
            {
                name: targetName || appId || taptapUrl,
                target_type: "game",
                params,
            },
        ];
    }

    if (collector === "monitor") {
        if (!targetName && !appId) {
            return [];
        }
        return [
            {
                name: targetName || appId,
                target_type: "game",
                params: {
                    app_id: appId,
                    days: Number(monitorDays || 30),
                    metrics: ["twitch_viewer_trend"],
                    ...(monitorTwitchName ? { twitch_name: monitorTwitchName } : {}),
                    ...(monitorSiteurl ? { siteurl: monitorSiteurl } : {}),
                },
            },
        ];
    }

    if (collector === "qimai") {
        if (!targetName && !qimaiAppId) {
            return [];
        }
        return [
            {
                name: targetName || qimaiAppId,
                target_type: "game",
                params: {
                    qimai_app_id: qimaiAppId,
                },
            },
        ];
    }

    if (collector === "official_site") {
        if (!officialSiteUrl) {
            return [];
        }
        return [
            {
                name: targetName || officialSiteUrl,
                target_type: "game",
                params: {
                    official_url: officialSiteUrl,
                    use_playwright: "auto",
                },
            },
        ];
    }

    if (!targetName && !appId) {
        return [];
    }

    const params = {
        ...(appId ? { app_id: appId } : {}),
        ...(!skipSteamdb && steamdbTimeSlice ? { steamdb_time_slice: steamdbTimeSlice } : {}),
        ...(skipSteamdb ? { skip_steamdb: true } : {}),
    };

    return [
        {
            name: targetName || appId,
            target_type: "game",
            params,
        },
    ];
}

async function createTask() {
    const name = document.getElementById("task-name")?.value.trim() || "";
    const dataGroup = document.getElementById("task-data-group")?.value.trim() || "";
    const pipelineName = document.getElementById("task-pipeline")?.value || "";
    const targetsRaw = taskTargetsEditor ? taskTargetsEditor.getValue().trim() : (document.getElementById("task-targets")?.value.trim() || "");
    const description = document.getElementById("task-desc")?.value.trim() || "";
    const targetName = document.getElementById("task-target-name")?.value.trim() || "";
    const steamAppId = document.getElementById("task-app-id")?.value.trim() || "";
    const steamDiscussionsAppId = document.getElementById("task-steam-discussions-app-id")?.value.trim() || "";
    const steamDiscussionsForumUrl = document.getElementById("task-steam-discussions-forum-url")?.value.trim() || "";
    const steamDiscussionsStart = document.getElementById("task-steam-discussions-start")?.value || "";
    const steamDiscussionsEnd = document.getElementById("task-steam-discussions-end")?.value || "";
    const steamDiscussionsMaxPages = document.getElementById("task-steam-discussions-max-pages")?.value || "50";
    const steamDiscussionsMaxTopics = document.getElementById("task-steam-discussions-max-topics")?.value || "1000";
    const steamDiscussionsIncludeReplies = document.getElementById("task-steam-discussions-include-replies")?.checked ?? true;
    const taptapAppId = document.getElementById("task-taptap-app-id")?.value.trim() || "";
    const skipSteamdb = document.getElementById("task-skip-steamdb")?.checked || false;
    const steamdbTimeSlice = document.getElementById("task-steamdb-time-slice")?.value || "monthly_peak_1y";
    const taptapUrl = document.getElementById("task-taptap-url")?.value.trim() || "";
    const taptapReviewsPages = document.getElementById("task-taptap-reviews-pages")?.value || "1";
    const taptapReviewsLimit = document.getElementById("task-taptap-reviews-limit")?.value || "20";
    const monitorAppId = document.getElementById("task-monitor-app-id")?.value.trim() || "";
    const monitorDays = document.getElementById("task-monitor-days")?.value || "30";
    const monitorTwitchName = document.getElementById("task-monitor-twitch-name")?.value.trim() || "";
    const monitorSiteurl = document.getElementById("task-monitor-siteurl")?.value.trim() || "";
    const qimaiAppId = document.getElementById("task-qimai-app-id")?.value.trim() || "";
    const officialSiteUrl = document.getElementById("task-official-site-url")?.value.trim() || "";
    const enableReport = document.getElementById("task-enable-report")?.checked || false;
    const reportPromptRaw = document.getElementById("task-report-prompt")?.value.trim() || "";
    const reportTemplate = document.getElementById("task-report-template")?.value || "default";
    const collector = getCollectorForPipeline(pipelineName);

    if (!name || !pipelineName) {
        toast("Task name and pipeline are required", "error");
        return;
    }

    let targets = buildTaskTargetsFromForm({
        collector,
        targetName,
        appId: collector === "taptap"
            ? taptapAppId
            : collector === "steam_discussions"
                ? steamDiscussionsAppId
                : steamAppId,
        ...(collector === "monitor" ? { appId: monitorAppId } : {}),
        skipSteamdb,
        steamdbTimeSlice,
        steamDiscussionsForumUrl,
        steamDiscussionsStart,
        steamDiscussionsEnd,
        steamDiscussionsMaxPages,
        steamDiscussionsMaxTopics,
        steamDiscussionsIncludeReplies,
        taptapUrl,
        taptapReviewsPages,
        taptapReviewsLimit,
        monitorDays,
        monitorTwitchName,
        monitorSiteurl,
        qimaiAppId,
        officialSiteUrl,
    });
    if (targetsRaw) {
        try {
            targets = JSON.parse(targetsRaw);
        } catch {
            toast("Targets JSON is invalid", "error");
            return;
        }
    }

    if (targets.length === 0) {
        toast("At least one target is required", "error");
        return;
    }

    const primarySubject = targetName
        || (collector === "taptap" ? taptapAppId : collector === "steam_discussions" ? steamDiscussionsAppId : steamAppId)
        || (collector === "official_site" ? officialSiteUrl : "")
        || name;
    const reportPrompt = reportPromptRaw
        || `基于本次采集结果，总结${primarySubject}的核心表现、版本更新、评论反馈和关键事件。`;
    const config = enableReport
        ? {
            report: {
                enabled: true,
                prompt: reportPrompt,
                template: reportTemplate,
                data_source: collector || pipelineName,
                params: {
                    use_vector: hasStorageStep(pipelineName, "vector"),
                },
            },
        }
        : {};
    if (dataGroup) {
        config.data_group = { id: dataGroup, name: dataGroup };
    }

    try {
        await api("/tasks", {
            method: "POST",
            body: JSON.stringify({
                name,
                pipeline_name: pipelineName,
                targets,
                description,
                config,
            }),
        });

        toast("Task created", "success");
        closeModal("modal-create-task");
        refreshDashboard();
        loadTasks();
    } catch (err) {
        toast(`Create failed: ${err.message}`, "error");
    }
}

async function cancelTask(id) {
    try {
        await api(`/tasks/${id}/cancel`, { method: "POST" });
        toast("Task cancelled", "success");
        refreshDashboard();
        loadTasks();
    } catch (err) {
        toast(`Cancel failed: ${err.message}`, "error");
    }
}

async function deleteTask(id) {
    if (!confirm(`Delete task "${id}"?`)) return;

    try {
        await api(`/tasks/${id}`, { method: "DELETE" });
        toast("Task deleted", "success");
        refreshDashboard();
        loadTasks();
    } catch (err) {
        toast(`Delete failed: ${err.message}`, "error");
    }
}

async function viewTaskLogs(id) {
    openModal("modal-task-logs");
    const modalLogs = document.getElementById("modal-task-logs");
    if (modalLogs) modalLogs.dataset.taskId = id; // 保存当前查看的任务 ID 供 WS 更新用
    
    const container = document.getElementById("task-logs-content");
    if (!container) return;

    container.innerHTML = '<p class="text-muted">Loading...</p>';

    try {
        const data = await api(`/tasks/${id}/logs`);
        if (!data.logs.length) {
            container.innerHTML = '<p class="text-muted">No logs</p>';
            return;
        }

        container.innerHTML = data.logs.map((log) => {
            const statusClass = log.status === "success"
                ? "log-success"
                : log.status === "failed"
                    ? "log-failed"
                    : "log-running";
            return `
                <div class="log-entry ${statusClass}">
                    <span class="log-step">${escapeHtml(log.step)}</span>
                    <span class="log-message">${escapeHtml(log.message || "")}</span>
                    ${log.error ? `<div style="color: var(--danger); margin-top: 0.25rem;">${escapeHtml(log.error)}</div>` : ""}
                    ${log.started_at ? `<span class="log-time">${formatTime(log.started_at)}</span>` : ""}
                </div>
            `;
        }).join("");
    } catch (err) {
        container.innerHTML = `<p style="color: var(--danger);">Load failed: ${escapeHtml(err.message)}</p>`;
    }
}

async function viewTaskDetail(id) {
    openModal("modal-task-detail");
    const container = document.getElementById("task-detail-content");
    if (!container) return;

    container.innerHTML = '<p class="text-muted">Loading...</p>';

    try {
        const task = await api(`/tasks/${id}`);
        const targets = task.targets?.length
            ? `<pre class="report-output">${escapeHtml(JSON.stringify(task.targets, null, 2))}</pre>`
            : '<p class="text-muted">No targets</p>';
        const config = Object.keys(task.config || {}).length
            ? `<pre class="report-output">${escapeHtml(JSON.stringify(task.config, null, 2))}</pre>`
            : '<p class="text-muted">No runtime config</p>';
        const resultSummary = task.result_summary
            ? `<pre class="report-output">${escapeHtml(JSON.stringify(task.result_summary, null, 2))}</pre>`
            : '<p class="text-muted">No result summary</p>';
        const autoReportLink = task.result_summary?.generated_report_id
            ? `<div style="margin-top: 0.75rem;"><button class="btn btn-primary btn-sm" onclick="viewReport('${task.result_summary.generated_report_id}')">Open Generated Report</button></div>`
            : "";
        const latestLogs = task.step_logs?.length
            ? task.step_logs.slice(-8).map((log) => `
                <div class="log-entry ${log.status === "success" ? "log-success" : log.status === "failed" ? "log-failed" : "log-running"}">
                    <span class="log-step">${escapeHtml(log.step)}</span>
                    <span class="log-message">${escapeHtml(log.message || "")}</span>
                    ${log.error ? `<div style="color: var(--danger); margin-top: 0.25rem;">${escapeHtml(log.error)}</div>` : ""}
                </div>
            `).join("")
            : '<p class="text-muted">No logs</p>';

        container.innerHTML = `
            <div class="detail-grid">
                <div class="detail-card">
                    <h3>Basic</h3>
                    <div class="detail-kv"><span>ID</span><code>${task.id}</code></div>
                    <div class="detail-kv"><span>Name</span><span>${escapeHtml(task.name)}</span></div>
                    <div class="detail-kv"><span>Status</span><span>${renderBadge(task.status)}</span></div>
                    <div class="detail-kv"><span>Pipeline</span><span>${escapeHtml(task.pipeline_name || "-")}</span></div>
                    <div class="detail-kv"><span>Progress</span><span>${Math.round(task.progress * 100)}%</span></div>
                    <div class="detail-kv"><span>Retry</span><span>${task.retry_count}/${task.max_retries}</span></div>
                    <div class="detail-kv"><span>Error</span><span>${escapeHtml(task.error || "-")}</span></div>
                </div>
                <div class="detail-card">
                    <h3>Description</h3>
                    <p>${escapeHtml(task.description || "No description")}</p>
                    <h3 style="margin-top: 1rem;">Recent Logs</h3>
                    ${latestLogs}
                </div>
            </div>
            <h3 style="margin-top: 1rem;">Targets</h3>
            ${targets}
            <h3 style="margin-top: 1rem;">Runtime Config</h3>
            ${config}
            <h3 style="margin-top: 1rem;">Result Summary</h3>
            ${resultSummary}
            ${autoReportLink}
        `;
    } catch (err) {
        container.innerHTML = `<p style="color: var(--danger);">Load failed: ${escapeHtml(err.message)}</p>`;
    }
}

async function loadComponents() {
    try {
        const components = await api("/components");
        availableComponents = components;

        const container = document.getElementById("components-list");
        if (!container) return;

        if (!Object.keys(components).length) {
            container.innerHTML = '<p class="text-muted">No components</p>';
            return;
        }

        container.innerHTML = Object.entries(components).map(([type, names]) => `
            <div class="component-group">
                <h3>${escapeHtml(type)}</h3>
                <div class="component-tags">
                    ${names.map((name) => `<span class="component-tag">${escapeHtml(name)}</span>`).join("")}
                </div>
            </div>
        `).join("");

        populatePipelineFormComponents();
    } catch (err) {
        console.error("Load components failed:", err);
    }
}

async function loadPipelineTemplates() {
    try {
        pipelineTemplates = await api("/pipeline-templates");
        const select = document.getElementById("pipeline-template");
        if (!select) return;

        select.innerHTML = '<option value="">-- Custom --</option>';
        for (const template of pipelineTemplates) {
            select.insertAdjacentHTML(
                "beforeend",
                `<option value="${template.id}">${escapeHtml(template.name)}</option>`
            );
        }
    } catch (err) {
        console.error("Load pipeline templates failed:", err);
    }
}

async function loadPipelines() {
    try {
        const pipelines = await api("/pipelines");
        const container = document.getElementById("pipelines-list");
        if (!container) return;

        const entries = Object.entries(pipelines);
        if (!entries.length) {
            container.innerHTML = '<p class="text-muted">No pipelines</p>';
            return;
        }

        container.innerHTML = entries.map(([name, config]) => `
            <div class="pipeline-item">
                <div class="pipeline-item-header">
                    <span class="pipeline-item-name">${escapeHtml(name)}</span>
                    <button class="btn btn-danger btn-sm" onclick="deletePipeline('${name}')">Delete</button>
                </div>
                <div class="pipeline-steps">
                    ${(config.steps || []).map((step, index) => `
                        ${index > 0 ? '<span class="pipeline-arrow">-></span>' : ""}
                        <span class="pipeline-step-tag ${escapeHtml(step.type)}">${escapeHtml(step.type)}:${escapeHtml(step.name)}</span>
                    `).join("")}
                </div>
            </div>
        `).join("");
    } catch (err) {
        console.error("Load pipelines failed:", err);
    }
}

function showCreatePipelineModal() {
    loadComponents();
    loadPipelineTemplates();
    openModal("modal-create-pipeline");
}

function populatePipelineFormComponents() {
    const collectorSelect = document.getElementById("pipeline-collector");
    if (!collectorSelect) return;

    const current = collectorSelect.value;
    const collectors = availableComponents.collector || [];
    collectorSelect.innerHTML = collectors.map((name) =>
        `<option value="${name}" ${name === current ? "selected" : ""}>${name}</option>`
    ).join("");
}

function applyPipelineTemplate() {
    const templateId = document.getElementById("pipeline-template")?.value;
    if (!templateId) return;

    const template = pipelineTemplates.find((item) => item.id === templateId);
    if (!template) return;

    setValue("pipeline-name", template.id);
    setValue("pipeline-collector", template.steps.find((step) => step.type === "collector")?.name || "");
    setChecked("pipeline-processor-cleaner", template.steps.some((step) => step.type === "processor" && step.name === "cleaner"));
    setChecked("pipeline-processor-embedding", template.steps.some((step) => step.type === "processor" && step.name === "embedding"));
    setChecked("pipeline-storage-local", template.steps.some((step) => step.type === "storage" && step.name === "local"));
    setChecked("pipeline-storage-vector", template.steps.some((step) => step.type === "storage" && step.name === "vector"));
    setValue("pipeline-steps", JSON.stringify(template.steps, null, 2));
}

function buildPipelineStepsFromForm() {
    const steps = [];
    const collector = document.getElementById("pipeline-collector")?.value || "";

    if (collector) {
        steps.push({ type: "collector", name: collector, config: {} });
    }
    if (document.getElementById("pipeline-processor-cleaner")?.checked) {
        steps.push({ type: "processor", name: "cleaner", config: {} });
    }
    if (document.getElementById("pipeline-processor-embedding")?.checked) {
        steps.push({ type: "processor", name: "embedding", config: {} });
    }
    if (document.getElementById("pipeline-storage-local")?.checked) {
        steps.push({ type: "storage", name: "local", config: {} });
    }
    if (document.getElementById("pipeline-storage-vector")?.checked) {
        steps.push({ type: "storage", name: "vector", config: {} });
    }

    return steps;
}

async function createPipeline() {
    const name = document.getElementById("pipeline-name")?.value.trim() || "";
    const stepsRaw = pipelineStepsEditor ? pipelineStepsEditor.getValue().trim() : (document.getElementById("pipeline-steps")?.value.trim() || "");

    if (!name) {
        toast("Pipeline name is required", "error");
        return;
    }

    let steps = buildPipelineStepsFromForm();
    if (stepsRaw) {
        try {
            steps = JSON.parse(stepsRaw);
        } catch {
            toast("Pipeline steps JSON is invalid", "error");
            return;
        }
    }

    if (!steps.length) {
        toast("Choose at least one collector and one storage step", "error");
        return;
    }

    try {
        await api("/pipelines", {
            method: "POST",
            body: JSON.stringify({ name, steps }),
        });
        toast("Pipeline created", "success");
        closeModal("modal-create-pipeline");
        loadPipelines();
        loadPipelineSelect("task-pipeline");
        loadPipelineSelect("cron-pipeline");
    } catch (err) {
        toast(`Create failed: ${err.message}`, "error");
    }
}

async function deletePipeline(name) {
    if (!confirm(`Delete pipeline "${name}"?`)) return;

    try {
        await api(`/pipelines/${name}`, { method: "DELETE" });
        toast("Pipeline deleted", "success");
        loadPipelines();
        loadPipelineSelect("task-pipeline");
        loadPipelineSelect("cron-pipeline");
    } catch (err) {
        toast(`Delete failed: ${err.message}`, "error");
    }
}

async function loadPipelineSelect(selectId) {
    try {
        const [pipelines, templates] = await Promise.all([
            api("/pipelines"),
            pipelineTemplates.length ? Promise.resolve(pipelineTemplates) : api("/pipeline-templates"),
        ]);
        pipelineTemplates = templates;
        availablePipelines = { ...pipelines };
        for (const template of pipelineTemplates) {
            if (!availablePipelines[template.id]) {
                availablePipelines[template.id] = template;
            }
        }
        const select = document.getElementById(selectId);
        if (!select) return;

        const current = select.value;
        select.innerHTML = '<option value="">-- Select Pipeline --</option>';
        for (const name of Object.keys(availablePipelines)) {
            select.insertAdjacentHTML(
                "beforeend",
                `<option value="${name}" ${name === current ? "selected" : ""}>${name}</option>`
            );
        }
        if (selectId === "task-pipeline") {
            updateTaskTargetFields();
        }
    } catch (err) {
        console.error("Load pipeline select failed:", err);
    }
}

async function loadDataGames() {
    try {
        dataGames = await api("/data/games");
        renderDataGames(dataGames);
        if (selectedDataGame && dataGames.some((game) => game.game_key === selectedDataGame.game_key)) {
            await selectDataGame(selectedDataGame.game_key);
        }
    } catch (err) {
        toast(`Load data failed: ${err.message}`, "error");
    }
}

async function loadDataGroups() {
    try {
        dataGroups = await api("/data/groups");
        const select = document.getElementById("report-group-select");
        if (select) {
            const current = select.value;
            select.innerHTML = '<option value="">-- Select group --</option>';
            for (const group of dataGroups) {
                select.insertAdjacentHTML(
                    "beforeend",
                    `<option value="${escapeHtml(group.group_id)}">${escapeHtml(group.group_name || group.group_id)} (${group.count})</option>`
                );
            }
            if ([...select.options].some((option) => option.value === current)) {
                select.value = current;
            }
        }
        return dataGroups;
    } catch (err) {
        console.error("Load groups failed:", err);
        return [];
    }
}

async function searchDataRecords() {
    const q = document.getElementById("data-search-query")?.value.trim() || "";
    if (!q) {
        loadDataGames();
        return;
    }
    try {
        selectedDataGame = null;
        currentDataRecords = await api(`/data/search?q=${encodeURIComponent(q)}`);
        const title = document.getElementById("data-records-title");
        const summary = document.getElementById("data-selected-summary");
        if (title) title.textContent = "Search results";
        if (summary) summary.textContent = `${currentDataRecords.length} matching records`;
        renderDataRecords(currentDataRecords);
    } catch (err) {
        toast(`Search failed: ${err.message}`, "error");
    }
}

function renderDataGames(games) {
    const container = document.getElementById("data-games-list");
    if (!container) return;

    if (!games.length) {
        container.innerHTML = '<p class="text-muted">暂无已落库数据</p>';
        return;
    }

    container.innerHTML = games.map((game) => {
        const activeClass = selectedDataGame?.game_key === game.game_key ? "active" : "";
        const sourceText = (game.sources || []).map((source) => `${source.name} ${source.count}`).join(" / ");
        return `
            <div class="data-game-item ${activeClass}" role="button" tabindex="0" onclick="selectDataGame('${escapeJs(game.game_key)}')" onkeydown="handleDataGameKeydown(event, '${escapeJs(game.game_key)}')">
                <button class="data-game-delete" type="button" title="Delete category" onclick="deleteDataGame(event, '${escapeJs(game.game_key)}')">Delete</button>
                <span class="data-game-name">${escapeHtml(game.game_name)}</span>
                <span class="data-game-meta">App ID: ${escapeHtml(game.app_id || "-")} | Group: ${escapeHtml(game.group_name || "-")} | ${game.total_records} records</span>
                <span class="data-game-sources">${escapeHtml(sourceText || "No source")}</span>
            </div>
        `;
    }).join("");
}

function handleDataGameKeydown(event, gameKey) {
    if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectDataGame(gameKey);
    }
}

async function selectDataGame(gameKey) {
    selectedDataGame = dataGames.find((game) => game.game_key === gameKey) || null;
    renderDataGames(dataGames);

    const title = document.getElementById("data-records-title");
    const summary = document.getElementById("data-selected-summary");
    const sourceFilter = document.getElementById("data-source-filter");

    if (!selectedDataGame) {
        if (title) title.textContent = "选择一个游戏";
        if (summary) summary.textContent = "按 App ID 或游戏名聚合已落库 JSON";
        return;
    }

    if (title) title.textContent = selectedDataGame.game_name;
    if (summary) {
        summary.textContent = `App ID: ${selectedDataGame.app_id || "-"} | ${selectedDataGame.total_records} 条记录 | 最新 ${formatTime(selectedDataGame.latest_stored_at)}`;
    }
    if (sourceFilter) {
        const current = sourceFilter.value;
        sourceFilter.innerHTML = '<option value="">全部数据源</option>';
        for (const source of selectedDataGame.sources || []) {
            sourceFilter.insertAdjacentHTML(
                "beforeend",
                `<option value="${escapeHtml(source.name)}">${escapeHtml(source.name)} (${source.count})</option>`
            );
        }
        sourceFilter.value = [...sourceFilter.options].some((option) => option.value === current) ? current : "";
    }

    await loadSelectedGameRecords();
}

async function loadSelectedGameRecords() {
    if (!selectedDataGame) return;
    const source = document.getElementById("data-source-filter")?.value || "";
    const query = source ? `?source=${encodeURIComponent(source)}` : "";
    try {
        currentDataRecords = await api(`/data/games/${encodeURIComponent(selectedDataGame.game_key)}/records${query}`);
        renderDataRecords(currentDataRecords);
    } catch (err) {
        toast(`Load records failed: ${err.message}`, "error");
    }
}

function renderDataRecords(records) {
    const tbody = document.getElementById("data-records-body");
    if (!tbody) return;

    if (!records.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="text-muted">该分类下暂无记录</td></tr>';
        return;
    }

    tbody.innerHTML = records.map((record) => `
        <tr>
            <td><code>${escapeHtml(record.key)}</code></td>
            <td>${escapeHtml(record.data_source)}</td>
            <td>${escapeHtml(formatDataSummary(record.summary || {}))}</td>
            <td>${formatTime(record.stored_at)}</td>
            <td>
                <div class="action-buttons">
                    <button class="btn btn-ghost btn-sm" onclick="previewDataRecord('${escapeJs(record.key)}')">预览</button>
                    <button class="btn btn-ghost btn-sm" onclick="downloadDataRecord('${escapeJs(record.key)}')">导出</button>
                    <button class="btn btn-ghost btn-sm" onclick="editDataRecord('${escapeJs(record.key)}')">Edit</button>
                    <button class="btn btn-ghost btn-sm" onclick="refreshDataRecord('${escapeJs(record.key)}')">Update</button>
                    <button class="btn btn-ghost btn-sm" onclick="scheduleDataRecordRefresh('${escapeJs(record.key)}')">Schedule</button>
                    <button class="btn btn-primary btn-sm" onclick="useDataRecordForReport('${escapeJs(record.key)}')">报告</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteDataRecord('${escapeJs(record.key)}')">Delete</button>
                </div>
            </td>
        </tr>
    `).join("");
}

function formatDataSummary(summary) {
    const entries = Object.entries(summary || {}).filter(([, value]) => value !== null && value !== undefined && value !== "");
    if (!entries.length) return "-";
    return entries.slice(0, 4).map(([key, value]) => `${key}: ${value}`).join(" | ");
}

async function previewDataRecord(key) {
    const preview = document.getElementById("data-preview");
    if (preview) preview.textContent = "加载中...";
    try {
        const detail = await api(`/data/records/${encodeURIComponent(key)}`);
        if (preview) {
            preview.textContent = JSON.stringify(detail, null, 2);
        }
    } catch (err) {
        if (preview) preview.textContent = `Load failed: ${err.message}`;
    }
}

function downloadDataRecord(key) {
    window.open(`/api/data/records/${encodeURIComponent(key)}/download`, "_blank");
}

async function editDataRecord(key) {
    const record = currentDataRecords.find((item) => item.key === key) || {};
    const groupName = prompt("Data group", record.group_name || record.group_id || "");
    if (groupName === null) return;
    const displayName = prompt("Display name", record.display_name || record.game_name || "");
    if (displayName === null) return;
    try {
        const updated = await api(`/data/records/${encodeURIComponent(key)}`, {
            method: "PATCH",
            body: JSON.stringify({
                group_id: groupName.trim(),
                group_name: groupName.trim(),
                display_name: displayName.trim(),
            }),
        });
        toast("Record updated", "success");
        await loadDataGames();
        if (selectedDataGame) {
            await loadSelectedGameRecords();
        } else {
            await searchDataRecords();
        }
        previewDataRecord(updated.key);
    } catch (err) {
        toast(`Update failed: ${err.message}`, "error");
    }
}

async function deleteDataRecord(key) {
    if (!confirm(`Delete data record ${key}?`)) return;
    try {
        await api(`/data/records/${encodeURIComponent(key)}`, { method: "DELETE" });
        toast("Record deleted", "success");
        await loadDataGames();
        if (selectedDataGame) {
            await loadSelectedGameRecords();
        } else {
            renderDataRecords(currentDataRecords.filter((item) => item.key !== key));
        }
    } catch (err) {
        toast(`Delete failed: ${err.message}`, "error");
    }
}

async function deleteDataGame(event, gameKey) {
    event?.stopPropagation();
    const game = dataGames.find((item) => item.game_key === gameKey);
    if (!game) return;
    const name = game.group_name || game.game_name || game.game_key;
    const message = `Delete category "${name}" and all related records, vector data, tasks, schedules, and reports?`;
    if (!confirm(message)) return;
    try {
        const resp = await api(`/data/games/${encodeURIComponent(gameKey)}`, { method: "DELETE" });
        toast(`Category deleted: ${resp.records_deleted} records`, "success");
        if (selectedDataGame?.game_key === gameKey) {
            selectedDataGame = null;
            currentDataRecords = [];
            renderDataRecords([]);
            const title = document.getElementById("data-records-title");
            const summary = document.getElementById("data-selected-summary");
            if (title) title.textContent = "Choose a game category";
            if (summary) summary.textContent = "";
        }
        await loadDataGames();
        await loadDataGroups();
        loadTasks();
        loadReports();
    } catch (err) {
        toast(`Delete category failed: ${err.message}`, "error");
    }
}

async function refreshDataRecord(key) {
    try {
        const resp = await api(`/data/records/${encodeURIComponent(key)}/refresh`, {
            method: "POST",
            body: JSON.stringify({ rolling_window: true }),
        });
        toast(`Refresh task submitted: ${resp.task_id}`, "success");
        activateTab("tasks");
        loadTasks();
    } catch (err) {
        toast(`Refresh failed: ${err.message}`, "error");
    }
}

async function scheduleDataRecordRefresh(key) {
    const cronExpr = prompt("Cron expression", "0 8 * * *");
    if (!cronExpr) return;
    const name = prompt("Schedule name", `refresh_${key.replace(/[^a-zA-Z0-9_-]+/g, "_").slice(0, 48)}`);
    if (!name) return;
    try {
        await api(`/data/records/${encodeURIComponent(key)}/refresh-schedules`, {
            method: "POST",
            body: JSON.stringify({ name, cron_expr: cronExpr, rolling_window: true }),
        });
        toast("Refresh schedule created", "success");
        loadCronJobs();
    } catch (err) {
        toast(`Schedule failed: ${err.message}`, "error");
    }
}

function useDataRecordForReport(key) {
    const record = currentDataRecords.find((item) => item.key === key);
    addReportRecordSelection(key, record);
    syncSelectedReportRecordKeys();
    setValue("report-data-source", record?.data_source || "");
    setValue("report-prompt", `基于所选原始 JSON 数据，生成${record?.game_name || "该游戏"}的数据分析报告。`);
    activateTab("reports");
    toast("已添加 1 条原始 JSON 用于报告", "success");
}

function useCurrentDataForReport() {
    if (!currentDataRecords.length) {
        toast("当前没有可用于报告的记录", "error");
        return;
    }
    for (const record of currentDataRecords) {
        addReportRecordSelection(record.key, record);
    }
    syncSelectedReportRecordKeys();
    setValue("report-data-source", selectedDataGame?.game_name || "");
    setValue("report-prompt", `基于${selectedDataGame?.game_name || "所选游戏"}当前筛选出的原始 JSON 数据，生成综合分析报告。`);
    activateTab("reports");
    toast(`已选择 ${selectedReportRecordKeys.length} 条原始 JSON 用于报告`, "success");
}

function clearSelectedReportRecords() {
    selectedReportRecordKeys = [];
    selectedReportRecordMeta = {};
    syncSelectedReportRecordKeys();
}

function syncSelectedReportRecordKeys() {
    const el = document.getElementById("report-record-keys");
    if (el) {
        el.value = selectedReportRecordKeys.join("\n");
    }
    renderSelectedReportRecords();
    updateReportTemplateHelp();
}

function syncReportRecordKeysFromTextarea() {
    const raw = document.getElementById("report-record-keys")?.value.trim() || "";
    const keys = raw ? raw.split(/\s+/).map((item) => item.trim()).filter(Boolean) : [];
    selectedReportRecordKeys = [...new Set(keys)];
    for (const key of Object.keys(selectedReportRecordMeta)) {
        if (!selectedReportRecordKeys.includes(key)) {
            delete selectedReportRecordMeta[key];
        }
    }
    renderSelectedReportRecords();
    updateReportTemplateHelp();
}

function addReportRecordSelection(key, meta = null) {
    if (!selectedReportRecordKeys.includes(key)) {
        selectedReportRecordKeys.push(key);
    }
    if (meta) {
        selectedReportRecordMeta[key] = {
            key,
            collector: meta.collector || "",
            data_source: meta.data_source || meta.collector || "",
            game_name: meta.game_name || "",
            app_id: meta.app_id || "",
        };
    }
}

function renderSelectedReportRecords() {
    const container = document.getElementById("report-selected-records");
    if (!container) return;
    if (!selectedReportRecordKeys.length) {
        container.innerHTML = '<p class="text-muted">尚未添加 JSON 数据源</p>';
        return;
    }
    container.innerHTML = selectedReportRecordKeys.map((key) => {
        const meta = selectedReportRecordMeta[key] || {};
        const label = meta.data_source || meta.collector || "手工输入";
        const title = meta.game_name ? `${meta.game_name} / ${label}` : label;
        return `
            <div class="selected-source-chip">
                <span>
                    <strong>${escapeHtml(title)}</strong>
                    <code>${escapeHtml(key)}</code>
                </span>
                <button class="btn btn-ghost btn-sm" type="button" onclick="removeReportRecordSelection('${escapeJs(key)}')">移除</button>
            </div>
        `;
    }).join("");
}

function removeReportRecordSelection(key) {
    selectedReportRecordKeys = selectedReportRecordKeys.filter((item) => item !== key);
    delete selectedReportRecordMeta[key];
    syncSelectedReportRecordKeys();
}

async function loadReportTemplates() {
    try {
        reportTemplates = await api("/reports/templates");
        const select = document.getElementById("report-template");
        if (!select) return;
        const current = select.value;
        select.innerHTML = reportTemplates.map((template) => (
            `<option value="${escapeHtml(template.id)}">${escapeHtml(template.name)}</option>`
        )).join("");
        if ([...select.options].some((option) => option.value === current)) {
            select.value = current;
        }
        updateReportTemplateHelp();
    } catch (err) {
        console.error("Load report templates failed:", err);
    }
}

function updateReportTemplateHelp() {
    const templateId = document.getElementById("report-template")?.value || "";
    const help = document.getElementById("report-template-help");
    if (!help) return;
    const template = reportTemplates.find((item) => item.id === templateId);
    if (!template) {
        help.textContent = "";
        return;
    }
    const knownCollectors = new Set(
        Object.values(selectedReportRecordMeta)
            .map((meta) => normalizeCollector(meta.collector))
            .filter(Boolean)
    );
    const missing = (template.required_collectors || []).filter((collector) => !knownCollectors.has(collector));
    const requirements = (template.required_collectors || []).map(labelCollector).join(" / ");
    const manualCount = selectedReportRecordKeys.filter((key) => !selectedReportRecordMeta[key]).length;
    const status = missing.length
        ? `缺少：${missing.map(labelCollector).join(" / ")}`
        : "已满足已知数据源要求";
    help.innerHTML = `
        <span>${escapeHtml(template.description)}</span><br>
        <span>必需数据源：${escapeHtml(requirements || "-")}；${escapeHtml(status)}</span>
        ${manualCount ? `<br><span>包含 ${manualCount} 个手工 key，前端无法识别来源，后端生成时会再次校验。</span>` : ""}
    `;
}

function normalizeCollector(value) {
    const normalized = String(value || "").toLowerCase();
    const aliases = {
        google_trends: "gtrends",
        pytrends: "gtrends",
        steam_api: "steam",
        steamdb: "steam",
        firecrawl: "steam",
    };
    return aliases[normalized] || normalized;
}

function labelCollector(value) {
    const labels = {
        steam: "Steam",
        taptap: "TapTap",
        gtrends: "Google Trends",
        monitor: "Monitor",
        events: "事件数据",
        steam_discussions: "Steam Community Discussions",
    };
    return labels[value] || value;
}

async function uploadReportJsonFiles() {
    const input = document.getElementById("report-json-files");
    const files = [...(input?.files || [])];
    if (!files.length) {
        toast("请选择 JSON 文件", "error");
        return;
    }
    const formData = new FormData();
    for (const file of files) {
        formData.append("files", file);
    }
    try {
        const resp = await fetch("/api/reports/upload-json", {
            method: "POST",
            body: formData,
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({ detail: resp.statusText }));
            throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        const uploaded = await resp.json();
        for (const item of uploaded) {
            addReportRecordSelection(item.key, {
                collector: item.collector,
                data_source: labelCollector(normalizeCollector(item.collector)),
                game_name: item.game_name,
                app_id: item.app_id,
            });
        }
        syncSelectedReportRecordKeys();
        if (input) input.value = "";
        loadDataGames();
        toast(`已导入 ${uploaded.length} 个 JSON 数据源`, "success");
    } catch (err) {
        toast(`Upload failed: ${err.message}`, "error");
    }
}

async function importReportGroupRecords() {
    const groupId = document.getElementById("report-group-select")?.value || "";
    if (!groupId) {
        toast("Choose a data group", "error");
        return;
    }
    try {
        const records = await api(`/reports/group-records?group_id=${encodeURIComponent(groupId)}`);
        for (const record of records) {
            addReportRecordSelection(record.key, record);
        }
        syncSelectedReportRecordKeys();
        toast(`Imported ${records.length} records`, "success");
    } catch (err) {
        toast(`Import failed: ${err.message}`, "error");
    }
}

async function loadReports() {
    try {
        const reports = await api("/reports");
        const container = document.getElementById("reports-list");
        if (!container) return;

        if (!reports.length) {
            container.innerHTML = '<p class="text-muted">No reports</p>';
            return;
        }

        container.innerHTML = reports.map((report) => `
            <div class="report-item">
                <button class="report-item-main" onclick="viewReport('${report.id}')">
                    <span class="report-item-title">${escapeHtml(report.title)}</span>
                    <span class="report-item-meta">${formatTime(report.generated_at)} | ${escapeHtml(report.template)} | ${report.matched_records} records</span>
                </button>
                <div class="inline-actions">
                    <button class="btn btn-ghost btn-sm" onclick="editReport('${report.id}')">Edit</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteReport('${report.id}')">Delete</button>
                </div>
            </div>
        `).join("");
    } catch (err) {
        toast(`Load failed: ${err.message}`, "error");
    }
}

function renderReport(report) {
    const container = document.getElementById("report-content");
    if (container) {
        let contentHtml = `<pre>${escapeHtml(report.content || "")}</pre>`;
        
        // Add Excel download button if it's an Excel report
        const isExcel = report.metadata?.format === "excel" || report.metadata?.excel_path;
        if (isExcel) {
            contentHtml = `
                <div style="margin-bottom: 1rem; padding: 1rem; background: var(--bg-card); border-radius: 4px; border: 1px solid var(--border);">
                    <h4 style="margin: 0 0 0.5rem 0; color: var(--success);">📊 Excel 报告已生成</h4>
                    <p style="margin: 0 0 1rem 0; color: var(--text-muted);">
                        该报告包含了清洗好的表格行、多个工作表以及统计图表。
                    </p>
                    <a href="/api/reports/${report.id}/download" class="btn btn-primary" target="_blank" download>
                        ⬇️ 下载 Excel 文件
                    </a>
                </div>
            ` + contentHtml;
        }
        
        container.innerHTML = contentHtml;
    }
}

async function generateReport() {
    syncReportRecordKeysFromTextarea();
    const prompt = document.getElementById("report-prompt")?.value.trim() || "";
    const dataSource = document.getElementById("report-data-source")?.value.trim() || "";
    const template = document.getElementById("report-template")?.value || "default";
    const recordKeysRaw = document.getElementById("report-record-keys")?.value.trim() || "";
    const recordKeys = recordKeysRaw
        ? recordKeysRaw.split(/\s+/).map((item) => item.trim()).filter(Boolean)
        : [];

    if (!prompt) {
        toast("Prompt is required", "error");
        return;
    }

    currentReportProgressId = `report_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    resetReportProgress();
    const button = document.getElementById("btn-generate-report");
    if (button) {
        button.disabled = true;
        button.textContent = "Generating...";
    }

    try {
        setReportProgress(0.08, "requesting", "Sending report request");
        const report = await api("/reports/generate-excel", {
            method: "POST",
            body: JSON.stringify({
                prompt,
                data_source: dataSource,
                template,
                record_keys: recordKeys,
                params: { progress_id: currentReportProgressId },
            }),
        });
        setReportProgress(1, "completed", "Report generated");
        renderReport(report);
        loadReports();
        toast("Report generated", "success");
    } catch (err) {
        setReportProgress(1, "failed", err.message);
        toast(`Generate failed: ${err.message}`, "error");
    } finally {
        if (button) {
            button.disabled = false;
            setTimeout(() => { button.textContent = "生成报告"; }, 0);
            button.textContent = "生成报告";
        }
    }
}

async function viewReport(id) {
    try {
        const report = await api(`/reports/${id}`);
        renderReport(report);
    } catch (err) {
        toast(`Load failed: ${err.message}`, "error");
    }
}

async function editReport(id) {
    try {
        const report = await api(`/reports/${id}`);
        const title = prompt("Report title", report.title || "");
        if (title === null) return;
        const notes = prompt("Notes", report.metadata?.notes || "");
        if (notes === null) return;
        const updated = await api(`/reports/${id}`, {
            method: "PATCH",
            body: JSON.stringify({ title: title.trim(), notes: notes.trim() }),
        });
        renderReport(updated);
        loadReports();
        toast("Report updated", "success");
    } catch (err) {
        toast(`Edit failed: ${err.message}`, "error");
    }
}

async function deleteReport(id) {
    if (!confirm(`Delete report ${id}?`)) return;
    try {
        await api(`/reports/${id}`, { method: "DELETE" });
        toast("Report deleted", "success");
        loadReports();
        const container = document.getElementById("report-content");
        if (container) container.textContent = "No report selected";
    } catch (err) {
        toast(`Delete failed: ${err.message}`, "error");
    }
}

async function loadCronJobs() {
    try {
        const jobs = await api("/cron-jobs");
        const container = document.getElementById("cron-list");
        if (!container) return;

        if (!jobs.length) {
            container.innerHTML = '<p class="text-muted">No cron jobs</p>';
            return;
        }

        container.innerHTML = jobs.map((job) => `
            <div class="cron-item">
                <div class="cron-info">
                    <span class="cron-name">${escapeHtml(job.name)}</span>
                    <span class="cron-detail">Trigger: ${escapeHtml(job.trigger)} | Next: ${job.next_run || "-"}</span>
                </div>
                <button class="btn btn-danger btn-sm" onclick="deleteCronJob('${job.id}')">Delete</button>
            </div>
        `).join("");
    } catch (err) {
        console.error("Load cron jobs failed:", err);
    }
}

function showCreateCronModal() {
    loadPipelineSelect("cron-pipeline");
    openModal("modal-create-cron");
}

async function createCronJob() {
    const name = document.getElementById("cron-name")?.value.trim() || "";
    const pipelineName = document.getElementById("cron-pipeline")?.value || "";
    const cronExpr = document.getElementById("cron-expr")?.value.trim() || "";

    if (!name || !pipelineName || !cronExpr) {
        toast("All cron fields are required", "error");
        return;
    }

    try {
        await api("/cron-jobs", {
            method: "POST",
            body: JSON.stringify({
                name,
                pipeline_name: pipelineName,
                cron_expr: cronExpr,
            }),
        });
        toast("Cron job created", "success");
        closeModal("modal-create-cron");
        loadCronJobs();
    } catch (err) {
        toast(`Create failed: ${err.message}`, "error");
    }
}

async function deleteCronJob(name) {
    if (!confirm(`Delete cron job "${name}"?`)) return;

    try {
        await api(`/cron-jobs/${name}`, { method: "DELETE" });
        toast("Cron job deleted", "success");
        loadCronJobs();
    } catch (err) {
        toast(`Delete failed: ${err.message}`, "error");
    }
}

function renderBadge(status) {
    const labels = {
        pending: "Pending",
        running: "Running",
        success: "Success",
        failed: "Failed",
        cancelled: "Cancelled",
        retrying: "Retrying",
    };

    return `<span class="badge badge-${status}">${labels[status] || status}</span>`;
}

function renderProgress(progress) {
    const pct = Math.round((progress || 0) * 100);
    return `
        <div style="display: flex; align-items: center; gap: 0.5rem;">
            <div class="progress-bar">
                <div class="progress-bar-fill" style="width: ${pct}%"></div>
            </div>
            <span style="font-size: 0.8rem; color: var(--text-muted);">${pct}%</span>
        </div>
    `;
}

function formatTime(isoStr) {
    if (!isoStr) return "-";
    const d = new Date(isoStr);
    return d.toLocaleString("zh-CN", {
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
    });
}

function setText(id, value) {
    const el = document.getElementById(id);
    if (el) {
        el.textContent = value;
    }
}

function setValue(id, value) {
    const el = document.getElementById(id);
    if (el) {
        el.value = value;
    }
}

function setChecked(id, value) {
    const el = document.getElementById(id);
    if (el) {
        el.checked = value;
    }
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function escapeJs(value) {
    return String(value).replaceAll("\\", "\\\\").replaceAll("'", "\\'");
}

function restartAutoRefresh() {
    if (autoRefreshHandle) {
        clearInterval(autoRefreshHandle);
        autoRefreshHandle = null;
    }

    autoRefreshHandle = setInterval(() => {
        if (activeTab === "dashboard") {
            refreshDashboard();
        } else if (activeTab === "tasks") {
            loadTasks();
        } else if (activeTab === "data") {
            loadDataGames();
        } else if (activeTab === "cron") {
            loadCronJobs();
        }
    }, AUTO_REFRESH_INTERVAL_MS);
}

function activateTab(tab) {
    activeTab = tab;

    document.querySelectorAll(".nav-link").forEach((item) => {
        item.classList.toggle("active", item.dataset.tab === tab);
    });

    document.querySelectorAll(".tab-content").forEach((panel) => panel.classList.remove("active"));
    document.getElementById(`tab-${tab}`)?.classList.add("active");

    loadTabData(tab);
    restartAutoRefresh();
}

function bindNavigation() {
    document.querySelectorAll(".nav-link").forEach((link) => {
        link.addEventListener("click", (e) => {
            e.preventDefault();
            activateTab(link.dataset.tab);
        });
    });
}

function bindModalOverlayClose() {
    document.querySelectorAll(".modal-overlay").forEach((overlay) => {
        overlay.addEventListener("click", (e) => {
            if (e.target === overlay) {
                overlay.classList.remove("show");
            }
        });
    });
}

document.addEventListener("DOMContentLoaded", () => {
    initWebSocket(); // 初始化 WebSocket
    initEditors(); // 初始化代码编辑器
    bindNavigation();
    bindModalOverlayClose();
    refreshDashboard();
    loadTasks();
    loadPipelineTemplates();
    loadComponents();
    loadReportTemplates();
    restartAutoRefresh();
});
