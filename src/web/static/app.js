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
let currentDataPage = 1;
let currentDataPageSize = 50;
let currentDataTotal = 0;
let currentDataSourceFilter = "";
let currentDataSortOrder = "desc";
let selectedDataRecordKeys = new Set();
let selectedReportRecordKeys = [];
let selectedReportRecordMeta = {};
let reportTemplates = [];
let currentReportProgressId = "";

let wsConnection = null;

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

function renderTaskPrecheck(precheck) {
    const container = document.getElementById("task-precheck");
    if (!container || !precheck) return;
    const issues = precheck.issues || [];
    const credentials = precheck.credential_status || {};
    const dataSources = precheck.data_source_status || {};
    const required = precheck.required_fields || [];
    container.style.display = "block";
    container.className = `task-precheck task-precheck-${precheck.status || "ok"}`;
    container.innerHTML = `
        <div class="task-precheck-title">Precheck: ${escapeHtml(precheck.status || "ok")}</div>
        <div class="task-precheck-grid">
            <span>Collector</span><strong>${escapeHtml(precheck.collector_name || "-")}</strong>
            <span>Required</span><strong>${escapeHtml(required.join(" / ") || "-")}</strong>
            <span>Credentials</span><strong>${escapeHtml(Object.entries(credentials).map(([key, value]) => `${key}: ${value}`).join(" / ") || "-")}</strong>
            <span>Data source</span><strong>${escapeHtml(Object.entries(dataSources).map(([key, value]) => `${key}: ${value}`).join(" / ") || "-")}</strong>
        </div>
        ${issues.length ? `<ul>${issues.map((issue) => `<li class="task-precheck-${escapeHtml(issue.level)}">${escapeHtml(issue.field)}: ${escapeHtml(issue.message)}</li>`).join("")}</ul>` : ""}
    `;
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

    const requestPayload = {
        name,
        pipeline_name: pipelineName,
        targets,
        description,
        config,
    };

    try {
        const precheck = await api("/tasks/precheck", {
            method: "POST",
            body: JSON.stringify(requestPayload),
        });
        renderTaskPrecheck(precheck);
        if (!precheck.can_submit) {
            toast("Task precheck failed", "error");
            return;
        }
        if (precheck.status === "warning") {
            const warningText = (precheck.issues || [])
                .filter((issue) => issue.level === "warning")
                .map((issue) => issue.message)
                .join("\n");
            if (!confirm(`Task precheck has warnings:\n${warningText}\n\nSubmit anyway?`)) {
                return;
            }
        }
        await api("/tasks", {
            method: "POST",
            body: JSON.stringify(requestPayload),
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
        await api(`/tasks/${encodeURIComponent(id)}?confirm=true`, { method: "DELETE" });
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
        await api(`/pipelines/${encodeURIComponent(name)}?confirm=true`, { method: "DELETE" });
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
        selectedDataRecordKeys.clear();
        const params = new URLSearchParams({ q, page: "1", page_size: String(currentDataPageSize) });
        const result = await api(`/data/records?${params.toString()}`);
        currentDataRecords = result.items;
        currentDataTotal = result.total;
        currentDataPage = result.page;
        const title = document.getElementById("data-records-title");
        const summary = document.getElementById("data-selected-summary");
        if (title) title.textContent = "Search results";
        if (summary) summary.textContent = `${result.total} matching records`;
        renderDataRecords(currentDataRecords);
        renderPagination(result);
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
    currentDataPage = 1;
    currentDataSourceFilter = "";
    selectedDataRecordKeys.clear();
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

    await loadSelectedGameRecords(1);
}

async function loadSelectedGameRecords(page = 1) {
    if (!selectedDataGame) return;
    const source = currentDataSourceFilter || document.getElementById("data-source-filter")?.value || "";
    const sortOrder = document.getElementById("data-sort-order")?.value || currentDataSortOrder;
    const pageSize = currentDataPageSize;
    currentDataPage = page;

    const params = new URLSearchParams();
    params.set("page", String(page));
    params.set("page_size", String(pageSize));
    params.set("sort_order", sortOrder);
    if (source) params.set("source", source);
    if (selectedDataGame.app_id) params.set("app_id", selectedDataGame.app_id);

    try {
        const result = await api(`/data/records?${params.toString()}`);
        currentDataRecords = result.items;
        currentDataTotal = result.total;
        currentDataPage = result.page;
        renderDataRecords(currentDataRecords);
        renderPagination(result);
    } catch (err) {
        toast(`Load records failed: ${err.message}`, "error");
    }
}

function renderDataRecords(records) {
    const tbody = document.getElementById("data-records-body");
    if (!tbody) return;

    if (!records.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-muted">该分类下暂无记录</td></tr>';
        return;
    }

    const allChecked = records.length > 0 && records.every((r) => selectedDataRecordKeys.has(r.key));

    tbody.innerHTML = records.map((record) => {
        const checked = selectedDataRecordKeys.has(record.key) ? "checked" : "";
        const comp = record.completeness || "full";
        const compLabel = { full: "完整", partial: "部分", empty: "空" }[comp] || comp;
        return `
        <tr>
            <td class="cell-checkbox">
                <input type="checkbox" class="record-checkbox" data-key="${escapeHtml(record.key)}" ${checked} onclick="toggleRecordSelect(this)" />
            </td>
            <td><code>${escapeHtml(record.key)}</code></td>
            <td>${escapeHtml(record.data_source)}</td>
            <td><span class="completeness-badge completeness-${comp}" title="数据完整度: ${compLabel}">${compLabel}</span> ${escapeHtml(formatDataSummary(record.summary || {}))}</td>
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
    `}).join("");

    const selectAll = document.getElementById("data-select-all");
    if (selectAll) {
        selectAll.checked = allChecked;
        selectAll.indeterminate = !allChecked && selectedDataRecordKeys.size > 0;
    }
    updateBatchActionBar();
}

function toggleRecordSelect(el) {
    const key = el.dataset.key;
    if (el.checked) {
        selectedDataRecordKeys.add(key);
    } else {
        selectedDataRecordKeys.delete(key);
    }
    updateBatchActionBar();
}

function toggleSelectAll(el) {
    if (el.checked) {
        currentDataRecords.forEach((r) => selectedDataRecordKeys.add(r.key));
    } else {
        currentDataRecords.forEach((r) => selectedDataRecordKeys.delete(r.key));
    }
    renderDataRecords(currentDataRecords);
}

function updateBatchActionBar() {
    const bar = document.getElementById("data-batch-bar");
    const countEl = document.getElementById("data-batch-count");
    if (!bar) return;
    if (selectedDataRecordKeys.size > 0) {
        bar.style.display = "flex";
        if (countEl) countEl.textContent = `已选 ${selectedDataRecordKeys.size} 条`;
    } else {
        bar.style.display = "none";
    }
}

async function batchDeleteSelected() {
    if (selectedDataRecordKeys.size === 0) return;
    const keys = Array.from(selectedDataRecordKeys);
    if (!confirm(`确定删除 ${keys.length} 条记录？此操作不可撤销。`)) return;
    try {
        const result = await api("/data/records/batch-delete", {
            method: "POST",
            body: JSON.stringify({ keys, confirm: true }),
        });
        toast(result.message, "success");
        selectedDataRecordKeys.clear();
        loadSelectedGameRecords(currentDataPage);
    } catch (err) {
        toast(`Batch delete failed: ${err.message}`, "error");
    }
}

async function batchExportSelected() {
    if (selectedDataRecordKeys.size === 0) return;
    const keys = Array.from(selectedDataRecordKeys);
    try {
        const result = await api("/data/records/batch-export", {
            method: "POST",
            body: JSON.stringify({ keys, confirm: false }),
        });
        const blob = new Blob([JSON.stringify(result, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `batch_export_${new Date().toISOString().slice(0, 10)}.json`;
        a.click();
        URL.revokeObjectURL(url);
        toast(`Exported ${result.count} records`, "success");
    } catch (err) {
        toast(`Batch export failed: ${err.message}`, "error");
    }
}

function renderPagination(result) {
    let container = document.getElementById("data-pagination");
    if (!container) {
        const table = document.getElementById("data-records-table");
        if (!table) return;
        container = document.createElement("div");
        container.id = "data-pagination";
        container.className = "pagination-bar";
        table.parentNode.insertBefore(container, table.nextSibling);
    }

    const totalPages = Math.ceil(result.total / result.page_size);
    if (totalPages <= 1 && result.page_size <= 50) {
        container.innerHTML = `<span class="text-muted">共 ${result.total} 条</span>`;
        return;
    }

    let pageBtns = "";
    const maxVisible = 5;
    let startPage = Math.max(1, result.page - Math.floor(maxVisible / 2));
    let endPage = Math.min(totalPages, startPage + maxVisible - 1);
    if (endPage - startPage < maxVisible - 1) startPage = Math.max(1, endPage - maxVisible + 1);

    for (let i = startPage; i <= endPage; i++) {
        pageBtns += `<button class="btn btn-ghost btn-sm ${i === result.page ? "active" : ""}" onclick="goToPage(${i})">${i}</button>`;
    }

    container.innerHTML = `
        <div class="pagination-controls">
            <button class="btn btn-ghost btn-sm" onclick="goToPage(${result.page - 1})" ${result.page <= 1 ? "disabled" : ""}>上一页</button>
            ${pageBtns}
            <button class="btn btn-ghost btn-sm" onclick="goToPage(${result.page + 1})" ${!result.has_more ? "disabled" : ""}>下一页</button>
            <span class="text-muted">共 ${result.total} 条</span>
            <select class="page-size-select" onchange="changePageSize(this.value)">
                <option value="20" ${result.page_size === 20 ? "selected" : ""}>20条/页</option>
                <option value="50" ${result.page_size === 50 ? "selected" : ""}>50条/页</option>
                <option value="100" ${result.page_size === 100 ? "selected" : ""}>100条/页</option>
                <option value="200" ${result.page_size === 200 ? "selected" : ""}>200条/页</option>
            </select>
        </div>
    `;
}

function goToPage(page) {
    if (selectedDataGame) {
        loadSelectedGameRecords(page);
    } else {
        const q = document.getElementById("data-search-query")?.value.trim() || "";
        if (q) {
            const params = new URLSearchParams({ q, page: String(page), page_size: String(currentDataPageSize) });
            api(`/data/records?${params.toString()}`).then((result) => {
                currentDataRecords = result.items;
                currentDataTotal = result.total;
                currentDataPage = result.page;
                renderDataRecords(currentDataRecords);
                renderPagination(result);
            }).catch((err) => toast(`Load failed: ${err.message}`, "error"));
        }
    }
}

function changePageSize(size) {
    currentDataPageSize = parseInt(size);
    currentDataPage = 1;
    goToPage(1);
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
        await api(`/data/records/${encodeURIComponent(key)}?confirm=true`, { method: "DELETE" });
        toast("Record deleted", "success");
        selectedDataRecordKeys.delete(key);
        await loadDataGames();
        loadSelectedGameRecords(currentDataPage);
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
        const resp = await api(`/data/games/${encodeURIComponent(gameKey)}?confirm=true`, { method: "DELETE" });
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

function renderReportPrecheck(precheck) {
    const container = document.getElementById("report-precheck");
    if (!container || !precheck) return;
    const status = precheck.status || "unchecked";
    const missing = precheck.missing_collectors || [];
    const available = precheck.available_collectors || [];
    const sourceCounts = precheck.source_counts || {};
    const recommendations = precheck.recommendations || [];
    const missingText = missing.length ? missing.map(labelCollector).join(" / ") : "None";
    const availableText = available.length ? available.map((collector) => {
        const count = sourceCounts[collector] || 0;
        return `${labelCollector(collector)}${count ? ` (${count})` : ""}`;
    }).join(" / ") : "None";
    container.style.display = "block";
    container.className = `report-precheck report-precheck-${status}`;
    container.innerHTML = `
        <div class="report-precheck-title">${escapeHtml(precheck.message || "Report precheck finished")}</div>
        <div class="report-precheck-grid">
            <span>Records</span><strong>${precheck.usable_records || 0}/${precheck.selected_records || 0}</strong>
            <span>Available</span><strong>${escapeHtml(availableText)}</strong>
            <span>Missing</span><strong>${escapeHtml(missingText)}</strong>
        </div>
        ${recommendations.length ? `<ul>${recommendations.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>` : ""}
    `;
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

    const requestPayload = {
        prompt,
        data_source: dataSource,
        template,
        record_keys: recordKeys,
        params: {},
    };

    currentReportProgressId = `report_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    resetReportProgress();
    const button = document.getElementById("btn-generate-report");
    if (button) {
        button.disabled = true;
        button.textContent = "Generating...";
    }

    try {
        setReportProgress(0.04, "precheck", "Checking report data coverage");
        const precheck = await api("/reports/precheck", {
            method: "POST",
            body: JSON.stringify(requestPayload),
        });
        renderReportPrecheck(precheck);
        if (precheck.status === "empty") {
            throw new Error(precheck.message || "No usable report data");
        }
        if (precheck.status === "partial") {
            const missing = (precheck.missing_collectors || []).map(labelCollector).join(" / ");
            const proceed = confirm(`Missing data sources: ${missing}. Generate report anyway?`);
            if (!proceed) {
                setReportProgress(0, "cancelled", "Report generation cancelled");
                return;
            }
        }
        setReportProgress(0.08, "requesting", "Sending report request");
        requestPayload.params = { progress_id: currentReportProgressId };
        const report = await api("/reports/generate-excel", {
            method: "POST",
            body: JSON.stringify(requestPayload),
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
        await api(`/reports/${encodeURIComponent(id)}?confirm=true`, { method: "DELETE" });
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
        await api(`/cron-jobs/${encodeURIComponent(name)}?confirm=true`, { method: "DELETE" });
        toast("Cron job deleted", "success");
        loadCronJobs();
    } catch (err) {
        toast(`Delete failed: ${err.message}`, "error");
    }
}

// ==================== 系统检查 ====================

async function loadSystemDiagnostics(options = {}) {
    try {
        const [health, diagnostics] = await Promise.all([
            api("/health"),
            api("/diagnostics/config"),
        ]);
        renderSystemStatus(health, diagnostics);
        renderSystemChecks(diagnostics.checks || []);
        renderSystemPaths(diagnostics.paths || {});
    } catch (err) {
        if (!options.silent) {
            toast(`系统检查失败: ${err.message}`, "error");
        }
        const list = document.getElementById("system-checks-list");
        if (list) {
            list.innerHTML = `<p class="text-muted">系统检查失败：${escapeHtml(err.message)}</p>`;
        }
    }
}

function renderSystemStatus(health, diagnostics) {
    const checks = diagnostics.checks || health.checks || [];
    const counts = checks.reduce((acc, check) => {
        acc[check.status] = (acc[check.status] || 0) + 1;
        return acc;
    }, {});
    const status = diagnostics.status || health.status || "unknown";

    setText("system-overall-status", status.toUpperCase());
    setText("system-error-count", counts.error || 0);
    setText("system-warning-count", counts.warning || 0);
    setText("system-ok-count", counts.ok || 0);

    const statusEl = document.getElementById("system-overall-status");
    if (statusEl) {
        statusEl.className = `stat-value system-status-${status}`;
    }
}

function renderSystemChecks(checks) {
    const list = document.getElementById("system-checks-list");
    if (!list) return;
    if (!checks.length) {
        list.innerHTML = '<p class="text-muted">暂无诊断项目</p>';
        return;
    }

    list.innerHTML = checks.map((check) => {
        const details = check.details && Object.keys(check.details).length
            ? `<pre class="system-check-details">${escapeHtml(JSON.stringify(check.details, null, 2))}</pre>`
            : "";
        return `
            <div class="system-check-row system-check-${escapeHtml(check.status)}">
                <div class="system-check-main">
                    <span class="system-check-status">${escapeHtml(check.status)}</span>
                    <div>
                        <div class="system-check-name">${escapeHtml(check.name)}</div>
                        <div class="system-check-message">${escapeHtml(check.message)}</div>
                    </div>
                </div>
                ${details}
            </div>
        `;
    }).join("");
}

function renderSystemPaths(paths) {
    const list = document.getElementById("system-paths-list");
    if (!list) return;
    const entries = Object.entries(paths);
    if (!entries.length) {
        list.innerHTML = '<p class="text-muted">暂无路径信息</p>';
        return;
    }
    list.innerHTML = entries.map(([key, value]) => `
        <div class="system-path-row">
            <span>${escapeHtml(key)}</span>
            <code>${escapeHtml(value)}</code>
        </div>
    `).join("");
}

// ==================== AI 助手 ====================

let agentSessionId = localStorage.getItem("agent_active_session") || "default";
let agentStreaming = false;
let trackedAgentTaskIds = new Set();

// --- Session management ---

function loadAgentSessions() {
    try {
        return JSON.parse(localStorage.getItem("agent_sessions") || "[]");
    } catch { return []; }
}

function saveAgentSessions(sessions) {
    localStorage.setItem("agent_sessions", JSON.stringify(sessions));
}

function ensureDefaultSession(sessions) {
    const hasDefault = sessions.some(s => s.id === "default");
    if (!hasDefault) {
        sessions.unshift({ id: "default", name: "默认会话", created_at: new Date().toISOString() });
    }
    return sessions;
}

function renderAgentSessions() {
    const listEl = document.getElementById("agent-session-list");
    if (!listEl) return;
    const sessions = loadAgentSessions();
    listEl.innerHTML = "";
    sessions.forEach(s => {
        const item = document.createElement("div");
        item.className = "agent-session-item" + (s.id === agentSessionId ? " active" : "");
        item.innerHTML = `
            <span class="agent-session-name" title="${escapeHtml(s.name)}">${escapeHtml(s.name)}</span>
            <button class="agent-session-delete" onclick="event.stopPropagation(); deleteAgentSession('${s.id}')" title="删除会话">&times;</button>
        `;
        item.onclick = () => switchAgentSession(s.id);
        listEl.appendChild(item);
    });
}

function createAgentSession() {
    const id = "sess_" + Date.now();
    const name = "会话 " + new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" });
    let sessions = loadAgentSessions();
    sessions.unshift({ id, name, created_at: new Date().toISOString() });
    saveAgentSessions(sessions);
    switchAgentSession(id);
    renderAgentSessions();
}

function switchAgentSession(id) {
    // Cache current session messages before switching
    cacheCurrentSessionMessages();

    agentSessionId = id;
    localStorage.setItem("agent_active_session", id);
    renderAgentSessions();

    // Clear chat and restore cached messages for the new session
    const container = document.getElementById("agent-messages");
    if (container) container.innerHTML = "";

    const cached = loadSessionMessages(id);
    if (cached.length > 0) {
        cached.forEach(m => appendAgentMessage(m.role, m.content));
    } else {
        appendAgentMessage("assistant", "你好！我是游戏数据助手，可以帮你：\n\n- 查看任务状态和系统概览\n- 创建数据采集任务\n- 配置 Pipeline 和定时任务\n- 浏览和搜索已采集数据\n- 生成数据分析报告\n\n请告诉我你需要什么帮助？");
    }
}

function deleteAgentSession(id) {
    let sessions = loadAgentSessions();
    if (sessions.length <= 1) {
        toast("至少保留一个会话", "error");
        return;
    }
    sessions = sessions.filter(s => s.id !== id);
    saveAgentSessions(sessions);

    // Delete messages cache
    localStorage.removeItem("agent_msgs_" + id);

    // Delete from server
    fetch(`/api/agent/history?session_id=${encodeURIComponent(id)}`, { method: "DELETE" }).catch(() => {});

    // If deleting current session, switch to first remaining
    if (id === agentSessionId) {
        switchAgentSession(sessions[0].id);
    }
    renderAgentSessions();
}

function loadSessionMessages(sessionId) {
    try {
        return JSON.parse(localStorage.getItem("agent_msgs_" + sessionId) || "[]");
    } catch { return []; }
}

function cacheAgentMessage(sessionId, role, content) {
    const key = "agent_msgs_" + sessionId;
    let msgs;
    try { msgs = JSON.parse(localStorage.getItem(key) || "[]"); } catch { msgs = []; }
    msgs.push({ role, content });
    if (msgs.length > 40) msgs = msgs.slice(-20);
    localStorage.setItem(key, JSON.stringify(msgs));
}

function cacheCurrentSessionMessages() {
    // No-op: messages are cached individually via cacheAgentMessage
}

// --- Provider selector ---

function providerLabel(key) { return key.charAt(0).toUpperCase() + key.slice(1); }

async function initAgentProviderSelector() {
    const select = document.getElementById("agent-provider-select");
    if (!select) return;

    try {
        const data = await api("/agent/providers");
        select.innerHTML = "";
        data.providers.forEach(p => {
            const opt = document.createElement("option");
            opt.value = p.key;
            opt.textContent = providerLabel(p.key) + " (" + p.model + ")";
            select.appendChild(opt);
        });
        select.value = data.active;

        // Restore from localStorage if available
        const saved = localStorage.getItem("agent_provider");
        if (saved && data.providers.some(p => p.key === saved)) {
            select.value = saved;
        }
    } catch {
        select.innerHTML = '<option value="">不可用</option>';
    }
}

async function onAgentProviderChange() {
    const select = document.getElementById("agent-provider-select");
    if (!select) return;
    const provider = select.value;
    const prevValue = localStorage.getItem("agent_provider") || "";

    try {
        await api("/agent/providers", {
            method: "POST",
            body: JSON.stringify({ provider }),
        });
        localStorage.setItem("agent_provider", provider);
        toast("已切换到 " + providerLabel(provider), "success");
    } catch (err) {
        toast("切换失败: " + err.message, "error");
        select.value = prevValue;
    }
}

// --- Provider config modal ---

async function showProviderConfigModal() {
    const modal = document.getElementById("modal-provider-config");
    if (!modal) return;

    try {
        const data = await api("/agent/providers/config");
        const listEl = document.getElementById("provider-config-list");
        if (listEl) listEl.innerHTML = "";
        data.providers.forEach(item => addProviderConfigRow(item));

        const defaultSelect = document.getElementById("provider-config-default");
        if (defaultSelect) {
            defaultSelect.innerHTML = "";
            data.providers.forEach(p => {
                const opt = document.createElement("option");
                opt.value = p.key;
                opt.textContent = providerLabel(p.key);
                defaultSelect.appendChild(opt);
            });
            defaultSelect.value = data.active || "";
        }
    } catch (err) {
        toast("加载配置失败: " + err.message, "error");
        return;
    }

    modal.classList.add("show");
}

function addProviderConfigRow(data) {
    const listEl = document.getElementById("provider-config-list");
    if (!listEl) return;

    data = data || { key: "", model: "", base_url: "", api_key: "", temperature: 0.3, max_tokens: 2000 };

    const row = document.createElement("div");
    row.className = "provider-config-row";
    row.innerHTML = `
        <div><label>Key</label><input type="text" class="prov-cfg-key" value="${escapeHtml(data.key || "")}" placeholder="qwen" ${data.key ? "readonly" : ""}></div>
        <div><label>模型</label><input type="text" class="prov-cfg-model" value="${escapeHtml(data.model || "")}" placeholder="qwen-max"></div>
        <div><label>Base URL</label><input type="text" class="prov-cfg-url" value="${escapeHtml(data.base_url || "")}" placeholder="https://..."></div>
        <div><label>API Key</label><input type="text" class="prov-cfg-keyval" value="${escapeHtml(data.api_key || "")}" placeholder="\${ENV_VAR} 或明文"></div>
        <div><button class="provider-config-delete" onclick="this.closest('.provider-config-row').remove()" title="删除">&times;</button></div>
    `;
    listEl.appendChild(row);

    // Update default select options whenever a row is added/removed
    refreshProviderDefaultSelect();
}

function refreshProviderDefaultSelect() {
    const defaultSelect = document.getElementById("provider-config-default");
    if (!defaultSelect) return;
    const currentVal = defaultSelect.value;
    const keys = Array.from(document.querySelectorAll(".prov-cfg-key")).map(el => el.value).filter(Boolean);
    defaultSelect.innerHTML = "";
    keys.forEach(k => {
        const opt = document.createElement("option");
        opt.value = k;
        opt.textContent = providerLabel(k);
        defaultSelect.appendChild(opt);
    });
    if (keys.includes(currentVal)) defaultSelect.value = currentVal;
}

async function saveProviderConfig() {
    const rows = document.querySelectorAll(".provider-config-row");
    const items = [];
    for (const row of rows) {
        const keyEl = row.querySelector(".prov-cfg-key");
        const modelEl = row.querySelector(".prov-cfg-model");
        const urlEl = row.querySelector(".prov-cfg-url");
        const keyvalEl = row.querySelector(".prov-cfg-keyval");
        if (!keyEl || !keyEl.value.trim() || !modelEl || !modelEl.value.trim()) continue;
        items.push({
            key: keyEl.value.trim(),
            model: modelEl.value.trim(),
            base_url: urlEl ? urlEl.value.trim() : "",
            api_key: keyvalEl ? keyvalEl.value.trim() : "",
            temperature: 0.3,
            max_tokens: 2000,
        });
    }

    if (items.length === 0) {
        toast("至少需要一个有效的 provider（key 和 model 必填）", "error");
        return;
    }

    const defaultProvider = document.getElementById("provider-config-default").value;

    try {
        await api("/agent/providers/config", {
            method: "PUT",
            body: JSON.stringify({ provider: defaultProvider, items }),
        });
        toast("配置已保存", "success");
        closeModal("modal-provider-config");
        // Refresh the provider dropdown
        initAgentProviderSelector();
    } catch (err) {
        toast("保存失败: " + err.message, "error");
    }
}

// --- Task progress tracking in agent ---

function renderAgentTaskProgressCard(taskId, taskName) {
    const container = document.getElementById("agent-messages");
    if (!container) return;

    const msgEl = document.createElement("div");
    msgEl.className = "agent-message assistant";

    const bubble = document.createElement("div");
    bubble.className = "agent-bubble assistant";

    const card = document.createElement("div");
    card.className = "agent-task-card";
    card.dataset.taskId = taskId;
    card.innerHTML = `
        <div class="agent-task-card-header">
            <span class="agent-task-card-name">${escapeHtml(taskName || taskId)}</span>
            <span class="badge badge-pending">pending</span>
        </div>
        <div class="agent-task-card-progress">
            <div class="agent-task-card-progress-fill"></div>
        </div>
        <div class="agent-task-card-logs"></div>
    `;

    bubble.appendChild(card);
    msgEl.appendChild(bubble);
    container.appendChild(msgEl);
    trackedAgentTaskIds.add(taskId);
    scrollAgentToBottom();
}

function updateAgentTaskCard(task) {
    const card = document.querySelector(`.agent-task-card[data-task-id="${task.id}"]`);
    if (!card) return;

    // Update badge
    const badge = card.querySelector(".badge");
    if (badge) {
        badge.className = `badge badge-${task.status}`;
        badge.textContent = task.status;
    }

    // Update progress bar
    const fill = card.querySelector(".agent-task-card-progress-fill");
    if (fill) {
        const pct = task.progress || 0;
        fill.style.width = pct + "%";
    }

    // Update logs (show last 5)
    const logsEl = card.querySelector(".agent-task-card-logs");
    if (logsEl && task.step_logs) {
        const recentLogs = task.step_logs.slice(-5);
        logsEl.innerHTML = recentLogs.map(log =>
            `<div class="agent-task-card-log-item">
                <span class="agent-task-card-log-time">${escapeHtml(log.time || "")}</span>
                <span>${escapeHtml(log.message || "")}</span>
            </div>`
        ).join("");
    }

    // Stop tracking on terminal status
    if (["success", "failed", "cancelled"].includes(task.status)) {
        trackedAgentTaskIds.delete(task.id);
    }

    scrollAgentToBottom();
}

// --- Core chat functions ---

function initAgentChat() {
    const input = document.getElementById("agent-input");
    if (input) {
        input.addEventListener("keydown", (e) => {
            if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
                e.preventDefault();
                sendAgentMessage();
            }
        });
    }

    // Initialize sessions
    let sessions = loadAgentSessions();
    sessions = ensureDefaultSession(sessions);
    saveAgentSessions(sessions);
    renderAgentSessions();

    // Restore active session
    const savedSession = localStorage.getItem("agent_active_session");
    if (savedSession && sessions.some(s => s.id === savedSession)) {
        agentSessionId = savedSession;
    } else {
        agentSessionId = "default";
        localStorage.setItem("agent_active_session", "default");
    }
    renderAgentSessions();

    // Load cached messages for active session
    const cached = loadSessionMessages(agentSessionId);
    if (cached.length > 0) {
        const container = document.getElementById("agent-messages");
        if (container) container.innerHTML = "";
        cached.forEach(m => appendAgentMessage(m.role, m.content));
    }

    // Initialize provider selector
    initAgentProviderSelector();
}

function appendAgentMessage(role, content) {
    const container = document.getElementById("agent-messages");
    if (!container) return;

    const msgEl = document.createElement("div");
    msgEl.className = `agent-message ${role}`;

    const avatar = document.createElement("div");
    avatar.className = `agent-avatar ${role}`;
    avatar.textContent = role === "user" ? "ME" : "AI";

    const bubble = document.createElement("div");
    bubble.className = `agent-bubble ${role}`;

    if (role === "assistant") {
        bubble.innerHTML = renderSafeMarkdown(content);
    } else {
        bubble.textContent = content;
    }

    msgEl.appendChild(avatar);
    msgEl.appendChild(bubble);
    container.appendChild(msgEl);
    scrollAgentToBottom();
}

function renderSafeMarkdown(content) {
    const text = String(content || "");
    if (typeof marked === "undefined") {
        return escapeHtml(text);
    }
    const rendered = marked.parse(text);
    if (typeof DOMPurify !== "undefined") {
        return DOMPurify.sanitize(rendered);
    }
    return escapeHtml(text);
}

// --- Structured streaming response ---

let currentResponseEl = null;      // .agent-message.assistant wrapper
let currentResponseSteps = null;   // .agent-response-steps container
let currentStepEl = null;          // current .agent-step
let currentThinkingDrawer = null;  // current .agent-thinking-drawer <details>
let currentThinkingBody = null;    // current thinking body div
let currentToolLine = null;        // current .agent-tool-line
let currentToolResult = null;      // current .agent-tool-result-inline
let agentFinalText = "";           // accumulated text for current segment
let currentTextBlock = null;       // current .agent-step-text inside a step

function createAgentResponseContainer() {
    const container = document.getElementById("agent-messages");
    if (!container) return null;

    const msgEl = document.createElement("div");
    msgEl.className = "agent-message assistant";

    const avatar = document.createElement("div");
    avatar.className = "agent-avatar assistant";
    avatar.textContent = "AI";

    const bubble = document.createElement("div");
    bubble.className = "agent-bubble assistant";

    // Response structure
    const respContainer = document.createElement("div");
    respContainer.className = "agent-response-container";

    // Steps area (tool calls + text segments all live here)
    currentResponseSteps = document.createElement("div");
    currentResponseSteps.className = "agent-response-steps";
    respContainer.appendChild(currentResponseSteps);

    // Status indicator
    const status = document.createElement("div");
    status.className = "agent-status-indicator";
    status.textContent = "思考中...";
    respContainer.appendChild(status);
    respContainer._statusEl = status;

    bubble.appendChild(respContainer);
    msgEl.appendChild(avatar);
    msgEl.appendChild(bubble);
    container.appendChild(msgEl);

    currentResponseEl = respContainer;
    agentFinalText = "";
    currentStepEl = null;
    currentTextBlock = null;
    currentThinkingDrawer = null;
    currentThinkingBody = null;
    currentToolLine = null;
    currentToolResult = null;

    scrollAgentToBottom();
    return respContainer;
}

function _ensureStep() {
    if (!currentStepEl) {
        currentStepEl = document.createElement("div");
        currentStepEl.className = "agent-step";
        currentResponseSteps.appendChild(currentStepEl);
        currentThinkingDrawer = null;
        currentThinkingBody = null;
        currentToolLine = null;
        currentToolResult = null;
    }
}

function _ensureThinkingDrawer() {
    _ensureStep();
    if (!currentThinkingDrawer) {
        const details = document.createElement("details");
        details.className = "agent-thinking-drawer";

        const summary = document.createElement("summary");
        summary.textContent = "思考过程";
        details.appendChild(summary);

        currentThinkingBody = document.createElement("div");
        currentThinkingBody.className = "agent-thinking-body";
        details.appendChild(currentThinkingBody);

        // Insert at top of step
        currentStepEl.insertBefore(details, currentStepEl.firstChild);
        currentThinkingDrawer = details;
    }
}

function _ensureToolLine(name, args) {
    _ensureStep();
    // Close previous thinking drawer
    if (currentThinkingDrawer) {
        currentThinkingDrawer.open = false;
        currentThinkingDrawer = null;
        currentThinkingBody = null;
    }

    currentToolLine = document.createElement("div");
    currentToolLine.className = "agent-tool-line";

    const badge = document.createElement("span");
    badge.className = "agent-tool-badge";
    const argsStr = typeof args === "object" ? JSON.stringify(args, null, 0) : String(args || "");
    const shortArgs = argsStr.length > 50 ? argsStr.substring(0, 50) + "..." : argsStr;
    badge.textContent = `⚙ ${name}(${shortArgs})`;
    badge.title = `${name}(${argsStr})`;
    currentToolLine.appendChild(badge);

    currentToolResult = document.createElement("span");
    currentToolResult.className = "agent-tool-result-inline";
    currentToolResult.textContent = "执行中...";
    currentToolLine.appendChild(currentToolResult);

    currentStepEl.appendChild(currentToolLine);
}

function _updateStatus(text) {
    if (currentResponseEl && currentResponseEl._statusEl) {
        currentResponseEl._statusEl.textContent = text;
    }
}

function _hideStatus() {
    if (currentResponseEl && currentResponseEl._statusEl) {
        currentResponseEl._statusEl.style.display = "none";
    }
}

function handleAgentEvent(event) {
    switch (event.type) {
        case "thinking": {
            if (!currentResponseEl) return;
            _updateStatus("思考中...");
            _ensureThinkingDrawer();
            const newContent = event.content || "";
            if (newContent && currentThinkingBody) {
                currentThinkingBody.textContent += newContent;
            }
            scrollAgentToBottom();
            break;
        }

        case "tool_call": {
            if (!currentResponseEl) return;
            // Close previous step's thinking
            if (currentThinkingDrawer) {
                currentThinkingDrawer.open = false;
                currentThinkingDrawer = null;
                currentThinkingBody = null;
            }
            // Finalize current text block before starting tool step
            currentTextBlock = null;
            // Start a new step
            currentStepEl = null;
            _ensureStep();
            _ensureToolLine(event.name, event.args);
            _updateStatus("执行工具: " + escapeHtml(event.name));
            scrollAgentToBottom();
            break;
        }

        case "tool_result": {
            if (!currentResponseEl) return;
            const content = event.content || "";
            // Update current tool result
            if (currentToolResult) {
                const truncated = content.length > 200 ? content.substring(0, 200) + "..." : content;
                currentToolResult.textContent = truncated;
                currentToolResult.title = content;
                if (content.includes("error") || content.includes("失败")) {
                    currentToolResult.classList.add("error");
                }
            }
            _updateStatus("");

            // Detect task creation
            _detectTaskCreation(content);

            // Reset text accumulator so next LLM output starts a new segment
            agentFinalText = "";
            currentTextBlock = null;

            scrollAgentToBottom();
            break;
        }

        case "final": {
            if (!currentResponseEl) return;
            _hideStatus();
            // Close any open thinking drawer when final answer starts
            if (currentThinkingDrawer) {
                currentThinkingDrawer.open = false;
                currentThinkingDrawer = null;
                currentThinkingBody = null;
            }

            const chunk = event.content || "";

            // Start a new text segment if needed
            if (!currentTextBlock) {
                currentStepEl = null;
                _ensureStep();
                currentToolLine = null;
                currentToolResult = null;

                currentTextBlock = document.createElement("div");
                currentTextBlock.className = "agent-step-text";
                currentStepEl.appendChild(currentTextBlock);
            }

            agentFinalText += chunk;
            currentTextBlock.innerHTML = renderSafeMarkdown(agentFinalText);
            scrollAgentToBottom();
            break;
        }

        case "error": {
            if (!currentResponseEl) return;
            _hideStatus();
            // Show error in its own step
            currentStepEl = null;
            currentTextBlock = null;
            _ensureStep();
            const errEl = document.createElement("div");
            errEl.className = "agent-step-text";
            errEl.style.color = "var(--danger)";
            errEl.textContent = "错误: " + event.content;
            currentStepEl.appendChild(errEl);
            scrollAgentToBottom();
            break;
        }
    }
}

function _detectTaskCreation(content) {
    try {
        let resultObj;
        try { resultObj = JSON.parse(content); } catch { resultObj = null; }
        if (resultObj && resultObj.task_id && resultObj.success) {
            const taskName = resultObj.task_name || resultObj.task_id;
            renderAgentTaskProgressCard(resultObj.task_id, taskName);
            return;
        }
        if (content.includes("task_id") && content.includes("success")) {
            const match = content.match(/"task_id"\s*:\s*"([^"]+)"/);
            if (match) renderAgentTaskProgressCard(match[1], match[1]);
        }
    } catch { /* ignore */ }
}

function clearAgentHistory() {
    fetch(`/api/agent/history?session_id=${encodeURIComponent(agentSessionId)}`, {
        method: "DELETE",
    }).catch(() => {});

    // Clear message cache for current session
    localStorage.removeItem("agent_msgs_" + agentSessionId);

    const container = document.getElementById("agent-messages");
    if (container) {
        container.innerHTML = "";
        appendAgentMessage("assistant", "对话已清空。请告诉我你需要什么帮助？");
    }
}

function _resetAgentStreamState() {
    currentResponseEl = null;
    currentResponseSteps = null;
    currentStepEl = null;
    currentTextBlock = null;
    currentThinkingDrawer = null;
    currentThinkingBody = null;
    currentToolLine = null;
    currentToolResult = null;
    agentFinalText = "";
}

function scrollAgentToBottom() {
    const container = document.getElementById("agent-messages");
    if (container) {
        container.scrollTop = container.scrollHeight;
    }
}

async function sendAgentMessage() {
    const input = document.getElementById("agent-input");
    const btn = document.getElementById("btn-send-agent");
    if (!input || agentStreaming) return;

    const message = input.value.trim();
    if (!message) return;

    input.value = "";
    appendAgentMessage("user", message);
    cacheAgentMessage(agentSessionId, "user", message);

    createAgentResponseContainer();
    agentStreaming = true;
    if (btn) btn.disabled = true;

    try {
        const response = await fetch("/api/agent/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message, session_id: agentSessionId }),
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: response.statusText }));
            appendAgentMessage("assistant", `请求失败: ${errorData.detail || response.status}`);
            return;
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });

            const lines = buffer.split("\n");
            buffer = lines.pop() || "";

            for (const line of lines) {
                if (line.startsWith("data: ")) {
                    try {
                        const event = JSON.parse(line.slice(6));
                        handleAgentEvent(event);
                    } catch (e) {
                        // 忽略解析错误
                    }
                }
            }
        }
    } catch (err) {
        appendAgentMessage("assistant", `连接出错: ${err.message}`);
    } finally {
        agentStreaming = false;
        if (btn) btn.disabled = false;
        if (agentFinalText) {
            cacheAgentMessage(agentSessionId, "assistant", agentFinalText);
        }
        _hideStatus();
        _resetAgentStreamState();
    }
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
    initAgentChat();
});
