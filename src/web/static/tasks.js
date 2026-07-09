"use strict";

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
    currentWizardStep = 1;
    updateWizardUI();
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
    const youtubeProfilesFields = document.getElementById("task-youtube-profiles-fields");
    const youtubeCommentsFields = document.getElementById("task-youtube-comments-fields");
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
    if (youtubeProfilesFields) {
        youtubeProfilesFields.style.display = collector === "youtube_profiles" ? "block" : "none";
    }
    if (youtubeCommentsFields) {
        youtubeCommentsFields.style.display = collector === "youtube_comments" ? "block" : "none";
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
        } else if (collector === "youtube_profiles") {
            helper.textContent = "YouTube profile tasks use an imported TXT list of channel URLs, IDs, or handles.";
        } else if (collector === "youtube_comments") {
            helper.textContent = "YouTube comment tasks use an imported TXT list of video URLs.";
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

    if (collector === "youtube_profiles" || collector === "youtube_comments") {
        return window._importedYouTubeTargetsByCollector?.[collector] || [];
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

let currentWizardStep = 1;

function updateWizardUI() {
    const steps = document.querySelectorAll("#task-wizard-steps .wizard-step");
    steps.forEach((el) => {
        const s = parseInt(el.dataset.step);
        el.classList.remove("active", "done");
        if (s < currentWizardStep) el.classList.add("done");
        if (s === currentWizardStep) el.classList.add("active");
    });
    document.querySelectorAll(".wizard-panel").forEach((el) => {
        el.style.display = parseInt(el.dataset.panel) === currentWizardStep ? "" : "none";
    });
    const back = document.getElementById("btn-wizard-back");
    const next = document.getElementById("btn-wizard-next");
    const submit = document.getElementById("btn-submit-task");
    if (back) back.style.display = currentWizardStep > 1 ? "" : "none";
    if (next) next.style.display = currentWizardStep < 3 ? "" : "none";
    if (submit) submit.style.display = currentWizardStep === 3 ? "" : "none";
}

function wizardNext() {
    if (currentWizardStep === 1) {
        const pipeline = document.getElementById("task-pipeline")?.value;
        if (!pipeline) { toast("请选择 Pipeline", "error"); return; }
        updateTaskTargetFields();
    }
    if (currentWizardStep < 3) {
        currentWizardStep++;
        updateWizardUI();
    }
}

function wizardPrev() {
    if (currentWizardStep > 1) {
        currentWizardStep--;
        updateWizardUI();
    }
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

window._importedYouTubeTargetsByCollector = window._importedYouTubeTargetsByCollector || {};
window._importedYouTubeTargets = window._importedYouTubeTargets || [];

async function importYouTubeTargets(collector, targetType) {
    const inputId = collector === "youtube_profiles" ? "task-yt-profiles-txt" : "task-yt-comments-txt";
    const previewId = collector === "youtube_profiles" ? "task-yt-profiles-preview" : "task-yt-comments-preview";
    const input = document.getElementById(inputId);
    const preview = document.getElementById(previewId);
    if (!input?.files?.length) return;

    const formData = new FormData();
    formData.append("file", input.files[0]);
    formData.append("collector_name", collector);
    formData.append("target_type", targetType);

    try {
        const response = await fetch("/api/tasks/import-targets", {
            method: "POST",
            body: formData,
        });
        if (!response.ok) {
            const err = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(err.detail || `HTTP ${response.status}`);
        }
        const resp = await response.json();
        const targets = resp.targets || [];
        window._importedYouTubeTargetsByCollector[collector] = targets;
        window._importedYouTubeTargets = targets;
        if (preview) {
            const skipped = resp.skipped > 0 ? `, skipped ${resp.skipped} lines` : "";
            const reasons = resp.skipped_reasons?.length
                ? `<br><span class="text-muted">${resp.skipped_reasons.slice(0, 3).map(escapeHtml).join("<br>")}</span>`
                : "";
            preview.style.display = "block";
            preview.className = "mt-3 rounded-lg bg-emerald-500/10 border border-emerald-500/20 p-3 text-xs text-emerald-300";
            preview.innerHTML = `Imported <strong>${resp.total}</strong> targets${skipped}${reasons}`;
        }
        toast(`Imported ${resp.total} YouTube targets`, "success");
    } catch (err) {
        window._importedYouTubeTargetsByCollector[collector] = [];
        window._importedYouTubeTargets = [];
        if (preview) {
            preview.style.display = "block";
            preview.className = "mt-3 rounded-lg bg-rose-500/10 border border-rose-500/20 p-3 text-xs text-rose-300";
            preview.textContent = `Import failed: ${err.message || "unknown error"}`;
        }
        toast(`YouTube targets import failed: ${err.message}`, "error");
    }
}
