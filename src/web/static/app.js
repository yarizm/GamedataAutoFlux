const AUTO_REFRESH_INTERVAL_MS = 5000;

let activeTab = "dashboard";
let autoRefreshHandle = null;
let pipelineTemplates = [];
let availableComponents = {};

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

function openModal(id) {
    const modal = document.getElementById(id);
    if (modal) {
        modal.classList.add("show");
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
        case "reports":
            loadReports();
            break;
        case "cron":
            loadCronJobs();
            break;
        default:
            break;
    }
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

    return actions.join(" ");
}

function showCreateTaskModal() {
    loadPipelineSelect("task-pipeline");
    openModal("modal-create-task");
}

function buildTaskTargetsFromForm(targetName, appId, skipSteamdb, steamdbTimeSlice) {
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
    const pipelineName = document.getElementById("task-pipeline")?.value || "";
    const targetsRaw = document.getElementById("task-targets")?.value.trim() || "";
    const description = document.getElementById("task-desc")?.value.trim() || "";
    const targetName = document.getElementById("task-target-name")?.value.trim() || "";
    const appId = document.getElementById("task-app-id")?.value.trim() || "";
    const skipSteamdb = document.getElementById("task-skip-steamdb")?.checked || false;
    const steamdbTimeSlice = document.getElementById("task-steamdb-time-slice")?.value || "monthly_peak_1y";

    if (!name || !pipelineName) {
        toast("Task name and pipeline are required", "error");
        return;
    }

    let targets = buildTaskTargetsFromForm(targetName, appId, skipSteamdb, steamdbTimeSlice);
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

    try {
        await api("/tasks", {
            method: "POST",
            body: JSON.stringify({
                name,
                pipeline_name: pipelineName,
                targets,
                description,
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
    const stepsRaw = document.getElementById("pipeline-steps")?.value.trim() || "";

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
        const pipelines = await api("/pipelines");
        const select = document.getElementById(selectId);
        if (!select) return;

        const current = select.value;
        select.innerHTML = '<option value="">-- Select Pipeline --</option>';
        for (const name of Object.keys(pipelines)) {
            select.insertAdjacentHTML(
                "beforeend",
                `<option value="${name}" ${name === current ? "selected" : ""}>${name}</option>`
            );
        }
    } catch (err) {
        console.error("Load pipeline select failed:", err);
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
            <button class="report-item" onclick="viewReport('${report.id}')">
                <span class="report-item-title">${escapeHtml(report.title)}</span>
                <span class="report-item-meta">${formatTime(report.generated_at)} | ${escapeHtml(report.template)} | ${report.matched_records} records</span>
            </button>
        `).join("");
    } catch (err) {
        toast(`Load failed: ${err.message}`, "error");
    }
}

function renderReport(report) {
    const container = document.getElementById("report-content");
    if (container) {
        container.textContent = report.content || "";
    }
}

async function generateReport() {
    const prompt = document.getElementById("report-prompt")?.value.trim() || "";
    const dataSource = document.getElementById("report-data-source")?.value.trim() || "";
    const template = document.getElementById("report-template")?.value || "default";

    if (!prompt) {
        toast("Prompt is required", "error");
        return;
    }

    try {
        const report = await api("/reports/generate", {
            method: "POST",
            body: JSON.stringify({
                prompt,
                data_source: dataSource,
                template,
            }),
        });
        renderReport(report);
        loadReports();
        toast("Report generated", "success");
    } catch (err) {
        toast(`Generate failed: ${err.message}`, "error");
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
        } else if (activeTab === "cron") {
            loadCronJobs();
        }
    }, AUTO_REFRESH_INTERVAL_MS);
}

function bindNavigation() {
    document.querySelectorAll(".nav-link").forEach((link) => {
        link.addEventListener("click", (e) => {
            e.preventDefault();
            const tab = link.dataset.tab;
            activeTab = tab;

            document.querySelectorAll(".nav-link").forEach((item) => item.classList.remove("active"));
            link.classList.add("active");

            document.querySelectorAll(".tab-content").forEach((panel) => panel.classList.remove("active"));
            document.getElementById(`tab-${tab}`)?.classList.add("active");

            loadTabData(tab);
            restartAutoRefresh();
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
    bindNavigation();
    bindModalOverlayClose();
    refreshDashboard();
    loadTasks();
    restartAutoRefresh();
});
