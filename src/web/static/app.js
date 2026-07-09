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
    setChecked("pipeline-storage-sqlalchemy", template.steps.some((step) => step.type === "storage" && (step.name === "sqlalchemy" || step.name === "local")));
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
    if (document.getElementById("pipeline-storage-sqlalchemy")?.checked) {
        steps.push({ type: "storage", name: "sqlalchemy", config: {} });
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

    try {
        const gameKey = encodeURIComponent(selectedDataGame.game_key);
        const params = new URLSearchParams();
        params.set("page", String(page));
        params.set("page_size", String(pageSize));
        params.set("sort_order", sortOrder);
        if (source) params.set("source", source);
        const result = await api(`/data/games/${gameKey}/records?${params.toString()}`);
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

function batchAddToReport() {
    if (selectedDataRecordKeys.size === 0) return;
    let added = 0;
    for (const key of selectedDataRecordKeys) {
        const record = currentDataRecords.find((item) => item.key === key);
        if (record) {
            addReportRecordSelection(key, record);
            added++;
        }
    }
    if (added > 0) {
        syncSelectedReportRecordKeys();
        setValue("report-data-source", selectedDataGame?.game_name || "");
        setValue("report-prompt", `基于 ${selectedDataGame?.game_name || "所选游戏"} 的 ${added} 条数据生成综合分析报告。`);
        activateTab("reports");
        toast(`已添加 ${added} 条记录用于报告`, "success");
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

async function batchExportXlsx() {
    if (selectedDataRecordKeys.size === 0) return;
    const taskIds = new Set();
    const collectors = new Set();
    for (const key of selectedDataRecordKeys) {
        const record = currentDataRecords.find((item) => item.key === key);
        if (record?.task_id) {
            taskIds.add(record.task_id);
        } else {
            const parts = key.split(":");
            if (parts.length >= 1) taskIds.add(parts[0]);
        }
        if (record?.collector) collectors.add(record.collector);
    }
    if (collectors.size !== 1) {
        toast("Please select records from one YouTube collector", "error");
        return;
    }
    const collector = Array.from(collectors)[0] || "";
    if (!["youtube_profiles", "youtube_comments"].includes(collector)) {
        toast("XLSX export only supports YouTube profile and comment data", "error");
        return;
    }
    try {
        const resp = await api("/data/export/youtube", {
            method: "POST",
            body: JSON.stringify({
                collector,
                task_ids: Array.from(taskIds),
                format: "xlsx",
            }),
        });
        if (resp.download_url) {
            window.open(resp.download_url, "_blank");
            toast(`Exported ${resp.record_count} records`, "success");
        }
    } catch (err) {
        toast(`XLSX export failed: ${err.message}`, "error");
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
    window.open(`/api/data/records/download?key=${encodeURIComponent(key)}`, "_blank");
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
        const resp = await api(`/data/records/refresh?key=${encodeURIComponent(key)}`, {
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
        await api(`/data/records/refresh-schedules?key=${encodeURIComponent(key)}`, {
            method: "POST",
            body: JSON.stringify({ name, cron_expr: cronExpr, rolling_window: true }),
        });
        toast("Refresh schedule created", "success");
        loadCronJobs();
    } catch (err) {
        toast(`Schedule failed: ${err.message}`, "error");
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
        let actionBtn = "";
        if (check.details && check.details.action === "open_steamdb_browser") {
            actionBtn = `<div style="margin-top: 8px;"><button class="btn btn-primary btn-sm" onclick="launchSteamDBBrowser()">一键启动浏览器</button></div>`;
        }
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
                        ${actionBtn}
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

let _steamdbLaunching = false;
async function launchSteamDBBrowser() {
    if (_steamdbLaunching) return;
    _steamdbLaunching = true;
    toast("正在启动浏览器，请稍候...", "info");
    try {
        await api("/diagnostics/steamdb/launch", { method: "POST" });
        toast("启动命令已发送", "success");
        setTimeout(() => {
            loadSystemDiagnostics();
            _steamdbLaunching = false;
        }, 5000);
    } catch (err) {
        toast(`启动浏览器失败: ${err.message}`, "error");
        _steamdbLaunching = false;
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
