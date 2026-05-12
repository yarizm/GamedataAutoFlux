// Core: WebSocket connection + event handlers
// (shared globals like wsConnection are declared in app.js)

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
    if (activeTab === "dashboard") { refreshDashboard(); }
    if (activeTab === "tasks") { loadTasks(); }
    if (activeTab === "agent" && trackedAgentTaskIds.has(task.id)) {
        updateAgentTaskCard(task);
    }

    const modalDetail = document.getElementById("modal-task-detail");
    if (modalDetail && modalDetail.classList.contains("show")) {
        const currentIdEl = document.querySelector("#task-detail-content .detail-kv code");
        if (currentIdEl && currentIdEl.textContent === task.id) {
            viewTaskDetail(task.id);
        }
    }

    const modalLogs = document.getElementById("modal-task-logs");
    if (modalLogs && modalLogs.classList.contains("show")) {
        if (modalLogs.dataset.taskId === task.id) {
            viewTaskLogs(task.id);
        }
    }
}

function handleStatsUpdate(stats) {
    if (activeTab === "dashboard") { refreshDashboard(); }
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
