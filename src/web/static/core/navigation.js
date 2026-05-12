// Core: Tab navigation, modal management, auto-refresh
// (shared globals like activeTab/autoRefreshHandle are declared in app.js)

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

function loadTabData(tab) {
    switch (tab) {
        case "dashboard": refreshDashboard(); break;
        case "tasks": loadTasks(); break;
        case "pipelines": loadComponents(); loadPipelines(); break;
        case "data": loadDataGames(); break;
        case "reports": loadReportTemplates(); loadDataGroups(); loadReports(); break;
        case "cron": loadCronJobs(); break;
        case "system": loadSystemDiagnostics(); break;
    }
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
        } else if (activeTab === "system") {
            loadSystemDiagnostics({ silent: true });
        }
    }, AUTO_REFRESH_INTERVAL_MS);
}

function openModal(id) {
    const modal = document.getElementById(id);
    if (modal) {
        modal.classList.add("show");
        setTimeout(() => {
            if (id === "modal-create-task" && typeof taskTargetsEditor !== "undefined" && taskTargetsEditor) {
                taskTargetsEditor.refresh();
            }
            if (id === "modal-create-pipeline" && typeof pipelineStepsEditor !== "undefined" && pipelineStepsEditor) {
                pipelineStepsEditor.refresh();
            }
        }, 10);
    }
}

function closeModal(id) {
    const modal = document.getElementById(id);
    if (modal) { modal.classList.remove("show"); }
}
