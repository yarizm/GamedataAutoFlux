// Core: API client + toast notifications + utility functions

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

function setText(id, value) {
    const el = document.getElementById(id);
    if (el) { el.textContent = value; }
}

function setValue(id, value) {
    const el = document.getElementById(id);
    if (el) { el.value = value; }
}

function setChecked(id, value) {
    const el = document.getElementById(id);
    if (el) { el.checked = value; }
}
