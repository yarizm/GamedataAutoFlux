"use strict";

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
            if (currentToolResult) {
                _renderStructuredToolResult(currentToolResult, content);
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

function _renderStructuredToolResult(el, content) {
    let parsed = null;
    try { parsed = JSON.parse(content); } catch { /* not JSON */ }

    if (parsed && parsed.status && parsed.summary) {
        const statusClass = { success: "success", ok: "success", error: "error", warning: "warning" }[parsed.status] || "";
        const statusLabel = { success: "成功", ok: "成功", error: "失败", warning: "警告" }[parsed.status] || parsed.status;

        let html = `<div class="tool-result-card ${statusClass}">`;
        html += `<span class="tool-result-status">${escapeHtml(statusLabel)}</span>`;
        html += `<span class="tool-result-summary">${escapeHtml(parsed.summary)}</span>`;

        if (parsed.record_count !== undefined) {
            html += `<span class="tool-result-count">${parsed.record_count} 条</span>`;
        }

        if (parsed.suggestion) {
            html += `<div class="tool-result-suggestion">建议: ${escapeHtml(parsed.suggestion)}</div>`;
        }

        if (parsed.data_truncated) {
            html += `<div class="tool-result-truncated">数据量过大已截断，请进一步查询</div>`;
        } else if (parsed.data !== undefined && parsed.data !== null) {
            const preview = typeof parsed.data === "string" ? parsed.data : JSON.stringify(parsed.data, null, 2);
            const safePreview = preview.length > 500 ? preview.substring(0, 500) + "..." : preview;
            html += `<details class="tool-result-data"><summary>数据预览 (${preview.length} 字符)</summary><pre>${escapeHtml(safePreview)}</pre></details>`;
        }

        if (parsed.warnings && parsed.warnings.length) {
            html += `<div class="tool-result-warnings">`;
            for (const w of parsed.warnings) html += `<div class="tool-result-warning-item">⚠ ${escapeHtml(w)}</div>`;
            html += `</div>`;
        }

        html += `</div>`;
        el.innerHTML = html;
    } else {
        const truncated = content.length > 300 ? content.substring(0, 300) + "..." : content;
        el.textContent = truncated;
        el.title = content;
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
