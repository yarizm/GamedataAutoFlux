"use strict";

// ==================== AI 助手 ====================

let agentSessionId = localStorage.getItem("agent_active_session") || "default";
let agentStreaming = false;
let trackedAgentTaskIds = new Set();
let abortController = null;
let currentResponseEvents = [];

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
            <button class="agent-session-edit" onclick="event.stopPropagation(); editAgentSession('${s.id}')" title="重命名">✎</button>
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

function editAgentSession(id) {
    let sessions = loadAgentSessions();
    const sess = sessions.find(s => s.id === id);
    if (!sess) return;
    const newName = prompt("修改会话标题", sess.name);
    if (newName === null) return;
    const trimmed = newName.trim();
    if (!trimmed) { toast("标题不能为空", "error"); return; }
    sess.name = trimmed;
    saveAgentSessions(sessions);
    renderAgentSessions();
}

function switchAgentSession(id) {
    if (agentStreaming && abortController) {
        if (!agentFinalText) agentFinalText = "*(已中止)*";
        cacheAgentMessage(agentSessionId, "assistant", agentFinalText,
            currentResponseEvents.length > 0 ? currentResponseEvents : undefined);
        agentFinalText = "";
        try { abortController.abort(); } catch (_) {}
        agentStreaming = false;
        abortController = null;
        const btn = document.getElementById("btn-send-agent");
        const btnStop = document.getElementById("btn-stop-agent");
        if (btn) btn.style.display = 'flex';
        if (btnStop) btnStop.style.display = 'none';
        _resetAgentStreamState();
    }
    
    agentSessionId = id;
    localStorage.setItem("agent_active_session", id);
    renderAgentSessions();

    const wrapper = document.getElementById("agent-messages");
    if (wrapper) {
        Array.from(wrapper.children).forEach(c => {
            if (c.classList.contains("agent-session-layer")) c.style.display = "none";
            else c.remove();
        });
        const layer = _getAgentLayer(id);
        if (layer) layer.style.display = "block";
    }
    scrollAgentToBottom();
}

function deleteAgentSession(id) {
    let sessions = loadAgentSessions();
    if (sessions.length <= 1) {
        toast("至少保留一个会话", "error");
        return;
    }
    sessions = sessions.filter(s => s.id !== id);
    saveAgentSessions(sessions);

    localStorage.removeItem("agent_msgs_" + id);
    const layer = document.getElementById("agent-layer-" + id);
    if (layer) layer.remove();

    fetch(`/api/agent/history?session_id=${encodeURIComponent(id)}`, { method: "DELETE" }).catch(() => {});

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

function cacheAgentMessage(sessionId, role, content, steps) {
    const key = "agent_msgs_" + sessionId;
    let msgs;
    try { msgs = JSON.parse(localStorage.getItem(key) || "[]"); } catch { msgs = []; }
    const entry = { role, content };
    if (steps && steps.length > 0) entry.steps = steps;
    msgs.push(entry);
    if (msgs.length > 40) msgs = msgs.slice(-20);
    localStorage.setItem(key, JSON.stringify(msgs));
}

function cacheCurrentSessionMessages() {
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

    data = data || { key: "", model: "", base_url: "", api_key: "", has_api_key: false, temperature: 0.3, max_tokens: 2000 };
    const apiKeyPlaceholder = data.has_api_key ? "留空表示保留当前密钥" : "${ENV_VAR} 或明文";

    const row = document.createElement("div");
    row.className = "provider-config-row";
    row.innerHTML = `
        <div><label>Key</label><input type="text" class="prov-cfg-key" value="${escapeHtml(data.key || "")}" placeholder="qwen" ${data.key ? "readonly" : ""}></div>
        <div><label>模型</label><input type="text" class="prov-cfg-model" value="${escapeHtml(data.model || "")}" placeholder="qwen-max"></div>
        <div><label>Base URL</label><input type="text" class="prov-cfg-url" value="${escapeHtml(data.base_url || "")}" placeholder="https://..."></div>
        <div><label>API Key</label><input type="text" class="prov-cfg-keyval" value="${escapeHtml(data.api_key || "")}"></div>
        <div><button class="provider-config-delete" onclick="this.closest('.provider-config-row').remove()" title="删除">&times;</button></div>
    `;
    listEl.appendChild(row);
    const apiKeyInput = row.querySelector(".prov-cfg-keyval");
    if (apiKeyInput) apiKeyInput.placeholder = apiKeyPlaceholder;

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
        initAgentProviderSelector();
    } catch (err) {
        toast("保存失败: " + err.message, "error");
    }
}

// --- Task progress tracking in agent ---

function renderAgentTaskProgressCard(taskId, taskName) {
    const container = _getAgentLayer(agentSessionId);
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

    const badge = card.querySelector(".badge");
    if (badge) {
        badge.className = `badge badge-${task.status}`;
        badge.textContent = task.status;
    }

    const fill = card.querySelector(".agent-task-card-progress-fill");
    if (fill) {
        const pct = task.progress || 0;
        fill.style.width = pct + "%";
    }

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

    let sessions = loadAgentSessions();
    sessions = ensureDefaultSession(sessions);
    saveAgentSessions(sessions);
    renderAgentSessions();

    const savedSession = localStorage.getItem("agent_active_session");
    if (savedSession && sessions.some(s => s.id === savedSession)) {
        agentSessionId = savedSession;
    } else {
        agentSessionId = "default";
        localStorage.setItem("agent_active_session", "default");
    }
    renderAgentSessions();

    switchAgentSession(agentSessionId);
    initAgentProviderSelector();
}

function replayStructuredMessage(msg, container) {
    if (!container) return;
    const wasStreaming = agentStreaming;
    agentStreaming = true;

    // Build response container DOM (mirrors createAgentResponseContainer)
    const msgEl = document.createElement("div");
    msgEl.className = "agent-message assistant";
    const avatar = document.createElement("div");
    avatar.className = "agent-avatar assistant";
    avatar.textContent = "AI";
    const bubble = document.createElement("div");
    bubble.className = "agent-bubble assistant";
    const resp = document.createElement("div");
    resp.className = "agent-response-container";
    const steps = document.createElement("div");
    steps.className = "agent-response-steps";
    resp.appendChild(steps);
    const status = document.createElement("div");
    status.className = "agent-status-indicator";
    status.textContent = "已完成";
    resp.appendChild(status);
    resp._statusEl = status;
    bubble.appendChild(resp);
    msgEl.appendChild(avatar);
    msgEl.appendChild(bubble);
    container.appendChild(msgEl);

    // Set globals so handleAgentEvent works
    currentResponseEl = resp;
    currentResponseSteps = steps;
    currentStepEl = null;
    currentThinkingDrawer = null;
    currentThinkingBody = null;
    currentToolLine = null;
    currentToolResult = null;
    agentFinalText = "";

    for (const event of msg.steps) {
        handleAgentEvent(event);
    }

    _hideStatus();
    _resetAgentStreamState();
    agentStreaming = wasStreaming;
    scrollAgentToBottom();
}

function _getAgentLayer(id) {
    const targetId = id || agentSessionId;
    const wrapper = document.getElementById("agent-messages");
    if (!wrapper) return null;
    let layer = document.getElementById("agent-layer-" + targetId);
    if (!layer) {
        layer = document.createElement("div");
        layer.id = "agent-layer-" + targetId;
        layer.className = "agent-session-layer w-full";
        wrapper.appendChild(layer);
        const cached = loadSessionMessages(targetId);
        if (cached.length > 0) {
            cached.forEach((m) => {
                if (m.steps && m.steps.length > 0) {
                    replayStructuredMessage(m, layer);
                } else {
                    appendAgentMessage(m.role, m.content, layer);
                }
            });
        } else {
            appendAgentMessage("assistant", "你好！我是 GamedataAutoFlux 智能助手。\n\n我可以帮你：\n- 查询或筛选历史采集数据\n- 解释数据趋势和字段含义\n- 配置或调整数据采集 pipeline\n- 诊断采集任务的错误日志", layer);
        }
    }
    return layer;
}

function appendAgentMessage(role, content, targetLayer) {
    const container = targetLayer || _getAgentLayer(agentSessionId);
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

let currentResponseEl = null;
let currentResponseSteps = null;
let currentStepEl = null;
let currentThinkingDrawer = null;
let currentThinkingBody = null;
let currentToolLine = null;
let currentToolResult = null;
let agentFinalText = "";
let currentTextBlock = null;

function createAgentResponseContainer() {
    const container = _getAgentLayer(agentSessionId);
    if (!container) return null;

    const msgEl = document.createElement("div");
    msgEl.className = "agent-message assistant";

    const avatar = document.createElement("div");
    avatar.className = "agent-avatar assistant";
    avatar.textContent = "AI";

    const bubble = document.createElement("div");
    bubble.className = "agent-bubble assistant";

    const respContainer = document.createElement("div");
    respContainer.className = "agent-response-container";

    currentResponseSteps = document.createElement("div");
    currentResponseSteps.className = "agent-response-steps";
    respContainer.appendChild(currentResponseSteps);

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
    currentResponseEvents = [];

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

        currentStepEl.insertBefore(details, currentStepEl.firstChild);
        currentThinkingDrawer = details;
    }
}

function _ensureToolLine(name, args) {
    _ensureStep();
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
            // Track: merge consecutive thinking
            const lastThink = currentResponseEvents[currentResponseEvents.length - 1];
            if (lastThink && lastThink.type === "thinking") {
                lastThink.content += newContent;
            } else {
                currentResponseEvents.push({type: "thinking", content: newContent});
            }
            scrollAgentToBottom();
            break;
        }

        case "tool_call": {
            if (!currentResponseEl) return;
            if (currentThinkingDrawer) {
                currentThinkingDrawer.open = false;
                currentThinkingDrawer = null;
                currentThinkingBody = null;
            }
            currentTextBlock = null;
            currentStepEl = null;
            _ensureStep();
            _ensureToolLine(event.name, event.args);
            _updateStatus("执行工具: " + escapeHtml(event.name));
            currentResponseEvents.push({type: "tool_call", name: event.name, args: event.args});
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

            _detectTaskCreation(content);

            agentFinalText = "";
            currentTextBlock = null;
            currentResponseEvents.push({type: "tool_result", name: event.name, content: content});

            scrollAgentToBottom();
            break;
        }

        case "final": {
            if (!currentResponseEl) return;
            _hideStatus();
            if (currentThinkingDrawer) {
                currentThinkingDrawer.open = false;
                currentThinkingDrawer = null;
                currentThinkingBody = null;
            }

            const chunk = event.content || "";

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
            // Track: merge consecutive final
            const lastFinal = currentResponseEvents[currentResponseEvents.length - 1];
            if (lastFinal && lastFinal.type === "final") {
                lastFinal.content += chunk;
            } else {
                currentResponseEvents.push({type: "final", content: chunk});
            }
            scrollAgentToBottom();
            break;
        }

        case "error": {
            if (!currentResponseEl) return;
            _hideStatus();
            currentStepEl = null;
            currentTextBlock = null;
            _ensureStep();
            const errEl = document.createElement("div");
            errEl.className = "agent-step-text";
            errEl.style.color = "var(--danger)";
            errEl.textContent = "错误: " + event.content;
            currentStepEl.appendChild(errEl);
            currentResponseEvents.push({type: "error", content: event.content || ""});
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

async function clearAgentHistory() {
    fetch(`/api/agent/history?session_id=${encodeURIComponent(agentSessionId)}`, {
        method: "DELETE",
    }).catch(() => {});
    localStorage.removeItem("agent_msgs_" + agentSessionId);
    const container = _getAgentLayer(agentSessionId);
    if (container) {
        container.innerHTML = "";
        appendAgentMessage("assistant", "对话历史已清空", container);
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
    currentResponseEvents = [];
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
    const btnStop = document.getElementById("btn-stop-agent");
    if (!input || agentStreaming) return;

    const message = input.value.trim();
    if (!message) return;

    input.value = "";
    appendAgentMessage("user", message);
    cacheAgentMessage(agentSessionId, "user", message);

    createAgentResponseContainer();
    agentStreaming = true;
    if (btn) btn.style.display = 'none';
    if (btnStop) btnStop.style.display = 'flex';

    if (abortController) {
        try { abortController.abort(); } catch (_) {}
    }
    abortController = new AbortController();
    const currentFetchSessionId = agentSessionId;

    try {
        const response = await fetch("/api/agent/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message, session_id: agentSessionId }),
            signal: abortController.signal,
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
        if (err.name === "AbortError") {
            if (currentResponseEl?._statusEl) currentResponseEl._statusEl.textContent = "已停止生成";
            if (!agentFinalText) agentFinalText = "*(已手动中止)*";
        } else {
            appendAgentMessage("assistant", `连接出错: ${err.message}`, _getAgentLayer(currentFetchSessionId));
        }
    } finally {
        agentStreaming = false;
        const btn = document.getElementById("btn-send-agent");
        const btnStop = document.getElementById("btn-stop-agent");
        if (btn) btn.style.display = 'flex';
        if (btnStop) btnStop.style.display = 'none';
        abortController = null;
        if (currentResponseEvents.length > 0) {
            cacheAgentMessage(currentFetchSessionId, "assistant", agentFinalText, currentResponseEvents);
        } else if (agentFinalText) {
            cacheAgentMessage(currentFetchSessionId, "assistant", agentFinalText);
        }
        
        if (currentFetchSessionId === agentSessionId) {
            _hideStatus();
        } else {
            if (currentResponseEl?._statusEl) currentResponseEl._statusEl.style.display = 'none';
        }
        _resetAgentStreamState();
    }
}

function stopAgentMessage() {
    if (agentStreaming && abortController) {
        try { abortController.abort(); } catch (_) {}
    }
}
