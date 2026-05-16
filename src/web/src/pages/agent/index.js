import { api, toast, escapeHtml } from '../../core/api.js';
import { marked } from 'marked';
import DOMPurify from 'dompurify';
import { t } from '../../core/i18n.js';

// ── State ──
let agentSessionId = localStorage.getItem('agent_active_session') || 'default';
let agentStreaming = false;
let trackedAgentTaskIds = new Set();
let currentResponseEl = null;
let currentResponseSteps = null;
let currentStepEl = null;
let currentThinkingDrawer = null;
let currentThinkingBody = null;
let currentToolLine = null;
let currentToolResult = null;
let agentFinalText = '';
let currentTextBlock = null;

// ── Helpers ──
function providerLabel(key) { return key.charAt(0).toUpperCase() + key.slice(1); }

function scrollToBottom() {
  const c = document.getElementById('agent-messages');
  if (c) c.scrollTop = c.scrollHeight;
}

function renderSafeMarkdown(content) {
  const text = String(content || '');
  try {
    return DOMPurify.sanitize(marked.parse(text));
  } catch { return escapeHtml(text); }
}

function resetStreamState() {
  currentResponseEl = null; currentResponseSteps = null; currentStepEl = null;
  currentTextBlock = null; currentThinkingDrawer = null; currentThinkingBody = null;
  currentToolLine = null; currentToolResult = null; agentFinalText = '';
}

// ── Session storage ──
function loadSessions() { try { return JSON.parse(localStorage.getItem('agent_sessions') || '[]'); } catch { return []; } }
function saveSessions(s) { localStorage.setItem('agent_sessions', JSON.stringify(s)); }
function loadSessionMessages(sid) { try { return JSON.parse(localStorage.getItem('agent_msgs_' + sid) || '[]'); } catch { return []; } }

function cacheAgentMessage(sid, role, content) {
  const key = 'agent_msgs_' + sid;
  let msgs; try { msgs = JSON.parse(localStorage.getItem(key) || '[]'); } catch { msgs = []; }
  msgs.push({ role, content });
  if (msgs.length > 40) msgs = msgs.slice(-20);
  localStorage.setItem(key, JSON.stringify(msgs));
}

export default {
  init(container, store) {
    this.container = container;
    this.store = store;
    this._unsub = store.subscribe((key, value) => {
      if (key === 'taskUpdate' && trackedAgentTaskIds.has(value?.id)) {
        this._updateTaskCard(value);
      }
    });
    this._init();
    return this;
  },

  destroy() { if (this._unsub) this._unsub(); },

  _init() {
    const input = document.getElementById('agent-input');
    if (input) {
      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
          e.preventDefault(); this._send();
        }
      });
    }

    let sessions = loadSessions();
    if (!sessions.some(s => s.id === 'default')) {
      sessions.unshift({ id: 'default', name: '默认会话', created_at: new Date().toISOString() });
    }
    saveSessions(sessions);

    const saved = localStorage.getItem('agent_active_session');
    if (saved && sessions.some(s => s.id === saved)) agentSessionId = saved;
    else { agentSessionId = 'default'; localStorage.setItem('agent_active_session', 'default'); }

    this._renderSessions();
    this._restoreMessages();
    this._initProviderSelector();
  },

  // ── Sessions ──

  _renderSessions() {
    const listEl = document.getElementById('agent-session-list');
    if (!listEl) return;
    const sessions = loadSessions();
    listEl.innerHTML = '';
    sessions.forEach(s => {
      const item = document.createElement('div');
      item.className = 'agent-session-item' + (s.id === agentSessionId ? ' active' : '');
      item.innerHTML = `<span class="agent-session-name" title="${escapeHtml(s.name)}">${escapeHtml(s.name)}</span>
        <button class="agent-session-delete" title="${t('common.delete')}">&times;</button>`;
      item.querySelector('button').addEventListener('click', (e) => { e.stopPropagation(); this._deleteSession(s.id); });
      item.addEventListener('click', () => this._switchSession(s.id));
      listEl.appendChild(item);
    });
  },

  _createSession() {
    const id = 'sess_' + Date.now();
    const name = '会话 ' + new Date().toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
    let sessions = loadSessions();
    sessions.unshift({ id, name, created_at: new Date().toISOString() });
    saveSessions(sessions);
    this._switchSession(id);
    this._renderSessions();
  },

  _switchSession(id) {
    agentSessionId = id;
    localStorage.setItem('agent_active_session', id);
    this._renderSessions();
    const container = document.getElementById('agent-messages');
    if (container) container.innerHTML = '';
    const cached = loadSessionMessages(id);
    if (cached.length > 0) cached.forEach(m => this._appendMessage(m.role, m.content));
    else this._appendMessage('assistant', `${t('agent.welcome')}\n\n- ${t('agent.help.status')}\n- ${t('agent.help.create')}\n- ${t('agent.help.pipeline')}\n- ${t('agent.help.data')}\n- ${t('agent.help.report')}\n\n${t('agent.help.ask')}`);
  },

  _deleteSession(id) {
    let sessions = loadSessions();
    if (sessions.length <= 1) { toast('至少保留一个会话', 'error'); return; }
    sessions = sessions.filter(s => s.id !== id);
    saveSessions(sessions);
    localStorage.removeItem('agent_msgs_' + id);
    fetch(`/api/agent/history?session_id=${encodeURIComponent(id)}`, { method: 'DELETE' }).catch(() => {});
    if (id === agentSessionId) this._switchSession(sessions[0].id);
    this._renderSessions();
  },

  _restoreMessages() {
    const cached = loadSessionMessages(agentSessionId);
    if (cached.length > 0) {
      const container = document.getElementById('agent-messages');
      if (container) container.innerHTML = '';
      cached.forEach(m => this._appendMessage(m.role, m.content));
    }
  },

  // ── Provider ──

  async _initProviderSelector() {
    const select = document.getElementById('agent-provider-select');
    if (!select) return;
    try {
      const data = await api('/agent/providers');
      select.innerHTML = '';
      data.providers.forEach(p => {
        const opt = document.createElement('option');
        opt.value = p.key; opt.textContent = providerLabel(p.key) + ' (' + p.model + ')';
        select.appendChild(opt);
      });
      select.value = data.active;
      const saved = localStorage.getItem('agent_provider');
      if (saved && data.providers.some(p => p.key === saved)) select.value = saved;
    } catch { select.innerHTML = '<option value="">不可用</option>'; }
  },

  async _onProviderChange() {
    const select = document.getElementById('agent-provider-select');
    if (!select) return;
    const provider = select.value;
    const prev = localStorage.getItem('agent_provider') || '';
    try {
      await api('/agent/providers', { method: 'POST', body: JSON.stringify({ provider }) });
      localStorage.setItem('agent_provider', provider);
      toast('已切换到 ' + providerLabel(provider), 'success');
    } catch (err) { toast(t('message.loadFailed', { error: err.message }), 'error'); select.value = prev; }
  },

  // ── Provider Config Modal ──

  async _showProviderConfig() {
    const modal = document.getElementById('modal-provider-config');
    if (!modal) return;
    try {
      const data = await api('/agent/providers/config');
      const listEl = document.getElementById('provider-config-list');
      if (listEl) listEl.innerHTML = '';
      data.providers.forEach(item => this._addConfigRow(item));
      const defSel = document.getElementById('provider-config-default');
      if (defSel) {
        defSel.innerHTML = '';
        data.providers.forEach(p => {
          const opt = document.createElement('option');
          opt.value = p.key; opt.textContent = providerLabel(p.key);
          defSel.appendChild(opt);
        });
        defSel.value = data.active || '';
      }
    } catch (err) { toast(t('message.loadFailed', { error: err.message }), 'error'); return; }
    modal.classList.add('show');
  },

  _addConfigRow(data) {
    const listEl = document.getElementById('provider-config-list');
    if (!listEl) return;
    data = data || { key: '', model: '', base_url: '', api_key: '', temperature: 0.3, max_tokens: 2000 };
    const row = document.createElement('div');
    row.className = 'provider-config-row';
    row.innerHTML = `
      <div><label>Key</label><input type="text" class="prov-cfg-key" value="${escapeHtml(data.key || '')}" placeholder="qwen" ${data.key ? 'readonly' : ''}></div>
      <div><label>模型</label><input type="text" class="prov-cfg-model" value="${escapeHtml(data.model || '')}" placeholder="qwen-max"></div>
      <div><label>Base URL</label><input type="text" class="prov-cfg-url" value="${escapeHtml(data.base_url || '')}" placeholder="https://..."></div>
      <div><label>API Key</label><input type="text" class="prov-cfg-keyval" value="${escapeHtml(data.api_key || '')}" placeholder="\${ENV_VAR} 或明文"></div>
      <div><button class="provider-config-delete" title="${t('common.delete')}">&times;</button></div>`;
    row.querySelector('.provider-config-delete').addEventListener('click', () => { row.remove(); this._refreshDefaultSelect(); });
    listEl.appendChild(row);
    this._refreshDefaultSelect();
  },

  _refreshDefaultSelect() {
    const defSel = document.getElementById('provider-config-default');
    if (!defSel) return;
    const cur = defSel.value;
    const keys = Array.from(document.querySelectorAll('.prov-cfg-key')).map(el => el.value).filter(Boolean);
    defSel.innerHTML = '';
    keys.forEach(k => { const opt = document.createElement('option'); opt.value = k; opt.textContent = providerLabel(k); defSel.appendChild(opt); });
    if (keys.includes(cur)) defSel.value = cur;
  },

  async _saveProviderConfig() {
    const rows = document.querySelectorAll('.provider-config-row');
    const items = [];
    for (const row of rows) {
      const keyEl = row.querySelector('.prov-cfg-key'), modelEl = row.querySelector('.prov-cfg-model');
      const urlEl = row.querySelector('.prov-cfg-url'), keyvalEl = row.querySelector('.prov-cfg-keyval');
      if (!keyEl?.value.trim() || !modelEl?.value.trim()) continue;
      items.push({ key: keyEl.value.trim(), model: modelEl.value.trim(), base_url: urlEl?.value.trim() || '', api_key: keyvalEl?.value.trim() || '', temperature: 0.3, max_tokens: 2000 });
    }
    if (!items.length) { toast('至少需要一个有效的 provider（key 和 model 必填）', 'error'); return; }
    try {
      await api('/agent/providers/config', { method: 'PUT', body: JSON.stringify({ provider: document.getElementById('provider-config-default').value, items }) });
      toast(t('common.save'), 'success');
      window.closeModal('modal-provider-config');
      this._initProviderSelector();
    } catch (err) { toast(t('message.editFailed', { error: err.message }), 'error'); }
  },

  // ── Messages ──

  _appendMessage(role, content) {
    const container = document.getElementById('agent-messages');
    if (!container) return;
    const msgEl = document.createElement('div');
    msgEl.className = `agent-message ${role}`;
    const avatar = document.createElement('div');
    avatar.className = `agent-avatar ${role}`;
    avatar.textContent = role === 'user' ? 'ME' : 'AI';
    const bubble = document.createElement('div');
    bubble.className = `agent-bubble ${role}`;
    bubble.innerHTML = role === 'assistant' ? renderSafeMarkdown(content) : '';
    if (role === 'user') bubble.textContent = content;
    msgEl.appendChild(avatar); msgEl.appendChild(bubble);
    container.appendChild(msgEl);
    scrollToBottom();
  },

  // ── Structured SSE streaming ──

  _createResponseContainer() {
    const container = document.getElementById('agent-messages');
    if (!container) return null;
    const msgEl = document.createElement('div'); msgEl.className = 'agent-message assistant';
    const avatar = document.createElement('div'); avatar.className = 'agent-avatar assistant'; avatar.textContent = 'AI';
    const bubble = document.createElement('div'); bubble.className = 'agent-bubble assistant';
    const resp = document.createElement('div'); resp.className = 'agent-response-container';
    currentResponseSteps = document.createElement('div'); currentResponseSteps.className = 'agent-response-steps';
    resp.appendChild(currentResponseSteps);
    const status = document.createElement('div'); status.className = 'agent-status-indicator'; status.textContent = t('agent.thinking');
    resp.appendChild(status); resp._statusEl = status;
    bubble.appendChild(resp); msgEl.appendChild(avatar); msgEl.appendChild(bubble);
    container.appendChild(msgEl);
    currentResponseEl = resp; agentFinalText = '';
    currentStepEl = null; currentTextBlock = null;
    currentThinkingDrawer = null; currentThinkingBody = null;
    currentToolLine = null; currentToolResult = null;
    scrollToBottom();
    return resp;
  },

  _ensureStep() {
    if (!currentStepEl) {
      currentStepEl = document.createElement('div'); currentStepEl.className = 'agent-step';
      currentResponseSteps.appendChild(currentStepEl);
      currentThinkingDrawer = null; currentThinkingBody = null;
      currentToolLine = null; currentToolResult = null;
    }
  },

  _ensureThinking() {
    this._ensureStep();
    if (!currentThinkingDrawer) {
      const details = document.createElement('details'); details.className = 'agent-thinking-drawer';
      const summary = document.createElement('summary'); summary.textContent = t('agent.thinkingProcess');
      details.appendChild(summary);
      currentThinkingBody = document.createElement('div'); currentThinkingBody.className = 'agent-thinking-body';
      details.appendChild(currentThinkingBody);
      currentStepEl.insertBefore(details, currentStepEl.firstChild);
      currentThinkingDrawer = details;
    }
  },

  _ensureToolLine(name, args) {
    this._ensureStep();
    if (currentThinkingDrawer) { currentThinkingDrawer.open = false; currentThinkingDrawer = null; currentThinkingBody = null; }
    currentToolLine = document.createElement('div'); currentToolLine.className = 'agent-tool-line';
    const badge = document.createElement('span'); badge.className = 'agent-tool-badge';
    const argsStr = typeof args === 'object' ? JSON.stringify(args, null, 0) : String(args || '');
    badge.textContent = `⚙ ${name}(${argsStr.length > 50 ? argsStr.substring(0, 50) + '...' : argsStr})`;
    badge.title = `${name}(${argsStr})`;
    currentToolLine.appendChild(badge);
    currentToolResult = document.createElement('span'); currentToolResult.className = 'agent-tool-result-inline';
    currentToolResult.textContent = t('agent.running');
    currentToolLine.appendChild(currentToolResult);
    currentStepEl.appendChild(currentToolLine);
  },

  _updateStatus(text) { if (currentResponseEl?._statusEl) currentResponseEl._statusEl.textContent = text; },
  _hideStatus() { if (currentResponseEl?._statusEl) currentResponseEl._statusEl.style.display = 'none'; },

  _handleEvent(event) {
    switch (event.type) {
      case 'thinking': {
        if (!currentResponseEl) return;
        this._updateStatus(t('agent.thinking')); this._ensureThinking();
        if (event.content && currentThinkingBody) currentThinkingBody.textContent += event.content;
        scrollToBottom(); break;
      }
      case 'tool_call': {
        if (!currentResponseEl) return;
        if (currentThinkingDrawer) { currentThinkingDrawer.open = false; currentThinkingDrawer = null; currentThinkingBody = null; }
        currentTextBlock = null; currentStepEl = null;
        this._ensureStep(); this._ensureToolLine(event.name, event.args);
        this._updateStatus(`${t('agent.running')}: ${escapeHtml(event.name)}`);
        scrollToBottom(); break;
      }
      case 'tool_result': {
        if (!currentResponseEl) return;
        if (currentToolResult) {
          let parsed = null; try { parsed = JSON.parse(event.content || ''); } catch {}
          if (parsed && parsed.status && parsed.summary) {
            const sc = { success: 'success', ok: 'success', error: 'error', warning: 'warning' }[parsed.status] || '';
            const sl = { success: t('status.success'), ok: t('common.ok'), error: t('status.failed'), warning: t('common.warning') }[parsed.status] || parsed.status;
            let html = `<div class="tool-result-card ${sc}"><span class="tool-result-status">${escapeHtml(sl)}</span><span class="tool-result-summary">${escapeHtml(parsed.summary)}</span>`;
            if (parsed.record_count !== undefined) html += `<span class="tool-result-count">${parsed.record_count} 条</span>`;
            if (parsed.suggestion) html += `<div class="tool-result-suggestion">建议: ${escapeHtml(parsed.suggestion)}</div>`;
            if (parsed.data_truncated) html += '<div class="tool-result-truncated">Data was truncated. Please narrow the query.</div>';
            else if (parsed.data !== undefined && parsed.data !== null) {
              const pv = typeof parsed.data === 'string' ? parsed.data : JSON.stringify(parsed.data, null, 2);
              html += `<details class="tool-result-data"><summary>Data preview (${pv.length})</summary><pre>${escapeHtml(pv.length > 500 ? pv.substring(0, 500) + '...' : pv)}</pre></details>`;
            }
            if (parsed.warnings?.length) { html += '<div class="tool-result-warnings">' + parsed.warnings.map(w => `<div class="tool-result-warning-item">⚠ ${escapeHtml(w)}</div>`).join('') + '</div>'; }
            currentToolResult.innerHTML = html + '</div>';
          } else {
            currentToolResult.textContent = event.content.length > 300 ? event.content.substring(0, 300) + '...' : event.content;
            currentToolResult.title = event.content;
          }
        }
        this._updateStatus('');
        try { let obj = JSON.parse(event.content || ''); if (obj?.task_id && obj?.success) this._renderTaskCard(obj.task_id, obj.task_name || obj.task_id); } catch {}
        agentFinalText = ''; currentTextBlock = null;
        scrollToBottom(); break;
      }
      case 'final': {
        if (!currentResponseEl) return;
        this._hideStatus();
        if (currentThinkingDrawer) { currentThinkingDrawer.open = false; currentThinkingDrawer = null; currentThinkingBody = null; }
        if (!currentTextBlock) { currentStepEl = null; this._ensureStep(); currentToolLine = null; currentToolResult = null; currentTextBlock = document.createElement('div'); currentTextBlock.className = 'agent-step-text'; currentStepEl.appendChild(currentTextBlock); }
        agentFinalText += event.content || '';
        currentTextBlock.innerHTML = renderSafeMarkdown(agentFinalText);
        scrollToBottom(); break;
      }
      case 'error': {
        if (!currentResponseEl) return;
        this._hideStatus(); currentStepEl = null; currentTextBlock = null;
        this._ensureStep();
        const errEl = document.createElement('div'); errEl.className = 'agent-step-text'; errEl.style.color = 'var(--danger)';
        errEl.textContent = `${t('common.error')}: ${event.content || ''}`;
        currentStepEl.appendChild(errEl);
        scrollToBottom(); break;
      }
    }
  },

  _renderTaskCard(taskId, taskName) {
    const container = document.getElementById('agent-messages');
    if (!container) return;
    const msgEl = document.createElement('div'); msgEl.className = 'agent-message assistant';
    const bubble = document.createElement('div'); bubble.className = 'agent-bubble assistant';
    const card = document.createElement('div'); card.className = 'agent-task-card'; card.dataset.taskId = taskId;
    card.innerHTML = `<div class="agent-task-card-header"><span class="agent-task-card-name">${escapeHtml(taskName || taskId)}</span><span class="badge badge-pending">pending</span></div>
      <div class="agent-task-card-progress"><div class="agent-task-card-progress-fill"></div></div><div class="agent-task-card-logs"></div>`;
    bubble.appendChild(card); msgEl.appendChild(bubble); container.appendChild(msgEl);
    trackedAgentTaskIds.add(taskId); scrollToBottom();
  },

  _updateTaskCard(task) {
    const card = document.querySelector(`.agent-task-card[data-task-id="${task.id}"]`);
    if (!card) return;
    const badge = card.querySelector('.badge');
    if (badge) { badge.className = `badge badge-${task.status}`; badge.textContent = task.status; }
    const fill = card.querySelector('.agent-task-card-progress-fill');
    if (fill) fill.style.width = (task.progress || 0) + '%';
    const logsEl = card.querySelector('.agent-task-card-logs');
    if (logsEl && task.step_logs) {
      logsEl.innerHTML = task.step_logs.slice(-5).map(log =>
        `<div class="agent-task-card-log-item"><span class="agent-task-card-log-time">${escapeHtml(log.time || '')}</span><span>${escapeHtml(log.message || '')}</span></div>`).join('');
    }
    if (['success', 'failed', 'cancelled'].includes(task.status)) trackedAgentTaskIds.delete(task.id);
    scrollToBottom();
  },

  async _clearHistory() {
    fetch(`/api/agent/history?session_id=${encodeURIComponent(agentSessionId)}`, { method: 'DELETE' }).catch(() => {});
    localStorage.removeItem('agent_msgs_' + agentSessionId);
    const container = document.getElementById('agent-messages');
    if (container) { container.innerHTML = ''; this._appendMessage('assistant', t('agent.cleared')); }
  },

  // ── Send ──

  async _send() {
    const input = document.getElementById('agent-input');
    const btn = document.getElementById('btn-send-agent');
    if (!input || agentStreaming) return;
    const message = input.value.trim();
    if (!message) return;
    input.value = '';

    this._appendMessage('user', message);
    cacheAgentMessage(agentSessionId, 'user', message);
    this._createResponseContainer();
    agentStreaming = true;
    if (btn) btn.disabled = true;

    try {
      const response = await fetch('/api/agent/chat', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message, session_id: agentSessionId }),
      });
      if (!response.ok) {
        const errData = await response.json().catch(() => ({ detail: response.statusText }));
        this._appendMessage('assistant', t('message.loadFailed', { error: errData.detail || response.status }));
        return;
      }
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try { this._handleEvent(JSON.parse(line.slice(6))); } catch {}
          }
        }
      }
    } catch (err) { this._appendMessage('assistant', `连接出错: ${err.message}`); }
    finally {
      agentStreaming = false;
      if (btn) btn.disabled = false;
      if (agentFinalText) cacheAgentMessage(agentSessionId, 'assistant', agentFinalText);
      this._hideStatus();
      resetStreamState();
    }
  },
};

// Global exports
window.initAgentChat = function () { /* handled by page init */ };
window.sendAgentMessage = function () { if (window._agentPage) window._agentPage._send(); };
window.clearAgentHistory = function () { if (window._agentPage) window._agentPage._clearHistory(); };
window.createAgentSession = function () { if (window._agentPage) window._agentPage._createSession(); };
window.switchAgentSession = function (id) { if (window._agentPage) window._agentPage._switchSession(id); };
window.deleteAgentSession = function (id) { if (window._agentPage) window._agentPage._deleteSession(id); };
window.showProviderConfigModal = function () { if (window._agentPage) window._agentPage._showProviderConfig(); };
window.addProviderConfigRow = function (d) { if (window._agentPage) window._agentPage._addConfigRow(d); };
window.refreshProviderDefaultSelect = function () { if (window._agentPage) window._agentPage._refreshDefaultSelect(); };
window.saveProviderConfig = function () { if (window._agentPage) window._agentPage._saveProviderConfig(); };
window.onAgentProviderChange = function () { if (window._agentPage) window._agentPage._onProviderChange(); };
window.initAgentProviderSelector = function () { if (window._agentPage) window._agentPage._initProviderSelector(); };
window.renderAgentSessions = function () { if (window._agentPage) window._agentPage._renderSessions(); };
window.appendAgentMessage = function (r, c) { if (window._agentPage) window._agentPage._appendMessage(r, c); };
window.renderSafeMarkdown = renderSafeMarkdown;
window.scrollAgentToBottom = scrollToBottom;
window.handleAgentEvent = function (e) { if (window._agentPage) window._agentPage._handleEvent(e); };
window.createAgentResponseContainer = function () { if (window._agentPage) return window._agentPage._createResponseContainer(); };
window.renderAgentTaskProgressCard = function (id, n) { if (window._agentPage) window._agentPage._renderTaskCard(id, n); };
window.updateAgentTaskCard = function (t) { if (window._agentPage) window._agentPage._updateTaskCard(t); };
window.loadAgentSessions = loadSessions;
window.saveAgentSessions = saveSessions;
window.ensureDefaultSession = function (s) { if (!s.some(x => x.id === 'default')) s.unshift({ id: 'default', name: '默认会话', created_at: new Date().toISOString() }); return s; };
window.loadSessionMessages = loadSessionMessages;
window.cacheAgentMessage = cacheAgentMessage;
window.cacheCurrentSessionMessages = function () {};
window.setReportProgress = function () {};
window.resetReportProgress = function () {};
window.handleReportProgress = function () {};
