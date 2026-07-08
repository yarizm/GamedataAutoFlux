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
let abortController = null;
let currentResponseEvents = [];
let autoScroll = true;

// ── Helpers ──
function providerLabel(key) { return key.charAt(0).toUpperCase() + key.slice(1); }

function scrollToBottom() {
  const c = document.getElementById('agent-messages');
  if (c && autoScroll) c.scrollTop = c.scrollHeight;
}

function checkAutoScroll() {
  const c = document.getElementById('agent-messages');
  if (!c) return;
  const threshold = 60;
  autoScroll = c.scrollHeight - c.scrollTop - c.clientHeight < threshold;
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
  currentResponseEvents = [];
}

// ── Session storage ──
function loadSessions() { try { return JSON.parse(localStorage.getItem('agent_sessions') || '[]'); } catch { return []; } }
function saveSessions(s) { localStorage.setItem('agent_sessions', JSON.stringify(s)); }
function loadSessionMessages(sid) { try { return JSON.parse(localStorage.getItem('agent_msgs_' + sid) || '[]'); } catch { return []; } }

function cacheAgentMessage(sid, role, content, steps) {
  const key = 'agent_msgs_' + sid;
  let msgs; try { msgs = JSON.parse(localStorage.getItem(key) || '[]'); } catch { msgs = []; }
  const entry = { role, content };
  if (steps && steps.length > 0) entry.steps = steps;
  msgs.push(entry);
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
      input.addEventListener('input', () => {
        input.style.height = 'auto';
        input.style.height = Math.min(input.scrollHeight, 128) + 'px';
      });
    }

    const msgContainer = document.getElementById('agent-messages');
    if (msgContainer) {
      msgContainer.addEventListener('scroll', () => checkAutoScroll());
    }

    const searchInput = document.getElementById('agent-session-search');
    if (searchInput) {
      searchInput.addEventListener('input', () => this._renderSessions());
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
    this._syncServerSessions();
    this._initProviderSelector();
    this._refreshStatus();
  },

  // ── Sessions ──

  _renderSessions() {
    const listEl = document.getElementById('agent-session-list');
    if (!listEl) return;
    const sessions = loadSessions();
    listEl.innerHTML = '';
    const searchInput = document.getElementById('agent-session-search');
    const filter = searchInput?.value?.toLowerCase().trim() || '';
    sessions.forEach(s => {
      if (filter && !s.name.toLowerCase().includes(filter)) return;
      const item = document.createElement('div');
      item.className = 'agent-session-item' + (s.id === agentSessionId ? ' active' : '');
      item.innerHTML = `<span class="agent-session-name" title="${escapeHtml(s.name)}">${escapeHtml(s.name)}</span>
        <button class="agent-session-edit" title="重命名">✎</button>
        <button class="agent-session-delete" title="${t('common.delete')}">&times;</button>`;
      item.querySelector('.agent-session-edit').addEventListener('click', (e) => { e.stopPropagation(); this._editSession(s.id); });
      item.querySelector('.agent-session-delete').addEventListener('click', (e) => { e.stopPropagation(); this._deleteSession(s.id); });
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

  _editSession(id) {
    let sessions = loadSessions();
    const sess = sessions.find(s => s.id === id);
    if (!sess) return;
    const newName = prompt('修改会话标题', sess.name);
    if (newName === null) return;
    const trimmed = newName.trim();
    if (!trimmed) { toast('标题不能为空', 'error'); return; }
    sess.name = trimmed;
    saveSessions(sessions);
    this._renderSessions();
  },

  _switchSession(id) {
    agentSessionId = id;
    localStorage.setItem('agent_active_session', id);
    this._renderSessions();
    const wrapper = document.getElementById('agent-messages');
    if (wrapper) {
      Array.from(wrapper.children).forEach(c => {
        if (c.classList.contains('agent-session-layer')) c.style.display = 'none';
        else c.remove();
      });
      const layer = this._getLayer(id);
      if (layer) layer.style.display = 'block';
    }
    scrollToBottom();
  },

  _deleteSession(id) {
    let sessions = loadSessions();
    if (sessions.length <= 1) { toast('至少保留一个会话', 'error'); return; }
    sessions = sessions.filter(s => s.id !== id);
    saveSessions(sessions);
    localStorage.removeItem('agent_msgs_' + id);
    fetch(`/api/agent/history?thread_id=${encodeURIComponent(id)}`, { method: 'DELETE' }).catch(() => {});
    if (id === agentSessionId) this._switchSession(sessions[0].id);
    this._renderSessions();
  },

  _restoreMessages() {
    const wrapper = document.getElementById('agent-messages');
    if (wrapper) wrapper.innerHTML = '';
    this._switchSession(agentSessionId);
    this._syncServerHistory();
  },

  async _syncServerHistory() {
    try {
      const data = await api(`/agent/history?thread_id=${encodeURIComponent(agentSessionId)}`);
      if (data.messages?.length > 0) {
        const cached = loadSessionMessages(agentSessionId);
        if (cached.length < data.messages.length) {
          const key = 'agent_msgs_' + agentSessionId;
          const serialized = data.messages.map(m => ({ role: m.role, content: m.content }));
          localStorage.setItem(key, JSON.stringify(serialized.slice(-20)));
          const layer = document.getElementById('agent-layer-' + agentSessionId);
          if (layer) {
            layer.innerHTML = '';
            serialized.forEach(m => this._appendMessage(m.role, m.content, layer));
          }
        }
      }
    } catch { /* server history not available, use localStorage */ }
  },

  async _syncServerSessions() {
    try {
      const data = await api('/agent/sessions');
      const serverIds = Array.isArray(data.threads)
        ? data.threads
        : Array.isArray(data.sessions)
          ? data.sessions
          : [];
      if (serverIds.length === 0) return;

      const localSessions = loadSessions();
      const localById = new Map(localSessions.map(session => [session.id, session]));
      const merged = [];
      const seen = new Set();

      serverIds.forEach((id) => {
        if (!id || seen.has(id)) return;
        seen.add(id);
        const existing = localById.get(id);
        if (existing) {
          merged.push(existing);
        } else {
          merged.push({ id, name: id, created_at: new Date().toISOString() });
        }
      });

      localSessions.forEach((session) => {
        if (!session?.id || seen.has(session.id)) return;
        seen.add(session.id);
        merged.push(session);
      });

      const unchanged = merged.length === localSessions.length
        && merged.every((session, index) => {
          const local = localSessions[index];
          return local && local.id === session.id && local.name === session.name;
        });
      if (unchanged) return;

      saveSessions(merged);
      if (!merged.some(session => session.id === agentSessionId)) {
        agentSessionId = merged[0]?.id || 'default';
        localStorage.setItem('agent_active_session', agentSessionId);
        this._restoreMessages();
        return;
      }

      this._renderSessions();
    } catch { /* keep local session list when server sync is unavailable */ }
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

  async _refreshStatus() {
    const text = document.getElementById('agent-status-text');
    const dot = document.getElementById('agent-status-dot');
    const wrap = document.getElementById('agent-runtime-status');
    if (!text) return;
    try {
      const status = await api('/agent/status');
      const model = status.model || status.provider || 'unknown';
      const toolCount = status.active_tool_count ?? 0;
      const mcp = status.mcp_running ? 'MCP on' : status.mcp_enabled ? 'MCP idle' : 'MCP off';
      const statusWarnings = status.status_warnings || [];
      const warningNote = statusWarnings.length ? ' · warning' : '';
      text.textContent = `${model} · ${toolCount} tools · ${mcp}${warningNote}`;
      if (wrap) {
        const warningText = statusWarnings.join('\n') || '-';
        wrap.title = `Provider: ${status.provider || '-'}\nAgent: ${status.agent_type || '-'}\nSessions: ${status.session_count ?? 0}\nHistory loaded: ${status.histories_loaded ? 'yes' : 'no'}\nWarnings: ${warningText}`;
      }
      if (dot) {
        if (status.status_health === 'warning') {
          dot.style.backgroundColor = '#f59e0b';
          dot.style.boxShadow = '0 0 8px rgba(245,158,11,0.8)';
        } else {
          dot.style.backgroundColor = status.initialized ? '#34d399' : '#a78bfa';
          dot.style.boxShadow = status.mcp_running ? '0 0 8px rgba(52,211,153,0.8)' : '0 0 5px rgba(139,92,246,0.8)';
        }
      }
    } catch {
      text.textContent = 'Agent unavailable';
      if (dot) {
        dot.style.backgroundColor = '#fb7185';
        dot.style.boxShadow = '0 0 8px rgba(251,113,133,0.8)';
      }
    }
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
      this._refreshStatus();
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
    data = data || { key: '', model: '', base_url: '', api_key: '', has_api_key: false, temperature: 0.3, max_tokens: 2000 };
    const apiKeyPlaceholder = data.has_api_key ? '留空表示保留当前密钥' : '${ENV_VAR} 或明文';
    const row = document.createElement('div');
    row.className = 'provider-config-row';
    row.innerHTML = `
      <div><label>Key</label><input type="text" class="prov-cfg-key" value="${escapeHtml(data.key || '')}" placeholder="qwen" ${data.key ? 'readonly' : ''}></div>
      <div><label>模型</label><input type="text" class="prov-cfg-model" value="${escapeHtml(data.model || '')}" placeholder="qwen-max"></div>
      <div><label>Base URL</label><input type="text" class="prov-cfg-url" value="${escapeHtml(data.base_url || '')}" placeholder="https://..."></div>
      <div><label>API Key</label><input type="text" class="prov-cfg-keyval" value="${escapeHtml(data.api_key || '')}"></div>
      <div><button class="provider-config-delete" title="${t('common.delete')}">&times;</button></div>`;
    row.querySelector('.provider-config-delete').addEventListener('click', () => { row.remove(); this._refreshDefaultSelect(); });
    const apiKeyInput = row.querySelector('.prov-cfg-keyval');
    if (apiKeyInput) apiKeyInput.placeholder = apiKeyPlaceholder;
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
      this._refreshStatus();
    } catch (err) { toast(t('message.editFailed', { error: err.message }), 'error'); }
  },

  // ── Messages ──

  _getLayer(id) {
    const targetId = id || agentSessionId;
    const wrapper = document.getElementById('agent-messages');
    if (!wrapper) return null;
    let layer = document.getElementById('agent-layer-' + targetId);
    if (!layer) {
      layer = document.createElement('div');
      layer.id = 'agent-layer-' + targetId;
      layer.className = 'agent-session-layer w-full';
      wrapper.appendChild(layer);
      const cached = loadSessionMessages(targetId);
      if (cached.length > 0) {
        cached.forEach(m => {
          if (m.steps && m.steps.length > 0) {
            this._replayStructured(m, layer);
          } else {
            this._appendMessage(m.role, m.content, layer);
          }
        });
      } else this._appendMessage('assistant', `${t('agent.welcome')}\n\n- ${t('agent.help.status')}\n- ${t('agent.help.create')}\n- ${t('agent.help.pipeline')}\n- ${t('agent.help.data')}\n- ${t('agent.help.report')}\n\n${t('agent.help.ask')}`, layer);
    }
    return layer;
  },

  _replayStructured(msg, container) {
    if (!container) return;
    const wasStreaming = agentStreaming;
    agentStreaming = true;

    const { resp, steps } = this._createAssistantBubble('已完成');
    container.appendChild(resp.closest('.agent-message'));

    currentResponseEl = resp;
    currentResponseSteps = steps;
    currentStepEl = null;
    currentThinkingDrawer = null;
    currentThinkingBody = null;
    currentToolLine = null;
    currentToolResult = null;
    agentFinalText = '';

    for (const event of msg.steps) {
      this._handleEvent(event);
    }

    this._hideStatus();
    resetStreamState();
    agentStreaming = wasStreaming;
    scrollToBottom();
  },

  _appendMessage(role, content, targetLayer) {
    const container = targetLayer || this._getLayer(agentSessionId);
    if (!container) return;
    const msgEl = document.createElement('div');
    msgEl.className = `agent-message ${role}`;
    const avatar = document.createElement('div');
    avatar.className = `agent-avatar ${role}`;
    avatar.textContent = role === 'user' ? 'ME' : 'AI';
    const bubble = document.createElement('div');
    bubble.className = `agent-bubble ${role}`;
    bubble.innerHTML = role === 'assistant' ? renderSafeMarkdown(content) : '';
    if (role === 'user') {
      bubble.textContent = content;
      const resendBtn = document.createElement('button');
      resendBtn.className = 'agent-resend-btn';
      resendBtn.title = '重发';
      resendBtn.textContent = '↻';
      resendBtn.addEventListener('click', () => this._resend(content));
      bubble.appendChild(resendBtn);
    }
    msgEl.appendChild(avatar); msgEl.appendChild(bubble);
    container.appendChild(msgEl);
    autoScroll = true;
    scrollToBottom();
  },

  _resend(message) {
    const doSend = () => {
      const input = document.getElementById('agent-input');
      if (input) input.value = message;
      this._send();
    };
    if (agentStreaming && abortController) {
      // _stop() → abort() → finally 块同步执行，会重置 agentStreaming/abortController。
      // 如果 doSend 同步运行（abort 事件同步触发），会在 finally 之前嵌套调用 _send()，
      // 导致 finally 覆盖新请求的状态。用 setTimeout 确保在第一个请求完全结束后再发送。
      this._stop();
      setTimeout(doSend, 0);
    } else {
      doSend();
    }
  },

  // ── Structured SSE streaming ──

  _createAssistantBubble(statusText) {
    const msgEl = document.createElement('div');
    msgEl.className = 'agent-message assistant';
    const avatar = document.createElement('div');
    avatar.className = 'agent-avatar assistant';
    avatar.textContent = 'AI';
    const bubble = document.createElement('div');
    bubble.className = 'agent-bubble assistant';
    const resp = document.createElement('div');
    resp.className = 'agent-response-container';
    const steps = document.createElement('div');
    steps.className = 'agent-response-steps';
    resp.appendChild(steps);
    const status = document.createElement('div');
    status.className = 'agent-status-indicator';
    status.textContent = statusText;
    resp.appendChild(status);
    resp._statusEl = status;
    bubble.appendChild(resp);
    msgEl.appendChild(avatar);
    msgEl.appendChild(bubble);
    return { msgEl, bubble, resp, steps, status };
  },

  _createResponseContainer() {
    const container = this._getLayer(agentSessionId);
    if (!container) return null;
    const { msgEl, resp, steps } = this._createAssistantBubble(t('agent.thinking'));
    container.appendChild(msgEl);
    currentResponseEl = resp;
    currentResponseSteps = steps;
    agentFinalText = '';
    currentStepEl = null;
    currentTextBlock = null;
    currentThinkingDrawer = null;
    currentThinkingBody = null;
    currentToolLine = null;
    currentToolResult = null;
    currentResponseEvents = [];
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
    const handlers = {
      thinking: '_handleThinking',
      tool_call: '_handleToolCall',
      tool_result: '_handleToolResult',
      final: '_handleFinal',
      error: '_handleError',
    };
    const fn = handlers[event.type];
    if (fn) this[fn](event);
  },

  _handleThinking(event) {
    if (!currentResponseEl) return;
    this._updateStatus(t('agent.thinking'));
    this._ensureThinking();
    if (event.content && currentThinkingBody) currentThinkingBody.textContent += event.content;
    const last = currentResponseEvents[currentResponseEvents.length - 1];
    if (last && last.type === 'thinking') {
      last.content += event.content || '';
    } else {
      currentResponseEvents.push({ type: 'thinking', content: event.content || '' });
    }
    scrollToBottom();
  },

  _handleToolCall(event) {
    if (!currentResponseEl) return;
    if (currentThinkingDrawer) {
      currentThinkingDrawer.open = false;
      currentThinkingDrawer = null;
      currentThinkingBody = null;
    }
    currentTextBlock = null;
    currentStepEl = null;
    this._ensureStep();
    this._ensureToolLine(event.name, event.args);
    this._updateStatus(`${t('agent.running')}: ${escapeHtml(event.name)}`);
    currentResponseEvents.push({ type: 'tool_call', name: event.name, args: event.args });
    scrollToBottom();
  },

  _handleToolResult(event) {
    if (!currentResponseEl) return;
    if (currentToolResult) {
      let parsed = null;
      try { parsed = JSON.parse(event.content || ''); } catch {}
      if (parsed && parsed.status && parsed.summary) {
        currentToolResult.innerHTML = this._renderToolResultCard(parsed);
      } else {
        currentToolResult.textContent = event.content.length > 300 ? event.content.substring(0, 300) + '...' : event.content;
        currentToolResult.title = event.content;
      }
    }
    this._updateStatus('');
    try {
      const obj = JSON.parse(event.content || '');
      if (obj?.task_id && obj?.success) this._renderTaskCard(obj.task_id, obj.task_name || obj.task_id);
    } catch {}
    agentFinalText = '';
    currentTextBlock = null;
    currentResponseEvents.push({ type: 'tool_result', name: event.name, content: event.content || '' });
    scrollToBottom();
  },

  _renderToolResultCard(parsed) {
    const statusMap = { success: 'success', ok: 'success', error: 'error', warning: 'warning' };
    const labelMap = { success: t('status.success'), ok: t('common.ok'), error: t('status.failed'), warning: t('common.warning') };
    const sc = statusMap[parsed.status] || '';
    const sl = labelMap[parsed.status] || parsed.status;
    let html = `<div class="tool-result-card ${sc}"><span class="tool-result-status">${escapeHtml(sl)}</span><span class="tool-result-summary">${escapeHtml(parsed.summary)}</span>`;
    if (parsed.record_count !== undefined) html += `<span class="tool-result-count">${parsed.record_count} 条</span>`;
    if (parsed.suggestion) html += `<div class="tool-result-suggestion">建议: ${escapeHtml(parsed.suggestion)}</div>`;
    if (parsed.data_truncated) {
      html += '<div class="tool-result-truncated">Data was truncated. Please narrow the query.</div>';
    } else if (parsed.data !== undefined && parsed.data !== null) {
      const pv = typeof parsed.data === 'string' ? parsed.data : JSON.stringify(parsed.data, null, 2);
      html += `<details class="tool-result-data"><summary>Data preview (${pv.length})</summary><pre>${escapeHtml(pv.length > 500 ? pv.substring(0, 500) + '...' : pv)}</pre></details>`;
    }
    if (parsed.warnings?.length) {
      html += '<div class="tool-result-warnings">' + parsed.warnings.map(w => `<div class="tool-result-warning-item">⚠ ${escapeHtml(w)}</div>`).join('') + '</div>';
    }
    return html + '</div>';
  },

  _handleFinal(event) {
    if (!currentResponseEl) return;
    this._hideStatus();
    if (currentThinkingDrawer) {
      currentThinkingDrawer.open = false;
      currentThinkingDrawer = null;
      currentThinkingBody = null;
    }
    if (!currentTextBlock) {
      currentStepEl = null;
      this._ensureStep();
      currentToolLine = null;
      currentToolResult = null;
      currentTextBlock = document.createElement('div');
      currentTextBlock.className = 'agent-step-text';
      currentStepEl.appendChild(currentTextBlock);
    }
    agentFinalText += event.content || '';
    currentTextBlock.innerHTML = renderSafeMarkdown(agentFinalText);
    const last = currentResponseEvents[currentResponseEvents.length - 1];
    if (last && last.type === 'final') {
      last.content += event.content || '';
    } else {
      currentResponseEvents.push({ type: 'final', content: event.content || '' });
    }
    scrollToBottom();
  },

  _handleError(event) {
    if (!currentResponseEl) return;
    this._hideStatus();
    currentStepEl = null;
    currentTextBlock = null;
    this._ensureStep();
    const errEl = document.createElement('div');
    errEl.className = 'agent-step-text';
    errEl.style.color = 'var(--danger)';
    errEl.textContent = `${t('common.error')}: ${event.content || ''}`;
    currentStepEl.appendChild(errEl);
    currentResponseEvents.push({ type: 'error', content: event.content || '' });
    scrollToBottom();
  },

  _renderTaskCard(taskId, taskName) {
    let container = null;
    if (currentResponseEl && currentResponseEl.closest) {
      container = currentResponseEl.closest('.agent-session-layer');
    }
    if (!container) {
      container = this._getLayer(agentSessionId);
    }
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
    fetch(`/api/agent/history?thread_id=${encodeURIComponent(agentSessionId)}`, { method: 'DELETE' }).catch(() => {});
    localStorage.removeItem('agent_msgs_' + agentSessionId);
    const container = this._getLayer(agentSessionId);
    if (container) { container.innerHTML = ''; this._appendMessage('assistant', t('agent.cleared'), container); }
  },

  // ── Send ──

  async _send() {
    const input = document.getElementById('agent-input');
    const btn = document.getElementById('btn-send-agent');
    const btnStop = document.getElementById('btn-stop-agent');
    if (!input || agentStreaming) return;
    const message = input.value.trim();
    if (!message) return;
    input.value = '';
    input.style.height = 'auto';

    this._appendMessage('user', message);
    cacheAgentMessage(agentSessionId, 'user', message);
    this._createResponseContainer();
    agentStreaming = true;
    if (btn) btn.style.display = 'none';
    if (btnStop) btnStop.style.display = 'flex';

    if (abortController) {
      try { abortController.abort(); } catch (_) {}
    }
    abortController = new AbortController();
    const currentFetchSessionId = agentSessionId;

    try {
      const response = await fetch('/api/agent/chat', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message, thread_id: agentSessionId }),
        signal: abortController.signal,
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
    } catch (err) {
      if (err.name === 'AbortError') {
        if (currentResponseEl?._statusEl) currentResponseEl._statusEl.textContent = '已停止生成';
        if (!agentFinalText) agentFinalText = '*(已手动中止)*';
      } else {
        this._appendMessage('assistant', `连接出错: ${err.message}`, this._getLayer(currentFetchSessionId));
      }
    }
    finally {
      agentStreaming = false;
      const btn = document.getElementById('btn-send-agent');
      const btnStop = document.getElementById('btn-stop-agent');
      if (btn) btn.style.display = 'flex';
      if (btnStop) btnStop.style.display = 'none';
      abortController = null;
      if (currentResponseEvents.length > 0) {
        cacheAgentMessage(currentFetchSessionId, 'assistant', agentFinalText, currentResponseEvents);
      } else if (agentFinalText) {
        cacheAgentMessage(currentFetchSessionId, 'assistant', agentFinalText);
      }

      if (currentFetchSessionId === agentSessionId) {
        this._hideStatus();
      } else {
        if (currentResponseEl?._statusEl) currentResponseEl._statusEl.style.display = 'none';
      }
      resetStreamState();
    }
  },

  _stop() {
    if (agentStreaming && abortController) {
      try { abortController.abort(); } catch (_) {}
    }
  },
};

// Global exports — only those needed by HTML onclick handlers outside main.js bridge
// Main agent actions (send/stop/clear/session/provider) are bridched via main.js installGlobalBridge()
window.renderSafeMarkdown = renderSafeMarkdown;
