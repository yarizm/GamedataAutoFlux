import { getLanguage, t } from './i18n.js';

export async function api(path, options = {}) {
  const resp = await fetch(`/api${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  });

  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(err.detail || `HTTP ${resp.status}`);
  }

  return resp.json();
}

export function toast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  if (!container) return;

  const el = document.createElement('div');
  el.className = `toast toast-${type}`;
  el.textContent = message;
  container.appendChild(el);
  setTimeout(() => el.remove(), 3000);
}

export function escapeJs(value) {
  return String(value).replaceAll('\\', '\\\\').replaceAll("'", "\\'");
}

export function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

export function formatTime(isoStr) {
  if (!isoStr) return '-';
  const d = new Date(isoStr);
  return d.toLocaleString(getLanguage(), {
    month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
}

export function renderBadge(status) {
  return `<span class="badge badge-${status}">${t(`status.${status}`) || status}</span>`;
}

export function renderProgress(progress) {
  const pct = Math.round((progress || 0) * 100);
  return `<div style="display:flex;align-items:center;gap:0.5rem">
    <div class="progress-bar"><div class="progress-bar-fill" style="width:${pct}%"></div></div>
    <span style="font-size:0.8rem;color:var(--text-muted)">${pct}%</span>
  </div>`;
}

export function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

export function setValue(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.value = value;
    const editor = el.nextElementSibling?.CodeMirror;
    if (editor && editor.getValue() !== String(value ?? '')) {
      editor.setValue(String(value ?? ''));
    }
  }
}

export function setChecked(id, value) {
  const el = document.getElementById(id);
  if (el) el.checked = value;
}

// Backward compatibility for legacy scripts
window.api = api;
window.toast = toast;
window.escapeJs = escapeJs;
window.escapeHtml = escapeHtml;
window.formatTime = formatTime;
window.renderBadge = renderBadge;
window.renderProgress = renderProgress;
window.setText = setText;
window.setValue = setValue;
window.setChecked = setChecked;
