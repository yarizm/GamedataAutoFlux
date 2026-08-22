/**
 * Global Command Palette (Ctrl+K / Cmd+K)
 * Provides fast navigation, quick actions, and search capabilities.
 */

import { navigate } from './router.js';
import { escapeHtml } from './api.js';
import { getLanguage, t } from './i18n.js';
import { setThemePreference } from './theme.js';

let commandPaletteInitialized = false;
let currentSelectedIndex = 0;
let filteredCommands = [];

/**
 * Standard list of global commands.
 * @param {object} store
 * @returns {Array<object>}
 */
export function getStandardCommands(store) {
  const activeTab = store?.get?.('activeTab') || 'dashboard';

  return [
    // Navigation items
    { id: 'nav-dashboard', category: 'navigation', label: t('nav.dashboard') || '仪表盘', icon: 'layout-dashboard', keywords: 'dashboard ybp yibiaopan 首页', action: () => navigate('dashboard', {}, { store }) },
    { id: 'nav-tasks', category: 'navigation', label: t('nav.tasks') || '任务管理', icon: 'list-todo', keywords: 'tasks renwu rw 任务 任务列表', action: () => navigate('tasks', {}, { store }) },
    { id: 'nav-pipelines', category: 'navigation', label: t('nav.pipelines') || '执行方案', icon: 'zap', keywords: 'pipelines fangan 方案 采集方案', action: () => navigate('pipelines', {}, { store }) },
    { id: 'nav-data', category: 'navigation', label: t('nav.data') || '数据浏览', icon: 'database', keywords: 'data shuju 数据 结果', action: () => navigate('data', {}, { store }) },
    { id: 'nav-reports', category: 'navigation', label: t('nav.reports') || '报告', icon: 'file-text', keywords: 'reports baogao 报告 分析', action: () => navigate('reports', {}, { store }) },
    { id: 'nav-cron', category: 'navigation', label: t('nav.cron') || '定时任务', icon: 'clock', keywords: 'cron dingshi 定时 周期 计划', action: () => navigate('cron', {}, { store }) },
    { id: 'nav-dag', category: 'navigation', label: t('nav.dag') || '流程编排', icon: 'git-merge', keywords: 'dag bianpai 编排 工作流 节点', action: () => navigate('dag', {}, { store }) },
    { id: 'nav-agent', category: 'navigation', label: t('nav.agent') || 'AI 助手', icon: 'bot', keywords: 'agent zhushou ai 对话 助理', action: () => navigate('agent', {}, { store }) },
    { id: 'nav-plugins', category: 'navigation', label: t('nav.plugins') || '插件中心', icon: 'puzzle', keywords: 'plugins chajian 插件 扩展 安装', action: () => navigate('plugins', {}, { store }) },
    { id: 'nav-system', category: 'navigation', label: t('nav.system') || '系统检查', icon: 'settings', keywords: 'system xitong 系统 诊断 日志 环境', action: () => navigate('system', {}, { store }) },

    // Quick Actions
    { id: 'act-create-task', category: 'action', label: t('tasks.create') || '创建新任务', icon: 'plus-circle', keywords: 'create task new xinjian cj 创建 任务', action: () => navigate('tasks', { modal: 'create' }, { store }) },
    { id: 'act-refresh', category: 'action', label: t('common.refresh') || '刷新当前页面', icon: 'refresh-cw', keywords: 'refresh reload shuaxin 刷新 重载', action: () => store?.set?.('refresh', activeTab) },
    { id: 'act-help', category: 'action', label: t('help.entry') || '打开使用帮助', icon: 'help-circle', keywords: 'help bangzhu 帮助 导览 指南', action: () => { const btn = document.getElementById('btn-help'); if (btn) btn.click(); } },

    // Theme Switch
    { id: 'act-theme-dark', category: 'action', label: `${t('common.theme')}: ${t('theme.dark') || '深色'}`, icon: 'moon', keywords: 'theme dark shense 黑夜 深色 暗色', action: () => { setThemePreference('dark'); } },
    { id: 'act-theme-light', category: 'action', label: `${t('common.theme')}: ${t('theme.light') || '浅色'}`, icon: 'sun', keywords: 'theme light qianse 白天 浅色 明亮', action: () => { setThemePreference('light'); } },
    { id: 'act-theme-system', category: 'action', label: `${t('common.theme')}: ${t('theme.system') || '系统'}`, icon: 'monitor', keywords: 'theme system xitong 自动 跟随系统', action: () => { setThemePreference('system'); } },

    // Language Switch
    { id: 'act-lang-zh', category: 'action', label: '切换语言: 中文 (zh-CN)', icon: 'globe', keywords: 'lang language zhongwen 中文 语言', action: () => { window.setLanguage?.('zh-CN'); } },
    { id: 'act-lang-en', category: 'action', label: 'Switch Language: English (en-US)', icon: 'globe', keywords: 'lang language english 英文 英语', action: () => { window.setLanguage?.('en-US'); } },
  ];
}

/**
 * Filter commands using fuzzy matching.
 * @param {string} query
 * @param {Array<object>} commands
 * @returns {Array<object>}
 */
export function filterCommands(query, commands = []) {
  const q = String(query || '').trim().toLowerCase();
  if (!q) return commands;

  const parts = q.split(/\s+/);
  return commands.filter((cmd) => {
    const haystack = `${cmd.label} ${cmd.keywords || ''} ${cmd.category || ''} ${cmd.id || ''}`.toLowerCase();
    return parts.every((part) => haystack.includes(part));
  });
}

/**
 * Translated badge label for a command category.
 * @param {string} category
 * @returns {string}
 */
export function getCategoryBadgeLabel(category) {
  return category === 'navigation'
    ? t('commandPalette.badge.navigation')
    : t('commandPalette.badge.action');
}

/**
 * Decide whether Ctrl+K should be captured by the palette for a given event
 * target. Text inputs and code editors own that chord, so the palette must
 * neither open nor call preventDefault() while one of them has focus.
 * @param {EventTarget|null} target
 * @returns {boolean}
 */
export function shouldInterceptPaletteShortcut(target) {
  if (!target) return true;
  // The palette's own search box keeps the chord so Ctrl+K toggles it closed.
  if (target.id === 'command-palette-input') return true;

  const tag = String(target.tagName || '').toLowerCase();
  if (tag === 'input' || tag === 'textarea' || tag === 'select') return false;
  if (target.isContentEditable) return false;
  if (target.closest?.('.cm-editor, .CodeMirror, [contenteditable="true"]')) return false;

  return true;
}

/**
 * Open the Command Palette modal.
 * @param {object} store
 */
export function openCommandPalette(store) {
  const overlay = document.getElementById('command-palette-overlay');
  const input = document.getElementById('command-palette-input');
  if (!overlay || !input) return;

  overlay.classList.add('show');
  input.value = '';
  currentSelectedIndex = 0;
  renderPaletteResults(filterCommands('', getStandardCommands(store)), store);
  setTimeout(() => input.focus(), 50);
}

/**
 * Close the Command Palette modal.
 */
export function closeCommandPalette() {
  const overlay = document.getElementById('command-palette-overlay');
  if (overlay) overlay.classList.remove('show');
}

/**
 * Render results in the palette DOM list.
 * @param {Array<object>} commands
 * @param {object} store
 */
function renderPaletteResults(commands, store) {
  const listEl = document.getElementById('command-palette-list');
  if (!listEl) return;

  filteredCommands = commands;
  if (!commands.length) {
    listEl.innerHTML = `<div class="p-4 text-center text-xs text-zinc-500">${escapeHtml(t('common.empty.noResults'))}</div>`;
    return;
  }

  if (currentSelectedIndex >= commands.length) {
    currentSelectedIndex = Math.max(0, commands.length - 1);
  }

  listEl.innerHTML = commands.map((cmd, idx) => {
    const isSelected = idx === currentSelectedIndex;
    const activeClass = isSelected ? 'active' : '';
    const categoryBadge = cmd.category === 'navigation'
      ? `<span class="text-[10px] uppercase font-bold tracking-wider px-1.5 py-0.5 rounded bg-zinc-800 text-zinc-400">${escapeHtml(getCategoryBadgeLabel('navigation'))}</span>`
      : `<span class="text-[10px] uppercase font-bold tracking-wider px-1.5 py-0.5 rounded bg-violet-500/20 text-violet-300">${escapeHtml(getCategoryBadgeLabel('action'))}</span>`;

    return `
      <div class="command-palette-item flex items-center justify-between p-2.5 rounded-xl text-xs cursor-pointer transition-colors ${activeClass}" data-index="${idx}" role="option" aria-selected="${isSelected ? 'true' : 'false'}">
        <div class="flex items-center gap-2.5 min-w-0">
          <span class="text-base">${cmd.category === 'navigation' ? '📌' : '⚡'}</span>
          <span class="font-medium truncate">${escapeHtml(cmd.label)}</span>
        </div>
        ${categoryBadge}
      </div>`;
  }).join('');

  // Scroll active item into view
  const activeEl = listEl.querySelector('.command-palette-item.active');
  if (activeEl) {
    activeEl.scrollIntoView({ block: 'nearest' });
  }

  // Bind click
  listEl.querySelectorAll('.command-palette-item').forEach((item) => {
    item.addEventListener('click', () => {
      const idx = Number(item.dataset.index);
      executeCommand(idx, store);
    });
  });
}

/**
 * Execute command by index.
 * @param {number} idx
 * @param {object} store
 */
function executeCommand(idx, store) {
  const cmd = filteredCommands[idx];
  if (cmd && typeof cmd.action === 'function') {
    closeCommandPalette();
    try {
      cmd.action();
    } catch (err) {
      console.error('[CommandPalette] Action error:', err);
    }
  }
}

/**
 * Initialize Command Palette global listeners and UI wiring.
 * @param {object} store
 */
export function initCommandPalette(store) {
  if (commandPaletteInitialized || typeof window === 'undefined') return;
  commandPaletteInitialized = true;

  const overlay = document.getElementById('command-palette-overlay');
  const input = document.getElementById('command-palette-input');

  // 1. Global shortcut Ctrl+K / Cmd+K
  window.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
      if (!shouldInterceptPaletteShortcut(e.target || document.activeElement)) return;
      e.preventDefault();
      if (overlay?.classList.contains('show')) {
        closeCommandPalette();
      } else {
        openCommandPalette(store);
      }
    }
  });

  if (!overlay || !input) return;

  // 2. Input typing filter
  input.addEventListener('input', () => {
    currentSelectedIndex = 0;
    const filtered = filterCommands(input.value, getStandardCommands(store));
    renderPaletteResults(filtered, store);
  });

  // 3. Keyboard navigation (Up, Down, Enter, Esc)
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      e.preventDefault();
      closeCommandPalette();
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (filteredCommands.length > 0) {
        currentSelectedIndex = (currentSelectedIndex + 1) % filteredCommands.length;
        renderPaletteResults(filteredCommands, store);
      }
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (filteredCommands.length > 0) {
        currentSelectedIndex = (currentSelectedIndex - 1 + filteredCommands.length) % filteredCommands.length;
        renderPaletteResults(filteredCommands, store);
      }
    } else if (e.key === 'Enter') {
      e.preventDefault();
      executeCommand(currentSelectedIndex, store);
    }
  });

  // 4. Click outside to dismiss
  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) {
      closeCommandPalette();
    }
  });

  // 5. Expose globally
  window.openCommandPalette = () => openCommandPalette(store);
  window.closeCommandPalette = closeCommandPalette;
}
