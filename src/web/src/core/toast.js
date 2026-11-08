// 全局 toast 提示。单独成模块，让 auth.js 能用它而不和 api.js 循环引用。
import { t } from './i18n.js';

const DEFAULT_DURATION_MS = 3000;

/**
 * @param {string} message
 * @param {'info'|'success'|'error'} [type]
 * @param {{ duration?: number, dismissible?: boolean }} [options]
 *   duration 为 0 时不自动消失；dismissible 时带关闭按钮。
 * @returns {HTMLElement|null}
 */
export function toast(message, type = 'info', options = {}) {
  const container = document.getElementById('toast-container');
  if (!container) return null;

  const { duration = DEFAULT_DURATION_MS, dismissible = false } = options;
  const el = document.createElement('div');
  el.className = `toast toast-${type}`;

  if (dismissible) {
    el.classList.add('toast-dismissible');
    el.setAttribute('role', type === 'error' ? 'alert' : 'status');
    const text = document.createElement('span');
    text.className = 'toast-message';
    text.textContent = message;
    const close = document.createElement('button');
    close.type = 'button';
    close.className = 'toast-close';
    close.setAttribute('aria-label', t('common.close'));
    close.title = t('common.close');
    close.textContent = '×';
    close.addEventListener('click', () => el.remove());
    el.append(text, close);
  } else {
    el.textContent = message;
  }

  container.appendChild(el);
  if (duration > 0) setTimeout(() => el.remove(), duration);
  return el;
}
