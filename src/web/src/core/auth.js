// 会话失效处理与退出登录。
//
// 登录门禁在服务端：GET / 未通过管理端鉴权时直接返回登录页（免 key 模式下的远程访问则是
// 「远程访问未开启」页），三条前端加载路径都覆盖。SPA 这里只负责两件事：
//   1. 中途收到 401 / WebSocket 握手被拒时确认状态，需要登录就刷新回到服务端登录页；
//   2. 退出登录。
import { t } from './i18n.js';
import { toast } from './toast.js';
import { formatApiError } from './formatError.js';

/** @typedef {'authenticated'|'login_required'|'remote_disabled'|'unknown'} AuthCheckResult */

let pendingCheck = null;
let reloading = false;
let remoteNoticeShown = false;

/** @returns {Promise<{ auth_required: boolean, authenticated: boolean }>} */
export async function fetchAuthStatus() {
  const resp = await fetch('/api/auth/status', { credentials: 'same-origin', cache: 'no-store' });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

/**
 * 收到 401（或 WebSocket 握手被拒）后调用。并发调用共享同一次状态检查。
 *
 * - 已登录：什么都不做，401 另有原因。
 * - 服务端要求登录但未登录（Cookie 过期、key 被更换）：刷新页面，服务端会返回登录页。
 * - 免 key 模式下的远程/代理访问：提示一次如何开启远程访问。
 *
 * @returns {Promise<AuthCheckResult>} 'unknown' 表示状态接口本身不可达（如断网）。
 */
export function handleUnauthorized() {
  if (reloading) return Promise.resolve('login_required');
  if (!pendingCheck) {
    pendingCheck = checkAuthStatus().finally(() => {
      pendingCheck = null;
    });
  }
  return pendingCheck;
}

/** @param {AuthCheckResult} result */
export function isSignedOut(result) {
  return result === 'login_required' || result === 'remote_disabled';
}

/** @returns {Promise<AuthCheckResult>} */
async function checkAuthStatus() {
  let status;
  try {
    status = await fetchAuthStatus();
  } catch (err) {
    console.error('Auth status check failed:', err);
    return 'unknown';
  }
  if (status?.authenticated) return 'authenticated';
  if (status?.auth_required) {
    reloading = true;
    window.location.reload();
    return 'login_required';
  }
  if (!remoteNoticeShown) {
    remoteNoticeShown = true;
    toast(t('auth.remoteAccessNotice'), 'error', { duration: 0, dismissible: true });
  }
  return 'remote_disabled';
}

/** @returns {Promise<boolean>} 是否已退出（成功后会刷新页面）。 */
export async function logout() {
  let resp;
  try {
    resp = await fetch('/api/auth/logout', { method: 'POST', credentials: 'same-origin' });
  } catch (err) {
    toast(t('auth.logoutFailed', { error: formatApiError(err) }), 'error');
    return false;
  }
  if (!resp.ok) {
    const body = await resp.json().catch(() => null);
    const reason = typeof body?.detail === 'string' ? formatApiError(body) : `HTTP ${resp.status}`;
    toast(t('auth.logoutFailed', { error: reason }), 'error');
    return false;
  }
  window.location.reload();
  return true;
}
