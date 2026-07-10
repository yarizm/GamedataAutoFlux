import { api, toast, escapeHtml } from '../../core/api.js';
import { t } from '../../core/i18n.js';

/**
 * Mount saved DAG list.
 * @param {HTMLElement} el
 * @param {{
 *   onLoad: (name: string, payload: object) => void,
 *   onDelete: (name: string) => Promise<void>|void,
 *   getActiveName: () => string,
 * }} opts
 */
export function mountSavedList(el, opts = {}) {
  if (!el) return { refresh: async () => ({}), getSaved: () => ({}) };

  let saved = {};

  async function refresh() {
    try {
      const data = await api('/dags');
      saved = data || {};
      const names = Object.keys(saved).sort();
      if (!names.length) {
        el.innerHTML = `<p class="text-muted text-xs">${escapeHtml(t('dag.empty.saved'))}</p>`;
        return saved;
      }
      const active = opts.getActiveName?.() || '';
      el.innerHTML = names.map((name) => {
        const dag = saved[name] || {};
        const nCount = (dag.nodes || []).length;
        const eCount = (dag.edges || []).length;
        const isActive = name === active;
        const activeCls = isActive ? 'is-active' : '';
        return `<div class="flex items-center gap-1">
          <button type="button" class="dag-saved-item ${activeCls}" data-load-dag="${escapeHtml(name)}">
            <div class="dag-saved-name">${escapeHtml(name)}</div>
            <div class="dag-saved-meta">${escapeHtml(t('dag.nodeEdgeCount', { n: nCount, e: eCount }))}</div>
          </button>
          <button type="button" class="dag-saved-del" data-del-dag="${escapeHtml(name)}" title="${escapeHtml(t('common.delete'))}">×</button>
        </div>`;
      }).join('');

      el.querySelectorAll('[data-load-dag]').forEach((btn) => {
        btn.addEventListener('click', () => {
          const name = btn.dataset.loadDag;
          const payload = saved[name];
          if (!payload) {
            toast(t('dag.notFound', { name }), 'error');
            return;
          }
          opts.onLoad?.(name, payload);
        });
      });
      el.querySelectorAll('[data-del-dag]').forEach((btn) => {
        btn.addEventListener('click', (ev) => {
          ev.stopPropagation();
          opts.onDelete?.(btn.dataset.delDag);
        });
      });
      return saved;
    } catch (e) {
      el.innerHTML = `<p class="text-xs" style="color:var(--danger)">${escapeHtml(t('message.loadFailed', { error: e.message || String(e) }))}</p>`;
      return saved;
    }
  }

  refresh();
  return {
    refresh,
    getSaved: () => saved,
  };
}
