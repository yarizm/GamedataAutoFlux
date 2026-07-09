import { api, toast, escapeHtml } from '../../core/api.js';

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
        el.innerHTML = '<p class="text-zinc-600 text-xs">暂无已保存的 DAG</p>';
        return saved;
      }
      const active = opts.getActiveName?.() || '';
      el.innerHTML = names.map((name) => {
        const dag = saved[name] || {};
        const nCount = (dag.nodes || []).length;
        const eCount = (dag.edges || []).length;
        const isActive = name === active;
        const activeCls = isActive
          ? 'border-sky-500/40 bg-sky-500/10'
          : 'border-white/5 bg-zinc-800/40';
        return `<div class="flex items-center gap-1">
          <button type="button" class="flex-1 text-left px-2 py-1.5 rounded-lg border ${activeCls} hover:border-sky-500/30 text-xs transition-all" data-load-dag="${escapeHtml(name)}">
            <div class="text-zinc-100 font-medium truncate">${escapeHtml(name)}</div>
            <div class="text-zinc-600 text-[10px]">${nCount} 节点 · ${eCount} 边</div>
          </button>
          <button type="button" class="px-1.5 py-1 text-zinc-600 hover:text-rose-400 text-xs" data-del-dag="${escapeHtml(name)}" title="删除">×</button>
        </div>`;
      }).join('');

      el.querySelectorAll('[data-load-dag]').forEach((btn) => {
        btn.addEventListener('click', () => {
          const name = btn.dataset.loadDag;
          const payload = saved[name];
          if (!payload) {
            toast(`未找到 DAG: ${name}`, 'error');
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
      el.innerHTML = `<p class="text-rose-400 text-xs">加载失败: ${escapeHtml(e.message || String(e))}</p>`;
      return saved;
    }
  }

  refresh();
  return {
    refresh,
    getSaved: () => saved,
  };
}
