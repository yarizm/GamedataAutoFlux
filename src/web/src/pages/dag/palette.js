import { api, toast, escapeHtml } from '../../core/api.js';
import { TYPE_COLORS } from './adapter.js';

/**
 * Mount component palette.
 * @param {HTMLElement} el
 * @param {{ onAdd: (type: string, component: string) => void }} opts
 */
export function mountPalette(el, opts = {}) {
  if (!el) return { refresh: async () => {} };

  async function refresh() {
    try {
      const data = await api('/components/metadata');
      const components = data.components || {};
      const items = [];
      for (const [type, names] of Object.entries(components)) {
        if (!['collector', 'processor', 'storage'].includes(type)) continue;
        for (const name of names) {
          // Business sink is sqlalchemy only; local is legacy alias
          if (type === 'storage' && name !== 'sqlalchemy') continue;
          items.push({ type, name });
        }
      }
      if (!items.length) {
        el.innerHTML = '<p class="text-zinc-600 text-xs">无可用组件</p>';
        return;
      }
      el.innerHTML = items.map((it) => {
        const color = TYPE_COLORS[it.type] || 'zinc';
        return `<button type="button" class="dag-palette-item" data-add-type="${escapeHtml(it.type)}" data-add-name="${escapeHtml(it.name)}">
          <span class="font-mono text-${color}-400">[${escapeHtml(it.type)}]</span>
          <span class="text-zinc-200 ml-1">${escapeHtml(it.name)}</span>
        </button>`;
      }).join('');
      el.querySelectorAll('[data-add-type]').forEach((btn) => {
        btn.addEventListener('click', () => {
          opts.onAdd?.(btn.dataset.addType, btn.dataset.addName);
        });
      });
    } catch (e) {
      el.innerHTML = `<p class="text-rose-400 text-xs">加载失败: ${escapeHtml(e.message || String(e))}</p>`;
      toast('加载节点库失败', 'error');
    }
  }

  refresh();
  return { refresh };
}
