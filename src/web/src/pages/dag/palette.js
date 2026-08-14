import { api, toast, escapeHtml } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { TYPE_COLORS } from './adapter.js';

/**
 * Mount component palette.
 * @param {HTMLElement} el
 * @param {{ onAdd: (type: string, component: string, definition?: object) => void }} opts
 */
export function mountPalette(el, opts = {}) {
  if (!el) return { refresh: async () => {} };

  async function refresh() {
    try {
      const data = await api('/components/metadata');
      const components = data.components || {};
      const collectors = data.collectors || {};
      const definitions = Array.isArray(data.dag_nodes) ? data.dag_nodes : [];
      const items = definitions.length ? definitions.map((definition) => {
        const collectorMeta = collectors[definition.component] || {};
        return {
          type: definition.type,
          name: definition.component,
          displayName: definition.display_name || definition.component,
          description: definition.description || collectorMeta.description || '',
          owner: definition.owner || 'core',
          capabilities: Array.isArray(collectorMeta.capabilities)
            ? collectorMeta.capabilities
            : [],
          recoveryLevel: collectorMeta.recovery_level || '',
          definition,
        };
      }) : [];
      if (!items.length) {
        for (const [type, names] of Object.entries(components)) {
          if (!['collector', 'processor', 'storage'].includes(type)) continue;
          for (const name of names) {
            items.push({
              type,
              name,
              displayName: name,
              description: '',
              owner: 'core',
              capabilities: [],
              recoveryLevel: '',
              definition: null,
            });
          }
        }
      }
      const visibleItems = items.filter((item) => (
        ['collector', 'processor', 'storage'].includes(item.type)
        // Business sink is sqlalchemy only; local is a legacy alias.
        && (item.type !== 'storage' || item.name === 'sqlalchemy')
      ));
      if (!visibleItems.length) {
        el.innerHTML = `<p class="text-muted text-xs">${escapeHtml(t('dag.empty.components'))}</p>`;
        return;
      }
      const definitionByKey = new Map();
      el.innerHTML = visibleItems.map((it) => {
        const color = TYPE_COLORS[it.type] || 'zinc';
        const key = `${it.type}:${it.name}`;
        definitionByKey.set(key, it.definition);
        // type + component names stay English (L3 technical ids)
        return `<button type="button" class="dag-palette-item" data-add-key="${escapeHtml(key)}" data-add-type="${escapeHtml(it.type)}" data-add-name="${escapeHtml(it.name)}" title="${escapeHtml(it.description)}">
          <span class="dag-palette-item-head">
            <span class="font-mono text-${color}-400">[${escapeHtml(it.type)}]</span>
            <strong>${escapeHtml(it.displayName)}</strong>
            ${it.displayName !== it.name ? `<span class="text-muted font-mono text-xs">${escapeHtml(it.name)}</span>` : ''}
          </span>
          <span class="dag-palette-item-description">${escapeHtml(it.description || t('dag.descriptionMissing'))}</span>
          <span class="dag-palette-item-meta">
            <span>${escapeHtml(t('dag.providedBy', { owner: it.owner }))}</span>
            ${it.recoveryLevel ? `<span>${escapeHtml(t('dag.recoveryLevel', { level: it.recoveryLevel }))}</span>` : ''}
          </span>
          ${it.capabilities.length ? `<span class="dag-palette-capabilities">${it.capabilities.slice(0, 3).map((capability) =>
            `<code>${escapeHtml(capability)}</code>`,
          ).join('')}</span>` : ''}
        </button>`;
      }).join('');
      el.querySelectorAll('[data-add-type]').forEach((btn) => {
        btn.addEventListener('click', () => {
          opts.onAdd?.(
            btn.dataset.addType,
            btn.dataset.addName,
            definitionByKey.get(btn.dataset.addKey) || null,
          );
        });
      });
    } catch (e) {
      el.innerHTML = `<p class="text-xs" style="color:var(--danger)">${escapeHtml(t('message.loadFailed', { error: e.message || String(e) }))}</p>`;
      toast(t('dag.paletteLoadFail'), 'error');
    }
  }

  refresh();
  return { refresh };
}
