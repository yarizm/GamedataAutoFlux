import { api, toast, escapeHtml } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import { TYPE_COLORS } from './adapter.js';

const GROUP_ORDER = ['input', 'collector', 'logic', 'processor', 'storage'];

function inputItem() {
  return {
    type: 'input',
    name: '',
    displayName: t('dag.inputNode'),
    description: t('dag.inputNodeDesc'),
    owner: 'core',
    capabilities: [],
    recoveryLevel: '',
    definition: {
      type: 'input',
      component: '',
      ports_in: [],
      ports_out: [{ name: 'params', required: true, type_hint: 'params' }],
    },
  };
}

function itemMatches(item, query) {
  if (!query) return true;
  const haystack = [item.displayName, item.name, item.description, item.type, ...(item.capabilities || [])]
    .join(' ')
    .toLowerCase();
  return query.split(/\s+/).every((part) => haystack.includes(part));
}

/**
 * Mount component palette (grouped by node type, searchable).
 * @param {HTMLElement} el
 * @param {{ onAdd: (type: string, component: string, definition?: object) => void }} opts
 */
export function mountPalette(el, opts = {}) {
  if (!el) return { refresh: async () => {} };

  let items = [];
  let query = '';

  function render() {
    const list = el.querySelector('[data-palette-list]');
    if (!list) return;
    const visible = items.filter((it) => itemMatches(it, query));
    if (!visible.length) {
      list.innerHTML = `<p class="text-muted text-xs">${escapeHtml(query ? t('dag.palette.noMatch') : t('dag.empty.components'))}</p>`;
      return;
    }
    const definitionByKey = new Map();
    list.innerHTML = GROUP_ORDER.map((group) => {
      const members = visible.filter((it) => it.type === group);
      if (!members.length) return '';
      const rows = members.map((it) => {
        const color = TYPE_COLORS[it.type] || 'zinc';
        const key = `${it.type}:${it.name}`;
        definitionByKey.set(key, it.definition);
        // type + component names stay English (L3 technical ids)
        return `<button type="button" class="dag-palette-item dag-palette-${escapeHtml(it.type)}" data-add-key="${escapeHtml(key)}" data-add-type="${escapeHtml(it.type)}" data-add-name="${escapeHtml(it.name)}" title="${escapeHtml(it.description)}">
          <span class="dag-palette-item-head">
            <span class="font-mono text-${color}-400">[${escapeHtml(it.type)}]</span>
            <strong>${escapeHtml(it.displayName)}</strong>
            ${it.name && it.displayName !== it.name ? `<span class="text-muted font-mono text-xs">${escapeHtml(it.name)}</span>` : ''}
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
      return `<div class="dag-palette-group">
        <div class="dag-palette-group-title">${escapeHtml(t(`dag.palette.group.${group}`))} <span>${members.length}</span></div>
        <div class="space-y-2">${rows}</div>
      </div>`;
    }).join('');
    list.querySelectorAll('[data-add-type]').forEach((btn) => {
      btn.addEventListener('click', () => {
        opts.onAdd?.(
          btn.dataset.addType,
          btn.dataset.addName,
          definitionByKey.get(btn.dataset.addKey) || null,
        );
      });
    });
  }

  function ensureShell() {
    if (el.querySelector('[data-palette-list]')) return;
    el.innerHTML = `
      <input type="search" class="dag-palette-search" data-palette-search placeholder="${escapeHtml(t('dag.palette.search'))}" />
      <div data-palette-list class="space-y-3"></div>
    `;
    el.querySelector('[data-palette-search]')?.addEventListener('input', (ev) => {
      query = String(ev.target.value || '').trim().toLowerCase();
      render();
    });
  }

  async function refresh() {
    ensureShell();
    const search = el.querySelector('[data-palette-search]');
    if (search) search.placeholder = t('dag.palette.search');
    try {
      const data = await api('/components/metadata');
      const components = data.components || {};
      const collectors = data.collectors || {};
      const definitions = Array.isArray(data.dag_nodes) ? data.dag_nodes : [];
      const next = definitions.map((definition) => {
        const collectorMeta = collectors[definition.component] || {};
        const isLogic = definition.type === 'logic';
        return {
          type: definition.type,
          name: definition.component,
          displayName: isLogic
            ? t(`dag.logic.${definition.component}`)
            : (definition.display_name || definition.component),
          description: isLogic
            ? t(`dag.logic.${definition.component}Desc`)
            : (definition.description || collectorMeta.description || ''),
          owner: definition.owner || 'core',
          capabilities: Array.isArray(collectorMeta.capabilities)
            ? collectorMeta.capabilities
            : [],
          recoveryLevel: collectorMeta.recovery_level || '',
          definition,
        };
      });
      if (!next.length) {
        for (const [type, names] of Object.entries(components)) {
          if (!['collector', 'processor', 'storage'].includes(type)) continue;
          for (const name of names) {
            next.push({
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
      items = [
        // 入口参数节点由核心提供（无插件 component），不来自 /components/metadata 的组件目录
        inputItem(),
        ...next.filter((item) => (
          ['collector', 'processor', 'storage', 'logic'].includes(item.type)
          // Business sink is sqlalchemy only; local is a legacy alias.
          && (item.type !== 'storage' || item.name === 'sqlalchemy')
        )),
      ];
      render();
    } catch (e) {
      const list = el.querySelector('[data-palette-list]');
      if (list) {
        list.innerHTML = `<p class="text-xs" style="color:var(--danger)">${escapeHtml(t('message.loadFailed', { error: e.message || String(e) }))}</p>`;
      }
      toast(t('dag.paletteLoadFail'), 'error');
    }
  }

  refresh();
  return { refresh };
}
