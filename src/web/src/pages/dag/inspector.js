import { escapeHtml } from '../../core/api.js';
import { t } from '../../core/i18n.js';
import {
  AUTO_UPSTREAM_FIELDS,
  getCachedCollectorMeta,
  inputParamsFromMetadata,
  loadCollectorMetaMap,
  outputFieldsForComponent,
  upstreamOutputFields,
} from './schemas.js';

function conditionOptions() {
  return [
    { value: '', label: t('dag.conditionNone') },
    { value: 'on_success', label: 'on_success' },
    { value: 'on_failure', label: 'on_failure' },
    { value: 'on_nonempty', label: 'on_nonempty' },
    { value: 'on_empty', label: 'on_empty' },
  ];
}

/**
 * @param {HTMLElement} el
 * @param {{
 *   onUpdateNode: (id: string, patch: object) => void,
 *   onUpdateEdge: (edgeKey: object, patch: object) => void,
 *   getEditor?: () => object,
 * }} opts
 */
export function mountInspector(el, opts = {}) {
  if (!el) return { render: () => {} };

  let selection = { kind: null };
  // warm metadata cache
  loadCollectorMetaMap().catch(() => {});

  function helpHtml() {
    return `
      <h2 class="font-bold tracking-tight text-sm uppercase mb-3" style="color:var(--text-primary)">${escapeHtml(t('dag.properties'))}</h2>
      <p class="text-muted text-xs leading-relaxed">${escapeHtml(t('dag.selectHint'))}</p>
      <ul class="text-muted text-[11px] mt-3 space-y-1.5 list-disc list-inside">
        <li>${escapeHtml(t('dag.help.pan'))}</li>
        <li>${escapeHtml(t('dag.help.zoom'))}</li>
        <li>${escapeHtml(t('dag.help.connect'))}</li>
        <li>${escapeHtml(t('dag.help.upstream'))}</li>
      </ul>
    `;
  }

  function renderInputsBlock(node) {
    if (node.type !== 'collector') {
      const outLabel = node.type === 'storage' ? t('dag.portsNone') : 'records';
      return `
        <div class="insp-field">
          <label>${escapeHtml(t('dag.ports'))}</label>
          <div class="insp-ro text-[11px]">${escapeHtml(t('dag.portsIO', { out: outLabel }))}</div>
        </div>`;
    }
    const meta = getCachedCollectorMeta(node.component);
    const inputs = inputParamsFromMetadata(meta, node.component);
    const outs = outputFieldsForComponent(node.component);
    // param keys (code) stay English; only chrome labels are translated
    const inHtml = inputs.length
      ? inputs.map((p) =>
        `<li><code>${escapeHtml(p.key)}</code>${p.required ? ' <span style="color:var(--warning)">*</span>' : ''} <span class="text-muted">${escapeHtml(p.label || '')}</span></li>`,
      ).join('')
      : `<li class="text-muted">${escapeHtml(t('dag.noDeclaredInputs'))}</li>`;
    const outHtml = outs.map((p) =>
      `<li><code>${escapeHtml(p.key)}</code> <span class="text-muted">${escapeHtml(p.label || '')}</span></li>`,
    ).join('');
    return `
      <div class="insp-field">
        <label>${escapeHtml(t('dag.expectedInputs'))}</label>
        <ul class="insp-param-list">${inHtml}</ul>
      </div>
      <div class="insp-field">
        <label>${escapeHtml(t('dag.knownOutputs'))}</label>
        <ul class="insp-param-list">${outHtml}</ul>
        <p class="text-muted text-[10px] mt-1">${escapeHtml(t('dag.downstreamHint'))}</p>
      </div>
    `;
  }

  function renderUpstreamMapper(node) {
    if (node.type !== 'collector') return '';
    const ed = opts.getEditor?.() || { nodes: [], edges: [] };
    const hasUp = Boolean(node.config?.from_upstream);
    const upCfg = typeof node.config?.from_upstream === 'object' && node.config.from_upstream
      ? node.config.from_upstream
      : { auto: true };
    const autoMode = hasUp && (upCfg.auto !== false) && !upCfg.map;
    const mapObj = upCfg.map && typeof upCfg.map === 'object' ? upCfg.map : {};

    const inputs = inputParamsFromMetadata(
      getCachedCollectorMeta(node.component),
      node.component,
    ).filter((p) => p.key !== '__name__');
    const targetKeys = inputs.length
      ? inputs.map((p) => p.key)
      : ['channel_url', 'channel_id', 'video_url', 'app_id'];

    const upFields = upstreamOutputFields(ed, node.id);

    const mapRows = targetKeys.map((tk) => {
      const cur = mapObj[tk] || '';
      return `
        <div class="insp-map-row">
          <span class="insp-map-target font-mono text-[11px]">${escapeHtml(tk)}</span>
          <span class="text-muted text-[10px]">←</span>
          <select data-map-target="${escapeHtml(tk)}" class="insp-map-src" ${!hasUp || autoMode ? 'disabled' : ''}>
            <option value="">${escapeHtml(t('dag.noMap'))}</option>
            ${upFields.map((f) =>
              `<option value="${escapeHtml(f.key)}" ${f.key === cur ? 'selected' : ''}>${escapeHtml(f.key)}</option>`,
            ).join('')}
          </select>
        </div>`;
    }).join('');

    const autoChecks = AUTO_UPSTREAM_FIELDS.map((k) => {
      const known = upFields.some((f) => f.key === k);
      return `<label class="insp-chip ${known ? 'is-known' : ''}" title="${escapeHtml(known ? t('dag.upstreamKnown') : t('dag.fieldCommon'))}">
        <code>${escapeHtml(k)}</code>
      </label>`;
    }).join('');

    return `
      <div class="insp-field">
        <label>${escapeHtml(t('dag.upstreamMap'))}</label>
        <label class="flex items-center gap-2 text-xs normal-case tracking-normal mb-2" style="color:var(--text-secondary)">
          <input id="insp-upstream" type="checkbox" ${hasUp ? 'checked' : ''} />
          ${escapeHtml(t('dag.enableUpstream'))}
        </label>
        <div id="insp-upstream-body" class="${hasUp ? '' : 'hidden'}">
          <div class="flex gap-3 mb-2 text-xs normal-case tracking-normal" style="color:var(--text-secondary)">
            <label class="flex items-center gap-1.5">
              <input type="radio" name="insp-up-mode" value="auto" ${autoMode || !hasUp ? 'checked' : ''} />
              ${escapeHtml(t('dag.modeAuto'))}
            </label>
            <label class="flex items-center gap-1.5">
              <input type="radio" name="insp-up-mode" value="map" ${hasUp && !autoMode ? 'checked' : ''} />
              ${escapeHtml(t('dag.modeMap'))}
            </label>
          </div>
          <div id="insp-auto-hint" class="${autoMode ? '' : 'hidden'}">
            <p class="text-muted text-[10px] mb-1">${escapeHtml(t('dag.autoHint'))}</p>
            <div class="insp-chip-row">${autoChecks}</div>
          </div>
          <div id="insp-map-panel" class="${!autoMode && hasUp ? '' : 'hidden'}">
            <p class="text-muted text-[10px] mb-1">${escapeHtml(t('dag.mapHint'))}</p>
            ${mapRows || `<p class="text-muted text-[10px]">${escapeHtml(t('dag.noTargetFields'))}</p>`}
            <p class="text-muted text-[10px] mt-2">${escapeHtml(t('dag.upstreamFields'))}</p>
            <div class="insp-chip-row">${upFields.length
              ? upFields.map((f) => `<span class="insp-chip is-known"><code>${escapeHtml(f.key)}</code></span>`).join('')
              : `<span class="text-muted text-[10px]">${escapeHtml(t('dag.connectUpstream'))}</span>`}</div>
          </div>
        </div>
      </div>
    `;
  }

  function bindUpstreamControls(node) {
    const upstreamEl = el.querySelector('#insp-upstream');
    const body = el.querySelector('#insp-upstream-body');
    const autoHint = el.querySelector('#insp-auto-hint');
    const mapPanel = el.querySelector('#insp-map-panel');

    function applyMode() {
      const hasUp = upstreamEl?.checked;
      const mode = el.querySelector('input[name="insp-up-mode"]:checked')?.value || 'auto';
      body?.classList.toggle('hidden', !hasUp);
      autoHint?.classList.toggle('hidden', !hasUp || mode !== 'auto');
      mapPanel?.classList.toggle('hidden', !hasUp || mode !== 'map');
      el.querySelectorAll('.insp-map-src').forEach((sel) => {
        sel.disabled = !hasUp || mode !== 'map';
      });

      if (!hasUp) {
        if (node.config?.from_upstream) {
          if (!window.confirm(t('dag.confirmDisableUpstream'))) {
            if (upstreamEl) upstreamEl.checked = true;
            body?.classList.remove('hidden');
            return;
          }
          const nextConfig = { ...(node.config || {}) };
          delete nextConfig.from_upstream;
          opts.onUpdateNode?.(node.id, { config: nextConfig });
        }
        return;
      }

      if (mode === 'auto') {
        opts.onUpdateNode?.(node.id, {
          config: {
            ...(node.config || {}),
            from_upstream: { auto: true },
          },
        });
        return;
      }

      // manual map
      const map = {};
      el.querySelectorAll('.insp-map-src').forEach((sel) => {
        const tk = sel.dataset.mapTarget;
        const sk = sel.value;
        if (tk && sk) map[tk] = sk;
      });
      opts.onUpdateNode?.(node.id, {
        config: {
          ...(node.config || {}),
          from_upstream: Object.keys(map).length
            ? { auto: false, map }
            : { auto: false, map: {} },
        },
      });
    }

    upstreamEl?.addEventListener('change', applyMode);
    el.querySelectorAll('input[name="insp-up-mode"]').forEach((r) => {
      r.addEventListener('change', applyMode);
    });
    el.querySelectorAll('.insp-map-src').forEach((sel) => {
      sel.addEventListener('change', applyMode);
    });
  }

  function renderNode(node) {
    const configText = JSON.stringify(node.config || {}, null, 2);

    el.innerHTML = `
      <h2 class="font-bold tracking-tight text-sm uppercase mb-3" style="color:var(--text-primary)">${escapeHtml(t('dag.node'))}</h2>
      <div class="insp-field">
        <label>ID</label>
        <div class="insp-ro font-mono">${escapeHtml(node.id)}</div>
      </div>
      <div class="insp-field">
        <label>Type / Component</label>
        <div class="insp-ro"><span class="font-mono" style="color:var(--text-muted)">[${escapeHtml(node.type)}]</span> ${escapeHtml(node.component)}</div>
      </div>
      <div class="insp-field">
        <label for="insp-label">${escapeHtml(t('dag.label'))}</label>
        <input id="insp-label" type="text" value="${escapeHtml(node.label || '')}" placeholder="${escapeHtml(t('dag.labelPlaceholder'))}" />
      </div>
      ${renderInputsBlock(node)}
      ${renderUpstreamMapper(node)}
      <div class="insp-field">
        <label for="insp-config">${escapeHtml(t('dag.configJson'))}</label>
        <textarea id="insp-config" spellcheck="false">${escapeHtml(configText)}</textarea>
        <div id="insp-config-err" class="insp-error hidden"></div>
      </div>
    `;

    const labelEl = el.querySelector('#insp-label');
    labelEl?.addEventListener('change', () => {
      opts.onUpdateNode?.(node.id, { label: labelEl.value.trim() });
    });
    labelEl?.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter') {
        opts.onUpdateNode?.(node.id, { label: labelEl.value.trim() });
      }
    });

    bindUpstreamControls(node);

    const configEl = el.querySelector('#insp-config');
    const errEl = el.querySelector('#insp-config-err');
    configEl?.addEventListener('change', () => {
      try {
        const parsed = JSON.parse(configEl.value || '{}');
        if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
          throw new Error(t('dag.configMustObject'));
        }
        errEl?.classList.add('hidden');
        if (errEl) errEl.textContent = '';
        opts.onUpdateNode?.(node.id, { config: parsed });
      } catch (e) {
        if (errEl) {
          errEl.textContent = e.message || t('dag.jsonParseFail');
          errEl.classList.remove('hidden');
        }
      }
    });
  }

  function renderEdge(edge) {
    const cond = edge.condition || '';
    const optsHtml = conditionOptions().map(
      (o) => `<option value="${o.value}" ${o.value === cond ? 'selected' : ''}>${escapeHtml(o.label)}</option>`,
    ).join('');
    el.innerHTML = `
      <h2 class="font-bold tracking-tight text-sm uppercase mb-3" style="color:var(--text-primary)">${escapeHtml(t('dag.edge'))}</h2>
      <div class="insp-field">
        <label>${escapeHtml(t('dag.connection'))}</label>
        <div class="insp-ro font-mono text-[11px]">
          ${escapeHtml(edge.from)}.${escapeHtml(edge.out || 'records')}
          →
          ${escapeHtml(edge.to)}.${escapeHtml(edge.in || 'records')}
        </div>
      </div>
      <div class="insp-field">
        <label for="insp-condition">${escapeHtml(t('dag.condition'))}</label>
        <select id="insp-condition">${optsHtml}</select>
      </div>
      <p class="text-muted text-[10px]">${escapeHtml(t('dag.edgeConditionHint'))}</p>
    `;
    el.querySelector('#insp-condition')?.addEventListener('change', (ev) => {
      const value = ev.target.value || null;
      opts.onUpdateEdge?.(edge, { condition: value || null });
    });
  }

  /**
   * @param {{ kind: 'node'|'edge'|null, node?: object, edge?: object }} sel
   */
  function render(sel) {
    selection = sel || { kind: null };
    // ensure meta loaded then re-render once for collectors
    if (selection.kind === 'node' && selection.node?.type === 'collector') {
      loadCollectorMetaMap().then(() => {
        if (selection.kind === 'node' && selection.node) {
          renderNode(selection.node);
        }
      });
    }
    if (selection.kind === 'node' && selection.node) {
      renderNode(selection.node);
      return;
    }
    if (selection.kind === 'edge' && selection.edge) {
      renderEdge(selection.edge);
      return;
    }
    el.innerHTML = helpHtml();
  }

  render({ kind: null });
  return { render, getSelection: () => selection };
}
