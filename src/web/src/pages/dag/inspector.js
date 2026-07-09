import { escapeHtml } from '../../core/api.js';
import {
  AUTO_UPSTREAM_FIELDS,
  getCachedCollectorMeta,
  inputParamsFromMetadata,
  loadCollectorMetaMap,
  outputFieldsForComponent,
  upstreamOutputFields,
} from './schemas.js';

const CONDITION_OPTIONS = [
  { value: '', label: '无（始终）' },
  { value: 'on_success', label: 'on_success' },
  { value: 'on_failure', label: 'on_failure' },
  { value: 'on_nonempty', label: 'on_nonempty' },
  { value: 'on_empty', label: 'on_empty' },
];

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
      <h2 class="text-zinc-100 font-bold tracking-tight text-sm uppercase mb-3">属性</h2>
      <p class="text-zinc-500 text-xs leading-relaxed">选中节点或边以编辑属性。</p>
      <ul class="text-zinc-600 text-[11px] mt-3 space-y-1.5 list-disc list-inside">
        <li><b>平移画布</b>：空白处拖拽 / 中键拖拽 / 按住空格再拖</li>
        <li><b>缩放</b>：滚轮；工具栏「适应画布」</li>
        <li>从输出端口拖到输入端口连线</li>
        <li>collector 串联后可在属性里选择上游字段作入参（类似 Dify）</li>
      </ul>
    `;
  }

  function renderInputsBlock(node) {
    if (node.type !== 'collector') {
      return `
        <div class="insp-field">
          <label>端口</label>
          <div class="insp-ro text-[11px]">输入 records · 输出 ${node.type === 'storage' ? '（无）' : 'records'}</div>
        </div>`;
    }
    const meta = getCachedCollectorMeta(node.component);
    const inputs = inputParamsFromMetadata(meta, node.component);
    const outs = outputFieldsForComponent(node.component);
    const inHtml = inputs.length
      ? inputs.map((p) =>
        `<li><code>${escapeHtml(p.key)}</code>${p.required ? ' <span class="text-amber-400">*</span>' : ''} <span class="text-zinc-600">${escapeHtml(p.label || '')}</span></li>`,
      ).join('')
      : '<li class="text-zinc-600">（无声明，任务 targets 直传）</li>';
    const outHtml = outs.map((p) =>
      `<li><code>${escapeHtml(p.key)}</code> <span class="text-zinc-600">${escapeHtml(p.label || '')}</span></li>`,
    ).join('');
    return `
      <div class="insp-field">
        <label>期望入参（任务 targets / 上游映射）</label>
        <ul class="insp-param-list">${inHtml}</ul>
      </div>
      <div class="insp-field">
        <label>已知输出字段</label>
        <ul class="insp-param-list">${outHtml}</ul>
        <p class="text-zinc-600 text-[10px] mt-1">下游 from_upstream 可勾选这些字段</p>
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
    const upOptions = upFields.map((f) =>
      `<option value="${escapeHtml(f.key)}">${escapeHtml(f.key)}${f.fromComponent ? ` ← ${escapeHtml(f.fromComponent)}` : ''}</option>`,
    ).join('');

    const mapRows = targetKeys.map((tk) => {
      const cur = mapObj[tk] || '';
      return `
        <div class="insp-map-row">
          <span class="insp-map-target font-mono text-[11px]">${escapeHtml(tk)}</span>
          <span class="text-zinc-600 text-[10px]">←</span>
          <select data-map-target="${escapeHtml(tk)}" class="insp-map-src" ${!hasUp || autoMode ? 'disabled' : ''}>
            <option value="">（不映射）</option>
            ${upFields.map((f) =>
              `<option value="${escapeHtml(f.key)}" ${f.key === cur ? 'selected' : ''}>${escapeHtml(f.key)}</option>`,
            ).join('')}
          </select>
        </div>`;
    }).join('');

    const autoChecks = AUTO_UPSTREAM_FIELDS.map((k) => {
      const known = upFields.some((f) => f.key === k);
      return `<label class="insp-chip ${known ? 'is-known' : ''}" title="${known ? '上游可能输出' : '通用字段'}">
        <code>${escapeHtml(k)}</code>
      </label>`;
    }).join('');

    return `
      <div class="insp-field">
        <label>上游字段映射（类似 Dify 变量）</label>
        <label class="flex items-center gap-2 text-xs text-zinc-300 normal-case tracking-normal mb-2">
          <input id="insp-upstream" type="checkbox" ${hasUp ? 'checked' : ''} />
          启用 from_upstream（用上游 records 生成 targets）
        </label>
        <div id="insp-upstream-body" class="${hasUp ? '' : 'hidden'}">
          <div class="flex gap-3 mb-2 text-xs text-zinc-300 normal-case tracking-normal">
            <label class="flex items-center gap-1.5">
              <input type="radio" name="insp-up-mode" value="auto" ${autoMode || !hasUp ? 'checked' : ''} />
              自动（推荐）
            </label>
            <label class="flex items-center gap-1.5">
              <input type="radio" name="insp-up-mode" value="map" ${hasUp && !autoMode ? 'checked' : ''} />
              手动映射
            </label>
          </div>
          <div id="insp-auto-hint" class="${autoMode ? '' : 'hidden'}">
            <p class="text-zinc-600 text-[10px] mb-1">自动抽取上游 data 中这些字段（有则映射）：</p>
            <div class="insp-chip-row">${autoChecks}</div>
          </div>
          <div id="insp-map-panel" class="${!autoMode && hasUp ? '' : 'hidden'}">
            <p class="text-zinc-600 text-[10px] mb-1">目标入参 ← 上游输出字段</p>
            ${mapRows || '<p class="text-zinc-600 text-[10px]">无可用目标字段</p>'}
            <p class="text-zinc-600 text-[10px] mt-2">上游可选字段：</p>
            <div class="insp-chip-row">${upOptions
              ? upFields.map((f) => `<span class="insp-chip is-known"><code>${escapeHtml(f.key)}</code></span>`).join('')
              : '<span class="text-zinc-600 text-[10px]">先连接上游 collector</span>'}</div>
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
          if (!window.confirm('关闭 from_upstream 后，将不再用上游结果生成 targets。确定？')) {
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
      <h2 class="text-zinc-100 font-bold tracking-tight text-sm uppercase mb-3">节点</h2>
      <div class="insp-field">
        <label>ID</label>
        <div class="insp-ro font-mono">${escapeHtml(node.id)}</div>
      </div>
      <div class="insp-field">
        <label>Type / Component</label>
        <div class="insp-ro"><span class="font-mono text-zinc-400">[${escapeHtml(node.type)}]</span> ${escapeHtml(node.component)}</div>
      </div>
      <div class="insp-field">
        <label for="insp-label">显示标签</label>
        <input id="insp-label" type="text" value="${escapeHtml(node.label || '')}" placeholder="默认用 component" />
      </div>
      ${renderInputsBlock(node)}
      ${renderUpstreamMapper(node)}
      <div class="insp-field">
        <label for="insp-config">Config (JSON 高级)</label>
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
          throw new Error('config 必须是 JSON 对象');
        }
        errEl?.classList.add('hidden');
        if (errEl) errEl.textContent = '';
        opts.onUpdateNode?.(node.id, { config: parsed });
      } catch (e) {
        if (errEl) {
          errEl.textContent = e.message || 'JSON 解析失败';
          errEl.classList.remove('hidden');
        }
      }
    });
  }

  function renderEdge(edge) {
    const cond = edge.condition || '';
    const optsHtml = CONDITION_OPTIONS.map(
      (o) => `<option value="${o.value}" ${o.value === cond ? 'selected' : ''}>${o.label}</option>`,
    ).join('');
    el.innerHTML = `
      <h2 class="text-zinc-100 font-bold tracking-tight text-sm uppercase mb-3">边</h2>
      <div class="insp-field">
        <label>连接</label>
        <div class="insp-ro font-mono text-[11px]">
          ${escapeHtml(edge.from)}.${escapeHtml(edge.out || 'records')}
          →
          ${escapeHtml(edge.to)}.${escapeHtml(edge.in || 'records')}
        </div>
      </div>
      <div class="insp-field">
        <label for="insp-condition">条件 (condition)</label>
        <select id="insp-condition">${optsHtml}</select>
      </div>
      <p class="text-zinc-600 text-[10px]">非空条件边在画布上显示为虚线。数据字段映射请在目标节点的「上游字段映射」中配置。</p>
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
