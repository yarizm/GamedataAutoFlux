import { escapeHtml } from '../../core/api.js';

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
 * }} opts
 */
export function mountInspector(el, opts = {}) {
  if (!el) return { render: () => {} };

  let selection = { kind: null };

  function helpHtml() {
    return `
      <h2 class="text-zinc-100 font-bold tracking-tight text-sm uppercase mb-3">属性</h2>
      <p class="text-zinc-500 text-xs leading-relaxed">选中节点或边以编辑属性。</p>
      <ul class="text-zinc-600 text-[11px] mt-3 space-y-1.5 list-disc list-inside">
        <li>从左侧节点库添加节点</li>
        <li>从输出端口拖到输入端口连线</li>
        <li>collector 串联会自动设置 from_upstream</li>
        <li>条件边可在边属性中选择谓词</li>
      </ul>
    `;
  }

  function renderNode(node) {
    const hasUpstream = Boolean(node.config?.from_upstream);
    const configText = JSON.stringify(node.config || {}, null, 2);
    const isCollector = node.type === 'collector';

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
      ${isCollector ? `
      <div class="insp-field">
        <label>上游映射 (from_upstream)</label>
        <label class="flex items-center gap-2 text-xs text-zinc-300 normal-case tracking-normal">
          <input id="insp-upstream" type="checkbox" ${hasUpstream ? 'checked' : ''} />
          启用 auto 上游 targets
        </label>
        <p class="text-zinc-600 text-[10px] mt-1">collector→collector 连线时会自动开启</p>
      </div>` : ''}
      <div class="insp-field">
        <label for="insp-config">Config (JSON)</label>
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

    const upstreamEl = el.querySelector('#insp-upstream');
    upstreamEl?.addEventListener('change', () => {
      if (upstreamEl.checked) {
        opts.onUpdateNode?.(node.id, {
          config: {
            ...(node.config || {}),
            from_upstream: node.config?.from_upstream || { auto: true },
          },
        });
      } else {
        if (!window.confirm('关闭 from_upstream 后，下游将不再用上游结果生成 targets。确定？')) {
          upstreamEl.checked = true;
          return;
        }
        const nextConfig = { ...(node.config || {}) };
        delete nextConfig.from_upstream;
        opts.onUpdateNode?.(node.id, { config: nextConfig });
      }
    });

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
      <p class="text-zinc-600 text-[10px]">非空条件边在画布上显示为虚线</p>
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
