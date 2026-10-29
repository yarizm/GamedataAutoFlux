/**
 * Inspector sub-editors for built-in logic nodes and node execution policy.
 *
 * Each editor owns its container and re-renders itself only on structural
 * changes (add/remove rows, mode switches); plain value edits update config
 * in place so typing never loses focus.
 */
import { escapeHtml } from '../../core/api.js';
import { t } from '../../core/i18n.js';

export const OPERATORS = [
  'eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'contains', 'not_contains',
  'starts_with', 'ends_with', 'in', 'not_in', 'empty', 'not_empty',
];
const UNARY = new Set(['empty', 'not_empty']);
const AGGREGATES = ['count', 'total', 'failed', 'success_rate'];
const TRANSFORM_TYPES = ['string', 'number', 'boolean', 'json'];

let datalistSeq = 0;

function clone(value) {
  return JSON.parse(JSON.stringify(value ?? {}));
}

function nextCaseId(cases) {
  const used = new Set(cases.map((c) => c.id));
  let i = cases.length + 1;
  while (used.has(`case_${i}`)) i += 1;
  return `case_${i}`;
}

function operatorOptions(selected) {
  return OPERATORS.map((op) =>
    `<option value="${op}" ${op === selected ? 'selected' : ''}>${escapeHtml(t(`dag.op.${op}`))}</option>`,
  ).join('');
}

function aggregateOptions(selected) {
  return AGGREGATES.map((name) =>
    `<option value="${name}" ${name === selected ? 'selected' : ''}>${escapeHtml(t(`dag.agg.${name}`))}</option>`,
  ).join('');
}

function fieldDatalist(id, options) {
  return `<datalist id="${id}">${(options || []).map((o) => `<option value="${escapeHtml(o)}"></option>`).join('')}</datalist>`;
}

function aggregateValue(raw) {
  const text = String(raw ?? '').trim();
  if (text !== '' && Number.isFinite(Number(text))) return Number(text);
  return text;
}

/**
 * Condition row markup shared by condition / filter editors.
 * @param {object} cond
 * @param {{ batch: boolean, listId: string, path: string }} opts
 */
function conditionRow(cond, { batch, listId, path }) {
  const source = batch ? (cond.source || 'record') : 'record';
  const unary = UNARY.has(cond.operator);
  const fieldControl = source === 'aggregate'
    ? `<select data-cond-field="${path}">${aggregateOptions(cond.field)}</select>`
    : `<input type="text" list="${listId}" data-cond-field="${path}" value="${escapeHtml(cond.field || '')}" placeholder="${escapeHtml(t('dag.logic.fieldPlaceholder'))}">`;
  return `<div class="logic-cond-row" data-cond-row="${path}">
    ${batch ? `<select data-cond-source="${path}" title="${escapeHtml(t('dag.logic.source'))}">
      <option value="record" ${source === 'record' ? 'selected' : ''}>${escapeHtml(t('dag.logic.sourceRecord'))}</option>
      <option value="aggregate" ${source === 'aggregate' ? 'selected' : ''}>${escapeHtml(t('dag.logic.sourceAggregate'))}</option>
    </select>` : ''}
    ${fieldControl}
    ${batch && source === 'record' ? `<select data-cond-quantifier="${path}" title="${escapeHtml(t('dag.logic.quantifier'))}">
      <option value="any" ${(cond.quantifier || 'any') === 'any' ? 'selected' : ''}>${escapeHtml(t('dag.logic.any'))}</option>
      <option value="all" ${cond.quantifier === 'all' ? 'selected' : ''}>${escapeHtml(t('dag.logic.all'))}</option>
    </select>` : ''}
    <select data-cond-operator="${path}">${operatorOptions(cond.operator || 'eq')}</select>
    <input type="text" data-cond-value="${path}" value="${escapeHtml(cond.value ?? '')}" placeholder="${escapeHtml(t('dag.logic.valuePlaceholder'))}" ${unary ? 'hidden' : ''}>
    <button type="button" class="logic-icon-btn" data-cond-remove="${path}" title="${escapeHtml(t('common.delete'))}">×</button>
  </div>`;
}

function parsePath(path) {
  return String(path).split('.').map((part) => (/^\d+$/.test(part) ? Number(part) : part));
}

function getAt(root, path) {
  return parsePath(path).reduce((cur, key) => (cur == null ? cur : cur[key]), root);
}

function bindConditionRows(container, config, emit, { batch }) {
  container.querySelectorAll('[data-cond-source]').forEach((sel) => {
    sel.addEventListener('change', () => {
      const cond = getAt(config, sel.dataset.condSource);
      cond.source = sel.value;
      if (sel.value === 'aggregate') {
        cond.field = 'count';
        delete cond.quantifier;
        if (cond.operator === undefined || !['eq', 'ne', 'gt', 'gte', 'lt', 'lte'].includes(cond.operator)) cond.operator = 'gt';
        cond.value = aggregateValue(cond.value ?? 0);
      } else {
        cond.field = '';
        cond.quantifier = 'any';
      }
      emit(true);
    });
  });
  container.querySelectorAll('[data-cond-field]').forEach((input) => {
    input.addEventListener('change', () => {
      getAt(config, input.dataset.condField).field = input.value.trim();
      emit(false);
    });
  });
  container.querySelectorAll('[data-cond-quantifier]').forEach((sel) => {
    sel.addEventListener('change', () => {
      getAt(config, sel.dataset.condQuantifier).quantifier = sel.value;
      emit(false);
    });
  });
  container.querySelectorAll('[data-cond-operator]').forEach((sel) => {
    sel.addEventListener('change', () => {
      const cond = getAt(config, sel.dataset.condOperator);
      const wasUnary = UNARY.has(cond.operator);
      cond.operator = sel.value;
      if (UNARY.has(sel.value)) delete cond.value;
      emit(wasUnary !== UNARY.has(sel.value));
    });
  });
  container.querySelectorAll('[data-cond-value]').forEach((input) => {
    input.addEventListener('change', () => {
      const cond = getAt(config, input.dataset.condValue);
      cond.value = batch && cond.source === 'aggregate' ? aggregateValue(input.value) : input.value;
      emit(false);
    });
  });
  container.querySelectorAll('[data-cond-remove]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const parts = parsePath(btn.dataset.condRemove);
      const index = parts.pop();
      const list = parts.reduce((cur, key) => cur[key], config);
      list.splice(index, 1);
      emit(true);
    });
  });
}

/**
 * IF / ELIF / ELSE builder.
 * @param {HTMLElement} container
 * @param {object} node
 * @param {{ fieldOptions: string[], onChange: (config: object, meta: { structural: boolean }) => void }} ctx
 */
export function mountConditionEditor(container, node, ctx) {
  const config = clone(node.config);
  if (!Array.isArray(config.cases)) config.cases = [];
  const listId = `dag-fields-${++datalistSeq}`;

  function emit(structural) {
    ctx.onChange(clone(config), { structural });
    if (structural) render();
  }

  function render() {
    const cases = config.cases.map((c, ci) => {
      const conditions = Array.isArray(c.conditions) ? c.conditions : (c.conditions = []);
      const op = c.logical_operator === 'or' ? 'or' : 'and';
      return `<div class="logic-case">
        <div class="logic-case-head">
          <span class="logic-case-tag">${ci === 0 ? 'IF' : 'ELIF'}</span>
          <input type="text" data-case-label="${ci}" value="${escapeHtml(c.label || '')}" placeholder="${escapeHtml(t('dag.logic.caseLabelPlaceholder', { id: c.id }))}">
          <code class="logic-case-port" title="${escapeHtml(t('dag.logic.casePort'))}">${escapeHtml(c.id)}</code>
          <button type="button" class="logic-icon-btn" data-case-remove="${ci}" title="${escapeHtml(t('dag.logic.removeCase'))}">×</button>
        </div>
        ${conditions.length > 1 ? `<div class="logic-join">
          <button type="button" class="logic-join-btn ${op === 'and' ? 'is-active' : ''}" data-case-join="${ci}" data-join="and">AND</button>
          <button type="button" class="logic-join-btn ${op === 'or' ? 'is-active' : ''}" data-case-join="${ci}" data-join="or">OR</button>
        </div>` : ''}
        ${conditions.map((cond, ki) => conditionRow(cond, { batch: true, listId, path: `cases.${ci}.conditions.${ki}` })).join('')}
        <button type="button" class="logic-add-btn" data-case-add-cond="${ci}">+ ${escapeHtml(t('dag.logic.addCondition'))}</button>
      </div>`;
    }).join('');
    container.innerHTML = `
      ${fieldDatalist(listId, ctx.fieldOptions)}
      ${cases || `<p class="text-muted text-[10px]">${escapeHtml(t('dag.logic.noCases'))}</p>`}
      <button type="button" class="logic-add-btn" data-case-add>+ ${escapeHtml(t('dag.logic.addCase'))}</button>
      <div class="logic-else"><span class="logic-case-tag">ELSE</span><span>${escapeHtml(t('dag.logic.elseHint'))}</span></div>
      <p class="logic-hint">${escapeHtml(t('dag.logic.conditionHint'))}</p>
    `;

    container.querySelector('[data-case-add]')?.addEventListener('click', () => {
      config.cases.push({
        id: nextCaseId(config.cases),
        label: '',
        logical_operator: 'and',
        conditions: [{ source: 'aggregate', field: 'count', operator: 'gt', value: 0 }],
      });
      emit(true);
    });
    container.querySelectorAll('[data-case-remove]').forEach((btn) => {
      btn.addEventListener('click', () => {
        config.cases.splice(Number(btn.dataset.caseRemove), 1);
        emit(true);
      });
    });
    container.querySelectorAll('[data-case-label]').forEach((input) => {
      input.addEventListener('change', () => {
        config.cases[Number(input.dataset.caseLabel)].label = input.value.trim();
        emit(false);
      });
    });
    container.querySelectorAll('[data-case-join]').forEach((btn) => {
      btn.addEventListener('click', () => {
        config.cases[Number(btn.dataset.caseJoin)].logical_operator = btn.dataset.join;
        emit(true);
      });
    });
    container.querySelectorAll('[data-case-add-cond]').forEach((btn) => {
      btn.addEventListener('click', () => {
        config.cases[Number(btn.dataset.caseAddCond)].conditions.push({
          source: 'record', field: '', quantifier: 'any', operator: 'eq', value: '',
        });
        emit(true);
      });
    });
    bindConditionRows(container, config, emit, { batch: true });
  }

  render();
}

/**
 * Filter / dedupe / sort / limit editor.
 * @param {HTMLElement} container
 * @param {object} node
 * @param {{ fieldOptions: string[], onChange: (config: object, meta: { structural: boolean }) => void }} ctx
 */
export function mountFilterEditor(container, node, ctx) {
  const config = clone(node.config);
  if (!Array.isArray(config.conditions)) config.conditions = [];
  const listId = `dag-fields-${++datalistSeq}`;

  function emit(structural) {
    ctx.onChange(clone(config), { structural });
    if (structural) render();
  }

  function render() {
    const op = config.logical_operator === 'or' ? 'or' : 'and';
    container.innerHTML = `
      ${fieldDatalist(listId, ctx.fieldOptions)}
      <div class="logic-section-title">${escapeHtml(t('dag.logic.keepWhen'))}</div>
      ${config.conditions.length > 1 ? `<div class="logic-join">
        <button type="button" class="logic-join-btn ${op === 'and' ? 'is-active' : ''}" data-join="and">AND</button>
        <button type="button" class="logic-join-btn ${op === 'or' ? 'is-active' : ''}" data-join="or">OR</button>
      </div>` : ''}
      ${config.conditions.map((cond, ki) => conditionRow(cond, { batch: false, listId, path: `conditions.${ki}` })).join('')
        || `<p class="text-muted text-[10px]">${escapeHtml(t('dag.logic.noConditions'))}</p>`}
      <button type="button" class="logic-add-btn" data-add-cond>+ ${escapeHtml(t('dag.logic.addCondition'))}</button>
      <label class="logic-check"><input type="checkbox" data-only-success ${config.only_success !== false ? 'checked' : ''}> ${escapeHtml(t('dag.logic.onlySuccess'))}</label>
      <div class="logic-grid">
        <label>${escapeHtml(t('dag.logic.dedupeBy'))}
          <input type="text" list="${listId}" data-dedupe value="${escapeHtml((config.dedupe_by || []).join(', '))}" placeholder="data.app_id">
        </label>
        <label>${escapeHtml(t('dag.logic.sortBy'))}
          <span class="logic-inline">
            <input type="text" list="${listId}" data-sort-by value="${escapeHtml(config.sort_by || '')}" placeholder="data.score">
            <select data-sort-order>
              <option value="asc" ${config.sort_order !== 'desc' ? 'selected' : ''}>${escapeHtml(t('dag.logic.asc'))}</option>
              <option value="desc" ${config.sort_order === 'desc' ? 'selected' : ''}>${escapeHtml(t('dag.logic.desc'))}</option>
            </select>
          </span>
        </label>
        <label>${escapeHtml(t('dag.logic.limit'))}
          <input type="number" min="0" step="1" data-limit value="${escapeHtml(String(config.limit ?? 0))}">
        </label>
      </div>
      <p class="logic-hint">${escapeHtml(t('dag.logic.filterHint'))}</p>
    `;
    container.querySelectorAll('[data-join]').forEach((btn) => {
      btn.addEventListener('click', () => {
        config.logical_operator = btn.dataset.join;
        emit(true);
      });
    });
    container.querySelector('[data-add-cond]')?.addEventListener('click', () => {
      config.conditions.push({ field: '', operator: 'eq', value: '' });
      emit(true);
    });
    container.querySelector('[data-only-success]')?.addEventListener('change', (ev) => {
      config.only_success = Boolean(ev.target.checked);
      emit(false);
    });
    container.querySelector('[data-dedupe]')?.addEventListener('change', (ev) => {
      config.dedupe_by = String(ev.target.value || '').split(',').map((s) => s.trim()).filter(Boolean);
      emit(false);
    });
    container.querySelector('[data-sort-by]')?.addEventListener('change', (ev) => {
      config.sort_by = String(ev.target.value || '').trim();
      emit(false);
    });
    container.querySelector('[data-sort-order]')?.addEventListener('change', (ev) => {
      config.sort_order = ev.target.value;
      emit(false);
    });
    container.querySelector('[data-limit]')?.addEventListener('change', (ev) => {
      const n = Math.max(0, Math.floor(Number(ev.target.value) || 0));
      config.limit = n;
      emit(false);
    });
    bindConditionRows(container, config, emit, { batch: false });
  }

  render();
}

/**
 * Template field editor.
 * @param {HTMLElement} container
 * @param {object} node
 * @param {{ fieldOptions: string[], onChange: (config: object, meta: { structural: boolean }) => void }} ctx
 */
export function mountTransformEditor(container, node, ctx) {
  const config = clone(node.config);
  if (!Array.isArray(config.fields)) config.fields = [];

  function emit(structural) {
    ctx.onChange(clone(config), { structural });
    if (structural) render();
  }

  function render() {
    const known = (ctx.fieldOptions || []).filter((f) => f.startsWith('data.')).slice(0, 8);
    container.innerHTML = `
      <div class="logic-section-title">${escapeHtml(t('dag.logic.outputFields'))}</div>
      ${config.fields.map((f, fi) => `<div class="logic-field-row">
        <div class="logic-inline">
          <input type="text" data-field-key="${fi}" value="${escapeHtml(f.key || '')}" placeholder="${escapeHtml(t('dag.logic.fieldKeyPlaceholder'))}">
          <select data-field-type="${fi}">${TRANSFORM_TYPES.map((type) =>
            `<option value="${type}" ${(f.type || 'string') === type ? 'selected' : ''}>${escapeHtml(t(`dag.logic.type.${type}`))}</option>`,
          ).join('')}</select>
          <button type="button" class="logic-icon-btn" data-field-remove="${fi}" title="${escapeHtml(t('common.delete'))}">×</button>
        </div>
        <textarea data-field-template="${fi}" rows="2" spellcheck="false" placeholder="{{ data.title }}">${escapeHtml(f.template || '')}</textarea>
      </div>`).join('') || `<p class="text-muted text-[10px]">${escapeHtml(t('dag.logic.noFields'))}</p>`}
      <button type="button" class="logic-add-btn" data-add-field>+ ${escapeHtml(t('dag.logic.addField'))}</button>
      <label class="logic-check"><input type="checkbox" data-keep-original ${config.keep_original !== false ? 'checked' : ''}> ${escapeHtml(t('dag.logic.keepOriginal'))}</label>
      <label class="logic-block-label">${escapeHtml(t('dag.logic.dropFields'))}
        <input type="text" data-drop-fields value="${escapeHtml((config.drop_fields || []).join(', '))}" placeholder="raw_html, debug">
      </label>
      <p class="logic-hint">${escapeHtml(t('dag.logic.templateHint'))}</p>
      ${known.length ? `<div class="insp-chip-row">${known.map((f) => `<button type="button" class="insp-chip is-known" data-insert-var="${escapeHtml(f)}"><code>{{ ${escapeHtml(f)} }}</code></button>`).join('')}</div>` : ''}
    `;
    let lastTemplate = null;
    container.querySelector('[data-add-field]')?.addEventListener('click', () => {
      config.fields.push({ key: `field_${config.fields.length + 1}`, template: '', type: 'string' });
      emit(true);
    });
    container.querySelectorAll('[data-field-remove]').forEach((btn) => {
      btn.addEventListener('click', () => {
        config.fields.splice(Number(btn.dataset.fieldRemove), 1);
        emit(true);
      });
    });
    container.querySelectorAll('[data-field-key]').forEach((input) => {
      input.addEventListener('change', () => {
        config.fields[Number(input.dataset.fieldKey)].key = input.value.trim();
        emit(false);
      });
    });
    container.querySelectorAll('[data-field-type]').forEach((sel) => {
      sel.addEventListener('change', () => {
        config.fields[Number(sel.dataset.fieldType)].type = sel.value;
        emit(false);
      });
    });
    container.querySelectorAll('[data-field-template]').forEach((area) => {
      area.addEventListener('focus', () => { lastTemplate = area; });
      area.addEventListener('change', () => {
        config.fields[Number(area.dataset.fieldTemplate)].template = area.value;
        emit(false);
      });
    });
    container.querySelector('[data-keep-original]')?.addEventListener('change', (ev) => {
      config.keep_original = Boolean(ev.target.checked);
      emit(false);
    });
    container.querySelector('[data-drop-fields]')?.addEventListener('change', (ev) => {
      config.drop_fields = String(ev.target.value || '').split(',').map((s) => s.trim()).filter(Boolean);
      emit(false);
    });
    container.querySelectorAll('[data-insert-var]').forEach((btn) => {
      btn.addEventListener('mousedown', (ev) => ev.preventDefault()); // keep textarea focus
      btn.addEventListener('click', () => {
        const area = lastTemplate || container.querySelector('[data-field-template]');
        if (!area) return;
        const snippet = `{{ ${btn.dataset.insertVar} }}`;
        const start = area.selectionStart ?? area.value.length;
        const end = area.selectionEnd ?? area.value.length;
        area.value = `${area.value.slice(0, start)}${snippet}${area.value.slice(end)}`;
        config.fields[Number(area.dataset.fieldTemplate)].template = area.value;
        emit(false);
        area.focus();
      });
    });
  }

  render();
}

/**
 * Node execution policy editor (retry / timeout / on_error).
 * @param {HTMLElement} container
 * @param {object} node
 * @param {{ onChange: (policy: object, meta: { structural: boolean }) => void }} ctx
 */
export function mountPolicyEditor(container, node, ctx) {
  const policy = clone(node.policy);
  const modes = ['fail', 'stop', 'continue', 'error_branch'];

  function emit(structural) {
    ctx.onChange(clone(policy), { structural });
    if (structural) render();
  }

  function render() {
    const mode = modes.includes(policy.on_error) ? policy.on_error : 'fail';
    container.innerHTML = `
      <div class="logic-grid policy-grid">
        <label>${escapeHtml(t('dag.policy.retries'))}
          <input type="number" min="0" max="10" step="1" data-policy-retries value="${escapeHtml(String(policy.max_retries ?? 0))}">
        </label>
        <label>${escapeHtml(t('dag.policy.interval'))}
          <input type="number" min="0" max="600" step="0.5" data-policy-interval value="${escapeHtml(String(policy.retry_interval_seconds ?? 0))}" ${Number(policy.max_retries) > 0 ? '' : 'disabled'}>
        </label>
        <label>${escapeHtml(t('dag.policy.timeout'))}
          <input type="number" min="0" max="86400" step="1" data-policy-timeout value="${escapeHtml(policy.timeout_seconds == null ? '' : String(policy.timeout_seconds))}" placeholder="${escapeHtml(t('dag.policy.noLimit'))}">
        </label>
      </div>
      <label class="logic-block-label">${escapeHtml(t('dag.policy.onError'))}
        <select data-policy-mode>${modes.map((m) =>
          `<option value="${m}" ${m === mode ? 'selected' : ''}>${escapeHtml(t(`dag.policy.mode.${m}`))}</option>`,
        ).join('')}</select>
      </label>
      <p class="logic-hint">${escapeHtml(t(`dag.policy.modeHint.${mode}`))}</p>
    `;
    container.querySelector('[data-policy-retries]')?.addEventListener('change', (ev) => {
      const n = Math.min(10, Math.max(0, Math.floor(Number(ev.target.value) || 0)));
      const toggled = (Number(policy.max_retries) > 0) !== (n > 0);
      if (n > 0) policy.max_retries = n;
      else {
        delete policy.max_retries;
        delete policy.retry_interval_seconds;
      }
      emit(toggled);
    });
    container.querySelector('[data-policy-interval]')?.addEventListener('change', (ev) => {
      const n = Math.min(600, Math.max(0, Number(ev.target.value) || 0));
      if (n > 0) policy.retry_interval_seconds = n;
      else delete policy.retry_interval_seconds;
      emit(false);
    });
    container.querySelector('[data-policy-timeout]')?.addEventListener('change', (ev) => {
      const n = Number(ev.target.value);
      if (ev.target.value === '' || !Number.isFinite(n) || n <= 0) delete policy.timeout_seconds;
      else policy.timeout_seconds = Math.min(86400, n);
      emit(false);
    });
    container.querySelector('[data-policy-mode]')?.addEventListener('change', (ev) => {
      if (ev.target.value === 'fail') delete policy.on_error;
      else policy.on_error = ev.target.value;
      emit(true);
    });
  }

  render();
}
