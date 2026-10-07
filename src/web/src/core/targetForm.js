/**
 * Collector target form — driven entirely by the collector's declared
 * target_schema (exposed by the /api pipelines metadata endpoint).
 *
 * Nothing here is per-collector: adding a collector plugin is enough for its
 * fields to appear in the task and cron forms. Element IDs use a prefix,
 * "task" or "cron".
 */

import { escapeHtml } from './api.js';
import { t } from './i18n.js';

function metadataFieldId(prefix, field) {
  const safeKey = String(field?.key || '').replace(/[^a-zA-Z0-9_-]/g, '-');
  return `${prefix}-metadata-${safeKey}`;
}

function schemaFields(metadata) {
  const schema = metadata?.target_schema || {};
  return Array.isArray(schema.fields) ? schema.fields : [];
}

function nameFieldOf(fields) {
  return fields.find((field) => field.location === 'name') || null;
}

function renderMetadataInput(prefix, field) {
  const id = metadataFieldId(prefix, field);
  const common = [
    `id="${escapeHtml(id)}"`,
    `data-target-key="${escapeHtml(field.key)}"`,
    `data-target-location="${escapeHtml(field.location || 'params')}"`,
    `data-target-input="${escapeHtml(field.input_type || 'text')}"`,
    field.required ? 'required' : '',
  ].filter(Boolean).join(' ');
  const placeholder = field.placeholder
    ? ` placeholder="${escapeHtml(field.placeholder)}"`
    : '';
  const defaultValue = field.default ?? '';

  if (field.input_type === 'boolean') {
    return `<label class="metadata-target-checkbox">
      <input type="checkbox" ${common} ${defaultValue ? 'checked' : ''}>
      <span>${escapeHtml(field.label)}</span>
    </label>`;
  }
  if (field.input_type === 'select') {
    return `<select ${common}>
      ${(field.options || []).map((option) => {
        const value = String(option.value ?? '');
        const label = String(option.label ?? value);
        return `<option value="${escapeHtml(value)}" ${value === String(defaultValue) ? 'selected' : ''}>${escapeHtml(label)}</option>`;
      }).join('')}
    </select>`;
  }
  if (field.input_type === 'textarea' || field.input_type === 'textarea_lines') {
    const rows = field.input_type === 'textarea_lines' ? 5 : 3;
    return `<textarea ${common} rows="${rows}"${placeholder}>${escapeHtml(defaultValue)}</textarea>`;
  }
  const type = ['url', 'number', 'date'].includes(field.input_type) ? field.input_type : 'text';
  const bounds = [
    field.minimum != null ? `min="${escapeHtml(String(field.minimum))}"` : '',
    field.maximum != null ? `max="${escapeHtml(String(field.maximum))}"` : '',
  ].filter(Boolean).join(' ');
  return `<input type="${type}" ${common} ${bounds}${placeholder} value="${escapeHtml(defaultValue)}">`;
}

/**
 * Render the collector's declared target fields into the shared containers.
 * Returns false when the collector declares no field contract, so callers can
 * fall back to the generic name input.
 */
export function renderMetadataTargetForm(prefix, metadata) {
  const container = document.getElementById(`${prefix}-metadata-target-fields`);
  if (!container) return false;
  const fields = schemaFields(metadata);
  const commonLabel = document.querySelector(`label[for="${prefix}-target-name"]`);
  const commonInput = document.getElementById(`${prefix}-target-name`);
  const commonHelp = document.getElementById(`${prefix}-target-name-help`);

  if (!fields.length) {
    container.innerHTML = '';
    container.hidden = true;
    if (commonLabel) commonLabel.textContent = t('tasks.targetName');
    if (commonInput) commonInput.placeholder = t('tasks.targetName');
    if (commonHelp) {
      commonHelp.textContent = '';
      commonHelp.hidden = true;
    }
    return false;
  }

  const nameField = nameFieldOf(fields);
  if (commonLabel && nameField?.label) {
    commonLabel.innerHTML = `${escapeHtml(nameField.label)}${nameField.required ? ' <span class="target-required">*</span>' : ''}`;
  } else if (commonLabel) {
    commonLabel.textContent = t('tasks.targetName');
  }
  if (commonInput) {
    commonInput.placeholder = nameField?.placeholder || t('tasks.targetName');
  }
  if (commonHelp) {
    commonHelp.textContent = nameField?.description || '';
    commonHelp.hidden = !nameField?.description;
  }

  const paramFields = fields.filter((field) => field.location !== 'name');
  container.hidden = false;
  container.innerHTML = paramFields.map((field) => `
    <div class="form-group metadata-target-field">
      ${field.input_type === 'boolean'
        ? renderMetadataInput(prefix, field)
        : `<label for="${escapeHtml(metadataFieldId(prefix, field))}">${escapeHtml(field.label)}${field.required ? ' <span class="target-required">*</span>' : ''}</label>
          ${renderMetadataInput(prefix, field)}`}
      <small class="metadata-target-help">${escapeHtml(field.description || '')}</small>
    </div>
  `).join('');
  return true;
}

function readMetadataFieldValue(prefix, field) {
  if (field.location === 'name') {
    return document.getElementById(`${prefix}-target-name`)?.value?.trim?.() || '';
  }
  const element = document.getElementById(metadataFieldId(prefix, field));
  if (!element) return '';
  if (field.input_type === 'boolean') return Boolean(element.checked);
  const raw = element.value?.trim?.() ?? element.value ?? '';
  if (raw === '') return '';
  if (field.input_type === 'number') return Number(raw);
  return raw;
}

/**
 * Build targets from the rendered field contract.
 * Returns null when the collector declares no fields.
 */
export function buildTargetsFromMetadata(prefix, metadata) {
  const fields = schemaFields(metadata);
  if (!fields.length) return null;

  const schema = metadata.target_schema || {};
  const nameField = nameFieldOf(fields);
  const targetName = String(
    nameField
      ? readMetadataFieldValue(prefix, nameField)
      : document.getElementById(`${prefix}-target-name`)?.value?.trim?.() || '',
  );
  const params = { ...(schema.default_params || {}) };
  let identityValue = targetName;
  let repeatedField = null;
  let repeatedValues = [];

  for (const field of fields) {
    if (field.location === 'name') continue;
    const value = readMetadataFieldValue(prefix, field);
    if (field.input_type === 'textarea_lines' || field.multiple) {
      repeatedField = field;
      repeatedValues = String(value || '')
        .split(/\r?\n/)
        .map((item) => item.trim())
        .filter(Boolean);
      if (repeatedValues.length) identityValue ||= repeatedValues[0];
      continue;
    }
    if (value === '' || value == null) continue;
    params[field.key] = value;
    if (field.default == null) identityValue ||= String(value);
  }

  if (repeatedField) {
    if (!repeatedValues.length) return [];
    return repeatedValues.map((value) => ({
      name: value,
      target_type: schema.target_type || 'game',
      params: { ...params, [repeatedField.key]: value },
    }));
  }
  if (!identityValue) return [];
  return [{
    name: targetName || identityValue,
    target_type: schema.target_type || 'game',
    params,
  }];
}

/**
 * Fill the rendered field contract from existing targets (edit mode).
 */
export function applyTargetsToMetadataForm(prefix, metadata, targets) {
  const fields = schemaFields(metadata);
  const list = Array.isArray(targets) ? targets : [];
  if (!fields.length || !list.length) return false;

  const nameField = nameFieldOf(fields);
  const nameInput = document.getElementById(`${prefix}-target-name`);
  if (nameInput) {
    const explicit = nameField ? list[0].params?.[nameField.key] : '';
    nameInput.value = String(explicit ?? list[0].name ?? '');
  }

  for (const field of fields) {
    if (field.location === 'name') continue;
    const element = document.getElementById(metadataFieldId(prefix, field));
    if (!element) continue;
    if (field.input_type === 'textarea_lines' || field.multiple) {
      const lines = list
        .map((target) => target.params?.[field.key])
        .filter((value) => value != null && value !== '')
        .map((value) => String(value));
      element.value = lines.length ? lines.join('\n') : String(field.default ?? '');
      continue;
    }
    const value = list[0].params?.[field.key];
    if (field.input_type === 'boolean') {
      element.checked = value == null ? Boolean(field.default) : Boolean(value);
    } else if (value != null && value !== '') {
      element.value = String(value);
    } else if (field.default != null) {
      element.value = String(field.default);
    }
  }
  return true;
}

/**
 * Clear every rendered field back to its declared default.
 */
export function resetMetadataTargetForm(prefix, metadata) {
  const nameInput = document.getElementById(`${prefix}-target-name`);
  if (nameInput) nameInput.value = '';
  for (const field of schemaFields(metadata)) {
    if (field.location === 'name') continue;
    const element = document.getElementById(metadataFieldId(prefix, field));
    if (!element) continue;
    if (field.input_type === 'boolean') {
      element.checked = Boolean(field.default);
    } else if (field.input_type === 'select') {
      if (field.default != null) element.value = String(field.default);
    } else {
      element.value = field.default == null ? '' : String(field.default);
    }
  }
}

/**
 * Generic single-target fallback for pipelines whose collector declares no
 * field contract. Server-side schema defaults still apply on create.
 */
export function buildFallbackTarget(prefix, targetType = 'game') {
  const name = document.getElementById(`${prefix}-target-name`)?.value?.trim?.() || '';
  if (!name) return [];
  return [{ name, target_type: targetType, params: {} }];
}

/**
 * Update the collector badge and helper text from plugin metadata.
 */
export function updateCollectorLabels(prefix, metadata) {
  const collector = metadata?.collector_id || '';
  const badge = document.getElementById(`${prefix}-collector-badge`);
  if (badge) {
    badge.textContent = t('cron.collectorBadge', {
      name: metadata?.display_name || collector || '—',
    });
  }
  const helper = document.getElementById(`${prefix}-target-helper`);
  if (helper) {
    helper.textContent = metadata?.description
      || (collector ? t('cron.collectorActive', { collector }) : t('cron.targetHelper'));
  }
}

/**
 * Parse optional advanced JSON override.
 * @returns {object[]|null} null if empty, array if valid
 */
export function parseAdvancedTargetsJson(raw, fallbackName = 'Target') {
  const text = String(raw || '').trim();
  if (!text) return null;
  let parsed = JSON.parse(text);
  if (!Array.isArray(parsed)) {
    if (typeof parsed === 'object' && parsed !== null) {
      parsed = [{ name: fallbackName, target_type: 'game', params: parsed }];
    } else {
      throw new Error('Invalid targets JSON');
    }
  }
  return parsed;
}
