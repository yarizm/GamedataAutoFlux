/**
 * Shared collector target form helpers for task create + cron create.
 * Field element IDs use a prefix: task-* or cron-*.
 */

import { escapeHtml } from './api.js';
import { t } from './i18n.js';

export const COLLECTOR_FIELD_KEYS = [
  'steam',
  'steam_discussions',
  'taptap',
  'monitor',
  'qimai',
  'official_site',
  'youtube_profiles',
  'youtube_comments',
];

export const COLLECTOR_TIPS = {
  taptap: 'TapTap：填写国内公开页 URL 或 App ID。',
  steam_discussions: 'Steam 讨论：App ID 或论坛 URL，可选时间区间。',
  monitor: 'Monitor：App ID，可选 Twitch / SullyGnome。',
  qimai: '七麦：App Store 数字 ID 或 Android 包名。',
  official_site: '官网：必填 official_url。',
  youtube_profiles: 'YouTube 频道：上传 TXT（每行一个频道链接）。',
  youtube_comments: 'YouTube 评论：上传 TXT（每行一个视频链接）。',
  steam: 'Steam：游戏名 + App ID（推荐填写 App ID）。',
  gtrends: 'Google Trends：填写关键词作为目标名称。',
};

function metadataFieldId(prefix, field) {
  const safeKey = String(field?.key || '').replace(/[^a-zA-Z0-9_-]/g, '-');
  return `${prefix}-metadata-${safeKey}`;
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
    return `<textarea ${common} rows="${field.input_type === 'textarea_lines' ? '5' : '3'}"${placeholder}>${escapeHtml(defaultValue)}</textarea>`;
  }
  const type = ['url', 'number', 'date'].includes(field.input_type)
    ? field.input_type
    : 'text';
  const bounds = [
    field.minimum != null ? `min="${escapeHtml(String(field.minimum))}"` : '',
    field.maximum != null ? `max="${escapeHtml(String(field.maximum))}"` : '',
  ].filter(Boolean).join(' ');
  return `<input type="${type}" ${common} ${bounds}${placeholder} value="${escapeHtml(defaultValue)}">`;
}

/**
 * Render plugin-owned target fields. Returns false for legacy metadata without
 * a field contract so the existing compatibility form can remain available.
 */
export function renderMetadataTargetForm(prefix, metadata) {
  const container = document.getElementById(`${prefix}-metadata-target-fields`);
  if (!container) return false;
  const schema = metadata?.target_schema || {};
  const fields = Array.isArray(schema.fields) ? schema.fields : [];
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

  const nameField = fields.find((field) => field.location === 'name');
  if (commonLabel && nameField?.label) {
    commonLabel.innerHTML = `${escapeHtml(nameField.label)}${nameField.required ? ' <span class="target-required">*</span>' : ''}`;
  } else if (commonLabel) {
    commonLabel.textContent = t('tasks.targetName');
  }
  if (commonInput && nameField?.placeholder) {
    commonInput.placeholder = nameField.placeholder;
  } else if (commonInput) {
    commonInput.placeholder = t('tasks.targetName');
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
 * Build targets using the plugin's declarative field contract.
 * Returns null when the plugin does not provide such a contract.
 */
export function buildTargetsFromMetadata(prefix, metadata) {
  const schema = metadata?.target_schema || {};
  const fields = Array.isArray(schema.fields) ? schema.fields : [];
  if (!fields.length) return null;

  const nameField = fields.find((field) => field.location === 'name');
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
 * Show/hide collector-specific panels and update helper text.
 * @param {string} prefix  "task" | "cron"
 * @param {string} collector
 */
export function updateTargetFieldPanels(prefix, collector) {
  const fields = {
    steam: `${prefix}-steam-fields`,
    steam_discussions: `${prefix}-steam-discussions-fields`,
    taptap: `${prefix}-taptap-fields`,
    monitor: `${prefix}-monitor-fields`,
    qimai: `${prefix}-qimai-fields`,
    official_site: `${prefix}-official-site-fields`,
    youtube_profiles: `${prefix}-youtube-profiles-fields`,
    youtube_comments: `${prefix}-youtube-comments-fields`,
  };
  Object.entries(fields).forEach(([c, id]) => {
    const el = document.getElementById(id);
    if (el) el.style.display = collector === c ? 'block' : 'none';
  });

  const common = document.getElementById(`${prefix}-target-common`);
  if (common) {
    // YouTube uses file import only — hide generic name when YT
    const hideName = collector === 'youtube_profiles' || collector === 'youtube_comments';
    common.style.display = hideName ? 'none' : 'block';
  }

  const helper = document.getElementById(`${prefix}-target-helper`);
  if (helper) {
    helper.textContent =
      COLLECTOR_TIPS[collector]
      || (collector
        ? t('cron.collectorActive', { collector })
        : t('cron.targetHelper'));
  }

  const badge = document.getElementById(`${prefix}-collector-badge`);
  if (badge) {
    badge.textContent = t('cron.collectorBadge', { name: collector || '—' });
  }
}

function val(id) {
  return document.getElementById(id)?.value?.trim?.() || document.getElementById(id)?.value || '';
}

function checked(id, fallback = false) {
  const el = document.getElementById(id);
  if (!el) return fallback;
  return Boolean(el.checked);
}

/**
 * Read structured form fields into the shape expected by buildTargets().
 */
export function readTargetFormState(prefix, collector) {
  const targetName = val(`${prefix}-target-name`);
  const state = { collector, targetName };

  if (collector === 'steam_discussions') {
    state.appId = val(`${prefix}-steam-discussions-app-id`);
    state.steamDiscussionsForumUrl = val(`${prefix}-steam-discussions-forum-url`);
    state.steamDiscussionsStart = val(`${prefix}-steam-discussions-start`);
    state.steamDiscussionsEnd = val(`${prefix}-steam-discussions-end`);
    state.steamDiscussionsMaxPages = val(`${prefix}-steam-discussions-max-pages`) || '50';
    state.steamDiscussionsMaxTopics = val(`${prefix}-steam-discussions-max-topics`) || '1000';
    state.steamDiscussionsIncludeReplies = checked(`${prefix}-steam-discussions-include-replies`, true);
  } else if (collector === 'taptap') {
    state.appId = val(`${prefix}-taptap-app-id`);
    state.taptapUrl = val(`${prefix}-taptap-url`);
    state.taptapReviewsPages = val(`${prefix}-taptap-reviews-pages`) || '1';
    state.taptapReviewsLimit = val(`${prefix}-taptap-reviews-limit`) || '20';
  } else if (collector === 'monitor') {
    state.appId = val(`${prefix}-monitor-app-id`);
    state.monitorDays = val(`${prefix}-monitor-days`) || '30';
    state.monitorTwitchName = val(`${prefix}-monitor-twitch-name`);
    state.monitorSiteurl = val(`${prefix}-monitor-siteurl`);
  } else if (collector === 'qimai') {
    state.qimaiAppId = val(`${prefix}-qimai-app-id`);
  } else if (collector === 'official_site') {
    state.officialSiteUrl = val(`${prefix}-official-site-url`);
  } else if (collector === 'steam' || !collector) {
    state.appId = val(`${prefix}-app-id`);
    state.skipSteamdb = checked(`${prefix}-skip-steamdb`, true);
    state.steamdbTimeSlice = val(`${prefix}-steamdb-time-slice`) || 'monthly_peak_1y';
  } else if (collector === 'gtrends') {
    // name only
  }

  return state;
}

/**
 * Build targets[] from collector-specific form state (same semantics as task wizard).
 */
export function buildTargets(formState) {
  const {
    collector,
    targetName,
    appId,
    skipSteamdb,
    steamdbTimeSlice,
    steamDiscussionsForumUrl,
    steamDiscussionsStart,
    steamDiscussionsEnd,
    steamDiscussionsMaxPages,
    steamDiscussionsMaxTopics,
    steamDiscussionsIncludeReplies,
    taptapUrl,
    taptapReviewsPages,
    taptapReviewsLimit,
    monitorDays,
    monitorTwitchName,
    monitorSiteurl,
    qimaiAppId,
    officialSiteUrl,
  } = formState || {};

  if (collector === 'steam_discussions') {
    if (!targetName && !appId && !steamDiscussionsForumUrl) return [];
    return [{
      name: targetName || appId || steamDiscussionsForumUrl,
      target_type: 'game',
      params: {
        ...(appId ? { app_id: appId } : {}),
        ...(steamDiscussionsForumUrl ? { forum_url: steamDiscussionsForumUrl } : {}),
        ...(steamDiscussionsStart ? { start_time: steamDiscussionsStart } : {}),
        ...(steamDiscussionsEnd ? { end_time: steamDiscussionsEnd } : {}),
        max_pages: Number(steamDiscussionsMaxPages || 50),
        max_topics: Number(steamDiscussionsMaxTopics || 1000),
        include_replies: Boolean(steamDiscussionsIncludeReplies),
      },
    }];
  }
  if (collector === 'taptap') {
    if (!targetName && !taptapUrl && !appId) return [];
    return [{
      name: targetName || appId || taptapUrl,
      target_type: 'game',
      params: {
        region: 'cn',
        metrics: ['details', 'reviews', 'updates'],
        reviews_pages: Number(taptapReviewsPages || 1),
        reviews_limit: Number(taptapReviewsLimit || 20),
        use_playwright: 'auto',
        ...(taptapUrl ? { page_url: taptapUrl } : {}),
        ...(appId ? { app_id: appId } : {}),
      },
    }];
  }
  if (collector === 'monitor') {
    if (!targetName && !appId) return [];
    return [{
      name: targetName || appId,
      target_type: 'game',
      params: {
        app_id: appId,
        days: Number(monitorDays || 30),
        metrics: ['twitch_viewer_trend'],
        ...(monitorTwitchName ? { twitch_name: monitorTwitchName } : {}),
        ...(monitorSiteurl ? { siteurl: monitorSiteurl } : {}),
      },
    }];
  }
  if (collector === 'qimai') {
    if (!targetName && !qimaiAppId) return [];
    return [{
      name: targetName || qimaiAppId,
      target_type: 'game',
      params: {
        qimai_app_id: qimaiAppId,
        // precheck metadata expects app_id; keep both when numeric store id
        ...(qimaiAppId ? { app_id: qimaiAppId } : {}),
      },
    }];
  }
  if (collector === 'official_site') {
    if (!officialSiteUrl) return [];
    return [{
      name: targetName || officialSiteUrl,
      target_type: 'game',
      params: { official_url: officialSiteUrl, use_playwright: 'auto' },
    }];
  }
  if (collector === 'youtube_profiles' || collector === 'youtube_comments') {
    return window._importedYouTubeTargetsByCollector?.[collector] || [];
  }
  if (collector === 'gtrends') {
    if (!targetName) return [];
    return [{ name: targetName, target_type: 'keyword', params: {} }];
  }
  // steam default / unknown
  if (!targetName && !appId) return [];
  return [{
    name: targetName || appId,
    target_type: 'game',
    params: {
      ...(appId ? { app_id: appId } : {}),
      ...(!skipSteamdb && steamdbTimeSlice ? { steamdb_time_slice: steamdbTimeSlice } : {}),
      ...(skipSteamdb ? { skip_steamdb: true } : {}),
    },
  }];
}

/**
 * Fill structured fields from the first target (edit mode).
 */
export function applyTargetToForm(prefix, collector, target) {
  if (!target || typeof target !== 'object') return;
  const name = String(target.name || '');
  const params = target.params && typeof target.params === 'object' ? target.params : {};
  const set = (id, value) => {
    const el = document.getElementById(id);
    if (el && value != null && value !== '') el.value = value;
  };
  const setCheck = (id, value) => {
    const el = document.getElementById(id);
    if (el) el.checked = Boolean(value);
  };

  set(`${prefix}-target-name`, name);

  if (collector === 'steam' || !collector) {
    set(`${prefix}-app-id`, params.app_id || '');
    if (params.skip_steamdb != null) setCheck(`${prefix}-skip-steamdb`, params.skip_steamdb);
    if (params.steamdb_time_slice) set(`${prefix}-steamdb-time-slice`, params.steamdb_time_slice);
  } else if (collector === 'steam_discussions') {
    set(`${prefix}-steam-discussions-app-id`, params.app_id || '');
    set(`${prefix}-steam-discussions-forum-url`, params.forum_url || '');
    set(`${prefix}-steam-discussions-start`, (params.start_time || '').slice(0, 10));
    set(`${prefix}-steam-discussions-end`, (params.end_time || '').slice(0, 10));
    if (params.max_pages != null) set(`${prefix}-steam-discussions-max-pages`, params.max_pages);
    if (params.max_topics != null) set(`${prefix}-steam-discussions-max-topics`, params.max_topics);
    if (params.include_replies != null) setCheck(`${prefix}-steam-discussions-include-replies`, params.include_replies);
  } else if (collector === 'taptap') {
    set(`${prefix}-taptap-url`, params.page_url || params.url || '');
    set(`${prefix}-taptap-app-id`, params.app_id || '');
    if (params.reviews_pages != null) set(`${prefix}-taptap-reviews-pages`, params.reviews_pages);
    if (params.reviews_limit != null) set(`${prefix}-taptap-reviews-limit`, params.reviews_limit);
  } else if (collector === 'monitor') {
    set(`${prefix}-monitor-app-id`, params.app_id || '');
    if (params.days != null) set(`${prefix}-monitor-days`, params.days);
    set(`${prefix}-monitor-twitch-name`, params.twitch_name || '');
    set(`${prefix}-monitor-siteurl`, params.siteurl || '');
  } else if (collector === 'qimai') {
    set(`${prefix}-qimai-app-id`, params.qimai_app_id || params.app_id || '');
  } else if (collector === 'official_site') {
    set(`${prefix}-official-site-url`, params.official_url || '');
  } else if (collector === 'youtube_profiles' || collector === 'youtube_comments') {
    // Keep imported list in memory for edit
    if (Array.isArray(arguments[3])) {
      // no-op; caller may pass full list
    }
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
