/**
 * Node I/O schema hints for DAG editor (Dify-style mapping UI).
 * Inputs: from collector_metadata API when available.
 * Outputs: known collector output field catalogs (static + extensible).
 */

/** Common fields produced by youtube_comments (and useful for profiles). */
const OUTPUT_CATALOG = {
  youtube_comments: [
    { key: 'video_url', label: '视频 URL' },
    { key: 'video_id', label: '视频 ID' },
    { key: 'title', label: '标题' },
    { key: 'channel_id', label: '频道 ID' },
    { key: 'channel_url', label: '频道主页 URL' },
    { key: 'channel_name', label: '频道名' },
    { key: 'subscriber_count', label: '粉丝数' },
    { key: 'view_count', label: '播放量' },
    { key: 'comment_count', label: '评论数' },
  ],
  youtube_profiles: [
    { key: 'channel_url', label: '频道主页 URL' },
    { key: 'channel_id', label: '频道 ID' },
    { key: 'author_name', label: '作者名' },
    { key: 'subscriber_count', label: '粉丝数' },
    { key: 'description', label: '简介' },
  ],
  steam: [
    { key: 'game_name', label: '游戏名' },
    { key: 'app_id', label: 'App ID' },
  ],
  taptap: [
    { key: 'game_name', label: '游戏名' },
    { key: 'app_id', label: 'App ID' },
  ],
};

/** Fallback target params when metadata not loaded. */
const INPUT_FALLBACK = {
  youtube_comments: [{ key: 'video_url', required: true, label: '视频 URL' }],
  youtube_profiles: [
    { key: 'channel_url', required: false, label: '频道 URL' },
    { key: 'channel_id', required: false, label: '频道 ID' },
    { key: 'handle', required: false, label: 'Handle' },
  ],
  steam: [
    { key: 'app_id', required: false, label: 'App ID' },
  ],
  taptap: [
    { key: 'app_id', required: false, label: 'App ID' },
  ],
  gtrends: [{ key: 'keyword', required: false, label: '关键词' }],
};

/** Fields used by from_upstream auto mode (must match dag_upstream.py). */
export const AUTO_UPSTREAM_FIELDS = [
  'channel_url',
  'channel_id',
  'handle',
  'video_url',
  'app_id',
  'url',
  'official_url',
];

/**
 * Parse collector_metadata payload → list of input param hints.
 * @param {object|null} meta - collectors[id] from /components/metadata
 */
export function inputParamsFromMetadata(meta, componentId) {
  if (!meta || typeof meta !== 'object') {
    return INPUT_FALLBACK[componentId] || [];
  }
  const schema = meta.target_schema || {};
  const fields = new Map();

  for (const raw of schema.required_fields || []) {
    const text = String(raw);
    // "target.params.video_url" or free text
    const m = text.match(/target\.params\.([a-zA-Z0-9_]+)/);
    if (m) {
      fields.set(m[1], { key: m[1], required: true, label: m[1] });
    } else if (text.includes('target.name')) {
      fields.set('__name__', { key: '__name__', required: true, label: 'target.name' });
    }
  }
  for (const rule of schema.rules || []) {
    for (const f of rule.fields || []) {
      const m = String(f).match(/target\.params\.([a-zA-Z0-9_]+)/);
      if (m && !fields.has(m[1])) {
        fields.set(m[1], {
          key: m[1],
          required: rule.level !== 'warning',
          label: m[1],
        });
      }
    }
  }

  if (!fields.size) {
    return INPUT_FALLBACK[componentId] || [];
  }
  return [...fields.values()];
}

export function outputFieldsForComponent(componentId) {
  return OUTPUT_CATALOG[componentId] || [
    { key: 'records', label: 'records（整包）' },
  ];
}

/**
 * Collect upstream output fields for a node given graph edges.
 * @param {object} editor
 * @param {string} nodeId
 */
export function upstreamOutputFields(editor, nodeId) {
  const edges = (editor?.edges || []).filter((e) => e.to === nodeId);
  const byKey = new Map();
  for (const e of edges) {
    const src = (editor.nodes || []).find((n) => n.id === e.from);
    if (!src) continue;
    for (const f of outputFieldsForComponent(src.component)) {
      if (!byKey.has(f.key)) {
        byKey.set(f.key, {
          ...f,
          fromNode: src.id,
          fromComponent: src.component,
        });
      }
    }
  }
  // always include auto catalog keys as optional picks
  for (const key of AUTO_UPSTREAM_FIELDS) {
    if (!byKey.has(key)) {
      byKey.set(key, { key, label: key, fromNode: '', fromComponent: '' });
    }
  }
  return [...byKey.values()];
}

let _metaCache = null;

export async function loadCollectorMetaMap() {
  if (_metaCache) return _metaCache;
  try {
    const { api } = await import('../../core/api.js');
    const data = await api('/components/metadata');
    _metaCache = data?.collectors || {};
  } catch {
    _metaCache = {};
  }
  return _metaCache;
}

export function getCachedCollectorMeta(componentId) {
  return _metaCache?.[componentId] || null;
}
