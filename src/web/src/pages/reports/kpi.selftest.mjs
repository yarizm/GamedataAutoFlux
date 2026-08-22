import assert from 'node:assert/strict';
import { buildReportKpis } from './kpi.js';

function byKey(report) {
  return Object.fromEntries(buildReportKpis(report).map((k) => [k.key, k.value]));
}

// ─────────────────────────────────────────────────────────────
// Fixture mirrors src/web/routes/reports.py :: ReportResponse.
// FastAPI strips anything not declared there, so the frontend may
// only read these top-level fields.
// ─────────────────────────────────────────────────────────────
const apiReport = {
  id: 'rpt-1',
  title: '周报 · Steam 热度',
  content: '# heading',
  generated_at: '2026-08-19T02:30:00+00:00',
  prompt: 'summarize',
  data_source: 'steam',
  template: 'weekly',
  matched_records: 40,
  // metadata keys come from generator.py :: _build_report_metadata
  metadata: {
    provider: 'qwen',
    template: 'weekly',
    format: 'markdown',
    source_record_count: 42,
    usable_record_count: 40,
    selected_record_keys: ['a', 'b'],
  },
  quality: {},
};

const kpis = byKey(apiReport);

assert.equal(kpis.title, '周报 · Steam 热度');

// Regression: read matched_records. The old code read `record_keys`
// (absent) then `metadata.record_count` (never written — the real key
// is source_record_count), so this rendered 0 for every report.
assert.equal(kpis.records, 40, 'must read matched_records, not the nonexistent metadata.record_count');

// Regression: read generated_at. The old code read `created_at`,
// which ReportResponse does not declare, so this rendered '-'.
assert.equal(kpis.time, '2026-08-19T02:30:00+00:00', 'must read generated_at, not created_at');

// provider genuinely lives under metadata (generator.py:1169).
assert.equal(kpis.provider, 'qwen');

// ─────────────────────────────────────────────────────────────
// Degraded inputs must not throw or leak "undefined" into the DOM.
// ─────────────────────────────────────────────────────────────
const bare = byKey({ id: 'rpt-2', title: '', matched_records: 0, metadata: {}, quality: {} });
assert.equal(bare.records, 0);
assert.equal(bare.provider, '');
assert.equal(bare.time, '');

const noMeta = byKey({ id: 'rpt-3', title: 'T', matched_records: 7 });
assert.equal(noMeta.records, 7, 'missing metadata must not break the record count');
assert.equal(noMeta.provider, '');

assert.doesNotThrow(() => buildReportKpis());
assert.doesNotThrow(() => buildReportKpis({ metadata: null }));

// Fallback: if matched_records is absent, fall back to the real metadata key.
const legacy = byKey({ id: 'rpt-4', title: 'T', metadata: { source_record_count: 12 } });
assert.equal(legacy.records, 12, 'falls back to metadata.source_record_count');

console.log('REPORT_KPI_SELFTEST_OK');
