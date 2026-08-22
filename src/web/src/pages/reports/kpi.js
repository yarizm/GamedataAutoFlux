/**
 * Report KPI bar data mapping.
 *
 * The field names here are load-bearing: they must match
 * `ReportResponse` in `src/web/routes/reports.py`, because FastAPI
 * strips every key that model does not declare. Anything read off a
 * report object that is not in that model silently arrives as
 * `undefined`, which is how the KPI bar shipped showing "0 条" and "-".
 *
 * Declared by ReportResponse: id, title, content, generated_at, prompt,
 * data_source, template, matched_records, metadata, quality.
 * `metadata` contents come from `_build_report_metadata` in
 * `src/reporting/generator.py` (provider, source_record_count, ...).
 */

/**
 * @typedef {{ key: string, value: string|number }} ReportKpi
 */

/**
 * Map an API report payload to the KPI values shown above a report.
 * Pure: no DOM, no formatting, no i18n — callers own presentation.
 * @param {object} [report]
 * @returns {ReportKpi[]}
 */
export function buildReportKpis(report = {}) {
  const source = report && typeof report === 'object' ? report : {};
  const meta =
    source.metadata && typeof source.metadata === 'object' ? source.metadata : {};

  const matched = Number(source.matched_records);
  const fromMeta = Number(meta.source_record_count);
  const records = Number.isFinite(matched)
    ? matched
    : Number.isFinite(fromMeta)
      ? fromMeta
      : 0;

  return [
    { key: 'title', value: source.title || '' },
    { key: 'provider', value: meta.provider || '' },
    { key: 'records', value: records },
    { key: 'time', value: source.generated_at || '' },
  ];
}
