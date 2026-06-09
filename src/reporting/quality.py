"""Shared report quality assessment helpers."""

from __future__ import annotations

from typing import Any

from src.services._utils import source_label


def build_report_quality_summary(
    metadata: dict[str, Any] | None,
    *,
    matched_records: int = 0,
    include_follow_up_actions: bool = False,
) -> dict[str, Any]:
    """Build compact, public report quality metadata from report storage metadata."""
    if not isinstance(metadata, dict):
        metadata = {}
    template_validation = (
        metadata.get("template_validation")
        if isinstance(metadata.get("template_validation"), dict)
        else {}
    )
    empty_record_keys = metadata.get("empty_record_keys", [])
    empty_record_count = len(empty_record_keys) if isinstance(empty_record_keys, list) else 0
    source_record_count = metadata.get("source_record_count", matched_records)
    usable_record_count = metadata.get("usable_record_count", source_record_count)
    fields: dict[str, Any] = {
        "format": metadata.get("format"),
        "source_record_count": source_record_count,
        "usable_record_count": usable_record_count,
        "source_coverage": metadata.get("source_coverage") or {},
        "record_completeness": metadata.get("record_completeness") or {},
        "source_freshness": metadata.get("source_freshness") or {},
        "template_status": template_validation.get("status", ""),
        "missing_collectors": template_validation.get("missing_collectors") or [],
        "empty_record_count": empty_record_count,
    }
    fields.update(
        assess_report_quality(
            fields,
            include_follow_up_actions=include_follow_up_actions,
        )
    )
    return {
        key: value
        for key, value in fields.items()
        if value not in (None, "", {}, [])
    }


def assess_report_quality(
    fields: dict[str, Any],
    *,
    include_follow_up_actions: bool = True,
) -> dict[str, Any]:
    """Derive human and machine-readable quality guidance from report metadata fields."""
    template_status = str(fields.get("template_status") or "").lower().strip()
    source_record_count = _quality_int(
        fields.get("source_record_count"),
        default=_quality_int(fields.get("matched_records")),
    )
    usable_record_count = _quality_int(
        fields.get("usable_record_count"),
        default=source_record_count,
    )
    empty_record_count = _quality_int(fields.get("empty_record_count"))
    missing_collectors = [
        str(collector)
        for collector in fields.get("missing_collectors", [])
        if str(collector or "").strip()
    ]
    missing_labels = [source_label(collector) for collector in missing_collectors]
    source_coverage = fields.get("source_coverage")
    if not isinstance(source_coverage, dict):
        source_coverage = {}
    source_freshness = fields.get("source_freshness")
    if not isinstance(source_freshness, dict):
        source_freshness = {}
    max_age_days = _quality_int(source_freshness.get("max_age_days"), default=-1)
    freshness_warning_days = _quality_int(source_freshness.get("warning_days"), default=30)
    is_stale = (
        max_age_days >= 0
        and freshness_warning_days > 0
        and max_age_days > freshness_warning_days
    )

    risks: list[str] = []
    if source_record_count <= 0:
        risks.append("No source records were selected for this report.")
    if usable_record_count <= 0:
        risks.append("No usable source records were available for report generation.")
    if missing_collectors:
        risks.append(
            "Template coverage is missing required sources: "
            + ", ".join(missing_labels)
            + "."
        )
    if empty_record_count:
        risks.append(f"{empty_record_count} selected records had no usable data.")
    if usable_record_count > 0 and not source_coverage:
        risks.append("Source coverage metadata is empty; verify selected records.")
    if template_status in {"unchecked", "unknown"}:
        risks.append("Template source validation was not completed for this report.")
    if is_stale:
        risks.append(
            f"Oldest selected source record is {max_age_days} days old; "
            "refresh source data if this report needs current signals."
        )

    if usable_record_count <= 0:
        quality_status = "empty"
        quality_summary = (
            "No usable source records were available; collect source data before "
            "relying on this report."
        )
    elif missing_collectors or template_status == "partial" or empty_record_count:
        quality_status = "partial"
        if missing_labels:
            quality_summary = (
                "Report was generated with partial source coverage; missing sources: "
                + ", ".join(missing_labels)
                + "."
            )
        else:
            quality_summary = (
                "Report was generated from usable records, but some selected records "
                "had no usable data."
            )
    elif template_status == "complete":
        if is_stale:
            quality_status = "stale"
            quality_summary = (
                "Report source coverage is complete, but selected source data may be stale."
            )
        else:
            quality_status = "complete"
            quality_summary = "Report source coverage is complete for the selected template."
    elif template_status == "unchecked":
        quality_status = "unchecked"
        quality_summary = "Report was generated, but template coverage was not validated."
    else:
        quality_status = template_status or "unknown"
        quality_summary = "Report was generated, but quality metadata is incomplete."

    regeneration_recommended = bool(
        usable_record_count <= 0
        or missing_collectors
        or template_status == "partial"
        or empty_record_count
        or is_stale
    )
    guidance: dict[str, Any] = {
        "quality_status": quality_status,
        "quality_summary": quality_summary,
        "regeneration_recommended": regeneration_recommended,
        "coverage_risks": risks,
    }
    if include_follow_up_actions:
        guidance["follow_up_actions"] = _report_quality_follow_up_actions(
            source_record_count=source_record_count,
            usable_record_count=usable_record_count,
            empty_record_count=empty_record_count,
            is_stale=is_stale,
            max_age_days=max_age_days,
            missing_collectors=missing_collectors,
            missing_labels=missing_labels,
            template_status=template_status,
            regeneration_recommended=regeneration_recommended,
        )
    return guidance


def _quality_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _report_quality_follow_up_actions(
    *,
    source_record_count: int,
    usable_record_count: int,
    empty_record_count: int,
    is_stale: bool,
    max_age_days: int,
    missing_collectors: list[str],
    missing_labels: list[str],
    template_status: str,
    regeneration_recommended: bool,
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if source_record_count <= 0 or usable_record_count <= 0:
        actions.append(
            {
                "type": "select_or_collect_source_records",
                "recommended_tool": "search_data",
                "reason": "Find usable source records or create collection tasks before regenerating.",
            }
        )
    if missing_collectors:
        actions.append(
            {
                "type": "collect_missing_sources",
                "recommended_tool": "precheck_report",
                "missing_collectors": missing_collectors,
                "missing_labels": missing_labels,
                "reason": (
                    "Run precheck_report to get executable collection drafts for the "
                    "missing sources."
                ),
            }
        )
    if empty_record_count:
        actions.append(
            {
                "type": "replace_empty_records",
                "recommended_tool": "search_data",
                "empty_record_count": empty_record_count,
                "reason": "Select usable records or rerun collectors for empty records.",
            }
        )
    if template_status in {"unchecked", "unknown"}:
        actions.append(
            {
                "type": "verify_template_coverage",
                "recommended_tool": "precheck_report",
                "reason": "Verify source coverage against the selected report template.",
            }
        )
    if is_stale:
        actions.append(
            {
                "type": "refresh_stale_sources",
                "recommended_tool": "precheck_report",
                "max_age_days": max_age_days,
                "reason": "Refresh source data before regenerating time-sensitive reports.",
            }
        )
    if regeneration_recommended:
        actions.append(
            {
                "type": "regenerate_report",
                "recommended_tool": "generate_report",
                "reason": "Regenerate after follow-up collection or record selection.",
            }
        )
    return actions
