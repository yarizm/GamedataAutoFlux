"""Task execution retry policy helpers."""

from __future__ import annotations

from typing import Any

from src.core.pipeline import PipelineResult


def pipeline_result_retry_suppression_reason(result: PipelineResult) -> str:
    """Return a reason when task-level retry would likely duplicate stored partial data."""
    if not has_stored_partial_collection_result(result):
        return ""
    summary = result.collection_summary
    failed_count = _safe_int(summary.get("failed_targets_count"))
    stored_count = int(getattr(result, "storage_count", 0) or 0)
    output_count = len(getattr(result, "output_records", []) or [])
    return (
        "Partial collection already produced stored records "
        f"(stored={stored_count}, output_records={output_count}, failed_targets={failed_count}). "
        "Review collection failures and create targeted follow-up tasks instead of retrying "
        "the whole pipeline."
    )


def has_stored_partial_collection_result(result: PipelineResult) -> bool:
    """Return true when partial collection already produced durable output."""
    stored_count = int(getattr(result, "storage_count", 0) or 0)
    output_records = getattr(result, "output_records", []) or []
    if stored_count <= 0 and not output_records:
        return False

    summary = getattr(result, "collection_summary", {})
    if not isinstance(summary, dict):
        return False
    return summary.get("status") == "partial" and _safe_int(summary.get("failed_targets_count")) > 0


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default
