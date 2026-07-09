"""Static DAG / pipeline graph checks for task precheck (no runtime execution)."""

from __future__ import annotations

from typing import Any


def normalize_from_upstream(raw: Any) -> dict[str, Any] | None:
    """Normalize from_upstream config; return None if not enabled."""
    if raw is None or raw is False:
        return None
    if raw is True:
        return {"auto": True}
    if isinstance(raw, dict):
        return dict(raw)
    return None


def validate_from_upstream_config(
    from_upstream: Any,
    *,
    collector_id: str = "",
    field_prefix: str = "",
) -> list[dict[str, Any]]:
    """Validate a single from_upstream config block.

    Returns list of issue dicts with keys:
    level, code, field, message, collector_id, category, suggested_action
    """
    cfg = normalize_from_upstream(from_upstream)
    if cfg is None:
        return []

    prefix = field_prefix or f"pipeline.steps.collector[{collector_id}].config.from_upstream"
    issues: list[dict[str, Any]] = []

    field_map = cfg.get("map") if isinstance(cfg.get("map"), dict) else None
    auto = bool(cfg.get("auto", field_map is None))

    if field_map is not None and not field_map and not auto:
        issues.append(
            {
                "level": "error",
                "code": "empty_from_upstream_map",
                "field": f"{prefix}.map",
                "message": (
                    f"{collector_id or 'collector'} has from_upstream.map but no field mappings "
                    "and auto mode is disabled."
                ),
                "collector_id": collector_id,
                "category": "graph",
                "suggested_action": "Add map entries or set from_upstream.auto=true.",
            }
        )
    elif field_map is not None and not field_map and auto:
        issues.append(
            {
                "level": "warning",
                "code": "empty_from_upstream_map_auto",
                "field": f"{prefix}.map",
                "message": (
                    f"{collector_id or 'collector'} has an empty from_upstream.map; "
                    "auto field extraction will be used."
                ),
                "collector_id": collector_id,
                "category": "graph",
                "suggested_action": "Prefer explicit map keys for stable chaining.",
            }
        )

    if field_map:
        for target_key, source_key in field_map.items():
            if not str(target_key or "").strip() or not str(source_key or "").strip():
                issues.append(
                    {
                        "level": "error",
                        "code": "invalid_from_upstream_map_entry",
                        "field": f"{prefix}.map",
                        "message": (
                            f"{collector_id or 'collector'} from_upstream.map has an empty "
                            "source or target key."
                        ),
                        "collector_id": collector_id,
                        "category": "graph",
                        "suggested_action": "Use non-empty map keys, e.g. channel_id: channel_id.",
                    }
                )
                break

    return issues


def collector_uses_from_upstream(config: dict[str, Any] | None) -> bool:
    """Return True when collector is configured to take targets from upstream."""
    if not isinstance(config, dict):
        return False
    return normalize_from_upstream(config.get("from_upstream")) is not None


def validate_pipeline_collector_upstream(
    collectors: list[tuple[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Validate from_upstream on each collector step (name, config).

    Does not require a full DAG graph object — pipeline linear steps are enough
    for config-level checks. Edge existence for multi-source graphs is optional.
    """
    issues: list[dict[str, Any]] = []
    for name, config in collectors:
        if not collector_uses_from_upstream(config):
            continue
        issues.extend(
            validate_from_upstream_config(
                config.get("from_upstream"),
                collector_id=name,
            )
        )
    return issues
