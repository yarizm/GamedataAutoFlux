"""Shared utility functions used by services, routes, and agent tools."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any


# ---------------------------------------------------------------------------
# Deep dict access
# ---------------------------------------------------------------------------


def nested_get(data: dict[str, Any], *keys: str) -> Any:
    """Traverse nested dicts by key path; returns None for any missing intermediate."""
    node: Any = data
    for key in keys:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


# ---------------------------------------------------------------------------
# First non-empty value
# ---------------------------------------------------------------------------


def first_str(*values: Any) -> str:
    """Return the first value that is neither None nor empty string."""
    for v in values:
        if v not in (None, ""):
            return str(v)
    return ""


# ---------------------------------------------------------------------------
# Record identity extraction (used by data routes, agent tools, reports)
# ---------------------------------------------------------------------------


def extract_record_identity(record: Any) -> dict[str, str] | None:
    """Extract game_name, app_id, collector, data_source from a StorageRecord."""
    data = record.data if hasattr(record, "data") else None
    meta = record.metadata if isinstance(getattr(record, "metadata", None), dict) else {}

    if not isinstance(data, dict):
        return None

    collector = first_str(
        data.get("collector"),
        nested_get(data, "content", "collector"),
        nested_get(data, "source_meta", "collector"),
        meta.get("collector"),
        nested_get(meta, "source_task", "collector_name"),
    ) or detect_collector(data)

    app_id = first_str(
        data.get("app_id"),
        nested_get(data, "snapshot", "app_id"),
        nested_get(data, "source_meta", "app_id"),
        nested_get(data, "content", "app_id"),
        nested_get(data, "content", "snapshot", "app_id"),
        nested_get(data, "game", "app_id"),
        nested_get(data, "game", "id"),
        meta.get("app_id"),
        nested_get(meta, "source_task", "target_params", "app_id"),
    )

    game_name = first_str(
        meta.get("display_name"),
        data.get("game_name"),
        nested_get(data, "snapshot", "name"),
        nested_get(data, "content", "game_name"),
        nested_get(data, "content", "snapshot", "name"),
        nested_get(data, "game", "title"),
        data.get("keyword"),
        meta.get("target"),
        nested_get(meta, "source_task", "target"),
        nested_get(meta, "source_task", "task_name"),
        nested_get(meta, "group_name"),
    )

    if not app_id and not game_name:
        return None

    # Prefer game_name as grouping key — same game across platforms shares one group
    game_key = f"name:{normalize_key(game_name)}" if game_name else f"app:{app_id}"
    slabel = collector or getattr(record, "source", "") or "unknown"
    return {
        "game_key": game_key,
        "game_name": game_name or app_id or "Unknown",
        "app_id": app_id or "",
        "collector": slabel,
        "data_source": source_label(slabel),
    }


# ---------------------------------------------------------------------------
# Collector detection / label
# ---------------------------------------------------------------------------


def detect_collector(data: dict[str, Any]) -> str:
    """Guess the collector type from data shape."""
    if "discussions" in data:
        return "steam_discussions"
    if "steamdb" in data or "news" in data:
        return "steam"
    if "reviews_summary" in data or "availability" in data:
        return "taptap"
    if "trend_history" in data:
        return "gtrends"
    if "events" in data or "event_history" in data:
        return "events"
    if "monitor_metrics" in data or "metrics" in data:
        return "monitor"
    if ("js_script" in data or data.get("extraction_mode") in ("js_evaluate", "css_selectors")) and "url" in data:
        return "dynamic_playwright"
    return "unknown"


def source_label(collector: str) -> str:
    labels: dict[str, str] = {
        "steam": "Steam",
        "steam_discussions": "Steam Community Discussions",
        "taptap": "TapTap",
        "gtrends": "Google Trends",
        "monitor": "Monitor",
        "events": "Events",
        "official_site": "official website",
        "qimai": "Qimai/App Store",
        "dynamic_playwright": "Dynamic Web Scraper",
    }
    return labels.get(collector, collector or "unknown")


# ---------------------------------------------------------------------------
# Record group extraction
# ---------------------------------------------------------------------------


def record_group(record: Any) -> dict[str, str]:
    metadata = record.metadata if isinstance(getattr(record, "metadata", None), dict) else {}
    group_id = str(metadata.get("group_id", "") or "").strip()
    group_name = str(metadata.get("group_name", "") or "").strip()
    return {"group_id": group_id or group_name, "group_name": group_name or group_id}


# ---------------------------------------------------------------------------
# Completeness scoring
# ---------------------------------------------------------------------------


def compute_record_completeness(record: Any) -> str:
    """full / partial / empty based on game + metrics + content dimensions."""
    data = record.data if hasattr(record, "data") else None
    if not isinstance(data, dict) or not data:
        return "empty"

    has_game = bool(
        data.get("game_name")
        or nested_get(data, "snapshot", "name")
        or nested_get(data, "game", "title")
    )
    has_metrics = bool(
        data.get("snapshot")
        or data.get("steam_api")
        or data.get("reviews_summary")
        or data.get("monitor_metrics")
        or data.get("discussions")
    )
    has_content = bool(
        data.get("items") or data.get("news") or data.get("updates") or data.get("reviews")
    )

    score = sum([has_game, has_metrics, has_content])
    if score >= 2:
        return "full"
    if score == 1:
        return "partial"
    return "empty"


# ---------------------------------------------------------------------------
# Summary building
# ---------------------------------------------------------------------------


def build_record_summary(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    snapshot = data.get("snapshot") if isinstance(data.get("snapshot"), dict) else {}
    discussions = data.get("discussions") if isinstance(data.get("discussions"), dict) else {}
    reviews = data.get("reviews") if isinstance(data.get("reviews"), dict) else {}
    monitor_metrics = (
        data.get("monitor_metrics") if isinstance(data.get("monitor_metrics"), dict) else {}
    )
    summary: dict[str, Any] = {}

    for key in (
        "current_players",
        "total_reviews",
        "review_score",
        "price",
        "score",
        "latest_topic_at",
    ):
        if snapshot.get(key) not in (None, ""):
            summary[key] = snapshot[key]
    if snapshot.get("latest_twitch_average_viewers") not in (None, ""):
        summary["latest_twitch_average_viewers"] = snapshot.get("latest_twitch_average_viewers")
    twitch = monitor_metrics.get("twitch_viewer_trend") if isinstance(monitor_metrics, dict) else {}
    if isinstance(twitch, dict) and twitch.get("latest_average_viewers") not in (None, ""):
        summary["latest_twitch_average_viewers"] = twitch.get("latest_average_viewers")
    if discussions:
        summary["topic_count"] = discussions.get("topic_count")
        summary["post_count"] = discussions.get("post_count")
    if reviews:
        if reviews.get("total") is not None:
            summary["review_count"] = reviews.get("total")
        if reviews.get("ratings_count") is not None:
            summary["ratings_count"] = reviews.get("ratings_count")
    return {key: value for key, value in summary.items() if value not in (None, "")}


# ---------------------------------------------------------------------------
# String key normalization
# ---------------------------------------------------------------------------


def normalize_key(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-") or "unknown"


# ---------------------------------------------------------------------------
# Date helpers (used by refresh-task building)
# ---------------------------------------------------------------------------


def max_iso(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    try:
        return max(datetime.fromisoformat(left), datetime.fromisoformat(right)).isoformat()
    except ValueError:
        return max(left, right)


def parse_date_prefix(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def replace_date_prefix(original: str, value: date) -> str:
    if len(original) > 10:
        return value.isoformat() + original[10:]
    return value.isoformat()


def roll_time_params(params: dict[str, Any]) -> None:
    today = date.today()
    for start_key, end_key in (("start_time", "end_time"), ("start_date", "end_date")):
        start_raw = params.get(start_key)
        end_raw = params.get(end_key)
        if not start_raw or not end_raw:
            continue
        start_date = parse_date_prefix(start_raw)
        end_date = parse_date_prefix(end_raw)
        if start_date is None or end_date is None:
            continue
        window = max((end_date - start_date).days, 0)
        params[start_key] = replace_date_prefix(str(start_raw), today - timedelta(days=window))
        params[end_key] = replace_date_prefix(str(end_raw), today)
