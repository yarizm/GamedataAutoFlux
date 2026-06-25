"""Shared utility functions used by services, routes, and agent tools."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from src.core.sensitive import redact_sensitive_text


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
    if (
        "js_script" in data or data.get("extraction_mode") in ("js_evaluate", "css_selectors")
    ) and "url" in data:
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


def filter_records_by_data_source(records: list[Any], data_source: str) -> list[Any]:
    """Filter storage records by collector key, raw source, or human source label."""
    needle = normalize_source_token(data_source)
    if not needle:
        return filter_source_data_records(records)

    exact_matches = []
    relaxed_matches = []
    for record in filter_source_data_records(records):
        candidate_tokens = {
            normalize_source_token(value)
            for value in record_source_values(record)
            if str(value or "").strip()
        }
        if needle in candidate_tokens:
            exact_matches.append(record)
        elif len(needle) >= 6 and any(
            needle in token or token in needle for token in candidate_tokens
        ):
            relaxed_matches.append(record)

    return exact_matches or relaxed_matches


def filter_source_data_records(records: list[Any]) -> list[Any]:
    """Keep source data records and exclude generated report history records."""
    return [record for record in records if not is_report_history_record(record)]


def is_report_history_record(record: Any) -> bool:
    metadata = record.metadata if isinstance(getattr(record, "metadata", None), dict) else {}
    return (
        str(getattr(record, "source", "") or "").lower() == "reporting"
        or str(metadata.get("kind") or "").lower() == "report"
    )


def record_source_values(record: Any) -> list[str]:
    identity = extract_record_identity(record) or {}
    data = record.data if isinstance(getattr(record, "data", None), dict) else {}
    metadata = record.metadata if isinstance(getattr(record, "metadata", None), dict) else {}
    source_task = metadata.get("source_task", {}) if isinstance(metadata, dict) else {}
    if not isinstance(source_task, dict):
        source_task = {}

    values: list[Any] = [
        getattr(record, "source", ""),
        identity.get("collector", ""),
        identity.get("data_source", ""),
        data.get("collector", ""),
        nested_get(data, "content", "collector"),
        nested_get(data, "source_meta", "collector"),
        metadata.get("collector", ""),
        metadata.get("data_source", ""),
        source_task.get("collector_name", ""),
        source_task.get("pipeline_name", ""),
    ]
    for value in list(values):
        if value:
            values.append(source_label(str(value)))
    for container in (data, metadata, nested_get(data, "source_meta") or {}):
        if isinstance(container, dict):
            source_values = container.get("data_sources")
            if isinstance(source_values, list):
                values.extend(str(item) for item in source_values)
            elif source_values:
                values.append(str(source_values))
    return [str(value) for value in values if value not in (None, "")]


def normalize_source_token(value: str) -> str:
    token = str(value or "").strip()
    if token.lower().startswith("source:"):
        token = token[7:]
    return "".join(ch.lower() for ch in token if ch.isalnum())


def coerce_record_limit(
    value: Any,
    *,
    default: int,
    minimum: int = 1,
    maximum: int = 1000,
) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


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
# Report collection target context
# ---------------------------------------------------------------------------


_COLLECTOR_CONTEXT_ALIASES = {
    "steam_api": "steam",
    "steamdb": "steam",
    "steam_community": "steam_discussions",
    "steam_community_discussions": "steam_discussions",
    "steam_discussion": "steam_discussions",
    "google_trends": "gtrends",
    "pytrends": "gtrends",
    "official": "official_site",
    "official_website": "official_site",
    "official_site": "official_site",
    "event": "events",
    "event_data": "events",
    "qimai_app_store": "qimai",
    "qimai_appstore": "qimai",
}


def derive_collection_target_context(
    records: list[Any] | None,
    *,
    prompt: str = "",
    data_source: str = "",
) -> dict[str, Any]:
    """Derive reusable collection target hints from selected source records."""
    fallback_name = _collection_target_subject(prompt=prompt, data_source=data_source)
    record_keys: list[str] = []
    source_collectors: set[str] = set()
    game_names: list[str] = []
    ids: dict[str, str] = {}

    for record in records or []:
        key = redact_sensitive_text(str(getattr(record, "key", "") or "")).strip()
        if key:
            record_keys.append(key)

        data = record.data if isinstance(getattr(record, "data", None), dict) else {}
        metadata = record.metadata if isinstance(getattr(record, "metadata", None), dict) else {}
        source_task = metadata.get("source_task", {}) if isinstance(metadata, dict) else {}
        if not isinstance(source_task, dict):
            source_task = {}
        target_params = source_task.get("target_params", {})
        if not isinstance(target_params, dict):
            target_params = {}

        identity = extract_record_identity(record) or {}
        collector = _normalize_context_collector(
            first_str(
                identity.get("collector"),
                data.get("collector"),
                nested_get(data, "content", "collector"),
                nested_get(data, "source_meta", "collector"),
                metadata.get("collector"),
                source_task.get("collector_name"),
                getattr(record, "source", ""),
            )
        )
        if collector and collector != "unknown":
            source_collectors.add(collector)

        game_name = _context_str(
            identity.get("game_name"),
            metadata.get("display_name"),
            data.get("game_name"),
            nested_get(data, "snapshot", "name"),
            nested_get(data, "content", "game_name"),
            nested_get(data, "content", "snapshot", "name"),
            nested_get(data, "game", "title"),
            data.get("keyword"),
            metadata.get("target"),
            source_task.get("target"),
        )
        if game_name and game_name.lower() != "unknown":
            game_names.append(game_name)

        app_id = _context_str(
            identity.get("app_id"),
            target_params.get("app_id"),
            data.get("app_id"),
            nested_get(data, "snapshot", "app_id"),
            nested_get(data, "source_meta", "app_id"),
            nested_get(data, "content", "app_id"),
            nested_get(data, "content", "snapshot", "app_id"),
            nested_get(data, "game", "app_id"),
            nested_get(data, "game", "id"),
            metadata.get("app_id"),
        )
        qimai_app_id = _context_str(
            target_params.get("qimai_app_id"),
            data.get("qimai_app_id"),
            nested_get(data, "qimai", "app_id"),
        )
        siteurl = _context_str(
            target_params.get("siteurl"),
            data.get("siteurl"),
            nested_get(data, "source_meta", "siteurl"),
            nested_get(data, "snapshot", "siteurl"),
        )
        official_url = _context_str(
            target_params.get("official_url"),
            target_params.get("url"),
            data.get("official_url"),
            data.get("url"),
            nested_get(data, "source_meta", "entry_url"),
        )
        forum_url = _context_str(
            target_params.get("forum_url"),
            data.get("forum_url"),
            nested_get(data, "source_meta", "forum_url"),
        )

        if qimai_app_id:
            ids.setdefault("qimai_app_id", qimai_app_id)
        if siteurl:
            ids.setdefault("siteurl", siteurl)
        if official_url:
            ids.setdefault("official_url", official_url)
        if forum_url:
            ids.setdefault("forum_url", forum_url)

        if not app_id:
            continue
        if collector in {"steam", "steam_discussions"}:
            ids.setdefault("steam_app_id", app_id)
        elif collector == "monitor":
            ids.setdefault("monitor_app_id", app_id)
        elif collector == "taptap":
            ids.setdefault("taptap_app_id", app_id)
        elif collector == "qimai":
            ids.setdefault("qimai_app_id", qimai_app_id or app_id)
        else:
            ids.setdefault("app_id", app_id)

    game_name = game_names[0] if game_names else ""
    target_name = game_name or fallback_name
    context: dict[str, Any] = {
        "target_name": target_name,
        "params_by_collector": _collection_params_by_collector(ids),
    }
    if game_name:
        context["game_name"] = game_name
    for key, value in ids.items():
        if value:
            context[key] = value
    if source_collectors:
        context["source_collectors"] = sorted(source_collectors)
    if record_keys:
        context["source_record_keys"] = record_keys[:10]
        context["source_record_count"] = len(record_keys)
    return context


def _collection_params_by_collector(ids: dict[str, str]) -> dict[str, dict[str, str]]:
    steam_app_id = ids.get("steam_app_id") or ids.get("app_id") or ""
    taptap_app_id = ids.get("taptap_app_id") or ""
    qimai_app_id = ids.get("qimai_app_id") or ""
    monitor_app_id = ids.get("monitor_app_id") or steam_app_id
    siteurl = ids.get("siteurl") or ""
    official_url = ids.get("official_url") or ""
    forum_url = ids.get("forum_url") or ""

    return {
        "steam": _compact_context_params({"app_id": steam_app_id}),
        "steam_discussions": _compact_context_params(
            {
                "app_id": steam_app_id,
                "forum_url": forum_url,
            }
        ),
        "taptap": _compact_context_params({"app_id": taptap_app_id}),
        "monitor": _compact_context_params(
            {
                "app_id": monitor_app_id,
                "siteurl": siteurl,
            }
        ),
        "qimai": _compact_context_params(
            {
                "app_id": qimai_app_id,
                "qimai_app_id": qimai_app_id,
            }
        ),
        "official_site": _compact_context_params({"official_url": official_url}),
        "events": _compact_context_params({"official_url": official_url}),
        "gtrends": {},
    }


def _collection_target_subject(*, prompt: str = "", data_source: str = "") -> str:
    subject = redact_sensitive_text(str(data_source or "")).strip()
    if not subject:
        subject = redact_sensitive_text(str(prompt or "")).strip()[:80]
    return subject or "same game as selected records"


def _compact_context_params(params: dict[str, Any]) -> dict[str, str]:
    return {
        key: redact_sensitive_text(str(value)).strip()
        for key, value in params.items()
        if value not in (None, "") and str(value).strip()
    }


def _context_str(*values: Any) -> str:
    return redact_sensitive_text(first_str(*values)).strip()


def _normalize_context_collector(value: str) -> str:
    token = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip()).strip(
        "_"
    )
    while "__" in token:
        token = token.replace("__", "_")
    return _COLLECTOR_CONTEXT_ALIASES.get(token, token)


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


# ---------------------------------------------------------------------------
# Embeddings Factory
# ---------------------------------------------------------------------------


def get_embeddings() -> Any:
    """Return a configured Embeddings instance based on settings."""
    from src.core.config import get_settings
    from langchain_community.embeddings import DashScopeEmbeddings
    from loguru import logger

    settings = get_settings()
    llm_config = settings.get("llm", {}).get("qwen", {})
    api_key = llm_config.get("api_key")

    try:
        # For DashScope, base_url is typically handled via environment variable or client kwargs
        # if using the compatible mode. But standard DashScopeEmbeddings doesn't accept base_url easily.
        # The model "text-embedding-v2" is fixed for 1536 dims.
        return DashScopeEmbeddings(model="text-embedding-v2", dashscope_api_key=api_key)
    except Exception as e:
        logger.error(f"Failed to initialize embeddings: {e}")
        return None
