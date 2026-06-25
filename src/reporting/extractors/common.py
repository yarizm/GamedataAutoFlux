"""Common helpers shared by reporting extractors."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any


def list_items(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def safe_int(value: Any) -> int | str:
    """Convert to int when possible, otherwise preserve a display-safe string."""
    if value is None:
        return ""
    try:
        return int(value)
    except (ValueError, TypeError):
        return str(value)


def truncate(text: str, max_len: int = 500) -> str:
    """Truncate long free-form text for report cells."""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def extract_time(data: dict[str, Any]) -> str:
    """Read collected_at from known metadata locations."""
    source_meta = data.get("source_meta", {})
    if isinstance(source_meta, dict) and source_meta.get("collected_at"):
        return str(source_meta["collected_at"])

    metadata = data.get("metadata", {})
    if isinstance(metadata, dict) and metadata.get("collected_at"):
        return str(metadata["collected_at"])

    return ""


def pivot_monitor_daily_rows(
    *,
    game_name: str,
    app_id: str | int,
    metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = {}

    def row_for(date_value: Any) -> dict[str, Any]:
        date_text = str(date_value or "")
        return by_date.setdefault(
            date_text,
            {
                "游戏名": game_name,
                "App ID": app_id,
                "日期": date_text,
            },
        )

    twitch_payload = metrics.get("twitch_viewer_trend")
    if isinstance(twitch_payload, dict):
        for item in twitch_payload.get("daily_rows", []) or []:
            if not isinstance(item, dict):
                continue
            row = row_for(item.get("date"))
            row["Twitch平均观看"] = item.get("average_viewers")
            row["Twitch峰值观看"] = item.get("peak_viewers")

    return [row for date_text, row in sorted(by_date.items()) if date_text]


def twitch_average_last_days(metrics: dict[str, Any], days: int) -> int | str:
    twitch = metrics.get("twitch_viewer_trend")
    if not isinstance(twitch, dict):
        return ""
    rows = twitch.get("daily_rows", [])
    if not isinstance(rows, list):
        return ""
    values = [
        row.get("average_viewers")
        for row in rows[-days:]
        if isinstance(row, dict) and isinstance(row.get("average_viewers"), (int, float))
    ]
    return round(sum(values) / len(values)) if values else ""


def twitch_trend_summary(metrics: dict[str, Any]) -> str:
    twitch = metrics.get("twitch_viewer_trend")
    if not isinstance(twitch, dict):
        return ""
    rows = twitch.get("daily_rows", [])
    if not isinstance(rows, list) or not rows:
        return ""
    return f"{min(len(rows), 90)} daily points"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def first_number(data: dict[str, Any], *keys: str) -> int | float | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, bool) or value in (None, ""):
            continue
        if isinstance(value, (int, float)):
            return value
        try:
            text = str(value).replace(",", "").strip()
            if "." in text:
                return float(text)
            return int(text)
        except (TypeError, ValueError):
            continue
    return None


def format_percent(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str) and value.endswith("%"):
        return value
    return f"{value}%"


def parse_datetime_value(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        try:
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%B %Y", "%b %Y"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def parse_date_only(value: Any) -> date | None:
    dt = parse_datetime_value(value)
    return dt.date() if dt is not None else None
