"""Cron schedule helpers: preset compilation, human labels, next run preview.

Canonical storage remains a 5-field cron expression. Presets compile to cron.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.triggers.cron import CronTrigger

# APScheduler uses 0-6 mon-sun or mon,tue... — we use mon-sun names for clarity
_WEEKDAY_NAMES = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_WEEKDAY_CN = {
    "mon": "一",
    "tue": "二",
    "wed": "三",
    "thu": "四",
    "fri": "五",
    "sat": "六",
    "sun": "日",
}

DEFAULT_TIMEZONE = "Asia/Shanghai"


def default_timezone() -> str:
    from src.core.config import get as get_config

    tz = str(get_config("scheduler.cron_timezone", "") or "").strip()
    return tz or DEFAULT_TIMEZONE


def resolve_timezone(name: str | None = None) -> ZoneInfo:
    tz_name = (name or default_timezone()).strip() or DEFAULT_TIMEZONE
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown timezone: {tz_name}") from exc


def validate_cron_expr(cron_expr: str) -> str:
    """Validate and normalize a 5-field cron expression."""
    expr = str(cron_expr or "").strip()
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression (need 5 fields): {cron_expr}")
    # Ensure CronTrigger accepts it
    try:
        CronTrigger.from_crontab(expr, timezone=resolve_timezone())
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Invalid cron expression: {cron_expr} ({exc})") from exc
    return expr


def compile_preset(preset: dict[str, Any]) -> str:
    """Compile a visual preset dict into a 5-field cron expression.

    Supported types:
      - every_minutes: {type, interval}  interval in {1,5,10,15,20,30}
      - every_hours:   {type, interval}  interval >= 1
      - daily:         {type, hour, minute}
      - weekly:        {type, hour, minute, weekdays: ["mon",...]}
      - monthly:       {type, hour, minute, day_of_month: 1-31}
      - hourly:        {type, minute}  every hour at minute
    """
    if not isinstance(preset, dict):
        raise ValueError("preset must be an object")
    ptype = str(preset.get("type") or "").strip().lower()
    if not ptype:
        raise ValueError("preset.type is required")

    if ptype == "every_minutes":
        interval = int(preset.get("interval") or 0)
        if interval not in {1, 5, 10, 15, 20, 30}:
            raise ValueError("every_minutes.interval must be one of 1,5,10,15,20,30")
        if interval == 1:
            return "* * * * *"
        return f"*/{interval} * * * *"

    if ptype == "hourly" or ptype == "every_hours":
        if ptype == "hourly":
            minute = int(preset.get("minute") or 0)
            if minute < 0 or minute > 59:
                raise ValueError("hourly.minute must be 0-59")
            return f"{minute} * * * *"
        interval = int(preset.get("interval") or 1)
        if interval < 1 or interval > 23:
            raise ValueError("every_hours.interval must be 1-23")
        minute = int(preset.get("minute") or 0)
        if minute < 0 or minute > 59:
            raise ValueError("every_hours.minute must be 0-59")
        if interval == 1:
            return f"{minute} * * * *"
        return f"{minute} */{interval} * * *"

    if ptype == "daily":
        hour, minute = _hour_minute(preset)
        return f"{minute} {hour} * * *"

    if ptype == "weekly":
        hour, minute = _hour_minute(preset)
        weekdays = preset.get("weekdays") or []
        if isinstance(weekdays, str):
            weekdays = [weekdays]
        normalized = []
        for day in weekdays:
            key = str(day).strip().lower()[:3]
            if key not in _WEEKDAY_NAMES:
                # allow 0-6 mon=0
                if key.isdigit() and 0 <= int(key) <= 6:
                    key = _WEEKDAY_NAMES[int(key)]
                else:
                    raise ValueError(f"Invalid weekday: {day}")
            if key not in normalized:
                normalized.append(key)
        if not normalized:
            raise ValueError("weekly.weekdays is required")
        return f"{minute} {hour} * * {','.join(normalized)}"

    if ptype == "monthly":
        hour, minute = _hour_minute(preset)
        dom = int(preset.get("day_of_month") or preset.get("day") or 1)
        if dom < 1 or dom > 31:
            raise ValueError("monthly.day_of_month must be 1-31")
        return f"{minute} {hour} {dom} * *"

    raise ValueError(f"Unsupported preset.type: {ptype}")


def _hour_minute(preset: dict[str, Any]) -> tuple[int, int]:
    # Accept time "HH:MM" or separate hour/minute
    time_str = str(preset.get("time") or "").strip()
    if time_str and ":" in time_str:
        parts = time_str.split(":")
        hour = int(parts[0])
        minute = int(parts[1])
    else:
        hour = int(preset.get("hour") if preset.get("hour") is not None else 8)
        minute = int(preset.get("minute") if preset.get("minute") is not None else 0)
    if hour < 0 or hour > 23:
        raise ValueError("hour must be 0-23")
    if minute < 0 or minute > 59:
        raise ValueError("minute must be 0-59")
    return hour, minute


def describe_cron(cron_expr: str, *, timezone: str | None = None) -> str:
    """Human-readable Chinese description of a 5-field cron (best-effort)."""
    try:
        expr = validate_cron_expr(cron_expr)
    except ValueError:
        return str(cron_expr or "").strip() or "无效表达式"

    minute, hour, day, month, dow = expr.split()
    tz = timezone or default_timezone()

    # Common patterns
    if minute.startswith("*/") and hour == "*" and day == "*" and month == "*" and dow == "*":
        return f"每 {minute[2:]} 分钟 ({tz})"
    if minute == "*" and hour == "*" and day == "*" and month == "*" and dow == "*":
        return f"每分钟 ({tz})"
    if hour == "*" and day == "*" and month == "*" and dow == "*" and minute.isdigit():
        return f"每小时的第 {int(minute)} 分 ({tz})"
    if hour.startswith("*/") and day == "*" and month == "*" and dow == "*":
        return f"每 {hour[2:]} 小时（第 {minute} 分）({tz})"
    if day == "*" and month == "*" and dow == "*" and minute.isdigit() and hour.isdigit():
        return f"每天 {int(hour):02d}:{int(minute):02d} ({tz})"
    if day == "*" and month == "*" and dow != "*" and minute.isdigit() and hour.isdigit():
        days = []
        for part in dow.split(","):
            key = part.strip().lower()[:3]
            days.append(_WEEKDAY_CN.get(key, part))
        return f"每周{'、'.join(days)} {int(hour):02d}:{int(minute):02d} ({tz})"
    if day.isdigit() and month == "*" and dow == "*" and minute.isdigit() and hour.isdigit():
        return f"每月 {int(day)} 日 {int(hour):02d}:{int(minute):02d} ({tz})"
    return f"{expr} ({tz})"


def next_run_times(
    cron_expr: str,
    *,
    count: int = 5,
    timezone: str | None = None,
    start: datetime | None = None,
) -> list[str]:
    """Return next ``count`` fire times as ISO strings."""
    expr = validate_cron_expr(cron_expr)
    tz = resolve_timezone(timezone)
    trigger = CronTrigger.from_crontab(expr, timezone=tz)
    cursor = start or datetime.now(tz)
    previous = None
    results: list[str] = []
    for _ in range(max(1, min(count, 20))):
        nxt = trigger.get_next_fire_time(previous, cursor)
        if nxt is None:
            break
        results.append(nxt.isoformat())
        previous = nxt
        cursor = nxt
    return results


def resolve_schedule_input(
    *,
    cron_expr: str | None = None,
    schedule: dict[str, Any] | None = None,
    timezone: str | None = None,
) -> dict[str, Any]:
    """Normalize create/update input into canonical cron fields.

    Accepts either:
      - cron_expr: "0 8 * * *"
      - schedule: {mode: preset|cron, preset: {...}, cron_expr: "...", timezone: "..."}
    """
    schedule = schedule if isinstance(schedule, dict) else {}
    mode = str(schedule.get("mode") or "").strip().lower()
    tz = (
        str(timezone or schedule.get("timezone") or "").strip()
        or default_timezone()
    )
    # resolve timezone early
    resolve_timezone(tz)

    schedule_meta: dict[str, Any] = {}
    expr = ""

    if mode == "preset" or (not cron_expr and schedule.get("preset")):
        preset = schedule.get("preset") if isinstance(schedule.get("preset"), dict) else {}
        expr = compile_preset(preset)
        schedule_meta = {"mode": "preset", "preset": dict(preset), "timezone": tz}
    else:
        expr = str(cron_expr or schedule.get("cron_expr") or "").strip()
        if not expr:
            raise ValueError("cron_expr or schedule.preset is required")
        expr = validate_cron_expr(expr)
        schedule_meta = {
            "mode": "cron",
            "timezone": tz,
        }
        if schedule.get("preset"):
            schedule_meta["preset"] = schedule.get("preset")

    human = describe_cron(expr, timezone=tz)
    schedule_meta["human_label"] = human
    return {
        "cron_expr": expr,
        "timezone": tz,
        "human_label": human,
        "schedule_meta": schedule_meta,
    }


def build_cron_public_view(
    *,
    name: str,
    pipeline_name: str,
    cron_expr: str,
    task_template: dict[str, Any] | None = None,
    enabled: bool = True,
    timezone: str | None = None,
    schedule_meta: dict[str, Any] | None = None,
    description: str = "",
    next_run: str | None = None,
    job_id: str | None = None,
    trigger: str | None = None,
    include_next_runs: int = 5,
) -> dict[str, Any]:
    """Build enriched list/detail payload for API consumers."""
    tz = timezone or (schedule_meta or {}).get("timezone") or default_timezone()
    human = ""
    if isinstance(schedule_meta, dict):
        human = str(schedule_meta.get("human_label") or "")
    if not human:
        human = describe_cron(cron_expr, timezone=tz)

    template = task_template if isinstance(task_template, dict) else {}
    targets = template.get("targets") if isinstance(template.get("targets"), list) else []
    config = template.get("config") if isinstance(template.get("config"), dict) else {}
    refresh = config.get("refresh") if isinstance(config.get("refresh"), dict) else {}

    next_runs: list[str] = []
    try:
        next_runs = next_run_times(cron_expr, count=include_next_runs, timezone=tz)
    except Exception:
        next_runs = []

    return {
        "id": job_id or name,
        "name": name,
        "description": description,
        "pipeline_name": pipeline_name,
        "cron_expr": cron_expr,
        "timezone": tz,
        "human_label": human,
        "enabled": enabled,
        "schedule_meta": schedule_meta or {"mode": "cron", "timezone": tz, "human_label": human},
        "task_template": template,
        "targets_count": len(targets),
        "rolling_window": bool(refresh.get("rolling_window")),
        "next_run": next_run or (next_runs[0] if next_runs else None),
        "next_runs": next_runs,
        "trigger": trigger or cron_expr,
        "kind": refresh.get("refresh_kind", "cron"),
    }
