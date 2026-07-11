"""NL / raw cron schedule parsing for Agent cron_workflow (P3)."""

from __future__ import annotations

import re
from typing import Any

from src.core.cron_schedule import (
    compile_preset,
    default_timezone,
    describe_cron,
    next_run_times,
    validate_cron_expr,
)

_RAW_CRON_PATTERN = re.compile(
    r"(?<![\w./-])"
    r"([*\d/,\-]+(?:\s+[*\d/,a-zA-Z\-]+){4})"
    r"(?![\w./-])"
)

_DAILY_TIME_PATTERNS = (
    re.compile(
        r"每天(?:上午|下午|早上|晚上)?\s*(\d{1,2})\s*[:：点时]\s*(\d{1,2})?",
        re.IGNORECASE,
    ),
    re.compile(
        r"daily\s+(?:at\s+)?(\d{1,2})\s*[:：hH]\s*(\d{1,2})?",
        re.IGNORECASE,
    ),
    re.compile(
        r"每天\s*(\d{1,2})\s*[:：]\s*(\d{1,2})",
        re.IGNORECASE,
    ),
)

_WEEKDAY_ALIASES: tuple[tuple[str, str], ...] = (
    ("monday", "mon"),
    ("tuesday", "tue"),
    ("wednesday", "wed"),
    ("thursday", "thu"),
    ("friday", "fri"),
    ("saturday", "sat"),
    ("sunday", "sun"),
    ("周一", "mon"),
    ("星期二", "tue"),
    ("周二", "tue"),
    ("星期三", "wed"),
    ("周三", "wed"),
    ("星期四", "thu"),
    ("周四", "thu"),
    ("星期五", "fri"),
    ("周五", "fri"),
    ("星期六", "sat"),
    ("周六", "sat"),
    ("星期日", "sun"),
    ("周日", "sun"),
    ("星期天", "sun"),
    ("星期一", "mon"),
)

_WEEKLY_TIME = re.compile(
    r"(?:每周|every)\s*(?P<day>[一二三四五六日天]|monday|tuesday|wednesday|thursday|"
    r"friday|saturday|sunday|mon|tue|wed|thu|fri|sat|sun)"
    r"[^\d]{0,8}?(?P<h>\d{1,2})\s*[:：点时]?\s*(?P<m>\d{1,2})?",
    re.IGNORECASE,
)

_HOURLY = re.compile(
    r"(?:每小时|hourly|every\s+hour)(?:[^\d]{0,6}(?P<m>\d{1,2})\s*分)?",
    re.IGNORECASE,
)

_EVERY_MINUTES = re.compile(
    r"(?:每|每隔|every)\s*(?P<n>1|5|10|15|20|30)\s*(?:分钟|min(?:ute)?s?)",
    re.IGNORECASE,
)

_MONTHLY = re.compile(
    r"(?:每月|monthly)\s*(?P<d>\d{1,2})\s*(?:号|日)?"
    r"[^\d]{0,6}?(?P<h>\d{1,2})\s*[:：点时]?\s*(?P<m>\d{1,2})?",
    re.IGNORECASE,
)

_PIPELINE_PATTERNS = (
    re.compile(
        r"(?:pipeline|流水线)\s*[:：#]?\s*[`'\"]?([A-Za-z0-9][\w.-]{0,63})[`'\"]?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:跑|执行|用|运行)\s*[`'\"]([A-Za-z0-9][\w.-]{0,63})[`'\"]",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:跑|执行|用|运行)\s+([A-Za-z][A-Za-z0-9_.-]{1,63})\b",
        re.IGNORECASE,
    ),
)

_JOB_NAME_PATTERNS = (
    re.compile(
        r"(?:名称|name|任务名|job)\s*[:：#]?\s*[`'\"]?([A-Za-z0-9][\w.-]{0,63})[`'\"]?",
        re.IGNORECASE,
    ),
    re.compile(r"[`'\"]([A-Za-z0-9][\w.-]{2,63})[`'\"]"),
)

_TIMEZONE_PATTERN = re.compile(
    r"(?:时区|timezone)\s*[:：]?\s*([A-Za-z_]+/[A-Za-z_]+)",
    re.IGNORECASE,
)


def extract_pipeline_name(text: str) -> str:
    content = str(text or "")
    for pattern in _PIPELINE_PATTERNS:
        match = pattern.search(content)
        if match:
            name = match.group(1).strip("`'\"，。,.;")
            if name.lower() in {"pipeline", "cron", "task", "daily", "weekly"}:
                continue
            return name
    return ""


def extract_job_name(text: str, *, pipeline_name: str = "", cron_expr: str = "") -> str:
    content = str(text or "")
    for pattern in _JOB_NAME_PATTERNS:
        match = pattern.search(content)
        if not match:
            continue
        name = match.group(1).strip("`'\"，。,.;")
        lowered = name.lower()
        if lowered in {"pipeline", "cron", "confirm", "true", "false", "name"}:
            continue
        if pipeline_name and name == pipeline_name and "名称" not in content and "name" not in content.lower():
            # bare quoted pipeline alone is not job name
            continue
        return name
    if pipeline_name:
        slug = _schedule_slug(cron_expr) or "scheduled"
        return f"{pipeline_name}_{slug}"[:64].strip("_")
    return ""


def extract_timezone(text: str) -> str:
    match = _TIMEZONE_PATTERN.search(str(text or ""))
    if match:
        return match.group(1).strip()
    return default_timezone()


def parse_schedule(text: str, *, timezone: str | None = None) -> dict[str, Any]:
    """Parse user text into cron expr + meta.

    Returns keys: cron_expr, schedule_meta, human_schedule, next_runs, issues
    """
    content = str(text or "").strip()
    tz = (timezone or extract_timezone(content) or default_timezone()).strip()
    issues: list[str] = []

    raw = _extract_raw_cron(content)
    if raw:
        try:
            expr = validate_cron_expr(raw)
            return _schedule_success(expr, {"mode": "cron", "cron_expr": expr}, tz)
        except ValueError as exc:
            issues.append(f"Cron 表达式无效: {exc}")

    preset = _match_preset(content)
    if preset is not None:
        try:
            expr = compile_preset(preset)
            expr = validate_cron_expr(expr)
            return _schedule_success(
                expr,
                {"mode": "preset", "preset": preset},
                tz,
            )
        except ValueError as exc:
            issues.append(f"调度预设无效: {exc}")

    if _looks_like_schedule_attempt(content):
        issues.append("未能解析调度时间（支持：每天/每周/每月/每小时/每N分钟 或 5 段 cron）")

    return {
        "cron_expr": "",
        "schedule_meta": {},
        "human_schedule": "",
        "next_runs": [],
        "issues": issues,
        "timezone": tz,
    }


def _schedule_success(expr: str, meta: dict[str, Any], tz: str) -> dict[str, Any]:
    human = describe_cron(expr, timezone=tz)
    try:
        runs = next_run_times(expr, count=3, timezone=tz)
    except Exception:
        runs = []
    return {
        "cron_expr": expr,
        "schedule_meta": meta,
        "human_schedule": human,
        "next_runs": runs,
        "issues": [],
        "timezone": tz,
    }


def _extract_raw_cron(text: str) -> str:
    match = _RAW_CRON_PATTERN.search(text)
    if not match:
        return ""
    candidate = " ".join(match.group(1).split())
    parts = candidate.split()
    if len(parts) != 5:
        return ""
    return candidate


def _match_preset(text: str) -> dict[str, Any] | None:
    lowered = text.lower()

    minutes = _EVERY_MINUTES.search(text)
    if minutes:
        return {"type": "every_minutes", "interval": int(minutes.group("n"))}

    hourly = _HOURLY.search(text)
    if hourly and "每天" not in text and "daily" not in lowered:
        minute = int(hourly.group("m") or 0)
        return {"type": "hourly", "minute": minute}

    monthly = _MONTHLY.search(text)
    if monthly:
        hour = int(monthly.group("h") or 8)
        minute = int(monthly.group("m") or 0)
        return {
            "type": "monthly",
            "day_of_month": int(monthly.group("d")),
            "hour": hour,
            "minute": minute,
        }

    weekly = _WEEKLY_TIME.search(text)
    if weekly:
        day_raw = weekly.group("day")
        weekday = _normalize_weekday(day_raw)
        if weekday:
            return {
                "type": "weekly",
                "hour": int(weekly.group("h") or 8),
                "minute": int(weekly.group("m") or 0),
                "weekdays": [weekday],
            }

    for pattern in _DAILY_TIME_PATTERNS:
        match = pattern.search(text)
        if match:
            hour = int(match.group(1))
            minute = int(match.group(2) or 0)
            if "下午" in text or "晚上" in text:
                if hour < 12:
                    hour += 12
            if hour > 23:
                hour = 23
            return {"type": "daily", "hour": hour, "minute": minute}

    # 每天 8 点 without captured minute group variants already covered;
    # bare 每天早上 / 每天 without time → unsupported
    return None


def _normalize_weekday(raw: str) -> str:
    key = str(raw or "").strip().lower()
    mapping = {
        "一": "mon",
        "二": "tue",
        "三": "wed",
        "四": "thu",
        "五": "fri",
        "六": "sat",
        "日": "sun",
        "天": "sun",
        "mon": "mon",
        "tue": "tue",
        "wed": "wed",
        "thu": "thu",
        "fri": "fri",
        "sat": "sat",
        "sun": "sun",
    }
    if key in mapping:
        return mapping[key]
    for alias, code in _WEEKDAY_ALIASES:
        if alias in key or key in alias:
            return code
    return ""


def _looks_like_schedule_attempt(text: str) -> bool:
    lowered = str(text or "").lower()
    markers = (
        "每天",
        "每周",
        "每月",
        "每小时",
        "每隔",
        "分钟",
        "daily",
        "weekly",
        "hourly",
        "cron",
        "点",
        ":",
    )
    return any(m in lowered or m in text for m in markers)


def _schedule_slug(cron_expr: str) -> str:
    expr = str(cron_expr or "").strip()
    if not expr:
        return "scheduled"
    parts = expr.split()
    if len(parts) == 5:
        minute, hour, day, _month, dow = parts
        if day == "*" and dow == "*" and hour.isdigit() and minute.isdigit():
            return f"daily_{int(hour):02d}{int(minute):02d}"
        if day == "*" and dow != "*" and hour.isdigit():
            return f"weekly_{dow}_{int(hour):02d}"
        if day.isdigit() and hour.isdigit():
            return f"monthly_d{day}"
        if hour == "*" and minute.startswith("*/"):
            return f"every_{minute[2:]}m"
        if hour == "*" and minute.isdigit():
            return "hourly"
    cleaned = re.sub(r"[^a-z0-9]+", "_", expr.lower()).strip("_")
    return cleaned[:24] or "scheduled"
