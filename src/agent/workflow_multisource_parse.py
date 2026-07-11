"""Parse multi-source collect intents for Agent multisource_workflow (P4)."""

from __future__ import annotations

import re
from typing import Any

# Collector id → default pipeline template / create_task fields
COLLECTOR_TASK_HINTS: dict[str, dict[str, str]] = {
    "steam": {"pipeline_name": "steam_basic", "collector_name": "steam"},
    "taptap": {"pipeline_name": "taptap_basic", "collector_name": "taptap"},
    "qimai": {"pipeline_name": "qimai_basic", "collector_name": "qimai"},
    "gtrends": {"pipeline_name": "gtrends_basic", "collector_name": "gtrends"},
    "monitor": {"pipeline_name": "monitor_basic", "collector_name": "monitor"},
    "steam_discussions": {
        "pipeline_name": "steam_discussions_basic",
        "collector_name": "steam_discussions",
    },
    "official_site": {
        "pipeline_name": "official_site_basic",
        "collector_name": "official_site",
    },
    "youtube_profiles": {
        "pipeline_name": "youtube_profiles_basic",
        "collector_name": "youtube_profiles",
    },
    "youtube_comments": {
        "pipeline_name": "youtube_comments_basic",
        "collector_name": "youtube_comments",
    },
}

# Longer aliases first
_COLLECTOR_ALIASES: tuple[tuple[str, str], ...] = (
    ("steam_discussions", "steam_discussions"),
    ("steam discussions", "steam_discussions"),
    ("youtube_comments", "youtube_comments"),
    ("youtube comments", "youtube_comments"),
    ("youtube_profiles", "youtube_profiles"),
    ("official_site", "official_site"),
    ("google trends", "gtrends"),
    ("steamdb", "steam"),
    ("steam", "steam"),
    ("蒸汽", "steam"),
    ("七麦", "qimai"),
    ("qimai", "qimai"),
    ("taptap", "taptap"),
    ("tap tap", "taptap"),
    ("youtube", "youtube_profiles"),
    ("官网", "official_site"),
    ("official", "official_site"),
    ("讨论", "steam_discussions"),
    ("discussions", "steam_discussions"),
    ("gtrends", "gtrends"),
    ("trends", "gtrends"),
    ("趋势", "gtrends"),
    ("monitor", "monitor"),
    ("监控", "monitor"),
)

_MULTI_KEYWORDS = (
    "多源",
    "多平台",
    "同时采",
    "一起采",
    "分别采",
    "多个采集",
    "multi-source",
    "multi source",
    "multisource",
    "multiple sources",
    "collect from",
)

_COLLECT_INTENT = (
    "采集",
    "抓取",
    "收集",
    "跑一下",
    "collect",
    "scrape",
    "crawl",
    "采一下",
    "采一采",
)

_CONFIRM_KEYWORDS = (
    "确认创建",
    "确认提交",
    "确认采集",
    "confirm=true",
    "confirm create",
    "confirm",
    "确认",
)

_GAME_PATTERNS = (
    re.compile(r"[《「]([^》」]{1,64})[》」]"),
    re.compile(
        r"(?:游戏|game)\s*[:：]?\s*[`'\"]?([^\s`'\"，,。]{1,64})[`'\"]?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:为|对|给)\s*[`'\"]?([A-Za-z0-9\u4e00-\u9fff][\w\u4e00-\u9fff·\- ]{0,40}?)[`'\"]?"
        r"(?:\s*(?:做|进行|跑|采))",
        re.IGNORECASE,
    ),
)

_DEFAULT_MULTI_COLLECTORS = ("steam", "taptap")


def looks_like_multisource_request(text: str) -> bool:
    lowered = str(text or "").lower()
    if any(k in lowered for k in _MULTI_KEYWORDS):
        return True
    collectors = extract_collectors(text)
    if len(collectors) >= 2 and any(k in lowered or k in text for k in _COLLECT_INTENT):
        return True
    return False


def extract_collectors(text: str) -> list[str]:
    lowered = str(text or "").lower()
    found: list[str] = []
    # youtube comments special-case
    if "youtube" in lowered and ("评论" in text or "comment" in lowered):
        found.append("youtube_comments")
    for alias, collector_id in _COLLECTOR_ALIASES:
        if alias in lowered or alias in text:
            if collector_id == "youtube_profiles" and "youtube_comments" in found:
                continue
            if collector_id not in found:
                found.append(collector_id)
    return found


def extract_game_name(text: str) -> str:
    content = str(text or "").strip()
    for pattern in _GAME_PATTERNS:
        match = pattern.search(content)
        if match:
            name = match.group(1).strip(" `'\"，。,.;：:")
            if name and name.lower() not in {"pipeline", "task", "game"}:
                return name

    # Fallback: strip known noise and take a remaining token group
    cleaned = content
    for alias, _ in _COLLECTOR_ALIASES:
        cleaned = re.sub(re.escape(alias), " ", cleaned, flags=re.IGNORECASE)
    for kw in _MULTI_KEYWORDS + _COLLECT_INTENT + _CONFIRM_KEYWORDS:
        cleaned = re.sub(re.escape(kw), " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"https?://\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"pipeline\s*[:：]\s*\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[和与、,+/|]+", " ", cleaned)
    tokens = [t for t in cleaned.split() if t and not t.isdigit()]
    # Prefer CJK-ish or multi-char tokens
    for token in tokens:
        if re.search(r"[\u4e00-\u9fff]", token) and len(token) >= 2:
            return token.strip(" `'\"，。")
    for token in tokens:
        if re.match(r"^[A-Za-z][A-Za-z0-9_\-]{1,40}$", token):
            return token
    return ""


def has_multisource_confirm(text: str) -> bool:
    lowered = str(text or "").lower()
    if any(k in lowered for k in ("确认创建", "确认提交", "确认采集", "confirm=true", "confirm create")):
        return True
    if "确认" in text or "confirm" in lowered:
        return looks_like_multisource_request(text) or bool(extract_collectors(text))
    return False


def build_multisource_draft(
    text: str,
    *,
    game_name: str | None = None,
    collectors: list[str] | None = None,
) -> dict[str, Any]:
    """Build per-collector create_task drafts. Pure; no scheduler side effects."""
    subject = (game_name or extract_game_name(text) or "").strip()
    cols = list(collectors) if collectors is not None else extract_collectors(text)
    issues: list[str] = []

    multi = any(k in str(text or "").lower() for k in _MULTI_KEYWORDS)
    if multi and not cols:
        cols = list(_DEFAULT_MULTI_COLLECTORS)
    if len(cols) < 2 and multi and len(cols) == 1:
        # keep single + note; user may want only one after multi keyword
        pass
    if len(cols) < 2 and not multi:
        issues.append("多源采集需要至少两个数据源，或使用「多源」关键词")
    if not subject:
        issues.append("缺少游戏/目标名称（可用《游戏名》或 游戏:xxx）")
    if not cols:
        issues.append("未识别到采集源（如 steam、七麦、taptap）")

    drafts: list[dict[str, Any]] = []
    for collector_id in cols:
        hint = COLLECTOR_TASK_HINTS.get(collector_id)
        if not hint:
            issues.append(f"未知采集源: {collector_id}")
            continue
        pipeline = hint["pipeline_name"]
        cname = hint.get("collector_name") or collector_id
        task_name = f"{subject}_{collector_id}"[:64] if subject else f"collect_{collector_id}"
        drafts.append(
            {
                "collector_id": collector_id,
                "name": task_name,
                "pipeline_name": pipeline,
                "collector_name": cname,
                "targets": [
                    {
                        "name": subject or task_name,
                        "target_type": "game",
                        "params": {},
                    }
                ],
                "config": {"batch_concurrency": 1},
            }
        )

    status = "incomplete" if issues or not drafts else "draft"
    return {
        "status": status,
        "game_name": subject,
        "collectors": cols,
        "task_drafts": drafts,
        "issues": issues,
        "confirm": has_multisource_confirm(text),
        "summary": _draft_summary(subject, cols, drafts, issues),
    }


def _draft_summary(
    game: str,
    collectors: list[str],
    drafts: list[dict[str, Any]],
    issues: list[str],
) -> str:
    if issues and not drafts:
        return "多源采集草案不完整：" + "；".join(issues[:3])
    sources = "、".join(collectors) if collectors else "未指定"
    subject = game or "未命名目标"
    base = f"将为「{subject}」准备 {len(drafts)} 路采集：{sources}。"
    if issues:
        base += " 注意：" + "；".join(issues[:2])
    return base


def draft_to_confirm_phrase(draft: dict[str, Any]) -> str:
    game = str(draft.get("game_name") or "").strip()
    collectors = draft.get("collectors") or []
    cols = " ".join(str(c) for c in collectors if str(c).strip())
    parts = ["确认创建 多源采集"]
    if game:
        parts.append(f"《{game}》")
    if cols:
        parts.append(cols)
    return " ".join(parts)
