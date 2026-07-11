"""Workflow request matching and message parsing helpers.

P1 fail-closed policy
---------------------
- Report / task-review workflows require an **explicit** ``task_id`` in the user
  text (patterns: ``task:…``, ``task-…``, ``任务 …``). Phrases like “最近 /
  刚完成 / 上一任务 / latest task” are **not** auto-resolved to a task id;
  without a parseable id the matcher returns ``None`` and routing falls back
  to ``general_agent``.
- Dynamic pipeline requires a URL **and** pipeline or collect intent keywords.
  A bare URL alone does **not** match (does not steal from general agent).
- Report keywords beat task-review when both classes match (resolve order:
  report → review → pipeline); ``workflow_auto_retry`` is true only on
  explicit retry language.
"""

from __future__ import annotations

import re
from typing import Any, Literal

from langchain_core.messages import BaseMessage, HumanMessage

from src.agent.workflow_support import _TEMPLATE_NAMES
from src.agent.workflow_types import WorkflowRoute

_REPORT_KEYWORDS = (
    "report",
    "precheck",
    "coverage",
    "generate_report",
    "excel 报告",
    "excel报告",
    "excel report",
    "xlsx",
    "分析报告",
    "分析",
    "复盘",
    "报告",
    "报表",
    "预检",
    "覆盖",
)
_GENERATE_KEYWORDS = (
    "generate report",
    "create report",
    "生成报告",
    "出分析报告",
    "出报告",
    "生成一份报告",
    "生成分析报告",
)
_TASK_REVIEW_KEYWORDS = (
    "review",
    "retry",
    "diagnose",
    "inspect",
    "error",
    "failed",
    "failure",
    "复查",
    "审查",
    "诊断",
    "排查",
    "重试",
    "重跑",
    "失败原因",
    "为什么失败",
    "为啥失败",
    "挂了",
    "报错",
    "查看问题",
)
_AUTO_RETRY_KEYWORDS = (
    "retry",
    "auto_retry",
    "自动重试",
    "重试",
    "重跑",
    "再试一次",
    "重新执行",
)
_PIPELINE_KEYWORDS = (
    "pipeline",
    "dynamic pipeline",
    "create_dynamic_pipeline",
    "动态 pipeline",
    "动态采集",
    "创建 pipeline",
    "生成 pipeline",
    "网页采集",
    "浏览器采集",
)
# Collect-ish intent: URL + any of these also routes to pipeline workflow.
_COLLECT_INTENT_KEYWORDS = (
    "采集",
    "抓取",
    "监控",
    "爬",
    "scrape",
    "collect",
    "crawl",
)
_URL_PATTERN = re.compile(r"https?://[^\s<>\"]+", re.IGNORECASE)
_TASK_ID_PATTERNS = (
    re.compile(
        r"(?:task[_\s-]?id|task|任务(?:id|ID)?|任务号)\s*[:：#]?\s*([A-Za-z0-9][\w:.-]{1,127})",
        re.IGNORECASE,
    ),
    re.compile(r"\b(task[-_:][A-Za-z0-9][\w:.-]{1,127})\b", re.IGNORECASE),
)


def _workflow_state(route: WorkflowRoute, **updates: Any) -> dict[str, Any]:
    state = {
        "route": route,
        "workflow_action": None,
        "workflow_task_id": "",
        "workflow_template": "",
        "workflow_prompt": "",
        "workflow_auto_retry": False,
        "workflow_url": "",
        "workflow_pipeline_name": "",
        "workflow_wait_strategy_type": "networkidle",
        "workflow_wait_strategy_selector": None,
        "workflow_js_script": "",
        "task_detail": None,
        "collection_review": None,
        "report_precheck": None,
        "generated_report": None,
        "dynamic_pipeline_result": None,
        "result_card": None,
        "workflow_collector_id": "",
        "workflow_readiness_scope": "system",
        "workflow_readiness_note": "",
        "readiness_config": None,
        "readiness_session": None,
        "workflow_cron_action": "",
        "workflow_cron_name": "",
        "workflow_cron_expr": "",
        "workflow_cron_timezone": "",
        "workflow_cron_confirm": False,
        "workflow_cron_schedule_meta": None,
        "cron_draft": None,
        "cron_result": None,
        "workflow_multisource_game": "",
        "workflow_multisource_collectors": [],
        "workflow_multisource_confirm": False,
        "multisource_draft": None,
        "multisource_result": None,
    }
    state.update(updates)
    return state


def _match_report_workflow(user_text: str) -> dict[str, Any] | None:
    """Match report workflow only when report intent **and** explicit task_id exist.

    Fail-closed: no task_id → None (no latest-task auto-resolve in P1).
    When both report and review keywords appear, this matcher still matches;
    callers should prefer report by resolving report before review.
    """
    task_id = _extract_task_id(user_text)
    if not (task_id and _looks_like_report_request(user_text)):
        return None
    return _workflow_state(
        "report_workflow",
        workflow_action=_workflow_action(user_text),
        workflow_task_id=task_id,
        workflow_template=_extract_template_name(user_text),
        workflow_prompt=user_text,
    )


def _match_task_review_workflow(user_text: str) -> dict[str, Any] | None:
    """Match task-review workflow when diagnose/review intent **and** task_id exist.

    Fail-closed: no task_id → None.
    ``workflow_auto_retry`` is set only on explicit retry language
    (see ``_AUTO_RETRY_KEYWORDS``), not on generic failure/diagnose words.
    """
    task_id = _extract_task_id(user_text)
    if not (task_id and _looks_like_task_review_request(user_text)):
        return None
    return _workflow_state(
        "task_review_workflow",
        workflow_action=_task_review_action(user_text),
        workflow_task_id=task_id,
        workflow_prompt=user_text,
        workflow_auto_retry=_task_review_auto_retry(user_text),
    )


def _match_readiness_workflow(user_text: str) -> dict[str, Any] | None:
    """Match system/collector readiness intent.

    Does not steal report/task_review (task_id + their intents) or pipeline
    (URL + collect intent). Deep probe is never implied by this match.
    """
    if not _looks_like_readiness_request(user_text):
        return None
    task_id = _extract_task_id(user_text)
    if task_id and (
        _looks_like_report_request(user_text) or _looks_like_task_review_request(user_text)
    ):
        return None
    if _extract_first_url(user_text) and _looks_like_pipeline_request(user_text):
        return None

    collector_id, note = _extract_collector_alias(user_text)
    scope: Literal["collector", "system"] = "collector" if collector_id else "system"
    return _workflow_state(
        "readiness_workflow",
        workflow_collector_id=collector_id or "",
        workflow_readiness_scope=scope,
        workflow_readiness_note=note or "",
        workflow_prompt=user_text,
    )


def _match_pipeline_workflow(user_text: str) -> dict[str, Any] | None:
    """Match dynamic pipeline when URL is present **and** collect/pipeline intent.

    Bare URL alone returns None so general_agent can handle free-form links.
    """
    workflow_url = _extract_first_url(user_text)
    if not (workflow_url and _looks_like_pipeline_request(user_text)):
        return None
    pipeline_name = _derive_pipeline_name(workflow_url, user_text)
    prepared = _build_dynamic_pipeline_draft(workflow_url, pipeline_name)
    return _workflow_state(
        "pipeline_workflow",
        workflow_action="pipeline",
        workflow_prompt=user_text,
        workflow_url=workflow_url,
        workflow_pipeline_name=pipeline_name,
        workflow_wait_strategy_type=str(prepared["wait_strategy_type"]),
        workflow_wait_strategy_selector=prepared["wait_strategy_selector"],
        workflow_js_script=str(prepared["js_script"]),
    )


def _match_cron_workflow(user_text: str) -> dict[str, Any] | None:
    """Match cron list / create / delete intents (P3).

    Fail-closed: bare schedule words without cron-domain or list/delete signals
    do not match. Create executes only when confirm language is present
    (enforced later in apply node); matcher still routes draft creates.
    """
    from src.agent.workflow_cron_parse import (
        extract_job_name,
        extract_pipeline_name,
        extract_timezone,
        parse_schedule,
    )

    text = str(user_text or "").strip()
    if not text:
        return None

    # Do not steal report / review / pipeline / readiness primary signals
    if _extract_task_id(text) and (
        _looks_like_report_request(text) or _looks_like_task_review_request(text)
    ):
        return None
    if _extract_first_url(text) and _looks_like_pipeline_request(text):
        return None
    if _looks_like_readiness_request(text) and not _looks_like_cron_domain(text):
        return None

    action = _cron_action(text)
    if action is None:
        return None

    pipeline_name = extract_pipeline_name(text) if action in {"create", "delete"} else ""
    schedule = parse_schedule(text) if action == "create" else {
        "cron_expr": "",
        "schedule_meta": {},
        "timezone": extract_timezone(text),
        "issues": [],
    }
    cron_expr = str(schedule.get("cron_expr") or "")
    job_name = ""
    if action in {"create", "delete"}:
        job_name = extract_job_name(
            text,
            pipeline_name=pipeline_name,
            cron_expr=cron_expr,
        )
    if action == "delete" and not job_name:
        # delete may reference name after 删除定时任务
        job_name = _extract_delete_job_name(text)

    return _workflow_state(
        "cron_workflow",
        workflow_prompt=text,
        workflow_pipeline_name=pipeline_name,
        workflow_cron_action=action,
        workflow_cron_name=job_name,
        workflow_cron_expr=cron_expr,
        workflow_cron_timezone=str(schedule.get("timezone") or extract_timezone(text)),
        workflow_cron_confirm=_cron_confirm(text),
        workflow_cron_schedule_meta=schedule.get("schedule_meta") or {},
        cron_draft={
            "human_schedule": schedule.get("human_schedule") or "",
            "next_runs": schedule.get("next_runs") or [],
            "issues": list(schedule.get("issues") or []),
        },
    )


_CRON_DOMAIN_KEYWORDS = (
    "定时任务",
    "定时",
    "cron",
    "调度",
    "scheduled",
    "schedule",
    "每天",
    "每周",
    "每月",
    "每小时",
    "每隔",
    "自动跑",
    "自动执行",
    "every day",
    "daily",
    "weekly",
    "hourly",
    "every hour",
    "every month",
)
_CRON_LIST_KEYWORDS = (
    "有哪些定时",
    "定时任务列表",
    "list cron",
    "列出定时",
    "查看定时",
    "所有定时",
    "定时列表",
    "list schedule",
    "scheduled jobs",
)
_CRON_DELETE_KEYWORDS = (
    "删除定时",
    "删掉定时",
    "取消定时",
    "remove cron",
    "delete cron",
    "删除调度",
)
_CRON_CREATE_KEYWORDS = (
    "创建定时",
    "添加定时",
    "新建定时",
    "create cron",
    "add cron",
    "schedule job",
    "定时跑",
    "定时执行",
)
_CRON_CONFIRM_KEYWORDS = (
    "确认创建",
    "确认删除",
    "确认",
    "confirm=true",
    "confirm create",
    "confirm delete",
    "confirmed",
    "confirm",
)


def _looks_like_cron_domain(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(keyword in lowered for keyword in _CRON_DOMAIN_KEYWORDS)


def _cron_action(text: str) -> Literal["list", "create", "delete"] | None:
    lowered = str(text or "").lower()
    if any(keyword in lowered for keyword in _CRON_DELETE_KEYWORDS):
        return "delete"
    if any(keyword in lowered for keyword in _CRON_LIST_KEYWORDS):
        return "list"
    if any(keyword in lowered for keyword in _CRON_CREATE_KEYWORDS):
        return "create"

    from src.agent.workflow_cron_parse import extract_pipeline_name, parse_schedule

    pipeline = extract_pipeline_name(text)
    schedule = parse_schedule(text)
    has_expr = bool(schedule.get("cron_expr"))
    has_schedule = has_expr or bool(schedule.get("issues"))

    # Card copy_confirm_create: "确认创建 pipeline:monitor */15 * * * * 名称 …"
    # Must rematch without NL domain/run cues.
    if _is_confirm_create_phrase(text) and pipeline and has_expr:
        return "create"

    # "每天 8 点跑 pipeline:x" / "每 15 分钟跑 pipeline:monitor"
    if pipeline and has_expr and _has_run_hint(text):
        return "create"
    if _looks_like_cron_domain(text):
        if pipeline and (has_expr or _has_run_hint(text)):
            return "create"
        if pipeline and has_schedule:
            return "create"
        if _has_run_hint(text) and (pipeline or has_expr):
            return "create"
    return None


def _is_confirm_create_phrase(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        k in lowered or k in text
        for k in ("确认创建", "confirm create", "confirm=true")
    )


def _has_run_hint(text: str) -> bool:
    lowered = str(text or "").lower()
    hints = ("跑", "执行", "运行", "run ", " schedule", "调度")
    return any(h in lowered or h in text for h in hints)


def _cron_confirm(text: str) -> bool:
    lowered = str(text or "").lower()
    # Prefer longer phrases first; bare confirm is allowed only with cron domain
    if any(k in lowered for k in ("确认创建", "确认删除", "confirm=true", "confirm create", "confirm delete")):
        return True
    if "确认" in text or "confirm" in lowered:
        return _looks_like_cron_domain(text) or any(
            k in lowered for k in _CRON_DELETE_KEYWORDS + _CRON_CREATE_KEYWORDS + _CRON_LIST_KEYWORDS
        )
    return False


def _extract_delete_job_name(text: str) -> str:
    patterns = (
        re.compile(
            r"(?:删除定时(?:任务)?|删掉定时(?:任务)?|取消定时(?:任务)?|delete cron|remove cron)"
            r"\s*[:：]?\s*[`'\"]?([A-Za-z0-9][\w.-]{0,63})[`'\"]?",
            re.IGNORECASE,
        ),
    )
    for pattern in patterns:
        match = pattern.search(str(text or ""))
        if match:
            return match.group(1).strip("`'\"，。,.;")
    return ""


def _match_multisource_workflow(user_text: str) -> dict[str, Any] | None:
    """Match multi-source collect intent (P4).

    Requires multi-source signal (keyword or ≥2 collectors + collect intent)
    and does not steal report/review/URL-pipeline/readiness/cron primaries
    (those are also ordered ahead in graph resolve).
    """
    from src.agent.workflow_multisource_parse import (
        build_multisource_draft,
        extract_collectors,
        extract_game_name,
        has_multisource_confirm,
        looks_like_multisource_request,
    )

    text = str(user_text or "").strip()
    if not text:
        return None

    if _extract_task_id(text) and (
        _looks_like_report_request(text) or _looks_like_task_review_request(text)
    ):
        return None
    if _extract_first_url(text) and _looks_like_pipeline_request(text):
        return None
    if _looks_like_readiness_request(text) and not looks_like_multisource_request(text):
        return None
    # cron domain without multi → leave to cron (resolve order still prefers cron if both)
    if not looks_like_multisource_request(text):
        return None

    game = extract_game_name(text)
    collectors = extract_collectors(text)
    draft = build_multisource_draft(text, game_name=game, collectors=collectors)
    # Fail-closed: need game or drafts path recognizable; empty multi alone → general
    if not game and not collectors and not draft.get("task_drafts"):
        return None

    return _workflow_state(
        "multisource_workflow",
        workflow_prompt=text,
        workflow_multisource_game=game,
        workflow_multisource_collectors=list(collectors or draft.get("collectors") or []),
        workflow_multisource_confirm=has_multisource_confirm(text),
        multisource_draft=draft,
    )


def _looks_like_report_request(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(keyword in lowered for keyword in _REPORT_KEYWORDS)


def _looks_like_task_review_request(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(keyword in lowered for keyword in _TASK_REVIEW_KEYWORDS)


def _looks_like_pipeline_request(text: str) -> bool:
    """True if text has pipeline keywords **or** collect-ish intent."""
    lowered = str(text or "").lower()
    if any(keyword in lowered for keyword in _PIPELINE_KEYWORDS):
        return True
    return any(keyword in lowered for keyword in _COLLECT_INTENT_KEYWORDS)


_READINESS_KEYWORDS = (
    "readiness",
    "ready to collect",
    "can i collect",
    "can we collect",
    "login status",
    "system check",
    "session check",
    "就绪",
    "能不能采",
    "可否采集",
    "能否采集",
    "可以采吗",
    "能采吗",
    "登录了吗",
    "登录态",
    "系统检查",
    "检查环境",
    "环境诊断",
    "采集准备",
    "检查是否就绪",
    "是否就绪",
)

_COLLECTOR_ALIASES: tuple[tuple[str, str], ...] = (
    ("steam_discussions", "steam_discussions"),
    ("steam discussions", "steam_discussions"),
    ("youtube_comments", "youtube_comments"),
    ("youtube comments", "youtube_comments"),
    ("youtube_profiles", "youtube_profiles"),
    ("official_site", "official_site"),
    ("steamdb", "steam"),
    ("steam", "steam"),
    ("蒸汽", "steam"),
    ("七麦", "qimai"),
    ("qimai", "qimai"),
    ("taptap", "taptap"),
    ("youtube", "youtube_profiles"),
    ("官网", "official_site"),
    ("official", "official_site"),
    ("讨论", "steam_discussions"),
    ("discussions", "steam_discussions"),
    ("gtrends", "gtrends"),
    ("google trends", "gtrends"),
    ("trends", "gtrends"),
    ("趋势", "gtrends"),
    ("monitor", "monitor"),
    ("监控", "monitor"),
)


def _looks_like_readiness_request(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(keyword in lowered for keyword in _READINESS_KEYWORDS)


def _extract_collector_alias(text: str) -> tuple[str, str]:
    """Return (collector_id, note)."""
    lowered = str(text or "").lower()
    if "youtube" in lowered and ("评论" in lowered or "comment" in lowered):
        return "youtube_comments", ""
    for alias, collector_id in _COLLECTOR_ALIASES:
        if alias in lowered:
            return collector_id, ""
    return "", ""


def _workflow_action(text: str) -> Literal["precheck", "generate"]:
    lowered = str(text or "").lower()
    if any(keyword in lowered for keyword in _GENERATE_KEYWORDS):
        return "generate"
    return "precheck"


def _task_review_action(text: str) -> Literal["review", "retry"]:
    return "retry" if _task_review_auto_retry(text) else "review"


def _task_review_auto_retry(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(keyword in lowered for keyword in _AUTO_RETRY_KEYWORDS)


def _extract_first_url(text: str) -> str:
    match = _URL_PATTERN.search(str(text or ""))
    return match.group(0).rstrip(".,);]}>") if match else ""


def _derive_pipeline_name(url: str, prompt: str) -> str:
    hostname = re.sub(r"^https?://", "", str(url or "").strip(), flags=re.IGNORECASE)
    hostname = hostname.split("/", 1)[0].lower()
    hostname = re.sub(r"[^a-z0-9]+", "_", hostname).strip("_")
    if not hostname:
        hostname = "dynamic_site"

    prompt_hint = re.sub(r"[^a-z0-9]+", "_", str(prompt or "").lower()).strip("_")
    prompt_hint = "_".join([part for part in prompt_hint.split("_") if part][:2])
    suffix = prompt_hint if prompt_hint and prompt_hint not in hostname else "page"
    return f"{hostname}_{suffix}"[:64].strip("_")


def _build_dynamic_pipeline_draft(url: str, pipeline_name: str) -> dict[str, Any]:
    return {
        "pipeline_name": pipeline_name,
        "wait_strategy_type": "networkidle",
        "wait_strategy_selector": None,
        "js_script": (
            "() => {\n"
            "  const pick = (selector) => document.querySelector(selector)?.textContent?.trim() || \"\";\n"
            "  const metaDescription = document.querySelector('meta[name=\"description\"]')?.content || \"\";\n"
            "  return {\n"
            "    url: location.href,\n"
            "    title: document.title,\n"
            "    description: metaDescription,\n"
            "    headings: Array.from(document.querySelectorAll('h1, h2')).slice(0, 10).map((node) => node.textContent?.trim() || \"\"),\n"
            "    primary_text: pick('main') || pick('article') || pick('body'),\n"
            "  };\n"
            "}"
        ),
    }


def _extract_template_name(text: str) -> str:
    lowered = str(text or "").lower()
    for template in _TEMPLATE_NAMES:
        if template in lowered:
            return template
    return ""


def _extract_task_id(text: str) -> str:
    content = str(text or "").strip()
    if not content:
        return ""
    for pattern in _TASK_ID_PATTERNS:
        match = pattern.search(content)
        if match:
            return match.group(1).strip("`'\"，。,.;；：:!?！？()[]{}")
    return ""


def _last_user_text(messages: list[Any]) -> str:
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            return _message_text(message.content)
        if isinstance(message, tuple) and len(message) >= 2 and message[0] in {"human", "user"}:
            return _message_text(message[1])
        if isinstance(message, dict) and str(message.get("role") or "") in {"human", "user"}:
            return _message_text(message.get("content"))
        if isinstance(message, BaseMessage) and getattr(message, "type", "") == "human":
            return _message_text(getattr(message, "content", ""))
    return ""


def _message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text", "")))
            elif isinstance(item, str):
                parts.append(item)
        return "".join(parts)
    return str(content or "")
