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
