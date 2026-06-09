"""
报告生成与查询工具
"""

from typing import Type
from urllib.parse import quote
from langchain_core.tools import BaseTool
from pydantic import BaseModel
from loguru import logger

from src.agent.schemas import (
    GenerateReportInput,
    GetReportContentInput,
    ListReportsInput,
    PrecheckReportInput,
)
from src.agent.tools.data import _list_available_games
from src.agent.tools.utils import _safe_json
from src.core.sensitive import redact_sensitive_text
from src.reporting.quality import assess_report_quality
from src.services._utils import (
    derive_collection_target_context,
    extract_record_identity,
    filter_records_by_data_source as _shared_filter_records_by_data_source,
    filter_source_data_records,
    is_report_history_record,
    normalize_source_token,
    source_label,
)


_COLLECTOR_PIPELINE_HINTS: dict[str, dict[str, str]] = {
    "steam": {
        "pipeline_name": "steam_basic",
        "target_hint": "Use the same game name and resolve Steam app_id before creating the task.",
    },
    "taptap": {
        "pipeline_name": "taptap_basic",
        "target_hint": "Use the same game name and TapTap app id when available.",
    },
    "gtrends": {
        "pipeline_name": "gtrends_basic",
        "target_hint": "Use the game name or search keyword as the target name.",
    },
    "monitor": {
        "pipeline_name": "monitor_basic",
        "target_hint": "Use the game's official or Steam page URL when available.",
    },
    "steam_discussions": {
        "pipeline_name": "steam_discussions_basic",
        "target_hint": "Use the Steam app_id and game name for discussion collection.",
    },
    "official_site": {
        "pipeline_name": "official_site_basic",
        "target_hint": "Use the official site URL as target params.official_url.",
    },
    "qimai": {
        "pipeline_name": "qimai_basic",
        "target_hint": "Use the App Store/Qimai identifier when available.",
    },
    "events": {
        "pipeline_name": "official_site_basic",
        "collector_name": "official_site",
        "target_hint": "Collect official news, patch notes, or event pages for the same game.",
    },
}

_COLLECTOR_ACTION_PRIORITIES: dict[str, tuple[int, str, str]] = {
    "steam": (
        10,
        "high",
        "Core store, player, review, and commercial metrics usually anchor the report.",
    ),
    "taptap": (
        10,
        "high",
        "TapTap user ratings, availability, and community signals are core mobile coverage.",
    ),
    "qimai": (
        15,
        "high",
        "Qimai/App Store identifiers and rankings are important for mobile market context.",
    ),
    "official_site": (
        20,
        "medium",
        "Official site data improves release, news, event, and patch-note grounding.",
    ),
    "events": (
        20,
        "medium",
        "Event/news coverage helps explain recent changes behind metric movement.",
    ),
    "steam_discussions": (
        30,
        "medium",
        "Steam discussions add qualitative community feedback and issue signals.",
    ),
    "monitor": (
        40,
        "medium",
        "Monitor metrics add operational and audience trend context.",
    ),
    "gtrends": (
        50,
        "low",
        "Google Trends adds external demand context but rarely blocks a baseline report.",
    ),
}


def _extract_prompt_keywords(prompt: str) -> list[str]:
    import re

    stop_words = {
        "帮我",
        "生成",
        "报告",
        "一个",
        "一份",
        "的",
        "了",
        "是",
        "在",
        "和",
        "请",
        "要",
        "需要",
        "分析",
        "综合",
        "全面",
        "关于",
        "对于",
        "这个",
        "include",
        "report",
        "generate",
        "for",
        "the",
        "a",
        "an",
    }
    split_pattern = re.compile(
        r"[，。！？、；：（）\s]+"
        r"|请对|进行|包括|并提|要求|帮我|生成|分析|综合|全面|完整|详细"
        r"|[a-zA-Z]{2,}"
    )
    raw_parts = re.findall(r"[一-鿿]{2,}", prompt)
    keywords = []
    for part in raw_parts:
        sub_parts = split_pattern.split(part)
        for sub in sub_parts:
            sub = sub.strip()
            if len(sub) >= 2 and sub not in stop_words:
                keywords.append(sub)
    eng_tokens = re.findall(r"[a-zA-Z0-9]{2,}", prompt.lower())
    for token in eng_tokens:
        if token not in stop_words:
            keywords.append(token)
    seen = set()
    result = []
    for kw in keywords:
        if kw not in seen:
            seen.add(kw)
            result.append(kw)
    return result[:5]


def _filter_records_by_keywords(records: list, keywords: list[str]) -> list:
    """Keep records whose identity fields match any keyword."""
    scored: list[tuple[int, object]] = []
    for record in records:
        score = _record_keyword_score(record, keywords)
        if score > 0:
            scored.append((score, record))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [record for _, record in scored]


def _filter_records_by_data_source(records: list, data_source: str) -> list:
    return _shared_filter_records_by_data_source(records, data_source)


def _prepare_report_content(content: str, *, max_chars: int) -> tuple[str, bool]:
    safe_content = _redact_report_text(str(content or ""))
    if len(safe_content) <= max_chars:
        return safe_content, False
    return safe_content[:max_chars] + "\n\n...[Report content truncated]", True


def _redact_report_text(text: str) -> str:
    return redact_sensitive_text(str(text or ""))


def _compact_report_text(value: object, *, max_chars: int) -> str:
    safe_text = _redact_report_text(str(value or ""))
    if len(safe_text) <= max_chars:
        return safe_text
    return safe_text[:max_chars] + "...[truncated]"


def _report_quality_fields(report) -> dict[str, object]:
    metadata = report.metadata if isinstance(getattr(report, "metadata", None), dict) else {}
    template_validation = (
        metadata.get("template_validation")
        if isinstance(metadata.get("template_validation"), dict)
        else {}
    )
    empty_record_keys = [
        str(key)
        for key in metadata.get("empty_record_keys", [])
        if str(key or "").strip()
    ]
    missing_collectors = [
        str(collector)
        for collector in template_validation.get("missing_collectors", [])
        if str(collector or "").strip()
    ]
    warnings = []
    if missing_collectors:
        warnings.append(
            "Report template is missing source coverage: "
            + ", ".join(missing_collectors)
        )
    if empty_record_keys:
        warnings.append(f"{len(empty_record_keys)} selected records had no usable data.")

    fields: dict[str, object] = {
        "matched_records": getattr(report, "matched_records", 0),
        "source_record_count": metadata.get("source_record_count", getattr(report, "matched_records", 0)),
        "usable_record_count": metadata.get("usable_record_count", getattr(report, "matched_records", 0)),
        "source_coverage": metadata.get("source_coverage", {}),
        "record_completeness": metadata.get("record_completeness", {}),
        "source_freshness": metadata.get("source_freshness", {}),
        "template_status": template_validation.get("status", ""),
        "missing_collectors": missing_collectors,
        "empty_record_count": len(empty_record_keys),
    }
    if empty_record_keys:
        fields["empty_record_keys"] = empty_record_keys[:20]
        if len(empty_record_keys) > 20:
            fields["empty_record_keys_truncated"] = True
    for key in ("selected_record_keys", "excluded_report_record_keys"):
        values = [
            str(value)
            for value in metadata.get(key, [])
            if str(value or "").strip()
        ]
        if values:
            fields[key] = values[:50]
            if len(values) > 50:
                fields[f"{key}_truncated"] = True
    if warnings:
        fields["quality_warnings"] = warnings
    fields.update(assess_report_quality(fields, include_follow_up_actions=True))
    target_context = metadata.get("target_context")
    if isinstance(target_context, dict):
        public_context = _public_target_context(target_context)
        if public_context:
            fields["target_context"] = public_context
        actions = _suggest_collection_actions(
            {
                "missing_collectors": missing_collectors,
                "source_counts": fields.get("source_coverage", {}),
            },
            prompt=str(getattr(report, "prompt", "") or ""),
            data_source=str(getattr(report, "data_source", "") or ""),
            target_context=target_context,
            can_generate=_safe_positive_int(fields.get("usable_record_count")) > 0,
        )
        if actions:
            fields["suggested_collection_actions"] = actions
            next_action = _next_best_report_action(
                actions,
                can_generate=_safe_positive_int(fields.get("usable_record_count")) > 0,
                should_collect_more=True,
            )
            if next_action:
                fields["next_best_action"] = next_action
    return fields


def _report_summary_quality_fields(report) -> dict[str, object]:
    metadata = report.metadata if isinstance(getattr(report, "metadata", None), dict) else {}
    fields = _report_quality_fields(report)
    summary: dict[str, object] = {
        "format": metadata.get("format"),
        "source_record_count": fields.get("source_record_count"),
        "usable_record_count": fields.get("usable_record_count"),
        "source_coverage": fields.get("source_coverage"),
        "record_completeness": fields.get("record_completeness"),
        "source_freshness": fields.get("source_freshness"),
        "template_status": fields.get("template_status"),
        "quality_status": fields.get("quality_status"),
        "quality_summary": fields.get("quality_summary"),
        "regeneration_recommended": fields.get("regeneration_recommended"),
        "coverage_risks": fields.get("coverage_risks"),
        "missing_collectors": fields.get("missing_collectors"),
        "empty_record_count": fields.get("empty_record_count"),
        "quality_warnings": fields.get("quality_warnings"),
    }
    return {
        key: value
        for key, value in summary.items()
        if value not in (None, "", {}, [])
    }


def _report_summary_payload(report) -> dict[str, object]:
    generated_at = getattr(report, "generated_at", "")
    if hasattr(generated_at, "isoformat"):
        generated_at = generated_at.isoformat()
    payload = {
        "id": getattr(report, "id", ""),
        "title": _compact_report_text(getattr(report, "title", ""), max_chars=160),
        "generated_at": str(generated_at),
        "prompt": _compact_report_text(getattr(report, "prompt", ""), max_chars=400),
        "data_source": _compact_report_text(getattr(report, "data_source", ""), max_chars=120),
        "template": _compact_report_text(getattr(report, "template", ""), max_chars=120),
        "matched_records": getattr(report, "matched_records", 0),
        "quality": _report_summary_quality_fields(report),
    }
    download_url = _report_download_url(report)
    if download_url:
        payload["download_url"] = download_url
    return payload


def _report_download_url(report) -> str:
    excel_path = getattr(report, "excel_path", None)
    metadata = report.metadata if isinstance(getattr(report, "metadata", None), dict) else {}
    if not excel_path:
        excel_path = metadata.get("excel_path")
    if not excel_path:
        return ""
    report_id = str(getattr(report, "id", "") or "")
    if not report_id:
        return ""
    return f"/api/reports/{quote(report_id, safe='')}/download"


def _report_source_tokens(report, metadata: dict[str, object], quality: dict[str, object]) -> set[str]:
    values: list[object] = [
        getattr(report, "data_source", ""),
        metadata.get("source_query", ""),
    ]
    source_coverage = quality.get("source_coverage", {})
    if isinstance(source_coverage, dict):
        values.extend(source_coverage.keys())
    template_validation = (
        metadata.get("template_validation")
        if isinstance(metadata.get("template_validation"), dict)
        else {}
    )
    for key in ("available_collectors",):
        source_values = template_validation.get(key, [])
        if isinstance(source_values, list):
            values.extend(source_values)
    for value in list(values):
        if value:
            values.append(source_label(str(value)))
    return {
        normalize_source_token(str(value))
        for value in values
        if str(value or "").strip()
    }


def _report_matches_filters(
    report,
    *,
    query: str = "",
    data_source: str = "",
    template: str = "",
    quality_status: str = "",
    report_format: str = "",
) -> bool:
    query = query.lower().strip()
    data_source = data_source.lower().strip()
    template = template.lower().strip()
    quality_status = quality_status.lower().strip()
    report_format = report_format.lower().strip()
    metadata = report.metadata if isinstance(getattr(report, "metadata", None), dict) else {}
    quality = _report_summary_quality_fields(report)

    if data_source and normalize_source_token(data_source) not in _report_source_tokens(
        report,
        metadata,
        quality,
    ):
        return False
    if template and template != str(getattr(report, "template", "")).lower():
        return False
    quality_statuses = {
        str(quality.get("template_status", "")).lower(),
        str(quality.get("quality_status", "")).lower(),
    }
    if quality_status and quality_status not in quality_statuses:
        return False
    if report_format and report_format != str(metadata.get("format", "")).lower():
        return False
    if query:
        haystack = " ".join(
            str(value)
            for value in (
                getattr(report, "id", ""),
                getattr(report, "title", ""),
                getattr(report, "prompt", ""),
                getattr(report, "data_source", ""),
                getattr(report, "template", ""),
                metadata.get("source_query", ""),
                " ".join(_report_source_tokens(report, metadata, quality)),
                " ".join(str(item) for item in quality.get("missing_collectors", []) or []),
                metadata.get("format", ""),
            )
            if value
        ).lower()
        if query not in haystack:
            return False
    return True


def _coerce_report_limit(limit: int) -> int:
    try:
        value = int(limit)
    except (TypeError, ValueError):
        value = 20
    return max(1, min(value, 100))


def _coerce_precheck_limit(limit: int) -> int:
    try:
        value = int(limit)
    except (TypeError, ValueError):
        value = 100
    return max(1, min(value, 5000))


def _safe_positive_int(value: object) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(parsed, 0)


def _target_params_for_collector(
    collector: str,
    target_context: dict[str, object] | None,
) -> dict[str, str]:
    if not isinstance(target_context, dict):
        return {}
    params_by_collector = target_context.get("params_by_collector", {})
    if not isinstance(params_by_collector, dict):
        return {}
    params = params_by_collector.get(collector, {})
    return dict(params) if isinstance(params, dict) else {}


def _collector_identifier_needed(collector: str, params: dict[str, str]) -> bool:
    if collector == "steam":
        return not _has_param(params, "app_id")
    if collector == "steam_discussions":
        return not (_has_param(params, "app_id") or _has_param(params, "forum_url"))
    if collector == "taptap":
        return not (_has_param(params, "app_id") or _has_param(params, "url"))
    if collector == "monitor":
        return not (_has_param(params, "app_id") or _has_param(params, "siteurl"))
    if collector == "qimai":
        return not (_has_param(params, "app_id") or _has_param(params, "qimai_app_id"))
    if collector in {"official_site", "events"}:
        return not _has_param(params, "official_url")
    return False


def _has_param(params: dict[str, str], key: str) -> bool:
    return str(params.get(key) or "").strip() != ""


def _public_target_context(target_context: dict[str, object] | None) -> dict[str, object]:
    if not isinstance(target_context, dict):
        return {}
    public_keys = {
        "target_name",
        "game_name",
        "steam_app_id",
        "taptap_app_id",
        "monitor_app_id",
        "siteurl",
        "official_url",
        "qimai_app_id",
        "source_collectors",
        "source_record_keys",
        "source_record_count",
    }
    return {
        key: value
        for key, value in target_context.items()
        if key in public_keys and value not in (None, "", [], {})
    }


def _report_precheck_payload(
    precheck,
    *,
    prompt: str = "",
    data_source: str = "",
    target_context: dict[str, object] | None = None,
) -> dict[str, object]:
    if hasattr(precheck, "model_dump"):
        payload = precheck.model_dump(mode="json")
    elif isinstance(precheck, dict):
        payload = dict(precheck)
    else:
        payload = {}

    status = str(payload.get("status") or "unknown")
    usable_records = int(payload.get("usable_records") or 0)
    can_generate = status in {"complete", "partial"} and usable_records > 0
    should_collect_more = status != "complete"
    decision = _report_precheck_decision(status=status, can_generate=can_generate)
    agent_guidance = _report_precheck_guidance(
        status=status,
        can_generate=can_generate,
        missing_collectors=payload.get("missing_collectors", []),
    )
    if target_context is None:
        target_context = derive_collection_target_context(
            [],
            prompt=prompt,
            data_source=data_source,
        )
    public_target_context = _public_target_context(target_context)
    actions = _suggest_collection_actions(
        payload,
        prompt=prompt,
        data_source=data_source,
        target_context=target_context,
        can_generate=can_generate,
    )
    payload.update(
        {
            "success": True,
            "can_generate": can_generate,
            "should_collect_more": should_collect_more,
            "decision": decision,
            "readiness": _report_readiness_payload(
                status=status,
                can_generate=can_generate,
                should_collect_more=should_collect_more,
                decision=decision,
            ),
            "coverage_summary": _report_coverage_summary(payload),
            "agent_guidance": agent_guidance,
            "suggested_collection_actions": actions,
        }
    )
    next_best_action = _next_best_report_action(
        actions,
        can_generate=can_generate,
        should_collect_more=should_collect_more,
    )
    if next_best_action:
        payload["next_best_action"] = next_best_action
    if public_target_context:
        payload["target_context"] = public_target_context
    return payload


def _report_precheck_decision(*, status: str, can_generate: bool) -> str:
    if status == "complete" and can_generate:
        return "generate_now"
    if can_generate:
        return "ask_user_generate_now_or_collect_first"
    return "collect_first"


def _report_precheck_guidance(
    *,
    status: str,
    can_generate: bool,
    missing_collectors: object,
) -> str:
    missing_labels = _missing_collector_labels(missing_collectors)
    if status == "complete" and can_generate:
        return "Generate the report now; required template sources are covered."
    if can_generate:
        suffix = f" Missing sources: {', '.join(missing_labels)}." if missing_labels else ""
        return (
            "Current source data is partial data. Ask whether to collect missing sources first, "
            "or generate now with an explicit coverage caveat."
            + suffix
        )
    suffix = f" Prioritize: {', '.join(missing_labels)}." if missing_labels else ""
    return "Collect or select source data records before generating a report." + suffix


def _missing_collector_labels(missing_collectors: object) -> list[str]:
    if not isinstance(missing_collectors, list):
        return []
    return [source_label(str(item)) for item in missing_collectors if str(item or "").strip()]


def _report_readiness_payload(
    *,
    status: str,
    can_generate: bool,
    should_collect_more: bool,
    decision: str,
) -> dict[str, object]:
    return {
        "status": status,
        "can_generate": can_generate,
        "should_collect_more": should_collect_more,
        "decision": decision,
    }


def _report_coverage_summary(precheck_payload: dict[str, object]) -> dict[str, object]:
    missing = precheck_payload.get("missing_collectors", [])
    if not isinstance(missing, list):
        missing = []
    available = precheck_payload.get("available_collectors", [])
    if not isinstance(available, list):
        available = []
    source_counts = precheck_payload.get("source_counts", {})
    if not isinstance(source_counts, dict):
        source_counts = {}
    return {
        "selected_records": int(precheck_payload.get("selected_records") or 0),
        "usable_records": int(precheck_payload.get("usable_records") or 0),
        "missing_count": len(missing),
        "missing_collectors": [str(item) for item in missing],
        "missing_labels": _missing_collector_labels(missing),
        "available_collectors": [str(item) for item in available],
        "source_counts": dict(source_counts),
    }


def _next_best_report_action(
    actions: list[dict[str, object]],
    *,
    can_generate: bool,
    should_collect_more: bool,
) -> dict[str, object]:
    if actions and should_collect_more:
        action = actions[0]
        return {
            "type": "collect_missing_source",
            "collector": action.get("collector", ""),
            "collector_label": action.get("collector_label", ""),
            "recommended_sequence": action.get("recommended_sequence", []),
            "reason": action.get("why", ""),
            "can_execute_now": action.get("can_execute_now", False),
            "create_task_draft": action.get("create_task_draft"),
        }
    if can_generate:
        return {
            "type": "generate_report",
            "recommended_tool": "generate_report",
            "reason": "Coverage is complete enough to generate the report.",
        }
    return {
        "type": "select_or_collect_source_records",
        "recommended_tool": "search_data",
        "reason": "No usable source records are available for this report.",
    }


def _suggest_collection_actions(
    precheck_payload: dict[str, object],
    *,
    prompt: str = "",
    data_source: str = "",
    target_context: dict[str, object] | None = None,
    can_generate: bool = False,
) -> list[dict[str, object]]:
    missing = precheck_payload.get("missing_collectors", [])
    if not isinstance(missing, list):
        return []
    if target_context is None:
        target_context = derive_collection_target_context(
            [],
            prompt=prompt,
            data_source=data_source,
        )
    subject = str(
        target_context.get("target_name")
        if isinstance(target_context, dict)
        else ""
    ).strip() or _collection_target_subject(prompt=prompt, data_source=data_source)
    actions = []
    for raw_collector in missing:
        collector = str(raw_collector or "").strip()
        if not collector:
            continue
        hint = _COLLECTOR_PIPELINE_HINTS.get(collector, {})
        pipeline_name = hint.get("pipeline_name", "")
        task_collector_name = hint.get("collector_name", collector)
        target_params = _target_params_for_collector(collector, target_context)
        identifier_capable = collector in {
            "steam",
            "steam_discussions",
            "taptap",
            "monitor",
            "official_site",
            "qimai",
            "events",
        }
        identifier_first = identifier_capable and _collector_identifier_needed(
            collector,
            target_params,
        )
        has_redacted_params = _contains_redacted_placeholder(target_params)
        missing_params = _collector_missing_params(collector, target_params)
        sequence = ["create_task"] if pipeline_name else ["search_game_identifiers"]
        if identifier_first:
            sequence = ["search_game_identifiers", *sequence]
        if has_redacted_params and "search_game_identifiers" not in sequence:
            sequence = ["search_game_identifiers", *sequence]
        priority, priority_label, why = _collector_action_priority(collector)
        action = {
            "collector": collector,
            "collector_label": source_label(collector),
            "priority": priority,
            "priority_label": priority_label,
            "why": why,
            "blocks_generation": not can_generate,
            "can_execute_now": bool(
                pipeline_name and not identifier_first and not has_redacted_params
            ),
            "next_tool": (
                "search_game_identifiers"
                if has_redacted_params or not pipeline_name
                else "create_task"
            ),
            "recommended_sequence": sequence,
            "pipeline_name": pipeline_name,
            "missing_params": missing_params,
            "target_hint": hint.get(
                "target_hint",
                "Discover the platform identifier first, then create a matching collection task.",
            ),
            "follow_up": "After the task succeeds, rerun precheck_report before generating.",
        }
        if pipeline_name:
            action["create_task_draft"] = {
                "name": f"Collect {source_label(collector)} data for report",
                "pipeline_name": pipeline_name,
                "targets": [
                    {
                        "name": subject,
                        "target_type": "game",
                        "params": target_params,
                    }
                ],
                "collector_name": task_collector_name,
                "config": {"batch_concurrency": 1},
            }
            if task_collector_name != collector:
                action["create_task_draft"]["source_gap_collector"] = collector
        if identifier_first:
            action["identifier_first"] = "search_game_identifiers"
            action["identifier_status"] = "needs_resolution"
        elif has_redacted_params:
            action["sensitive_params_redacted"] = True
            action["identifier_status"] = "needs_original_sensitive_params"
        elif identifier_capable:
            action["identifier_status"] = "ready_from_selected_records"
        actions.append(action)
    actions.sort(
        key=lambda item: (
            0 if item.get("can_execute_now") else 1,
            int(item.get("priority") or 999),
        )
    )
    return actions


def _collector_action_priority(collector: str) -> tuple[int, str, str]:
    return _COLLECTOR_ACTION_PRIORITIES.get(
        collector,
        (
            90,
            "low",
            "This source can improve coverage but is not recognized as a core report blocker.",
        ),
    )


def _collector_missing_params(collector: str, params: dict[str, str]) -> list[str]:
    if collector == "steam":
        return [] if _has_param(params, "app_id") else ["app_id"]
    if collector == "steam_discussions":
        return [] if (_has_param(params, "app_id") or _has_param(params, "forum_url")) else [
            "app_id",
            "forum_url",
        ]
    if collector == "taptap":
        return [] if (_has_param(params, "app_id") or _has_param(params, "url")) else [
            "app_id",
            "url",
        ]
    if collector == "monitor":
        return [] if (_has_param(params, "app_id") or _has_param(params, "siteurl")) else [
            "app_id",
            "siteurl",
        ]
    if collector == "qimai":
        return [] if (_has_param(params, "app_id") or _has_param(params, "qimai_app_id")) else [
            "app_id",
            "qimai_app_id",
        ]
    if collector in {"official_site", "events"}:
        return [] if _has_param(params, "official_url") else ["official_url"]
    return []


def _contains_redacted_placeholder(value: object) -> bool:
    if isinstance(value, dict):
        return any(_contains_redacted_placeholder(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_redacted_placeholder(item) for item in value)
    if isinstance(value, str):
        return "[REDACTED" in value.upper()
    return False


def _collection_target_subject(*, prompt: str = "", data_source: str = "") -> str:
    subject = str(data_source or "").strip()
    if not subject:
        subject = _compact_report_text(prompt, max_chars=80).strip()
    return subject or "same game as selected records"


def _record_keyword_score(record, keywords: list[str]) -> int:
    identity = extract_record_identity(record)
    if not identity:
        return 0
    metadata = record.metadata if isinstance(getattr(record, "metadata", None), dict) else {}
    source_task = metadata.get("source_task", {}) if isinstance(metadata, dict) else {}
    if not isinstance(source_task, dict):
        source_task = {}
    haystack_values = [
        getattr(record, "key", ""),
        getattr(record, "source", ""),
        identity.get("game_name", ""),
        identity.get("app_id", ""),
        identity.get("collector", ""),
        identity.get("data_source", ""),
        " ".join(str(tag) for tag in getattr(record, "tags", []) or []),
        metadata.get("target", ""),
        metadata.get("task_id", ""),
        metadata.get("task_name", ""),
        metadata.get("collector", ""),
        metadata.get("group_id", ""),
        metadata.get("group_name", ""),
        source_task.get("task_id", ""),
        source_task.get("task_name", ""),
        source_task.get("pipeline_name", ""),
        source_task.get("collector_name", ""),
        source_task.get("target", ""),
    ]
    haystack = " ".join(str(value) for value in haystack_values if value).lower()
    score = 0
    for keyword in keywords:
        kw = keyword.lower().strip()
        if not kw:
            continue
        game_name = str(identity.get("game_name", "")).lower()
        if kw == game_name:
            score += 100
        elif kw in game_name or game_name in kw:
            score += 60
        elif kw in haystack:
            score += 20
    return score


async def _load_candidate_records(store, keywords: list[str], *, fallback_limit: int = 2000) -> list:
    """Collect candidate records from multiple keyword queries with key-based fallback."""
    records_by_key = {}
    for keyword in keywords[:5]:
        result = await store.query(keyword, limit=500)
        for record in result.records:
            records_by_key[record.key] = record

    source_records = filter_source_data_records(list(records_by_key.values()))
    if source_records:
        return source_records

    records_by_key = {}
    result = await store.query("key:", limit=fallback_limit)
    for record in result.records:
        records_by_key[record.key] = record

    return filter_source_data_records(list(records_by_key.values()))


class PrecheckReportTool(BaseTool):
    name: str = "precheck_report"
    description: str = (
        "Check source data and template coverage before generating a report. "
        "Use this when deciding whether a report can be generated, which data sources "
        "are missing, or whether the user should collect more data first."
    )
    args_schema: Type[BaseModel] = PrecheckReportInput

    async def _arun(
        self,
        prompt: str,
        data_source: str = "",
        template: str = "general_game",
        record_keys: list[str] | None = None,
        limit: int = 100,
    ) -> str:
        from src.web.routes.reports import (
            GenerateReportRequest,
            _build_report_precheck,
            _load_report_precheck_records,
        )

        safe_limit = _coerce_precheck_limit(limit)
        try:
            request = GenerateReportRequest(
                prompt=str(prompt or ""),
                data_source=str(data_source or ""),
                template=str(template or "general_game"),
                record_keys=record_keys or [],
                params={"limit": safe_limit},
            )
            records = await _load_report_precheck_records(request)
            precheck = _build_report_precheck(request.template, records)
            target_context = derive_collection_target_context(
                records,
                prompt=request.prompt,
                data_source=request.data_source,
            )
            return _safe_json(
                _report_precheck_payload(
                    precheck,
                    prompt=request.prompt,
                    data_source=request.data_source,
                    target_context=target_context,
                )
            )
        except Exception as e:
            error = _redact_report_text(str(getattr(e, "detail", str(e))))
            logger.error(f"Agent report precheck failed: {error}")
            return _safe_json({"success": False, "error": error})

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class ListReportsTool(BaseTool):
    name: str = "list_reports"
    description: str = (
        "List generated reports with report ids and compact quality metadata. "
        "Use this before get_report_content when the user asks about existing, "
        "latest, or previous reports without providing a report_id."
    )
    args_schema: Type[BaseModel] = ListReportsInput

    async def _arun(
        self,
        limit: int = 20,
        query: str = "",
        data_source: str = "",
        template: str = "",
        quality_status: str = "",
        report_format: str = "",
    ) -> str:
        from src.web.app import report_generator

        safe_limit = _coerce_report_limit(limit)
        has_filters = any(
            str(value or "").strip()
            for value in (query, data_source, template, quality_status, report_format)
        )
        scan_limit = min(max(safe_limit * 5, 50), 200) if has_filters else safe_limit
        try:
            reports = await report_generator.list_reports(limit=scan_limit)
            filtered = [
                report
                for report in reports
                if _report_matches_filters(
                    report,
                    query=query,
                    data_source=data_source,
                    template=template,
                    quality_status=quality_status,
                    report_format=report_format,
                )
            ]
            items = [_report_summary_payload(report) for report in filtered[:safe_limit]]
            suggestion = (
                "Use get_report_content with a report_id to inspect a report."
                if items
                else "No matching generated reports found. Adjust filters or use generate_report to create one."
            )
            return _safe_json(
                {
                    "success": True,
                    "reports": items,
                    "record_count": len(items),
                    "limit": safe_limit,
                    "scan_limit": scan_limit,
                    "filters": {
                        "query": query,
                        "data_source": data_source,
                        "template": template,
                        "quality_status": quality_status,
                        "report_format": report_format,
                    },
                    "suggestion": suggestion,
                }
            )
        except Exception as e:
            error = _redact_report_text(str(e))
            logger.error(f"Agent list reports failed: {error}")
            return _safe_json({"success": False, "error": error})

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class GenerateReportTool(BaseTool):
    name: str = "generate_report"
    description: str = (
        "生成数据分析报告（Excel 格式）。"
        "需要 prompt(分析提示词)、data_source(数据源标签) 或 record_keys(指定记录)。"
        "template 可选: general_game / steam_game / taptap_game"
    )
    args_schema: Type[BaseModel] = GenerateReportInput

    async def _arun(
        self,
        prompt: str,
        data_source: str = "",
        template: str = "general_game",
        record_keys: list[str] | None = None,
    ) -> str:
        from src.web.app import report_generator
        from src.storage.factory import get_storage

        record_keys = record_keys or []
        records = None
        metadata = None

        store = get_storage()
        await store.initialize()
        try:
            if record_keys:
                records = []
                excluded_report_keys = []
                for key in record_keys:
                    record = await store.load(key)
                    if record is None:
                        safe_key = _redact_report_text(str(key))
                        return _safe_json({"success": False, "error": f"数据记录不存在: {safe_key}"})
                    if is_report_history_record(record):
                        excluded_report_keys.append(record.key)
                        continue
                    records.append(record)
                if not records:
                    return _safe_json(
                        {
                            "success": False,
                            "error": (
                                "Selected keys only contain generated report history. "
                                "Use get_report_content for existing reports or choose source data records."
                            ),
                            "excluded_report_record_keys": excluded_report_keys,
                        }
                    )
                metadata = {"selected_record_keys": [record.key for record in records]}
                if excluded_report_keys:
                    metadata["excluded_report_record_keys"] = excluded_report_keys
            else:
                keywords = _extract_prompt_keywords(prompt)

                all_records = await _load_candidate_records(store, keywords)
                if not all_records:
                    return _safe_json(
                        {"success": False, "error": "系统中没有找到相关数据记录，请先采集数据"}
                    )

                if data_source:
                    source_records = _filter_records_by_data_source(all_records, data_source)
                    if not source_records:
                        fallback_records = await _load_candidate_records(store, [])
                        source_records = _filter_records_by_data_source(
                            fallback_records,
                            data_source,
                        )
                    if not source_records:
                        safe_data_source = _redact_report_text(str(data_source))
                        return _safe_json(
                            {
                                "success": False,
                                "error": (
                                    f"未找到数据源 '{safe_data_source}' 的数据记录。"
                                    "请检查 data_source，或先执行对应采集任务。"
                                ),
                            }
                        )
                    all_records = source_records

                if keywords:
                    matched = _filter_records_by_keywords(all_records, keywords)
                    if matched:
                        records = matched
                    else:
                        safe_keywords = _redact_report_text(" ".join(keywords))
                        return _safe_json(
                            {
                                "success": False,
                                "error": (
                                    f"未找到与 '{safe_keywords}' 相关的数据记录。"
                                    f"请检查游戏名称是否正确，或先执行采集任务。"
                                    f"当前查询到的游戏: {_list_available_games(all_records)}"
                                ),
                            }
                        )
                else:
                    records = all_records

                metadata = {
                    "selected_record_keys": [r.key for r in records],
                    "data_source_filter": data_source or "",
                }
        finally:
            await store.close()

        try:
            result = await report_generator.generate_excel(
                prompt=prompt,
                data_source=data_source or "",
                template=template,
                records=records,
                metadata=metadata,
            )
            response = {
                "success": True,
                "report_id": result.id,
                "title": result.title,
                **_report_quality_fields(result),
            }
            download_url = _report_download_url(result)
            if download_url:
                response["download_url"] = download_url
            if result.content:
                content, truncated = _prepare_report_content(result.content, max_chars=4000)
                response["content"] = content
                if truncated:
                    response["content_truncated"] = True
            return _safe_json(response)
        except Exception as e:
            error = _redact_report_text(str(e))
            logger.error(f"Agent 生成报告失败: {error}")
            return _safe_json({"success": False, "error": error})

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class GetReportContentTool(BaseTool):
    name: str = "get_report_content"
    description: str = (
        "获取已生成报告的完整内容。需要 report_id。当用户要求查看报告详情、分析结果时使用此工具。"
    )
    args_schema: Type[BaseModel] = GetReportContentInput

    async def _arun(self, report_id: str) -> str:
        from src.web.app import report_generator

        try:
            report = await report_generator.get_report(report_id)
            if report is None:
                return _safe_json({"success": False, "error": f"报告不存在: {report_id}"})
            content, truncated = _prepare_report_content(report.content, max_chars=8000)
            return _safe_json(
                {
                    "success": True,
                    "report_id": report.id,
                    "title": report.title,
                    "content": content,
                    "content_truncated": truncated,
                    "excel_path": report.excel_path,
                    "download_url": _report_download_url(report),
                    **_report_quality_fields(report),
                }
            )
        except Exception as e:
            error = _redact_report_text(str(e))
            logger.error(f"获取报告内容失败: {error}")
            return _safe_json({"success": False, "error": error})

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")
