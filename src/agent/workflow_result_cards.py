"""Structured result_card builders for Agent workflows."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.agent.workflow_events import result_card_event


def build_report_result_card(state: Mapping[str, Any]) -> dict[str, Any]:
    """Build a result_card event for the report workflow."""
    task_id = str(state.get("workflow_task_id") or "").strip()
    task_detail = state.get("task_detail") or {}
    review = state.get("collection_review") or {}
    precheck = state.get("report_precheck") or {}
    generated = state.get("generated_report") or {}

    if str(task_detail.get("status") or "").lower() != "ok":
        summary = str(
            task_detail.get("summary") or task_detail.get("error") or "任务详情读取失败"
        )
        return result_card_event(
            "report",
            "任务详情不可用",
            summary,
            actions=_task_navigate_actions(task_id),
            payload=_base_payload(task_id, status="error"),
        )

    if state.get("workflow_action") == "generate" and bool(generated.get("success")):
        title = str(generated.get("title") or generated.get("report_id") or "报告").strip()
        report_id = str(generated.get("report_id") or "").strip()
        download_url = str(generated.get("download_url") or "").strip()
        summary = f"已基于任务 {task_id or '当前任务'} 生成报告《{title}》。"
        actions = [
            _navigate_action("open_reports", "打开报告", "reports"),
            *_task_navigate_actions(task_id),
        ]
        if download_url:
            actions.append(
                {
                    "id": "copy_download_url",
                    "label": "复制下载链接",
                    "kind": "copy",
                    "payload": {"text": download_url},
                }
            )
        return result_card_event(
            "report",
            "报告已生成",
            summary,
            actions=actions,
            payload=_compact_payload(
                {
                    **_base_payload(task_id, status="success"),
                    "report_id": report_id,
                    "download_url": download_url,
                    "title": title,
                }
            ),
        )

    if precheck:
        can_generate = bool(precheck.get("can_generate"))
        status = str(precheck.get("status") or "").strip() or (
            "ready" if can_generate else "blocked"
        )
        if can_generate:
            title = "报告预检通过"
            summary = f"任务 {task_id or '当前任务'} 可生成报告。"
        else:
            title = "报告预检未通过"
            summary = f"任务 {task_id or '当前任务'} 暂不建议直接生成报告。"
        actions = [
            *_task_navigate_actions(task_id),
            _navigate_action("open_reports", "查看报告", "reports"),
        ]
        return result_card_event(
            "report",
            title,
            summary,
            actions=actions,
            payload={
                **_base_payload(task_id, status=status),
                "can_generate": can_generate,
            },
        )

    record_count = int(review.get("record_count") or 0) if isinstance(review, dict) else 0
    completeness = (
        str(review.get("completeness") or "unknown") if isinstance(review, dict) else "unknown"
    )
    summary = (
        f"任务 {task_id or '当前任务'} 已复查采集结果"
        + (f"，{record_count} 条记录" if record_count else "")
        + f"（完整度：{completeness}）。"
    )
    return result_card_event(
        "report",
        "采集复查完成",
        summary,
        actions=_task_navigate_actions(task_id),
        payload={
            **_base_payload(task_id, status=completeness),
            "record_count": record_count,
        },
    )


def build_task_review_result_card(state: Mapping[str, Any]) -> dict[str, Any]:
    """Build a result_card event for the task review workflow."""
    task_id = str(state.get("workflow_task_id") or "").strip()
    task_detail = state.get("task_detail") or {}
    review = state.get("collection_review") or {}
    auto_retry = bool(state.get("workflow_auto_retry"))

    if str(task_detail.get("status") or "").lower() != "ok":
        summary = str(
            task_detail.get("summary") or task_detail.get("error") or "任务详情读取失败"
        )
        return result_card_event(
            "task_review",
            "任务详情不可用",
            summary,
            actions=_task_navigate_actions(task_id),
            payload=_base_payload(task_id, status="error"),
        )

    if not review:
        return result_card_event(
            "task_review",
            "任务详情已检查",
            f"任务 {task_id or '当前任务'} 已完成详情检查。",
            actions=_task_navigate_actions(task_id),
            payload=_base_payload(task_id, status="checked"),
        )

    completeness = str(review.get("completeness") or "unknown")
    record_count = int(review.get("record_count") or 0)
    issues = review.get("issues") if isinstance(review.get("issues"), list) else []
    summary = (
        f"任务 {task_id or '当前任务'} 采集复查：{completeness}"
        + (f"，{record_count} 条记录" if record_count else "")
        + "。"
    )
    payload: dict[str, Any] = {
        **_base_payload(task_id, status=completeness),
        "record_count": record_count,
        "issues": issues,
        "auto_retry": auto_retry,
    }
    retry_task_id = str(review.get("retry_task_id") or "").strip()
    if retry_task_id:
        payload["retry_task_id"] = retry_task_id
        summary += f" 已创建重试任务 {retry_task_id}。"

    return result_card_event(
        "task_review",
        "任务复查完成",
        summary,
        actions=_task_navigate_actions(task_id),
        payload=payload,
    )


def build_pipeline_result_card(state: Mapping[str, Any]) -> dict[str, Any]:
    """Build a result_card event for the dynamic pipeline workflow."""
    url = str(state.get("workflow_url") or "").strip()
    pipeline_name = str(state.get("workflow_pipeline_name") or "").strip()
    result = state.get("dynamic_pipeline_result") or {}

    if not url:
        return result_card_event(
            "dynamic_pipeline",
            "未识别到 URL",
            "未识别到可用于创建动态采集 Pipeline 的网页地址。",
            actions=[_navigate_action("open_pipelines", "打开 Pipeline", "pipelines")],
            payload={"status": "error"},
        )

    data = result.get("data") if isinstance(result, dict) else None
    data = data if isinstance(data, dict) else {}
    resolved_name = str(data.get("pipeline_name") or pipeline_name or "").strip()

    if result:
        status = str(result.get("status") or "").lower()
        summary = str(result.get("summary") or "").strip()
        if status == "ok":
            return result_card_event(
                "dynamic_pipeline",
                "Pipeline 已创建",
                summary or f"已为 {url} 创建动态采集 Pipeline {resolved_name}。",
                actions=[
                    _navigate_action("open_pipelines", "打开 Pipeline", "pipelines"),
                ],
                payload=_compact_payload(
                    {
                        "status": "success",
                        "pipeline_name": resolved_name,
                        "url": url,
                    }
                ),
            )
        return result_card_event(
            "dynamic_pipeline",
            "Pipeline 创建失败",
            summary or f"为 {url} 创建动态采集 Pipeline 失败。",
            actions=[_navigate_action("open_pipelines", "打开 Pipeline", "pipelines")],
            payload=_compact_payload(
                {
                    "status": "error",
                    "pipeline_name": resolved_name,
                    "url": url,
                }
            ),
        )

    return result_card_event(
        "dynamic_pipeline",
        "Pipeline 草案已准备",
        f"已为 {url} 准备动态采集 Pipeline 草案：{resolved_name or pipeline_name or '未命名'}。",
        actions=[_navigate_action("open_pipelines", "打开 Pipeline", "pipelines")],
        payload=_compact_payload(
            {
                "status": "draft",
                "pipeline_name": resolved_name or pipeline_name,
                "url": url,
            }
        ),
    )


def _navigate_action(action_id: str, label: str, href: str) -> dict[str, Any]:
    return {
        "id": action_id,
        "label": label,
        "kind": "navigate",
        "href": href,
    }


def _task_navigate_actions(task_id: str) -> list[dict[str, Any]]:
    # Always offer tasks navigation when relevant; task_id is optional for UI routing.
    _ = task_id
    return [_navigate_action("open_task", "查看任务", "tasks")]


def _base_payload(task_id: str, *, status: str) -> dict[str, Any]:
    payload: dict[str, Any] = {"status": status}
    if task_id:
        payload["task_id"] = task_id
    return payload


def _compact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None and value != ""}
