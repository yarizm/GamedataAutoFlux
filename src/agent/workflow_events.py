"""SSE payload builders for Agent workflow meta-events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

STEP_RUNNING = "running"
STEP_DONE = "done"
STEP_FAILED = "failed"
STEP_SKIPPED = "skipped"

END_SUCCESS = "success"
END_FAILED = "failed"
END_FALLBACK = "fallback"


@dataclass(frozen=True)
class WorkflowMeta:
    """Static metadata for a routed workflow graph (P1 path bar)."""

    workflow_id: str
    label: str
    entry_node: str
    compose_node: str
    steps: tuple[dict[str, str], ...]


# Step ids align with WorkflowToolBridgeDefinition.step_id in workflow_graphs.py
# plus prepare/respond logical steps that have no tool bridge.
WORKFLOW_META: dict[str, WorkflowMeta] = {
    "report_workflow": WorkflowMeta(
        workflow_id="report_workflow",
        label="报告链路",
        entry_node="load_task_detail_report",
        compose_node="compose_report_response",
        steps=(
            {"id": "load_task", "label": "加载任务"},
            {"id": "review", "label": "复查采集"},
            {"id": "precheck", "label": "报告预检"},
            {"id": "generate", "label": "生成报告"},
            {"id": "respond", "label": "汇总"},
        ),
    ),
    "task_review_workflow": WorkflowMeta(
        workflow_id="task_review_workflow",
        label="任务诊断",
        entry_node="load_task_detail_task_review",
        compose_node="compose_task_review_response",
        steps=(
            {"id": "load_task", "label": "加载任务"},
            {"id": "review", "label": "复查采集"},
            {"id": "respond", "label": "汇总"},
        ),
    ),
    "pipeline_workflow": WorkflowMeta(
        workflow_id="pipeline_workflow",
        label="动态采集",
        entry_node="prepare_dynamic_pipeline",
        compose_node="compose_pipeline_response",
        steps=(
            {"id": "prepare", "label": "准备草案"},
            {"id": "create_pipeline", "label": "创建Pipeline"},
            {"id": "respond", "label": "汇总"},
        ),
    ),
    "readiness_workflow": WorkflowMeta(
        workflow_id="readiness_workflow",
        label="系统就绪",
        entry_node="resolve_readiness_target",
        compose_node="compose_readiness_response",
        steps=(
            {"id": "resolve_target", "label": "识别目标"},
            {"id": "check_config", "label": "配置检查"},
            {"id": "check_session", "label": "会话/登录态"},
            {"id": "respond", "label": "汇总"},
        ),
    ),
    "cron_workflow": WorkflowMeta(
        workflow_id="cron_workflow",
        label="定时任务",
        entry_node="resolve_cron_intent",
        compose_node="compose_cron_response",
        steps=(
            {"id": "resolve_intent", "label": "识别意图"},
            {"id": "resolve_schedule", "label": "解析调度"},
            {"id": "apply_action", "label": "执行操作"},
            {"id": "respond", "label": "汇总"},
        ),
    ),
    "multisource_workflow": WorkflowMeta(
        workflow_id="multisource_workflow",
        label="多源采集",
        entry_node="resolve_multisource_intent",
        compose_node="compose_multisource_response",
        steps=(
            {"id": "resolve_intent", "label": "识别目标"},
            {"id": "build_draft", "label": "生成草案"},
            {"id": "apply_action", "label": "提交任务"},
            {"id": "respond", "label": "汇总"},
        ),
    ),
}

ENTRY_NODE_TO_WORKFLOW: dict[str, str] = {
    meta.entry_node: meta.workflow_id for meta in WORKFLOW_META.values()
}

COMPOSE_NODE_TO_WORKFLOW: dict[str, str] = {
    meta.compose_node: meta.workflow_id for meta in WORKFLOW_META.values()
}


def workflow_start_event(
    workflow_id: str,
    label: str,
    steps: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "type": "workflow_start",
        "workflow_id": workflow_id,
        "label": label,
        "steps": [
            {"id": str(s.get("id") or ""), "label": str(s.get("label") or "")}
            for s in steps
        ],
    }


def workflow_step_event(
    workflow_id: str,
    step_id: str,
    label: str,
    status: str,
) -> dict[str, Any]:
    return {
        "type": "workflow_step",
        "workflow_id": workflow_id,
        "step_id": step_id,
        "label": label,
        "status": status,
    }


def workflow_end_event(
    workflow_id: str,
    status: str,
    reason: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "type": "workflow_end",
        "workflow_id": workflow_id,
        "status": status,
    }
    if reason:
        payload["reason"] = reason
    return payload


def result_card_event(
    card_type: str,
    title: str,
    summary: str,
    *,
    actions: list[dict[str, Any]] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "type": "result_card",
        "card_type": card_type,
        "title": title,
        "summary": summary,
        "actions": list(actions or []),
        "payload": dict(payload or {}),
    }
