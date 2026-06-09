"""
任务管理工具
"""

import copy
from typing import Any, Type
from langchain_core.tools import BaseTool
from pydantic import BaseModel

from src.agent.schemas import (
    CancelTaskInput,
    CreateTaskInput,
    GetTaskDetailInput,
    ListTasksInput,
)
from src.agent.tools.utils import _format_result, _safe_error_text
from src.agent.tools.identifiers import _auto_fill_identifiers


def _identifier_changes(before: list[dict], after: list[dict]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for index, target in enumerate(after):
        previous = before[index] if index < len(before) and isinstance(before[index], dict) else {}
        previous_params = previous.get("params", {}) if isinstance(previous.get("params"), dict) else {}
        params = target.get("params", {}) if isinstance(target.get("params"), dict) else {}
        added = {
            key: value
            for key, value in params.items()
            if value not in (None, "") and previous_params.get(key) in (None, "")
        }
        changed = {
            key: value
            for key, value in params.items()
            if value not in (None, "")
            and key in previous_params
            and previous_params.get(key) not in (None, "", value)
        }
        if added or changed:
            changes.append(
                {
                    "target_index": index,
                    "target_name": target.get("name", ""),
                    "added_params": added,
                    "changed_params": changed,
                }
            )
    return changes


def _task_detail_guidance(payload: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    task_id = str(payload.get("id") or "")
    status = str(payload.get("status") or "unknown")
    result_summary = payload.get("result_summary")
    if not isinstance(result_summary, dict):
        result_summary = {}
    collection_summary = result_summary.get("collection_summary")
    if not isinstance(collection_summary, dict):
        collection_summary = {}

    collection_status = str(collection_summary.get("status") or "")
    failed_targets = _safe_int(collection_summary.get("failed_targets_count"))
    stored_count = _safe_int(result_summary.get("storage_count"))

    if collection_status == "partial" and failed_targets > 0:
        return (
            "Task kept usable partial collection data but some targets failed. Review the "
            "collection failures, create targeted follow-up collection tasks, then rerun "
            "report precheck before generating a report.",
            [
                {
                    "type": "review_collection_results",
                    "recommended_tool": "review_collection_results",
                    "args": {"task_id": task_id, "auto_retry": False},
                    "why": "Inspect failed targets, retry metadata, and stored source records.",
                },
                {
                    "type": "precheck_report",
                    "recommended_tool": "precheck_report",
                    "why": "Confirm whether the partial source data is enough for the requested report.",
                },
            ],
        )

    if collection_status == "failed" or status == "failed":
        return (
            "Task failed before producing enough usable source data. Review collection results "
            "and identifiers before creating a retry or replacement task.",
            [
                {
                    "type": "review_collection_results",
                    "recommended_tool": "review_collection_results",
                    "args": {"task_id": task_id, "auto_retry": False},
                    "why": "Use structured failure details before deciding whether to retry.",
                },
                {
                    "type": "create_task",
                    "recommended_tool": "create_task",
                    "why": "Create a corrected follow-up task after fixing identifiers or parameters.",
                },
            ],
        )

    if status == "success" or stored_count > 0:
        return (
            "Task produced source data. Run report precheck to verify coverage before generating.",
            [
                {
                    "type": "precheck_report",
                    "recommended_tool": "precheck_report",
                    "why": "Check source coverage and missing collectors before report generation.",
                },
                {
                    "type": "generate_report",
                    "recommended_tool": "generate_report",
                    "why": "Generate the report once source coverage is acceptable.",
                },
            ],
        )

    return "", []


def _task_detail_suggestion(
    *,
    status: str,
    guidance: str,
    actions: list[dict[str, Any]],
) -> str:
    if actions:
        tool_names = [str(action.get("recommended_tool") or "") for action in actions]
        ordered = [tool for tool in tool_names if tool]
        if ordered:
            return guidance + " Suggested tool order: " + " -> ".join(ordered)
    if status == "success":
        return "使用 generate_report 为此任务的数据生成报告"
    return ""


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


class ListTasksTool(BaseTool):
    name: str = "list_tasks"
    description: str = (
        "获取任务列表，可按状态过滤。"
        "status 可选值: pending / running / success / failed / cancelled"
    )
    args_schema: Type[BaseModel] = ListTasksInput

    async def _arun(self, status: str | None = None) -> str:
        from src.web.app import get_task_service

        try:
            tasks = get_task_service().list_tasks(status)
        except ValueError:
            return _format_result(
                "error",
                f"无效的状态: {status}",
                suggestion="status 可选: pending / running / success / failed / cancelled",
            )

        summaries = [t.to_summary() for t in tasks[:50]]
        status_counts = {}
        for t in tasks:
            s = t.status.value if hasattr(t.status, "value") else str(t.status)
            status_counts[s] = status_counts.get(s, 0) + 1
        count_desc = ", ".join(f"{v} {k}" for k, v in sorted(status_counts.items()))
        return _format_result(
            "ok",
            f"共 {len(tasks)} 个任务（{count_desc}），展示最近 {len(summaries)} 个",
            summaries,
            record_count=len(summaries),
            suggestion="使用 get_task_detail 查看任意任务详情",
        )

    def _run(self, status: str | None = None) -> str:
        raise NotImplementedError("Use _arun")


class GetTaskDetailTool(BaseTool):
    name: str = "get_task_detail"
    description: str = "获取单个任务的详细信息，包括步骤日志和结果摘要"
    args_schema: Type[BaseModel] = GetTaskDetailInput

    async def _arun(self, task_id: str) -> str:
        from src.web.app import get_task_service

        task = get_task_service().get_task(task_id)
        if not task:
            return _format_result(
                "error", f"任务不存在: {task_id}", suggestion="使用 list_tasks 查看所有任务"
            )
        payload = task.to_public_payload()
        status = payload.get("status", "unknown")
        guidance, recommended_actions = _task_detail_guidance(payload)
        if guidance:
            payload["agent_guidance"] = guidance
        if recommended_actions:
            payload["recommended_actions"] = recommended_actions
        return _format_result(
            "ok",
            f"任务 '{payload.get('name', task_id)}' 当前状态: {status}",
            payload,
            record_count=1,
            suggestion=_task_detail_suggestion(
                status=str(status),
                guidance=guidance,
                actions=recommended_actions,
            ),
        )

    def _run(self, task_id: str) -> str:
        raise NotImplementedError("Use _arun")


class CreateTaskTool(BaseTool):
    name: str = "create_task"
    description: str = (
        "创建并提交一个新的数据采集任务。"
        "需要指定任务名称(name)、Pipeline 模板 ID(pipeline_name)和采集目标(targets)。"
        'targets 格式: [{"name": "游戏名", "target_type": "game", "params": {"app_id": 123}}]。'
        "config 可选，支持 report.enabled / data_group 等配置。"
    )
    args_schema: Type[BaseModel] = CreateTaskInput

    async def _arun(
        self,
        name: str,
        pipeline_name: str,
        targets: list[dict] | None = None,
        collector_name: str = "",
        config: dict | None = None,
    ) -> str:
        from src.web.app import get_task_service

        targets = targets or []
        config = config or {}

        ts = get_task_service()

        requested_targets = copy.deepcopy(targets)
        targets = await _auto_fill_identifiers(targets, pipeline_name)
        identifier_changes = _identifier_changes(requested_targets, targets)

        precheck = ts.precheck(
            name=name,
            pipeline_name=pipeline_name,
            collector_name=collector_name,
            targets=targets,
        )
        if not precheck.can_submit:
            issues_desc = "; ".join(f"[{i.level}] {i.field}: {i.message}" for i in precheck.issues)
            return _format_result(
                "error",
                f"任务创建预校验失败: {issues_desc}",
                [
                    {"level": i.level, "code": i.code, "field": i.field, "message": i.message}
                    for i in precheck.issues
                ],
                warnings=[i.message for i in precheck.issues if i.level == "warning"],
                suggestion="请补充必填字段后重试。必填字段: " + ", ".join(precheck.required_fields),
            )

        try:
            task = await ts.create(
                name=name,
                pipeline_name=pipeline_name,
                collector_name=collector_name,
                targets=targets,
                config=config,
            )
            response = {
                "success": True,
                "task_id": task.id,
                "task_name": name,
                "pipeline": pipeline_name,
                "collector_name": getattr(task, "collector_name", collector_name),
                "targets_count": len(targets),
                "targets": targets,
            }
            if identifier_changes:
                response["auto_filled_identifiers"] = identifier_changes
            warnings = [i.message for i in precheck.issues if i.level == "warning"]
            return _format_result(
                "ok",
                f"任务 '{name}' 已创建并提交，task_id: {task.id}",
                response,
                record_count=1,
                warnings=warnings if warnings else None,
                suggestion="使用 list_tasks 查看任务状态，或使用 get_task_detail 查看详情",
            )
        except Exception as e:
            from src.core.errors import classify_exception, error_summary

            code = classify_exception(e)
            safe_error = _safe_error_text(e)
            from loguru import logger

            logger.error(f"Agent 创建任务失败: [{code.value}] {safe_error}")
            return _format_result(
                "error",
                f"任务提交失败: {safe_error}",
                error_summary(code, safe_error),
                suggestion=error_summary(code)["suggestion"],
            )

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")


class CancelTaskTool(BaseTool):
    name: str = "cancel_task"
    description: str = "取消一个正在运行或等待中的任务"
    args_schema: Type[BaseModel] = CancelTaskInput

    async def _arun(self, task_id: str) -> str:
        from src.web.app import get_task_service

        ok = await get_task_service().cancel(task_id)
        if ok:
            return _format_result("ok", f"任务已取消: {task_id}")
        return _format_result("error", f"取消失败（任务可能已结束或不存在）: {task_id}")

    def _run(self, task_id: str) -> str:
        raise NotImplementedError("Use _arun")
