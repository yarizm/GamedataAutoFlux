"""Recovery and resume helpers for Pipeline execution.

恢复上下文/断点状态历史上是匿名 dict；本模块用 TypedDict 固化它们的
键契约（运行时仍是普通 dict——这些结构会整体 JSON 序列化进 checkpoint
与 worker claim payload，形状必须保持兼容）。输入侧仍接受未信任的
``dict[str, Any]``（来自持久化 JSON），**输出**类型即本模块契约。
"""

from __future__ import annotations

from typing import Any, TypedDict

from src.collectors.base import CollectResult, CollectTarget
from src.core.task import Task
from src.processors.base import ProcessInput
from src.storage.base import StorageRecord


class CollectResumeContext(TypedDict, total=False):
    """多 target 断点续采上下文（recovery_context["collect"]）。"""

    enabled: bool
    next_target_index: int
    target_order: list[str]
    completed_targets: list[str]
    cursor: dict[str, Any]
    state: dict[str, Any]


class PipelineRecoveryContext(TypedDict, total=False):
    """checkpoint → 执行上下文的投影（build_pipeline_recovery_context）。"""

    checkpoint_id: str
    task_id: str
    seq: int
    collector_name: str
    recovery_level: str
    cursor: dict[str, Any]
    state: dict[str, Any]
    metadata: dict[str, Any]
    collect: CollectResumeContext


class StorageResumeContext(TypedDict, total=False):
    """存储键续跑定位（resolve_storage_resume_context）。"""

    resume_run_index: int
    resume_offset: int
    start_target: str


class PipelineResumeState(TypedDict, total=False):
    """任务结束时的续跑快照（PipelineResult.resume_state）。"""

    target_order: list[str]
    next_target_index: int
    completed_targets: list[str]
    successful_targets: list[str]
    failed_targets: list[str]
    output_record_keys: list[str]


def build_pipeline_recovery_context(
    task: Task,
    recovery_checkpoint: dict[str, Any] | None,
) -> PipelineRecoveryContext:
    checkpoint = recovery_checkpoint if isinstance(recovery_checkpoint, dict) else {}
    if not checkpoint:
        return {}

    checkpoint_task_id = str(checkpoint.get("task_id") or "").strip()
    if checkpoint_task_id and checkpoint_task_id != task.id:
        return {}

    cursor = checkpoint.get("cursor")
    cursor_payload = dict(cursor) if isinstance(cursor, dict) else {}
    state = checkpoint.get("state")
    state_payload = dict(state) if isinstance(state, dict) else {}
    metadata = checkpoint.get("metadata")
    metadata_payload = dict(metadata) if isinstance(metadata, dict) else {}
    recovery_level = str(checkpoint.get("recovery_level") or "").strip().upper()

    return {
        "checkpoint_id": str(checkpoint.get("checkpoint_id") or "").strip(),
        "task_id": checkpoint_task_id or task.id,
        "seq": non_negative_int(checkpoint.get("seq")),
        "collector_name": str(
            checkpoint.get("collector_name") or task.collector_name or ""
        ).strip(),
        "recovery_level": recovery_level or "L0",
        "cursor": cursor_payload,
        "state": state_payload,
        "metadata": metadata_payload,
        "collect": build_collect_resume_context(
            task,
            cursor=cursor_payload,
            state=state_payload,
        ),
    }


def build_collect_resume_context(
    task: Task,
    *,
    cursor: dict[str, Any],
    state: dict[str, Any],
) -> CollectResumeContext:
    target_order = state.get("target_order")
    if not isinstance(target_order, list) or not target_order:
        return {}

    normalized_targets = [str(name).strip() for name in target_order if str(name or "").strip()]
    if not normalized_targets:
        return {}

    current_targets = [str(target.name or "").strip() for target in task.targets]
    if current_targets and current_targets != normalized_targets:
        return {}

    next_index = non_negative_int(state.get("next_target_index"))
    completed_targets = state.get("completed_targets")
    completed_names = []
    if isinstance(completed_targets, list):
        completed_names = [
            str(name).strip() for name in completed_targets if str(name or "").strip()
        ]

    return {
        "enabled": True,
        "next_target_index": min(next_index, len(normalized_targets)),
        "target_order": normalized_targets,
        "completed_targets": completed_names,
        "cursor": cursor,
        "state": state,
    }


def apply_collect_resume_context(
    targets: list[CollectTarget],
    collect_context: dict[str, Any],
) -> list[CollectTarget]:
    if not collect_context.get("enabled"):
        return list(targets)

    next_index = non_negative_int(collect_context.get("next_target_index"))
    if next_index <= 0:
        return list(targets)
    if next_index >= len(targets):
        return []
    return list(targets[next_index:])


def resolve_storage_resume_context(
    recovery_context: dict[str, Any],
    *,
    current_data: list[ProcessInput],
) -> StorageResumeContext:
    collect_context = recovery_context.get("collect", {})
    if not isinstance(collect_context, dict) or not collect_context.get("enabled"):
        return {"resume_run_index": 0, "resume_offset": 0}

    next_index = non_negative_int(collect_context.get("next_target_index"))
    start_target = ""
    if current_data:
        start_target = str(current_data[0].source or "").strip()

    resume_run_index = non_negative_int(recovery_context.get("seq"))
    if resume_run_index <= 0:
        resume_run_index = 1

    return {
        "resume_run_index": resume_run_index,
        "resume_offset": next_index,
        "start_target": start_target,
    }


def build_storage_record_key(
    task: Task,
    process_input: ProcessInput,
    *,
    index: int,
    storage_context: dict[str, Any],
) -> str:
    source = str(process_input.source or "unknown").strip() or "unknown"
    resume_run_index = non_negative_int(storage_context.get("resume_run_index"))
    resume_offset = non_negative_int(storage_context.get("resume_offset"))
    sequence = resume_offset + max(0, int(index))
    if resume_run_index > 0:
        return f"{task.id}:{source}:{resume_run_index}:{sequence}"
    return f"{task.id}:{source}:{sequence}"


def _stored_target_names(records: list[StorageRecord]) -> set[str]:
    """从落库记录反推 target 名（存储 metadata 同时保留 target 与 source_task.target）。"""
    names: set[str] = set()
    for record in records or []:
        meta = getattr(record, "metadata", None) or {}
        if not isinstance(meta, dict):
            continue
        source_task = meta.get("source_task")
        candidates = [meta.get("target")]
        if isinstance(source_task, dict):
            candidates.append(source_task.get("target"))
        for candidate in candidates:
            name = str(candidate or "").strip()
            if name:
                names.add(name)
                break
    return names


def _restrict_resume_state_to_stored(
    state: PipelineResumeState,
    records: list[StorageRecord],
) -> PipelineResumeState:
    """失败收尾时把"完成"收紧到真正落库的 target。

    checkpoint 此前只按"采集成功"推进 ``next_target_index``；采集之后的阶段失败时
    数据从未落库，这个"完成"是假的——自动重试据此跳过全部采集，手里又没有数据，
    必然再次失败。这里按落库结果重算，让重试能真正重采未落库的 target，
    已落库的部分仍然保留，不浪费。
    """
    stored = _stored_target_names(records)
    order = [str(name) for name in (state.get("target_order") or [])]
    next_index = 0
    for idx, name in enumerate(order):
        if name not in stored:
            break
        next_index = idx + 1
    bounded = min(next_index, len(order))
    return {
        **state,
        "next_target_index": bounded,
        "completed_targets": order[:bounded],
        "successful_targets": [n for n in order if n in stored],
    }


def build_pipeline_resume_state(
    task: Task,
    *,
    recovery_context: dict[str, Any],
    collect_results: list[CollectResult],
    output_records: list[StorageRecord],
    run_succeeded: bool = True,
) -> PipelineResumeState:
    from src.core.collector_resume import merge_checkpoint_state

    target_order = [
        str(target.name or "").strip() for target in task.targets if str(target.name or "").strip()
    ]
    collect_context = (
        recovery_context.get("collect", {}) if isinstance(recovery_context, dict) else {}
    )
    if not isinstance(collect_context, dict):
        collect_context = {}

    # Prefer explicit success lists; fall back to completed_targets as success prefix only.
    previous_success = collect_context.get("successful_targets")
    if not isinstance(previous_success, list):
        previous_success = collect_context.get("completed_targets") or []

    merged = merge_checkpoint_state(
        target_order=target_order,
        previous={"successful_targets": list(previous_success)},
        collect_results=collect_results,
    )
    merged["output_record_keys"] = [record.key for record in output_records]
    if not run_succeeded:
        merged = _restrict_resume_state_to_stored(merged, output_records)
    return merged


def non_negative_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default
