"""Recovery and resume helpers for Pipeline execution."""

from __future__ import annotations

from typing import Any

from src.collectors.base import CollectResult, CollectTarget
from src.core.task import Task
from src.processors.base import ProcessInput
from src.storage.base import StorageRecord


def build_pipeline_recovery_context(
    task: Task,
    recovery_checkpoint: dict[str, Any] | None,
) -> dict[str, Any]:
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
) -> dict[str, Any]:
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
) -> dict[str, Any]:
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


def build_pipeline_resume_state(
    task: Task,
    *,
    recovery_context: dict[str, Any],
    collect_results: list[CollectResult],
    output_records: list[StorageRecord],
) -> dict[str, Any]:
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
    return merged


def non_negative_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default
