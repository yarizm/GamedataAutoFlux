from src.collectors.base import CollectResult, CollectTarget
from src.core.pipeline_recovery import (
    apply_collect_resume_context,
    build_collect_resume_context,
    build_pipeline_recovery_context,
    build_pipeline_resume_state,
    build_storage_record_key,
    non_negative_int,
    resolve_storage_resume_context,
)
from src.core.task import Task, TaskTarget
from src.processors.base import ProcessInput
from src.storage.base import StorageRecord


def test_build_pipeline_recovery_context_rejects_other_task_checkpoint() -> None:
    task = Task(id="task-a", name="Task A", pipeline_name="p", collector_name="gtrends")

    context = build_pipeline_recovery_context(
        task,
        {
            "task_id": "task-b",
            "seq": 1,
            "state": {"target_order": ["A"], "next_target_index": 1},
        },
    )

    assert context == {}


def test_build_collect_resume_context_rejects_target_order_mismatch() -> None:
    task = Task(
        id="task-a",
        name="Task A",
        pipeline_name="p",
        collector_name="gtrends",
        targets=[TaskTarget(name="A"), TaskTarget(name="B")],
    )

    context = build_collect_resume_context(
        task,
        cursor={"stage": "collect"},
        state={"target_order": ["A", "C"], "next_target_index": 1},
    )

    assert context == {}


def test_apply_collect_resume_context_skips_completed_targets() -> None:
    targets = [CollectTarget(name="A"), CollectTarget(name="B"), CollectTarget(name="C")]

    remaining = apply_collect_resume_context(
        targets,
        {"enabled": True, "next_target_index": 2},
    )

    assert [target.name for target in remaining] == ["C"]


def test_resolve_storage_resume_context_uses_checkpoint_sequence_and_offset() -> None:
    context = resolve_storage_resume_context(
        {
            "seq": 3,
            "collect": {
                "enabled": True,
                "next_target_index": 2,
            },
        },
        current_data=[ProcessInput(data={"value": 1}, metadata={}, source="target-c")],
    )

    assert context == {
        "resume_run_index": 3,
        "resume_offset": 2,
        "start_target": "target-c",
    }


def test_build_storage_record_key_uses_resume_run_and_offset() -> None:
    task = Task(id="task-a", name="Task A", pipeline_name="p")
    process_input = ProcessInput(data={"value": 1}, metadata={}, source="processor")

    key = build_storage_record_key(
        task,
        process_input,
        index=1,
        storage_context={"resume_run_index": 2, "resume_offset": 3},
    )

    assert key == "task-a:processor:2:4"


def test_build_pipeline_resume_state_tracks_completed_targets_and_keys() -> None:
    task = Task(
        id="task-a",
        name="Task A",
        pipeline_name="p",
        targets=[TaskTarget(name="A"), TaskTarget(name="B"), TaskTarget(name="C")],
    )
    collect_results = [
        CollectResult(target=CollectTarget(name="C"), success=True, data={"value": 1}),
    ]
    output_records = [
        StorageRecord(
            key="task-a:processor:1:2",
            data={"value": 1},
            metadata={},
            source="processor",
        )
    ]

    state = build_pipeline_resume_state(
        task,
        recovery_context={"collect": {"next_target_index": 2}},
        collect_results=collect_results,
        output_records=output_records,
    )

    assert state == {
        "target_order": ["A", "B", "C"],
        "next_target_index": 3,
        "completed_targets": ["A", "B", "C"],
        "successful_targets": ["C"],
        "failed_targets": [],
        "output_record_keys": ["task-a:processor:1:2"],
    }


def test_non_negative_int_clamps_invalid_values() -> None:
    assert non_negative_int("3") == 3
    assert non_negative_int(-1, default=5) == 5
    assert non_negative_int("oops", default=7) == 7
