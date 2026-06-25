"""Tests for Pipeline model and PipelineResult."""

from src.core.pipeline import (
    Pipeline,
    PipelineResult,
    PipelineStep,
    StepType,
    _build_storage_metadata,
    _collect_failure_message,
)
from src.collectors.base import CollectResult, CollectTarget
from src.core.task import Task, TaskTarget
from src.collectors.base import BaseCollector
from src.core.registry import registry
from src.storage.base import BaseStorage, StorageRecord
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput


class TestPipelineConstruction:
    def test_default_name(self):
        p = Pipeline("test")
        assert p.name == "test"
        assert p.steps == []

    def test_add_collector(self):
        p = Pipeline("p")
        p.add_collector("steam", {"delay": 1.0})
        assert len(p.steps) == 1
        assert p.steps[0].step_type == StepType.COLLECTOR
        assert p.steps[0].component_name == "steam"
        assert p.steps[0].config == {"delay": 1.0}

    def test_add_processor(self):
        p = Pipeline("p")
        p.add_processor("cleaner")
        assert p.steps[0].step_type == StepType.PROCESSOR
        assert p.steps[0].component_name == "cleaner"
        assert p.steps[0].config == {}

    def test_add_storage(self):
        p = Pipeline("p")
        p.add_storage("local", {"db_name": "test.db"})
        assert p.steps[0].step_type == StepType.STORAGE
        assert p.steps[0].component_name == "local"

    def test_chaining(self):
        p = Pipeline("p").add_collector("steam").add_processor("cleaner").add_storage("local")
        assert len(p.steps) == 3
        assert [s.step_type for s in p.steps] == [
            StepType.COLLECTOR,
            StepType.PROCESSOR,
            StepType.STORAGE,
        ]

    def test_on_progress(self):
        called = []

        async def cb(tid, prog, msg):
            called.append((tid, prog, msg))

        p = Pipeline("p").on_progress(cb)
        assert p._progress_callback is cb


class TestPipelineStepFilters:
    def test_get_collectors(self, pipeline_basic):
        cs = pipeline_basic._get_collectors()
        assert len(cs) == 1
        assert cs[0].component_name == "steam"

    def test_get_processors(self, pipeline_basic):
        ps = pipeline_basic._get_processors()
        assert len(ps) == 1
        assert ps[0].component_name == "cleaner"

    def test_get_storages(self, pipeline_basic):
        ss = pipeline_basic._get_storages()
        assert len(ss) == 1
        assert ss[0].component_name == "local"


class TestPipelineConfig:
    def test_to_config(self, pipeline_basic):
        cfg = pipeline_basic.to_config()
        assert cfg["name"] == "test_pipeline"
        assert len(cfg["steps"]) == 3
        assert cfg["steps"][0]["type"] == "collector"
        assert cfg["steps"][0]["name"] == "steam"
        assert cfg["steps"][1]["type"] == "processor"
        assert cfg["steps"][2]["type"] == "storage"

    def test_from_config(self):
        cfg = {
            "name": "restored",
            "steps": [
                {"type": "collector", "name": "taptap", "config": {"delay": 2.0}},
                {"type": "storage", "name": "local"},
            ],
        }
        p = Pipeline.from_config(cfg)
        assert p.name == "restored"
        assert len(p.steps) == 2
        assert p.steps[0].component_name == "taptap"
        assert p.steps[0].config == {"delay": 2.0}

    def test_roundtrip(self, pipeline_basic):
        cfg = pipeline_basic.to_config()
        restored = Pipeline.from_config(cfg)
        assert restored.name == pipeline_basic.name
        assert len(restored.steps) == len(pipeline_basic.steps)
        for a, b in zip(restored.steps, pipeline_basic.steps):
            assert a.step_type == b.step_type
            assert a.component_name == b.component_name
            assert a.config == b.config


class TestPipelineResult:
    def test_default_values(self):
        r = PipelineResult(pipeline_name="p", task_id="t1")
        assert r.pipeline_name == "p"
        assert r.task_id == "t1"
        assert r.success is True
        assert r.collect_results == []
        assert r.process_results == []
        assert r.output_records == []
        assert r.storage_count == 0
        assert r.errors == []

    def test_duration_none_before_complete(self):
        r = PipelineResult(pipeline_name="p", task_id="t1")
        assert r.duration_seconds is None

    def test_errors_default_empty(self):
        r = PipelineResult(pipeline_name="p", task_id="t1")
        r.errors.append("something went wrong")
        assert r.errors == ["something went wrong"]

    def test_collection_summary_reports_failures_and_retries_redacted(self):
        r = PipelineResult(pipeline_name="p", task_id="t1")
        r.collect_results = [
            CollectResult(
                target=CollectTarget(name="CS2 api_key=target-secret"),
                success=False,
                error="network failed api_key=result-secret",
                error_code="network_unreachable",
                metadata={
                    "attempts": 3,
                    "max_attempts": 3,
                    "retry_attempts": 2,
                    "last_retry_error": "timeout token=retry-secret",
                },
            ),
            CollectResult(
                target=CollectTarget(name="Dota 2"),
                success=True,
                data={"ok": True},
                metadata={"attempts": 2, "max_attempts": 3, "retry_attempts": 1},
            ),
        ]

        summary = r.collection_summary
        rendered = str(summary)

        assert summary["status"] == "partial"
        assert summary["total_targets"] == 2
        assert summary["successful_targets"] == 1
        assert summary["failed_targets_count"] == 1
        assert summary["retried_targets_count"] == 2
        assert summary["retry_attempts_total"] == 3
        assert summary["error_codes"] == {"network_unreachable": 1}
        assert summary["failed_targets"][0]["target"] == "CS2 api_key=[REDACTED]"
        assert summary["failed_targets"][0]["retry"]["retry_attempts"] == 2
        assert summary["retried_targets"][1]["target"] == "Dota 2"
        assert "target-secret" not in rendered
        assert "result-secret" not in rendered
        assert "retry-secret" not in rendered

    def test_task_result_summary_includes_collection_summary(self):
        r = PipelineResult(pipeline_name="p", task_id="t1", success=False)
        r.collect_results = [
            CollectResult(
                target=CollectTarget(name="CS2"),
                success=False,
                error="rate limit 429",
                error_code="rate_limited",
                metadata={"attempts": 2, "max_attempts": 2, "retry_attempts": 1},
            )
        ]
        task = Task(id="t1", name="T")
        task.result = r

        summary = task.result_summary

        assert summary is not None
        assert summary["collection_summary"]["status"] == "failed"
        assert summary["collection_summary"]["failed_targets_count"] == 1
        assert summary["collection_summary"]["failed_targets"][0]["error_code"] == "rate_limited"

    def test_collect_failure_message_includes_last_retry_context_redacted(self):
        message = _collect_failure_message(
            CollectResult(
                target=CollectTarget(name="CS2 api_key=target-secret"),
                success=False,
                error="network failed token=result-secret",
                error_code="network_unreachable",
                metadata={
                    "attempts": 3,
                    "max_attempts": 3,
                    "retry_attempts": 2,
                    "last_retry_error": "HTTP 429 password=retry-secret",
                    "last_retry_error_code": "rate_limited",
                },
            )
        )

        assert "attempts 3/3, retries 2" in message
        assert "last retry [rate_limited] HTTP 429 password=[REDACTED]" in message
        assert "target-secret" not in message
        assert "result-secret" not in message
        assert "retry-secret" not in message


class TestPipelineStorageMetadata:
    def test_build_storage_metadata_redacts_collector_metadata_and_task_context(self):
        task = Task(
            id="task-redact",
            name="Secret Task",
            pipeline_name="steam_basic",
            collector_name="steam",
            targets=[
                TaskTarget(
                    name="CS2",
                    target_type="game",
                    params={"app_id": "730", "api_key": "target-secret"},
                )
            ],
            config={"cookie": "task-cookie", "data_group": {"name": "CS2"}},
        )

        metadata = _build_storage_metadata(
            task,
            {
                "target": "CS2",
                "collector": "steam",
                "api_key": "collector-secret",
                "nested": {"token": "collector-token"},
            },
        )

        assert metadata["api_key"] == "[REDACTED]"
        assert metadata["nested"]["token"] == "[REDACTED]"
        assert metadata["source_task"]["target_params"] == {
            "app_id": "730",
            "api_key": "[REDACTED]",
        }
        assert metadata["source_task"]["task_config"]["cookie"] == "[REDACTED]"
        assert "collector-secret" not in str(metadata)
        assert "collector-token" not in str(metadata)
        assert "target-secret" not in str(metadata)
        assert "task-cookie" not in str(metadata)


class TestPipelineStep:
    def test_defaults(self):
        s = PipelineStep(step_type=StepType.COLLECTOR, component_name="steam")
        assert s.step_type == StepType.COLLECTOR
        assert s.component_name == "steam"
        assert s.config == {}
        assert s.instance is None

    def test_with_config(self):
        s = PipelineStep(step_type=StepType.STORAGE, component_name="local", config={"db": "x.db"})
        assert s.config == {"db": "x.db"}


@registry.register("collector", "resume_test_collector")
class _ResumeTestCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(
            target=target,
            data={"value": target.name},
            metadata={"collector": "resume_test", "target": target.name},
        )


@registry.register("processor", "resume_test_processor")
class _ResumeTestProcessor(BaseProcessor):
    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        return ProcessOutput(
            data=input_data.data,
            metadata=input_data.metadata,
            processor_name="resume_test_processor",
        )


@registry.register("storage", "resume_test_storage")
class _ResumeTestStorage(BaseStorage):
    saved_batches: list[list[StorageRecord]] = []

    async def save(self, record: StorageRecord) -> None:
        self.saved_batches.append([record])

    async def save_batch(self, records: list[StorageRecord]) -> None:
        self.saved_batches.append(list(records))

    async def load(self, key: str) -> StorageRecord | None:
        return None

    async def query(self, query: str, limit: int = 10, **kwargs):
        raise NotImplementedError


def test_pipeline_resume_skips_completed_targets_and_avoids_key_collisions():
    import src.storage.factory as storage_factory

    _ResumeTestStorage.saved_batches.clear()
    original_get_storage = storage_factory.get_storage
    storage_factory.get_storage = lambda name=None: _ResumeTestStorage()
    pipeline = (
        Pipeline("resume_pipeline")
        .add_collector("resume_test_collector")
        .add_processor("resume_test_processor")
        .add_storage("resume_test_storage")
    )
    task = Task(
        id="resume-task",
        name="Resume Task",
        pipeline_name="resume_pipeline",
        collector_name="gtrends",
        targets=[
            TaskTarget(name="A"),
            TaskTarget(name="B"),
            TaskTarget(name="C"),
        ],
    )

    import asyncio

    checkpoint = {
        "checkpoint_id": "checkpoint-1",
        "task_id": task.id,
        "recovery_level": "L1",
        "cursor": {"stage": "collect", "component": "gtrends", "status": "failed"},
        "state": {
            "target_order": ["A", "B", "C"],
            "next_target_index": 2,
            "completed_targets": ["A", "B"],
        },
    }
    try:
        result = asyncio.run(pipeline.execute(task, recovery_checkpoint=checkpoint))
    finally:
        storage_factory.get_storage = original_get_storage

    assert result.success is True
    assert [item.target.name for item in result.collect_results] == ["C"]
    assert result.resume_state["next_target_index"] == 3
    assert result.output_records[0].key == "resume-task:resume_test_processor:1:2"
    assert _ResumeTestStorage.saved_batches[-1][0].key == "resume-task:resume_test_processor:1:2"
