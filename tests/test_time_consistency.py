"""Core runtime timestamps are timezone-aware UTC by default."""

from datetime import timezone

from src.collectors.base import CollectResult, CollectTarget
from src.core.dag import DAGResult
from src.core.pipeline import PipelineResult
from src.processors.base import ProcessOutput
from src.storage.base import StorageRecord


def test_core_result_timestamps_are_utc_aware() -> None:
    assert CollectResult(target=CollectTarget(name="x")).collected_at.tzinfo == timezone.utc
    assert ProcessOutput(data={}, processor_name="test").processed_at.tzinfo == timezone.utc
    assert StorageRecord(key="x", data={}).stored_at.tzinfo == timezone.utc
    assert PipelineResult(pipeline_name="p", task_id="t").started_at.tzinfo == timezone.utc
    assert DAGResult(pipeline_name="d", task_id="t").started_at.tzinfo == timezone.utc
