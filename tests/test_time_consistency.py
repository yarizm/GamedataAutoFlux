"""Core runtime timestamps are timezone-aware UTC by default."""

from datetime import datetime, timedelta, timezone

from src.collectors.base import CollectResult, CollectTarget
from src.core.dag import DAGResult
from src.core.pipeline import PipelineResult
from src.processors.base import ProcessOutput
from src.storage.base import StorageRecord
from src.storage.models import to_naive_utc


def test_core_result_timestamps_are_utc_aware() -> None:
    assert CollectResult(target=CollectTarget(name="x")).collected_at.tzinfo == timezone.utc
    assert ProcessOutput(data={}, processor_name="test").processed_at.tzinfo == timezone.utc
    assert StorageRecord(key="x", data={}).stored_at.tzinfo == timezone.utc
    assert PipelineResult(pipeline_name="p", task_id="t").started_at.tzinfo == timezone.utc
    assert DAGResult(pipeline_name="d", task_id="t").started_at.tzinfo == timezone.utc


def test_to_naive_utc_converts_aware_and_keeps_naive() -> None:
    """DB 边界转换：aware → naive UTC，naive 原样返回。"""
    aware = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=8)))
    converted = to_naive_utc(aware)
    assert converted.tzinfo is None
    assert converted == datetime(2026, 1, 1, 4, 0, 0)

    naive = datetime(2026, 1, 1, 12, 0, 0)
    assert to_naive_utc(naive) == naive


async def test_storage_writes_aware_stored_at_as_naive_utc() -> None:
    """aware stored_at 落库后必须是同一时刻的 naive UTC。

    列类型是 naive DateTime（PostgreSQL TIMESTAMP WITHOUT TIME ZONE）：
    asyncpg 会直接拒绝 aware 值，SQLite 则会静默丢掉偏移量，
    因此这里断言的是"存进去的时刻没被扭曲"。
    """
    from src.storage.factory import get_storage

    store = get_storage()
    await store.initialize()
    try:
        aware = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=8)))
        await store.save(StorageRecord(key="tz:aware", data={}, stored_at=aware))

        loaded = await store.load("tz:aware")
        assert loaded is not None
        assert loaded.stored_at.tzinfo is None
        assert loaded.stored_at == datetime(2026, 1, 1, 4, 0, 0)
    finally:
        await store.close()
