from datetime import datetime, timedelta

import pytest

from src.storage.base import StorageRecord
from src.storage.local_store import LocalStorage


@pytest.mark.asyncio
async def test_local_storage_query_supports_offset_and_matched_total(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.storage.local_store.get_data_dir", lambda: tmp_path)
    store = LocalStorage({"db_name": "test.db", "json_dir": "records"})
    await store.initialize()
    try:
        base_time = datetime(2026, 1, 1, 12, 0, 0)
        for index in range(5):
            await store.save(
                StorageRecord(
                    key=f"task:{index}",
                    source="steam" if index < 3 else "taptap",
                    data={"index": index},
                    stored_at=base_time + timedelta(minutes=index),
                )
            )

        result = await store.query("key:task:", limit=2, offset=1)

        assert result.total == 5
        assert [record.key for record in result.records] == ["task:3", "task:2"]
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_local_storage_query_counts_filtered_source(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.storage.local_store.get_data_dir", lambda: tmp_path)
    store = LocalStorage({"db_name": "test.db", "json_dir": "records"})
    await store.initialize()
    try:
        for index, source in enumerate(["steam", "steam", "taptap"]):
            await store.save(
                StorageRecord(
                    key=f"record:{index}",
                    source=source,
                    data={"source": source},
                    stored_at=datetime(2026, 1, 1, 12, index, 0),
                )
            )

        result = await store.query("source:steam", limit=10)

        assert result.total == 2
        assert {record.source for record in result.records} == {"steam"}
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_local_storage_query_supports_ascending_order(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.storage.local_store.get_data_dir", lambda: tmp_path)
    store = LocalStorage({"db_name": "test.db", "json_dir": "records"})
    await store.initialize()
    try:
        for index in range(3):
            await store.save(
                StorageRecord(
                    key=f"record:{index}",
                    source="steam",
                    data={"index": index},
                    stored_at=datetime(2026, 1, 1, 12, index, 0),
                )
            )

        result = await store.query("source:steam", limit=3, order="asc")

        assert [record.key for record in result.records] == ["record:0", "record:1", "record:2"]
    finally:
        await store.close()
