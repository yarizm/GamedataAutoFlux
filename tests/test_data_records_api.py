from datetime import datetime

from fastapi.testclient import TestClient

from src.storage.base import StorageRecord
from src.storage.local_store import LocalStorage
from src.web.app import app


def test_data_records_api_returns_paginated_records(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.storage.local_store.get_data_dir", lambda: tmp_path)
    for index in range(3):
        _save_record(
            StorageRecord(
                key=f"record:{index}",
                source="steam" if index < 2 else "taptap",
                data={"collector": "steam" if index < 2 else "taptap", "game_name": f"Game {index}"},
                stored_at=datetime(2026, 1, 1, 12, index, 0),
            )
        )

    with TestClient(app) as client:
        response = client.get("/api/data/records?page=2&page_size=2")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert payload["page"] == 2
    assert payload["page_size"] == 2
    assert payload["has_more"] is False
    assert [item["key"] for item in payload["items"]] == ["record:0"]


def test_data_records_api_filters_by_source_and_sort_order(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.storage.local_store.get_data_dir", lambda: tmp_path)
    for index, source in enumerate(["steam", "steam", "taptap"]):
        _save_record(
            StorageRecord(
                key=f"record:{index}",
                source=source,
                data={"collector": source, "game_name": f"Game {index}"},
                stored_at=datetime(2026, 1, 1, 12, index, 0),
            )
        )

    with TestClient(app) as client:
        response = client.get("/api/data/records?source=steam&page_size=10&sort_order=asc")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert [item["key"] for item in payload["items"]] == ["record:0", "record:1"]
    assert {item["source"] for item in payload["items"]} == {"steam"}


def test_data_records_api_validates_page_size(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.storage.local_store.get_data_dir", lambda: tmp_path)

    with TestClient(app) as client:
        response = client.get("/api/data/records?page_size=201")

    assert response.status_code == 422


def _save_record(record: StorageRecord) -> None:
    import asyncio

    async def run() -> None:
        store = LocalStorage()
        await store.initialize()
        try:
            await store.save(record)
        finally:
            await store.close()

    asyncio.run(run())
