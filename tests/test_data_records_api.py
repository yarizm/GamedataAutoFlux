from datetime import datetime

from fastapi.testclient import TestClient

from src.storage.base import QueryResult, StorageRecord
from src.storage.factory import get_storage
from src.web.app import app


def test_data_records_api_returns_paginated_records(tmp_path, monkeypatch) -> None:
    for index in range(3):
        _save_record(
            StorageRecord(
                key=f"record:{index}",
                source="steam" if index < 2 else "taptap",
                data={
                    "collector": "steam" if index < 2 else "taptap",
                    "game_name": f"Game {index}",
                },
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


def test_data_records_api_exposes_top_level_task_metadata(tmp_path, monkeypatch) -> None:
    _save_record(
        StorageRecord(
            key="record:task-top",
            source="steam",
            data={"collector": "steam", "game_name": "Task Game"},
            metadata={"task_id": "task-top", "task_name": "Top Task"},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )

    with TestClient(app) as client:
        response = client.get("/api/data/records?task_id=task-top")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["task_id"] == "task-top"
    assert payload["items"][0]["task_name"] == "Top Task"


def test_game_records_source_filter_matches_collector_key(tmp_path, monkeypatch) -> None:
    _save_record(
        StorageRecord(
            key="record:official",
            source="cleaner",
            data={"collector": "official_site", "game_name": "Example Game"},
            metadata={"source_task": {"collector_name": "official_site"}},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )

    with TestClient(app) as client:
        response = client.get(
            "/api/data/games/name:example-game/records",
            params={"source": "official_site"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["key"] == "record:official"
    assert payload["items"][0]["data_source"] == "official website"


def test_data_records_api_source_filter_matches_collector_key(tmp_path, monkeypatch) -> None:
    _save_record(
        StorageRecord(
            key="record:official-list",
            source="cleaner",
            data={"collector": "official_site", "game_name": "Example Game"},
            metadata={"source_task": {"collector_name": "official_site"}},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )

    with TestClient(app) as client:
        response = client.get("/api/data/records", params={"source": "official_site"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["key"] == "record:official-list"
    assert payload["items"][0]["source"] == "cleaner"
    assert payload["items"][0]["data_source"] == "official website"


def test_data_records_api_source_filter_avoids_short_alias_overmatch(tmp_path, monkeypatch) -> None:
    _save_record(
        StorageRecord(
            key="record:discussions",
            source="steam_discussions",
            data={"collector": "steam_discussions", "game_name": "Counter-Strike 2"},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )

    with TestClient(app) as client:
        response = client.get("/api/data/records", params={"source": "Steam"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 0
    assert payload["items"] == []


def test_data_search_matches_collector_key(tmp_path, monkeypatch) -> None:
    _save_record(
        StorageRecord(
            key="record:official-search",
            source="cleaner",
            data={"collector": "official_site", "game_name": "Example Game"},
            metadata={"source_task": {"collector_name": "official_site"}},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )

    with TestClient(app) as client:
        response = client.get("/api/data/search", params={"q": "official_site"})

    assert response.status_code == 200
    payload = response.json()
    assert [item["key"] for item in payload] == ["record:official-search"]


def test_data_record_detail_download_and_export_redact_sensitive_content(
    tmp_path,
    monkeypatch,
) -> None:
    record = StorageRecord(
        key="record:sensitive-detail",
        source="steam",
        data={
            "collector": "steam",
            "game_name": "Sensitive Game",
            "api_key": "data-secret",
            "snapshot": {"price": "token=summary-secret"},
            "nested": {"token": "nested-secret"},
        },
        metadata={
            "api_key": "metadata-secret",
            "display_name": "token=display-secret",
            "source_task": {
                "task_id": "task-sensitive",
                "task_name": "api_key=task-secret",
            },
        },
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    _save_record(record)

    with TestClient(app) as client:
        detail = client.get("/api/data/records/record:sensitive-detail")
        download = client.get("/api/data/records/record:sensitive-detail/download")
        exported = client.post(
            "/api/data/records/batch-export",
            json={"keys": ["record:sensitive-detail"]},
        )

    assert detail.status_code == 200
    detail_payload = detail.json()
    assert detail_payload["metadata"]["api_key"] == "[REDACTED]"
    assert detail_payload["display_name"] == "token=[REDACTED]"
    assert detail_payload["task_name"] == "api_key=[REDACTED]"
    assert detail_payload["data"]["api_key"] == "[REDACTED]"
    assert detail_payload["data"]["nested"]["token"] == "[REDACTED]"
    assert detail_payload["summary"]["price"] == "token=[REDACTED]"

    assert download.status_code == 200
    assert exported.status_code == 200
    rendered = detail.text + download.text + exported.text
    assert "data-secret" not in rendered
    assert "metadata-secret" not in rendered
    assert "summary-secret" not in rendered
    assert "nested-secret" not in rendered
    assert "display-secret" not in rendered
    assert "task-secret" not in rendered


def test_data_records_api_closes_storage_after_listing(monkeypatch) -> None:
    fake_store = _FakeListStorage(
        [
            StorageRecord(
                key="record:close",
                source="steam",
                data={"collector": "steam", "game_name": "Close Test"},
                stored_at=datetime(2026, 1, 1, 12, 0, 0),
            )
        ]
    )
    monkeypatch.setattr("src.web.routes.data.get_storage", lambda: fake_store)

    with TestClient(app) as client:
        response = client.get("/api/data/records")

    assert response.status_code == 200
    assert fake_store.initialized is True
    assert fake_store.closed is True


def test_data_records_api_source_filter_scans_all_storage_pages(monkeypatch) -> None:
    records = [
        StorageRecord(
            key=f"record:taptap:{index}",
            source="taptap",
            data={"collector": "taptap", "game_name": f"TapTap {index}"},
            stored_at=datetime(2026, 1, 1, 12, index, 0),
        )
        for index in range(4)
    ]
    records.extend(
        [
            StorageRecord(
                key="record:steam:0",
                source="steam",
                data={"collector": "steam", "game_name": "Steam 0"},
                stored_at=datetime(2026, 1, 1, 12, 4, 0),
            ),
            StorageRecord(
                key="record:steam:1",
                source="steam",
                data={"collector": "steam", "game_name": "Steam 1"},
                stored_at=datetime(2026, 1, 1, 12, 5, 0),
            ),
        ]
    )
    fake_store = _FakeListStorage(records)
    monkeypatch.setattr("src.web.routes.data.get_storage", lambda: fake_store)
    monkeypatch.setattr("src.web.routes.data._SOURCE_FILTER_SCAN_PAGE_SIZE", 2)

    with TestClient(app) as client:
        response = client.get(
            "/api/data/records",
            params={"source": "steam", "page_size": 1, "sort_order": "asc"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["has_more"] is True
    assert [item["key"] for item in payload["items"]] == ["record:steam:0"]
    assert [call["offset"] for call in fake_store.query_calls] == [0, 2, 4]


def test_data_records_api_validates_page_size(tmp_path, monkeypatch) -> None:

    with TestClient(app) as client:
        response = client.get("/api/data/records?page_size=201")

    assert response.status_code == 422


def _save_record(record: StorageRecord) -> None:
    import asyncio

    async def run() -> None:
        store = get_storage()
        await store.initialize()
        try:
            await store.save(record)
        finally:
            await store.close()

    asyncio.run(run())


class _FakeListStorage:
    def __init__(self, records: list[StorageRecord]) -> None:
        self.records = records
        self.initialized = False
        self.closed = False
        self.query_calls: list[dict] = []

    async def initialize(self) -> None:
        self.initialized = True

    async def close(self) -> None:
        self.closed = True

    async def query(self, query: str, limit: int = 10, **kwargs):
        offset = int(kwargs.get("offset", 0) or 0)
        order = str(kwargs.get("order", "desc") or "desc")
        records = sorted(
            self.records,
            key=lambda record: record.stored_at,
            reverse=order != "asc",
        )
        self.query_calls.append({"query": query, "limit": limit, "offset": offset, "order": order})
        return QueryResult(
            records=records[offset : offset + limit],
            total=len(records),
            query=query,
        )
