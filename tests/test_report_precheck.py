from datetime import datetime

from fastapi.testclient import TestClient

from src.storage.base import StorageRecord
from src.storage.local_store import LocalStorage
from src.web.app import app


def test_report_precheck_reports_missing_template_sources(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.storage.local_store.get_data_dir", lambda: tmp_path)
    _save_record(
        tmp_path,
        StorageRecord(
            key="report:steam:1",
            source="steam",
            data={"collector": "steam", "game": {"name": "Example Game"}},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        ),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/reports/precheck",
            json={
                "prompt": "generate report",
                "template": "steam_game",
                "record_keys": ["report:steam:1"],
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "partial"
    assert payload["selected_records"] == 1
    assert payload["source_counts"]["steam"] == 1
    assert {"gtrends", "monitor", "events", "steam_discussions"} <= set(
        payload["missing_collectors"]
    )


def test_report_precheck_accepts_complete_taptap_template(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.storage.local_store.get_data_dir", lambda: tmp_path)
    _save_record(
        tmp_path,
        StorageRecord(
            key="report:taptap:1",
            source="taptap",
            data={"collector": "taptap", "game": {"name": "Example Mobile Game"}},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        ),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/reports/precheck",
            json={
                "prompt": "generate report",
                "template": "taptap_game",
                "record_keys": ["report:taptap:1"],
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "complete"
    assert payload["missing_collectors"] == []


def test_report_precheck_rejects_missing_record(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.storage.local_store.get_data_dir", lambda: tmp_path)

    with TestClient(app) as client:
        response = client.post(
            "/api/reports/precheck",
            json={
                "prompt": "generate report",
                "template": "taptap_game",
                "record_keys": ["missing"],
            },
        )

    assert response.status_code == 404


def test_report_precheck_empty_result_keeps_template_requirements(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("src.storage.local_store.get_data_dir", lambda: tmp_path)

    with TestClient(app) as client:
        response = client.post(
            "/api/reports/precheck",
            json={
                "prompt": "generate report",
                "template": "steam_game",
                "data_source": "missing-source",
                "params": {"limit": 1},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "empty"
    assert payload["known_template"] is True
    assert "steam" in payload["missing_collectors"]


def _save_record(tmp_path, record: StorageRecord) -> None:
    import asyncio

    async def run() -> None:
        store = LocalStorage()
        await store.initialize()
        try:
            await store.save(record)
        finally:
            await store.close()

    asyncio.run(run())
