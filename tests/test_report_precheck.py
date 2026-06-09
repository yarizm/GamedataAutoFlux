from datetime import datetime

from fastapi.testclient import TestClient

from src.reporting.generator import GeneratedReport
from src.storage.base import StorageRecord
from src.storage.factory import get_storage
from src.web.app import app


def test_report_precheck_reports_missing_template_sources(tmp_path, monkeypatch) -> None:
    _save_record(
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
    _save_record(
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


def test_report_precheck_rejects_report_history_record_key(tmp_path, monkeypatch) -> None:
    _save_report(report_id="history-only", excel_path="")

    with TestClient(app) as client:
        response = client.post(
            "/api/reports/precheck",
            json={
                "prompt": "generate report",
                "template": "auto",
                "record_keys": ["report:history-only"],
            },
        )

    assert response.status_code == 400
    assert "report history" in response.json()["detail"]


def test_report_precheck_filters_report_history_from_selected_record_keys(
    tmp_path,
    monkeypatch,
) -> None:
    _save_report(report_id="history-mixed", excel_path="")
    _save_record(
        StorageRecord(
            key="record:steam:selected",
            source="steam",
            data={"collector": "steam", "game_name": "Example Game"},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        ),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/reports/precheck",
            json={
                "prompt": "generate report",
                "template": "auto",
                "record_keys": ["report:history-mixed", "record:steam:selected"],
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_records"] == 1
    assert payload["source_counts"]["steam"] == 1


def test_report_precheck_empty_result_keeps_template_requirements(tmp_path, monkeypatch) -> None:

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


def test_report_precheck_data_source_filter_matches_source_labels(tmp_path, monkeypatch) -> None:
    _save_record(
        StorageRecord(
            key="report:official-history",
            source="reporting",
            data={"title": "Old official report", "data_source": "official website"},
            metadata={"kind": "report", "data_source": "official website"},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        ),
    )
    _save_record(
        StorageRecord(
            key="report:official:1",
            source="cleaner",
            data={
                "collector": "official_site",
                "game_name": "Example Game",
                "items": [{"title": "News"}],
            },
            metadata={"source_task": {"collector_name": "official_site"}},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        ),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/reports/precheck",
            json={
                "prompt": "generate official report",
                "template": "auto",
                "data_source": "official website",
                "params": {"limit": 5},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "complete"
    assert payload["selected_records"] == 1
    assert payload["source_counts"]["official_site"] == 1


def test_report_precheck_auto_scan_excludes_report_history(tmp_path, monkeypatch) -> None:
    _save_report(report_id="precheck-history", excel_path="")
    _save_record(
        StorageRecord(
            key="record:steam:precheck",
            source="steam",
            data={"collector": "steam", "game_name": "Example Game"},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        ),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/reports/precheck",
            json={
                "prompt": "generate report",
                "template": "auto",
                "params": {"limit": 5},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_records"] == 1
    assert payload["source_counts"]["steam"] == 1


def test_report_precheck_accepts_invalid_limit_param(tmp_path, monkeypatch) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/reports/precheck",
            json={
                "prompt": "generate report",
                "template": "steam_game",
                "data_source": "missing-source",
                "params": {"limit": "not-a-number"},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "empty"


def test_report_group_records_source_filter_matches_collector_key(tmp_path, monkeypatch) -> None:
    _save_record(
        StorageRecord(
            key="report:official:grouped",
            source="cleaner",
            data={
                "collector": "official_site",
                "game_name": "Example Game",
                "items": [{"title": "News"}],
            },
            metadata={
                "group_id": "example-group",
                "group_name": "Example Group",
                "source_task": {"collector_name": "official_site"},
            },
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        ),
    )

    with TestClient(app) as client:
        response = client.get(
            "/api/reports/group-records",
            params={"group_id": "example-group", "source": "official_site"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["key"] == "report:official:grouped"
    assert payload[0]["data_source"] == "official website"


def test_excel_report_download_allows_configured_reports_dir(tmp_path, monkeypatch) -> None:
    report_id = "download-ok"
    excel_path = tmp_path / "report_download-ok.xlsx"
    excel_path.write_bytes(b"xlsx")
    _save_report(report_id=report_id, excel_path=str(excel_path))

    with TestClient(app) as client:
        _set_reports_dir(str(tmp_path))
        response = client.get(f"/api/reports/{report_id}/download")

    assert response.status_code == 200
    assert response.content == b"xlsx"


def test_excel_report_download_rejects_outside_reports_dir(tmp_path, monkeypatch) -> None:
    allowed_dir = tmp_path / "reports"
    allowed_dir.mkdir()
    outside_path = tmp_path / "outside.xlsx"
    outside_path.write_bytes(b"xlsx")
    report_id = "download-forbidden"
    _save_report(report_id=report_id, excel_path=str(outside_path))

    with TestClient(app) as client:
        _set_reports_dir(str(allowed_dir))
        response = client.get(f"/api/reports/{report_id}/download")

    assert response.status_code == 403


def test_delete_report_removes_excel_inside_reports_dir(tmp_path, monkeypatch) -> None:
    allowed_dir = tmp_path / "reports"
    allowed_dir.mkdir()
    excel_path = allowed_dir / "report_delete-ok.xlsx"
    excel_path.write_bytes(b"xlsx")
    report_id = "delete-ok"
    _save_report(report_id=report_id, excel_path=str(excel_path))

    with TestClient(app) as client:
        _set_reports_dir(str(allowed_dir))
        response = client.delete(f"/api/reports/{report_id}", params={"confirm": True})

    assert response.status_code == 200
    assert not excel_path.exists()


def test_delete_report_keeps_excel_outside_reports_dir(tmp_path, monkeypatch) -> None:
    captured: list[str] = []
    allowed_dir = tmp_path / "reports"
    allowed_dir.mkdir()
    outside_path = tmp_path / "outside-api_key=delete-secret.xlsx"
    outside_path.write_bytes(b"xlsx")
    report_id = "delete-outside"
    _save_report(report_id=report_id, excel_path=str(outside_path))
    monkeypatch.setattr(
        "src.reporting.generator.logger.warning",
        lambda message, *args: captured.append(str(message).format(*args)),
    )

    with TestClient(app) as client:
        _set_reports_dir(str(allowed_dir))
        response = client.delete(f"/api/reports/{report_id}", params={"confirm": True})

    assert response.status_code == 200
    assert outside_path.exists()
    rendered = " ".join(captured)
    assert "delete-secret" not in rendered
    assert "api_key=[REDACTED]" in rendered


def test_report_list_includes_compact_quality_summary(tmp_path, monkeypatch) -> None:
    _save_report(
        report_id="quality-list",
        excel_path="",
        metadata={
            "format": "excel",
            "source_record_count": 3,
            "usable_record_count": 2,
            "source_record_keys": ["record:one", "record:two", "record:three"],
            "source_coverage": {"steam": 1, "gtrends": 1},
            "record_completeness": {"full": 2, "empty": 1},
            "empty_record_keys": ["record:empty"],
            "template_validation": {
                "status": "partial",
                "missing_collectors": ["monitor"],
            },
        },
    )

    with TestClient(app) as client:
        response = client.get("/api/reports", params={"limit": 20})

    assert response.status_code == 200
    report = next(item for item in response.json() if item["id"] == "quality-list")
    assert report["quality"] == {
        "format": "excel",
        "source_record_count": 3,
        "usable_record_count": 2,
        "source_coverage": {"steam": 1, "gtrends": 1},
        "record_completeness": {"full": 2, "empty": 1},
        "template_status": "partial",
        "quality_status": "partial",
        "quality_summary": "Report was generated with partial source coverage; missing sources: Monitor.",
        "regeneration_recommended": True,
        "coverage_risks": [
            "Template coverage is missing required sources: Monitor.",
            "1 selected records had no usable data.",
        ],
        "missing_collectors": ["monitor"],
        "empty_record_count": 1,
    }
    assert "source_record_keys" not in report["quality"]


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


def _save_report(
    *,
    report_id: str,
    excel_path: str,
    metadata: dict | None = None,
) -> None:
    report = GeneratedReport(
        id=report_id,
        title="Download test",
        prompt="test",
        data_source="",
        template="default",
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        matched_records=0,
        content="Report generated as an Excel file",
        excel_path=excel_path,
        metadata=metadata or {},
    )
    _save_record(
        StorageRecord(
            key=f"report:{report_id}",
            source="reporting",
            data=report.model_dump(mode="json"),
            metadata={"kind": "report", "template": "default"},
            tags=["report", "default"],
        ),
    )


def _set_reports_dir(path: str) -> None:
    from src.core.config import get_settings

    get_settings().setdefault("storage", {})["reports_dir"] = path
