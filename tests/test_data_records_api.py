from datetime import datetime

from fastapi.testclient import TestClient

from src.storage.base import QueryResult, StorageRecord
from src.services.session_registry import InMemorySessionRegistry
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


def test_update_data_record_uses_management_service(monkeypatch) -> None:
    record = StorageRecord(
        key="record:update-managed",
        source="steam",
        data={"collector": "steam", "game_name": "Managed Game"},
        metadata={"display_name": "Before"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )
    _save_record(record)

    called: dict[str, object] = {}

    class _FakeManagementService:
        async def update_record_metadata(self, record_key: str, req) -> None:
            called["record_key"] = record_key
            called["display_name"] = req.display_name
            store = get_storage()
            await store.initialize()
            try:
                current = await store.load(record_key)
                assert current is not None
                await store.save(
                    StorageRecord(
                        key=current.key,
                        source=current.source,
                        data=current.data,
                        stored_at=current.stored_at,
                        tags=current.tags,
                        metadata={**current.metadata, "display_name": req.display_name},
                    )
                )
            finally:
                await store.close()

    monkeypatch.setattr(
        "src.web.routes.data._get_data_management_service",
        lambda: _FakeManagementService(),
    )

    with TestClient(app) as client:
        response = client.patch(
            "/api/data/records/record:update-managed",
            json={"display_name": "After"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert called == {"record_key": "record:update-managed", "display_name": "After"}
    assert payload["display_name"] == "After"


def test_delete_data_category_uses_management_service(monkeypatch) -> None:
    called: dict[str, object] = {}

    class _FakeManagementService:
        async def delete_data_category(
            self,
            *,
            scheduler,
            report_generator,
            game_key: str = "",
            group_id: str = "",
        ) -> dict[str, object]:
            called["game_key"] = game_key
            called["group_id"] = group_id
            called["scheduler"] = scheduler
            called["report_generator"] = report_generator
            return {
                "message": "Data category deleted",
                "game_key": game_key,
                "group_id": group_id,
                "records_deleted": 2,
                "tasks_deleted": 1,
                "cron_jobs_deleted": 1,
                "reports_deleted": 1,
            }

    monkeypatch.setattr(
        "src.web.routes.data._get_data_management_service",
        lambda: _FakeManagementService(),
    )

    with TestClient(app) as client:
        import src.web.app as web_app

        called["scheduler"] = web_app.scheduler
        called["report_generator"] = web_app.report_generator
        response = client.delete("/api/data/games/name:managed-game", params={"confirm": "true"})

    assert response.status_code == 200
    payload = response.json()
    assert called["game_key"] == "name:managed-game"
    assert called["group_id"] == ""
    assert payload["records_deleted"] == 2
    assert payload["tasks_deleted"] == 1
    assert payload["cron_jobs_deleted"] == 1
    assert payload["reports_deleted"] == 1


def test_refresh_routes_use_management_service(monkeypatch) -> None:
    calls: list[tuple[str, str, object]] = []

    class _FakeManagementService:
        async def submit_refresh_task(
            self,
            *,
            task_service,
            record_key: str,
            rolling_window: bool,
        ) -> dict[str, str]:
            calls.append(("refresh", record_key, rolling_window))
            assert task_service is fake_task_service
            return {"message": "Refresh task submitted", "task_id": "task-refresh"}

        async def create_refresh_schedule(
            self,
            *,
            scheduler,
            task_service,
            record_key: str,
            req,
            safe_filename,
        ) -> dict[str, str]:
            calls.append(("schedule", record_key, req.cron_expr))
            assert scheduler is fake_scheduler
            assert task_service is fake_task_service
            assert safe_filename("record:key") == "record_key"
            return {"message": "Refresh schedule created", "job_id": "job-refresh"}

    monkeypatch.setattr(
        "src.web.routes.data._get_data_management_service",
        lambda: _FakeManagementService(),
    )

    with TestClient(app) as client:
        import src.web.app as web_app

        fake_scheduler = web_app.scheduler
        fake_task_service = object()
        monkeypatch.setattr(web_app, "get_task_service", lambda: fake_task_service)
        refresh = client.post(
            "/api/data/records/record:refresh/refresh",
            json={"rolling_window": False},
        )
        schedule = client.post(
            "/api/data/records/record:refresh/refresh-schedules",
            json={"cron_expr": "0 0 * * *", "rolling_window": True},
        )

    assert refresh.status_code == 200
    assert schedule.status_code == 200
    assert refresh.json()["task_id"] == "task-refresh"
    assert schedule.json()["job_id"] == "job-refresh"
    assert calls == [
        ("refresh", "record:refresh", False),
        ("schedule", "record:refresh", "0 0 * * *"),
    ]


def test_refresh_route_uses_task_service_precheck_for_invalid_source_task(
    tmp_path, monkeypatch
) -> None:
    _save_record(
        StorageRecord(
            key="record:dynamic-refresh",
            source="dynamic_playwright",
            data={"collector": "dynamic_playwright", "game_name": "Dynamic Refresh"},
            metadata={
                "source_task": {
                    "task_name": "Dynamic Refresh Source",
                    "pipeline_name": "dynamic_playwright_basic",
                    "collector_name": "dynamic_playwright",
                    "target": "Example Page",
                    "target_type": "web",
                    "target_params": {},
                    "task_config": {},
                }
            },
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/data/records/record:dynamic-refresh/refresh",
            json={"rolling_window": False},
        )

    assert response.status_code == 400
    assert "missing_collector_config" in response.json()["detail"]


def test_refresh_schedule_route_uses_task_service_precheck_for_invalid_source_task(
    tmp_path, monkeypatch
) -> None:
    _save_record(
        StorageRecord(
            key="record:dynamic-refresh-schedule",
            source="dynamic_playwright",
            data={"collector": "dynamic_playwright", "game_name": "Dynamic Refresh Schedule"},
            metadata={
                "source_task": {
                    "task_name": "Dynamic Refresh Schedule Source",
                    "pipeline_name": "dynamic_playwright_basic",
                    "collector_name": "dynamic_playwright",
                    "target": "Example Page",
                    "target_type": "web",
                    "target_params": {},
                    "task_config": {},
                }
            },
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/data/records/record:dynamic-refresh-schedule/refresh-schedules",
            json={"cron_expr": "0 0 * * *", "rolling_window": False},
        )

    assert response.status_code == 400
    assert "missing_collector_config" in response.json()["detail"]


def test_refresh_schedule_succeeds_when_session_inventory_sync_fails(monkeypatch, tmp_path) -> None:
    import src.web.app as app_module

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    class FailingSyncRegistry(InMemorySessionRegistry):
        async def sync_from_diagnostics(self, diagnostics: dict):
            raise RuntimeError("sync failed token=data-refresh-schedule-secret")

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)
    monkeypatch.setattr(app_module, "get_session_registry", lambda: FailingSyncRegistry())

    _save_record(
        StorageRecord(
            key="record:qimai-refresh-schedule",
            source="qimai",
            data={"collector": "qimai", "game_name": "Qimai Refresh Schedule"},
            metadata={
                "source_task": {
                    "task_name": "Qimai Refresh Source",
                    "pipeline_name": "qimai_refresh_schedule_pipeline",
                    "collector_name": "qimai",
                    "target": "Example App",
                    "target_type": "app",
                    "target_params": {"app_id": "123456"},
                    "task_config": {},
                }
            },
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )

    with TestClient(app) as client:
        client.post(
            "/api/pipelines",
            json={
                "name": "qimai_refresh_schedule_pipeline",
                "steps": [{"type": "collector", "name": "qimai", "config": {}}],
            },
        )
        response = client.post(
            "/api/data/records/record:qimai-refresh-schedule/refresh-schedules",
            json={"cron_expr": "0 0 * * *", "rolling_window": False},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Refresh schedule created"
    assert payload["job_id"]


def test_refresh_schedule_succeeds_when_session_registry_lookup_fails(
    monkeypatch, tmp_path
) -> None:
    import src.web.app as app_module

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    def broken_registry_provider():
        raise RuntimeError("registry lookup failed token=data-refresh-schedule-lookup-secret")

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)
    monkeypatch.setattr(app_module, "get_session_registry", broken_registry_provider)

    _save_record(
        StorageRecord(
            key="record:qimai-refresh-schedule-lookup",
            source="qimai",
            data={"collector": "qimai", "game_name": "Qimai Refresh Schedule Lookup"},
            metadata={
                "source_task": {
                    "task_name": "Qimai Refresh Lookup Source",
                    "pipeline_name": "qimai_refresh_schedule_lookup_pipeline",
                    "collector_name": "qimai",
                    "target": "Example App",
                    "target_type": "app",
                    "target_params": {"app_id": "123456"},
                    "task_config": {},
                }
            },
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )

    with TestClient(app) as client:
        client.post(
            "/api/pipelines",
            json={
                "name": "qimai_refresh_schedule_lookup_pipeline",
                "steps": [{"type": "collector", "name": "qimai", "config": {}}],
            },
        )
        response = client.post(
            "/api/data/records/record:qimai-refresh-schedule-lookup/refresh-schedules",
            json={"cron_expr": "0 0 * * *", "rolling_window": False},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Refresh schedule created"
    assert payload["job_id"]


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
