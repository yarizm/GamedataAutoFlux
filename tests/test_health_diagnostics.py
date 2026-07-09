from fastapi.testclient import TestClient

from src.core.config import get as get_config
from src.core.config import load_settings
from src.core.config_schema import validate_settings_payload
from src.core.diagnostics import (
    build_collector_session_diagnostics,
    build_config_diagnostics,
    build_health_report,
    build_session_diagnostics_overview,
)
from src.services.session_registry import InMemorySessionRegistry
from src.web.app import app


def test_build_health_report_shape() -> None:
    report = build_health_report({"started": True})

    assert report["status"] in {"ok", "warning", "error"}
    assert isinstance(report["checks"], list)
    assert report["summary"]["scheduler"]["started"] is True
    assert all({"name", "status", "message", "details"} <= set(check) for check in report["checks"])
    assert any(check["name"] == "database.config" for check in report["checks"])
    assert any(check["name"] == "scheduler.execution_backend" for check in report["checks"])


def test_build_config_diagnostics_shape() -> None:
    diagnostics = build_config_diagnostics()

    assert diagnostics["status"] in {"ok", "warning", "error"}
    assert isinstance(diagnostics["checks"], list)
    assert diagnostics["paths"]["root_dir"]
    assert any(check["name"] == "llm.provider" for check in diagnostics["checks"])
    assert any(check["name"] == "settings_schema" for check in diagnostics["checks"])
    assert any(check["name"] == "agent.runtime_compatibility" for check in diagnostics["checks"])


def test_build_collector_session_diagnostics_for_qimai_profile(monkeypatch, tmp_path) -> None:
    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)

    diagnostics = build_collector_session_diagnostics("qimai")

    assert diagnostics["collector_id"] == "qimai"
    assert diagnostics["requires_session"] is True
    assert diagnostics["session_mode"] == "local_profile"
    assert diagnostics["worker_binding"] == "sticky"
    assert diagnostics["required_worker_capabilities"] == [
        "session:qimai_profile",
        "session_mode:local_profile",
    ]
    assert diagnostics["session_account"]["account_kind"] == "local_profile"
    assert diagnostics["session_account"]["account_id"] == "local:qimai_profile"
    assert diagnostics["session_lease"]["strategy"] == "sticky_worker"
    assert diagnostics["session_state"]["health"] == "ready"
    assert diagnostics["session_state"]["local_profile_ready"] is True
    assert diagnostics["status"] == "ok"
    assert any(check["name"] == "session:qimai_profile" for check in diagnostics["checks"])


def test_build_session_diagnostics_overview(monkeypatch, tmp_path) -> None:
    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
        "steam.steamdb.enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)

    overview = build_session_diagnostics_overview(["qimai", "steam"])

    assert overview["summary"]["collectors"] == 2
    assert overview["summary"]["requires_session"] == 1
    assert overview["status"] in {"ok", "warning", "error"}
    assert [item["collector_id"] for item in overview["collectors"]] == ["qimai", "steam"]
    assert overview["collectors"][0]["session_account"]["account_kind"] == "local_profile"
    assert overview["collectors"][1]["session_lease"]["transferable"] is True


def test_build_collector_session_diagnostics_for_qimai_managed_state(monkeypatch, tmp_path) -> None:
    storage_state = tmp_path / "qimai_storage_state.json"
    storage_state.write_text("{}", encoding="utf-8")

    values = {
        "qimai.session_mode": "managed_state",
        "qimai.storage_state_path": str(storage_state),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    diagnostics = build_collector_session_diagnostics("qimai")

    assert diagnostics["session_mode"] == "managed_state"
    assert diagnostics["worker_binding"] == "lease"
    assert diagnostics["required_worker_capabilities"] == ["session_mode:managed_state"]
    assert diagnostics["session_account"]["account_kind"] == "managed_state"
    assert diagnostics["session_lease"]["strategy"] == "exclusive_lease"
    assert diagnostics["session_state"]["health"] == "ready"
    assert diagnostics["session_state"]["storage_state_ready"] is True
    assert any(check["name"] == "session:qimai_storage_state" for check in diagnostics["checks"])


def test_settings_schema_accepts_minimal_payload() -> None:
    validation = validate_settings_payload(
        {
            "server": {"port": 8000},
            "scheduler": {"max_concurrent_tasks": 1},
        }
    )

    assert validation["valid"] is True
    assert validation["issues"] == []


def test_settings_schema_accepts_batch_concurrency() -> None:
    validation = validate_settings_payload(
        {
            "collector": {
                "batch_concurrency": 2,
                "collect_timeout": 30,
                "collect_retries": 1,
                "collect_retry_delay": 0.5,
            },
            "steam": {"batch_concurrency": 2, "collect_timeout": 30, "collect_retries": 1},
            "taptap": {"batch_concurrency": 2, "collect_timeout": 30, "collect_retries": 1},
            "monitor": {"batch_concurrency": 2, "collect_timeout": 30, "collect_retries": 1},
            "gtrends": {"collect_timeout": 30, "collect_retries": 1},
        }
    )

    assert validation["valid"] is True
    assert validation["normalized"]["collector"]["batch_concurrency"] == 2
    assert validation["normalized"]["steam"]["batch_concurrency"] == 2
    assert validation["normalized"]["taptap"]["batch_concurrency"] == 2
    assert validation["normalized"]["monitor"]["batch_concurrency"] == 2
    assert validation["normalized"]["collector"]["collect_timeout"] == 30
    assert validation["normalized"]["collector"]["collect_retries"] == 1
    assert validation["normalized"]["collector"]["collect_retry_delay"] == 0.5
    assert validation["normalized"]["gtrends"]["collect_timeout"] == 30
    assert validation["normalized"]["gtrends"]["collect_retries"] == 1


def test_settings_schema_accepts_agent_runtime_controls() -> None:
    validation = validate_settings_payload(
        {
            "agent": {
                "runtime_backend": "langgraph_agent",
                "agent_type": "openai_tools",
                "langgraph_checkpointer": {"backend": "file", "file_path": "data/agent_checkpoints.json"},
                "playwright_mcp": {
                    "enabled": True,
                    "command": "npx",
                    "args": ["-y", "@playwright/mcp"],
                    "headless": True,
                    "max_exploration_steps": 12,
                },
            }
        }
    )

    assert validation["valid"] is True
    assert validation["warnings"] == []
    assert validation["normalized"]["agent"]["runtime_backend"] == "langgraph_agent"
    assert validation["normalized"]["agent"]["agent_type"] == "openai_tools"
    assert validation["normalized"]["agent"]["langgraph_checkpointer"]["backend"] == "file"
    assert (
        validation["normalized"]["agent"]["langgraph_checkpointer"]["file_path"]
        == "data/agent_checkpoints.json"
    )
    assert validation["normalized"]["agent"]["playwright_mcp"]["enabled"] is True
    assert validation["normalized"]["agent"]["playwright_mcp"]["max_exploration_steps"] == 12


def test_settings_schema_defaults_to_langgraph_runtime_backend() -> None:
    validation = validate_settings_payload({"agent": {}})

    assert validation["valid"] is True
    assert validation["normalized"]["agent"]["runtime_backend"] == "langgraph_agent"
    assert validation["normalized"]["agent"]["agent_type"] == "openai_tools"


def test_settings_schema_warns_on_langgraph_react_combo() -> None:
    validation = validate_settings_payload(
        {
            "agent": {
                "runtime_backend": "langgraph_agent",
                "agent_type": "react",
            }
        }
    )

    assert validation["valid"] is True
    assert validation["issues"] == []
    assert validation["warnings"] == [
        {
            "path": "agent.agent_type",
            "message": (
                "agent.agent_type=react is ignored when "
                "agent.runtime_backend=langgraph_agent; the effective mode is openai_tools."
            ),
            "type": "compatibility_warning",
        }
    ]


def test_build_config_diagnostics_warns_on_langgraph_react_combo(monkeypatch) -> None:
    def fake_get_config(key: str, default=None):
        if key == "agent.runtime_backend":
            return "langgraph_agent"
        if key == "agent.agent_type":
            return "react"
        return get_config(key, default)

    monkeypatch.setattr(
        "src.core.diagnostics.get_settings_validation",
        lambda: {"valid": True, "issues": [], "warnings": [], "normalized": {}},
    )
    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)

    diagnostics = build_config_diagnostics()
    check = next(
        item for item in diagnostics["checks"] if item["name"] == "agent.runtime_compatibility"
    )

    assert check["status"] == "warning"
    assert check["details"]["runtime_backend"] == "langgraph_agent"
    assert check["details"]["configured_agent_type"] == "react"
    assert check["details"]["effective_agent_type"] == "openai_tools"


def test_settings_schema_reports_invalid_values() -> None:
    validation = validate_settings_payload(
        {
            "server": {"port": 70000},
            "scheduler": {"max_concurrent_tasks": 0},
        }
    )

    assert validation["valid"] is False
    assert {issue["path"] for issue in validation["issues"]} == {
        "server.port",
        "scheduler.max_concurrent_tasks",
    }


def test_settings_schema_rejects_invalid_agent_runtime_controls() -> None:
    validation = validate_settings_payload(
        {
            "agent": {
                "runtime_backend": "legacy",
                "agent_type": "xml",
                "langgraph_checkpointer": {"backend": "redis", "file_path": ""},
                "playwright_mcp": {"max_exploration_steps": 0},
            }
        }
    )

    assert validation["valid"] is False
    assert {issue["path"] for issue in validation["issues"]} == {
        "agent.runtime_backend",
        "agent.agent_type",
        "agent.langgraph_checkpointer.backend",
        "agent.playwright_mcp.max_exploration_steps",
    }


def test_settings_schema_rejects_invalid_batch_concurrency() -> None:
    validation = validate_settings_payload(
        {
            "collector": {
                "batch_concurrency": 0,
                "collect_timeout": -1,
                "collect_retries": -1,
                "collect_retry_delay": -1,
            },
            "steam": {"batch_concurrency": 0, "collect_timeout": -1, "collect_retries": -1},
            "taptap": {"batch_concurrency": 0, "collect_timeout": -1, "collect_retries": -1},
            "monitor": {"batch_concurrency": 0, "collect_timeout": -1, "collect_retries": -1},
            "gtrends": {"collect_timeout": -1, "collect_retries": -1},
        }
    )

    assert validation["valid"] is False
    assert {issue["path"] for issue in validation["issues"]} == {
        "collector.batch_concurrency",
        "steam.batch_concurrency",
        "taptap.batch_concurrency",
        "monitor.batch_concurrency",
        "collector.collect_timeout",
        "collector.collect_retries",
        "collector.collect_retry_delay",
        "steam.collect_timeout",
        "steam.collect_retries",
        "taptap.collect_timeout",
        "taptap.collect_retries",
        "monitor.collect_timeout",
        "monitor.collect_retries",
        "gtrends.collect_timeout",
        "gtrends.collect_retries",
    }


def test_server_env_overrides_settings(monkeypatch) -> None:
    monkeypatch.setenv("SERVER_HOST", "0.0.0.0")
    monkeypatch.setenv("SERVER_PORT", "8123")

    load_settings()

    assert get_config("server.host") == "0.0.0.0"
    assert get_config("server.port") == 8123


def test_health_api_returns_diagnostic_payload() -> None:
    with TestClient(app) as client:
        response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] in {"ok", "warning", "error"}
    assert isinstance(payload["checks"], list)
    assert "summary" in payload


def test_config_diagnostics_api_returns_diagnostic_payload() -> None:
    with TestClient(app) as client:
        response = client.get("/api/diagnostics/config")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] in {"ok", "warning", "error"}
    assert isinstance(payload["checks"], list)
    assert "paths" in payload


def test_session_diagnostics_api_returns_diagnostic_payload() -> None:
    with TestClient(app) as client:
        response = client.get("/api/diagnostics/sessions")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] in {"ok", "warning", "error"}
    assert "summary" in payload
    assert isinstance(payload["collectors"], list)


def test_session_diagnostics_api_returns_payload_when_inventory_sync_fails(
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

    class FailingSyncRegistry(InMemorySessionRegistry):
        async def sync_from_diagnostics(self, diagnostics: dict):
            raise RuntimeError("sync failed token=health-overview-secret")

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)
    monkeypatch.setattr(app_module, "get_session_registry", lambda: FailingSyncRegistry())

    with TestClient(app) as client:
        response = client.get("/api/diagnostics/sessions?collectors=qimai")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["collectors"] == 1
    assert payload["collectors"][0]["collector_id"] == "qimai"


def test_session_diagnostics_api_returns_payload_when_registry_lookup_fails(
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
        raise RuntimeError("registry lookup failed token=health-lookup-secret")

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)
    monkeypatch.setattr(app_module, "get_session_registry", broken_registry_provider)

    with TestClient(app) as client:
        response = client.get("/api/diagnostics/sessions?collectors=qimai")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["collectors"] == 1
    assert payload["collectors"][0]["collector_id"] == "qimai"


def test_single_collector_session_diagnostics_api_returns_runtime_model() -> None:
    with TestClient(app) as client:
        response = client.get("/api/diagnostics/sessions/qimai")

    assert response.status_code == 200
    payload = response.json()
    assert payload["collector_id"] == "qimai"
    assert "session_account" in payload
    assert "session_state" in payload
    assert "session_lease" in payload


def test_single_collector_session_diagnostics_api_returns_payload_when_inventory_sync_fails(
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

    class FailingSyncRegistry(InMemorySessionRegistry):
        async def sync_from_diagnostics(self, diagnostics: dict):
            raise RuntimeError("sync failed token=health-single-secret")

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)
    monkeypatch.setattr(app_module, "get_session_registry", lambda: FailingSyncRegistry())

    with TestClient(app) as client:
        response = client.get("/api/diagnostics/sessions/qimai")

    assert response.status_code == 200
    payload = response.json()
    assert payload["collector_id"] == "qimai"
    assert payload["session_mode"] == "local_profile"


def test_single_collector_session_diagnostics_api_returns_payload_when_registry_lookup_fails(
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
        raise RuntimeError("registry lookup failed token=health-single-lookup-secret")

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)
    monkeypatch.setattr(app_module, "get_session_registry", broken_registry_provider)

    with TestClient(app) as client:
        response = client.get("/api/diagnostics/sessions/qimai")

    assert response.status_code == 200
    payload = response.json()
    assert payload["collector_id"] == "qimai"
    assert payload["session_mode"] == "local_profile"


def test_session_inventory_api_persists_and_lists_diagnostics(monkeypatch, tmp_path) -> None:
    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
        "steam.steamdb.enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(app) as client:
        sync_response = client.get("/api/diagnostics/sessions?collectors=qimai")
        inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")

    assert sync_response.status_code == 200
    assert inventory_response.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["summary"]["items"] == 1
    assert inventory["summary"]["ready"] == 1
    assert inventory["summary"]["session_modes"]["local_profile"] == 1
    item = inventory["items"][0]
    assert item["collector_id"] == "qimai"
    assert item["session_mode"] == "local_profile"
    assert item["worker_binding"] == "sticky"
    assert item["health"] == "ready"
    assert item["account_kind"] == "local_profile"


def test_session_inventory_api_tracks_managed_state(monkeypatch, tmp_path) -> None:
    storage_state = tmp_path / "qimai_storage_state.json"
    storage_state.write_text("{}", encoding="utf-8")

    values = {
        "qimai.session_mode": "managed_state",
        "qimai.storage_state_path": str(storage_state),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(app) as client:
        sync_response = client.get("/api/diagnostics/sessions/qimai")
        inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")

    assert sync_response.status_code == 200
    assert inventory_response.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["summary"]["items"] == 1
    assert inventory["summary"]["ready"] == 1
    assert inventory["summary"]["session_modes"]["managed_state"] == 1
    item = inventory["items"][0]
    assert item["session_mode"] == "managed_state"
    assert item["worker_binding"] == "lease"
    assert item["account_kind"] == "managed_state"
    assert item["health"] == "ready"
    assert item["session_lease"]["strategy"] == "exclusive_lease"


def test_session_inventory_api_syncs_from_live_diagnostics(monkeypatch, tmp_path) -> None:
    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
        "steam.steamdb.enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(app) as client:
        inventory_response = client.get(
            "/api/diagnostics/sessions-inventory?collectors=qimai&sync=true"
        )

    assert inventory_response.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["summary"]["items"] == 1
    item = inventory["items"][0]
    assert item["collector_id"] == "qimai"
    assert item["session_mode"] == "local_profile"


def test_session_inventory_sync_preserves_existing_lease_state(monkeypatch, tmp_path) -> None:
    import src.web.app as app_module

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
        "steam.steamdb.enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    registry = InMemorySessionRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    diagnostics = build_collector_session_diagnostics("qimai")

    async def seed_registry() -> None:
        await registry.bind_session(
            diagnostics,
            worker_id="sync-worker",
            task_id="sync-task",
        )

    import asyncio

    asyncio.run(seed_registry())

    with TestClient(app) as client:
        inventory_response = client.get(
            "/api/diagnostics/sessions-inventory?collectors=qimai&sync=true"
        )

    assert inventory_response.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["summary"]["claimed"] == 1
    assert inventory["summary"]["lease_statuses"]["claimed"] == 1
    assert inventory["items"][0]["lease_status"] == "claimed"
    assert inventory["items"][0]["lease_worker_id"] == "sync-worker"
    assert inventory["items"][0]["lease_task_id"] == "sync-task"


def test_session_inventory_api_returns_persisted_entries_when_live_sync_fails(
    monkeypatch, tmp_path
) -> None:
    import asyncio
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
        def __init__(self) -> None:
            super().__init__()
            self.fail_sync = False

        async def sync_from_diagnostics(self, diagnostics: dict):
            if self.fail_sync:
                raise RuntimeError("sync failed token=health-inventory-secret")
            return await super().sync_from_diagnostics(diagnostics)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.collector_metadata.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    registry = FailingSyncRegistry()
    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)

    diagnostics = build_collector_session_diagnostics("qimai")
    asyncio.run(registry.sync_from_diagnostics(diagnostics))
    registry.fail_sync = True

    with TestClient(app) as client:
        inventory_response = client.get(
            "/api/diagnostics/sessions-inventory?collectors=qimai&sync=true"
        )

    assert inventory_response.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["summary"]["items"] == 1
    assert inventory["items"][0]["collector_id"] == "qimai"
    assert inventory["items"][0]["session_mode"] == "local_profile"


def test_get_task_service_rebuilds_after_scheduler_replaced(monkeypatch) -> None:
    import src.web.app as app_module

    class SchedulerA:
        pass

    class SchedulerB:
        pass

    original_scheduler = app_module.scheduler
    original_task_service = app_module._task_service
    try:
        app_module.scheduler = SchedulerA()
        app_module._task_service = None
        first = app_module.get_task_service()

        app_module.scheduler = SchedulerB()
        app_module._task_service = None
        second = app_module.get_task_service()
    finally:
        app_module.scheduler = original_scheduler
        app_module._task_service = original_task_service

    assert first is not second
    assert first._scheduler.__class__ is SchedulerA
    assert second._scheduler.__class__ is SchedulerB
