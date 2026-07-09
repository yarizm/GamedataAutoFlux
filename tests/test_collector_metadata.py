from fastapi.testclient import TestClient
import pytest

from src.core.collector_metadata import (
    collector_metadata_payload,
    get_collector_metadata,
    list_collector_metadata,
    required_worker_capabilities,
    resolve_session_mode,
    worker_binding_mode,
)
from src.core.pipeline import Pipeline
from src.core.pipeline_templates import PIPELINE_TEMPLATES
from src.core.scheduler import Scheduler
from src.services.session_registry import InMemorySessionRegistry
from src.services.task_service import TaskService
from src.web.app import app


def test_collector_metadata_covers_pipeline_template_collectors() -> None:
    collector_ids = {
        step["name"]
        for template in PIPELINE_TEMPLATES
        for step in template.get("steps", [])
        if step.get("type") == "collector"
    }

    metadata = list_collector_metadata(sorted(collector_ids))

    assert set(metadata) == collector_ids
    assert metadata["qimai"]["requires_session"] is True
    assert metadata["qimai"]["session_mode"] == "local_profile"
    assert metadata["gtrends"]["supports_checkpoint"] is True
    assert metadata["monitor"]["recovery_level"] == "L1"
    assert metadata["taptap"]["recovery_level"] == "L0"
    assert metadata["steam"]["target_schema"]["required_fields"] == [
        "target.name",
        "target.params.app_id (recommended)",
    ]


def test_youtube_collectors_define_targets_and_api_key_credentials() -> None:
    profiles = get_collector_metadata("youtube_profiles")
    comments = get_collector_metadata("youtube_comments")

    assert profiles is not None
    assert comments is not None
    assert profiles.credential_profiles == ["youtube_api_key"]
    assert comments.credential_profiles == ["youtube_api_key"]
    assert profiles.target_schema.required_fields == [
        "target.params.channel_url or target.params.channel_id or target.params.handle"
    ]
    assert comments.target_schema.required_fields == ["target.params.video_url"]


def test_task_precheck_uses_collector_metadata_for_required_fields() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Qimai task",
                "pipeline_name": "qimai_basic",
                "targets": [{"name": "Example App", "target_type": "app", "params": {}}],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()

    assert payload["status"] == "error"
    assert payload["collector_name"] == "qimai"
    assert payload["required_fields"] == ["target.params.app_id"]
    assert payload["collector_metadata"]["session_mode"] == "local_profile"
    assert payload["session_diagnostics"]["collector_id"] == "qimai"
    assert payload["session_diagnostics"]["session_mode"] == "local_profile"
    assert payload["session_readiness"]["required"] is True
    assert payload["session_readiness"]["mode"] == "local_profile"
    assert payload["session_readiness"]["binding"] == "sticky"
    assert payload["recovery"]["recovery_level"] == "L0"
    assert payload["recovery"]["recommended_action"] == "rerun_task"
    assert any(issue["code"] == "missing_qimai_app_id" for issue in payload["issues"])


def test_task_precheck_preserves_steam_app_id_warning() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Steam task",
                "pipeline_name": "steam_basic",
                "targets": [{"name": "Example Game", "target_type": "game", "params": {}}],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()

    assert payload["can_submit"] is True
    assert payload["collector_metadata"]["collector_id"] == "steam"
    assert any(issue["code"] == "missing_steam_app_id" for issue in payload["issues"])


def test_task_precheck_blocks_youtube_when_api_keys_are_missing(monkeypatch) -> None:
    def fake_get_config(key: str, default=None):
        if key == "youtube.api_keys":
            return ["${YOUTUBE_API_KEY_1}", ""]
        return default

    monkeypatch.setattr("src.services.task_precheck_service.get_config", fake_get_config)
    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)

    scheduler = Scheduler()
    scheduler._pipelines["youtube_profiles_test"] = Pipeline(
        "youtube_profiles_test"
    ).add_collector("youtube_profiles")
    service = TaskService(scheduler)

    precheck = service.precheck(
        name="YouTube profiles",
        pipeline_name="youtube_profiles_test",
        targets=[
            {
                "name": "Example Channel",
                "target_type": "youtube_channel",
                "params": {"channel_url": "https://www.youtube.com/@example"},
            }
        ],
    )

    assert precheck.status == "error"
    assert precheck.can_submit is False
    assert precheck.collector_name == "youtube_profiles"
    assert precheck.credential_status["youtube.api_keys"] == "missing"
    assert any(issue.code == "missing_youtube_api_key" for issue in precheck.issues)


def test_task_precheck_validates_youtube_comment_targets(monkeypatch) -> None:
    def fake_get_config(key: str, default=None):
        if key == "youtube.api_keys":
            return ["configured-key"]
        return default

    monkeypatch.setattr("src.services.task_precheck_service.get_config", fake_get_config)
    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)

    scheduler = Scheduler()
    scheduler._pipelines["youtube_comments_test"] = Pipeline(
        "youtube_comments_test"
    ).add_collector("youtube_comments")
    service = TaskService(scheduler)

    precheck = service.precheck(
        name="YouTube comments",
        pipeline_name="youtube_comments_test",
        targets=[
            {
                "name": "Missing Video",
                "target_type": "youtube_video",
                "params": {},
            }
        ],
    )

    assert precheck.status == "error"
    assert precheck.required_fields == ["target.params.video_url"]
    assert precheck.credential_status["youtube.api_keys"] == "configured"
    assert any(issue.code == "missing_youtube_video_url" for issue in precheck.issues)


def test_task_precheck_reports_l1_checkpoint_recovery_for_gtrends() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Trends task",
                "pipeline_name": "gtrends_basic",
                "targets": [{"name": "Example Game", "target_type": "game", "params": {}}],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()

    assert payload["can_submit"] is True
    assert payload["collector_metadata"]["supports_checkpoint"] is True
    assert payload["recovery"]["recovery_level"] == "L1"
    assert payload["recovery"]["recommended_action"] == "record_checkpoint"


def test_task_precheck_reports_l1_checkpoint_recovery_for_steam_discussions() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Steam Discussions task",
                "pipeline_name": "steam_discussions_basic",
                "targets": [
                    {
                        "name": "Example Game",
                        "target_type": "game",
                        "params": {"app_id": "730"},
                    }
                ],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()

    assert payload["can_submit"] is True
    assert payload["collector_metadata"]["supports_checkpoint"] is True
    assert payload["recovery"]["recovery_level"] == "L1"
    assert payload["recovery"]["recommended_action"] == "record_checkpoint"


def test_task_precheck_blocks_qimai_when_required_profile_is_missing(monkeypatch, tmp_path) -> None:
    missing_profile = tmp_path / "missing_qimai_profile"

    values = {
        "qimai.user_data_dir": str(missing_profile),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Qimai task",
                "pipeline_name": "qimai_basic",
                "targets": [
                    {
                        "name": "Example App",
                        "target_type": "app",
                        "params": {"app_id": "123", "qimai_app_id": "123"},
                    }
                ],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["can_submit"] is False
    assert payload["session_readiness"]["precheck_status"] == "error"
    assert payload["session_readiness"]["recommended_action"] == "prepare_local_profile"
    assert any(issue["code"] == "session_blocked" for issue in payload["issues"])


def test_task_precheck_reports_managed_state_readiness(monkeypatch, tmp_path) -> None:
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
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Qimai managed task",
                "pipeline_name": "qimai_basic",
                "targets": [
                    {
                        "name": "Example App",
                        "target_type": "app",
                        "params": {"app_id": "123", "qimai_app_id": "123"},
                    }
                ],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["can_submit"] is True
    assert payload["session_readiness"]["mode"] == "managed_state"
    assert payload["session_readiness"]["status"] == "ready"
    assert payload["session_readiness"]["precheck_status"] == "ok"
    assert payload["session_readiness"]["recommended_action"] == "none"


def test_task_precheck_syncs_session_inventory(monkeypatch, tmp_path) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()
    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)
    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Qimai task",
                "pipeline_name": "qimai_basic",
                "targets": [
                    {
                        "name": "Example App",
                        "target_type": "app",
                        "params": {"app_id": "123", "qimai_app_id": "123"},
                    }
                ],
                "config": {},
            },
        )
        inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")

    assert response.status_code == 200
    assert inventory_response.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["items"][0]["collector_id"] == "qimai"
    assert inventory["items"][0]["session_mode"] == "local_profile"


def test_task_precheck_succeeds_when_session_inventory_sync_fails(monkeypatch, tmp_path) -> None:
    import src.web.app as app_module

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    class BrokenRegistry:
        async def sync_from_diagnostics(self, diagnostics):
            raise RuntimeError("precheck sync failed token=broken-secret")

    monkeypatch.setattr(app_module, "get_session_registry", lambda: BrokenRegistry())
    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Qimai task",
                "pipeline_name": "qimai_basic",
                "targets": [
                    {
                        "name": "Example App",
                        "target_type": "app",
                        "params": {"app_id": "123", "qimai_app_id": "123"},
                    }
                ],
                "config": {},
            },
        )

    assert response.status_code == 200
    assert response.json()["can_submit"] is True


def test_task_precheck_succeeds_when_session_registry_lookup_fails(monkeypatch, tmp_path) -> None:
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
        raise RuntimeError("registry lookup failed token=precheck-lookup-secret")

    monkeypatch.setattr(app_module, "get_session_registry", broken_registry_provider)
    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Qimai task",
                "pipeline_name": "qimai_basic",
                "targets": [
                    {
                        "name": "Example App",
                        "target_type": "app",
                        "params": {"app_id": "123", "qimai_app_id": "123"},
                    }
                ],
                "config": {},
            },
        )

    assert response.status_code == 200
    assert response.json()["can_submit"] is True


def test_task_create_syncs_session_inventory(monkeypatch, tmp_path) -> None:
    import src.web.app as app_module

    registry = InMemorySessionRegistry()
    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    monkeypatch.setattr(app_module, "get_session_registry", lambda: registry)
    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(app) as client:
        pipeline_response = client.post(
            "/api/pipelines",
            json={
                "name": "api_qimai_pipeline",
                "steps": [{"type": "collector", "name": "qimai", "config": {}}],
            },
        )
        response = client.post(
            "/api/tasks",
            json={
                "name": "Qimai create task",
                "pipeline_name": "api_qimai_pipeline",
                "collector_name": "qimai",
                "targets": [
                    {
                        "name": "Example App",
                        "target_type": "app",
                        "params": {"app_id": "123", "qimai_app_id": "123"},
                    }
                ],
                "config": {},
            },
        )
        inventory_response = client.get("/api/diagnostics/sessions-inventory?collectors=qimai")

    assert pipeline_response.status_code == 200
    assert response.status_code == 200
    assert inventory_response.status_code == 200
    inventory = inventory_response.json()
    assert inventory["count"] == 1
    assert inventory["items"][0]["collector_id"] == "qimai"
    assert inventory["items"][0]["session_mode"] == "local_profile"


def test_task_create_succeeds_when_session_inventory_sync_fails(monkeypatch, tmp_path) -> None:
    import src.web.app as app_module

    profile_dir = tmp_path / "qimai_profile"
    profile_dir.mkdir()

    values = {
        "qimai.user_data_dir": str(profile_dir),
        "qimai.cdp_enabled": False,
    }

    def fake_get_config(key: str, default=None):
        return values.get(key, default)

    class BrokenRegistry:
        async def sync_from_diagnostics(self, diagnostics):
            raise RuntimeError("session registry sync failed token=broken-secret")

    monkeypatch.setattr(app_module, "get_session_registry", lambda: BrokenRegistry())
    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(app) as client:
        pipeline_response = client.post(
            "/api/pipelines",
            json={
                "name": "api_qimai_pipeline_sync_failure",
                "steps": [{"type": "collector", "name": "qimai", "config": {}}],
            },
        )
        response = client.post(
            "/api/tasks",
            json={
                "name": "Qimai create task with sync failure",
                "pipeline_name": "api_qimai_pipeline_sync_failure",
                "collector_name": "qimai",
                "targets": [
                    {
                        "name": "Example App",
                        "target_type": "app",
                        "params": {"app_id": "123", "qimai_app_id": "123"},
                    }
                ],
                "config": {},
            },
        )

    assert pipeline_response.status_code == 200
    assert response.status_code == 200
    assert response.json()["collector_name"] == "qimai"


def test_task_create_succeeds_when_session_registry_lookup_fails(monkeypatch, tmp_path) -> None:
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
        raise RuntimeError("registry lookup failed token=create-lookup-secret")

    monkeypatch.setattr(app_module, "get_session_registry", broken_registry_provider)
    monkeypatch.setattr("src.core.diagnostics.get_config", fake_get_config)
    monkeypatch.setattr("src.core.session_runtime.get_config", fake_get_config)

    with TestClient(app) as client:
        pipeline_response = client.post(
            "/api/pipelines",
            json={
                "name": "api_qimai_pipeline_lookup_failure",
                "steps": [{"type": "collector", "name": "qimai", "config": {}}],
            },
        )
        response = client.post(
            "/api/tasks",
            json={
                "name": "Qimai create task with registry lookup failure",
                "pipeline_name": "api_qimai_pipeline_lookup_failure",
                "collector_name": "qimai",
                "targets": [
                    {
                        "name": "Example App",
                        "target_type": "app",
                        "params": {"app_id": "123", "qimai_app_id": "123"},
                    }
                ],
                "config": {},
            },
        )

    assert pipeline_response.status_code == 200
    assert response.status_code == 200
    assert response.json()["pipeline_name"] == "api_qimai_pipeline_lookup_failure"


def test_task_precheck_validates_collector_config_schema() -> None:
    scheduler = Scheduler()
    scheduler._pipelines["invalid_steam_config"] = Pipeline("invalid_steam_config").add_collector(
        "steam",
        {"request_delay": -1},
    )
    service = TaskService(scheduler)

    precheck = service.precheck(
        name="Invalid Steam Config",
        pipeline_name="invalid_steam_config",
        targets=[{"name": "Example Game", "target_type": "game", "params": {"app_id": "730"}}],
    )

    assert precheck.can_submit is False
    assert any(
        issue.code == "invalid_collector_config_minimum"
        and issue.field == "pipeline.steps.collector[steam].config.request_delay"
        for issue in precheck.issues
    )


def test_task_precheck_requires_dynamic_playwright_url_config() -> None:
    scheduler = Scheduler()
    service = TaskService(scheduler)

    precheck = service.precheck(
        name="Dynamic Task",
        pipeline_name="dynamic_playwright_basic",
        targets=[{"name": "Example Page", "target_type": "web", "params": {}}],
    )

    assert precheck.can_submit is False
    assert any(
        issue.code == "missing_collector_config"
        and issue.field == "pipeline.steps.collector[dynamic_playwright].config.url"
        for issue in precheck.issues
    )


def test_task_precheck_rejects_dynamic_playwright_private_url_config() -> None:
    scheduler = Scheduler()
    scheduler._pipelines["unsafe_dynamic_config"] = Pipeline("unsafe_dynamic_config").add_collector(
        "dynamic_playwright",
        {"url": "http://127.0.0.1:8000/private", "fields": {}},
    )
    service = TaskService(scheduler)

    precheck = service.precheck(
        name="Dynamic Task",
        pipeline_name="unsafe_dynamic_config",
        targets=[{"name": "Example Page", "target_type": "web", "params": {}}],
    )

    assert precheck.can_submit is False
    assert any(
        issue.code == "unsafe_dynamic_playwright_config"
        and issue.field == "pipeline.steps.collector[dynamic_playwright].config.url"
        and "not allowed" in issue.message
        for issue in precheck.issues
    )


@pytest.mark.asyncio
async def test_task_create_blocks_dynamic_playwright_without_url_config() -> None:
    scheduler = Scheduler()
    service = TaskService(scheduler)

    with pytest.raises(ValueError, match="missing_collector_config"):
        await service.create(
            name="Dynamic Task",
            pipeline_name="dynamic_playwright_basic",
            targets=[{"name": "Example Page", "target_type": "web", "params": {}}],
        )


def test_components_metadata_endpoint_keeps_legacy_components_shape() -> None:
    with TestClient(app) as client:
        legacy = client.get("/api/components")
        metadata = client.get("/api/components/metadata")

    assert legacy.status_code == 200
    assert metadata.status_code == 200
    legacy_payload = legacy.json()
    metadata_payload = metadata.json()

    assert isinstance(legacy_payload["collector"], list)
    assert metadata_payload["components"] == legacy_payload
    assert metadata_payload["collectors"]["qimai"]["requires_session"] is True


def test_unknown_collector_metadata_returns_none() -> None:
    assert get_collector_metadata("missing") is None


def test_local_profile_collectors_expose_required_worker_capabilities() -> None:
    assert required_worker_capabilities("qimai") == {
        "session_mode:local_profile",
        "session:qimai_profile",
    }
    assert required_worker_capabilities("steam") == set()


def test_qimai_session_mode_can_be_overridden_to_managed_state(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.core.collector_metadata.get_config",
        lambda key, default=None: "managed_state" if key == "qimai.session_mode" else default,
    )

    assert resolve_session_mode("qimai") == "managed_state"
    assert required_worker_capabilities("qimai") == {"session_mode:managed_state"}
    assert worker_binding_mode("qimai") == "lease"

    payload = collector_metadata_payload("qimai")
    assert payload["default_session_mode"] == "local_profile"
    assert payload["session_mode"] == "managed_state"
    assert payload["configured_session_mode"] == "managed_state"
    assert payload["session_mode_source"] == "config"
    assert payload["session_mode_override_status"] == "applied"
