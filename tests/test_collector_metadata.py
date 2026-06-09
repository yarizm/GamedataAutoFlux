from fastapi.testclient import TestClient
import pytest

from src.core.collector_metadata import get_collector_metadata, list_collector_metadata
from src.core.pipeline import Pipeline
from src.core.pipeline_templates import PIPELINE_TEMPLATES
from src.core.scheduler import Scheduler
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
    scheduler._pipelines["unsafe_dynamic_config"] = Pipeline(
        "unsafe_dynamic_config"
    ).add_collector(
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
