"""Pipeline CRUD lifecycle tests via FastAPI TestClient."""

from fastapi.testclient import TestClient

from src.core.collector_validators import (
    CollectorConfigIssue,
    register_collector_config_validator,
    restore_collector_config_validators,
    snapshot_collector_config_validators,
)
from src.web.app import app
from src.core.pipeline import Pipeline
from src.web import app as web_app


def test_list_pipelines():
    with TestClient(app) as client:
        resp = client.get("/api/pipelines")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)


def test_list_available_pipelines_excludes_inactive_plugin_components():
    pipeline_name = "__inactive_plugin_pipeline__"
    with TestClient(app) as client:
        web_app.scheduler.pipeline_service.registry[pipeline_name] = Pipeline(pipeline_name).add_collector(
            "__collector_from_uninstalled_plugin__"
        )
        try:
            all_response = client.get("/api/pipelines")
            available_response = client.get("/api/pipelines?available_only=true")
        finally:
            web_app.scheduler.pipeline_service.registry.pop(pipeline_name, None)

    assert all_response.status_code == 200
    assert pipeline_name in all_response.json()
    assert available_response.status_code == 200
    assert pipeline_name not in available_response.json()


def test_task_precheck_rejects_pipeline_with_inactive_plugin_components():
    pipeline_name = "__inactive_plugin_precheck__"
    missing_collector = "__collector_from_uninstalled_plugin__"
    with TestClient(app) as client:
        web_app.scheduler.pipeline_service.registry[pipeline_name] = Pipeline(pipeline_name).add_collector(
            missing_collector
        )
        try:
            response = client.post(
                "/api/tasks/precheck",
                json={
                    "name": "inactive plugin pipeline",
                    "pipeline_name": pipeline_name,
                    "collector_name": missing_collector,
                    "targets": [{"name": "test", "target_type": "default", "params": {}}],
                },
            )
        finally:
            web_app.scheduler.pipeline_service.registry.pop(pipeline_name, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["can_submit"] is False
    assert any(
        issue["code"] == "pipeline_components_unavailable"
        for issue in payload["issues"]
    )


def test_list_pipeline_templates():
    with TestClient(app) as client:
        resp = client.get("/api/pipeline-templates")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        for template in data:
            assert "id" in template
            assert "name" in template


def test_list_collector_plugins():
    with TestClient(app) as client:
        response = client.get("/api/plugins")
        assert response.status_code == 200
        payload = response.json()
        assert payload["entry_point_group"] == "gamedata_autoflux.plugins"
        assert payload["active"] >= 1
        assert payload["failed"] == 0
        assert all(item["state"] == "active" for item in payload["plugins"])
        assert all(item["capabilities"] for item in payload["plugins"])


def test_component_metadata_exposes_plugin_owned_dag_nodes() -> None:
    with TestClient(app) as client:
        response = client.get("/api/components/metadata")

    assert response.status_code == 200
    payload = response.json()
    steam = next(
        item
        for item in payload["dag_nodes"]
        if item["type"] == "collector" and item["component"] == "steam"
    )
    assert steam["owner"] == "autoflux-plugin-steam"
    assert steam["display_name"] == "Steam"
    assert steam["ports_out"] == [
        {"name": "records", "required": True, "type_hint": "records"}
    ]
    assert {item["key"] for item in steam["output_fields"]} == {
        "game_name",
        "app_id",
    }


def test_create_dag_rejects_component_from_uninstalled_plugin() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/dags",
            json={
                "name": "__missing_plugin_component__",
                "nodes": [
                    {
                        "id": "missing",
                        "type": "collector",
                        "component": "not_installed_collector",
                    }
                ],
                "edges": [],
            },
        )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "dag_component_unavailable"


def test_create_and_delete_pipeline():
    pipeline_name = "__test_pipeline_smoke__"
    payload = {
        "name": pipeline_name,
        "steps": [
            {"type": "collector", "name": "steam", "config": {"request_delay": 0.5}},
            {"type": "processor", "name": "cleaner"},
            {"type": "storage", "name": "sqlalchemy"},
        ],
    }
    with TestClient(app) as client:
        # Create
        resp = client.post("/api/pipelines", json=payload)
        assert resp.status_code == 200
        result = resp.json()
        assert result.get("message", "").find(pipeline_name) >= 0

        # Verify listed
        resp2 = client.get("/api/pipelines")
        assert pipeline_name in resp2.json()

        # Delete (requires confirm)
        resp3 = client.delete(f"/api/pipelines/{pipeline_name}")
        assert resp3.status_code == 400  # missing confirm

        resp4 = client.delete(f"/api/pipelines/{pipeline_name}?confirm=true")
        assert resp4.status_code == 200

        # Verify removed
        resp5 = client.get("/api/pipelines")
        assert pipeline_name not in resp5.json()


def test_delete_missing_pipeline_fails():
    with TestClient(app) as client:
        resp = client.delete("/api/pipelines/__nonexistent_pipeline__?confirm=true")
        assert resp.status_code == 404


def test_create_pipeline_missing_steps():
    with TestClient(app) as client:
        resp = client.post("/api/pipelines", json={"name": "bad_pipeline"})
        assert resp.status_code == 422  # validation error


def test_pipeline_api_runs_validator_registered_by_any_collector_plugin() -> None:
    validator_snapshot = snapshot_collector_config_validators()

    def validate(config: dict):
        if config.get("tenant"):
            return []
        return [
            CollectorConfigIssue(
                code="missing_tenant",
                field="tenant",
                message="A tenant is required by this collector plugin.",
            )
        ]

    register_collector_config_validator("steam", validate, owner="test-third-party-validator")
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/pipelines",
                json={
                    "name": "__test_plugin_validator__",
                    "steps": [{"type": "collector", "name": "steam", "config": {}}],
                },
            )
    finally:
        restore_collector_config_validators(validator_snapshot)

    assert response.status_code == 400
    assert response.json()["detail"] == {
        "code": "missing_tenant",
        "field": "tenant",
        "message": "A tenant is required by this collector plugin.",
    }


def test_create_and_get_dag():
    dag_name = "__test_dag_api__"
    payload = {
        "name": dag_name,
        "nodes": [
            {
                "id": "src",
                "type": "collector",
                "component": "steam",
                "ports_out": [{"name": "records"}],
            },
            {
                "id": "store",
                "type": "storage",
                "component": "sqlalchemy",
                "ports_in": [{"name": "records"}],
            },
        ],
        "edges": [
            {"from": "src", "out": "records", "to": "store", "in": "records"},
        ],
    }
    with TestClient(app) as client:
        resp = client.post("/api/dags", json=payload)
        assert resp.status_code == 200
        body = resp.json()
        assert dag_name in body.get("message", "")
        assert body["config"]["name"] == dag_name
        assert body["config"]["kind"] == "dag"
        assert len(body["config"]["nodes"]) == 2
        assert len(body["config"]["edges"]) == 1
        # 双写：投影 Pipeline 供任务选择
        assert "pipeline" in body
        assert body["pipeline"]["name"] == dag_name
        assert any(s["type"] == "collector" for s in body["pipeline"]["steps"])

        got = client.get(f"/api/dags/{dag_name}")
        assert got.status_code == 200
        data = got.json()
        assert data["name"] == dag_name
        assert data["kind"] == "dag"
        assert any(n["id"] == "src" for n in data["nodes"])

        listed_dags = client.get("/api/dags")
        assert listed_dags.status_code == 200
        assert dag_name in listed_dags.json()

        listed = client.get("/api/pipelines")
        assert listed.status_code == 200
        assert dag_name in listed.json()


def test_get_missing_dag_returns_404():
    with TestClient(app) as client:
        resp = client.get("/api/dags/__nonexistent_dag__")
        assert resp.status_code == 404


def test_create_dag_with_condition_edge():
    dag_name = "__test_dag_condition__"
    payload = {
        "name": dag_name,
        "nodes": [
            {
                "id": "primary",
                "type": "collector",
                "component": "steam",
                "ports_out": [{"name": "records"}],
            },
            {
                "id": "fallback",
                "type": "collector",
                "component": "taptap",
                "ports_out": [{"name": "records"}],
            },
            {
                "id": "store",
                "type": "storage",
                "component": "sqlalchemy",
                "ports_in": [{"name": "records"}],
            },
        ],
        "edges": [
            {
                "from": "primary",
                "out": "records",
                "to": "store",
                "in": "records",
                "condition": "on_success",
            },
            {
                "from": "fallback",
                "out": "records",
                "to": "store",
                "in": "records",
                "condition": "on_failure",
            },
        ],
        "conditions": ["on_success", "on_failure"],
    }
    with TestClient(app) as client:
        resp = client.post("/api/dags", json=payload)
        assert resp.status_code == 200
        got = client.get(f"/api/dags/{dag_name}")
        assert got.status_code == 200
        edges = got.json()["edges"]
        assert any(e.get("condition") == "on_success" for e in edges)
        assert any(e.get("condition") == "on_failure" for e in edges)
