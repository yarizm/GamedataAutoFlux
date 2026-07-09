"""Pipeline CRUD lifecycle tests via FastAPI TestClient."""

from fastapi.testclient import TestClient

from src.web.app import app


def test_list_pipelines():
    with TestClient(app) as client:
        resp = client.get("/api/pipelines")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)


def test_list_pipeline_templates():
    with TestClient(app) as client:
        resp = client.get("/api/pipeline-templates")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        for template in data:
            assert "id" in template
            assert "name" in template


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
