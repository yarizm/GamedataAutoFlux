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
            {"type": "storage", "name": "local"},
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
