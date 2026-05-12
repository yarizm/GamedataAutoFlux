"""Cron job CRUD lifecycle tests via FastAPI TestClient."""

from fastapi.testclient import TestClient

from src.web.app import app


def test_list_cron_jobs():
    with TestClient(app) as client:
        resp = client.get("/api/cron-jobs")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


def test_create_and_delete_cron_job():
    job_name = "__test_cron_smoke__"
    payload = {
        "name": job_name,
        "pipeline_name": "steam_basic",
        "cron_expr": "0 3 * * *",
        "task_template": {
            "name": "CS2 Auto",
            "targets": [{"name": "CS2", "target_type": "game", "params": {"app_id": "730"}}],
        },
    }
    with TestClient(app) as client:
        # Create
        resp = client.post("/api/cron-jobs", json=payload)
        assert resp.status_code == 200
        result = resp.json()
        assert result.get("message", "").find(job_name) >= 0

        # Verify listed
        resp2 = client.get("/api/cron-jobs")
        listed = [j for j in resp2.json() if j.get("name") == job_name]
        assert len(listed) == 1

        # Delete (requires confirm)
        resp3 = client.delete(f"/api/cron-jobs/{job_name}")
        assert resp3.status_code == 400

        resp4 = client.delete(f"/api/cron-jobs/{job_name}?confirm=true")
        assert resp4.status_code == 200

        # Verify removed
        resp5 = client.get("/api/cron-jobs")
        listed2 = [j for j in resp5.json() if j.get("name") == job_name]
        assert len(listed2) == 0


def test_delete_missing_cron_job_fails():
    with TestClient(app) as client:
        resp = client.delete("/api/cron-jobs/__nonexistent_cron__?confirm=true")
        assert resp.status_code == 404


def test_create_cron_job_missing_fields():
    with TestClient(app) as client:
        resp = client.post("/api/cron-jobs", json={"name": "bad_cron"})
        assert resp.status_code == 422
