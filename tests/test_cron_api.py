"""API tests for enriched cron jobs."""

from fastapi.testclient import TestClient

from src.web.app import app


def test_cron_preview_preset() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/cron-jobs/preview",
            json={
                "schedule": {
                    "mode": "preset",
                    "preset": {"type": "daily", "time": "08:00"},
                },
                "timezone": "Asia/Shanghai",
                "count": 3,
            },
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["cron_expr"] == "0 8 * * *"
    assert payload["valid"] is True
    assert len(payload["next_runs"]) == 3
    assert "每天" in payload["human_label"]


def test_create_cron_with_preset_and_targets() -> None:
    name = "test_cron_daily_taptap_ui"
    with TestClient(app) as client:
        # cleanup if exists
        client.delete(f"/api/cron-jobs/{name}?confirm=true")
        response = client.post(
            "/api/cron-jobs",
            json={
                "name": name,
                "pipeline_name": "taptap_basic",
                "schedule": {
                    "mode": "preset",
                    "preset": {"type": "daily", "time": "09:00"},
                    "timezone": "Asia/Shanghai",
                },
                "task_template": {
                    "targets": [
                        {
                            "name": "Example",
                            "target_type": "game",
                            "params": {"app_id": "12345"},
                        }
                    ],
                    "config": {"refresh": {"rolling_window": True}},
                },
                "enabled": True,
                "description": "daily taptap",
            },
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["job_id"] == name
        assert body["job"]["cron_expr"] == "0 9 * * *"
        assert body["job"]["targets_count"] == 1
        assert body["job"]["rolling_window"] is True
        assert body["job"]["human_label"]

        listed = client.get("/api/cron-jobs").json()
        match = next(j for j in listed if j["name"] == name)
        assert match["pipeline_name"] == "taptap_basic"

        # pause
        toggled = client.patch(
            f"/api/cron-jobs/{name}/enabled",
            json={"enabled": False},
        )
        assert toggled.status_code == 200
        assert toggled.json()["job"]["enabled"] is False

        # run now
        run = client.post(f"/api/cron-jobs/{name}/run")
        assert run.status_code == 200
        assert run.json().get("task_id")

        # delete
        deleted = client.delete(f"/api/cron-jobs/{name}?confirm=true")
        assert deleted.status_code == 200


def test_create_cron_invalid_expr() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/cron-jobs",
            json={
                "name": "bad_cron",
                "pipeline_name": "taptap_basic",
                "cron_expr": "invalid",
            },
        )
    assert response.status_code == 400
