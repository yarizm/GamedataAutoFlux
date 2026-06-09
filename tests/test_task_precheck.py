from fastapi.testclient import TestClient

from src.web.app import app


def test_task_precheck_rejects_missing_targets() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "TapTap task",
                "pipeline_name": "taptap_basic",
                "targets": [],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "error"
    assert payload["can_submit"] is False
    assert any(issue["code"] == "missing_targets" for issue in payload["issues"])


def test_task_precheck_infers_collector_from_template() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "TapTap task",
                "pipeline_name": "taptap_basic",
                "targets": [
                    {
                        "name": "Example Game",
                        "target_type": "game",
                        "params": {"app_id": "12345"},
                    }
                ],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["collector_name"] == "taptap"
    assert payload["can_submit"] is True
    assert payload["data_source_status"]["taptap"] == "available"


def test_task_precheck_warns_for_steam_without_app_id() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Steam task",
                "pipeline_name": "steam_basic",
                "targets": [
                    {
                        "name": "Example Game",
                        "target_type": "game",
                        "params": {},
                    }
                ],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["collector_name"] == "steam"
    assert payload["can_submit"] is True
    assert any(issue["code"] == "missing_steam_app_id" for issue in payload["issues"])


def test_task_precheck_accepts_monitor_siteurl_without_app_id() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Monitor task",
                "pipeline_name": "monitor_basic",
                "targets": [
                    {
                        "name": "Counter-Strike 2",
                        "target_type": "game",
                        "params": {"siteurl": "counter-strike_2"},
                    }
                ],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["collector_name"] == "monitor"
    assert payload["can_submit"] is True
    assert payload["status"] == "ok"
    assert payload["required_fields"] == [
        "target.params.app_id or target.params.siteurl",
        "target.params.twitch_name (optional)",
    ]


def test_task_precheck_rejects_unknown_pipeline() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/precheck",
            json={
                "name": "Unknown task",
                "pipeline_name": "missing_pipeline",
                "targets": [{"name": "Example Game", "target_type": "game", "params": {}}],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "error"
    assert any(issue["code"] == "unknown_pipeline" for issue in payload["issues"])
