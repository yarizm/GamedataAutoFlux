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


def test_task_create_rejects_precheck_errors() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks",
            json={
                "name": "Dynamic task",
                "pipeline_name": "dynamic_playwright_basic",
                "targets": [{"name": "Example Page", "target_type": "web", "params": {}}],
                "config": {},
            },
        )

    assert response.status_code == 400
    assert "missing_collector_config" in response.json()["detail"]


def test_task_precheck_returns_collectors_list() -> None:
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
    assert payload["collectors"] == ["taptap"]
    assert payload["collectors_readiness"]
    assert payload["collectors_readiness"][0]["collector_id"] == "taptap"
    assert "category" in payload["issues"][0] if payload["issues"] else True


def test_task_precheck_warns_invalid_app_id_format() -> None:
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
                        "params": {"app_id": "not-a-number"},
                    }
                ],
                "config": {},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["can_submit"] is True
    assert any(issue["code"] == "invalid_app_id_format" for issue in payload["issues"])


def test_precheck_allows_empty_targets_when_all_from_upstream() -> None:
    from src.core.pipeline import Pipeline
    from src.services.task_precheck_service import TaskPrecheckService

    class FakeSched:
        def __init__(self, pipeline):
            self._pipeline = pipeline

        def get_pipeline(self, name):
            return self._pipeline

    pipeline = Pipeline(name="profiles_from_upstream")
    pipeline.add_collector("youtube_profiles", {"from_upstream": {"auto": True}})
    service = TaskPrecheckService(FakeSched(pipeline))
    result = service.precheck(
        name="chain",
        pipeline_name="profiles_from_upstream",
        targets=[],
    )
    assert result.can_submit is True or any(
        i.code == "missing_youtube_api_key" for i in result.issues
    )
    assert not any(i.code == "missing_targets" for i in result.issues)
    assert result.collectors == ["youtube_profiles"]
    assert result.collectors_readiness[0].from_upstream is True


def test_precheck_rejects_empty_from_upstream_map() -> None:
    from src.core.pipeline import Pipeline
    from src.services.task_precheck_service import TaskPrecheckService

    class FakeSched:
        def __init__(self, pipeline):
            self._pipeline = pipeline

        def get_pipeline(self, name):
            return self._pipeline

    pipeline = Pipeline(name="bad_map")
    pipeline.add_collector(
        "youtube_profiles",
        {"from_upstream": {"map": {}, "auto": False}},
    )
    service = TaskPrecheckService(FakeSched(pipeline))
    result = service.precheck(name="t", pipeline_name="bad_map", targets=[])
    assert result.can_submit is False
    assert any(i.code == "empty_from_upstream_map" for i in result.issues)


def test_precheck_multi_collector_checks_each_credential(monkeypatch) -> None:
    from src.core.pipeline import Pipeline
    from src.services.task_precheck_service import TaskPrecheckService

    class FakeSched:
        def __init__(self, pipeline):
            self._pipeline = pipeline

        def get_pipeline(self, name):
            return self._pipeline

    monkeypatch.setattr(
        "src.services.task_precheck_service.get_config",
        lambda key, default=None: [] if key == "youtube.api_keys" else default,
    )
    pipeline = Pipeline(name="multi")
    pipeline.add_collector("youtube_comments", {})
    pipeline.add_collector("youtube_profiles", {"from_upstream": {"auto": True}})
    service = TaskPrecheckService(FakeSched(pipeline))
    result = service.precheck(
        name="t",
        pipeline_name="multi",
        targets=[{"name": "v", "params": {"video_url": "https://www.youtube.com/watch?v=abc"}}],
    )
    assert result.collectors == ["youtube_comments", "youtube_profiles"]
    youtube_key_issues = [i for i in result.issues if i.code == "missing_youtube_api_key"]
    assert len(youtube_key_issues) >= 1
    assert {i.collector_id for i in youtube_key_issues} <= {
        "youtube_comments",
        "youtube_profiles",
    }
