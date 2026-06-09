from fastapi.testclient import TestClient

from src.core.config import get as get_config
from src.core.config import load_settings
from src.core.config_schema import validate_settings_payload
from src.core.diagnostics import (
    build_collector_session_diagnostics,
    build_config_diagnostics,
    build_health_report,
)
from src.web.app import app


def test_build_health_report_shape() -> None:
    report = build_health_report({"started": True})

    assert report["status"] in {"ok", "warning", "error"}
    assert isinstance(report["checks"], list)
    assert report["summary"]["scheduler"]["started"] is True
    assert all({"name", "status", "message", "details"} <= set(check) for check in report["checks"])


def test_build_config_diagnostics_shape() -> None:
    diagnostics = build_config_diagnostics()

    assert diagnostics["status"] in {"ok", "warning", "error"}
    assert isinstance(diagnostics["checks"], list)
    assert diagnostics["paths"]["root_dir"]
    assert any(check["name"] == "llm.provider" for check in diagnostics["checks"])
    assert any(check["name"] == "settings_schema" for check in diagnostics["checks"])


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
    assert diagnostics["status"] == "ok"
    assert any(check["name"] == "session:qimai_profile" for check in diagnostics["checks"])


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
