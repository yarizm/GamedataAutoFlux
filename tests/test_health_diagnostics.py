from fastapi.testclient import TestClient

from src.core.config_schema import validate_settings_payload
from src.core.diagnostics import build_config_diagnostics, build_health_report
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


def test_settings_schema_accepts_minimal_payload() -> None:
    validation = validate_settings_payload(
        {
            "server": {"port": 8000},
            "scheduler": {"max_concurrent_tasks": 1},
        }
    )

    assert validation["valid"] is True
    assert validation["issues"] == []


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
