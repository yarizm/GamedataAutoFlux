from fastapi.testclient import TestClient

from src.web.app import app


def test_destructive_data_record_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/data/records/missing-record")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_destructive_data_category_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/data/games/missing-game")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_destructive_pipeline_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/pipelines/missing-pipeline")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_destructive_cron_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/cron-jobs/missing-job")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_destructive_task_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/tasks/missing-task")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]


def test_destructive_report_delete_requires_confirmation() -> None:
    with TestClient(app) as client:
        response = client.delete("/api/reports/missing-report")

    assert response.status_code == 400
    assert "confirm=true" in response.json()["detail"]
