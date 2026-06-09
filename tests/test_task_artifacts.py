import asyncio
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from src.core.events import TaskCompletedEvent
from src.core.hooks import ReportGenerationHook
from src.core.pipeline import PipelineResult
from src.core.scheduler import Scheduler
from src.core.task import Task
from src.reporting.generator import GeneratedReport
from src.services.task_artifact_service import InMemoryTaskArtifactService
from src.services.task_event_service import InMemoryTaskEventService


@pytest.mark.asyncio
async def test_in_memory_task_artifact_service_orders_and_redacts() -> None:
    service = InMemoryTaskArtifactService()

    first = await service.append(
        "task-1",
        "report_excel",
        name="report api_key=name-secret.xlsx",
        path="C:/tmp/report.xlsx",
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        size=123,
        download_url="/api/reports/report-1/download",
        metadata={"token": "metadata-secret"},
    )
    second = await service.append("task-1", "html_snapshot", name="snapshot.html")
    await service.append("task-2", "report_excel", name="other.xlsx")

    artifacts = await service.list_artifacts("task-1")
    rendered = str([artifact.to_public_payload() for artifact in artifacts])

    assert [artifact.seq for artifact in artifacts] == [1, 2]
    assert first.seq == 1
    assert second.seq == 2
    assert artifacts[0].path == "C:/tmp/report.xlsx"
    assert artifacts[0].to_public_payload()["path"] == ""
    assert artifacts[0].name == "report api_key=[REDACTED]"
    assert artifacts[0].metadata["token"] == "[REDACTED]"
    assert artifacts[0].download_url == "/api/reports/report-1/download"
    assert "name-secret" not in rendered
    assert "metadata-secret" not in rendered


@pytest.mark.asyncio
async def test_task_artifact_service_filters_unsafe_download_urls() -> None:
    service = InMemoryTaskArtifactService()

    unsafe = await service.append(
        "task-1",
        "html",
        name="unsafe.html",
        download_url="javascript:alert(1)",
    )
    external = await service.append(
        "task-1",
        "html",
        name="external.html",
        download_url="https://example.com/artifact.html",
    )

    assert unsafe.download_url == ""
    assert external.download_url == "https://example.com/artifact.html"


@pytest.mark.asyncio
async def test_scheduler_registers_report_artifact_and_emits_task_event(tmp_path) -> None:
    artifact_service = InMemoryTaskArtifactService()
    event_service = InMemoryTaskEventService()
    event_bus = _FakeEventBus()
    scheduler = Scheduler(
        max_concurrent=1,
        task_event_service=event_service,
        task_artifact_service=artifact_service,
        event_bus=event_bus,
    )
    task = Task(id="artifact-task", name="Artifact Task", pipeline_name="p")
    excel_path = tmp_path / "report.xlsx"
    excel_path.write_bytes(b"xlsx")
    report = _report(report_id="report-1", title="Artifact Report", excel_path=str(excel_path))

    await scheduler.register_report_artifact(task, report)
    if scheduler._background_tasks:
        await asyncio.gather(*scheduler._background_tasks)

    artifacts = await artifact_service.list_artifacts(task.id)
    events = await event_service.list_events(task.id)

    assert artifacts[0].type == "report_excel"
    assert artifacts[0].size == 4
    assert artifacts[0].download_url == "/api/reports/report-1/download"
    assert artifacts[0].metadata["report_id"] == "report-1"
    assert events[-1].type == "artifact"
    assert event_bus.task_events[-1].event["type"] == "artifact"


@pytest.mark.asyncio
async def test_report_generation_hook_registers_generated_report_artifact(tmp_path) -> None:
    report = _report(
        report_id="hook-report",
        title="Hook Report",
        excel_path=str(tmp_path / "hook-report.xlsx"),
    )
    report_generator = _FakeReportGenerator(report)
    scheduler = _FakeArtifactScheduler()
    hook = ReportGenerationHook(report_generator, scheduler=scheduler)
    task = Task(
        id="hook-task",
        name="Hook Task",
        pipeline_name="p",
        config={"report": {"enabled": True}},
    )
    result = PipelineResult(pipeline_name="p", task_id=task.id)

    await hook.handle(
        TaskCompletedEvent(
            task_id=task.id,
            success=True,
            result=result,
            task=task,
        )
    )

    assert scheduler.registered == [(task, report)]
    assert result.generated_report_id == "hook-report"


def test_task_artifacts_api_returns_artifacts(monkeypatch) -> None:
    from src.web import app as app_module
    from src.web.app import create_app

    service = InMemoryTaskArtifactService()
    artifact = asyncio.run(
        service.append(
            "task-api-artifacts",
            "report_excel",
            name="Report",
            path="C:/secret/report.xlsx",
            download_url="/api/reports/report-1/download",
        )
    )

    class FakeTaskService:
        async def get_task_artifacts(self, task_id: str, *, limit: int = 200, offset: int = 0):
            if task_id != "task-api-artifacts":
                return None
            return [artifact]

    monkeypatch.setattr(app_module, "get_task_service", lambda: FakeTaskService())

    client = TestClient(create_app())
    response = client.get("/api/tasks/task-api-artifacts/artifacts")

    assert response.status_code == 200
    item = response.json()["artifacts"][0]
    assert item["type"] == "report_excel"
    assert item["path"] == ""
    assert item["download_url"] == "/api/reports/report-1/download"


def _report(report_id: str, title: str, excel_path: str) -> GeneratedReport:
    return GeneratedReport(
        id=report_id,
        title=title,
        prompt="prompt",
        data_source="steam",
        template="default",
        generated_at=datetime.now(),
        matched_records=3,
        content="content",
        excel_path=excel_path,
    )


class _FakeReportGenerator:
    def __init__(self, report: GeneratedReport) -> None:
        self.report = report

    async def generate_excel(self, **kwargs):
        return self.report


class _FakeArtifactScheduler:
    def __init__(self) -> None:
        self.registered = []

    async def register_report_artifact(self, task: Task, report: GeneratedReport) -> None:
        self.registered.append((task, report))


class _FakeEventBus:
    def __init__(self) -> None:
        self.task_events = []

    async def emit(self, event_name: str, event) -> None:
        if event_name == "task_event":
            self.task_events.append(event)
