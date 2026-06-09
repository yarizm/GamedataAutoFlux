import asyncio

import pytest
from fastapi.testclient import TestClient

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.pipeline import Pipeline, PipelineResult
from src.core.registry import registry
from src.core.scheduler import Scheduler
from src.core.task import Task, TaskTarget
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.services.task_event_service import InMemoryTaskEventService
from src.services.task_repository import InMemoryTaskRepository


@pytest.mark.asyncio
async def test_in_memory_task_event_service_orders_and_redacts_events() -> None:
    service = InMemoryTaskEventService()

    first = await service.append(
        "task-1",
        "collect",
        message="start api_key=message-secret",
        payload={"token": "payload-secret"},
    )
    second = await service.append("task-1", "complete", payload={"ok": True})
    await service.append("task-2", "collect")

    events = await service.list_events("task-1")
    latest = await service.list_events("task-1", limit=1, order="desc")
    rendered = str([event.to_public_payload() for event in events])

    assert [event.seq for event in events] == [1, 2]
    assert [event.seq for event in latest] == [2]
    assert first.seq == 1
    assert second.seq == 2
    assert events[0].message == "start api_key=[REDACTED]"
    assert events[0].payload["token"] == "[REDACTED]"
    assert "message-secret" not in rendered
    assert "payload-secret" not in rendered


@pytest.mark.asyncio
async def test_scheduler_emits_structured_task_lifecycle_events() -> None:
    event_service = InMemoryTaskEventService()
    event_bus = _FakeEventBus()
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=0,
        task_repo=InMemoryTaskRepository(),
        event_bus=event_bus,
        task_event_service=event_service,
    )
    scheduler._semaphore = asyncio.Semaphore(1)

    task = Task(id="task-events-success", name="Task Events", pipeline_name="p", max_retries=0)
    scheduler._tasks[task.id] = task
    result = PipelineResult(pipeline_name="p", task_id=task.id, success=True)

    returned = await scheduler._execute_task(task, _StaticPipeline(result))
    if scheduler._background_tasks:
        await asyncio.gather(*scheduler._background_tasks)

    events = await event_service.list_events(task.id)

    assert returned is result
    assert [event.type for event in events] == ["status", "complete"]
    assert events[0].payload["task_status"] == "running"
    assert events[-1].payload["task_status"] == "success"
    assert event_bus.task_events[-1].event["type"] == "complete"


@pytest.mark.asyncio
async def test_pipeline_emits_stage_events() -> None:
    events = []

    async def on_event(task_id, event_type, level, message, payload):
        events.append(
            {
                "task_id": task_id,
                "type": event_type,
                "level": level,
                "message": message,
                "payload": payload,
            }
        )

    task = Task(
        id="pipeline-events",
        name="Pipeline Events",
        targets=[TaskTarget(name="CS2", params={"app_id": "730"})],
    )
    pipeline = Pipeline("event_pipeline").add_collector("event_test_collector").on_event(on_event)

    result = await pipeline.execute(task)

    assert result.success is True
    assert [event["type"] for event in events] == ["collect", "collect", "pipeline"]
    assert events[0]["payload"]["status"] == "started"
    assert events[1]["payload"]["status"] == "succeeded"
    assert events[-1]["payload"]["storage_count"] == 0


def test_task_events_api_returns_events(monkeypatch) -> None:
    from src.web import app as app_module
    from src.web.app import create_app

    service = InMemoryTaskEventService()
    event = asyncio.run(service.append("task-api-events", "collect", message="ok"))

    class FakeTaskService:
        def __init__(self) -> None:
            self.order = ""

        async def get_task_events(
            self,
            task_id: str,
            *,
            limit: int = 200,
            offset: int = 0,
            order: str = "asc",
        ):
            self.order = order
            if task_id != "task-api-events":
                return None
            return [event]

    fake_service = FakeTaskService()
    monkeypatch.setattr(app_module, "get_task_service", lambda: fake_service)

    client = TestClient(create_app())
    response = client.get("/api/tasks/task-api-events/events?order=desc")

    assert response.status_code == 200
    assert response.json()["events"][0]["type"] == "collect"
    assert fake_service.order == "desc"


@registry.register("collector", "event_test_collector")
class _EventTestCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, data={"value": 1}, metadata={"target": target.name})


@registry.register("processor", "event_test_processor")
class _EventTestProcessor(BaseProcessor):
    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        return ProcessOutput(data=input_data.data, processor_name="event_test_processor")


class _StaticPipeline:
    name = "p"

    def __init__(self, result: PipelineResult) -> None:
        self._result = result

    async def execute(self, task: Task) -> PipelineResult:
        return self._result


class _FakeEventBus:
    def __init__(self) -> None:
        self.completed_events = []
        self.updated_events = []
        self.task_events = []

    async def emit(self, event_name: str, event) -> None:
        if event_name == "task_completed":
            self.completed_events.append(event)
        elif event_name == "task_updated":
            self.updated_events.append(event)
        elif event_name == "task_event":
            self.task_events.append(event)
