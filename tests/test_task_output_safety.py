import json

import pytest

from src.core.events import TaskCompletedEvent
from src.core.hooks import AlertHook
from src.core.scheduler import _join_safe_error_messages
from src.core.task import Task, TaskStatus, TaskTarget
from src.services.task_service import TaskService
from src.web.routes.tasks import _task_to_detail_response, _task_to_response


def test_task_route_responses_redact_in_memory_sensitive_errors() -> None:
    task = Task(
        id="task-output-redact",
        name="api_key=task-secret",
        description="description token=description-secret",
        pipeline_name="steam_basic",
        collector_name="steam",
        targets=[
            TaskTarget(
                name="CS2",
                target_type="game",
                params={"api_key": "target-secret", "app_id": "730"},
            )
        ],
        config={"cookie": "config-cookie"},
    )
    task.add_step_log(
        "collect:api_key=step-secret",
        TaskStatus.FAILED,
        "message token=message-secret",
        error="error api_key=log-secret",
    )
    task.fail("failed api_key=error-secret")

    summary = _task_to_response(task)
    detail = _task_to_detail_response(task)
    rendered = summary.model_dump_json() + detail.model_dump_json()

    assert summary.name == "api_key=[REDACTED]"
    assert summary.error == "failed api_key=[REDACTED]"
    assert detail.description == "description token=[REDACTED]"
    assert detail.targets[0]["params"]["api_key"] == "[REDACTED]"
    assert detail.config["cookie"] == "[REDACTED]"
    assert detail.step_logs[0].step == "collect:api_key=[REDACTED]"
    assert detail.step_logs[0].message == "message token=[REDACTED]"
    assert detail.step_logs[0].error == "error api_key=[REDACTED]"
    assert "task-secret" not in rendered
    assert "description-secret" not in rendered
    assert "target-secret" not in rendered
    assert "message-secret" not in rendered
    assert "error-secret" not in rendered


def test_task_service_logs_redact_sensitive_text() -> None:
    task = Task(id="task-log-redact", name="Task")
    task.add_step_log(
        "collect:api_key=step-secret",
        TaskStatus.FAILED,
        "message token=message-secret",
        error="error api_key=log-secret",
    )
    service = TaskService(_FakeScheduler(task))

    logs = service.get_task_logs(task.id)
    rendered = json.dumps(logs, ensure_ascii=False)

    assert logs == [
        {
            "step": "collect:api_key=[REDACTED]",
            "status": "failed",
            "message": "message token=[REDACTED]",
            "error": "error api_key=[REDACTED]",
            "started_at": logs[0]["started_at"],
            "completed_at": logs[0]["completed_at"],
        }
    ]
    assert "step-secret" not in rendered
    assert "message-secret" not in rendered
    assert "log-secret" not in rendered


def test_scheduler_error_join_redacts_sensitive_text() -> None:
    message = _join_safe_error_messages(
        ["collect failed api_key=error-secret", "retry token=retry-secret"]
    )

    assert message == "collect failed api_key=[REDACTED]; retry token=[REDACTED]"
    assert "error-secret" not in message
    assert "retry-secret" not in message


@pytest.mark.asyncio
async def test_alert_hook_redacts_sensitive_task_name_and_errors() -> None:
    task = Task(id="task-alert-redact", name="api_key=task-secret")
    alert_service = _FakeAlertService()
    hook = AlertHook(alert_service)

    await hook.handle(
        TaskCompletedEvent(
            task_id=task.id,
            success=False,
            result=None,
            task=task,
            errors=["failed api_key=error-secret", "retry token=retry-secret"],
        )
    )

    assert alert_service.messages == [
        {
            "title": "任务执行失败: api_key=[REDACTED]",
            "content": (
                "**Task ID**: task-alert-redact\n"
                "**Error**: failed api_key=[REDACTED]; retry token=[REDACTED]"
            ),
            "level": "error",
        }
    ]
    rendered = json.dumps(alert_service.messages, ensure_ascii=False)
    assert "task-secret" not in rendered
    assert "error-secret" not in rendered
    assert "retry-secret" not in rendered


class _FakeScheduler:
    def __init__(self, task: Task) -> None:
        self.task = task

    def get_task(self, task_id: str) -> Task | None:
        return self.task if task_id == self.task.id else None


class _FakeAlertService:
    def __init__(self) -> None:
        self.messages: list[dict[str, str]] = []

    async def send_alert(self, title: str, content: str, level: str = "error", **kwargs) -> None:
        self.messages.append({"title": title, "content": content, "level": level})
