import json
from types import SimpleNamespace

import pytest

from src.agent.tools.tasks import CreateTaskTool, GetTaskDetailTool
from src.collectors.base import CollectResult, CollectTarget
from src.core.pipeline import PipelineResult
from src.core.task import Task


@pytest.mark.asyncio
async def test_create_task_tool_returns_final_targets_and_auto_fill_summary(
    monkeypatch,
) -> None:
    async def fake_auto_fill(targets, pipeline_name):
        assert pipeline_name == "steam_basic"
        next_targets = [dict(target) for target in targets]
        next_targets[0] = {
            **next_targets[0],
            "params": {
                **next_targets[0].get("params", {}),
                "app_id": "730",
            },
        }
        return next_targets

    fake_service = _FakeCreateTaskService()
    monkeypatch.setattr("src.agent.tools.tasks._auto_fill_identifiers", fake_auto_fill)
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)

    payload = json.loads(
        await CreateTaskTool()._arun(
            name="Collect CS2",
            pipeline_name="steam_basic",
            collector_name="steam",
            targets=[
                {
                    "name": "Counter-Strike 2",
                    "target_type": "game",
                    "params": {"api_key": "secret-key"},
                }
            ],
            config={"batch_concurrency": 1},
        )
    )

    assert payload["status"] == "ok"
    assert payload["record_count"] == 1
    assert payload["data"]["success"] is True
    assert payload["data"]["task_id"] == "task-created"
    assert payload["data"]["collector_name"] == "steam"
    assert payload["data"]["targets_count"] == 1
    assert payload["data"]["targets"] == [
        {
            "name": "Counter-Strike 2",
            "target_type": "game",
            "params": {"api_key": "[REDACTED]", "app_id": "730"},
        }
    ]
    assert payload["data"]["auto_filled_identifiers"] == [
        {
            "target_index": 0,
            "target_name": "Counter-Strike 2",
            "added_params": {"app_id": "730"},
            "changed_params": {},
        }
    ]
    assert "secret-key" not in json.dumps(payload, ensure_ascii=False)
    assert fake_service.created["targets"][0]["params"] == {
        "api_key": "secret-key",
        "app_id": "730",
    }


@pytest.mark.asyncio
async def test_create_task_tool_redacts_create_exception_detail(monkeypatch) -> None:
    async def passthrough_auto_fill(targets, pipeline_name):
        return targets

    fake_service = _FakeCreateTaskService()
    fake_service.create_error = RuntimeError(
        "create failed with api_key=secret-key; token: secret-token"
    )
    monkeypatch.setattr("src.agent.tools.tasks._auto_fill_identifiers", passthrough_auto_fill)
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)

    payload = json.loads(
        await CreateTaskTool()._arun(
            name="Collect secret",
            pipeline_name="steam_basic",
            collector_name="steam",
            targets=[{"name": "Counter-Strike 2", "target_type": "game", "params": {}}],
            config={},
        )
    )

    rendered = json.dumps(payload, ensure_ascii=False)

    assert payload["status"] == "error"
    assert "secret-key" not in rendered
    assert "secret-token" not in rendered
    assert "api_key=[REDACTED]" in rendered
    assert "token=[REDACTED]" in rendered


@pytest.mark.asyncio
async def test_create_task_tool_guides_session_setup_when_precheck_is_blocked(monkeypatch) -> None:
    async def passthrough_auto_fill(targets, pipeline_name):
        return targets

    fake_service = _FakeCreateTaskService()
    fake_service.precheck_response = SimpleNamespace(
        can_submit=False,
        issues=[
            SimpleNamespace(
                level="error",
                code="session_blocked",
                field="session",
                message="Local browser profile is missing.",
            )
        ],
        required_fields=["target.params.app_id"],
        session_readiness={
            "precheck_status": "error",
            "recommended_action": "prepare_local_profile",
            "summary": (
                "Local browser profile is missing. Complete the one-time browser login "
                "before submitting this task."
            ),
        },
    )
    monkeypatch.setattr("src.agent.tools.tasks._auto_fill_identifiers", passthrough_auto_fill)
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)

    payload = json.loads(
        await CreateTaskTool()._arun(
            name="Collect Qimai",
            pipeline_name="qimai_basic",
            collector_name="qimai",
            targets=[{"name": "Example", "target_type": "app", "params": {"app_id": "123"}}],
            config={},
        )
    )

    assert payload["status"] == "error"
    assert "Prepare the collector browser profile before retrying." in payload["suggestion"]
    assert "Current session state" in payload["suggestion"]


@pytest.mark.asyncio
async def test_get_task_detail_guides_partial_collection_follow_up(monkeypatch) -> None:
    task = Task(
        id="task-partial",
        name="Partial Task",
        pipeline_name="steam_basic",
        collector_name="steam",
    )
    result = PipelineResult(pipeline_name=task.pipeline_name, task_id=task.id, success=False)
    result.storage_count = 1
    result.errors = ["collect failed api_key=result-secret"]
    result.collect_results = [
        CollectResult(
            target=CollectTarget(name="ok"),
            success=True,
            data={"value": 1},
        ),
        CollectResult(
            target=CollectTarget(name="failed api_key=target-secret"),
            success=False,
            error="network token=collector-secret",
            error_code="network_unreachable",
            metadata={"attempts": 2, "max_attempts": 2, "retry_attempts": 1},
        ),
    ]
    task.result = result
    task.fail("partial failure api_key=task-secret")
    restored = Task.from_storage_payload(task.to_storage_payload())
    monkeypatch.setattr(
        "src.web.app.get_task_service",
        lambda: _FakeTaskDetailService(restored),
    )

    payload = json.loads(await GetTaskDetailTool()._arun(task.id))
    rendered = json.dumps(payload, ensure_ascii=False)
    data = payload["data"]

    assert payload["status"] == "ok"
    assert data["result_summary"]["collection_summary"]["status"] == "partial"
    assert "usable partial collection data" in data["agent_guidance"]
    assert data["recommended_actions"][0] == {
        "type": "review_collection_results",
        "recommended_tool": "review_collection_results",
        "args": {"task_id": task.id, "auto_retry": False},
        "why": "Inspect failed targets, retry metadata, and stored source records.",
    }
    assert data["recommended_actions"][1]["recommended_tool"] == "precheck_report"
    assert "review_collection_results -> precheck_report" in payload["suggestion"]
    assert "target-secret" not in rendered
    assert "collector-secret" not in rendered
    assert "result-secret" not in rendered
    assert "task-secret" not in rendered


@pytest.mark.asyncio
async def test_get_task_detail_guides_successful_task_to_report_precheck(monkeypatch) -> None:
    task = Task(
        id="task-success",
        name="Success Task",
        pipeline_name="steam_basic",
        collector_name="steam",
    )
    task.complete({"success": True, "storage_count": 2})
    monkeypatch.setattr(
        "src.web.app.get_task_service",
        lambda: _FakeTaskDetailService(task),
    )

    payload = json.loads(await GetTaskDetailTool()._arun(task.id))
    data = payload["data"]

    assert payload["status"] == "ok"
    assert "Run report precheck" in data["agent_guidance"]
    assert [action["recommended_tool"] for action in data["recommended_actions"]] == [
        "precheck_report",
        "generate_report",
    ]
    assert "precheck_report -> generate_report" in payload["suggestion"]


@pytest.mark.asyncio
async def test_get_task_detail_includes_recovery_guidance(monkeypatch) -> None:
    task = Task(
        id="task-recovery",
        name="Recovery Task",
        pipeline_name="gtrends_basic",
        collector_name="gtrends",
    )
    task.fail("failed")
    fake_service = _FakeTaskDetailService(task)
    fake_service.recovery = {
        "collector_id": "gtrends",
        "supports_checkpoint": True,
        "recovery_level": "L1",
        "latest_checkpoint": {"checkpoint_id": "checkpoint-1", "seq": 1},
    }
    fake_service.collector_metadata = {
        "collector_id": "gtrends",
        "session_mode": "api_only",
        "supports_checkpoint": True,
    }
    fake_service.session_diagnostics = {
        "collector_id": "gtrends",
        "session_mode": "api_only",
        "status": "ok",
    }
    fake_service.session_readiness = {
        "status": "not_required",
        "summary": "No local session required for task submission.",
    }
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)

    payload = json.loads(await GetTaskDetailTool()._arun(task.id))
    data = payload["data"]

    assert data["recovery"]["recovery_level"] == "L1"
    assert data["collector_metadata"]["collector_id"] == "gtrends"
    assert data["session_diagnostics"]["status"] == "ok"
    assert data["session_readiness"]["status"] == "not_required"
    assert "checkpoint is available" in data["agent_guidance"]


class _FakeCreateTaskService:
    def __init__(self) -> None:
        self.created = {}
        self.create_error = None
        self.precheck_response = SimpleNamespace(
            can_submit=True,
            issues=[],
            required_fields=["target.name"],
            session_readiness={},
        )

    def precheck(self, **kwargs):
        return self.precheck_response

    async def create(self, **kwargs):
        if self.create_error is not None:
            raise self.create_error
        self.created = kwargs
        return SimpleNamespace(
            id="task-created",
            collector_name=kwargs["collector_name"],
        )


class _FakeTaskDetailService:
    def __init__(self, task: Task) -> None:
        self.task = task
        self.recovery = {}
        self.collector_metadata = {}
        self.session_diagnostics = {}
        self.session_readiness = {}

    def get_task(self, task_id: str) -> Task | None:
        return self.task if task_id == self.task.id else None

    async def get_task_recovery_info(self, task_id: str):
        return self.recovery if task_id == self.task.id else None

    def get_task_collector_metadata(self, task_id: str):
        return self.collector_metadata if task_id == self.task.id else None

    def get_task_session_diagnostics(self, task_id: str):
        return self.session_diagnostics if task_id == self.task.id else None

    def get_task_session_readiness(self, task_id: str):
        return self.session_readiness if task_id == self.task.id else None
