import json
from types import SimpleNamespace

import pytest

from src.agent.tools.data import (
    GetDataRecordContentTool,
    ListDataGamesTool,
    ReviewCollectionResultsTool,
    SearchDataTool,
)
from src.collectors.base import CollectResult, CollectTarget
from src.core.pipeline import PipelineResult
from src.core.task import Task, TaskStatus, TaskTarget
from src.storage.base import QueryResult, StorageRecord
from src.storage.factory import get_storage


@pytest.mark.asyncio
async def test_list_data_games_excludes_report_history(monkeypatch) -> None:
    fake_store = _FakeDataStore(
        {
            "key:": [
                StorageRecord(
                    key="report:history",
                    source="reporting",
                    data={"collector": "steam", "game_name": "Old Report Game"},
                    metadata={"kind": "report", "data_source": "steam"},
                ),
                StorageRecord(
                    key="record:steam",
                    source="steam",
                    data={"collector": "steam", "game_name": "Counter-Strike 2"},
                ),
            ]
        }
    )
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(await ListDataGamesTool()._arun(limit=1))

    assert payload["status"] == "ok"
    assert payload["record_count"] == 1
    assert payload["data"] == [{"game": "Counter-Strike 2", "sources": ["Steam"]}]
    assert fake_store.queries == [("key:", 20)]


@pytest.mark.asyncio
async def test_search_data_excludes_report_history(monkeypatch) -> None:
    fake_store = _FakeDataStore(
        {
            "Counter-Strike": [
                StorageRecord(
                    key="report:history",
                    source="reporting",
                    data={"collector": "steam", "game_name": "Counter-Strike 2"},
                    metadata={"kind": "report", "data_source": "steam"},
                ),
                StorageRecord(
                    key="record:steam",
                    source="steam",
                    data={"collector": "steam", "game_name": "Counter-Strike 2"},
                ),
            ]
        }
    )
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(await SearchDataTool()._arun("Counter-Strike", limit=1))

    assert payload["status"] == "ok"
    assert payload["record_count"] == 1
    assert payload["data"][0]["key"] == "record:steam"
    assert fake_store.queries == [("Counter-Strike", 20)]


@pytest.mark.asyncio
async def test_get_data_record_content_returns_structured_source_record(monkeypatch) -> None:
    record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
        metadata={"collector": "steam", "api_key": "secret"},
    )
    fake_store = _FakeDataStore({}, records_by_key={record.key: record})
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(await GetDataRecordContentTool()._arun(record.key))

    assert payload["status"] == "ok"
    assert payload["record_count"] == 1
    assert payload["data"]["key"] == "record:steam"
    assert payload["data"]["source"] == "steam"
    assert payload["data"]["metadata"]["api_key"] == "[REDACTED]"
    assert payload["data"]["data"]["game_name"] == "Counter-Strike 2"
    assert fake_store.closed is True


@pytest.mark.asyncio
async def test_get_data_record_content_warns_for_report_history(monkeypatch) -> None:
    record = StorageRecord(
        key="report:history",
        source="reporting",
        data={"title": "Generated report"},
        metadata={"kind": "report", "template": "default"},
    )
    fake_store = _FakeDataStore({}, records_by_key={record.key: record})
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(await GetDataRecordContentTool()._arun(record.key))

    assert payload["status"] == "warning"
    assert payload["record_count"] == 0
    assert payload["data"]["key"] == "report:history"
    assert "get_report_content" in payload["suggestion"]


@pytest.mark.asyncio
async def test_get_data_record_content_missing_key_does_not_echo_secret(monkeypatch) -> None:
    fake_store = _FakeDataStore({})
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(await GetDataRecordContentTool()._arun("missing:api_key=secret-value"))

    assert payload["status"] == "error"
    assert "secret-value" not in payload["summary"]


@pytest.mark.asyncio
async def test_review_collection_results_does_not_retry_without_flag(monkeypatch) -> None:
    task = _make_review_task("review-no-retry")
    await _save_record(StorageRecord(key=f"{task.id}:empty", data={}, source="steam"))
    fake_service = _FakeTaskService(task)
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)

    payload = json.loads(await ReviewCollectionResultsTool()._arun(task.id, auto_retry=False))

    assert payload["completeness"] == "partial"
    assert payload["record_count"] == 1
    assert payload["source_coverage"] == {"steam": 1}
    assert payload["completeness_counts"] == {"empty": 1}
    assert payload["record_summaries"][0]["key"] == f"{task.id}:empty"
    assert payload["record_summaries"][0]["source"] == "steam"
    assert payload["record_summaries"][0]["completeness"] == "empty"
    assert "retry_created" not in payload
    assert fake_service.created == []


@pytest.mark.asyncio
async def test_review_collection_results_auto_retry_creates_task(monkeypatch) -> None:
    task = _make_review_task("review-auto-retry")
    await _save_record(StorageRecord(key=f"{task.id}:empty", data={}, source="steam"))
    fake_service = _FakeTaskService(task)
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)

    payload = json.loads(await ReviewCollectionResultsTool()._arun(task.id, auto_retry=True))

    assert payload["retry_created"] is True
    assert payload["retry_task_id"] == "retry-task"
    assert payload["retry_task_name"] == "Review Task (review retry)"
    assert fake_service.created == [
        {
            "name": "Review Task (review retry)",
            "pipeline_name": "steam_basic",
            "collector_name": "steam",
            "targets": [
                {
                    "name": "Counter-Strike 2",
                    "target_type": "game",
                    "params": {"app_id": "730"},
                }
            ],
            "config": {"batch_concurrency": 2},
            "description": f"Auto retry created by collection review for task {task.id}.",
        }
    ]


@pytest.mark.asyncio
async def test_review_collection_results_auto_retry_targets_failed_collects_only(
    monkeypatch,
) -> None:
    task = Task(
        id="review-targeted-retry",
        name="Targeted Review Task",
        pipeline_name="steam_basic",
        collector_name="steam",
        targets=[
            TaskTarget(
                name="Succeeded Game",
                target_type="game",
                params={"app_id": "100"},
            ),
            TaskTarget(
                name="Failed Game",
                target_type="game",
                params={"app_id": "200"},
            ),
        ],
        config={"batch_concurrency": 2},
    )
    result = PipelineResult(pipeline_name=task.pipeline_name, task_id=task.id, success=False)
    result.storage_count = 1
    result.collect_results = [
        CollectResult(
            target=CollectTarget(name="Succeeded Game"),
            success=True,
            data={"value": 1},
        ),
        CollectResult(
            target=CollectTarget(name="Failed Game"),
            success=False,
            error="timeout",
            error_code="network_unreachable",
            metadata={"attempts": 2, "max_attempts": 2, "retry_attempts": 1},
        ),
    ]
    task.result = result
    task.fail("partial failure")
    restored = Task.from_storage_payload(task.to_storage_payload())
    fake_service = _FakeTaskService(restored)
    fake_store = _FakeDataStore({})
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(await ReviewCollectionResultsTool()._arun(task.id, auto_retry=True))

    assert payload["retry_created"] is True
    assert payload["retry_task_name"] == "Targeted Review Task (targeted review retry)"
    assert fake_service.created == [
        {
            "name": "Targeted Review Task (targeted review retry)",
            "pipeline_name": "steam_basic",
            "collector_name": "steam",
            "targets": [
                {
                    "name": "Failed Game",
                    "target_type": "game",
                    "params": {"app_id": "200"},
                }
            ],
            "config": {"batch_concurrency": 2},
            "description": (
                f"Auto targeted retry created by collection review for task {task.id}."
            ),
        }
    ]


@pytest.mark.asyncio
async def test_review_collection_results_auto_retry_distinguishes_same_named_targets(
    monkeypatch,
) -> None:
    task = Task(
        id="review-targeted-same-name",
        name="Same Name Review Task",
        pipeline_name="steam_basic",
        collector_name="steam",
        targets=[
            TaskTarget(
                name="Same Game",
                target_type="game",
                params={"app_id": "100"},
            ),
            TaskTarget(
                name="Same Game",
                target_type="game",
                params={"app_id": "200", "region": "global"},
            ),
        ],
        config={"batch_concurrency": 2},
    )
    result = PipelineResult(pipeline_name=task.pipeline_name, task_id=task.id, success=False)
    result.storage_count = 1
    result.collect_results = [
        CollectResult(
            target=CollectTarget(
                name="Same Game",
                target_type="game",
                params={"app_id": "100"},
            ),
            success=True,
            data={"value": 1},
        ),
        CollectResult(
            target=CollectTarget(
                name="Same Game",
                target_type="game",
                params={"app_id": "200", "region": "global"},
            ),
            success=False,
            error="timeout",
            error_code="network_unreachable",
        ),
    ]
    task.result = result
    task.fail("partial failure")
    restored = Task.from_storage_payload(task.to_storage_payload())
    fake_service = _FakeTaskService(restored)
    fake_store = _FakeDataStore({})
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(await ReviewCollectionResultsTool()._arun(task.id, auto_retry=True))

    assert payload["retry_created"] is True
    assert fake_service.created[0]["targets"] == [
        {
            "name": "Same Game",
            "target_type": "game",
            "params": {"app_id": "200", "region": "global"},
        }
    ]
    assert payload["collection_summary"]["failed_targets"][0]["target_params"] == {
        "app_id": "200",
        "region": "global",
    }


@pytest.mark.asyncio
async def test_review_collection_results_auto_retry_blocks_redacted_target_params(
    monkeypatch,
) -> None:
    task = Task(
        id="review-targeted-redacted-param",
        name="Redacted Param Review Task",
        pipeline_name="steam_basic",
        collector_name="steam",
        targets=[
            TaskTarget(
                name="Same Game",
                target_type="game",
                params={"app_id": "100"},
            ),
            TaskTarget(
                name="Same Game",
                target_type="game",
                params={"app_id": "200", "api_key": "target-secret"},
            ),
        ],
        config={"batch_concurrency": 2},
    )
    result = PipelineResult(pipeline_name=task.pipeline_name, task_id=task.id, success=False)
    result.storage_count = 1
    result.collect_results = [
        CollectResult(
            target=CollectTarget(
                name="Same Game",
                target_type="game",
                params={"app_id": "100"},
            ),
            success=True,
            data={"value": 1},
        ),
        CollectResult(
            target=CollectTarget(
                name="Same Game",
                target_type="game",
                params={"app_id": "200", "api_key": "target-secret"},
            ),
            success=False,
            error="timeout",
            error_code="network_unreachable",
        ),
    ]
    task.result = result
    task.fail("partial failure")
    restored = Task.from_storage_payload(task.to_public_payload())
    fake_service = _FakeTaskService(restored)
    fake_store = _FakeDataStore({})
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(await ReviewCollectionResultsTool()._arun(task.id, auto_retry=True))
    rendered = json.dumps(payload, ensure_ascii=False)

    assert payload["retry_created"] is False
    assert payload["retry_error"].startswith(
        "Cannot auto retry because selected target params contain redacted"
    )
    assert "retry_task_id" not in payload
    assert "retry_blocked_redacted_params" in {issue["category"] for issue in payload["issues"]}
    assert fake_service.created == []
    assert payload["collection_summary"]["failed_targets"][0]["target_params"] == {
        "app_id": "200",
        "api_key": "[REDACTED]",
    }
    assert "target-secret" not in rendered


@pytest.mark.asyncio
async def test_review_collection_results_auto_retry_redacts_creation_error(monkeypatch) -> None:
    task = _make_review_task("review-auto-retry-error")
    await _save_record(StorageRecord(key=f"{task.id}:empty", data={}, source="steam"))
    fake_service = _FakeTaskService(task)
    fake_service.create_error = RuntimeError("retry failed: api_key=secret-key")
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)

    payload = json.loads(await ReviewCollectionResultsTool()._arun(task.id, auto_retry=True))
    rendered = json.dumps(payload, ensure_ascii=False)

    assert payload["retry_created"] is False
    assert payload["issues"][-1]["category"] == "retry_failed"
    assert "secret-key" not in rendered
    assert "api_key=[REDACTED]" in rendered


@pytest.mark.asyncio
async def test_review_collection_results_matches_task_id_metadata(monkeypatch) -> None:
    task = _make_review_task("review-metadata-match")
    await _save_record(
        StorageRecord(
            key="collector-output:steam:0",
            data={"collector": "steam", "game_name": "Counter-Strike 2"},
            metadata={"task_id": task.id, "collector": "steam"},
            source="steam",
        )
    )
    fake_service = _FakeTaskService(task)
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)

    payload = json.loads(await ReviewCollectionResultsTool()._arun(task.id, auto_retry=False))

    assert payload["record_count"] == 1
    assert payload["source_coverage"] == {"steam": 1}
    assert payload["completeness_counts"] == {"partial": 1}
    assert payload["record_summaries"] == [
        {
            "key": "collector-output:steam:0",
            "source": "steam",
            "collector": "steam",
            "data_source": "Steam",
            "game": "Counter-Strike 2",
            "app_id": "",
            "completeness": "partial",
            "stored_at": payload["record_summaries"][0]["stored_at"],
        }
    ]
    assert payload["issues"] == [
        {
            "level": "info",
            "category": "partial_data",
            "message": "记录 collector-output:steam:0 数据部分完整",
        }
    ]
    assert fake_service.created == []


@pytest.mark.asyncio
async def test_review_collection_results_excludes_report_history_records(monkeypatch) -> None:
    task = _make_review_task("review-report-history")
    await _save_record(
        StorageRecord(
            key="report:auto-history",
            source="reporting",
            data={"title": "Generated report"},
            metadata={"kind": "report", "task_id": task.id},
        )
    )
    await _save_record(
        StorageRecord(
            key="collector-output:steam:0",
            data={"collector": "steam", "game_name": "Counter-Strike 2"},
            metadata={"task_id": task.id, "collector": "steam"},
            source="steam",
        )
    )
    fake_service = _FakeTaskService(task)
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)

    payload = json.loads(await ReviewCollectionResultsTool()._arun(task.id, auto_retry=False))

    assert payload["record_count"] == 1
    assert payload["source_coverage"] == {"steam": 1}
    assert payload["record_summaries"][0]["key"] == "collector-output:steam:0"
    assert "report:auto-history" not in json.dumps(payload, ensure_ascii=False)
    assert payload["issues"][0]["level"] == "info"
    assert payload["issues"][0]["category"] == "partial_data"
    assert "collector-output:steam:0" in payload["issues"][0]["message"]


@pytest.mark.asyncio
async def test_review_collection_results_includes_collector_failure_summary(monkeypatch) -> None:
    task = _make_review_task("review-collector-failure")
    task.status = TaskStatus.FAILED
    task.error = "collect failed api_key=task-secret"
    result = PipelineResult(pipeline_name=task.pipeline_name, task_id=task.id, success=False)
    result.collect_results = [
        CollectResult(
            target=CollectTarget(name="CS2 api_key=target-secret"),
            success=False,
            error="network failed token=result-secret",
            error_code="network_unreachable",
            metadata={
                "attempts": 3,
                "max_attempts": 3,
                "retry_attempts": 2,
                "last_retry_error": "HTTP 429 password=retry-secret",
                "last_retry_error_code": "rate_limited",
            },
        )
    ]
    task.result = result
    task = Task.from_storage_payload(task.to_storage_payload())
    fake_service = _FakeTaskService(task)
    fake_store = _FakeDataStore({})
    monkeypatch.setattr("src.web.app.get_task_service", lambda: fake_service)
    monkeypatch.setattr("src.storage.factory.get_storage", lambda: fake_store)

    payload = json.loads(await ReviewCollectionResultsTool()._arun(task.id, auto_retry=False))
    rendered = json.dumps(payload, ensure_ascii=False)

    assert payload["collection_summary"]["status"] == "failed"
    assert payload["collection_summary"]["failed_targets_count"] == 1
    assert payload["collection_summary"]["failed_targets"][0]["retry"]["retry_attempts"] == 2
    issue_messages = "\n".join(issue["message"] for issue in payload["issues"])
    assert "Last retry failed with [rate_limited] HTTP 429 password=[REDACTED]." in issue_messages
    assert "collector_failure" in {issue["category"] for issue in payload["issues"]}
    assert "empty_result" in {issue["category"] for issue in payload["issues"]}
    assert "api_key=[REDACTED]" in rendered
    assert "target-secret" not in rendered
    assert "task-secret" not in rendered
    assert "result-secret" not in rendered
    assert "retry-secret" not in rendered


def _make_review_task(task_id: str) -> Task:
    task = Task(
        id=task_id,
        name="Review Task",
        pipeline_name="steam_basic",
        collector_name="steam",
        targets=[
            TaskTarget(
                name="Counter-Strike 2",
                target_type="game",
                params={"app_id": "730"},
            )
        ],
        config={"batch_concurrency": 2},
    )
    task.complete({"success": True})
    return task


async def _save_record(record: StorageRecord) -> None:
    store = get_storage()
    await store.initialize()
    try:
        await store.save(record)
    finally:
        await store.close()


class _FakeTaskService:
    def __init__(self, task: Task) -> None:
        self.task = task
        self.created: list[dict] = []
        self.create_error = None

    def get_task(self, task_id: str) -> Task | None:
        return self.task if task_id == self.task.id else None

    def precheck(self, **kwargs):
        return SimpleNamespace(can_submit=True, issues=[])

    async def create(self, **kwargs) -> Task:
        if self.create_error is not None:
            raise self.create_error
        self.created.append(kwargs)
        return Task(
            id="retry-task",
            name=kwargs["name"],
            description=kwargs.get("description", ""),
            pipeline_name=kwargs["pipeline_name"],
            collector_name=kwargs["collector_name"],
            targets=[TaskTarget(**target) for target in kwargs["targets"]],
            config=kwargs["config"],
        )


class _FakeDataStore:
    def __init__(
        self,
        records_by_query: dict[str, list[StorageRecord]],
        *,
        records_by_key: dict[str, StorageRecord] | None = None,
    ) -> None:
        self.records_by_query = records_by_query
        self.records_by_key = records_by_key or {}
        self.queries: list[tuple[str, int]] = []
        self.closed = False

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def query(self, query: str = "", limit: int = 10, **kwargs) -> QueryResult:
        query = query or str(kwargs.get("query") or "")
        self.queries.append((query, limit))
        records = list(self.records_by_query.get(query, []))[:limit]
        return QueryResult(records=records, total=len(records), query=query)

    async def load(self, key: str) -> StorageRecord | None:
        return self.records_by_key.get(key)
