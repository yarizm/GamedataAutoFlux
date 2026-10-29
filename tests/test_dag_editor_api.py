"""DAG 编辑器相关接口：逻辑节点/策略的保存与校验、节点目录、运行追踪。"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.dag_trace import fold_dag_node_events
from src.core.pipeline import Pipeline
from src.core.registry import registry
from src.core.task import Task, TaskStatus, TaskTarget
from src.web.app import app

_GRAPH_NAME = "__dag_editor_api_logic__"


def _graph(**overrides):
    nodes = [
        {"id": "input_params", "type": "input", "component": "", "ports_out": [{"name": "params"}]},
        {
            "id": "steam_1",
            "type": "collector",
            "component": "steam",
            "policy": {"max_retries": 1, "retry_interval_seconds": 0, "on_error": "error_branch"},
        },
        {
            "id": "cond",
            "type": "logic",
            "component": "condition",
            # 客户端上报的端口会被忽略，由服务端按 cases 推导
            "ports_out": [{"name": "bogus"}],
            "config": {"cases": [{
                "id": "has_data",
                "conditions": [{"source": "aggregate", "field": "count", "operator": "gt", "value": 0}],
            }]},
        },
        {"id": "store", "type": "storage", "component": "sqlalchemy"},
    ]
    edges = [
        {"from": "input_params", "out": "params", "to": "steam_1", "in": "params"},
        {"from": "steam_1", "out": "records", "to": "cond", "in": "records"},
        {"from": "cond", "out": "has_data", "to": "store", "in": "records"},
        {"from": "steam_1", "out": "error", "to": "store", "in": "records"},
    ]
    payload = {"name": _GRAPH_NAME, "nodes": nodes, "edges": edges}
    payload.update(overrides)
    return payload


def test_save_dag_with_logic_node_and_policy_roundtrip():
    with TestClient(app) as client:
        response = client.post("/api/dags", json=_graph())
        try:
            assert response.status_code == 200, response.text
            saved = {node["id"]: node for node in response.json()["config"]["nodes"]}
            assert [p["name"] for p in saved["cond"]["ports_out"]] == ["has_data", "else"]
            assert "error" in [p["name"] for p in saved["steam_1"]["ports_out"]]
            assert saved["steam_1"]["policy"]["on_error"] == "error_branch"

            loaded = client.get(f"/api/dags/{_GRAPH_NAME}").json()
            loaded_nodes = {node["id"]: node for node in loaded["nodes"]}
            assert loaded_nodes["steam_1"]["policy"]["max_retries"] == 1
            assert loaded_nodes["cond"]["config"]["cases"][0]["id"] == "has_data"
        finally:
            client.delete(f"/api/dags/{_GRAPH_NAME}?confirm=true")


def test_save_dag_rejects_invalid_logic_config_and_policy():
    with TestClient(app) as client:
        bad_logic = _graph()
        bad_logic["nodes"][2]["config"] = {"cases": []}
        response = client.post("/api/dags", json=bad_logic)
        assert response.status_code == 400
        assert response.json()["detail"]["code"] == "dag_logic_config_invalid"
        assert response.json()["detail"]["node_id"] == "cond"

        bad_policy = _graph()
        bad_policy["nodes"][1]["policy"] = {"max_retries": 99}
        response = client.post("/api/dags", json=bad_policy)
        assert response.status_code == 400
        assert response.json()["detail"]["code"] == "dag_policy_invalid"


def test_validate_endpoint_returns_issue_details_without_saving():
    with TestClient(app) as client:
        graph = _graph()
        graph["edges"][2]["out"] = "missing_case"
        graph["edges"].pop()  # error 端口不接 → warning
        response = client.post("/api/dags/validate", json=graph)
        listed = client.get("/api/dags").json()

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    errors = [i for i in body["issues"] if i["severity"] == "error"]
    assert any(i["code"] == "missing_output_port" and i["node_id"] == "cond" for i in errors)
    warnings = [i for i in body["issues"] if i["severity"] == "warning"]
    assert any(i["code"] == "error_branch_unconnected" and i["node_id"] == "steam_1" for i in warnings)
    assert _GRAPH_NAME not in listed


def test_component_metadata_includes_builtin_logic_nodes():
    with TestClient(app) as client:
        payload = client.get("/api/components/metadata").json()
    logic = {item["component"]: item for item in payload["dag_nodes"] if item["type"] == "logic"}
    assert set(logic) == {"condition", "filter", "transform"}
    assert logic["condition"]["default_config"]["cases"]
    assert "gte" in logic["filter"]["operators"]


def test_dag_trace_endpoint_folds_node_events(monkeypatch):
    task = Task(name="traced", pipeline_name="p", targets=[TaskTarget(name="g")])
    task.status = TaskStatus.SUCCESS
    events = [
        SimpleNamespace(to_public_payload=lambda: {"payload": {"node_id": "c1", "status": "started", "node": {"node_id": "c1", "status": "running"}}}),
        SimpleNamespace(to_public_payload=lambda: {"payload": {"node_id": "c1", "status": "progress", "checkpoint_cursor": {"i": 1}}}),
        SimpleNamespace(to_public_payload=lambda: {"payload": {"node_id": "c1", "status": "succeeded", "node": {"node_id": "c1", "status": "succeeded", "elapsed_ms": 12}}}),
        SimpleNamespace(to_public_payload=lambda: {"payload": {"node_id": "p1", "status": "skipped"}}),
        SimpleNamespace(to_public_payload=lambda: {"payload": {"progress": 0.5}}),
    ]

    class _FakeTaskService:
        def get_task(self, task_id):
            return task if task_id == task.id else None

        async def get_task_events(self, task_id, *, limit, offset, order):
            return events[offset:offset + limit]

    with TestClient(app) as client:
        monkeypatch.setattr("src.bootstrap.container.get_task_service", lambda: _FakeTaskService())
        response = client.get(f"/api/tasks/{task.id}/dag-trace")
        missing = client.get("/api/tasks/not-a-task/dag-trace")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["nodes"]["c1"] == {"node_id": "c1", "status": "succeeded", "elapsed_ms": 12}
    assert body["nodes"]["p1"]["status"] == "skipped"
    assert missing.status_code == 404


class _TraceCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, data={"name": target.name})


@pytest.mark.asyncio
async def test_pipeline_dag_events_carry_node_snapshots():
    snap = registry.snapshot()
    registry.register("collector", "_trace_collector")(_TraceCollector)
    events: list[dict] = []

    async def on_event(task_id, event_type, level, message, payload):
        events.append({"type": event_type, "level": level, "payload": payload or {}})

    try:
        pipeline = (
            Pipeline("trace_events")
            .add_collector("_trace_collector")
            .add_processor("cleaner")
            .on_event(on_event)
        )
        result = await pipeline.execute(Task(name="t", targets=[TaskTarget(name="g")]))
    finally:
        registry.restore(snap)

    assert result.success
    node_events = [e for e in events if e["payload"].get("node_id")]
    assert {e["payload"]["node_id"] for e in node_events} == {"collect_0__trace_collector", "process_0_cleaner"}
    assert all(isinstance(e["payload"].get("node"), dict) for e in node_events)
    folded = fold_dag_node_events(node_events)
    assert {item["status"] for item in folded.values()} == {"succeeded"}
    assert folded["collect_0__trace_collector"]["output_counts"] == {"records": 1}


@pytest.mark.asyncio
async def test_worker_dag_callbacks_forward_trace_and_handled_level():
    from src.core.dag import NodeSpec
    from src.worker.agent import WorkerAgent, WorkerAgentConfig

    agent = WorkerAgent(WorkerAgentConfig(base_url="http://127.0.0.1:8000", worker_id="w-trace"))
    captured: list[dict] = []

    async def fake_append_event(task_id, *, event_type, level="info", message="", payload=None):
        captured.append({"type": event_type, "level": level, "payload": payload})

    agent._append_event = fake_append_event  # type: ignore[method-assign]
    _, on_event = agent._build_dag_callbacks("task-1")
    spec = NodeSpec("cond", "logic", "condition", {}, [], [], set())
    await on_event("task-1", spec, "retry", error="boom", trace={"status": "retrying"})
    await on_event("task-1", spec, "error", error="boom", trace={"status": "exception", "on_error": "continue"})

    assert [item["type"] for item in captured] == ["logic", "logic"]
    assert captured[0]["level"] == "warning" and captured[0]["payload"]["status"] == "retrying"
    assert captured[1]["level"] == "warning" and captured[1]["payload"]["handled"] is True
    assert captured[1]["payload"]["node"] == {"status": "exception", "on_error": "continue"}
