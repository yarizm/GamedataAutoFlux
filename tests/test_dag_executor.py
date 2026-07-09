# tests/test_dag_executor.py
import pytest
from src.core.dag import DAG, NodeSpec, Edge, PortSpec
from src.core.dag_executor import validate_dag, topological_layers, DAGValidationError, DAGExecutor
from src.core.task import Task, TaskTarget
from src.core.registry import registry
from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput


def _src_node(id="src"):
    return NodeSpec(id, "collector", "steam", {}, [], [PortSpec("records")], set())


def _sink_node(id="store"):
    return NodeSpec(id, "storage", "sqlalchemy", {}, [PortSpec("records")], [], set())


def _proc_node(id="proc"):
    return NodeSpec(id, "processor", "cleaner", {}, [PortSpec("records")], [PortSpec("records")], set())


# ── Task 4: validation + topology ──

def test_validate_dag_rejects_cycle():
    dag = DAG(
        name="cyc",
        nodes=[_src_node("a"), _src_node("b")],
        edges=[Edge("a", "records", "b", "records"), Edge("b", "records", "a", "records")],
    )
    issues = validate_dag(dag)
    assert any("cycle" in i.lower() for i in issues)


def test_validate_dag_rejects_dangling_required_input():
    dag = DAG(name="dangling", nodes=[_sink_node("store")], edges=[])
    issues = validate_dag(dag)
    assert any("dangling" in i.lower() or "unconnected" in i.lower() for i in issues)


def test_validate_dag_allows_optional_input_unconnected():
    dag = DAG(
        name="opt",
        nodes=[NodeSpec("src", "collector", "steam", {}, [], [PortSpec("records")], set()),
               NodeSpec("sink", "storage", "sqlalchemy", {}, [PortSpec("records", required=False)], [], set())],
        edges=[Edge("src", "records", "sink", "records")],
    )
    issues = validate_dag(dag)
    assert len(issues) == 0


def test_topological_layers_parallel_sources():
    dag = DAG(
        name="parallel",
        nodes=[_src_node("steam"), _src_node("taptap"), _sink_node("store")],
        edges=[
            Edge("steam", "records", "store", "records"),
            Edge("taptap", "records", "store", "records"),
        ],
    )
    layers = topological_layers(dag)
    assert layers[0] == ["steam", "taptap"]
    assert layers[1] == ["store"]


def test_topological_layers_chain():
    dag = DAG(
        name="chain",
        nodes=[_src_node("src"), _proc_node("proc"), _sink_node("store")],
        edges=[Edge("src", "records", "proc", "records"), Edge("proc", "records", "store", "records")],
    )
    layers = topological_layers(dag)
    assert layers == [["src"], ["proc"], ["store"]]


# ── Task 5-6: execution + fallback ──

class _DummyCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, data={"name": target.name, "value": 1}, success=True)


class _FailingCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, success=False, error="boom", data=None)


class _DummyProcessor(BaseProcessor):
    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        return ProcessOutput(success=True, data={"name": input_data.data.get("name"), "doubled": input_data.data.get("value", 0) * 2}, metadata=input_data.metadata, processor_name="dummy")


@pytest.mark.asyncio
async def test_dag_executor_runs_chain():
    snap = registry.snapshot()
    registry.register("collector", "_dummy_chain")(_DummyCollector)
    registry.register("processor", "_dummy_proc")(_DummyProcessor)
    try:
        dag = DAG(
            name="chain_dag",
            nodes=[
                NodeSpec("src", "collector", "_dummy_chain", {}, [], [PortSpec("records")], set()),
                NodeSpec("proc", "processor", "_dummy_proc", {}, [PortSpec("records")], [PortSpec("records")], set()),
            ],
            edges=[Edge("src", "records", "proc", "records")],
        )
        task = Task(name="t", targets=[TaskTarget(name="gameA")])
        executor = DAGExecutor()
        result = await executor.execute(task, dag)
        assert result.success
        assert len(result.collect_results) == 1
        assert len(result.process_results) == 1
        assert result.process_results[0].data["doubled"] == 2
    finally:
        registry.restore(snap)


@pytest.mark.asyncio
async def test_dag_executor_failure_fallback():
    snap = registry.snapshot()
    registry.register("collector", "_failing")(_FailingCollector)
    registry.register("collector", "_dummy_fb")(_DummyCollector)
    registry.register("processor", "_dummy_proc2")(_DummyProcessor)
    try:
        dag = DAG(
            name="fallback_dag",
            nodes=[
                NodeSpec("primary", "collector", "_failing", {}, [], [PortSpec("records")], set()),
                NodeSpec("fallback", "collector", "_dummy_fb", {}, [], [PortSpec("records")], set()),
                NodeSpec("proc", "processor", "_dummy_proc2", {}, [PortSpec("records")], [PortSpec("records")], set()),
            ],
            edges=[
                Edge("primary", "records", "proc", "records", condition="on_success"),
                Edge("fallback", "records", "proc", "records"),
            ],
        )
        task = Task(name="t", targets=[TaskTarget(name="gameA")])
        result = await DAGExecutor().execute(task, dag)
        assert len(result.collect_results) >= 1
        assert result.process_results
    finally:
        registry.restore(snap)


@pytest.mark.asyncio
async def test_dag_executor_recovery_skips_completed_targets():
    """恢复时 collector 按 checkpoint 的 collect 上下文跳过已采目标。"""
    snap = registry.snapshot()
    registry.register("collector", "_dummy_skip")(_DummyCollector)
    registry.register("processor", "_dummy_skip_proc")(_DummyProcessor)
    try:
        dag = DAG(
            name="skip_dag",
            nodes=[
                NodeSpec("src", "collector", "_dummy_skip", {}, [], [PortSpec("records")], set()),
                NodeSpec("proc", "processor", "_dummy_skip_proc", {}, [PortSpec("records")], [PortSpec("records")], set()),
            ],
            edges=[Edge("src", "records", "proc", "records")],
        )
        task = Task(name="t", targets=[TaskTarget(name="gameA"), TaskTarget(name="gameB")])
        # TaskCheckpoint 形状：state 携带 pipeline 格式 resume_state，指示 gameA 已完成
        checkpoint = {
            "task_id": task.id,
            "seq": 1,
            "state": {
                "target_order": ["gameA", "gameB"],
                "next_target_index": 1,
                "completed_targets": ["gameA"],
            },
        }
        result = await DAGExecutor().execute(task, dag, recovery_checkpoint=checkpoint)
        assert result.success
        # 只采 gameB（gameA 被跳过）
        assert len(result.collect_results) == 1
        assert result.collect_results[0].target.name == "gameB"
        assert len(result.process_results) == 1
    finally:
        registry.restore(snap)
