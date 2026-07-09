# tests/test_dag_scenarios.py
"""四类 DAG 验证场景 + Pipeline 等价测试。"""
import pytest

from src.core.dag import DAG, Edge, NodeSpec, PortSpec, pipeline_to_dag
from src.core.dag_executor import DAGExecutor
from src.core.registry import registry
from src.core.task import Task, TaskTarget
from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput


class _SumCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(
            target=target,
            data={"name": target.name, "src": self.config.get("src", "x"), "value": 1},
            success=True,
        )


class _FailingCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, success=False, error="boom", data=None)


class _PassthroughProcessor(BaseProcessor):
    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        return ProcessOutput(
            success=True, data=input_data.data,
            metadata=input_data.metadata, processor_name="passthrough",
        )


def _src(id, comp, config=None):
    return NodeSpec(id, "collector", comp, config or {}, [], [PortSpec("records")], set())


def _proc(id, comp="passthrough"):
    return NodeSpec(id, "processor", comp, {}, [PortSpec("records")], [PortSpec("records")], set())


def _sink(id, comp="sqlalchemy"):
    return NodeSpec(id, "storage", comp, {}, [PortSpec("records")], [], set())


def _register_dummy_components():
    """注册测试用 collector/processor/storage，返回快照供恢复。"""
    from src.storage.base import BaseStorage, QueryResult, StorageRecord

    class _MemStorage(BaseStorage):
        saved: list = []

        async def save(self, record):
            _MemStorage.saved.append(record)

        async def save_batch(self, records):
            _MemStorage.saved.extend(records)

        async def load(self, key):
            return None

        async def query(self, query, limit=10, **kwargs):
            return QueryResult(records=[], total=0, query=query)

    snap = registry.snapshot()
    registry.register("collector", "_sum")(_SumCollector)
    registry.register("collector", "_failing_sc")(_FailingCollector)
    registry.register("processor", "passthrough")(_PassthroughProcessor)
    registry.register("storage", "sqlalchemy")(_MemStorage)
    _MemStorage.saved = []
    return snap, _MemStorage


@pytest.mark.asyncio
async def test_scenario1_multi_source_parallel_merge():
    """场景1：多源并行汇合，2 collector → 1 processor → storage，全数据到达。"""
    snap, mem = _register_dummy_components()
    try:
        dag = DAG(
            name="multi_src",
            nodes=[
                _src("steam", "_sum", {"src": "steam"}),
                _src("taptap", "_sum", {"src": "taptap"}),
                _proc("proc"),
                _sink("store"),
            ],
            edges=[
                Edge("steam", "records", "proc", "records"),
                Edge("taptap", "records", "proc", "records"),
                Edge("proc", "records", "store", "records"),
            ],
        )
        task = Task(name="t", targets=[TaskTarget(name="gameA")])
        result = await DAGExecutor().execute(task, dag)
        assert result.success
        # 两个 collector 的数据都应到达 processor（多入边聚合）
        assert len(result.collect_results) == 2
        assert len(result.process_results) == 2
    finally:
        registry.restore(snap)


@pytest.mark.asyncio
async def test_scenario2_conditional_failure_fallback():
    """场景2：条件分支/故障转移。primary on_success，fallback 直连。"""
    snap, mem = _register_dummy_components()
    try:
        dag = DAG(
            name="fallback",
            nodes=[
                NodeSpec("primary", "collector", "_failing_sc", {}, [], [PortSpec("records")], set()),
                NodeSpec("fallback", "collector", "_sum", {"src": "fb"}, [], [PortSpec("records")], set()),
                _proc("proc"),
                _sink("store"),
            ],
            edges=[
                Edge("primary", "records", "proc", "records", condition="on_success"),
                Edge("fallback", "records", "proc", "records"),
                Edge("proc", "records", "store", "records"),
            ],
        )
        task = Task(name="t", targets=[TaskTarget(name="gameA")])
        result = await DAGExecutor().execute(task, dag)
        # primary 失败 → on_success 抑制 primary 边；fallback 数据走通
        assert len(result.collect_results) >= 1
        assert result.process_results
        # fallback 的数据应到达
        assert any(r.data.get("src") == "fb" for r in result.collect_results)
    finally:
        registry.restore(snap)


@pytest.mark.asyncio
async def test_scenario3_data_dependency_passthrough():
    """场景3：节点间数据依赖传递（collector → processor → storage 端到端）。"""
    snap, mem = _register_dummy_components()
    try:
        dag = DAG(
            name="dep_chain",
            nodes=[_src("src", "_sum", {"src": "chain"}), _proc("proc"), _sink("store")],
            edges=[
                Edge("src", "records", "proc", "records"),
                Edge("proc", "records", "store", "records"),
            ],
        )
        task = Task(name="t", targets=[TaskTarget(name="gameA"), TaskTarget(name="gameB")])
        result = await DAGExecutor().execute(task, dag)
        assert result.success
        assert len(result.collect_results) == 2
        assert len(result.process_results) == 2
        # storage 收到记录
        assert len(mem.saved) == 2
        # output_records 被填充
        assert len(result.output_records) == 2
    finally:
        registry.restore(snap)


@pytest.mark.asyncio
async def test_scenario4_composite_subgraph():
    """场景4：可复用子图（CompositeNode 展开）。"""
    snap, mem = _register_dummy_components()
    try:
        # 子图：sum → passthrough
        sub_dag = DAG(
            name="sub_pipeline",
            nodes=[_src("sub_src", "_sum", {"src": "sub"}), _proc("sub_proc")],
            edges=[Edge("sub_src", "records", "sub_proc", "records")],
        )

        def loader(name):
            return sub_dag if name == "sub_pipeline" else None

        # 父图：composite 节点 + storage
        parent = DAG(
            name="with_composite",
            nodes=[
                NodeSpec("comp", "composite", "", {}, [], [PortSpec("records")], set(), subgraph_name="sub_pipeline"),
                _sink("store"),
            ],
            edges=[Edge("comp", "records", "store", "records")],
        )
        task = Task(name="t", targets=[TaskTarget(name="gameA")])
        executor = DAGExecutor(subgraph_loader=loader)
        result = await executor.execute(task, parent)
        assert result.success
        # 子图 collector + processor 都执行
        assert len(result.collect_results) == 1
        assert len(result.process_results) == 1
        assert len(mem.saved) == 1
    finally:
        registry.restore(snap)


@pytest.mark.asyncio
async def test_scenario5_pipeline_dag_equivalence():
    """场景5：同任务走 Pipeline.execute() 与 pipeline_to_dag+DAGExecutor 行为等价。"""
    from src.core.pipeline import Pipeline

    snap, mem = _register_dummy_components()
    try:
        targets = [TaskTarget(name="A"), TaskTarget(name="B")]
        pipeline = (
            Pipeline("equiv_pipeline")
            .add_collector("_sum", config={"src": "equiv"})
            .add_processor("passthrough")
            .add_storage("sqlalchemy")
        )

        # Pipeline 路径
        mem.saved = []
        task1 = Task(name="t1", targets=targets)
        r1 = await pipeline.execute(task1)

        # DAG 路径
        mem.saved = []
        dag = pipeline_to_dag(pipeline)
        task2 = Task(name="t2", targets=targets)
        r2 = await DAGExecutor().execute(task2, dag)

        # 等价性断言
        assert r1.success == r2.success
        assert len(r1.collect_results) == len(r2.collect_results)
        assert len(r1.process_results) == len(r2.process_results)
        assert r1.storage_count == r2.storage_count
        assert len(r1.output_records) == len(r2.output_records)
        # 存储键格式一致（task.id 不同故只比结构：去掉 task.id 前缀后应相同）
        def _key_tail(key):
            return key.split(":", 1)[1] if ":" in key else key

        assert _key_tail(r1.output_records[0].key) == _key_tail(r2.output_records[0].key)
        # resume_state 都是 pipeline 格式
        assert "target_order" in r1.resume_state
        assert "target_order" in r2.resume_state
    finally:
        registry.restore(snap)
