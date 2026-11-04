"""处理器链：DAG 引擎要和旧三段式引擎落库结果一致。

回归：ProcessorNode 只接收 CollectResult，A → B → 存储 这种链第二步输入为空，
任务显示成功（errors 为空）却一条数据都没存。
"""

from __future__ import annotations

from typing import Any

import pytest

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.dag import DAG, Edge, NodeSpec, PortSpec
from src.core.dag_executor import DAGExecutor
from src.core.pipeline import Pipeline
from src.core.registry import registry
from src.core.task import Task, TaskTarget
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.storage.base import BaseStorage, QueryResult


class _Source(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, data={"name": target.name}, success=True)


class _Step(BaseProcessor):
    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        if self.config.get("fail"):
            return ProcessOutput(
                success=False, data=None, metadata=input_data.metadata, processor_name="step"
            )
        data = dict(input_data.data)
        data["steps"] = [*data.get("steps", []), self.config["tag"]]
        return ProcessOutput(
            success=True,
            data=data,
            metadata=input_data.metadata,
            processor_name=self.config["tag"],
        )


class _MemoryStorage(BaseStorage):
    saved: list[Any] = []

    async def save(self, record: Any) -> None:
        _MemoryStorage.saved.append(record)

    async def save_batch(self, records: list[Any]) -> None:
        _MemoryStorage.saved.extend(records)

    async def load(self, key: str) -> None:
        return None

    async def query(self, query: Any, limit: int = 10, **kwargs: Any) -> QueryResult:
        return QueryResult(records=[], total=0, query=query)


@pytest.fixture
def components():
    snapshot = registry.snapshot()
    _MemoryStorage.saved = []
    registry.register("collector", "_chain_src")(_Source)
    registry.register("processor", "_chain_step")(_Step)
    registry.register("storage", "sqlalchemy")(_MemoryStorage)
    try:
        yield
    finally:
        registry.restore(snapshot)


def _pipeline() -> Pipeline:
    return (
        Pipeline("chain")
        .add_collector("_chain_src")
        .add_processor("_chain_step", {"tag": "first"})
        .add_processor("_chain_step", {"tag": "second"})
        .add_storage("sqlalchemy")
    )


def _task() -> Task:
    return Task(id="chain-task", name="t", targets=[TaskTarget(name="gameA")])


async def test_processor_chain_reaches_storage(components) -> None:
    result = await _pipeline()._execute_via_dag(_task())

    assert result.success, result.errors
    assert result.storage_count == 1
    assert [output.processor_name for output in result.process_results] == ["first", "second"]
    assert _MemoryStorage.saved[0].data == {"name": "gameA", "steps": ["first", "second"]}


async def test_dag_and_legacy_engines_store_the_same_records(components) -> None:
    await _pipeline()._execute_via_dag(_task())
    dag_records = _MemoryStorage.saved

    _MemoryStorage.saved = []
    await _pipeline()._execute_legacy(_task())
    legacy_records = _MemoryStorage.saved

    assert [r.key for r in dag_records] == [r.key for r in legacy_records]
    assert [r.data for r in dag_records] == [r.data for r in legacy_records]
    assert [r.source for r in dag_records] == [r.source for r in legacy_records]


async def test_failed_output_is_not_passed_to_next_processor(components) -> None:
    def processor(node_id: str, config: dict[str, Any]) -> NodeSpec:
        return NodeSpec(
            node_id,
            "processor",
            "_chain_step",
            config,
            [PortSpec("records")],
            [PortSpec("records")],
            set(),
        )

    dag = DAG(
        name="chain_fail",
        nodes=[
            NodeSpec("src", "collector", "_chain_src", {}, [], [PortSpec("records")], set()),
            processor("bad", {"tag": "bad", "fail": True}),
            processor("next", {"tag": "next"}),
        ],
        edges=[
            Edge("src", "records", "bad", "records"),
            Edge("bad", "records", "next", "records"),
        ],
    )

    result = await DAGExecutor().execute(_task(), dag)

    runs = {run["node_id"]: run for run in result.node_runs}
    assert runs["next"]["input_count"] == 1
    assert runs["next"]["output_counts"] == {"records": 0}
