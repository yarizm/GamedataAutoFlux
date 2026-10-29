"""DAG 引擎：就绪队列调度与节点级策略（重试 / 超时 / on_error）。"""
from __future__ import annotations

import asyncio
import time

import pytest

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.dag import DAG, Edge, NodeSpec, PortSpec
from src.core.dag_executor import DAGExecutor, validate_dag, validate_dag_detailed
from src.core.dag_policy import parse_node_policy, validate_node_policy
from src.core.registry import registry
from src.core.task import Task, TaskTarget
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.storage.base import BaseStorage, QueryResult

EXECUTED: list[str] = []
SETUP_CALLS: dict[str, int] = {}
GATE: dict[str, asyncio.Event] = {}


class _OkCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(
            target=target,
            data={"src": self.config.get("src", "ok"), "name": target.name},
            success=True,
        )


class _FlakyCollector(BaseCollector):
    """setup 前 fail_times 次抛异常（节点级失败，而不是单 target 失败）。"""

    async def setup(self, config=None) -> None:
        key = self.config.get("key", "flaky")
        SETUP_CALLS[key] = SETUP_CALLS.get(key, 0) + 1
        if SETUP_CALLS[key] <= int(self.config.get("fail_times", 0)):
            raise RuntimeError(f"flaky boom #{SETUP_CALLS[key]}")

    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, data={"src": "flaky", "name": target.name})


class _SlowCollector(BaseCollector):
    async def setup(self, config=None) -> None:
        await asyncio.sleep(float(self.config.get("delay", 0)))

    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, data={"src": "slow", "name": target.name})


class _GateCollector(BaseCollector):
    """等 GATE 被下游另一分支打开才返回：按层同步调度会在这里死锁。"""

    async def setup(self, config=None) -> None:
        await GATE["open"].wait()

    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, data={"src": "gate", "name": target.name})


class _RecorderProcessor(BaseProcessor):
    async def setup(self) -> None:
        EXECUTED.append(self.config.get("tag", "proc"))
        if self.config.get("open_gate"):
            GATE["open"].set()

    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        return ProcessOutput(
            success=True,
            data=input_data.data,
            metadata=input_data.metadata,
            processor_name=self.config.get("tag", "proc"),
        )


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


@pytest.fixture
def components():
    snap = registry.snapshot()
    EXECUTED.clear()
    SETUP_CALLS.clear()
    GATE.clear()
    GATE["open"] = asyncio.Event()
    _MemStorage.saved = []
    registry.register("collector", "_pe_ok")(_OkCollector)
    registry.register("collector", "_pe_flaky")(_FlakyCollector)
    registry.register("collector", "_pe_slow")(_SlowCollector)
    registry.register("collector", "_pe_gate")(_GateCollector)
    registry.register("processor", "_pe_rec")(_RecorderProcessor)
    registry.register("storage", "sqlalchemy")(_MemStorage)
    try:
        yield
    finally:
        registry.restore(snap)


def _collector(node_id, component="_pe_ok", *, config=None, policy=None, optional_input=False):
    ports_in = [PortSpec("records", required=False)] if optional_input else []
    return NodeSpec(
        node_id, "collector", component, dict(config or {}),
        ports_in, [PortSpec("records")], set(), policy=dict(policy or {}),
    )


def _processor(node_id, *, config=None, policy=None):
    return NodeSpec(
        node_id, "processor", "_pe_rec", {"tag": node_id, **(config or {})},
        [PortSpec("records")], [PortSpec("records")], set(), policy=dict(policy or {}),
    )


def _storage(node_id):
    return NodeSpec(node_id, "storage", "sqlalchemy", {}, [PortSpec("records")], [], set())


def _task():
    return Task(name="t", targets=[TaskTarget(name="gameA")])


def _runs(result):
    return {run["node_id"]: run for run in result.node_runs}


def _sources(result):
    return {
        r.data.get("src")
        for r in result.process_results
        if isinstance(r.data, dict)
    }


# ── 就绪队列调度 ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fast_branch_downstream_does_not_wait_for_slow_sibling(components):
    """gate 要等 fast 分支的下游 opener 打开才能完成；按层同步会死锁。"""
    dag = DAG(
        name="ready_queue",
        nodes=[
            _collector("gate", "_pe_gate"),
            _collector("fast"),
            _processor("opener", config={"open_gate": True}),
            _storage("store"),
        ],
        edges=[
            Edge("fast", "records", "opener", "records"),
            Edge("opener", "records", "store", "records"),
            Edge("gate", "records", "store", "records"),
        ],
    )
    result = await asyncio.wait_for(DAGExecutor().execute(_task(), dag), timeout=5)

    assert result.success
    assert EXECUTED == ["opener"]
    # store 等两路上游都到齐才执行：两份采集结果都被存下
    assert len(_MemStorage.saved) == 2
    assert {run["status"] for run in result.node_runs} == {"succeeded"}


@pytest.mark.asyncio
async def test_node_runs_record_counts_and_timing(components):
    dag = DAG(
        name="trace",
        nodes=[_collector("src"), _processor("proc"), _storage("store")],
        edges=[
            Edge("src", "records", "proc", "records"),
            Edge("proc", "records", "store", "records"),
        ],
    )
    task = Task(name="t", targets=[TaskTarget(name="a"), TaskTarget(name="b")])
    result = await DAGExecutor().execute(task, dag)

    runs = _runs(result)
    assert runs["src"]["output_counts"] == {"records": 2}
    assert runs["proc"]["input_count"] == 2
    assert runs["store"]["output_counts"] == {"stored": 2}
    assert all(run["elapsed_ms"] is not None for run in runs.values())
    assert all(run["attempts"] == 1 for run in runs.values())


@pytest.mark.asyncio
async def test_external_cancel_cancels_running_nodes(components):
    dag = DAG(name="cancel", nodes=[_collector("slow", "_pe_slow", config={"delay": 30})], edges=[])
    job = asyncio.create_task(DAGExecutor().execute(_task(), dag))
    await asyncio.sleep(0.05)
    job.cancel()
    started = time.perf_counter()
    with pytest.raises(asyncio.CancelledError):
        await job
    assert time.perf_counter() - started < 2


# ── 重试与超时 ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_retry_until_success(components):
    phases: list[tuple[str, str]] = []

    async def on_event(task_id, node_spec, phase, *, out=None, error=None, trace=None):
        phases.append((node_spec.id, phase))

    dag = DAG(
        name="retry_ok",
        nodes=[_collector(
            "flaky", "_pe_flaky",
            config={"key": "retry_ok", "fail_times": 2},
            policy={"max_retries": 2},
        )],
        edges=[],
    )
    result = await DAGExecutor().execute(_task(), dag, on_event=on_event)

    assert result.success
    run = _runs(result)["flaky"]
    assert run["status"] == "succeeded"
    assert run["attempts"] == 3
    assert run["max_attempts"] == 3
    assert [phase for _, phase in phases] == ["start", "retry", "retry", "complete"]


@pytest.mark.asyncio
async def test_retry_exhausted_marks_node_failed(components):
    dag = DAG(
        name="retry_fail",
        nodes=[_collector(
            "flaky", "_pe_flaky",
            config={"key": "retry_fail", "fail_times": 99},
            policy={"max_retries": 1},
        )],
        edges=[],
    )
    result = await DAGExecutor().execute(_task(), dag)

    assert not result.success
    run = _runs(result)["flaky"]
    assert run["status"] == "failed"
    assert run["attempts"] == 2
    assert "flaky boom #2" in run["error"]
    assert SETUP_CALLS["retry_fail"] == 2


@pytest.mark.asyncio
async def test_retry_wait_releases_concurrency_slot(components):
    """重试等待期间不占并发名额：并发 1 时，另一个节点能在等待窗口里跑完。"""
    completed: list[str] = []

    async def on_event(task_id, node_spec, phase, *, out=None, error=None, trace=None):
        if phase == "complete":
            completed.append(node_spec.id)

    dag = DAG(
        name="retry_slot",
        nodes=[
            _collector(
                "a_flaky", "_pe_flaky",
                config={"key": "retry_slot", "fail_times": 1},
                policy={"max_retries": 1, "retry_interval_seconds": 0.3},
            ),
            _collector("b_ok"),
        ],
        edges=[],
    )
    result = await DAGExecutor().execute(
        _task(), dag, semaphore=asyncio.Semaphore(1), on_event=on_event
    )

    assert result.success
    assert completed == ["b_ok", "a_flaky"]


@pytest.mark.asyncio
async def test_timeout_fails_node_with_typed_code(components):
    dag = DAG(
        name="timeout",
        nodes=[_collector("slow", "_pe_slow", config={"delay": 10}, policy={"timeout_seconds": 0.2})],
        edges=[],
    )
    started = time.perf_counter()
    result = await DAGExecutor().execute(_task(), dag)

    assert time.perf_counter() - started < 3
    assert not result.success
    assert result.error_code == "node_timeout"
    run = _runs(result)["slow"]
    assert run["status"] == "failed"
    assert run["error_code"] == "node_timeout"
    assert "timed out" in run["error"]


# ── on_error ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_on_error_fail_skips_dependents_but_keeps_other_branches(components):
    dag = DAG(
        name="fail_default",
        nodes=[
            _collector("boom", "_pe_flaky", config={"key": "fail_default", "fail_times": 99}),
            _collector("ok", config={"src": "ok"}),
            _processor("merge"),
            _processor("after"),
            _collector("rescue", config={"src": "rescue"}, optional_input=True),
            _processor("rescue_proc"),
        ],
        edges=[
            Edge("boom", "records", "merge", "records"),
            Edge("ok", "records", "merge", "records"),
            Edge("boom", "records", "after", "records"),
            Edge("boom", "records", "rescue", "records", condition="on_failure"),
            Edge("rescue", "records", "rescue_proc", "records"),
        ],
    )
    result = await DAGExecutor().execute(_task(), dag)

    runs = _runs(result)
    assert not result.success
    assert runs["boom"]["status"] == "failed"
    # 只依赖失败节点的下游跳过，不再拿空数据空跑
    assert runs["after"]["status"] == "skipped"
    assert "after" not in EXECUTED
    # 还有其他激活入边的节点照常执行
    assert "merge" in EXECUTED
    # on_failure 条件边照常激活故障转移
    assert runs["rescue"]["status"] == "succeeded"
    assert _sources(result) == {"ok", "rescue"}


@pytest.mark.asyncio
async def test_on_error_stop_prevents_new_nodes(components):
    dag = DAG(
        name="stop",
        nodes=[
            _collector("boom", "_pe_flaky", config={"key": "stop", "fail_times": 99}, policy={"on_error": "stop"}),
            _collector("slow", "_pe_slow", config={"delay": 0.3}),
            _processor("later"),
        ],
        edges=[Edge("slow", "records", "later", "records")],
    )
    result = await DAGExecutor().execute(_task(), dag)

    runs = _runs(result)
    assert not result.success
    assert runs["slow"]["status"] == "succeeded"  # 已在跑的节点跑完
    assert runs["later"]["status"] == "not_run"
    assert "later" not in EXECUTED
    assert any("on_error=stop" in error for error in result.errors)


@pytest.mark.asyncio
async def test_on_error_continue_passes_empty_output_and_keeps_run_green(components):
    dag = DAG(
        name="continue",
        nodes=[
            _collector("boom", "_pe_flaky", config={"key": "continue", "fail_times": 99}, policy={"on_error": "continue"}),
            _processor("after"),
            _collector("ok"),
            _storage("store"),
        ],
        edges=[
            Edge("boom", "records", "after", "records"),
            Edge("ok", "records", "store", "records"),
        ],
    )
    result = await DAGExecutor().execute(_task(), dag)

    runs = _runs(result)
    assert result.success
    assert result.errors == []
    assert runs["boom"]["status"] == "exception"
    assert runs["boom"]["error"]
    assert "after" in EXECUTED
    assert runs["after"]["input_count"] == 0


@pytest.mark.asyncio
async def test_on_error_branch_routes_to_error_port(components):
    dag = DAG(
        name="error_branch",
        nodes=[
            _collector("primary", "_pe_flaky", config={"key": "branch", "fail_times": 99}, policy={"on_error": "error_branch"}),
            _collector("backup", config={"src": "backup"}, optional_input=True),
            _processor("proc"),
        ],
        edges=[
            Edge("primary", "records", "proc", "records"),
            Edge("primary", "error", "backup", "records"),
            Edge("backup", "records", "proc", "records"),
        ],
    )
    result = await DAGExecutor().execute(_task(), dag)

    runs = _runs(result)
    assert result.success
    assert runs["primary"]["status"] == "exception"
    assert runs["primary"]["branch"] == "error"
    assert runs["backup"]["status"] == "succeeded"
    assert _sources(result) == {"backup"}


@pytest.mark.asyncio
async def test_error_port_is_inactive_when_node_succeeds(components):
    dag = DAG(
        name="error_branch_ok",
        nodes=[
            _collector("primary", policy={"on_error": "error_branch"}),
            _collector("backup", config={"src": "backup"}, optional_input=True),
            _processor("proc"),
        ],
        edges=[
            Edge("primary", "records", "proc", "records"),
            Edge("primary", "error", "backup", "records"),
        ],
    )
    result = await DAGExecutor().execute(_task(), dag)

    runs = _runs(result)
    assert result.success
    assert runs["backup"]["status"] == "skipped"
    assert _sources(result) == {"ok"}


# ── 校验 ────────────────────────────────────────────────────────────


def test_policy_validation_rejects_bad_values():
    assert validate_node_policy({}) == []
    assert validate_node_policy(None) == []
    assert validate_node_policy({"max_retries": 3, "retry_interval_seconds": 1.5, "timeout_seconds": 30, "on_error": "continue"}) == []
    assert validate_node_policy({"max_retries": 99})
    assert validate_node_policy({"max_retries": True})
    assert validate_node_policy({"timeout_seconds": 0})
    assert validate_node_policy({"on_error": "explode"})
    assert validate_node_policy({"retries": 1})  # 拼写错误的字段
    assert validate_node_policy("stop")
    assert parse_node_policy({"max_retries": 2}).max_attempts == 3
    assert parse_node_policy({"max_retries": 99}).max_attempts == 1  # 非法值回落默认


def test_dag_validation_reports_policy_and_error_port_issues():
    bad_policy = DAG(
        name="bad_policy",
        nodes=[_collector("c", policy={"on_error": "explode"})],
        edges=[],
    )
    assert any("policy" in message for message in validate_dag(bad_policy))

    undeclared_error_port = DAG(
        name="no_error_port",
        nodes=[_collector("c"), _processor("p")],
        edges=[Edge("c", "error", "p", "records")],
    )
    assert any("undeclared output port" in message for message in validate_dag(undeclared_error_port))

    unconnected = DAG(
        name="unconnected",
        nodes=[_collector("c", policy={"on_error": "error_branch"})],
        edges=[],
    )
    issues = validate_dag_detailed(unconnected)
    assert not [i for i in issues if i.severity == "error"]
    warning = next(i for i in issues if i.code == "error_branch_unconnected")
    assert warning.severity == "warning"
    assert warning.node_id == "c"
