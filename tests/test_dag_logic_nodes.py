"""DAG 内置逻辑节点：condition / filter / transform 的语义与配置校验。"""
from __future__ import annotations

import pytest

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.dag import DAG, Edge, NodeSpec, PortSpec
from src.core.dag_executor import DAGExecutor, validate_dag
from src.core.dag_logic import (
    compare,
    logic_node_catalog,
    logic_node_ports,
    resolve_path,
    validate_logic_config,
)
from src.core.registry import registry
from src.core.task import Task, TaskTarget
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput

EXECUTED: list[str] = []


class _ParamCollector(BaseCollector):
    """每个 target 产出一条记录，data 直接取 target.params；fail=True 的 target 失败。"""

    async def collect(self, target: CollectTarget) -> CollectResult:
        if target.params.get("fail"):
            return CollectResult(target=target, success=False, error="boom", data=None)
        return CollectResult(target=target, data={"name": target.name, **target.params})


class _AlwaysOkCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(target=target, data={"name": target.name, "src": "backup"})


class _RecorderProcessor(BaseProcessor):
    async def setup(self) -> None:
        EXECUTED.append(self.config.get("tag", "proc"))

    async def process(self, input_data: ProcessInput) -> ProcessOutput:
        return ProcessOutput(
            success=True,
            data=input_data.data,
            metadata=input_data.metadata,
            processor_name=self.config.get("tag", "proc"),
        )


@pytest.fixture
def components():
    snap = registry.snapshot()
    EXECUTED.clear()
    registry.register("collector", "_lg_param")(_ParamCollector)
    registry.register("collector", "_lg_ok")(_AlwaysOkCollector)
    registry.register("processor", "_lg_rec")(_RecorderProcessor)
    try:
        yield
    finally:
        registry.restore(snap)


def _collector(node_id="src", *, optional_input=False, component="_lg_param"):
    ports_in = [PortSpec("records", required=False)] if optional_input else []
    return NodeSpec(node_id, "collector", component, {}, ports_in, [PortSpec("records")], set())


def _logic(node_id, component, config, policy=None):
    ports_in, ports_out = logic_node_ports(component, config)
    return NodeSpec(node_id, "logic", component, config, ports_in, ports_out, set(), policy=policy or {})


def _processor(node_id):
    return NodeSpec(
        node_id, "processor", "_lg_rec", {"tag": node_id},
        [PortSpec("records")], [PortSpec("records")], set(),
    )


def _task(*targets):
    return Task(
        name="t",
        targets=[TaskTarget(name=name, params=params) for name, params in targets],
    )


def _runs(result):
    return {run["node_id"]: run for run in result.node_runs}


def _processed(result, tag):
    return [r.data for r in result.process_results if r.processor_name == tag]


# ── 比较运算与取值 ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("actual", "operator", "expected", "outcome"),
    [
        (90, "gte", "80", True),
        ("90", "gt", "100", False),  # 两侧都能转数字时按数值比较
        ("2026-10-01", "gt", "2026-09-30", True),  # 非数值按文本比较（ISO 日期可用）
        (5, "eq", "5", True),
        ("007", "eq", "7", False),  # 两侧都是字符串时按文本比较
        (True, "eq", "true", True),
        (None, "eq", "", True),
        ("Hello World", "contains", "World", True),
        (["rpg", "indie"], "contains", "indie", True),
        ({"a": 1}, "contains", "a", True),
        ("steam", "in", "steam, taptap", True),
        ("qimai", "not_in", ["steam", "taptap"], True),
        ("", "empty", None, True),
        ([], "not_empty", None, False),
        ("prefix-x", "starts_with", "prefix", True),
        ("file.png", "ends_with", ".png", True),
        (None, "gt", 1, False),
    ],
)
def test_compare_semantics(actual, operator, expected, outcome):
    assert compare(actual, operator, expected) is outcome


def test_resolve_path_defaults_to_data_root():
    view = {"data": {"title": "x", "stats": {"likes": 3}, "tags": ["a", "b"]}, "target": {"name": "g"}}
    assert resolve_path(view, "title") == "x"
    assert resolve_path(view, "data.stats.likes") == 3
    assert resolve_path(view, "stats.likes") == 3
    assert resolve_path(view, "tags.1") == "b"
    assert resolve_path(view, "target.name") == "g"
    assert resolve_path(view, "missing.path") is None


# ── condition ───────────────────────────────────────────────────────


def _condition_dag(config):
    return DAG(
        name="cond",
        nodes=[
            _collector(),
            _logic("cond", "condition", config),
            _processor("hot"),
            _processor("cold"),
        ],
        edges=[
            Edge("src", "records", "cond", "records"),
            Edge("cond", "high", "hot", "records"),
            Edge("cond", "else", "cold", "records"),
        ],
    )


@pytest.mark.asyncio
async def test_condition_routes_whole_batch_to_first_matching_case(components):
    config = {"cases": [{
        "id": "high",
        "conditions": [{"source": "record", "field": "score", "quantifier": "any", "operator": "gte", "value": 80}],
    }]}
    result = await DAGExecutor().execute(
        _task(("a", {"score": 90}), ("b", {"score": 10})), _condition_dag(config)
    )

    runs = _runs(result)
    assert result.success
    assert runs["cond"]["branch"] == "high"
    assert runs["cond"]["output_counts"] == {"high": 2, "else": 0}
    assert runs["cold"]["status"] == "skipped"
    # 整批记录（类型不变）流向命中的分支，下游 processor 照常消费
    assert sorted(item["name"] for item in _processed(result, "hot")) == ["a", "b"]


@pytest.mark.asyncio
async def test_condition_all_quantifier_falls_through_to_else(components):
    config = {"cases": [{
        "id": "high",
        "conditions": [{"field": "score", "quantifier": "all", "operator": "gte", "value": 80}],
    }]}
    result = await DAGExecutor().execute(
        _task(("a", {"score": 90}), ("b", {"score": 10})), _condition_dag(config)
    )

    assert _runs(result)["cond"]["branch"] == "else"
    assert EXECUTED == ["cold"]


@pytest.mark.asyncio
async def test_condition_first_match_wins_and_or_operator(components):
    config = {"cases": [
        {
            "id": "high",
            "logical_operator": "or",
            "conditions": [
                {"field": "score", "operator": "gt", "value": 1000},
                {"source": "aggregate", "field": "count", "operator": "eq", "value": 1},
            ],
        },
        {"id": "also", "conditions": [{"source": "aggregate", "field": "total", "operator": "gte", "value": 1}]},
    ]}
    result = await DAGExecutor().execute(_task(("a", {"score": 5})), _condition_dag(config))

    assert _runs(result)["cond"]["branch"] == "high"
    assert _runs(result)["cond"]["output_counts"] == {"high": 1, "also": 0, "else": 0}


@pytest.mark.asyncio
async def test_condition_on_empty_input_can_drive_fallback(components):
    """上游全失败（0 条可用记录）→ else 分支跑兜底采集。"""
    config = {"cases": [{
        "id": "has_data",
        "conditions": [{"source": "aggregate", "field": "count", "operator": "gt", "value": 0}],
    }]}
    dag = DAG(
        name="cond_fallback",
        nodes=[
            _collector("primary"),
            _logic("cond", "condition", config),
            _processor("normal"),
            _collector("backup", optional_input=True, component="_lg_ok"),
            _processor("rescued"),
        ],
        edges=[
            Edge("primary", "records", "cond", "records"),
            Edge("cond", "has_data", "normal", "records"),
            Edge("cond", "else", "backup", "records"),
            Edge("backup", "records", "rescued", "records"),
        ],
    )
    result = await DAGExecutor().execute(_task(("a", {"fail": True})), dag)

    runs = _runs(result)
    assert runs["cond"]["branch"] == "else"
    assert runs["normal"]["status"] == "skipped"
    assert runs["backup"]["status"] == "succeeded"
    assert EXECUTED == ["rescued"]


# ── filter ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_filter_conditions_dedupe_sort_limit_and_rejected_port(components):
    config = {
        "conditions": [{"field": "score", "operator": "gte", "value": 50}],
        "dedupe_by": ["data.app_id"],
        "sort_by": "score",
        "sort_order": "desc",
        "limit": 2,
    }
    dag = DAG(
        name="filter",
        nodes=[
            _collector(),
            _logic("flt", "filter", config),
            _processor("kept"),
            _processor("dropped"),
        ],
        edges=[
            Edge("src", "records", "flt", "records"),
            Edge("flt", "records", "kept", "records"),
            Edge("flt", "rejected", "dropped", "records"),
        ],
    )
    task = _task(
        ("low", {"score": 10, "app_id": 1}),
        ("mid", {"score": 60, "app_id": 2}),
        ("top", {"score": 95, "app_id": 3}),
        ("top_dup", {"score": 99, "app_id": 3}),
        ("high", {"score": 80, "app_id": 4}),
        ("broken", {"fail": True}),
    )
    result = await DAGExecutor().execute(task, dag)

    # top 与 top_dup 同 app_id，按输入顺序保留先出现的 top；再按分数降序取前 2
    assert [item["name"] for item in _processed(result, "kept")] == ["top", "high"]
    runs = _runs(result)
    assert runs["flt"]["output_counts"] == {"records": 2, "rejected": 2}
    # rejected 包括不满足条件的 low 与失败记录 broken；processor 只吃成功记录
    assert [item["name"] for item in _processed(result, "dropped")] == ["low"]


# ── transform ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_transform_renders_typed_fields(components):
    config = {
        "fields": [
            {"key": "title_upper", "template": "{{ data.title | upper }}"},
            {"key": "likes", "template": "{{ data.raw_likes }}", "type": "number"},
            {"key": "is_hot", "template": "{{ data.raw_likes | int > 100 }}", "type": "boolean"},
            {"key": "label", "template": "{{ target.name }}#{{ index }}"},
        ],
        "keep_original": True,
        "drop_fields": ["raw_likes"],
    }
    dag = DAG(
        name="transform",
        nodes=[_collector(), _logic("tf", "transform", config), _processor("out")],
        edges=[
            Edge("src", "records", "tf", "records"),
            Edge("tf", "records", "out", "records"),
        ],
    )
    result = await DAGExecutor().execute(
        _task(("g1", {"title": "abc", "raw_likes": "150"}), ("g2", {"fail": True})), dag
    )

    assert result.success
    [data] = _processed(result, "out")
    assert data == {
        "name": "g1",
        "title": "abc",
        "title_upper": "ABC",
        "likes": 150,
        "is_hot": True,
        "label": "g1#0",
    }
    # 失败记录原样透传
    assert _runs(result)["tf"]["output_counts"] == {"records": 2}


@pytest.mark.asyncio
async def test_transform_without_original_and_sandbox_blocks_power(components):
    replace = DAG(
        name="transform_replace",
        nodes=[
            _collector(),
            _logic("tf", "transform", {"fields": [{"key": "only", "template": "{{ data.title }}"}], "keep_original": False}),
            _processor("out"),
        ],
        edges=[Edge("src", "records", "tf", "records"), Edge("tf", "records", "out", "records")],
    )
    result = await DAGExecutor().execute(_task(("g", {"title": "x"})), replace)
    assert _processed(result, "out") == [{"only": "x"}]

    bomb = DAG(
        name="transform_bomb",
        nodes=[
            _collector(),
            _logic("tf", "transform", {"fields": [{"key": "big", "template": "{{ 9 ** 9999999 }}"}]}),
        ],
        edges=[Edge("src", "records", "tf", "records")],
    )
    result = await DAGExecutor().execute(_task(("g", {"title": "x"})), bomb)
    assert not result.success
    assert "not allowed" in _runs(result)["tf"]["error"]


# ── 配置校验与目录 ──────────────────────────────────────────────────


def test_validate_logic_config_reports_problems():
    assert validate_logic_config("condition", {"cases": [{"id": "ok", "conditions": [{"field": "x", "operator": "empty"}]}]}) == []
    assert validate_logic_config("condition", {"cases": []})
    assert validate_logic_config("condition", {"cases": [{"id": "else", "conditions": [{"field": "x", "operator": "empty"}]}]})
    assert validate_logic_config("condition", {"cases": [
        {"id": "a", "conditions": [{"field": "x", "operator": "empty"}]},
        {"id": "a", "conditions": [{"field": "x", "operator": "empty"}]},
    ]})
    assert validate_logic_config("condition", {"cases": [{"id": "a", "conditions": [{"field": "x", "operator": "like"}]}]})
    assert validate_logic_config("condition", {"cases": [{"id": "a", "conditions": [{"field": "x", "operator": "eq"}]}]})  # 缺 value
    assert validate_logic_config("condition", {"cases": [{"id": "a", "conditions": [{"source": "aggregate", "field": "avg", "operator": "gt", "value": 1}]}]})
    assert validate_logic_config("filter", {"limit": -1})
    assert validate_logic_config("filter", {"conditions": [{"source": "aggregate", "field": "count", "operator": "gt", "value": 0}]})
    assert validate_logic_config("transform", {"fields": [{"key": "a", "template": "{{ unclosed "}]})
    assert validate_logic_config("transform", {"fields": []})
    assert validate_logic_config("transform", {"fields": [{"key": "bad key", "template": "x"}]})
    assert validate_logic_config("loop", {})


def test_condition_ports_follow_cases_and_validation_uses_them(components):
    config = {"cases": [{"id": "a", "conditions": [{"field": "x", "operator": "empty"}]}]}
    _, ports_out = logic_node_ports("condition", config)
    assert [p.name for p in ports_out] == ["a", "else"]

    dag = DAG(
        name="bad_port",
        nodes=[_collector(), _logic("cond", "condition", config), _processor("p")],
        edges=[
            Edge("src", "records", "cond", "records"),
            Edge("cond", "missing_case", "p", "records"),
        ],
    )
    assert any("undeclared output port" in message for message in validate_dag(dag))

    broken = DAG(
        name="bad_logic",
        nodes=[_collector(), _logic("cond", "condition", {"cases": []})],
        edges=[Edge("src", "records", "cond", "records")],
    )
    assert any("at least one case" in message for message in validate_dag(broken))


def test_logic_catalog_entries():
    catalog = {item["component"]: item for item in logic_node_catalog()}
    assert set(catalog) == {"condition", "filter", "transform"}
    assert all(item["type"] == "logic" and item["owner"] == "core" for item in catalog.values())
    assert [p["name"] for p in catalog["condition"]["ports_out"]] == ["case_1", "else"]
    assert [p["name"] for p in catalog["filter"]["ports_out"]] == ["records", "rejected"]
    assert validate_logic_config("condition", catalog["condition"]["default_config"]) == []
    assert validate_logic_config("filter", catalog["filter"]["default_config"]) == []
    assert validate_logic_config("transform", catalog["transform"]["default_config"]) == []
