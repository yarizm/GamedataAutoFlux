"""DAG 结构校验测试（修复清单 P1/P2：非法 DAG 在保存/运行前即可报错）。"""

from src.core.dag import DAG, Edge, NodeSpec, PortSpec
from src.core.dag_executor import validate_dag, validate_dag_detailed


def _codes(issues):
    return [i.code for i in issues]


def _collector(nid="c1", component="steam"):
    return NodeSpec(
        nid, "collector", component, {}, [], [PortSpec("records")], set()
    )


def _processor(nid="p1", component="dummy"):
    return NodeSpec(
        nid, "processor", component, {}, [PortSpec("records")], [PortSpec("records")], set()
    )


def _storage(nid="s1", component="local"):
    return NodeSpec(nid, "storage", component, {}, [PortSpec("records")], [], set())


def test_valid_pipeline_shaped_dag_has_no_errors():
    dag = DAG(
        name="ok",
        nodes=[_collector(), _processor(), _storage()],
        edges=[
            Edge("c1", "records", "p1", "records"),
            Edge("p1", "records", "s1", "records"),
        ],
    )
    issues = validate_dag_detailed(dag)
    assert [i for i in issues if i.severity == "error"] == []
    assert validate_dag(dag) == []


def test_empty_dag_rejected():
    issues = validate_dag_detailed(DAG(name="empty", nodes=[], edges=[]))
    assert "empty_dag" in _codes(issues)


def test_duplicate_node_id_rejected():
    dag = DAG(
        name="dup",
        nodes=[_collector("c1"), _collector("c1")],
        edges=[],
    )
    assert "duplicate_node_id" in _codes(validate_dag_detailed(dag))


def test_unknown_node_type_rejected():
    node = NodeSpec("x", "transformer", "whatever", {}, [], [], set())
    issues = validate_dag_detailed(DAG(name="t", nodes=[node], edges=[]))
    assert "invalid_node_type" in _codes(issues)


def test_self_loop_and_duplicate_edge_rejected():
    dag = DAG(
        name="bad_edges",
        nodes=[_collector(), _processor()],
        edges=[
            Edge("c1", "records", "p1", "records"),
            Edge("c1", "records", "p1", "records"),  # 重复边
            Edge("p1", "records", "p1", "records"),  # 自环
        ],
    )
    codes = _codes(validate_dag_detailed(dag))
    assert "duplicate_edge" in codes
    assert "self_loop" in codes


def test_undeclared_port_rejected_when_ports_declared():
    # 节点声明了端口但边引用了不存在的端口名
    dag = DAG(
        name="bad_port",
        nodes=[_collector(), _processor()],
        edges=[Edge("c1", "wrong_out", "p1", "records")],
    )
    codes = _codes(validate_dag_detailed(dag))
    assert "missing_output_port" in codes


def test_port_type_mismatch_rejected():
    c = NodeSpec(
        "c1", "collector", "steam", {}, [],
        [PortSpec("records", type_hint="records")], set(),
    )
    p = NodeSpec(
        "p1", "processor", "x", {},
        [PortSpec("records", type_hint="metrics")], [PortSpec("records")], set(),
    )
    dag = DAG(name="mismatch", nodes=[c, p], edges=[Edge("c1", "records", "p1", "records")])
    assert "port_type_mismatch" in _codes(validate_dag_detailed(dag))


def test_undeclared_ports_are_loose_when_not_declared():
    # 未声明端口的节点（UI 手绘常见）不做端口存在性硬校验
    c = NodeSpec("c1", "collector", "steam", {}, [], [], set())
    p = NodeSpec("p1", "processor", "x", {}, [], [], set())
    dag = DAG(name="loose", nodes=[c, p], edges=[Edge("c1", "records", "p1", "records")])
    assert [i for i in validate_dag_detailed(dag) if i.severity == "error"] == []


def test_unknown_condition_rejected():
    dag = DAG(
        name="bad_cond",
        nodes=[_collector(), _processor()],
        edges=[Edge("c1", "records", "p1", "records", condition="on_vibes")],
    )
    assert "unknown_condition" in _codes(validate_dag_detailed(dag))


def test_known_condition_accepted():
    dag = DAG(
        name="ok_cond",
        nodes=[_collector(), _processor()],
        edges=[Edge("c1", "records", "p1", "records", condition="on_success")],
    )
    assert [i for i in validate_dag_detailed(dag) if i.severity == "error"] == []


def test_no_collector_or_composite_rejected():
    dag = DAG(name="sink_only", nodes=[_storage()], edges=[])
    assert "no_source" in _codes(validate_dag_detailed(dag))


def test_composite_counts_as_source():
    node = NodeSpec("sub", "composite", "", {}, [], [], set(), subgraph_name="shared_sub")
    issues = validate_dag_detailed(
        DAG(name="comp", nodes=[node], edges=[]),
        subgraph_loader=lambda name: DAG(name, [_collector()], []),
    )
    assert [i for i in issues if i.severity == "error"] == []


def test_composite_requires_subgraph_name():
    node = NodeSpec("sub", "composite", "", {}, [], [], set(), subgraph_name=None)
    assert "composite_missing_subgraph" in _codes(
        validate_dag_detailed(DAG(name="comp", nodes=[node], edges=[]))
    )


def test_composite_subgraph_resolution_checked_with_loader():
    node = NodeSpec("sub", "composite", "", {}, [], [], set(), subgraph_name="missing")
    issues = validate_dag_detailed(
        DAG(name="comp", nodes=[node], edges=[]), subgraph_loader=lambda name: None
    )
    assert "composite_subgraph_unresolvable" in _codes(issues)


def test_conditional_only_required_input_is_warning_not_error():
    """故障转移模式：必需输入全部来自条件边 —— 合法，但值得告警。"""
    primary = _collector("c_primary")
    fallback = _collector("c_fallback")
    merge = _processor("p_merge")
    dag = DAG(
        name="failover",
        nodes=[primary, fallback, merge],
        edges=[
            Edge("c_primary", "records", "p_merge", "records", condition="on_nonempty"),
            Edge("c_fallback", "records", "p_merge", "records", condition="on_failure"),
        ],
    )
    issues = validate_dag_detailed(dag)
    assert [i for i in issues if i.severity == "error"] == []
    assert "conditional_required_input" in _codes(issues)


def test_unreachable_node_is_warning():
    """无入边的孤立节点视作源（合法）；真正不可达的是只被死区节点喂养的节点。"""
    p1 = _processor("p1")
    p2 = _processor("p2")
    dag = DAG(
        name="dead_zone",
        nodes=[_collector("c1"), p1, p2],
        edges=[Edge("p1", "records", "p2", "records")],  # p1/p2 均不可达自源
    )
    codes = _codes(validate_dag_detailed(dag))
    assert "unreachable_node" in codes
    # p1 的必需输入悬空 → error；p2 由 p1 喂，不悬空
    assert "dangling_required_input" in codes


def test_backward_compat_validate_dag_returns_error_messages_only():
    dag = DAG(
        name="compat",
        nodes=[_collector()],
        edges=[Edge("c1", "records", "ghost", "records")],
    )
    messages = validate_dag(dag)
    assert any("edge references missing node" in m for m in messages)
    assert all(isinstance(m, str) for m in messages)
