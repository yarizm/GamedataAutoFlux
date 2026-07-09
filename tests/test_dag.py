# tests/test_dag.py
from src.core.dag import PortSpec, NodeSpec, Edge, DAG


def test_port_spec_defaults():
    p = PortSpec(name="records")
    assert p.required is True
    assert p.type_hint == ""


def test_node_spec_construction():
    n = NodeSpec(
        id="steam_src",
        type="collector",
        component="steam",
        config={"app_id": "123"},
        ports_in=[],
        ports_out=[PortSpec("records")],
        is_param_port=set(),
    )
    assert n.id == "steam_src"
    assert n.ports_out[0].name == "records"


def test_dag_to_storage_roundtrip():
    dag = DAG(
        name="test_dag",
        nodes=[
            NodeSpec("src", "collector", "steam", {}, [], [PortSpec("records")], set()),
            NodeSpec("store", "storage", "sqlalchemy", {}, [PortSpec("records")], [], set()),
        ],
        edges=[Edge("src", "records", "store", "records", None)],
        conditions={},
    )
    payload = dag.to_storage()
    assert payload["kind"] == "dag"
    assert payload["name"] == "test_dag"
    restored = DAG.from_storage(payload)
    assert restored.name == "test_dag"
    assert len(restored.nodes) == 2
    assert restored.edges[0].from_node == "src"
