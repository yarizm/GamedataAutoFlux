# tests/test_pipeline_to_dag.py
from src.core.pipeline import Pipeline
from src.core.dag import dag_to_pipeline, pipeline_to_dag


def test_dag_to_pipeline_roundtrip_steps():
    p = (
        Pipeline("rt")
        .add_collector("steam")
        .add_processor("cleaner")
        .add_storage("sqlalchemy")
    )
    dag = pipeline_to_dag(p)
    back = dag_to_pipeline(dag)
    assert back.name == "rt"
    types = [s.step_type.value for s in back.steps]
    assert types == ["collector", "processor", "storage"]
    assert [s.component_name for s in back.steps] == ["steam", "cleaner", "sqlalchemy"]


def test_legacy_local_storage_normalizes_to_sqlalchemy():
    from src.core.dag import DAG, NodeSpec, PortSpec
    from src.storage.factory import normalize_storage_name

    assert normalize_storage_name("local") == "sqlalchemy"
    # add_storage 归一
    p = Pipeline("legacy").add_collector("steam").add_storage("local")
    assert p.steps[-1].component_name == "sqlalchemy"
    # pipeline_to_dag 归一
    dag = pipeline_to_dag(p)
    stores = [n for n in dag.nodes if n.type == "storage"]
    assert stores and stores[0].component == "sqlalchemy"
    # from_storage 归一
    restored = DAG.from_storage({
        "name": "g",
        "kind": "dag",
        "nodes": [
            {
                "id": "s",
                "type": "storage",
                "component": "local",
                "ports_in": [{"name": "records"}],
                "ports_out": [],
            }
        ],
        "edges": [],
    })
    assert restored.nodes[0].component == "sqlalchemy"


def test_pipeline_to_dag_three_stage():

    p = (
        Pipeline("steam_basic")
        .add_collector("steam", config={"app_id": "123"})
        .add_processor("cleaner")
        .add_storage("sqlalchemy")
    )
    dag = pipeline_to_dag(p)
    assert dag.name == "steam_basic"
    types = [n.type for n in dag.nodes]
    assert "collector" in types and "processor" in types and "storage" in types
    proc_id = next(n.id for n in dag.nodes if n.type == "processor")
    store_id = next(n.id for n in dag.nodes if n.type == "storage")
    assert any(e.to_node == proc_id and e.from_port == "records" for e in dag.edges)
    assert any(e.to_node == store_id and e.from_port == "records" for e in dag.edges)


def test_pipeline_to_dag_multi_collector_parallel():
    p = (
        Pipeline("multi")
        .add_collector("steam")
        .add_collector("taptap")
        .add_processor("cleaner")
        .add_storage("sqlalchemy")
    )
    dag = pipeline_to_dag(p)
    collectors = [n for n in dag.nodes if n.type == "collector"]
    assert len(collectors) == 2
    proc_id = next(n.id for n in dag.nodes if n.type == "processor")
    incoming = [e for e in dag.edges if e.to_node == proc_id]
    assert len(incoming) == 2


def test_pipeline_to_dag_no_processor_direct_storage():
    p = Pipeline("direct").add_collector("steam").add_storage("sqlalchemy")
    dag = pipeline_to_dag(p)
    store_id = next(n.id for n in dag.nodes if n.type == "storage")
    incoming = [e for e in dag.edges if e.to_node == store_id]
    assert len(incoming) == 1


def test_pipeline_to_dag_roundtrip_via_storage():
    """DAG.to_storage() 再 from_storage() 应保持一致。"""
    from src.core.dag import DAG
    p = Pipeline("roundtrip").add_collector("steam").add_processor("cleaner").add_storage("sqlalchemy")
    dag = pipeline_to_dag(p)
    payload = dag.to_storage()
    assert payload["name"] == "roundtrip"
    restored = DAG.from_storage(payload)
    assert restored.name == "roundtrip"
    assert len(restored.nodes) == len(dag.nodes)
    assert len(restored.edges) == len(dag.edges)
