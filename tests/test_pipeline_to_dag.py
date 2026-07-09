# tests/test_pipeline_to_dag.py
from src.core.pipeline import Pipeline
from src.core.dag import pipeline_to_dag


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
