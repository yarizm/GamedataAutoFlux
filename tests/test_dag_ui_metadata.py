from src.core.dag import DAG, NodeSpec, Edge, PortSpec


def test_dag_to_storage_preserves_node_ui_and_graph_ui():
    dag = DAG(
        name="with_ui",
        nodes=[
            NodeSpec(
                id="src",
                type="collector",
                component="steam",
                ports_out=[PortSpec("records")],
                ui={"x": 12, "y": 34, "label": "Steam源"},
            ),
        ],
        edges=[],
        ui={"zoom": 0.9, "pan": {"x": 1, "y": 2}},
    )
    payload = dag.to_storage()
    assert payload["nodes"][0]["ui"]["x"] == 12
    assert payload["nodes"][0]["ui"]["label"] == "Steam源"
    assert payload["ui"]["zoom"] == 0.9


def test_dag_from_storage_roundtrip_ui():
    payload = {
        "name": "rt",
        "kind": "dag",
        "nodes": [
            {
                "id": "a",
                "type": "collector",
                "component": "steam",
                "ports_in": [],
                "ports_out": [{"name": "records", "required": True, "type_hint": ""}],
                "ui": {"x": 100, "y": 200},
            }
        ],
        "edges": [],
        "ui": {"zoom": 1.2},
    }
    dag = DAG.from_storage(payload)
    assert dag.nodes[0].ui["x"] == 100
    assert dag.ui["zoom"] == 1.2
    back = dag.to_storage()
    assert back["nodes"][0]["ui"]["y"] == 200


def test_dag_ui_defaults_empty():
    dag = DAG(
        name="no_ui",
        nodes=[NodeSpec("src", "collector", "steam", {}, [], [PortSpec("records")], set())],
        edges=[],
    )
    assert dag.ui == {}
    assert dag.nodes[0].ui == {}
    payload = dag.to_storage()
    assert payload["ui"] == {}
    assert payload["nodes"][0]["ui"] == {}


def test_api_create_dag_preserves_ui(monkeypatch):
    """POST /api/dags must persist node.ui and graph.ui via request models."""
    from fastapi.testclient import TestClient

    from src.web.app import app

    dag_name = "__test_dag_ui_api__"
    payload = {
        "name": dag_name,
        "nodes": [
            {
                "id": "src",
                "type": "collector",
                "component": "steam",
                "ports_out": [{"name": "records"}],
                "ui": {"x": 42, "y": 77, "label": "源"},
            },
            {
                "id": "store",
                "type": "storage",
                "component": "sqlalchemy",
                "ports_in": [{"name": "records"}],
                "ui": {"x": 300, "y": 80},
            },
        ],
        "edges": [{"from": "src", "out": "records", "to": "store", "in": "records"}],
        "ui": {"zoom": 0.85},
    }
    with TestClient(app) as client:
        resp = client.post("/api/dags", json=payload)
        assert resp.status_code == 200, resp.text
        got = client.get(f"/api/dags/{dag_name}")
        assert got.status_code == 200
        data = got.json()
        src = next(n for n in data["nodes"] if n["id"] == "src")
        assert src["ui"]["x"] == 42
        assert src["ui"]["y"] == 77
        assert src["ui"]["label"] == "源"
        assert data["ui"]["zoom"] == 0.85
