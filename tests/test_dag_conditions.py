# tests/test_dag_conditions.py
from src.core.dag_conditions import on_success, on_failure, on_nonempty, on_empty, CONDITION_PREDICATES
from src.core.dag_nodes import NodeContext
from src.core.task import Task


def test_on_success():
    ctx = NodeContext(inputs={}, task=Task(name="t"), config={})
    assert on_success({}, True, ctx) is True
    assert on_success({}, False, ctx) is False


def test_on_failure():
    ctx = NodeContext(inputs={}, task=Task(name="t"), config={})
    assert on_failure({}, False, ctx) is True
    assert on_failure({}, True, ctx) is False


def test_on_nonempty():
    ctx = NodeContext(inputs={}, task=Task(name="t"), config={})
    assert on_nonempty({"records": [1, 2]}, True, ctx) is True
    assert on_nonempty({"records": []}, True, ctx) is False


def test_on_empty():
    ctx = NodeContext(inputs={}, task=Task(name="t"), config={})
    assert on_empty({"records": []}, True, ctx) is True
    assert on_empty({"records": [1]}, True, ctx) is False


def test_condition_registry():
    assert "on_success" in CONDITION_PREDICATES
    assert "on_failure" in CONDITION_PREDICATES
    assert "on_nonempty" in CONDITION_PREDICATES
    assert "on_empty" in CONDITION_PREDICATES
