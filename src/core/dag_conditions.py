"""DAG 条件边预置谓词。"""
from __future__ import annotations

from typing import Any, Callable

from src.core.dag_nodes import NodeContext


def _records_port(output: dict[str, Any]) -> list:
    records = output.get("records")
    if isinstance(records, list):
        return records
    return []


def on_success(output: dict[str, Any], success: bool, ctx: NodeContext) -> bool:
    return success


def on_failure(output: dict[str, Any], success: bool, ctx: NodeContext) -> bool:
    return not success


def on_nonempty(output: dict[str, Any], success: bool, ctx: NodeContext) -> bool:
    return success and len(_records_port(output)) > 0


def on_empty(output: dict[str, Any], success: bool, ctx: NodeContext) -> bool:
    return success and len(_records_port(output)) == 0


CONDITION_PREDICATES: dict[str, Callable] = {
    "on_success": on_success,
    "on_failure": on_failure,
    "on_nonempty": on_nonempty,
    "on_empty": on_empty,
}


def resolve_condition(name: str | None) -> Callable | None:
    if name is None:
        return None
    return CONDITION_PREDICATES.get(name)
