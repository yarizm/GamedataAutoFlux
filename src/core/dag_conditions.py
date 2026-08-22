"""DAG 条件边预置谓词。

传入的 `success` 只表示"节点执行期间没有抛异常"，它不足以决定分支走向：
collector 的所有 target 都失败时一样不抛异常。所以谓词还要看 records 端口里
有没有**可用**记录（success=True 且 data 不为 None）。否则 on_failure 分支
永远不会触发，故障转移形同虚设。
"""
from __future__ import annotations

from typing import Any, Callable

from src.core.dag_nodes import NodeContext


def _is_usable(record: Any) -> bool:
    """CollectResult / ProcessOutput 要 success 且带 data；裸值一律算可用。"""
    success = getattr(record, "success", None)
    if success is None:
        return True
    return bool(success) and getattr(record, "data", None) is not None


def _usable_records(output: dict[str, Any]) -> list:
    records = output.get("records")
    if not isinstance(records, list):
        return []
    return [item for item in records if _is_usable(item)]


def _has_records_port(output: dict[str, Any]) -> bool:
    return isinstance(output.get("records"), list)


def on_success(output: dict[str, Any], success: bool, ctx: NodeContext) -> bool:
    if not success:
        return False
    # storage 这类没有 records 出端口的节点，只能以"未抛异常"为准
    if not _has_records_port(output):
        return True
    return len(_usable_records(output)) > 0


def on_failure(output: dict[str, Any], success: bool, ctx: NodeContext) -> bool:
    return not on_success(output, success, ctx)


def on_nonempty(output: dict[str, Any], success: bool, ctx: NodeContext) -> bool:
    return success and len(_usable_records(output)) > 0


def on_empty(output: dict[str, Any], success: bool, ctx: NodeContext) -> bool:
    return success and len(_usable_records(output)) == 0


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
