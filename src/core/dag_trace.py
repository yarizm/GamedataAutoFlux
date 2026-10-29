"""把任务事件流折叠成 DAG 逐节点运行状态（对标 Dify 的节点执行记录视图）。

执行引擎在每个节点生命周期事件的 payload 里带 ``node_id`` 与 ``node``（NodeRun
快照）。这里按事件顺序取每个节点的最后一份快照；只有 ``status`` 没有快照的旧
事件按状态映射兜底。前端 ``pages/dag/runTrace.js`` 用同样的规则折叠 WebSocket
实时事件，两边结果一致。
"""

from __future__ import annotations

from typing import Any, Iterable

_STATUS_FROM_EVENT = {
    "started": "running",
    "succeeded": "succeeded",
    "failed": "failed",
    "retrying": "retrying",
    "skipped": "skipped",
}


def _event_payload(event: Any) -> dict[str, Any]:
    if isinstance(event, dict):
        payload = event.get("payload")
    else:
        payload = getattr(event, "payload", None)
    return payload if isinstance(payload, dict) else {}


def fold_dag_node_events(events: Iterable[Any]) -> dict[str, dict[str, Any]]:
    """按 seq 升序的事件 → {node_id: 节点运行状态}。"""
    nodes: dict[str, dict[str, Any]] = {}
    for event in events:
        payload = _event_payload(event)
        node_id = payload.get("node_id")
        if not isinstance(node_id, str) or not node_id:
            continue
        snapshot = payload.get("node")
        entry = nodes.setdefault(node_id, {"node_id": node_id})
        if isinstance(snapshot, dict):
            entry.update(snapshot)
            continue
        # 进度事件（status=progress）不改变节点状态
        status = _STATUS_FROM_EVENT.get(str(payload.get("status") or ""))
        if status is None:
            continue
        entry["status"] = status
        if payload.get("error"):
            entry["error"] = payload["error"]
        if payload.get("component"):
            entry.setdefault("component", payload["component"])
    return nodes
