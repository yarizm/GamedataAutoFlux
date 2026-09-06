"""Worker claim REST 协议类型（Scheduler ↔ Worker Agent 跨进程契约）。

`POST /api/workers/{id}/claim-task` 的响应体 historically 是匿名
`dict[str, Any]`；本模块给它一个静态契约（TypedDict，运行时仍是普通
dict，线上格式零变化），pyright 校验构造侧的键名与值类型。

响应有三种形态：
- ``None``：队列空；
- blocked：``claim_status="blocked"``，只有 claim_reason / blocked_sessions；
- claimed：完整载荷（task 快照 + pipeline/graph 执行定义 + 恢复上下文）。

``payload_version="2"`` 表示执行定义优先读 ``graph``（完整 DAG，
含条件边/拓扑），旧 Worker 回退读 ``pipeline``（三段式投影）。
"""

from __future__ import annotations

from typing import Any, TypedDict


class WorkerClaimPayload(TypedDict, total=False):
    """claim 响应载荷（claimed 必含 task_id/task/payload_version；
    blocked 只含 claim_status/claim_reason/blocked_sessions）。"""

    task_id: str
    claim_status: str  # "claimed" | "blocked" | "no_task"
    claim_reason: str
    blocked_sessions: list[dict[str, Any]]
    # Task.to_storage_payload()：完整任务快照（由 Task 模型产出，形状随模型演进）
    task: dict[str, Any]
    # Pipeline.to_config()：三段式投影（旧 Worker 回退执行定义）
    pipeline: dict[str, Any] | None
    # DAG.to_storage()：完整执行图（payload_version="2" 的首选执行定义）
    graph: dict[str, Any] | None
    payload_version: str
    latest_checkpoint: dict[str, Any] | None
    collector_metadata: dict[str, Any]
    session_diagnostics: dict[str, Any]
    session_reserved: bool
    recovery: dict[str, Any]
