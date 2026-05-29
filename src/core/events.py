"""
事件总线

提供类型化的发布/订阅机制，解耦 Scheduler 与 WebSocket 广播、报告生成、告警等副作用。

使用示例:
    from src.core.events import event_bus, TaskUpdatedEvent

    async def on_task_updated(event: TaskUpdatedEvent):
        ...

    event_bus.on("task_updated", on_task_updated)
    await event_bus.emit("task_updated", TaskUpdatedEvent(...))
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

from loguru import logger


# ---------------------------------------------------------------------------
# 事件类型
# ---------------------------------------------------------------------------


@dataclass
class TaskUpdatedEvent:
    """任务状态更新事件"""

    task_id: str
    payload: dict[str, Any]
    status: str = ""
    pipeline_name: str = ""


@dataclass
class TaskCompletedEvent:
    """任务完成事件（成功或失败）"""

    task_id: str
    success: bool
    result: Any  # PipelineResult
    task: Any  # Task（避免循环导入，用 Any）
    pipeline: Any = None  # Pipeline 实例（供 hooks 访问 steps 等）
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# EventBus
# ---------------------------------------------------------------------------

Handler = Callable[[Any], Awaitable[None]]


class EventBus:
    """
    异步事件总线。

    - on(event_type, handler, priority=0) 注册处理器
    - off(event_type, handler) 移除处理器
    - emit(event_type, event) 异步发布事件

    同 priority 的处理器并发执行（asyncio.gather），
    不同 priority 按升序执行。
    单个处理器异常不会阻塞其他处理器。
    """

    def __init__(self) -> None:
        self._handlers: dict[str, list[tuple[int, Handler]]] = defaultdict(list)

    def on(self, event_type: str, handler: Handler, priority: int = 0) -> None:
        """注册事件处理器"""
        self._handlers[event_type].append((priority, handler))
        self._handlers[event_type].sort(key=lambda pair: pair[0])

    def off(self, event_type: str, handler: Handler) -> None:
        """移除事件处理器"""
        self._handlers[event_type] = [
            (pri, h) for pri, h in self._handlers[event_type] if h is not handler
        ]

    async def emit(self, event_type: str, event: Any) -> None:
        """
        异步发布事件。

        按 priority 分组，同组并发执行，组间顺序执行。
        单个处理器异常被捕获并记录，不会阻塞其他。
        """
        handlers = self._handlers.get(event_type, [])
        if not handlers:
            return

        # 按 priority 分组
        groups: dict[int, list[Handler]] = defaultdict(list)
        for pri, handler in handlers:
            groups[pri].append(handler)

        for pri in sorted(groups.keys()):
            group = groups[pri]
            results = await asyncio.gather(
                *[self._safe_call(handler, event) for handler in group],
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, Exception):
                    logger.opt(exception=result).error(
                        f"EventBus handler error in '{event_type}': {result}"
                    )

    @staticmethod
    async def _safe_call(handler: Handler, event: Any) -> None:
        """安全调用处理器，异常由 emit() 的 gather 统一收集并记录"""
        await handler(event)

    def clear(self, event_type: str | None = None) -> None:
        """清除处理器。event_type=None 时清除所有。"""
        if event_type is None:
            self._handlers.clear()
        else:
            self._handlers.pop(event_type, None)


# 模块级单例
event_bus = EventBus()
