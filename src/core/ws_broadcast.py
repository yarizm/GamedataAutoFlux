"""核心层的 WebSocket 广播端口。

core / reporting 等业务代码需要向前端推送任务事件与报告进度，但不允许
反向 import Web 层（`src.web.routes.ws.manager`）。Web 层在启动时通过
`set_broadcaster` 注册真正的发送函数，业务侧只面向本模块；未注册时
广播为 no-op（嵌入/单测场景），错误一律吞掉不影响主流程。
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from loguru import logger

Broadcaster = Callable[[dict[str, Any]], Awaitable[None]]

_broadcaster: Broadcaster | None = None


def set_broadcaster(broadcaster: Broadcaster | None) -> None:
    """注册/清除广播函数（Web 层 lifespan 调用）。"""
    global _broadcaster
    _broadcaster = broadcaster


def get_broadcaster() -> Broadcaster | None:
    return _broadcaster


async def broadcast_ws(payload: dict[str, Any]) -> None:
    """尽力而为的广播：未注册或失败均不影响调用方主流程。"""
    if _broadcaster is None:
        return
    try:
        await _broadcaster(payload)
    except Exception as exc:
        logger.debug(f"WebSocket broadcast failed: {exc}")
