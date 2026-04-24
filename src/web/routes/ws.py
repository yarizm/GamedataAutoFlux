"""
WebSocket 路由和连接管理器。

处理实时任务状态和日志的推送。
"""
from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from loguru import logger

router = APIRouter(tags=["websocket"])


class ConnectionManager:
    def __init__(self):
        # 活跃的 WebSocket 连接集合
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.debug(f"WebSocket client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.debug(f"WebSocket client disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict[str, Any]):
        """向所有连接的客户端广播消息"""
        if not self.active_connections:
            return

        dead_connections = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                # 客户端断开连接或发送失败
                dead_connections.append(connection)

        # 清理死连接
        for connection in dead_connections:
            self.disconnect(connection)


# 全局连接管理器单例
manager = ConnectionManager()


@router.websocket("/ws/tasks")
async def websocket_endpoint(websocket: WebSocket):
    """
    前端建立 WebSocket 连接的端点。
    主要用于服务器主动向前端推送任务更新 (task_update) 和大盘更新 (stats_update)。
    """
    await manager.connect(websocket)
    try:
        while True:
            # 目前主要是单向推送 (Server -> Client)，但为了保持连接，需要一个 await 循环
            # 可以接收客户端的 ping 或者过滤订阅请求
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as exc:
        logger.warning(f"WebSocket connection error: {exc}")
        manager.disconnect(websocket)
