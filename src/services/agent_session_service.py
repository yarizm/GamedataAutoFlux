"""
Agent 会话持久化服务

从 AgentService 提取，将聊天历史的数据库存取逻辑独立出来。
通过依赖注入获取 session_factory，不复用 Agent 自建 DB engine。
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    ChatMessage,
    FunctionMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from loguru import logger
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.storage.models import AgentSessionModel

_MSG_CLASSES: dict[str, type[BaseMessage]] = {
    "human": HumanMessage,
    "ai": AIMessage,
    "system": SystemMessage,
    "tool": ToolMessage,
    "function": FunctionMessage,
    "chat": ChatMessage,
}


def _utc_timestamp(dt: datetime) -> float:
    """将 naive（假设 UTC）或 aware datetime 转为 Unix timestamp。

    utcnow() 写入 DB 的是 naive UTC datetime，读出后需按 UTC 解释，
    而非依赖本地时区（datetime.timestamp() 的默认行为）。
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


class AgentSessionService:
    """Agent 会话持久化服务。

    管理聊天历史的数据库存取，JSON 迁移，超时清理。
    不持有会话内存状态 —— 由 AgentService 持有。
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        session_timeout: int = 3600,
        max_sessions: int = 50,
        old_persist_path: Path | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._session_timeout = session_timeout
        self._max_sessions = max_sessions
        self._old_persist_path = old_persist_path

    async def load_histories(
        self,
    ) -> tuple[
        dict[str, list[BaseMessage]],
        dict[str, float],
    ]:
        """
        从数据库加载所有会话历史。

        Returns
        -------
        tuple[dict[str, list[BaseMessage]], dict[str, float]]
            (histories, timestamps) — 会话 ID → 消息列表 和 会话 ID → 最后活跃时间
        """
        histories: dict[str, list[BaseMessage]] = {}
        timestamps: dict[str, float] = {}

        # 先处理旧版 JSON 迁移
        await self._migrate_old_json()

        try:
            async with self._session_factory() as session:
                result = await session.execute(select(AgentSessionModel))
                rows = result.scalars().all()

                for row in rows:
                    sid = row.session_id
                    try:
                        raw_msgs = (
                            row.messages
                            if isinstance(row.messages, list)
                            else json.loads(row.messages)
                        )
                    except (json.JSONDecodeError, TypeError):
                        continue

                    last_active = (
                        _utc_timestamp(row.last_active_at)
                        if row.last_active_at is not None
                        else time.time()
                    )

                    msgs: list[BaseMessage] = []
                    for raw in raw_msgs:
                        msg_type = raw.get("type", "")
                        cls = _MSG_CLASSES.get(msg_type)
                        if cls is not None:
                            msgs.append(cls.model_validate(raw))
                        else:
                            logger.warning(f"恢复会话时跳过未知消息类型: {msg_type!r}")

                    if msgs:
                        histories[sid] = msgs
                        timestamps[sid] = last_active

                if histories:
                    logger.info(f"已恢复 {len(histories)} 个 Agent 会话历史")
        except Exception as e:
            logger.warning(f"加载 Agent 会话历史失败: {e}")

        return histories, timestamps

    async def save_histories(
        self,
        histories: dict[str, list[BaseMessage]],
        timestamps: dict[str, float],
        last_save_time: float,
        force: bool = False,
    ) -> float:
        """
        持久化会话历史到数据库。

        调用者应持有锁以确保 concurrent 安全。

        Parameters
        ----------
        histories : dict
            会话 ID → 消息列表
        timestamps : dict
            会话 ID → 最后活跃时间戳
        last_save_time : float
            上次保存时间，用于节流
        force : bool
            强制保存，绕过节流

        Returns
        -------
        float
            新的 last_save_time
        """
        now = time.time()
        if not force and now - last_save_time < 5:
            return last_save_time

        # 裁剪超量会话
        if len(histories) > self._max_sessions:
            sorted_sids = sorted(
                histories.keys(),
                key=lambda sid: timestamps.get(sid, 0),
                reverse=True,
            )
            stale = sorted_sids[self._max_sessions :]
            for sid in stale:
                histories.pop(sid, None)
                timestamps.pop(sid, None)

        try:
            async with self._session_factory() as session:
                for sid, msgs in histories.items():
                    messages_json = json.dumps(
                        [msg.model_dump(mode="json") for msg in msgs],
                        ensure_ascii=False,
                    )
                    last_active = timestamps.get(sid, now)
                    result = await session.execute(
                        select(AgentSessionModel).where(AgentSessionModel.session_id == sid)
                    )
                    existing = result.scalars().first()
                    if existing:
                        existing.messages = messages_json
                        existing.last_active_at = datetime.fromtimestamp(
                            last_active, tz=timezone.utc
                        ).replace(tzinfo=None)
                    else:
                        session.add(
                            AgentSessionModel(
                                session_id=sid,
                                messages=messages_json,
                                last_active_at=datetime.fromtimestamp(
                                    last_active, tz=timezone.utc
                                ).replace(tzinfo=None),
                            )
                        )

                # 清理数据库中已不存在于内存的过期会话
                active_sids = list(histories.keys())
                if active_sids:
                    await session.execute(
                        delete(AgentSessionModel).where(
                            ~AgentSessionModel.session_id.in_(active_sids)
                        )
                    )

                await session.commit()
            return now
        except Exception as exc:
            logger.warning(f"保存 Agent 会话历史失败: {exc}")
            return last_save_time

    async def cleanup_stale(
        self,
        histories: dict[str, list[BaseMessage]],
        timestamps: dict[str, float],
    ) -> None:
        """
        清理超时的会话记忆（内存 + 数据库）。

        调用者应持有锁以确保 concurrent 安全。
        """
        if not self._session_timeout:
            return

        now = time.time()
        stale = [sid for sid, ts in list(timestamps.items()) if now - ts > self._session_timeout]
        if not stale:
            return

        for sid in stale:
            histories.pop(sid, None)
            timestamps.pop(sid, None)

        try:
            async with self._session_factory() as session:
                await session.execute(
                    delete(AgentSessionModel).where(AgentSessionModel.session_id.in_(stale))
                )
                await session.commit()
        except Exception as e:
            logger.warning(f"清理过期数据库会话失败: {e}")

        logger.debug(f"清理了 {len(stale)} 个超时会话")

    async def _migrate_old_json(self) -> None:
        """从旧版 JSON 文件迁移会话数据到数据库（仅执行一次）"""
        if self._old_persist_path is None or not self._old_persist_path.exists():
            return

        logger.info("发现旧版 JSON 会话数据，正在迁移到数据库...")
        try:
            data = json.loads(self._old_persist_path.read_text(encoding="utf-8"))
            async with self._session_factory() as session:
                for sid, blob in data.items():
                    if isinstance(blob, dict) and "messages" in blob:
                        raw_msgs = blob.get("messages", [])
                        last_active = blob.get("last_active_at", time.time())
                    elif isinstance(blob, list):
                        raw_msgs = blob
                        last_active = time.time()
                    else:
                        continue

                    result = await session.execute(
                        select(AgentSessionModel).where(AgentSessionModel.session_id == sid)
                    )
                    if result.scalars().first() is None:
                        session.add(
                            AgentSessionModel(
                                session_id=sid,
                                messages=raw_msgs,
                                last_active_at=datetime.fromtimestamp(
                                    last_active, tz=timezone.utc
                                ).replace(tzinfo=None),
                            )
                        )
                await session.commit()

            os.replace(
                str(self._old_persist_path),
                str(self._old_persist_path.with_suffix(".json.bak")),
            )
            logger.info("JSON 数据迁移完成，已重命名为 .bak")
        except Exception as e:
            logger.warning(f"迁移 JSON 会话历史失败: {e}")
