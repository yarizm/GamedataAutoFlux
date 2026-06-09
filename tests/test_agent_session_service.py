"""AgentSessionService 测试"""

import json
import time

import pytest
from langchain_core.messages import HumanMessage, AIMessage
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.services.agent_session_service import AgentSessionService
from src.storage.models import Base, AgentSessionModel


@pytest.fixture
async def session_factory():
    """创建内存 SQLite async session factory"""
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    yield factory
    await engine.dispose()


@pytest.fixture
def svc(session_factory):
    return AgentSessionService(
        session_factory=session_factory,
        session_timeout=3600,
        max_sessions=50,
    )


@pytest.mark.asyncio
async def test_load_empty(svc):
    """空数据库返回空 dict"""
    histories, timestamps = await svc.load_histories()
    assert histories == {}
    assert timestamps == {}


@pytest.mark.asyncio
async def test_save_and_load(svc, session_factory):
    """保存后应能加载"""
    histories = {
        "s1": [HumanMessage(content="你好"), AIMessage(content="你好！")],
    }
    timestamps = {"s1": time.time()}

    await svc.save_histories(histories, timestamps, last_save_time=0, force=True)

    # 新 instance 加载
    svc2 = AgentSessionService(session_factory)
    loaded_h, loaded_t = await svc2.load_histories()
    assert "s1" in loaded_h
    assert len(loaded_h["s1"]) == 2
    assert loaded_h["s1"][0].content == "你好"
    assert loaded_h["s1"][1].content == "你好！"


@pytest.mark.asyncio
async def test_load_redacts_unknown_message_type_log(session_factory, monkeypatch):
    captured: list[str] = []
    monkeypatch.setattr(
        "src.services.agent_session_service.logger.warning",
        lambda message, *args: captured.append(str(message).format(*args)),
    )
    async with session_factory() as session:
        session.add(
            AgentSessionModel(
                session_id="sensitive-type",
                messages=json.dumps(
                    [{"type": "tool_token=message-secret", "content": "x"}],
                    ensure_ascii=False,
                ),
            )
        )
        await session.commit()

    await AgentSessionService(session_factory).load_histories()

    rendered = " ".join(captured)
    assert "message-secret" not in rendered
    assert "tool_token=[REDACTED]" in rendered


@pytest.mark.asyncio
async def test_load_failure_log_redacts_exception_text(monkeypatch):
    captured: list[str] = []
    monkeypatch.setattr(
        "src.services.agent_session_service.logger.warning",
        lambda message, *args: captured.append(str(message).format(*args)),
    )

    class FailingSession:
        async def __aenter__(self):
            raise RuntimeError("db failed api_key=session-secret")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def failing_session_factory():
        return FailingSession()

    await AgentSessionService(failing_session_factory).load_histories()

    rendered = " ".join(captured)
    assert "session-secret" not in rendered
    assert "api_key=[REDACTED]" in rendered


@pytest.mark.asyncio
async def test_save_throttle(svc):
    """保存节流：5秒内非 force 不写入"""
    histories = {"s1": [HumanMessage(content="test")]}
    timestamps = {"s1": time.time()}

    last_save = await svc.save_histories(
        histories, timestamps, last_save_time=time.time(), force=False
    )
    # 应该跳过写入，返回原 last_save_time
    assert last_save < time.time()  # 返回的是旧值（未更新）


@pytest.mark.asyncio
async def test_save_updates_existing(svc, session_factory):
    """保存同一 session 应更新而非新增"""
    histories = {"s1": [HumanMessage(content="v1")]}
    timestamps = {"s1": time.time()}
    await svc.save_histories(histories, timestamps, last_save_time=0, force=True)

    # 更新
    histories["s1"] = [HumanMessage(content="v2")]
    await svc.save_histories(histories, timestamps, last_save_time=0, force=True)

    loaded_h, _ = await svc.load_histories()
    assert loaded_h["s1"][0].content == "v2"


@pytest.mark.asyncio
async def test_cleanup_stale(svc, session_factory):
    """清理超时会话"""
    now = time.time()
    histories = {
        "active": [HumanMessage(content="active")],
        "stale": [HumanMessage(content="stale")],
    }
    timestamps = {
        "active": now,
        "stale": now - 7200,  # 2小时前
    }
    await svc.save_histories(histories, timestamps, last_save_time=0, force=True)

    # session_timeout=3600，stale 应被清理
    await svc.cleanup_stale(histories, timestamps)

    assert "active" in histories
    assert "stale" not in histories

    # 验证数据库中也被清理
    async with session_factory() as session:
        from sqlalchemy import select

        result = await session.execute(select(AgentSessionModel))
        rows = result.scalars().all()
        sids = {r.session_id for r in rows}
    assert "active" in sids
    assert "stale" not in sids


@pytest.mark.asyncio
async def test_delete_sessions_removes_persisted_rows(svc, session_factory):
    histories = {
        "keep": [HumanMessage(content="keep")],
        "delete": [HumanMessage(content="delete")],
    }
    timestamps = {"keep": time.time(), "delete": time.time()}
    await svc.save_histories(histories, timestamps, last_save_time=0, force=True)

    await svc.delete_sessions(["delete"])

    async with session_factory() as session:
        from sqlalchemy import select

        result = await session.execute(select(AgentSessionModel))
        sids = {r.session_id for r in result.scalars().all()}
        assert "keep" in sids
        assert "delete" not in sids


@pytest.mark.asyncio
async def test_cap_max_sessions(svc, session_factory):
    """超过 max_sessions 时裁剪最旧的"""
    now = time.time()
    svc._max_sessions = 2
    histories = {}
    timestamps = {}
    for i in range(5):
        sid = f"s{i}"
        histories[sid] = [HumanMessage(content=f"msg{i}")]
        timestamps[sid] = now - (5 - i) * 100  # s4 最新, s0 最旧

    # 最多保留 2 个，应保留 s3, s4
    await svc.save_histories(histories, timestamps, last_save_time=0, force=True)
    assert len(histories) == 2
    assert "s3" in histories
    assert "s4" in histories

    async with session_factory() as session:
        from sqlalchemy import select

        result = await session.execute(select(AgentSessionModel))
        sids = {r.session_id for r in result.scalars().all()}
        assert sids == {"s3", "s4"}


@pytest.mark.asyncio
async def test_cleanup_stale_no_timeout(svc):
    """session_timeout=0 时不清理"""
    svc._session_timeout = 0
    histories = {"s1": [HumanMessage(content="x")]}
    timestamps = {"s1": 0}  # 很久以前
    await svc.cleanup_stale(histories, timestamps)
    assert "s1" in histories


@pytest.mark.asyncio
async def test_json_migration(tmp_path, session_factory):
    """旧版 JSON 迁移测试"""
    old_json = tmp_path / "agent_sessions.json"
    old_json.write_text(
        json.dumps(
            {
                "old_session": {
                    "messages": [
                        {"type": "human", "content": "旧数据"},
                        {"type": "ai", "content": "回复"},
                    ],
                    "last_active_at": time.time(),
                }
            }
        ),
        encoding="utf-8",
    )

    svc = AgentSessionService(
        session_factory=session_factory,
        old_persist_path=old_json,
    )

    histories, timestamps = await svc.load_histories()
    assert "old_session" in histories
    assert len(histories["old_session"]) == 2
    assert histories["old_session"][0].content == "旧数据"

    # 文件应被重命名为 .bak
    assert not old_json.exists()
    assert old_json.with_suffix(".json.bak").exists()
