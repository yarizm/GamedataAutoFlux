"""_try_load_stored_dag / load_as_dag 错误语义测试（修复清单 P1）。

验收：
- 没存过 graph → None（允许回退 steps 投影）；
- 数据库未初始化 → None（无 DB 的嵌入/单测场景走投影）；
- 数据库读写失败 → 直接抛出，不得解释成“没有 DAG”；
- 存储的 graph 损坏 → 抛 ValueError，不得静默降级（会丢条件边/拓扑）。
"""

import os

import pytest

from src.core.dag import DAG, NodeSpec, PortSpec
from src.core.pipeline import Pipeline
from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository


async def _init_sf():
    from src.storage.session_factory import init_shared_session_factory

    return await init_shared_session_factory()


async def _close_sf():
    from src.storage.session_factory import close_shared_session_factory

    await close_shared_session_factory()


def _simple_dag(name: str) -> DAG:
    return DAG(
        name=name,
        nodes=[NodeSpec("src", "collector", "steam", {}, [], [PortSpec("records")], set())],
        edges=[],
    )


async def test_load_as_dag_returns_none_when_nothing_stored(isolated_db_config):
    sf = await _init_sf()
    try:
        assert await SQLAlchemyPipelineRepository(sf).load_as_dag("never_saved") is None
    finally:
        await _close_sf()


async def test_load_as_dag_roundtrip(isolated_db_config):
    sf = await _init_sf()
    try:
        await _save_via_dag_repo(sf, "rt_check")
        loaded = await SQLAlchemyPipelineRepository(sf).load_as_dag("rt_check")
        assert loaded is not None and loaded.name == "rt_check"
    finally:
        await _close_sf()


async def _save_via_dag_repo(sf, name: str) -> None:
    from src.services.sqlalchemy_dag_repository import SQLAlchemyDAGRepository

    await SQLAlchemyDAGRepository(sf).save(_simple_dag(name))


async def test_load_as_dag_raises_on_corrupt_graph(isolated_db_config):
    """graph 行存在但 payload 非法 → 抛错而非静默降级。"""
    sf = await _init_sf()
    try:
        from src.storage.models import SchedulerStateModel

        async with sf() as session:
            session.add(
                SchedulerStateModel(key="graph:corrupt", state_type="graph", data={"nodes": "x"})
            )
            await session.commit()

        with pytest.raises(ValueError, match="unreadable|malformed"):
            await SQLAlchemyPipelineRepository(sf).load_as_dag("corrupt")
    finally:
        await _close_sf()


async def test_db_failure_propagates_through_try_load(isolated_db_config):
    """数据库不可用 → Pipeline 侧必须让异常穿透，不得当作“没有 DAG”。"""
    from src.storage import session_factory as sf_mod

    await _init_sf()
    # 释放连接池后删除 SQLite 文件；factory 全局保留（保持“已初始化”），
    # 让下一次会话真实经历连接失败而非走“未初始化”分支
    assert sf_mod._engine is not None
    await sf_mod._engine.dispose()
    db_path = os.environ["DATABASE_URL"].split("///", 1)[1]
    if os.path.exists(db_path):
        os.remove(db_path)

    with pytest.raises(Exception):
        await Pipeline("broken_db_pipeline")._try_load_stored_dag()


async def test_uninitialized_db_returns_none(isolated_db_config):
    """无数据库环境（factory 未初始化）→ None，走 steps 投影。"""
    assert await Pipeline("embedded_pipeline")._try_load_stored_dag() is None
