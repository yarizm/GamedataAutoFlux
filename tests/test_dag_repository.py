# tests/test_dag_repository.py
import pytest
from src.core.dag import DAG, NodeSpec, Edge, PortSpec
from src.services.sqlalchemy_dag_repository import SQLAlchemyDAGRepository


async def _init_test_sf():
    from src.storage.session_factory import init_shared_session_factory
    return await init_shared_session_factory()


async def _close_test_sf():
    from src.storage.session_factory import close_shared_session_factory
    await close_shared_session_factory()


async def _make_repo():
    sf = await _init_test_sf()
    return sf, SQLAlchemyDAGRepository(sf)


@pytest.mark.asyncio
async def test_dag_repository_save_load(isolated_db_config):
    sf, repo = await _make_repo()
    try:
        dag = DAG(name="repo_test", nodes=[
            NodeSpec("src", "collector", "steam", {}, [], [PortSpec("records")], set()),
        ], edges=[])
        await repo.save(dag)
        loaded = await repo.load("repo_test")
        assert loaded is not None
        assert loaded.name == "repo_test"
        assert len(loaded.nodes) == 1
    finally:
        await _close_test_sf()


@pytest.mark.asyncio
async def test_dag_repository_list_all(isolated_db_config):
    sf, repo = await _make_repo()
    try:
        a = DAG("a", [NodeSpec("s", "collector", "steam", {}, [], [PortSpec("records")], set())], [])
        b = DAG("b", [NodeSpec("s", "collector", "steam", {}, [], [PortSpec("records")], set())], [])
        await repo.save(a)
        await repo.save(b)
        names = sorted(d.name for d in await repo.list_all())
        assert names == ["a", "b"]
    finally:
        await _close_test_sf()


@pytest.mark.asyncio
async def test_dag_repository_delete(isolated_db_config):
    sf, repo = await _make_repo()
    try:
        dag = DAG("del_me", [NodeSpec("s", "collector", "steam", {}, [], [PortSpec("records")], set())], [])
        await repo.save(dag)
        assert await repo.load("del_me") is not None
        assert await repo.delete("del_me") is True
        assert await repo.load("del_me") is None
        assert await repo.delete("del_me") is False
    finally:
        await _close_test_sf()


@pytest.mark.asyncio
async def test_dag_repository_save_updates(isolated_db_config):
    sf, repo = await _make_repo()
    try:
        dag = DAG("update", [NodeSpec("src", "collector", "steam", {}, [], [PortSpec("records")], set())], [])
        await repo.save(dag)
        loaded = await repo.load("update")
        assert loaded is not None and len(loaded.nodes) == 1

        dag2 = DAG("update", [
            NodeSpec("a", "collector", "steam", {}, [], [PortSpec("records")], set()),
            NodeSpec("b", "collector", "taptap", {}, [], [PortSpec("records")], set()),
        ], [])
        await repo.save(dag2)
        loaded2 = await repo.load("update")
        assert loaded2 is not None and len(loaded2.nodes) == 2
    finally:
        await _close_test_sf()


@pytest.mark.asyncio
async def test_pipeline_repo_load_as_dag_from_legacy(isolated_db_config):
    """旧 state_type=pipeline 记录可通过 load_as_dag 即时转换成 DAG。"""
    from src.core.pipeline import Pipeline
    from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository

    sf = await _init_test_sf()
    try:
        repo = SQLAlchemyPipelineRepository(sf)
        await repo.save(
            Pipeline("legacy").add_collector("steam").add_storage("sqlalchemy")
        )
        dag = await repo.load_as_dag("legacy")
        assert dag is not None
        assert dag.name == "legacy"
        assert any(n.type == "collector" for n in dag.nodes)
        assert any(n.type == "storage" for n in dag.nodes)
        assert dag.edges  # collector → storage 至少有一条边
    finally:
        await _close_test_sf()


@pytest.mark.asyncio
async def test_pipeline_repo_load_as_dag_prefers_graph(isolated_db_config):
    """同名 graph 优先于 legacy pipeline。"""
    from src.core.pipeline import Pipeline
    from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository

    sf = await _init_test_sf()
    try:
        pipe_repo = SQLAlchemyPipelineRepository(sf)
        dag_repo = SQLAlchemyDAGRepository(sf)

        await pipe_repo.save(
            Pipeline("prefer_me").add_collector("steam").add_storage("sqlalchemy")
        )
        # graph 只有一个 collector 节点，与 pipeline 转换结果可区分
        pure = DAG(
            "prefer_me",
            [NodeSpec("only", "collector", "taptap", {}, [], [PortSpec("records")], set())],
            [],
        )
        await dag_repo.save(pure)

        loaded = await pipe_repo.load_as_dag("prefer_me")
        assert loaded is not None
        assert len(loaded.nodes) == 1
        assert loaded.nodes[0].component == "taptap"
    finally:
        await _close_test_sf()


@pytest.mark.asyncio
async def test_pipeline_repo_load_as_dag_missing_returns_none(isolated_db_config):
    from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository

    sf = await _init_test_sf()
    try:
        repo = SQLAlchemyPipelineRepository(sf)
        assert await repo.load_as_dag("__no_such_pipeline_or_graph__") is None
    finally:
        await _close_test_sf()
