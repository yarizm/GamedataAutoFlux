"""CronRepository 和 PipelineRepository 测试"""

import pytest

from src.core.pipeline import Pipeline
from src.services.cron_repository import CronJobConfig, InMemoryCronRepository
from src.services.pipeline_repository import InMemoryPipelineRepository


# ---- CronRepository 测试 ----


class TestCronRepository:
    @pytest.fixture
    def repo(self):
        return InMemoryCronRepository()

    @pytest.fixture
    def sample_job(self):
        return CronJobConfig(
            name="daily_steam",
            pipeline_name="steam_basic",
            cron_expr="0 8 * * *",
            task_template={"targets": [{"name": "原神"}]},
        )

    @pytest.mark.asyncio
    async def test_save_and_load(self, repo, sample_job):
        await repo.save(sample_job)
        loaded = await repo.load("daily_steam")
        assert loaded is not None
        assert loaded.name == "daily_steam"
        assert loaded.pipeline_name == "steam_basic"
        assert loaded.cron_expr == "0 8 * * *"

    @pytest.mark.asyncio
    async def test_load_missing(self, repo):
        result = await repo.load("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete(self, repo, sample_job):
        await repo.save(sample_job)
        success = await repo.delete("daily_steam")
        assert success is True
        assert await repo.load("daily_steam") is None

    @pytest.mark.asyncio
    async def test_delete_missing(self, repo):
        success = await repo.delete("nonexistent")
        assert success is False

    @pytest.mark.asyncio
    async def test_list_all(self, repo, sample_job):
        await repo.save(sample_job)
        job2 = CronJobConfig(name="hourly", pipeline_name="taptap", cron_expr="0 * * * *")
        await repo.save(job2)
        jobs = await repo.list_all()
        assert len(jobs) == 2

    @pytest.mark.asyncio
    async def test_save_updates_existing(self, repo, sample_job):
        await repo.save(sample_job)
        updated = CronJobConfig(
            name="daily_steam",
            pipeline_name="steam_basic",
            cron_expr="0 10 * * *",  # 修改了 cron_expr
        )
        await repo.save(updated)
        loaded = await repo.load("daily_steam")
        assert loaded is not None
        assert loaded.cron_expr == "0 10 * * *"


# ---- PipelineRepository 测试 ----


class TestPipelineRepository:
    @pytest.fixture
    def repo(self):
        return InMemoryPipelineRepository()

    @pytest.fixture
    def sample_pipeline(self):
        return (
            Pipeline("steam_basic")
            .add_collector("steam", config={"app_id": "123"})
            .add_processor("cleaner")
            .add_storage("sqlalchemy")
        )

    @pytest.mark.asyncio
    async def test_save_and_load(self, repo, sample_pipeline):
        await repo.save(sample_pipeline)
        loaded = await repo.load("steam_basic")
        assert loaded is not None
        assert loaded.name == "steam_basic"
        assert len(loaded.steps) == 3

    @pytest.mark.asyncio
    async def test_load_missing(self, repo):
        result = await repo.load("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete(self, repo, sample_pipeline):
        await repo.save(sample_pipeline)
        success = await repo.delete("steam_basic")
        assert success is True
        assert await repo.load("steam_basic") is None

    @pytest.mark.asyncio
    async def test_list_all(self, repo, sample_pipeline):
        await repo.save(sample_pipeline)
        p2 = (
            Pipeline("taptap_basic")
            .add_collector("taptap")
            .add_processor("cleaner")
            .add_storage("sqlalchemy")
        )
        await repo.save(p2)
        pipelines = await repo.list_all()
        assert len(pipelines) == 2

    @pytest.mark.asyncio
    async def test_save_updates_existing(self, repo, sample_pipeline):
        await repo.save(sample_pipeline)
        updated = Pipeline("steam_basic").add_collector("steam").add_storage("sqlalchemy")
        await repo.save(updated)
        loaded = await repo.load("steam_basic")
        assert loaded is not None
        assert len(loaded.steps) == 2
