"""Scheduler must not hide persisted DAG repository failures."""

import pytest

from src.core.scheduler import Scheduler


class _BrokenPipelineRepo:
    async def load_as_dag(self, name: str):
        raise RuntimeError("database unavailable")


@pytest.mark.asyncio
async def test_resolve_pipeline_propagates_dag_repository_failure() -> None:
    scheduler = Scheduler(
        max_concurrent=1,
        default_retries=0,
        pipeline_repo=_BrokenPipelineRepo(),
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        await scheduler.resolve_pipeline("steam_basic")
