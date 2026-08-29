"""Scheduler state restore must surface malformed persisted records."""

from types import SimpleNamespace

import pytest

from src.core.scheduler_state_service import SchedulerStateService


class _Store:
    def __init__(self, records):
        self._records = records

    async def query(self, *_args, **_kwargs):
        return SimpleNamespace(records=self._records)


def _service(store):
    return SchedulerStateService(
        get_task_repo=lambda: None,
        get_pipeline_repo=lambda: None,
        get_cron_repo=lambda: None,
        get_task_store=lambda: store,
        get_event_bus=lambda: None,
        create_background_task=lambda coro: coro,
    )


@pytest.mark.asyncio
async def test_restore_tasks_surfaces_malformed_record() -> None:
    store = _Store([SimpleNamespace(key="task:broken", data={"status": object()})])

    with pytest.raises(ValueError, match="stored task .* unreadable"):
        await _service(store).restore_tasks()


@pytest.mark.asyncio
async def test_restore_pipelines_surfaces_malformed_record() -> None:
    store = _Store([SimpleNamespace(key="pipeline:broken", data={"steps": "bad"})])

    with pytest.raises(ValueError, match="stored pipeline .* unreadable"):
        await _service(store).restore_pipelines()
