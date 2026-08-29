"""Cron persistence restore must not hide malformed records."""

from types import SimpleNamespace

import pytest

from src.core.scheduler_cron_service import SchedulerCronService


def _service(records):
    async def restore():
        return records

    async def noop(*args, **kwargs):
        return None

    return SchedulerCronService(
        submit_task=noop,
        persist_cron_job=noop,
        delete_cron_job=noop,
        restore_cron_jobs=restore,
    )


@pytest.mark.asyncio
async def test_restore_cron_jobs_surfaces_missing_schedule_fields() -> None:
    service = _service([SimpleNamespace(name="broken", pipeline_name="p", cron_expr="")])

    with pytest.raises(ValueError, match="missing cron_expr"):
        await service.restore_cron_jobs_from_store()
