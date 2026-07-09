"""CLI 入口：把 state_type=pipeline 记录转成 state_type=graph。幂等，只转不删。

实现位于 src.services.pipeline_dag_migration，本脚本仅作手动运行入口。
"""
from __future__ import annotations

import asyncio

from src.services.pipeline_dag_migration import migrate_pipelines_to_dag


async def _main() -> None:
    from src.storage.session_factory import init_shared_session_factory

    sf = await init_shared_session_factory()
    result = await migrate_pipelines_to_dag(sf)
    print(result)


if __name__ == "__main__":
    asyncio.run(_main())
