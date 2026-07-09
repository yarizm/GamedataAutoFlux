"""Health and diagnostics API routes."""

from __future__ import annotations

import asyncio

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from src.core.diagnostics import (
    build_collector_session_diagnostics,
    build_config_diagnostics,
    build_health_report,
    build_session_diagnostics_overview,
)
from src.services.session_registry import build_session_inventory_summary
from src.services.session_inventory_sync import sync_session_inventory_via_provider_best_effort
from src.web.safety import require_admin


_launch_lock = asyncio.Lock()

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check():
    """Return compact runtime health status."""
    from src.web.app import scheduler

    return build_health_report(scheduler.get_stats())


@router.get("/diagnostics/config", dependencies=[Depends(require_admin)])
async def config_diagnostics():
    """Return detailed local configuration diagnostics."""
    return build_config_diagnostics()


@router.get("/diagnostics/sessions", dependencies=[Depends(require_admin)])
async def session_diagnostics(
    collectors: Annotated[list[str] | None, Query(description="Optional collector ids")] = None,
):
    """Return local browser/session diagnostics for session-sensitive collectors."""
    from src.web.app import get_session_registry

    payload = build_session_diagnostics_overview(collectors)
    for collector in payload.get("collectors", []) or []:
        await _sync_session_inventory_best_effort(get_session_registry, collector)
    return payload


@router.get("/diagnostics/sessions/{collector_id}", dependencies=[Depends(require_admin)])
async def collector_session_diagnostics(collector_id: str):
    """Return local browser/session diagnostics for one collector."""
    from src.web.app import get_session_registry

    payload = build_collector_session_diagnostics(collector_id)
    await _sync_session_inventory_best_effort(get_session_registry, payload)
    return payload


@router.get("/diagnostics/sessions-inventory", dependencies=[Depends(require_admin)])
async def session_inventory(
    collectors: Annotated[list[str] | None, Query(description="Optional collector ids")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    offset: Annotated[int, Query(ge=0)] = 0,
    sync: Annotated[
        bool,
        Query(description="Refresh persisted inventory from live diagnostics before listing"),
    ] = False,
):
    """Return persisted session inventory snapshots derived from diagnostics."""
    from src.web.app import get_session_registry

    if sync:
        payload = build_session_diagnostics_overview(collectors)
        for collector in payload.get("collectors", []) or []:
            await _sync_session_inventory_best_effort(get_session_registry, collector)

    registry = get_session_registry()
    entries = await registry.list_sessions(
        collector_ids=collectors,
        limit=limit,
        offset=offset,
    )
    return {
        "items": [entry.to_public_payload() for entry in entries],
        "count": len(entries),
        "summary": build_session_inventory_summary(entries),
    }


@router.post("/diagnostics/steamdb/launch", dependencies=[Depends(require_admin)])
async def launch_steamdb_browser():
    """Launch the SteamDB login browser via subprocess."""
    import subprocess
    from src.core.diagnostics import build_steamdb_launch_command

    async with _launch_lock:
        cmd = build_steamdb_launch_command()

        try:
            subprocess.Popen(cmd)
            return {"status": "ok", "message": "Browser launch command executed"}
        except Exception as e:
            from fastapi import HTTPException

            raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnostics/probes", dependencies=[Depends(require_admin)])
async def run_deep_probes(
    collectors: Annotated[
        list[str] | None,
        Query(description="Collector ids to probe; default = session-sensitive set"),
    ] = None,
):
    """Run optional deep probes (network / API validity). Not used on default precheck."""
    from src.core.collector_metadata import list_session_sensitive_collectors
    from src.core.collector_probes import build_probe_report, run_collector_probes

    collector_ids = collectors or list_session_sensitive_collectors()
    results = await run_collector_probes(collector_ids, targets=[])
    return build_probe_report(results)


async def _sync_session_inventory_best_effort(get_registry, diagnostics: dict) -> None:
    await sync_session_inventory_via_provider_best_effort(
        get_registry,
        diagnostics,
        context="health_diagnostics",
    )
