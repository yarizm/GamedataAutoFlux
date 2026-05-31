"""Health and diagnostics API routes."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends

from src.core.diagnostics import build_config_diagnostics, build_health_report
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
