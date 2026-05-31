"""Health and diagnostics API routes."""

from __future__ import annotations

import asyncio
import secrets

from fastapi import APIRouter, Depends, HTTPException, Header

from src.core.config import get as get_config
from src.core.diagnostics import build_config_diagnostics, build_health_report


def verify_admin(x_api_key: str | None = Header(None, alias="X-API-Key"), api_key: str | None = None):
    expected_key = get_config("server.api_key")
    if expected_key:
        token = x_api_key or api_key
        if not token or not secrets.compare_digest(token, expected_key):
            raise HTTPException(status_code=401, detail="Unauthorized")

_launch_lock = asyncio.Lock()

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check():
    """Return compact runtime health status."""
    from src.web.app import scheduler

    return build_health_report(scheduler.get_stats())


@router.get("/diagnostics/config")
async def config_diagnostics():
    """Return detailed local configuration diagnostics."""
    return build_config_diagnostics()


@router.post("/diagnostics/steamdb/launch", dependencies=[Depends(verify_admin)])
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
