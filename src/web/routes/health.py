"""Health and diagnostics API routes."""

from __future__ import annotations

from fastapi import APIRouter

from src.core.diagnostics import build_config_diagnostics, build_health_report

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


@router.post("/diagnostics/steamdb/launch")
async def launch_steamdb_browser():
    """Launch the SteamDB login browser via subprocess."""
    import subprocess
    from src.core.diagnostics import build_steamdb_launch_command

    cmd = build_steamdb_launch_command()

    try:
        subprocess.Popen(cmd)
        return {"status": "ok", "message": "Browser launch command executed"}
    except Exception as e:
        from fastapi import HTTPException

        raise HTTPException(status_code=500, detail=str(e))
