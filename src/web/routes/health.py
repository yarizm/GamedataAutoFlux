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
    import sys
    from src.core.config import get as get_config
    from src.core.config import get_root_dir

    cdp_port = get_config("steam.steamdb.cdp_port", 9222)
    profile_dir = str(get_config("steam.steamdb.cdp_profile_dir", "") or "").strip()
    
    cmd = [
        sys.executable,
        str(get_root_dir() / "scripts" / "steamdb_login.py"),
        "--no-wait",
    ]
    if cdp_port is not None:
        cmd.extend(["--port", str(cdp_port)])
    if profile_dir:
        cmd.extend(["--profile-dir", profile_dir])
        
    try:
        subprocess.Popen(cmd)
        return {"status": "ok", "message": "Browser launch command executed"}
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))
