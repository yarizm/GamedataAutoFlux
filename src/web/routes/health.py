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
