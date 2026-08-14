"""Deep readiness probes owned by the Steam plugin."""

from __future__ import annotations

from typing import Any

from src.core import collector_probes as core_probes
from src.core.collector_probes import ProbeResult, register_collector_probe
from src.core.errors import ErrorCode


async def _probe_api_key(collector_id: str, targets: list[dict[str, Any]]) -> ProbeResult:
    del targets
    api_key = str(core_probes.get_config("steam.api_key", "") or "").strip()
    if not api_key or api_key.startswith("${"):
        return ProbeResult(
            collector_id,
            "steam.api_key",
            "warning",
            "Steam API key is not configured; public endpoints remain available",
            ErrorCode.missing_credentials.value,
        )
    return ProbeResult(collector_id, "steam.api_key", "ok", "Steam API key is configured")


async def _probe_app_ids(collector_id: str, targets: list[dict[str, Any]]) -> ProbeResult:
    app_ids = []
    for target in targets[:5]:
        params = target.get("params", {}) if isinstance(target.get("params"), dict) else {}
        app_id = str(params.get("app_id") or "").strip()
        if app_id:
            app_ids.append(app_id)
    invalid = [app_id for app_id in app_ids if not app_id.isdigit()]
    if invalid:
        return ProbeResult(
            collector_id,
            "steam.app_id",
            "warning",
            "Some sampled Steam app ids are not numeric",
            details={"invalid": invalid},
        )
    return ProbeResult(collector_id, "steam.app_id", "ok", "Sampled Steam app ids are valid")


async def _probe_steamdb(collector_id: str, targets: list[dict[str, Any]]) -> ProbeResult:
    del targets
    enabled = bool(core_probes.get_config("steam.steamdb.enabled", False))
    cdp_enabled = bool(core_probes.get_config("steam.steamdb.cdp_enabled", False))
    if not enabled or not cdp_enabled:
        return ProbeResult(collector_id, "steam.steamdb_cdp", "skipped", "SteamDB CDP is disabled")
    endpoint = str(core_probes.get_config("steam.steamdb.cdp_url", "") or "").strip()
    if not endpoint:
        return ProbeResult(
            collector_id,
            "steam.steamdb_cdp",
            "warning",
            "SteamDB CDP is enabled but cdp_url is missing",
            ErrorCode.missing_credentials.value,
        )
    return await core_probes.probe_http_reachability(
        collector_id,
        "steam.steamdb_cdp",
        endpoint,
    )


register_collector_probe("steam", "steam.api_key", _probe_api_key, owner="autoflux-plugin-steam")
register_collector_probe("steam", "steam.app_id", _probe_app_ids, owner="autoflux-plugin-steam")
register_collector_probe(
    "steam",
    "steam.steamdb_cdp",
    _probe_steamdb,
    owner="autoflux-plugin-steam",
)
