"""Optional deep probes for collectors (network / API validity).

Default precheck stays static. Call ``run_collector_probes`` only when
``deep=true`` or the System page requests a deep check.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from src.core.config import get as get_config
from src.core.config import get_root_dir
from src.core.errors import ErrorCode

# Process-local TTL cache: key -> (expires_at, ProbeResult)
_PROBE_CACHE: dict[str, tuple[float, "ProbeResult"]] = {}


@dataclass
class ProbeResult:
    collector_id: str
    name: str
    status: str  # ok | warning | error | skipped
    message: str
    error_code: str | None = None
    latency_ms: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_precheck_issue_dict(self) -> dict[str, Any] | None:
        if self.status not in {"warning", "error"}:
            return None
        return {
            "level": self.status,
            "code": f"probe_{self.name.replace(':', '_').replace('.', '_')}",
            "field": f"probe.{self.name}",
            "message": self.message,
            "collector_id": self.collector_id,
            "category": "probe",
            "suggested_action": _suggested_action(self.error_code),
        }


def _suggested_action(error_code: str | None) -> str:
    if not error_code:
        return "Review probe details and fix configuration or environment."
    try:
        return ErrorCode(error_code).suggestion
    except ValueError:
        return "Review probe details and fix configuration or environment."


def _timeout_s() -> float:
    try:
        return max(1.0, float(get_config("precheck.probe_timeout_seconds", 5) or 5))
    except (TypeError, ValueError):
        return 5.0


def _cache_ttl_s() -> float:
    try:
        return max(0.0, float(get_config("precheck.probe_cache_ttl_seconds", 120) or 120))
    except (TypeError, ValueError):
        return 120.0


def _blocking_collectors() -> set[str]:
    raw = get_config("precheck.blocking_probes", []) or []
    if isinstance(raw, str):
        raw = [raw]
    return {str(item).strip() for item in raw if str(item).strip()}


def clear_probe_cache() -> None:
    _PROBE_CACHE.clear()


def _cache_get(key: str) -> ProbeResult | None:
    ttl = _cache_ttl_s()
    if ttl <= 0:
        return None
    entry = _PROBE_CACHE.get(key)
    if not entry:
        return None
    expires_at, result = entry
    if time.time() > expires_at:
        _PROBE_CACHE.pop(key, None)
        return None
    return result


def _cache_set(key: str, result: ProbeResult) -> None:
    ttl = _cache_ttl_s()
    if ttl <= 0:
        return
    _PROBE_CACHE[key] = (time.time() + ttl, result)


async def run_collector_probes(
    collector_ids: list[str],
    *,
    targets: list[dict[str, Any]] | None = None,
    timeout_s: float | None = None,
) -> list[ProbeResult]:
    """Run deep probes for the given collectors (concurrent, bounded timeout)."""
    timeout = timeout_s if timeout_s is not None else _timeout_s()
    targets = targets or []
    unique = list(dict.fromkeys(cid for cid in collector_ids if cid))
    if not unique:
        return []

    tasks = [
        asyncio.create_task(
            _run_probes_for_collector(
                cid,
                targets=targets,
                timeout_s=timeout,
                include_storage_ping=(index == 0),
            )
        )
        for index, cid in enumerate(unique)
    ]
    nested = await asyncio.gather(*tasks, return_exceptions=True)
    results: list[ProbeResult] = []
    for cid, item in zip(unique, nested):
        if isinstance(item, Exception):
            results.append(
                ProbeResult(
                    collector_id=cid,
                    name="probe",
                    status="warning",
                    message=f"Probe runner failed: {item}",
                    error_code=ErrorCode.unknown.value,
                )
            )
            continue
        results.extend(item)
    return results


async def _run_probes_for_collector(
    collector_id: str,
    *,
    targets: list[dict[str, Any]],
    timeout_s: float,
    include_storage_ping: bool = False,
) -> list[ProbeResult]:
    runners = _probe_runners(
        collector_id,
        targets=targets,
        include_storage_ping=include_storage_ping,
    )
    if not runners:
        return [
            ProbeResult(
                collector_id=collector_id,
                name="probe",
                status="skipped",
                message="No deep probes registered for this collector",
            )
        ]

    results: list[ProbeResult] = []
    for name, coro_factory in runners:
        cache_key = f"{collector_id}:{name}"
        cached = _cache_get(cache_key)
        if cached is not None:
            results.append(cached)
            continue
        started = time.perf_counter()
        try:
            result = await asyncio.wait_for(coro_factory(), timeout=timeout_s)
        except asyncio.TimeoutError:
            result = ProbeResult(
                collector_id=collector_id,
                name=name,
                status="warning",
                message=f"Probe timed out after {timeout_s:.0f}s",
                error_code=ErrorCode.network_unreachable.value,
                latency_ms=int((time.perf_counter() - started) * 1000),
            )
        except Exception as exc:  # noqa: BLE001 — probe must never break precheck
            result = ProbeResult(
                collector_id=collector_id,
                name=name,
                status="warning",
                message=f"Probe error: {exc}",
                error_code=ErrorCode.unknown.value,
                latency_ms=int((time.perf_counter() - started) * 1000),
            )
        if result.latency_ms <= 0:
            result.latency_ms = int((time.perf_counter() - started) * 1000)
        _cache_set(cache_key, result)
        results.append(result)
    return results


def _probe_runners(
    collector_id: str,
    *,
    targets: list[dict[str, Any]],
    include_storage_ping: bool = False,
) -> list[tuple[str, Any]]:
    """Return ordered (name, zero-arg async factory) for a collector."""
    runners: list[tuple[str, Any]] = []

    if collector_id in {"youtube_profiles", "youtube_comments"}:
        runners.append(("youtube.api_keys", lambda: _probe_youtube_api(collector_id)))
        if collector_id == "youtube_comments" and targets:
            runners.append(
                (
                    "youtube.video_url",
                    lambda: _probe_youtube_video_targets(collector_id, targets),
                )
            )

    if collector_id == "steam":
        runners.append(("steam.api_key", lambda: _probe_steam_api(collector_id)))
        if targets:
            runners.append(
                ("steam.app_id", lambda: _probe_steam_app_ids(collector_id, targets))
            )
        if bool(get_config("steam.steamdb.enabled", False)):
            runners.append(("steam.steamdb_cdp", lambda: _probe_steamdb_cdp(collector_id)))

    if collector_id == "qimai":
        runners.append(("qimai.session_asset", lambda: _probe_qimai_session(collector_id)))

    if collector_id == "gtrends":
        runners.append(("gtrends.network", lambda: _probe_http_reachability(
            collector_id,
            "gtrends.network",
            "https://trends.google.com/",
        )))

    if collector_id == "monitor":
        runners.append(("monitor.steam_network", lambda: _probe_http_reachability(
            collector_id,
            "monitor.steam_network",
            "https://store.steampowered.com/",
        )))

    if collector_id == "official_site" and targets:
        runners.append(
            ("official_site.url", lambda: _probe_official_urls(collector_id, targets))
        )

    if include_storage_ping:
        runners.append(("storage.ping", lambda: _probe_storage_ping(collector_id)))

    return runners


async def _probe_youtube_api(collector_id: str) -> ProbeResult:
    raw_keys = get_config("youtube.api_keys", []) or []
    if isinstance(raw_keys, str):
        raw_keys = [raw_keys]
    keys = [
        str(k).strip()
        for k in raw_keys
        if str(k).strip() and not str(k).strip().startswith("${")
    ]
    if not keys:
        return ProbeResult(
            collector_id=collector_id,
            name="youtube.api_keys",
            status="error",
            message="YouTube API keys are not configured",
            error_code=ErrorCode.missing_credentials.value,
        )

    import httpx

    base = str(
        get_config("youtube.api_base_url", "https://youtube.googleapis.com/youtube/v3") or ""
    ).rstrip("/") or "https://youtube.googleapis.com/youtube/v3"
    # Public video "jNQXAC9IVRw" (Me at the zoo) — cheap videos.list probe
    last_error = ""
    for key in keys:
        try:
            async with httpx.AsyncClient(timeout=_timeout_s()) as client:
                resp = await client.get(
                    f"{base}/videos",
                    params={"part": "id", "id": "jNQXAC9IVRw", "key": key},
                )
            if resp.status_code == 200:
                return ProbeResult(
                    collector_id=collector_id,
                    name="youtube.api_keys",
                    status="ok",
                    message="At least one YouTube API key is valid",
                    details={"keys_configured": len(keys)},
                )
            body = {}
            try:
                body = resp.json()
            except Exception:
                pass
            reasons = [
                e.get("reason")
                for e in (body.get("error", {}) or {}).get("errors", []) or []
                if isinstance(e, dict)
            ]
            if resp.status_code in {403, 429} and any(
                r in {"quotaExceeded", "dailyLimitExceeded", "rateLimitExceeded"}
                for r in reasons
            ):
                last_error = "quota"
                continue
            if resp.status_code in {400, 403}:
                last_error = f"http {resp.status_code}"
                continue
            last_error = f"http {resp.status_code}"
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)

    if last_error == "quota":
        return ProbeResult(
            collector_id=collector_id,
            name="youtube.api_keys",
            status="warning",
            message="YouTube API keys appear quota-exhausted",
            error_code=ErrorCode.rate_limited.value,
            details={"keys_configured": len(keys)},
        )
    return ProbeResult(
        collector_id=collector_id,
        name="youtube.api_keys",
        status="error",
        message=f"YouTube API key probe failed ({last_error or 'unknown'})",
        error_code=ErrorCode.missing_credentials.value,
        details={"keys_configured": len(keys)},
    )


async def _probe_youtube_video_targets(
    collector_id: str,
    targets: list[dict[str, Any]],
) -> ProbeResult:
    """Static-ish deep check: video_url host only (API resolve is expensive)."""
    bad: list[str] = []
    for target in targets[:5]:
        params = target.get("params") if isinstance(target.get("params"), dict) else {}
        raw = str(params.get("video_url") or "").strip()
        if not raw:
            continue
        host = (urlsplit(raw).hostname or "").lower()
        if raw.startswith("http") and not (host.endswith("youtube.com") or host == "youtu.be"):
            bad.append(raw)
    if bad:
        return ProbeResult(
            collector_id=collector_id,
            name="youtube.video_url",
            status="warning",
            message=f"{len(bad)} video_url value(s) do not look like YouTube URLs",
            error_code=ErrorCode.invalid_params.value,
            details={"samples": bad[:3]},
        )
    return ProbeResult(
        collector_id=collector_id,
        name="youtube.video_url",
        status="ok",
        message="YouTube video_url hosts look valid",
    )


async def _probe_steam_api(collector_id: str) -> ProbeResult:
    key = str(get_config("steam.api_key", "") or "").strip()
    if not key or key.startswith("${"):
        return ProbeResult(
            collector_id=collector_id,
            name="steam.api_key",
            status="warning",
            message="Steam API key is not configured; official APIs may be unavailable",
            error_code=ErrorCode.missing_credentials.value,
        )

    import httpx

    url = "https://api.steampowered.com/ISteamWebAPIUtil/GetSupportedAPIList/v1/"
    try:
        async with httpx.AsyncClient(timeout=_timeout_s()) as client:
            resp = await client.get(url, params={"key": key})
        if resp.status_code == 200:
            return ProbeResult(
                collector_id=collector_id,
                name="steam.api_key",
                status="ok",
                message="Steam API key is accepted",
            )
        if resp.status_code in {401, 403}:
            return ProbeResult(
                collector_id=collector_id,
                name="steam.api_key",
                status="error",
                message="Steam API key was rejected",
                error_code=ErrorCode.missing_credentials.value,
                details={"http_status": resp.status_code},
            )
        return ProbeResult(
            collector_id=collector_id,
            name="steam.api_key",
            status="warning",
            message=f"Steam API probe returned HTTP {resp.status_code}",
            error_code=ErrorCode.network_unreachable.value,
            details={"http_status": resp.status_code},
        )
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(
            collector_id=collector_id,
            name="steam.api_key",
            status="warning",
            message=f"Steam API probe network error: {exc}",
            error_code=ErrorCode.network_unreachable.value,
        )


async def _probe_steam_app_ids(
    collector_id: str,
    targets: list[dict[str, Any]],
) -> ProbeResult:
    import httpx

    app_ids: list[str] = []
    for target in targets[:5]:
        params = target.get("params") if isinstance(target.get("params"), dict) else {}
        app_id = str(params.get("app_id") or "").strip()
        if app_id.isdigit():
            app_ids.append(app_id)
    if not app_ids:
        return ProbeResult(
            collector_id=collector_id,
            name="steam.app_id",
            status="skipped",
            message="No numeric app_id in targets to resolve",
        )

    missing: list[str] = []
    try:
        async with httpx.AsyncClient(timeout=_timeout_s()) as client:
            for app_id in app_ids:
                resp = await client.get(
                    "https://store.steampowered.com/api/appdetails",
                    params={"appids": app_id, "cc": "us", "l": "english"},
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                entry = data.get(app_id) if isinstance(data, dict) else None
                if not isinstance(entry, dict) or not entry.get("success"):
                    missing.append(app_id)
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(
            collector_id=collector_id,
            name="steam.app_id",
            status="warning",
            message=f"Steam app_id resolve network error: {exc}",
            error_code=ErrorCode.network_unreachable.value,
        )

    if missing:
        return ProbeResult(
            collector_id=collector_id,
            name="steam.app_id",
            status="warning",
            message=f"Steam store could not resolve app_id(s): {', '.join(missing)}",
            error_code=ErrorCode.invalid_params.value,
            details={"missing_app_ids": missing},
        )
    return ProbeResult(
        collector_id=collector_id,
        name="steam.app_id",
        status="ok",
        message=f"Resolved {len(app_ids)} Steam app_id(s)",
        details={"app_ids": app_ids},
    )


async def _probe_steamdb_cdp(collector_id: str) -> ProbeResult:
    from src.core.diagnostics import build_collector_session_diagnostics

    diagnostics = build_collector_session_diagnostics("steam")
    for check in diagnostics.get("checks", []) or []:
        if str(check.get("name") or "") == "session:steamdb":
            status = str(check.get("status") or "warning")
            if status == "ok":
                return ProbeResult(
                    collector_id=collector_id,
                    name="steam.steamdb_cdp",
                    status="ok",
                    message=str(check.get("message") or "SteamDB CDP reachable"),
                )
            return ProbeResult(
                collector_id=collector_id,
                name="steam.steamdb_cdp",
                status="warning" if status != "error" else "error",
                message=str(check.get("message") or "SteamDB CDP not ready"),
                error_code=ErrorCode.login_required.value,
            )
    return ProbeResult(
        collector_id=collector_id,
        name="steam.steamdb_cdp",
        status="skipped",
        message="SteamDB session check not applicable",
    )


async def _probe_qimai_session(collector_id: str) -> ProbeResult:
    """Validate session assets without launching a full browser login flow."""
    mode = str(get_config("qimai.session_mode", "") or "").strip().lower()
    if mode == "managed_state":
        path = Path(str(get_config("qimai.storage_state_path", "") or "data/qimai_storage_state.json"))
        if not path.is_absolute():
            path = get_root_dir() / path
        if not path.is_file():
            return ProbeResult(
                collector_id=collector_id,
                name="qimai.session_asset",
                status="error",
                message="Qimai storage_state file is missing",
                error_code=ErrorCode.login_required.value,
                details={"path": str(path)},
            )
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            cookies = payload.get("cookies") if isinstance(payload, dict) else None
            if not cookies:
                return ProbeResult(
                    collector_id=collector_id,
                    name="qimai.session_asset",
                    status="warning",
                    message="Qimai storage_state has no cookies; re-export login state",
                    error_code=ErrorCode.login_required.value,
                )
            return ProbeResult(
                collector_id=collector_id,
                name="qimai.session_asset",
                status="ok",
                message=f"Qimai storage_state present with {len(cookies)} cookie(s)",
                details={"path": str(path)},
            )
        except Exception as exc:  # noqa: BLE001
            return ProbeResult(
                collector_id=collector_id,
                name="qimai.session_asset",
                status="error",
                message=f"Qimai storage_state is invalid JSON: {exc}",
                error_code=ErrorCode.login_required.value,
            )

    profile = Path(str(get_config("qimai.user_data_dir", "") or "data/qimai_profile"))
    if not profile.is_absolute():
        profile = get_root_dir() / profile
    if profile.is_dir():
        # Presence only — true login requires browser probe (skipped for stability).
        return ProbeResult(
            collector_id=collector_id,
            name="qimai.session_asset",
            status="ok",
            message="Qimai local profile directory exists (login not browser-verified)",
            details={"path": str(profile), "login_verified": False},
        )
    return ProbeResult(
        collector_id=collector_id,
        name="qimai.session_asset",
        status="error",
        message="Qimai browser profile directory is missing; run qimai login helper",
        error_code=ErrorCode.login_required.value,
        details={"path": str(profile)},
    )


async def _probe_http_reachability(
    collector_id: str,
    name: str,
    url: str,
) -> ProbeResult:
    import httpx

    try:
        async with httpx.AsyncClient(timeout=_timeout_s(), follow_redirects=True) as client:
            resp = await client.head(url)
            if resp.status_code >= 500:
                resp = await client.get(url)
        if resp.status_code < 500:
            return ProbeResult(
                collector_id=collector_id,
                name=name,
                status="ok",
                message=f"Reachable: {url}",
                details={"http_status": resp.status_code},
            )
        return ProbeResult(
            collector_id=collector_id,
            name=name,
            status="warning",
            message=f"Unhealthy response from {url}: HTTP {resp.status_code}",
            error_code=ErrorCode.network_unreachable.value,
            details={"http_status": resp.status_code},
        )
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(
            collector_id=collector_id,
            name=name,
            status="warning",
            message=f"Cannot reach {url}: {exc}",
            error_code=ErrorCode.network_unreachable.value,
        )


async def _probe_official_urls(
    collector_id: str,
    targets: list[dict[str, Any]],
) -> ProbeResult:
    import httpx

    urls: list[str] = []
    for target in targets[:5]:
        params = target.get("params") if isinstance(target.get("params"), dict) else {}
        url = str(params.get("official_url") or "").strip()
        if url.startswith("http"):
            urls.append(url)
    if not urls:
        return ProbeResult(
            collector_id=collector_id,
            name="official_site.url",
            status="skipped",
            message="No official_url to probe",
        )

    failures: list[str] = []
    try:
        async with httpx.AsyncClient(timeout=_timeout_s(), follow_redirects=True) as client:
            for url in urls:
                try:
                    resp = await client.head(url)
                    if resp.status_code >= 400:
                        resp = await client.get(url)
                    if resp.status_code >= 400:
                        failures.append(f"{url}→{resp.status_code}")
                except Exception as exc:  # noqa: BLE001
                    failures.append(f"{url}→{exc}")
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(
            collector_id=collector_id,
            name="official_site.url",
            status="warning",
            message=f"Official URL probe failed: {exc}",
            error_code=ErrorCode.network_unreachable.value,
        )

    if failures:
        return ProbeResult(
            collector_id=collector_id,
            name="official_site.url",
            status="warning",
            message=f"{len(failures)} official_url probe failure(s)",
            error_code=ErrorCode.network_unreachable.value,
            details={"failures": failures[:5]},
        )
    return ProbeResult(
        collector_id=collector_id,
        name="official_site.url",
        status="ok",
        message=f"Probed {len(urls)} official_url(s)",
    )


async def _probe_storage_ping(collector_id: str) -> ProbeResult:
    """Best-effort: ensure storage factory can be obtained (no heavy queries)."""
    try:
        from src.storage.factory import get_storage

        storage = get_storage()
        if storage is None:
            return ProbeResult(
                collector_id=collector_id,
                name="storage.ping",
                status="warning",
                message="Storage factory returned None",
                error_code=ErrorCode.unknown.value,
            )
        return ProbeResult(
            collector_id=collector_id,
            name="storage.ping",
            status="ok",
            message=f"Storage available: {type(storage).__name__}",
        )
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(
            collector_id=collector_id,
            name="storage.ping",
            status="error",
            message=f"Storage is not available: {exc}",
            error_code=ErrorCode.unknown.value,
        )


def merge_probe_issues(
    *,
    probe_results: list[ProbeResult],
    blocking_collectors: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Convert probe results to precheck issue dicts.

    Non-blocking collectors: error probes downgraded to warning unless listed in
    ``blocking_probes`` config / argument.
    """
    blocking = blocking_collectors if blocking_collectors is not None else _blocking_collectors()
    issues: list[dict[str, Any]] = []
    for result in probe_results:
        issue = result.to_precheck_issue_dict()
        if issue is None:
            continue
        if issue["level"] == "error" and result.collector_id not in blocking:
            # Default: deep probe failures do not hard-block submit.
            issue["level"] = "warning"
            issue["message"] = f"[deep] {issue['message']}"
        elif issue["level"] == "error":
            issue["message"] = f"[deep] {issue['message']}"
        else:
            issue["message"] = f"[deep] {issue['message']}"
        issues.append(issue)
    return issues


def build_probe_report(probe_results: list[ProbeResult]) -> dict[str, Any]:
    statuses = {r.status for r in probe_results}
    if "error" in statuses:
        status = "error"
    elif "warning" in statuses:
        status = "warning"
    elif probe_results and all(r.status == "skipped" for r in probe_results):
        status = "ok"
    else:
        status = "ok"
    return {
        "status": status,
        "summary": {
            "total": len(probe_results),
            "ok": sum(1 for r in probe_results if r.status == "ok"),
            "warning": sum(1 for r in probe_results if r.status == "warning"),
            "error": sum(1 for r in probe_results if r.status == "error"),
            "skipped": sum(1 for r in probe_results if r.status == "skipped"),
        },
        "probes": [r.to_dict() for r in probe_results],
    }
