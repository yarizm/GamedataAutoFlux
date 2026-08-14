"""Plugin-extensible deep probes for collector readiness checks."""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable

from src.core.config import get as get_config
from src.core.errors import ErrorCode


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


ProbeRunner = Callable[[str, list[dict[str, Any]]], Awaitable[ProbeResult]]
_PROBE_RUNNERS: dict[str, list[tuple[str, ProbeRunner, str]]] = {}
_PROBE_CACHE: dict[str, tuple[float, ProbeResult]] = {}


def register_collector_probe(
    collector_id: str,
    name: str,
    runner: ProbeRunner,
    *,
    owner: str,
) -> None:
    """Register a named deep probe contributed by one collector plugin."""

    if not collector_id or not name or not owner:
        raise ValueError("collector_id, probe name, and owner are required")
    entries = _PROBE_RUNNERS.setdefault(collector_id, [])
    for index, (current_name, _, current_owner) in enumerate(entries):
        if current_name != name:
            continue
        if current_owner != owner:
            raise ValueError(
                f"probe '{collector_id}:{name}' already belongs to plugin '{current_owner}'"
            )
        entries[index] = (name, runner, owner)
        return
    entries.append((name, runner, owner))


def snapshot_collector_probes() -> dict[str, list[tuple[str, ProbeRunner, str]]]:
    return {collector_id: list(entries) for collector_id, entries in _PROBE_RUNNERS.items()}


def restore_collector_probes(
    snapshot: dict[str, list[tuple[str, ProbeRunner, str]]],
) -> None:
    _PROBE_RUNNERS.clear()
    _PROBE_RUNNERS.update(
        {collector_id: list(entries) for collector_id, entries in snapshot.items()}
    )


def clear_probe_cache() -> None:
    _PROBE_CACHE.clear()


def probe_timeout_seconds() -> float:
    try:
        return max(1.0, float(get_config("precheck.probe_timeout_seconds", 5) or 5))
    except (TypeError, ValueError):
        return 5.0


async def probe_http_reachability(
    collector_id: str,
    name: str,
    url: str,
) -> ProbeResult:
    """Reusable HTTP reachability probe for plugin-owned endpoints."""

    try:
        import httpx

        async with httpx.AsyncClient(
            timeout=probe_timeout_seconds(), follow_redirects=True
        ) as client:
            response = await client.get(url)
        if response.status_code < 500:
            return ProbeResult(
                collector_id=collector_id,
                name=name,
                status="ok",
                message=f"Endpoint reachable (HTTP {response.status_code})",
                details={"url": url, "status_code": response.status_code},
            )
        return ProbeResult(
            collector_id=collector_id,
            name=name,
            status="warning",
            message=f"Endpoint returned HTTP {response.status_code}",
            error_code=ErrorCode.network_unreachable.value,
            details={"url": url, "status_code": response.status_code},
        )
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(
            collector_id=collector_id,
            name=name,
            status="warning",
            message=f"Endpoint is not reachable: {exc}",
            error_code=ErrorCode.network_unreachable.value,
            details={"url": url},
        )


async def run_collector_probes(
    collector_ids: list[str],
    *,
    targets: list[dict[str, Any]] | None = None,
    timeout_s: float | None = None,
) -> list[ProbeResult]:
    """Run installed plugins' deep probes concurrently by collector."""

    timeout = timeout_s if timeout_s is not None else probe_timeout_seconds()
    target_list = targets or []
    unique = list(dict.fromkeys(cid for cid in collector_ids if cid))
    tasks = [
        asyncio.create_task(
            _run_probes_for_collector(
                collector_id,
                targets=target_list,
                timeout_s=timeout,
                include_storage_ping=index == 0,
            )
        )
        for index, collector_id in enumerate(unique)
    ]
    nested = await asyncio.gather(*tasks, return_exceptions=True)
    results: list[ProbeResult] = []
    for collector_id, item in zip(unique, nested):
        if isinstance(item, Exception):
            results.append(
                ProbeResult(
                    collector_id=collector_id,
                    name="probe",
                    status="warning",
                    message=f"Probe runner failed: {item}",
                    error_code=ErrorCode.unknown.value,
                )
            )
        else:
            results.extend(item)
    return results


async def _run_probes_for_collector(
    collector_id: str,
    *,
    targets: list[dict[str, Any]],
    timeout_s: float,
    include_storage_ping: bool,
) -> list[ProbeResult]:
    runners = [
        (name, lambda runner=runner: runner(collector_id, targets))
        for name, runner, _ in _PROBE_RUNNERS.get(collector_id, [])
    ]
    if include_storage_ping:
        runners.append(("storage.ping", lambda: _probe_storage_ping(collector_id)))
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
            )
        except Exception as exc:  # noqa: BLE001
            result = ProbeResult(
                collector_id=collector_id,
                name=name,
                status="warning",
                message=f"Probe error: {exc}",
                error_code=ErrorCode.unknown.value,
            )
        if result.latency_ms <= 0:
            result.latency_ms = int((time.perf_counter() - started) * 1000)
        _cache_set(cache_key, result)
        results.append(result)
    return results


def _cache_ttl_seconds() -> float:
    try:
        return max(0.0, float(get_config("precheck.probe_cache_ttl_seconds", 120) or 120))
    except (TypeError, ValueError):
        return 120.0


def _cache_get(key: str) -> ProbeResult | None:
    if _cache_ttl_seconds() <= 0:
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
    ttl = _cache_ttl_seconds()
    if ttl > 0:
        _PROBE_CACHE[key] = (time.time() + ttl, result)


async def _probe_storage_ping(collector_id: str) -> ProbeResult:
    try:
        from src.storage.factory import get_storage

        storage = get_storage()
        if storage is None:
            raise RuntimeError("storage factory returned None")
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


def _blocking_collectors() -> set[str]:
    raw = get_config("precheck.blocking_probes", []) or []
    if isinstance(raw, str):
        raw = [raw]
    return {str(item).strip() for item in raw if str(item).strip()}


def merge_probe_issues(
    *,
    probe_results: list[ProbeResult],
    blocking_collectors: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Convert probe results to precheck issues with opt-in blocking."""

    blocking = blocking_collectors if blocking_collectors is not None else _blocking_collectors()
    issues: list[dict[str, Any]] = []
    for result in probe_results:
        issue = result.to_precheck_issue_dict()
        if issue is None:
            continue
        if issue["level"] == "error" and result.collector_id not in blocking:
            issue["level"] = "warning"
        issue["message"] = f"[deep] {issue['message']}"
        issues.append(issue)
    return issues


def build_probe_report(probe_results: list[ProbeResult]) -> dict[str, Any]:
    statuses = {result.status for result in probe_results}
    status = "error" if "error" in statuses else "warning" if "warning" in statuses else "ok"
    return {
        "status": status,
        "summary": {
            "total": len(probe_results),
            "ok": sum(result.status == "ok" for result in probe_results),
            "warning": sum(result.status == "warning" for result in probe_results),
            "error": sum(result.status == "error" for result in probe_results),
            "skipped": sum(result.status == "skipped" for result in probe_results),
        },
        "probes": [result.to_dict() for result in probe_results],
    }


def _suggested_action(error_code: str | None) -> str:
    if error_code:
        try:
            return ErrorCode(error_code).suggestion
        except ValueError:
            pass
    return "Review probe details and fix configuration or environment."
