"""Deep readiness probes owned by the YouTube plugin."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from src.core import collector_probes as core_probes
from src.core.collector_probes import ProbeResult, register_collector_probe
from src.core.errors import ErrorCode


async def _probe_api_keys(
    collector_id: str,
    targets: list[dict[str, Any]],
) -> ProbeResult:
    del targets
    raw_keys = core_probes.get_config("youtube.api_keys", []) or []
    if isinstance(raw_keys, str):
        raw_keys = [raw_keys]
    keys = [
        str(key).strip()
        for key in raw_keys
        if str(key).strip() and not str(key).strip().startswith("${")
    ]
    if not keys:
        return ProbeResult(
            collector_id,
            "youtube.api_keys",
            "error",
            "YouTube API keys are not configured",
            ErrorCode.missing_credentials.value,
        )

    import httpx

    base = (
        str(
            core_probes.get_config(
                "youtube.api_base_url",
                "https://youtube.googleapis.com/youtube/v3",
            )
            or ""
        ).rstrip("/")
        or "https://youtube.googleapis.com/youtube/v3"
    )
    last_error = ""
    for key in keys:
        try:
            async with httpx.AsyncClient(timeout=core_probes.probe_timeout_seconds()) as client:
                response = await client.get(
                    f"{base}/videos",
                    params={"part": "id", "id": "jNQXAC9IVRw", "key": key},
                )
            if response.status_code == 200:
                return ProbeResult(
                    collector_id,
                    "youtube.api_keys",
                    "ok",
                    "At least one YouTube API key is valid",
                    details={"keys_configured": len(keys)},
                )
            last_error = f"http {response.status_code}"
            if response.status_code in {403, 429}:
                try:
                    reasons = [
                        error.get("reason")
                        for error in response.json().get("error", {}).get("errors", [])
                        if isinstance(error, dict)
                    ]
                except Exception:  # noqa: BLE001
                    reasons = []
                if any(reason in {"quotaExceeded", "dailyLimitExceeded"} for reason in reasons):
                    last_error = "quota"
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)

    if last_error == "quota":
        return ProbeResult(
            collector_id,
            "youtube.api_keys",
            "warning",
            "YouTube API keys appear quota-exhausted",
            ErrorCode.rate_limited.value,
            details={"keys_configured": len(keys)},
        )
    return ProbeResult(
        collector_id,
        "youtube.api_keys",
        "error",
        f"YouTube API key probe failed ({last_error or 'unknown'})",
        ErrorCode.missing_credentials.value,
        details={"keys_configured": len(keys)},
    )


async def _probe_video_urls(
    collector_id: str,
    targets: list[dict[str, Any]],
) -> ProbeResult:
    invalid: list[str] = []
    for target in targets[:5]:
        params = target.get("params", {}) if isinstance(target.get("params"), dict) else {}
        value = str(params.get("video_url") or "").strip()
        if not value:
            continue
        host = (urlsplit(value).hostname or "").lower()
        if host != "youtu.be" and not host.endswith("youtube.com"):
            invalid.append(value)
    if invalid:
        return ProbeResult(
            collector_id,
            "youtube.video_url",
            "warning",
            f"{len(invalid)} sampled target(s) are not YouTube URLs",
            details={"invalid_samples": invalid},
        )
    return ProbeResult(
        collector_id,
        "youtube.video_url",
        "ok",
        "Sampled video URLs use recognized YouTube hosts",
    )


for _collector_id in ("youtube_profiles", "youtube_comments"):
    register_collector_probe(
        _collector_id,
        "youtube.api_keys",
        _probe_api_keys,
        owner="autoflux-plugin-youtube",
    )
register_collector_probe(
    "youtube_comments",
    "youtube.video_url",
    _probe_video_urls,
    owner="autoflux-plugin-youtube",
)
