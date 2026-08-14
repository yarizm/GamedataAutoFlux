"""Deep target probes owned by the official-site plugin."""

from typing import Any
from urllib.parse import urlsplit

from src.core.collector_probes import ProbeResult, probe_http_reachability, register_collector_probe
from src.core.errors import ErrorCode


async def _probe_urls(collector_id: str, targets: list[dict[str, Any]]) -> ProbeResult:
    urls: list[str] = []
    for target in targets[:3]:
        params = target.get("params", {}) if isinstance(target.get("params"), dict) else {}
        url = str(params.get("official_url") or "").strip()
        if url:
            urls.append(url)
    invalid = [url for url in urls if not urlsplit(url).scheme or not urlsplit(url).netloc]
    if invalid:
        return ProbeResult(
            collector_id,
            "official_site.url",
            "error",
            "Official-site targets must be absolute URLs",
            ErrorCode.invalid_params.value,
            details={"invalid": invalid},
        )
    if not urls:
        return ProbeResult(collector_id, "official_site.url", "skipped", "No URLs to probe")
    return await probe_http_reachability(collector_id, "official_site.url", urls[0])


register_collector_probe(
    "official_site",
    "official_site.url",
    _probe_urls,
    owner="autoflux-plugin-official-site",
)
