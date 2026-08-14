"""Deep readiness probes owned by the Google Trends plugin."""

from typing import Any

from src.core.collector_probes import ProbeResult, probe_http_reachability, register_collector_probe


async def _probe_network(collector_id: str, targets: list[dict[str, Any]]) -> ProbeResult:
    del targets
    return await probe_http_reachability(
        collector_id,
        "gtrends.network",
        "https://trends.google.com/",
    )


register_collector_probe(
    "gtrends",
    "gtrends.network",
    _probe_network,
    owner="autoflux-plugin-gtrends",
)
