"""Start a local worker agent for the worker-claim backend."""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import socket
import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import get as get_config  # noqa: E402
from src.core.config import load_settings  # noqa: E402
from src.core.logging_config import configure_logging  # noqa: E402
from src.core.sensitive import redact_sensitive_text  # noqa: E402
from src.worker.agent import WorkerAgent, WorkerAgentConfig  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local worker-claim agent.")
    parser.add_argument("--base-url", default=os.environ.get("AUTOFLUX_BASE_URL", ""), help="Server base URL, e.g. http://127.0.0.1:8000")
    parser.add_argument("--worker-id", default=os.environ.get("AUTOFLUX_WORKER_ID", ""), help="Stable worker id. Auto-generated when omitted.")
    parser.add_argument("--hostname", default=os.environ.get("AUTOFLUX_WORKER_HOSTNAME", ""), help="Worker host name reported to the server.")
    parser.add_argument("--capability", action="append", dest="capabilities", default=None, help="Advertise one capability. Repeatable.")
    parser.add_argument("--api-key", default=os.environ.get("AUTOFLUX_API_KEY", ""), help="Admin API key when the server requires it.")
    parser.add_argument("--heartbeat-interval", type=float, default=float(os.environ.get("AUTOFLUX_WORKER_HEARTBEAT", "15")), help="Heartbeat interval in seconds.")
    parser.add_argument("--claim-poll-interval", type=float, default=float(os.environ.get("AUTOFLUX_WORKER_POLL", "3")), help="Claim polling interval in seconds.")
    parser.add_argument("--request-timeout", type=float, default=float(os.environ.get("AUTOFLUX_WORKER_TIMEOUT", "30")), help="HTTP request timeout in seconds.")
    parser.add_argument("--drain-on-shutdown", action=argparse.BooleanOptionalAction, default=True, help="Report draining before offline on shutdown.")
    return parser.parse_args()


def _resolve_base_url(raw: str) -> str:
    value = raw.strip()
    if value:
        return value.rstrip("/")
    host = str(get_config("server.host", "127.0.0.1") or "127.0.0.1").strip()
    port = int(get_config("server.port", 8000) or 8000)
    return f"http://{host}:{port}"


def _resolve_capabilities(raw: list[str] | None) -> list[str]:
    if raw:
        return [redact_sensitive_text(item) for item in raw if str(item or "").strip()]
    configured = get_config("worker.capabilities", [])
    if isinstance(configured, list):
        cleaned = [redact_sensitive_text(str(item)) for item in configured if str(item or "").strip()]
        if cleaned:
            return cleaned
    return []


async def _run(args: argparse.Namespace) -> int:
    agent = WorkerAgent(
        WorkerAgentConfig(
            base_url=_resolve_base_url(args.base_url),
            worker_id=args.worker_id.strip(),
            hostname=args.hostname.strip() or socket.gethostname(),
            capabilities=_resolve_capabilities(args.capabilities),
            api_key=args.api_key.strip(),
            heartbeat_interval_seconds=max(0.5, float(args.heartbeat_interval)),
            claim_poll_interval_seconds=max(0.1, float(args.claim_poll_interval)),
            request_timeout_seconds=max(1.0, float(args.request_timeout)),
            drain_on_shutdown=bool(args.drain_on_shutdown),
            metadata={"source": "scripts.worker_agent"},
        )
    )

    def _request_stop() -> None:
        logger.info("Worker agent stop requested")
        agent.request_stop()

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: _request_stop())

    logger.info(
        "Starting worker agent id={} base_url={} capabilities={}",
        agent.worker_id,
        _resolve_base_url(args.base_url),
        _resolve_capabilities(args.capabilities),
    )

    await agent.start()
    try:
        await agent.run_forever()
        return 0
    finally:
        await agent.stop()


def main() -> int:
    load_settings()
    configure_logging()
    args = _parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
