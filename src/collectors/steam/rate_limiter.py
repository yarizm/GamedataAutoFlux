"""Adaptive delay helper for SteamDB collection."""

from __future__ import annotations

import asyncio
import random
import time

from loguru import logger


class AdaptiveRateLimiter:
    """Human-ish request pacing with light backoff after blocks/challenges."""

    def __init__(
        self,
        base_delay: float = 8.0,
        jitter_std: float = 4.0,
        min_delay: float = 3.0,
        max_delay: float = 30.0,
        backoff_factor: float = 1.5,
        cooldown_on_block: float = 60.0,
        max_requests_per_session: int = 50,
    ):
        self.base_delay = base_delay
        self.jitter_std = jitter_std
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.cooldown_on_block = cooldown_on_block
        self.max_requests_per_session = max_requests_per_session
        self.request_count = 0
        self.consecutive_events = 0
        self.cooldown_until = 0.0

    async def wait(self, reason: str = "") -> float:
        delay = self._next_delay()
        logger.debug(f"[SteamDB] wait {delay:.1f}s {f'({reason})' if reason else ''}")
        await asyncio.sleep(delay)
        self.request_count += 1
        return delay

    def wait_sync(self, reason: str = "") -> float:
        delay = self._next_delay()
        logger.debug(f"[SteamDB] wait {delay:.1f}s {f'({reason})' if reason else ''}")
        time.sleep(delay)
        self.request_count += 1
        return delay

    def report_success(self) -> None:
        self.consecutive_events = max(0, self.consecutive_events - 1)

    def report_blocked(self) -> None:
        self.consecutive_events += 1
        self.cooldown_until = max(self.cooldown_until, time.monotonic() + self.cooldown_on_block)

    def report_challenge(self) -> None:
        self.consecutive_events += 2
        self.cooldown_until = max(self.cooldown_until, time.monotonic() + self.cooldown_on_block * 2)

    def _next_delay(self) -> float:
        now = time.monotonic()
        cooldown = max(0.0, self.cooldown_until - now)
        gaussian = random.gauss(0, self.jitter_std)
        backoff = self.backoff_factor ** max(0, self.consecutive_events)
        delay = (self.base_delay + gaussian) * backoff
        return min(max(delay, self.min_delay) + cooldown, self.max_delay + cooldown)
