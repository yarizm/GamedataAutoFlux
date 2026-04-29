"""Small browser behavior simulation helpers for SteamDB pages."""

from __future__ import annotations

import random
import time
from typing import Any


class HumanBehaviorSimulator:
    async def random_scroll(self, page: Any, min_scrolls: int = 1, max_scrolls: int = 3) -> None:
        for _ in range(random.randint(min_scrolls, max_scrolls)):
            await page.mouse.wheel(0, random.randint(250, 900))
            await page.wait_for_timeout(random.randint(300, 1200))

    async def random_mouse_move(self, page: Any) -> None:
        viewport = page.viewport_size or {"width": 1280, "height": 720}
        await page.mouse.move(
            random.randint(40, max(41, int(viewport["width"]) - 40)),
            random.randint(40, max(41, int(viewport["height"]) - 40)),
            steps=random.randint(5, 18),
        )

    async def simulate_reading(self, page: Any, min_time: float = 2.0, max_time: float = 5.0) -> None:
        await page.wait_for_timeout(int(random.uniform(min_time, max_time) * 1000))

    async def after_navigation(self, page: Any) -> None:
        await self.random_mouse_move(page)
        await self.random_scroll(page)
        await self.simulate_reading(page)

    def random_scroll_sync(self, page: Any, min_scrolls: int = 1, max_scrolls: int = 3) -> None:
        for _ in range(random.randint(min_scrolls, max_scrolls)):
            page.mouse.wheel(0, random.randint(250, 900))
            page.wait_for_timeout(random.randint(300, 1200))

    def random_mouse_move_sync(self, page: Any) -> None:
        viewport = page.viewport_size or {"width": 1280, "height": 720}
        page.mouse.move(
            random.randint(40, max(41, int(viewport["width"]) - 40)),
            random.randint(40, max(41, int(viewport["height"]) - 40)),
            steps=random.randint(5, 18),
        )

    def simulate_reading_sync(self, page: Any, min_time: float = 2.0, max_time: float = 5.0) -> None:
        time.sleep(random.uniform(min_time, max_time))

    def after_navigation_sync(self, page: Any) -> None:
        self.random_mouse_move_sync(page)
        self.random_scroll_sync(page)
        self.simulate_reading_sync(page)
