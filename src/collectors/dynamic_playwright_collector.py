import asyncio
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, Optional

from loguru import logger
from playwright.sync_api import sync_playwright
from src.collectors.base import BaseCollector, CollectTarget, CollectResult
from src.core.registry import registry


def _should_use_threaded_playwright() -> bool:
    if sys.platform != "win32":
        return False
    try:
        loop = asyncio.get_running_loop()
        return isinstance(loop, asyncio.SelectorEventLoop)
    except RuntimeError:
        return False


@registry.register("collector", "dynamic_playwright")
class DynamicPlaywrightCollector(BaseCollector):
    """A generic, config-driven Playwright collector.

    On Windows SelectorEventLoop (which can't spawn subprocesses), it uses
    sync_playwright in a dedicated single-thread executor — same pattern
    as the existing steamdb / qimai / taptap scrapers.
    """

    _SINGLE_EXECUTOR: Optional[ThreadPoolExecutor] = None

    @classmethod
    def _get_executor(cls) -> ThreadPoolExecutor:
        if cls._SINGLE_EXECUTOR is None:
            cls._SINGLE_EXECUTOR = ThreadPoolExecutor(max_workers=1)
        return cls._SINGLE_EXECUTOR

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self._pw = None
        self._browser = None
        self._context = None
        if self.config:
            self._normalize_config(self.config)

    @staticmethod
    def _normalize_config(config: Dict[str, Any]) -> None:
        """Normalize config from Agent-written shorthand to the expected dict format."""
        ws = config.get("wait_strategy")
        if isinstance(ws, str):
            config["wait_strategy"] = {"type": ws, "timeout_ms": 10000}
        elif isinstance(ws, dict) and "type" not in ws:
            config["wait_strategy"] = {"type": "networkidle", "timeout_ms": 10000, **ws}
        elif not isinstance(ws, dict):
            config["wait_strategy"] = {}

        fields = config.get("fields")
        if isinstance(fields, list):
            normalized = {}
            for item in fields:
                if isinstance(item, dict):
                    name = item.get("name", item.get("field", ""))
                    if name:
                        normalized[name] = {
                            "selector": item.get("selector", item.get("css", "")),
                            "attribute": item.get("attribute", item.get("attr", "innerText")),
                            "multiple": item.get("multiple", False),
                        }
            config["fields"] = normalized if normalized else {}
        elif not isinstance(fields, dict):
            config["fields"] = {}

    def validate_config(self, config: Dict[str, Any]) -> bool:
        if not config:
            return False
        if "url" not in config:
            logger.error("DynamicPlaywrightCollector config missing 'url' template.")
            return False
        mode = config.get("extraction_mode", "css_selectors")
        if mode not in ["css_selectors", "js_evaluate"]:
            logger.error(f"Invalid extraction_mode: {mode}")
            return False
        if mode == "css_selectors" and "fields" not in config:
            logger.error("extraction_mode is css_selectors but 'fields' is missing.")
            return False
        if mode == "js_evaluate" and "js_script" not in config:
            logger.error("extraction_mode is js_evaluate but 'js_script' is missing.")
            return False
        return True

    async def setup(self, config: Dict[str, Any] = None) -> None:
        loop = asyncio.get_running_loop()
        executor = self._get_executor()

        def _sync_setup():
            pw = sync_playwright().start()
            browser = pw.chromium.launch(headless=self.config.get("headless", True))
            ua = self.config.get(
                "user_agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            )
            context = browser.new_context(user_agent=ua)
            return pw, browser, context

        self._pw, self._browser, self._context = await loop.run_in_executor(executor, _sync_setup)

    async def teardown(self) -> None:
        if self._pw is None:
            return

        loop = asyncio.get_running_loop()
        executor = self._get_executor()

        def _sync_teardown():
            if self._context:
                self._context.close()
            if self._browser:
                self._browser.close()
            if self._pw:
                self._pw.stop()

        await loop.run_in_executor(executor, _sync_teardown)
        self._context = None
        self._browser = None
        self._pw = None

    async def collect(self, target: CollectTarget) -> CollectResult:
        if not self._context:
            return CollectResult(target=target, success=False, error="Browser not initialized")

        loop = asyncio.get_running_loop()
        executor = self._get_executor()
        config = self.config

        def _sync_collect() -> CollectResult:
            url_template = config.get("url", "")
            try:
                url = url_template.format(**target.params)
            except KeyError as e:
                return CollectResult(target=target, success=False, error=f"Missing URL param: {e}")

            page = self._context.new_page()
            try:
                logger.info(f"[dynamic_playwright] Navigating to: {url}")
                page.goto(url, wait_until="domcontentloaded", timeout=config.get("timeout_ms", 30000))

                wait_strategy = config.get("wait_strategy", {})
                if isinstance(wait_strategy, dict):
                    stype = wait_strategy.get("type", "")
                    timeout = wait_strategy.get("timeout_ms", 10000)
                    if stype == "selector":
                        sel = wait_strategy.get("selector", "")
                        if sel:
                            page.wait_for_selector(sel, timeout=timeout)
                    elif stype == "networkidle":
                        page.wait_for_load_state("networkidle", timeout=timeout)

                if config.get("scroll_to_bottom", False):
                    delay_ms = config.get("scroll_delay_ms", 500)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(delay_ms / 1000.0)

                data = _sync_extract(page, config)
                return CollectResult(
                    target=target,
                    success=True,
                    data=data,
                    metadata={
                        "collector": "dynamic_playwright",
                        "url": url,
                        "timestamp": datetime.now().isoformat(),
                    },
                )
            finally:
                page.close()

        return await loop.run_in_executor(executor, _sync_collect)


def _sync_extract(page, config: dict) -> Dict[str, Any]:
    """Extract data using sync Playwright page."""
    mode = config.get("extraction_mode", "css_selectors")

    if mode == "js_evaluate":
        js_script = config.get("js_script", "")
        return page.evaluate(js_script)

    elif mode == "css_selectors":
        fields = config.get("fields", {})
        if not isinstance(fields, dict):
            return {}
        result = {}
        for field_name, field_config in fields.items():
            if not isinstance(field_config, dict):
                continue
            selector = field_config.get("selector", "")
            attribute = field_config.get("attribute", "innerText")
            multiple = field_config.get("multiple", False)
            if not selector:
                continue

            if multiple:
                elements = page.query_selector_all(selector)
                values = []
                for el in elements:
                    if attribute == "innerText":
                        values.append(el.inner_text())
                    elif attribute == "textContent":
                        values.append(el.text_content())
                    else:
                        values.append(el.get_attribute(attribute))
                result[field_name] = values
            else:
                element = page.query_selector(selector)
                if element:
                    if attribute == "innerText":
                        result[field_name] = element.inner_text()
                    elif attribute == "textContent":
                        result[field_name] = element.text_content()
                    else:
                        result[field_name] = element.get_attribute(attribute)
                else:
                    result[field_name] = None
        return result

    return {}
