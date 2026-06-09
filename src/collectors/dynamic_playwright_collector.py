import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, Optional

from loguru import logger
from src.collectors.base import BaseCollector, CollectTarget, CollectResult
from src.core.sensitive import redact_sensitive_text
from src.core.registry import registry


def _safe_log_text(value: Any) -> str:
    return redact_sensitive_text(str(value or ""))


@registry.register("collector", "dynamic_playwright")
class DynamicPlaywrightCollector(BaseCollector):
    """A generic, config-driven Playwright collector using an isolated worker thread."""

    _SINGLE_EXECUTOR: Optional[ThreadPoolExecutor] = None

    @classmethod
    def _get_executor(cls) -> ThreadPoolExecutor:
        if cls._SINGLE_EXECUTOR is None:
            cls._SINGLE_EXECUTOR = ThreadPoolExecutor(max_workers=1)
        return cls._SINGLE_EXECUTOR

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self._pw_mgr = None
        self._pw = None
        self._browser = None
        self._context = None
        self._worker_loop = None
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
            logger.error(f"Invalid extraction_mode: {_safe_log_text(mode)}")
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

        def _worker_setup():
            if sys.platform == "win32":
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)

            async def _do_setup():
                from playwright.async_api import async_playwright

                pw_mgr = async_playwright()
                pw = await pw_mgr.start()
                browser = await pw.chromium.launch(headless=self.config.get("headless", True))
                ua = self.config.get(
                    "user_agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
                )
                context = await browser.new_context(user_agent=ua)
                return pw_mgr, pw, browser, context

            pw_mgr, pw, browser, context = new_loop.run_until_complete(_do_setup())
            return new_loop, pw_mgr, pw, browser, context

        (
            self._worker_loop,
            self._pw_mgr,
            self._pw,
            self._browser,
            self._context,
        ) = await loop.run_in_executor(executor, _worker_setup)

    async def teardown(self) -> None:
        if not self._worker_loop:
            return

        loop = asyncio.get_running_loop()
        executor = self._get_executor()

        def _worker_teardown():
            async def _do_teardown():
                if self._context:
                    await self._context.close()
                if self._browser:
                    await self._browser.close()
                if self._pw:
                    await self._pw.stop()

            try:
                self._worker_loop.run_until_complete(_do_teardown())
            except Exception as e:
                logger.error(f"[dynamic_playwright] Error during teardown: {_safe_log_text(e)}")
            finally:
                try:
                    self._worker_loop.close()
                except Exception:
                    pass

        await loop.run_in_executor(executor, _worker_teardown)
        self._context = None
        self._browser = None
        self._pw = None
        self._pw_mgr = None
        self._worker_loop = None

        cls = self.__class__
        if cls._SINGLE_EXECUTOR is not None:
            try:
                cls._SINGLE_EXECUTOR.shutdown(wait=False)
            except Exception as e:
                logger.error(
                    f"[dynamic_playwright] Error shutting down executor: {_safe_log_text(e)}"
                )
            finally:
                cls._SINGLE_EXECUTOR = None

    async def collect(self, target: CollectTarget) -> CollectResult:
        if not self._worker_loop:
            return CollectResult(target=target, success=False, error="Browser not initialized")

        loop = asyncio.get_running_loop()
        executor = self._get_executor()
        config = self.config

        def _worker_collect():
            async def _do_collect():
                url_template = config.get("url", "")
                try:
                    url = url_template.format(**target.params)
                except KeyError as e:
                    return CollectResult(
                        target=target, success=False, error=f"Missing URL param: {e}"
                    )

                page = await self._context.new_page()
                try:
                    from src.web.safety import validate_url_runtime
                    validate_url_runtime(url)
                    logger.info(f"[dynamic_playwright] Navigating to: {_safe_log_text(url)}")
                    await page.goto(
                        url, wait_until="domcontentloaded", timeout=config.get("timeout_ms", 30000)
                    )

                    wait_strategy = config.get("wait_strategy", {})
                    if isinstance(wait_strategy, dict):
                        stype = wait_strategy.get("type", "")
                        timeout = wait_strategy.get("timeout_ms", 10000)
                        if stype == "selector":
                            sel = wait_strategy.get("selector", "")
                            if sel:
                                await page.wait_for_selector(sel, timeout=timeout)
                        elif stype == "networkidle":
                            await page.wait_for_load_state("networkidle", timeout=timeout)

                    if config.get("scroll_to_bottom", False):
                        delay_ms = config.get("scroll_delay_ms", 500)
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(delay_ms / 1000.0)

                    data = await _async_extract(page, config)
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
                except Exception as e:
                    logger.error(
                        "[dynamic_playwright] Error collecting "
                        f"{_safe_log_text(url)}: {_safe_log_text(e)}"
                    )
                    # Ensure str(e) is never empty, as empty strings cause logging issues upstream
                    err_msg = str(e) if str(e).strip() else repr(e)
                    return CollectResult(target=target, success=False, error=err_msg)
                finally:
                    await page.close()

            return self._worker_loop.run_until_complete(_do_collect())

        return await loop.run_in_executor(executor, _worker_collect)


async def _async_extract(page, config: dict) -> Dict[str, Any]:
    """Extract data using async Playwright page."""
    mode = config.get("extraction_mode", "css_selectors")

    if mode == "js_evaluate":
        js_script = config.get("js_script", "")
        return await page.evaluate(js_script)

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
                elements = await page.query_selector_all(selector)
                values = []
                for el in elements:
                    if attribute == "innerText":
                        values.append(await el.inner_text())
                    elif attribute == "textContent":
                        values.append(await el.text_content())
                    else:
                        values.append(await el.get_attribute(attribute))
                result[field_name] = values
            else:
                element = await page.query_selector(selector)
                if element:
                    if attribute == "innerText":
                        result[field_name] = await element.inner_text()
                    elif attribute == "textContent":
                        result[field_name] = await element.text_content()
                    else:
                        result[field_name] = await element.get_attribute(attribute)
                else:
                    result[field_name] = None
        return result

    return {}
