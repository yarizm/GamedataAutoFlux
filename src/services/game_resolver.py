"""游戏标识符解析服务 — 使用 Playwright + API 自动搜索各平台游戏 ID"""

from __future__ import annotations

import asyncio
import json
import random
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from loguru import logger

from src.agent.schemas import (
    IdentifierCandidate,
    IdentifierConfidence,
    IdentifierResult,
    GameIdentifiers,
)
from src.core.config import get as get_config

# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
]

STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en'] });
window.chrome = { runtime: {} };
"""

GAME_RESOLVER_CACHE = Path("data/game_resolver_cache.json")
CACHE_TTL_SECONDS = 3600

_names_cache: dict[str, tuple[float, IdentifierResult]] = {}
_cache_loaded = False


def _load_cache() -> None:
    global _names_cache, _cache_loaded
    if _cache_loaded:
        return
    _cache_loaded = True
    if not GAME_RESOLVER_CACHE.exists():
        return
    try:
        raw = json.loads(GAME_RESOLVER_CACHE.read_text(encoding="utf-8"))
        now = asyncio.get_event_loop().time()
        for key, (ts, data) in raw.items():
            if now - ts < CACHE_TTL_SECONDS:
                _names_cache[key] = (ts, IdentifierResult.model_validate(data))
    except Exception:
        pass


def _save_cache() -> None:
    try:
        GAME_RESOLVER_CACHE.parent.mkdir(parents=True, exist_ok=True)
        raw = {
            key: (ts, result.model_dump(mode="json")) for key, (ts, result) in _names_cache.items()
        }
        GAME_RESOLVER_CACHE.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _cache_key(platform: str, game_name: str) -> str:
    return f"{platform}:{game_name.strip().lower()}"


def _should_use_threaded_playwright() -> bool:
    if sys.platform != "win32":
        return False
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return False
    return "Selector" in loop.__class__.__name__


# ---------------------------------------------------------------------------
# 解析器
# ---------------------------------------------------------------------------


class GameIdentifierResolver:
    """封装 Playwright + HTTP 的多平台游戏标识符搜索。"""

    def __init__(self) -> None:
        self._pw: Any = None
        self._browser: Any = None
        self._browser_is_cdp: bool = False
        self._lock = asyncio.Lock()

    # ---- 生命周期 ---------------------------------------------------------

    async def setup(self) -> None:
        if self._browser is not None:
            return
        _load_cache()
        async with self._lock:
            if self._browser is not None:
                return
            from playwright.async_api import async_playwright

            self._pw = await async_playwright().start()
            cdp_port = int(get_config("qimai.cdp_port", 9222))
            try:
                self._browser = await self._pw.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{cdp_port}"
                )
                self._browser_is_cdp = True
                logger.info(f"[GameResolver] 已通过 CDP 端口 {cdp_port} 连接浏览器")
                return
            except Exception as exc:
                logger.debug(f"[GameResolver] CDP 连接失败，启动新浏览器: {exc}")

            self._browser = await self._pw.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
            logger.info("[GameResolver] 已启动新 Playwright 浏览器")

    async def teardown(self) -> None:
        if self._browser and not self._browser_is_cdp:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._pw:
            try:
                await self._pw.stop()
            except Exception:
                pass
        self._browser = None
        self._pw = None
        _save_cache()

    async def _safe_resolve(
        self, resolver, game_name: str, platform: str
    ) -> IdentifierResult | None:
        """带异常保护的解析器调用。"""
        try:
            return await resolver(game_name)
        except Exception as exc:
            logger.warning(f"[GameResolver] {platform} 解析失败: {exc}")
            return None

    # ---- 新上下文辅助 ------------------------------------------------------

    async def _new_context(self):
        return await self._browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=random.choice(USER_AGENTS),
            locale="zh-CN",
        )

    async def _new_stealth_context(self):
        ctx = await self._new_context()
        await ctx.add_init_script(STEALTH_SCRIPT)
        return ctx

    # ---- 主入口 -----------------------------------------------------------

    async def resolve_all(
        self, game_name: str, platforms: list[str] | None = None
    ) -> GameIdentifiers:
        await self.setup()
        if platforms is None:
            platforms = ["steam", "taptap", "monitor", "official_site", "gtrends"]
        if "qimai" not in platforms and self._browser_is_cdp:
            platforms.append("qimai")

        resolvers: dict[str, Any] = {
            "steam": self.resolve_steam,
            "taptap": self.resolve_taptap,
            "qimai": self.resolve_qimai,
            "monitor": self.resolve_monitor_name,
            "official_site": self.resolve_official_site,
            "gtrends": self.resolve_gtrends,
        }

        results: dict[str, IdentifierResult | None] = {}
        tasks = {}
        for platform in platforms:
            resolver = resolvers.get(platform)
            if resolver is None:
                continue
            tasks[platform] = asyncio.create_task(self._safe_resolve(resolver, game_name, platform))

        for platform, task in tasks.items():
            try:
                results[platform] = await task
            except Exception as exc:
                logger.warning(f"[GameResolver] {platform} 解析失败: {exc}")
                results[platform] = None

        kwargs: dict[str, Any] = {"game_name": game_name}
        for platform in platforms:
            kwargs[platform] = results.get(platform)
        return GameIdentifiers(**kwargs)

    # ---- Steam ------------------------------------------------------------

    async def resolve_steam(self, game_name: str) -> IdentifierResult | None:
        cached = _names_cache.get(_cache_key("steam", game_name))
        if cached:
            logger.debug(f"[GameResolver] Steam 缓存命中: {game_name}")
            return cached[1]

        candidates: list[IdentifierCandidate] = []
        seen_ids: set[str] = set()

        async with httpx.AsyncClient(
            timeout=10,
            follow_redirects=True,
            headers={"User-Agent": random.choice(USER_AGENTS)},
        ) as client:
            # Community SearchApps
            try:
                resp = await client.get(
                    f"https://steamcommunity.com/actions/SearchApps/{quote(game_name)}"
                )
                if resp.status_code == 200 and isinstance(resp.json(), list):
                    for item in resp.json():
                        app_id = str(item.get("appid", ""))
                        name = str(item.get("name", ""))
                        if app_id and name and app_id not in seen_ids:
                            seen_ids.add(app_id)
                            candidates.append(
                                IdentifierCandidate(
                                    identifier=app_id,
                                    identifier_type="steam_app_id",
                                    name=name,
                                    source="api",
                                )
                            )
            except Exception:
                pass

            # Store search EN
            try:
                resp = await client.get(
                    "https://store.steampowered.com/api/storesearch/",
                    params={"term": game_name, "l": "english", "cc": "us"},
                )
                if resp.status_code == 200:
                    for item in resp.json().get("items", []) or []:
                        app_id = str(item.get("id", ""))
                        name = str(item.get("name", ""))
                        if app_id and name and app_id not in seen_ids:
                            seen_ids.add(app_id)
                            candidates.append(
                                IdentifierCandidate(
                                    identifier=app_id,
                                    identifier_type="steam_app_id",
                                    name=name,
                                    source="api",
                                )
                            )
            except Exception:
                pass

        result = _build_platform_result("steam", "steam_app_id", game_name, candidates)
        if result:
            _names_cache[_cache_key("steam", game_name)] = (asyncio.get_event_loop().time(), result)
        return result

    # ---- TapTap -----------------------------------------------------------

    async def resolve_taptap(self, game_name: str) -> IdentifierResult | None:
        cached = _names_cache.get(_cache_key("taptap", game_name))
        if cached:
            return cached[1]
        await self.setup()

        if _should_use_threaded_playwright():
            return await asyncio.to_thread(self._resolve_taptap_sync, game_name)

        candidates = await self._resolve_taptap_async(game_name)
        result = _build_platform_result("taptap", "taptap_app_id", game_name, candidates)
        if result:
            result.url = (
                f"https://www.taptap.cn/app/{result.identifier}" if result.identifier else ""
            )
            _names_cache[_cache_key("taptap", game_name)] = (
                asyncio.get_event_loop().time(),
                result,
            )
        return result

    async def _resolve_taptap_async(self, game_name: str) -> list[IdentifierCandidate]:
        ctx = await self._new_stealth_context()
        try:
            page = await ctx.new_page()
            search_url = f"https://www.taptap.cn/search/{quote(game_name)}"
            await page.goto(search_url, wait_until="domcontentloaded", timeout=25000)
            await page.wait_for_timeout(3000)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.5)")
            await page.wait_for_timeout(1500)

            cards = await page.query_selector_all("a[href*='/app/']")
            candidates: list[IdentifierCandidate] = []
            seen: set[str] = set()
            for card in cards[:15]:
                href = (await card.get_attribute("href")) or ""
                text = (await card.inner_text() or "").strip()
                match = re.search(r"/app/(\d+)", href)
                if match and match.group(1) not in seen:
                    app_id = match.group(1)
                    seen.add(app_id)
                    name = text.split("\n")[0].strip()[:80] if text else ""
                    candidates.append(
                        IdentifierCandidate(
                            identifier=app_id,
                            identifier_type="taptap_app_id",
                            name=name,
                            source="playwright",
                        )
                    )
            return candidates
        finally:
            await ctx.close()

    @staticmethod
    def _resolve_taptap_sync(game_name: str) -> IdentifierResult | None:
        from playwright.sync_api import sync_playwright

        candidates: list[IdentifierCandidate] = []
        seen: set[str] = set()
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                ctx = browser.new_context(
                    viewport={"width": 1440, "height": 900},
                    user_agent=random.choice(USER_AGENTS),
                    locale="zh-CN",
                )
                ctx.add_init_script(STEALTH_SCRIPT)
                page = ctx.new_page()
                page.goto(
                    f"https://www.taptap.cn/search/{quote(game_name)}",
                    wait_until="domcontentloaded",
                    timeout=25000,
                )
                page.wait_for_timeout(3000)
                cards = page.query_selector_all("a[href*='/app/']")
                for card in cards[:15]:
                    href = card.get_attribute("href") or ""
                    text = (card.inner_text() or "").strip()
                    match = re.search(r"/app/(\d+)", href)
                    if match and match.group(1) not in seen:
                        app_id = match.group(1)
                        seen.add(app_id)
                        name = text.split("\n")[0].strip()[:80] if text else ""
                        candidates.append(
                            IdentifierCandidate(
                                identifier=app_id,
                                identifier_type="taptap_app_id",
                                name=name,
                                source="playwright",
                            )
                        )
                ctx.close()
                browser.close()
        except Exception as exc:
            logger.warning(f"[GameResolver] TapTap sync 失败: {exc}")
            return None
        return _build_platform_result("taptap", "taptap_app_id", game_name, candidates)

    # ---- Qimai ------------------------------------------------------------

    async def resolve_qimai(self, game_name: str) -> IdentifierResult | None:
        cached = _names_cache.get(_cache_key("qimai", game_name))
        if cached:
            return cached[1]
        await self.setup()

        if not self._browser_is_cdp:
            logger.debug("[GameResolver] Qimai 需要 CDP 会话，跳过")
            return None

        if _should_use_threaded_playwright():
            return await asyncio.to_thread(self._resolve_qimai_sync, game_name)
        return await self._resolve_qimai_async(game_name)

    async def _resolve_qimai_async(self, game_name: str) -> IdentifierResult | None:
        contexts = self._browser.contexts
        ctx = contexts[0] if contexts else await self._new_context()
        try:
            page = await ctx.new_page()
            await page.goto(
                f"https://www.qimai.cn/search?search={quote(game_name)}",
                wait_until="domcontentloaded",
                timeout=45000,
            )
            await page.wait_for_timeout(5000)

            candidates: list[IdentifierCandidate] = []
            seen: set[str] = set()
            for selector in (
                ".applistRow a[href*='appid']",
                ".search-result-item a[href*='appid']",
                "a[href*='appid']",
            ):
                links = await page.query_selector_all(selector)
                for link in links[:10]:
                    href = (await link.get_attribute("href")) or ""
                    text = (await link.inner_text() or "").strip()
                    match = re.search(r"appid(\d+)", href)
                    if match and match.group(1) not in seen:
                        app_id = match.group(1)
                        seen.add(app_id)
                        name = text.split("\n")[0].strip()[:80] if text else ""
                        candidates.append(
                            IdentifierCandidate(
                                identifier=app_id,
                                identifier_type="qimai_app_id",
                                name=name,
                                source="playwright",
                            )
                        )
                if candidates:
                    break
            await page.close()
            result = _build_platform_result("qimai", "qimai_app_id", game_name, candidates)
            if result:
                _names_cache[_cache_key("qimai", game_name)] = (
                    asyncio.get_event_loop().time(),
                    result,
                )
            return result
        finally:
            if not self._browser_is_cdp:
                await ctx.close()

    @staticmethod
    def _resolve_qimai_sync(game_name: str) -> IdentifierResult | None:
        logger.debug("[GameResolver] Qimai sync 搜索暂不支持")
        return None

    # ---- Monitor (SullyGnome) ---------------------------------------------

    async def resolve_monitor_name(self, game_name: str) -> IdentifierResult | None:
        cached = _names_cache.get(_cache_key("monitor", game_name))
        if cached:
            return cached[1]

        # 检查覆盖表
        from src.collectors.monitor_collector import _resolve_sully_siteurl_override

        override = _resolve_sully_siteurl_override(0, game_name, None)
        if override:
            result = IdentifierResult(
                platform="monitor",
                identifier=override,
                identifier_type="siteurl",
                game_name=game_name,
                confidence=IdentifierConfidence.HIGH,
                source="config",
                url=f"https://sullygnome.com/game/{override}",
            )
            _names_cache[_cache_key("monitor", game_name)] = (
                asyncio.get_event_loop().time(),
                result,
            )
            return result

        # 搜索变体
        from src.collectors.monitor_collector import _generate_search_variants

        search_names = _generate_search_variants(game_name, None)

        candidates: list[IdentifierCandidate] = []
        seen_siteurls: set[str] = set()
        async with httpx.AsyncClient(timeout=10) as client:
            for name in search_names:
                try:
                    resp = await client.get(
                        f"https://sullygnome.com/api/standardsearch/{quote(name)}"
                    )
                    if resp.status_code != 200:
                        continue
                    items = resp.json()
                    if not isinstance(items, list):
                        continue
                    for item in items:
                        if not isinstance(item, dict) or item.get("itemtype") != 2:
                            continue
                        siteurl = str(item.get("siteurl", "")).strip()
                        display = str(item.get("displaytext", "")).strip()
                        if not siteurl or siteurl in seen_siteurls:
                            continue
                        seen_siteurls.add(siteurl)
                        candidates.append(
                            IdentifierCandidate(
                                identifier=siteurl,
                                identifier_type="siteurl",
                                name=display,
                                source="api",
                            )
                        )
                except Exception:
                    continue

        result = _build_platform_result("monitor", "siteurl", game_name, candidates)
        if result:
            result.url = (
                f"https://sullygnome.com/game/{result.identifier}" if result.identifier else ""
            )
            _names_cache[_cache_key("monitor", game_name)] = (
                asyncio.get_event_loop().time(),
                result,
            )
        return result

    # ---- Official Site ----------------------------------------------------

    async def resolve_official_site(self, game_name: str) -> IdentifierResult | None:
        cached = _names_cache.get(_cache_key("official_site", game_name))
        if cached:
            return cached[1]

        # 已知 recipes
        recipes = get_config("official_site.recipes", {})
        aliases = get_config("official_site.recipe_aliases", {})
        canon = aliases.get(game_name, game_name)
        if canon in recipes:
            entry = recipes[canon]
            url = entry if isinstance(entry, str) else entry.get("official_url", "")
            if url:
                result = IdentifierResult(
                    platform="official_site",
                    identifier=url,
                    identifier_type="official_url",
                    game_name=canon,
                    confidence=IdentifierConfidence.HIGH,
                    source="config",
                    url=url,
                )
                _names_cache[_cache_key("official_site", game_name)] = (
                    asyncio.get_event_loop().time(),
                    result,
                )
                return result

        # DuckDuckGo 搜索
        await self.setup()
        try:
            ctx = await self._new_stealth_context()
            page = await ctx.new_page()
            try:
                await page.goto(
                    f"https://html.duckduckgo.com/html/?q={quote(game_name + ' 官网')}",
                    wait_until="domcontentloaded",
                    timeout=15000,
                )
                await page.wait_for_timeout(2000)

                candidates: list[IdentifierCandidate] = []
                seen: set[str] = set()
                for selector in (".result__a", ".result__url", "a.result__a"):
                    links = await page.query_selector_all(selector)
                    for link in links[:8]:
                        href = (await link.get_attribute("href")) or ""
                        text = (await link.inner_text() or "").strip()
                        if (
                            href
                            and href not in seen
                            and not any(
                                skip in href
                                for skip in ("baike.", "wiki.", "zhihu.", "bilibili.", "douban.")
                            )
                        ):
                            seen.add(href)
                            candidates.append(
                                IdentifierCandidate(
                                    identifier=href,
                                    identifier_type="official_url",
                                    name=text[:80],
                                    source="playwright",
                                )
                            )
                    if candidates:
                        break

                return _build_platform_result(
                    "official_site", "official_url", game_name, candidates
                )
            finally:
                await ctx.close()
        except Exception as exc:
            logger.warning(f"[GameResolver] Official site 搜索失败: {exc}")
            return None

    # ---- GTrends ----------------------------------------------------------

    async def resolve_gtrends(self, game_name: str) -> IdentifierResult:
        return IdentifierResult(
            platform="gtrends",
            identifier=game_name,
            identifier_type="keyword",
            game_name=game_name,
            confidence=IdentifierConfidence.HIGH,
            source="direct",
        )

    # ---- 验证 --------------------------------------------------------------

    async def verify_identifier(
        self, platform: str, identifier: str, game_name: str
    ) -> dict[str, Any]:
        if platform == "steam":
            return await self._verify_steam(identifier, game_name)
        if platform == "taptap":
            return await self._verify_taptap(identifier, game_name)
        if platform == "qimai":
            return await self._verify_qimai(identifier, game_name)
        if platform == "monitor":
            return await self._verify_monitor(identifier, game_name)
        if platform == "official_site":
            return await self._verify_official_site(identifier, game_name)
        return {"valid": False, "error": f"Unknown platform: {platform}"}

    async def _verify_steam(self, app_id: str, game_name: str) -> dict[str, Any]:
        async with httpx.AsyncClient(
            timeout=10,
            follow_redirects=True,
            headers={"User-Agent": random.choice(USER_AGENTS)},
        ) as client:
            try:
                resp = await client.get(
                    "https://store.steampowered.com/api/appdetails",
                    params={"appids": app_id},
                )
                entry = (resp.json() or {}).get(app_id, {})
                if entry.get("success"):
                    name = entry["data"].get("name", "")
                    sim = SequenceMatcher(None, game_name.lower(), name.lower()).ratio()
                    return {
                        "valid": True,
                        "platform": "steam",
                        "app_id": int(app_id),
                        "name": name,
                        "similarity": round(sim, 3),
                        "confidence": "high" if sim >= 0.8 else "medium" if sim >= 0.5 else "low",
                    }
                return {
                    "valid": False,
                    "platform": "steam",
                    "app_id": app_id,
                    "error": "App not found",
                }
            except Exception as e:
                return {"valid": False, "platform": "steam", "app_id": app_id, "error": str(e)}

    async def _verify_taptap(self, app_id: str, game_name: str) -> dict[str, Any]:
        await self.setup()
        async with await self._new_stealth_context() as ctx:
            try:
                page = await ctx.new_page()
                await page.goto(
                    f"https://www.taptap.cn/app/{app_id}",
                    wait_until="domcontentloaded",
                    timeout=20000,
                )
                await page.wait_for_timeout(2000)
                title = (await page.title() or "").strip()
                sim = SequenceMatcher(None, game_name.lower(), title.lower()).ratio()
                return {
                    "valid": sim >= 0.3,
                    "platform": "taptap",
                    "app_id": app_id,
                    "page_title": title,
                    "similarity": round(sim, 3),
                    "confidence": "high" if sim >= 0.8 else "medium" if sim >= 0.5 else "low",
                }
            except Exception as e:
                return {"valid": False, "platform": "taptap", "app_id": app_id, "error": str(e)}

    async def _verify_qimai(self, app_id: str, game_name: str) -> dict[str, Any]:
        return {
            "valid": True,
            "platform": "qimai",
            "app_id": app_id,
            "note": "七麦验证需要已登录浏览器会话，当前信任搜索结果的最高匹配",
            "confidence": "medium",
        }

    async def _verify_monitor(self, siteurl: str, game_name: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10) as client:
            try:
                resp = await client.get(f"https://sullygnome.com/game/{siteurl}/90/summary")
                if resp.status_code == 200 and "PageInfo" in resp.text:
                    return {
                        "valid": True,
                        "platform": "monitor",
                        "siteurl": siteurl,
                        "confidence": "high",
                    }
                return {
                    "valid": False,
                    "platform": "monitor",
                    "siteurl": siteurl,
                    "error": "Page not found",
                }
            except Exception as e:
                return {"valid": False, "platform": "monitor", "siteurl": siteurl, "error": str(e)}

    async def _verify_official_site(self, url: str, game_name: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            try:
                resp = await client.get(url)
                title_match = re.search(r"<title>(.*?)</title>", resp.text, re.IGNORECASE)
                page_title = title_match.group(1).strip() if title_match else ""
                sim = SequenceMatcher(None, game_name.lower(), page_title.lower()).ratio()
                return {
                    "valid": resp.status_code < 400,
                    "platform": "official_site",
                    "url": url,
                    "page_title": page_title,
                    "similarity": round(sim, 3),
                    "confidence": "high" if sim >= 0.3 else "low",
                }
            except Exception as e:
                return {"valid": False, "platform": "official_site", "url": url, "error": str(e)}


# ---------------------------------------------------------------------------
# 公共辅助
# ---------------------------------------------------------------------------


def _build_platform_result(
    platform: str,
    id_type: str,
    game_name: str,
    candidates: list[IdentifierCandidate],
) -> IdentifierResult | None:
    if not candidates:
        return IdentifierResult(
            platform=platform,
            identifier="",
            identifier_type=id_type,
            game_name=game_name,
            confidence=IdentifierConfidence.LOW,
            source="none",
            status="not_found",
            detail=f"未找到 {platform} 相关结果",
        )

    name_lower = game_name.lower().strip()
    exact: list[IdentifierCandidate] = []
    fuzzy: list[IdentifierCandidate] = []
    for candidate in candidates:
        cn = candidate.name.lower().strip()
        sim = SequenceMatcher(None, name_lower, cn).ratio()
        candidate.similarity = sim
        if cn == name_lower or name_lower in cn or cn in name_lower:
            exact.append(candidate)
        else:
            fuzzy.append(candidate)

    fuzzy.sort(key=lambda c: c.similarity or 0, reverse=True)
    all_sorted = exact + fuzzy
    best = all_sorted[0]

    if exact:
        confidence = IdentifierConfidence.HIGH
        status = "found"
        detail = ""
    elif best.similarity and best.similarity >= 0.6:
        confidence = IdentifierConfidence.MEDIUM
        status = "multiple_candidates"
        detail = "名称部分匹配，可能存在歧义"
    else:
        confidence = IdentifierConfidence.LOW
        status = "multiple_candidates"
        detail = "未找到高匹配结果，可能需要人工确认"

    return IdentifierResult(
        platform=platform,
        identifier=best.identifier,
        identifier_type=id_type,
        game_name=best.name,
        confidence=confidence,
        source=best.source,
        candidates=all_sorted[:10],
        status=status,
        detail=detail,
    )
