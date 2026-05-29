"""
平台标识符相关工具
"""
import json
from pathlib import Path
from typing import ClassVar, Type
from langchain_core.tools import BaseTool
from pydantic import BaseModel

from src.agent.schemas import (
    IdentifierConfidence,
    ResolveSteamAppIdInput,
    SearchGameIdentifiersInput,
    VerifyGameIdentifierInput,
    VerifySteamAppIdInput,
)
from src.agent.tools.utils import _format_result, _safe_json
from loguru import logger

async def _auto_fill_identifiers(targets: list[dict], pipeline_name: str) -> list[dict]:
    """在创建任务前自动发现缺失的平台标识符（仅 HIGH 置信度时自动填充）。"""
    from src.services.game_resolver import GameIdentifierResolver

    needs_resolve = any(
        pipeline_name.startswith(prefix)
        for prefix in ("steam", "taptap", "monitor", "qimai", "official_site")
    )
    if not needs_resolve:
        return targets

    resolver = GameIdentifierResolver()
    try:
        await resolver.setup()
    except (NotImplementedError, RuntimeError, OSError) as e:
        logger.warning(f"标识符自动填充跳过 (Playwright/浏览器不可用): {e}")
        return targets

    try:
        for target in targets:
            params = dict(target.get("params", {}) or {})
            name = str(target.get("name", "") or "").strip()
            if not name:
                continue

            if pipeline_name.startswith("steam") and not params.get("app_id"):
                result = await resolver.resolve_steam(name)
                if result and result.confidence == IdentifierConfidence.HIGH:
                    params["app_id"] = int(result.identifier)
                    target["params"] = params

            elif (
                pipeline_name.startswith("taptap")
                and not params.get("app_id")
                and not params.get("url")
            ):
                result = await resolver.resolve_taptap(name)
                if result and result.confidence == IdentifierConfidence.HIGH:
                    params["app_id"] = result.identifier
                    target["params"] = params

            elif pipeline_name.startswith("monitor") and not params.get("siteurl"):
                result = await resolver.resolve_monitor_name(name)
                if result and result.confidence == IdentifierConfidence.HIGH:
                    params["siteurl"] = result.identifier
                    target["params"] = params

            elif (
                pipeline_name.startswith("qimai")
                and not params.get("app_id")
                and not params.get("qimai_app_id")
            ):
                result = await resolver.resolve_qimai(name)
                if result and result.confidence == IdentifierConfidence.HIGH:
                    params["qimai_app_id"] = result.identifier
                    target["params"] = params

            elif pipeline_name.startswith("official_site") and not params.get("official_url"):
                result = await resolver.resolve_official_site(name)
                if result and result.confidence == IdentifierConfidence.HIGH:
                    params["official_url"] = result.identifier
                    target["params"] = params
    finally:
        await resolver.teardown()
    return targets


class ResolveSteamAppIdTool(BaseTool):
    name: str = "resolve_steam_app_id"
    description: str = (
        "按游戏名称搜索 Steam App ID。支持中文或英文游戏名，返回精确或模糊匹配结果。"
        "创建 Steam 采集任务前必须使用此工具获取正确的 app_id，不要凭记忆猜测。"
    )
    args_schema: Type[BaseModel] = ResolveSteamAppIdInput

    STORE_SEARCH_URL: ClassVar[str] = "https://store.steampowered.com/api/storesearch/"
    COMMUNITY_SEARCH_URL: ClassVar[str] = "https://steamcommunity.com/actions/SearchApps/"

    async def _arun(self, game_name: str) -> str:
        import httpx
        from tenacity import retry, stop_after_attempt, wait_exponential

        all_items: list[dict] = []
        seen_ids: set[int] = set()

        def add_items(items: list[dict], key_id: str = "app_id"):
            for item in items:
                app_id = item.get(key_id, 0)
                name = item.get("name", "")
                if not app_id or not name:
                    continue
                if app_id in seen_ids:
                    continue
                seen_ids.add(app_id)
                all_items.append({"app_id": app_id, "name": name})

        # 添加重试机制防封禁
        @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
        async def fetch_with_retry(client, url, params=None):
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp

        async with httpx.AsyncClient(
            timeout=10,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        ) as client:
            try:
                resp = await fetch_with_retry(client, f"{self.COMMUNITY_SEARCH_URL}{game_name}")
                data = resp.json()
                if isinstance(data, list):
                    add_items(data, key_id="appid")
            except Exception as e:
                logger.warning(f"Steam Community SearchApps error: {e}")

            try:
                resp = await fetch_with_retry(
                    client, self.STORE_SEARCH_URL, params={"term": game_name, "l": "english", "cc": "us"}
                )
                data = resp.json()
                add_items(data.get("items", []), key_id="id")
            except Exception as e:
                logger.warning(f"Steam Store Search(EN) error: {e}")

            try:
                resp = await fetch_with_retry(
                    client, self.STORE_SEARCH_URL, params={"term": game_name, "l": "schinese", "cc": "cn"}
                )
                data = resp.json()
                add_items(data.get("items", []), key_id="id")
            except Exception as e:
                logger.warning(f"Steam Store Search(CN) error: {e}")

        if all_items:
            return _safe_json({"found": True, "source": "steam_api", "results": all_items[:10]})

        cache_file = Path("data/steam_app_list.json")
        if cache_file.exists():
            cache_result = self._search_cache(cache_file, game_name)
            if cache_result:
                return cache_result
            return json.dumps(
                {
                    "found": False,
                    "source": "cache",
                    "message": f"所有在线 API 及本地缓存均未找到 '{game_name}'，请尝试英文名或手动提供 app_id",
                },
                ensure_ascii=False,
            )

        return json.dumps(
            {
                "found": False,
                "error": "所有 Steam 搜索 API 均不可用且本地缓存不存在。"
                "请尝试英文名搜索，或手动提供 app_id。",
            },
            ensure_ascii=False,
        )

    @staticmethod
    def _search_cache(cache_file: Path, game_name: str) -> str | None:
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            apps = data if isinstance(data, list) else data.get("apps", [])
        except Exception:
            return None

        name_lower = game_name.lower().strip()
        exact: list[dict] = []
        fuzzy: list[dict] = []

        for app in apps:
            app_name = app.get("name", "")
            if not app_name:
                continue
            raw_app_id = app.get("appid") or app.get("app_id", 0)
            if app_name.lower().strip() == name_lower:
                exact.append({"app_id": raw_app_id, "name": app_name})
            elif name_lower in app_name.lower():
                fuzzy.append({"app_id": raw_app_id, "name": app_name})
                if len(fuzzy) >= 30:
                    break

        results = exact + fuzzy
        if not results:
            return None

        seen: set[int] = set()
        filtered: list[dict] = []
        for item in results:
            if item["app_id"] in seen:
                continue
            seen.add(item["app_id"])
            filtered.append(item)

        return _safe_json({"found": True, "source": "cache", "results": filtered[:10]})

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")

class VerifySteamAppIdTool(BaseTool):
    name: str = "verify_steam_app_id"
    description: str = (
        "通过 Steam Store API 验证一个 App ID 是否有效，返回游戏名称。"
        "用于确认 resolve_steam_app_id 返回的 app_id 是否正确。"
    )
    args_schema: Type[BaseModel] = VerifySteamAppIdInput

    async def _arun(self, app_id: int) -> str:
        import httpx

        try:
            async with httpx.AsyncClient(
                timeout=10,
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                },
            ) as client:
                resp = await client.get(
                    "https://store.steampowered.com/api/appdetails",
                    params={"appids": str(app_id)},
                )
                resp.raise_for_status()
                data = resp.json()
                entry = data.get(str(app_id), {})
                if entry.get("success"):
                    name = entry["data"].get("name", "")
                    return json.dumps(
                        {"valid": True, "app_id": app_id, "name": name}, ensure_ascii=False
                    )
                return json.dumps({"valid": False, "app_id": app_id}, ensure_ascii=False)
        except Exception as e:
            return json.dumps(
                {"valid": False, "app_id": app_id, "error": str(e)}, ensure_ascii=False
            )

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")

class SearchGameIdentifiersTool(BaseTool):
    name: str = "search_game_identifiers"
    description: str = (
        "给定游戏名称，自动搜索所有平台的标识符（Steam App ID, TapTap ID, "
        "Qimai App ID, Monitor siteurl, 官网 URL 等）。"
        "返回结构化结果，包含每个平台的置信度评分（high/medium/low）和可用候选项。"
        "创建采集任务前如果缺少平台标识符，应优先调用此工具。"
    )
    args_schema: Type[BaseModel] = SearchGameIdentifiersInput

    async def _arun(self, game_name: str, platforms: list[str] | None = None) -> str:
        from src.services.game_resolver import GameIdentifierResolver

        resolver = GameIdentifierResolver()
        try:
            result = await resolver.resolve_all(game_name, platforms)
            data = result.model_dump(mode="json", exclude_none=True)
            high = result.high_confidence()
            missing = [
                p
                for p in ("steam", "taptap", "qimai", "monitor", "official_site")
                if getattr(result, p, None) is None
            ]
            return _format_result(
                "ok",
                f"已搜索 '{game_name}' 的平台标识符: {len(high)} 个高置信度, {len(missing)} 个未找到",
                data,
                record_count=len(result.found_platforms()),
                suggestion=(
                    f"高置信度平台: {', '.join(high)}。可直接创建采集任务。"
                    if high
                    else "部分平台置信度较低，建议向用户确认后创建任务"
                ),
            )
        except Exception as e:
            return _format_result("error", f"搜索标识符失败: {e}")
        finally:
            await resolver.teardown()

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")

class VerifyGameIdentifierTool(BaseTool):
    name: str = "verify_game_identifier"
    description: str = (
        "验证单个平台的标识符是否有效并对应预期的游戏名称。"
        "用于确认 search_game_identifiers 返回的标识符是否正确。"
    )
    args_schema: Type[BaseModel] = VerifyGameIdentifierInput

    async def _arun(self, platform: str, identifier: str, game_name: str) -> str:
        from src.services.game_resolver import GameIdentifierResolver

        resolver = GameIdentifierResolver()
        try:
            await resolver.setup()
            result = await resolver.verify_identifier(platform, identifier, game_name)
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"valid": False, "error": str(e)}, ensure_ascii=False)
        finally:
            await resolver.teardown()

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")
