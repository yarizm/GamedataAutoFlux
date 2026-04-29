"""
Steam 数据采集器 — 主入口

编排三层数据采集:
  1. Steam 官方 API (httpx)    → 始终执行
  2. SteamDB Playwright (主)   → 可选, 尝试执行
  3. Firecrawl 兜底 (副)       → Playwright 失败时自动切换

通过 @registry.register("collector", "steam") 注册到系统。

使用方式:
    pipeline = (
        Pipeline("steam_monitor")
        .add_collector("steam", {"request_delay": 1.5})
        .add_processor("cleaner")
        .add_storage("local")
    )
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.collectors.base import BaseCollector, CollectTarget, CollectResult
from src.collectors.steam.steam_api_client import SteamAPIClient
from src.collectors.steam.steamdb_scraper import SteamDBScraper, SteamDBScrapeFailed
from src.collectors.steam.firecrawl_fallback import FirecrawlFallback
from src.core.registry import registry


@registry.register("collector", "steam")
class SteamCollector(BaseCollector):
    """
    Steam 数据采集器。

    采集维度:
      ┌─ 官方 API ─────────────────────────────────────────┐
      │  • 游戏详情/价格/标签 (appdetails)                   │
      │  • 当前在线人数 (GetNumberOfCurrentPlayers)          │
      │  • 评论数据/好评率 (appreviews, 分页)                │
      │  • 成就完成率 (GetGlobalAchievementPercentages)      │
      │  • 游戏新闻/更新 (GetNewsForApp)                    │
      └────────────────────────────────────────────────────┘
      ┌─ SteamDB (Playwright 主 → Firecrawl 兜底) ─────────┐
      │  • 历史在线趋势/峰值 (charts 页面)                    │
      │  • 版本更新/发布商信息 (info 页面)                    │
      └────────────────────────────────────────────────────┘

    配置 (target.params):
      - app_id: Steam App ID (如 "730"), 若不提供则按 name 模糊匹配
      - skip_steamdb: 是否跳过 SteamDB 采集 (默认 False)
      - max_reviews: 最大评论采集数 (默认取 settings)
    """

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._steam_api: SteamAPIClient | None = None
        self._steamdb: SteamDBScraper | None = None
        self._firecrawl: FirecrawlFallback | None = None

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        await super().setup(config)

        # 读取全局配置
        try:
            from src.core.config import get_settings
            settings = get_settings()
            steam_cfg = settings.get("steam", {})
            firecrawl_cfg = settings.get("firecrawl", {})
            collector_cfg = settings.get("collector", {})
        except Exception:
            steam_cfg = {}
            firecrawl_cfg = {}
            collector_cfg = {}

        # ── 1. Steam API Client ──
        api_key = self.config.get("api_key", "") or steam_cfg.get("api_key", "")
        # 清除未替换的环境变量占位符
        if api_key.startswith("${"):
            api_key = ""

        self.config.setdefault("max_reviews", int(steam_cfg.get("max_reviews", 200)))
        self.config.setdefault("review_language", steam_cfg.get("review_language", "all"))
        self.config.setdefault("review_trend_days", int(steam_cfg.get("review_trend_days", 90)))
        self.config.setdefault("review_trend_mode", steam_cfg.get("review_trend_mode", "summary"))
        self.config.setdefault(
            "review_summary_concurrency",
            int(steam_cfg.get("review_summary_concurrency", 4)),
        )
        self.config.setdefault(
            "max_review_trend_reviews",
            int(steam_cfg.get("max_review_trend_reviews", 10000)),
        )

        self._steam_api = SteamAPIClient(
            api_key=api_key,
            request_delay=float(
                self.config.get("request_delay", steam_cfg.get("request_delay", 1.5))
            ),
            timeout=float(collector_cfg.get("request_timeout", 30)),
            proxy=collector_cfg.get("proxy"),
        )
        await self._steam_api.setup()

        # ── 2. SteamDB Playwright ──
        steamdb_cfg = steam_cfg.get("steamdb", {})
        if steamdb_cfg.get("enabled", True):
            self._steamdb = SteamDBScraper(
                headless=steamdb_cfg.get("headless", True),
                timeout=steamdb_cfg.get("timeout", 30000),
                request_delay=float(steamdb_cfg.get("request_delay", 3.0)),
                cookie=str(steamdb_cfg.get("cookie", "") or ""),
                extra_headers=_clean_headers(steamdb_cfg.get("headers", {})),
                cdp_enabled=bool(steamdb_cfg.get("cdp_enabled", True)),
                cdp_port=int(steamdb_cfg.get("cdp_port", 9222)),
                request_jitter=float(steamdb_cfg.get("request_jitter", 4.0)),
                page_delay=float(steamdb_cfg.get("page_delay", 5.0)),
                max_games_per_session=int(steamdb_cfg.get("max_games_per_session", 10)),
            )
            # 延迟初始化: 不在 setup 时启动浏览器，在首次使用时启动

        # ── 3. Firecrawl Fallback ──
        fc_key = firecrawl_cfg.get("api_key", "")
        if fc_key and not fc_key.startswith("${"):
            self._firecrawl = FirecrawlFallback(
                api_key=fc_key,
                timeout=int(firecrawl_cfg.get("timeout", 30)),
                headers=_clean_headers(firecrawl_cfg.get("headers", {})),
                cookie=str(firecrawl_cfg.get("cookie", "") or steamdb_cfg.get("cookie", "") or ""),
            )

        logger.info(
            f"[SteamCollector] 初始化完成 — "
            f"API: ✓, SteamDB: {'✓' if self._steamdb else '✗'}, "
            f"Firecrawl: {'✓' if self._firecrawl else '✗'}"
        )

    async def collect(self, target: CollectTarget) -> CollectResult:
        """
        执行采集: 官方 API + SteamDB (Playwright→Firecrawl)
        """
        app_id = target.params.get("app_id", "")

        # 如果没有 app_id，尝试按名称解析
        if not app_id:
            logger.info(f"[Steam] 按名称查找 app_id: {target.name}")
            resolved = await self._steam_api.resolve_app_id(target.name)
            if resolved:
                app_id = resolved
                logger.info(f"[Steam] 找到 app_id={app_id} for '{target.name}'")
            else:
                return CollectResult(
                    target=target,
                    success=False,
                    error=f"无法解析游戏名称 '{target.name}' 的 app_id",
                )

        logger.info(f"[Steam] === 开始采集: {target.name} (app_id={app_id}) ===")

        # ── 阶段1: 官方 API（必执行）──
        logger.info("[Steam] 阶段1: 官方 API 采集")
        try:
            max_reviews = int(target.params.get("max_reviews", self.config.get("max_reviews", 200)))
            review_language = str(target.params.get("review_language", self.config.get("review_language", "all")))
            review_trend_days = int(target.params.get("review_trend_days", self.config.get("review_trend_days", 90)))
            review_trend_mode = str(target.params.get("review_trend_mode", self.config.get("review_trend_mode", "summary")))
            review_summary_concurrency = int(
                target.params.get(
                    "review_summary_concurrency",
                    self.config.get("review_summary_concurrency", 4),
                )
            )
            max_review_trend_reviews = int(
                target.params.get(
                    "max_review_trend_reviews",
                    self.config.get("max_review_trend_reviews", 10000),
                )
            )
            steam_data = await self._steam_api.collect_all(
                app_id,
                max_reviews=max_reviews,
                review_language=review_language,
                review_trend_days=review_trend_days,
                max_review_trend_reviews=max_review_trend_reviews,
                review_trend_mode=review_trend_mode,
                review_summary_concurrency=review_summary_concurrency,
            )
        except Exception as e:
            logger.error(f"[Steam] 官方 API 采集失败: {e}")
            steam_data = {"source": "steam_api", "error": str(e)}

        steam_api_ok = (
            not steam_data.get("error")
            and any(
                steam_data.get(key) is not None
                for key in ("details", "current_players", "reviews", "achievements", "news")
            )
        )
        if not steam_api_ok:
            return CollectResult(
                target=target,
                success=False,
                error=f"Steam 官方 API 采集失败: {steam_data.get('error', '未返回有效数据')}",
                metadata={
                    "collector": "steam",
                    "data_sources": _list_sources(steam_data, None),
                },
                raw_data=steam_data,
            )

        # ── 阶段2: SteamDB（可选）──
        skip_steamdb = target.params.get("skip_steamdb", False)
        steamdb_data: dict[str, Any] | None = None
        steamdb_warning: str | None = None

        if not skip_steamdb:
            requested_time_slice = target.params.get("steamdb_time_slice", "monthly_peak_1y")
            steamdb_cookie = str(target.params.get("steamdb_cookie", "") or "")
            steamdb_headers = _clean_headers(target.params.get("steamdb_headers", {}))
            firecrawl_cookie = str(target.params.get("firecrawl_cookie", "") or "")
            firecrawl_headers = _clean_headers(target.params.get("firecrawl_headers", {}))
            if self._steamdb:
                logger.info("[Steam] 阶段2: SteamDB Playwright 采集")
                try:
                    steamdb_data = await self._steamdb.scrape(
                        app_id,
                        time_slice=requested_time_slice,
                        cookie=steamdb_cookie,
                        extra_headers=steamdb_headers,
                    )
                    logger.info("[Steam] SteamDB Playwright ✓")
                except SteamDBScrapeFailed as e:
                    steamdb_warning = f"SteamDB Playwright 失败: {e}"
                    logger.warning(f"[Steam] Playwright 失败: {e}")
                    steamdb_data = await self._run_firecrawl_fallback(
                        app_id,
                        steamdb_warning,
                        requested_time_slice,
                        cookie=firecrawl_cookie,
                        headers=firecrawl_headers,
                    )
                except Exception as e:
                    steamdb_warning = f"SteamDB 可选采集异常: {e}"
                    logger.warning(f"[Steam] SteamDB 可选采集异常，保留官方 API 结果: {e}")
                    steamdb_data = await self._run_firecrawl_fallback(
                        app_id,
                        steamdb_warning,
                        requested_time_slice,
                        cookie=firecrawl_cookie,
                        headers=firecrawl_headers,
                    )
            elif self._firecrawl:
                steamdb_warning = "SteamDB Playwright 未启用，直接使用 Firecrawl 兜底"
                steamdb_data = await self._run_firecrawl_fallback(
                    app_id,
                    steamdb_warning,
                    requested_time_slice,
                    cookie=firecrawl_cookie,
                    headers=firecrawl_headers,
                )

        # ── 合并结果 ──
        merged_data = {
            "game_name": target.name,
            "app_id": int(app_id),
            "steam_api": steam_data,
        }
        if steamdb_data:
            requested_time_slice = target.params.get("steamdb_time_slice", "monthly_peak_1y")
            _apply_steamdb_time_slice(steamdb_data, requested_time_slice)
            merged_data["steamdb"] = steamdb_data

        # 提取关键快照指标
        details = steam_data.get("details") or {}
        reviews = steam_data.get("reviews") or {}
        review_score_percent = reviews.get("review_score_percent", 0)
        review_score_desc = reviews.get("review_score_desc", "")
        review_score_formatted = f"{review_score_percent}% ({review_score_desc})" if review_score_percent else review_score_desc
        steamdb_review_summary = _extract_steamdb_review_summary(steamdb_data)
        if steamdb_review_summary.get("score_text"):
            review_score_formatted = steamdb_review_summary["score_text"]

        merged_data["snapshot"] = {
            "name": details.get("name", target.name),
            "current_players": steam_data.get("current_players", 0),
            "total_reviews": steamdb_review_summary.get("total_reviews") or reviews.get("total_reviews", 0),
            "review_score": review_score_formatted,
            "price": details.get("price"),
        }

        logger.info(f"[Steam] === 采集完成: {target.name} ===")

        return CollectResult(
            target=target,
            data=merged_data,
            metadata={
                "collector": "steam",
                "data_sources": _list_sources(steam_data, steamdb_data),
                **({"warnings": [steamdb_warning]} if steamdb_warning else {}),
            },
            success=True,
        )

    async def _run_firecrawl_fallback(
        self,
        app_id: str | int,
        warning_message: str,
        requested_time_slice: str = "monthly_peak_1y",
        *,
        cookie: str = "",
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """SteamDB 失败时尝试 Firecrawl，失败则保留错误信息。"""
        if self._firecrawl:
            logger.info("[Steam] 切换到 Firecrawl 兜底采集")
            try:
                result = await self._firecrawl.scrape(
                    app_id,
                    time_slice=requested_time_slice,
                    cookie=cookie,
                    headers=headers,
                )
                logger.info("[Steam] Firecrawl 兜底 ✓")
                return result
            except Exception as fc_err:
                logger.error(f"[Steam] Firecrawl 也失败: {fc_err}")
                return {
                    "source": "firecrawl",
                    "error": str(fc_err),
                }

        return {
            "source": "steamdb",
            "error": warning_message,
        }

    async def teardown(self) -> None:
        """清理所有子组件"""
        if self._steam_api:
            await self._steam_api.teardown()
        if self._steamdb:
            await self._steamdb.teardown()
        if self._firecrawl:
            await self._firecrawl.teardown()
        await super().teardown()

    def validate_config(self, config: dict[str, Any] | None = None) -> bool:
        return True


def _list_sources(
    steam_data: dict | None, steamdb_data: dict | None
) -> list[str]:
    """列出实际使用的数据源"""
    sources = []
    if steam_data and not steam_data.get("error"):
        sources.append("steam_api")
    if steamdb_data:
        src = steamdb_data.get("source", "unknown")
        if not steamdb_data.get("error"):
            sources.append(src)
        else:
            sources.append(f"{src}(failed)")
    return sources


def _apply_steamdb_time_slice(steamdb_data: dict[str, Any], requested_slice: str) -> None:
    charts = steamdb_data.get("charts")
    if not isinstance(charts, dict):
        return

    monthly = charts.get("online_history_monthly_peak_1y") or charts.get("online_history_1y") or []
    daily90 = charts.get("online_history_daily_precise_90d") or []
    daily = daily90 or charts.get("online_history_daily_precise_30d") or []
    availability = charts.get("online_history_availability") or {
        "monthly_peak_1y": bool(monthly),
        "daily_precise_90d": bool(daily90),
        "daily_precise_30d": bool(daily),
    }
    unavailable_reasons = charts.get("online_history_unavailable_reasons") or {}

    slices = {
        "monthly_peak_1y": monthly,
        "daily_precise_90d": daily90 or daily,
        "daily_precise_30d": daily,
    }
    selected_slice = requested_slice if requested_slice in slices else "daily_precise_90d"
    if selected_slice == "daily_precise_90d" and not slices[selected_slice]:
        selected_slice = "monthly_peak_1y"
    selected_records = slices[selected_slice]

    charts["requested_time_slice"] = selected_slice
    charts["online_history"] = {
        "requested_slice": selected_slice,
        "available_slices": [name for name, available in availability.items() if available],
        "records": selected_records,
        "record_count": len(selected_records),
        "is_available": bool(availability.get(selected_slice)),
        "unavailable_reason": unavailable_reasons.get(selected_slice),
    }


def _extract_steamdb_review_summary(steamdb_data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(steamdb_data, dict) or steamdb_data.get("error"):
        return {}
    charts = steamdb_data.get("charts") if isinstance(steamdb_data.get("charts"), dict) else {}
    info = steamdb_data.get("info") if isinstance(steamdb_data.get("info"), dict) else {}
    rows = []
    if isinstance(charts, dict):
        rows = charts.get("user_reviews_history_90d") or charts.get("user_reviews_history") or []
    if isinstance(rows, list) and rows:
        latest = rows[-1]
        if isinstance(latest, dict):
            rate = _safe_float(latest.get("positive_rate"))
            total = _safe_int(latest.get("total"))
            return {
                "score_text": f"{rate:.2f}% (SteamDB)" if rate is not None else "",
                "total_reviews": total,
            }

    for container in (charts, info, steamdb_data):
        if not isinstance(container, dict):
            continue
        rate = _safe_float(
            container.get("steamdb_rating_percent")
            or container.get("review_score_percent")
            or container.get("positive_reviews_percent")
        )
        total = _safe_int(container.get("total_reviews"))
        if rate is not None or total is not None:
            return {
                "score_text": f"{rate:.2f}% (SteamDB)" if rate is not None else "",
                "total_reviews": total,
            }
    return {}


def _clean_headers(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): str(header_value)
        for key, header_value in value.items()
        if key not in (None, "") and header_value not in (None, "")
    }


def _safe_int(value: Any) -> int | None:
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
