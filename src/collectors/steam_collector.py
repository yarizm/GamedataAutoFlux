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
        .add_storage("sqlalchemy")
    )
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.collectors.base import BaseCollector, CollectTarget, CollectResult
from src.collectors.steam.steam_api_client import SteamAPIClient
from src.collectors.steam.steamdb_scraper import SteamDBScraper, SteamDBScrapeFailed
from src.collectors.steam.firecrawl_fallback import FirecrawlFallback
from src.core.collector_resume import (
    build_collector_cursor,
    cap_partial_list,
    parse_recovery_cursor,
)
from src.core.errors import ErrorCode, classify_exception
from src.core.sensitive import redact_sensitive_text
from src.core.registry import registry

# Stage machine (S1 deep resume)
_STAGE_RESOLVE = "resolve_app_id"
_STAGE_API_LIGHT = "api_light"
_STAGE_API_REVIEWS = "api_reviews"
_STAGE_STEAMDB = "steamdb"
_STAGE_DONE = "done"


def _reviews_payload_succeeded(reviews_val: Any) -> bool:
    """True only when reviews is a successful payload dict (not None / pure error blob)."""
    if not isinstance(reviews_val, dict):
        return False
    # Pure error blob: has error and lacks normal review structure fields.
    if reviews_val.get("error") is not None and not any(
        k in reviews_val for k in ("reviews", "total_reviews", "review_count_fetched")
    ):
        return False
    return True


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

        S1 stage machine: resolve_app_id → api_light → api_reviews → steamdb → done.
        Deep resume via recovery_checkpoint cursor (review_cursor / completed_stages).
        """
        last_cursor: dict[str, Any] | None = None

        # ── Stage: resolve_app_id ──
        app_id = str(target.params.get("app_id", "") or "").strip()
        if not app_id:
            logger.info(f"[Steam] 按名称查找 app_id: {target.name}")
            try:
                resolved = await self._steam_api.resolve_app_id(target.name)
            except Exception as e:
                safe_error = _safe_log_text(e)
                logger.error(f"[Steam] resolve_app_id 失败: {safe_error}")
                return CollectResult(
                    target=target,
                    success=False,
                    error=f"解析 app_id 失败: {safe_error}",
                    error_code=classify_exception(e).value,
                )
            if resolved:
                app_id = str(resolved)
                logger.info(f"[Steam] 找到 app_id={app_id} for '{target.name}'")
            else:
                return CollectResult(
                    target=target,
                    success=False,
                    error=f"无法解析游戏名称 '{target.name}' 的 app_id",
                    error_code=ErrorCode.empty_data.value,
                )

        target_key = f"app:{app_id}"
        recovery_cursor = parse_recovery_cursor(
            self.config.get("recovery_checkpoint") if isinstance(self.config, dict) else None,
            collector_id="steam",
            target_key=target_key,
        )
        resume_payload: dict[str, Any] = {}
        if isinstance(recovery_cursor, dict):
            raw_payload = recovery_cursor.get("payload")
            if isinstance(raw_payload, dict):
                resume_payload = dict(raw_payload)

        completed_stages = [
            str(s).strip()
            for s in (resume_payload.get("completed_stages") or [])
            if str(s or "").strip()
        ]
        if _STAGE_RESOLVE not in completed_stages:
            completed_stages.append(_STAGE_RESOLVE)

        try:
            resume_collected = int(resume_payload.get("collected_count") or 0)
        except (TypeError, ValueError):
            resume_collected = 0
        resume_collected = max(0, resume_collected)
        resume_review_cursor = str(resume_payload.get("review_cursor") or "").strip()
        raw_partial = resume_payload.get("partial_reviews")
        partial_source = list(raw_partial) if isinstance(raw_partial, list) else []
        partial_reviews, partial_was_truncated = cap_partial_list(partial_source)
        steamdb_done = bool(resume_payload.get("steamdb_done")) or (
            _STAGE_STEAMDB in completed_stages
        )
        reviews_done = _STAGE_API_REVIEWS in completed_stages

        max_reviews = int(target.params.get("max_reviews", self.config.get("max_reviews", 200)))
        review_language = str(
            target.params.get("review_language", self.config.get("review_language", "all"))
        )
        review_trend_days = int(
            target.params.get("review_trend_days", self.config.get("review_trend_days", 90))
        )
        review_trend_mode = str(
            target.params.get("review_trend_mode", self.config.get("review_trend_mode", "summary"))
        )
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
        review_filter = str(resume_payload.get("review_filter") or "recent")

        def _base_payload(
            *,
            stage_list: list[str],
            review_cursor: str = "",
            collected_count: int = 0,
            partial: list[Any] | None = None,
            steamdb: bool = False,
            extra: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            capped, truncated = cap_partial_list(list(partial or []))
            body: dict[str, Any] = {
                "app_id": str(app_id),
                "completed_stages": list(stage_list),
                "review_cursor": review_cursor,
                "review_language": review_language,
                "review_filter": review_filter,
                "collected_count": int(collected_count),
                "max_reviews": int(max_reviews),
                "partial_reviews": capped,
                "steamdb_done": bool(steamdb),
            }
            if truncated:
                body["partial_reviews_truncated"] = True
            if extra:
                body.update(extra)
            return body

        async def _emit(
            stage: str,
            payload: dict[str, Any],
            *,
            state: dict[str, Any] | None = None,
            stats: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            nonlocal last_cursor
            cursor = build_collector_cursor(
                collector_id="steam",
                target_key=target_key,
                stage=stage,
                payload=payload,
            )
            last_cursor = cursor
            emit_fn = self.config.get("_emit_checkpoint") if isinstance(self.config, dict) else None
            if callable(emit_fn):
                try:
                    await emit_fn(cursor, state=state, stats=stats)
                except Exception as emit_err:
                    logger.warning(
                        "[Steam] checkpoint emit failed: {}",
                        _safe_log_text(emit_err),
                    )
            return cursor

        async def _emit_failure_cursor() -> None:
            if last_cursor is not None:
                await _emit(
                    str(last_cursor.get("stage") or _STAGE_API_REVIEWS),
                    dict(last_cursor.get("payload") or {}),
                )
                return
            # Best-effort mid-fail cursor so resume can at least see completed stages.
            await _emit(
                _STAGE_API_REVIEWS if not reviews_done else _STAGE_STEAMDB,
                _base_payload(
                    stage_list=completed_stages,
                    review_cursor=resume_review_cursor,
                    collected_count=resume_collected,
                    partial=partial_reviews,
                    steamdb=steamdb_done,
                ),
            )

        logger.info(f"[Steam] === 开始采集: {target.name} (app_id={app_id}) ===")
        if recovery_cursor:
            logger.info(
                "[Steam] resume cursor stage={} completed={} review_cursor={} collected={}",
                recovery_cursor.get("stage"),
                completed_stages,
                resume_review_cursor or "*",
                resume_collected,
            )

        # ── Stage: api_light + api_reviews (via collect_all; light re-fetch OK) ──
        # Resume kwargs: never pass seed_reviews=[] — only non-empty seed or already_collected.
        # Seed only when partial is complete vs collected_count; else count-only (cursor+count).
        start_cursor = "*"
        already_collected = 0
        seed_reviews: list[dict[str, Any]] | None = None
        if reviews_done:
            # Reviews already finished in a prior run — skip expensive re-pull.
            already_collected = max_reviews
            seed_reviews = None
            start_cursor = "*"
        else:
            if resume_review_cursor and resume_review_cursor != "*":
                start_cursor = resume_review_cursor
            seed_candidate = [r for r in partial_reviews if isinstance(r, dict)]
            partial_complete = (
                bool(seed_candidate)
                and resume_collected <= len(seed_candidate)
                and not (partial_was_truncated and resume_collected > len(seed_candidate))
            )
            if partial_complete:
                # Full partial list relative to collected_count — seed it.
                seed_reviews = seed_candidate
                already_collected = 0
            else:
                # Count-only / incomplete / truncated partial: do not seed.
                seed_reviews = None
                already_collected = resume_collected

        # Track whether on_page saw a seed-based list (for collected_count accounting).
        using_seed = seed_reviews is not None
        base_already = already_collected

        async def on_page(
            *,
            cursor: str,
            reviews: list[Any],
            query_summary: dict[str, Any] | None = None,
        ) -> None:
            if using_seed:
                count = len(reviews)
                # Seed path: partial list is complete so far (all_reviews including seed).
                partial_for_emit: list[Any] = [r for r in reviews if isinstance(r, dict)]
            else:
                count = base_already + len(reviews)
                # Count-only path: emit empty partial so resume never mis-seeds on a
                # short newly-fetched list (resume_collected > len(partial)).
                partial_for_emit = []
            stages = list(completed_stages)
            if _STAGE_API_LIGHT not in stages:
                stages.append(_STAGE_API_LIGHT)
            await _emit(
                _STAGE_API_REVIEWS,
                _base_payload(
                    stage_list=stages,
                    review_cursor=str(cursor or ""),
                    collected_count=count,
                    partial=partial_for_emit,
                    steamdb=False,
                ),
                stats={
                    "collected_count": count,
                    "query_summary": query_summary or {},
                },
            )

        logger.info("[Steam] 阶段: 官方 API 采集 (api_light + api_reviews)")
        try:
            steam_data = await self._steam_api.collect_all(
                app_id,
                max_reviews=max_reviews,
                review_language=review_language,
                review_trend_days=review_trend_days,
                max_review_trend_reviews=max_review_trend_reviews,
                review_trend_mode=review_trend_mode,
                review_summary_concurrency=review_summary_concurrency,
                start_cursor=start_cursor,
                already_collected=already_collected,
                seed_reviews=seed_reviews,
                on_page=None if reviews_done else on_page,
            )
        except Exception as e:
            safe_error = _safe_log_text(e)
            logger.error(f"[Steam] 官方 API 采集失败: {safe_error}")
            await _emit_failure_cursor()
            steam_data = {
                "source": "steam_api",
                "error": safe_error,
                "_error_code": classify_exception(e).value,
            }

        steam_api_ok = not steam_data.get("error") and any(
            steam_data.get(key) is not None
            for key in ("details", "current_players", "reviews", "achievements", "news")
        )
        if not steam_api_ok:
            await _emit_failure_cursor()
            return CollectResult(
                target=target,
                success=False,
                error=f"Steam 官方 API 采集失败: {steam_data.get('error', '未返回有效数据')}",
                error_code=steam_data.get("_error_code", ErrorCode.unknown.value),
                metadata={
                    "collector": "steam",
                    "data_sources": _list_sources(steam_data, None),
                    "target_key": target_key,
                },
                raw_data=steam_data,
            )

        if _STAGE_API_LIGHT not in completed_stages:
            completed_stages.append(_STAGE_API_LIGHT)
        # Only mark api_reviews complete when reviews payload actually succeeded.
        # Light-only success (reviews=None / pure error) leaves stage incomplete so resume re-tries.
        reviews_ok = _reviews_payload_succeeded(steam_data.get("reviews"))
        if reviews_ok and not reviews_done and _STAGE_API_REVIEWS not in completed_stages:
            completed_stages.append(_STAGE_API_REVIEWS)
            reviews_done = True

        reviews_blob = (
            steam_data.get("reviews") if isinstance(steam_data.get("reviews"), dict) else {}
        )
        review_items = reviews_blob.get("reviews") if isinstance(reviews_blob, dict) else None
        if not isinstance(review_items, list):
            review_items = []
        try:
            fetched_n = int(reviews_blob.get("review_count_fetched") or 0)
        except (TypeError, ValueError):
            fetched_n = 0
        if using_seed:
            final_review_count = fetched_n or len(review_items)
        elif base_already:
            final_review_count = base_already + (fetched_n or len(review_items))
        else:
            final_review_count = fetched_n or len(review_items) or resume_collected

        # Count-only path: keep partial empty (cursor+count only). Seed path: full list.
        if using_seed and reviews_ok:
            partial_after_api: list[Any] = (
                [r for r in review_items if isinstance(r, dict)]
                if review_items
                else list(partial_reviews)
            )
        elif reviews_ok and not base_already:
            # Fresh full pull (no already_collected) — list is complete for max_reviews.
            partial_after_api = [r for r in review_items if isinstance(r, dict)]
        else:
            # Count-only mid-run / reviews failed: do not store incomplete partials.
            partial_after_api = []

        # Prefer on_page progress when reviews not completed (cursor advanced mid-run).
        emit_review_cursor = ""
        emit_collected = final_review_count or resume_collected
        if not reviews_done:
            last_payload = last_cursor.get("payload") if isinstance(last_cursor, dict) else None
            if isinstance(last_payload, dict):
                emit_review_cursor = str(
                    last_payload.get("review_cursor") or resume_review_cursor or ""
                )
                try:
                    emit_collected = int(last_payload.get("collected_count") or emit_collected)
                except (TypeError, ValueError):
                    pass
                if using_seed and isinstance(last_payload.get("partial_reviews"), list):
                    partial_after_api = list(last_payload.get("partial_reviews") or [])
            else:
                emit_review_cursor = resume_review_cursor

        await _emit(
            _STAGE_STEAMDB if not steamdb_done else _STAGE_DONE,
            _base_payload(
                stage_list=completed_stages,
                review_cursor=emit_review_cursor,
                collected_count=emit_collected,
                partial=partial_after_api,
                steamdb=steamdb_done,
            ),
            stats={"collected_count": emit_collected},
        )

        # ── Stage: steamdb (optional; skip when already done or skip_steamdb) ──
        skip_steamdb_param = bool(target.params.get("skip_steamdb", False))
        skip_steamdb = skip_steamdb_param or steamdb_done
        steamdb_data: dict[str, Any] | None = None
        steamdb_warning: str | None = None

        if skip_steamdb:
            if steamdb_done:
                logger.info("[Steam] 跳过 SteamDB（checkpoint steamdb_done）")
            elif skip_steamdb_param:
                logger.info("[Steam] 跳过 SteamDB（skip_steamdb=true）")
        else:
            requested_time_slice = target.params.get("steamdb_time_slice", "monthly_peak_1y")
            steamdb_cookie = str(target.params.get("steamdb_cookie", "") or "")
            steamdb_headers = _clean_headers(target.params.get("steamdb_headers", {}))
            firecrawl_cookie = str(target.params.get("firecrawl_cookie", "") or "")
            firecrawl_headers = _clean_headers(target.params.get("firecrawl_headers", {}))
            if self._steamdb:
                logger.info("[Steam] 阶段: SteamDB Playwright 采集")
                try:
                    steamdb_data = await self._steamdb.scrape(
                        app_id,
                        time_slice=requested_time_slice,
                        cookie=steamdb_cookie,
                        extra_headers=steamdb_headers,
                    )
                    logger.info("[Steam] SteamDB Playwright ✓")
                except SteamDBScrapeFailed as e:
                    safe_error = _safe_log_text(e)
                    steamdb_warning = f"SteamDB Playwright 失败: {safe_error}"
                    logger.warning(f"[Steam] Playwright 失败: {safe_error}")
                    steamdb_data = await self._run_firecrawl_fallback(
                        app_id,
                        steamdb_warning,
                        requested_time_slice,
                        cookie=firecrawl_cookie,
                        headers=firecrawl_headers,
                    )
                except Exception as e:
                    safe_error = _safe_log_text(e)
                    steamdb_warning = f"SteamDB 可选采集异常: {safe_error}"
                    logger.warning(f"[Steam] SteamDB 可选采集异常，保留官方 API 结果: {safe_error}")
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

            steamdb_done = True
            if _STAGE_STEAMDB not in completed_stages:
                completed_stages.append(_STAGE_STEAMDB)

        # done
        if _STAGE_DONE not in completed_stages:
            completed_stages.append(_STAGE_DONE)
        await _emit(
            _STAGE_DONE,
            _base_payload(
                stage_list=completed_stages,
                review_cursor="",
                collected_count=final_review_count or resume_collected,
                partial=[],
                steamdb=True if skip_steamdb_param or steamdb_done else steamdb_done,
            ),
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

        details = steam_data.get("details") or {}
        reviews = steam_data.get("reviews") or {}
        review_score_percent = reviews.get("review_score_percent", 0)
        review_score_desc = reviews.get("review_score_desc", "")
        review_score_formatted = (
            f"{review_score_percent}% ({review_score_desc})"
            if review_score_percent
            else review_score_desc
        )
        steamdb_review_summary = _extract_steamdb_review_summary(steamdb_data)
        if steamdb_review_summary.get("score_text"):
            review_score_formatted = steamdb_review_summary["score_text"]

        merged_data["snapshot"] = {
            "name": details.get("name", target.name),
            "current_players": steam_data.get("current_players", 0),
            "total_reviews": steamdb_review_summary.get("total_reviews")
            or reviews.get("total_reviews", 0),
            "review_score": review_score_formatted,
            "price": details.get("price"),
        }

        logger.info(f"[Steam] === 采集完成: {target.name} ===")

        resume_meta: dict[str, Any] = {}
        if recovery_cursor:
            resume_meta["resume"] = {
                "resumed": True,
                "target_key": target_key,
                "stage": recovery_cursor.get("stage"),
            }

        return CollectResult(
            target=target,
            data=merged_data,
            metadata={
                "collector": "steam",
                "data_sources": _list_sources(steam_data, steamdb_data),
                "target_key": target_key,
                **resume_meta,
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
                safe_error = _safe_log_text(fc_err)
                logger.error(f"[Steam] Firecrawl 也失败: {safe_error}")
                return {
                    "source": "firecrawl",
                    "error": safe_error,
                }

        return {
            "source": "steamdb",
            "error": warning_message,
        }

    async def teardown(self) -> None:
        """清理所有子组件"""
        if self._steam_api:
            try:
                await self._steam_api.teardown()
            except Exception as e:
                logger.error(f"[SteamCollector] steam_api teardown failed: {_safe_log_text(e)}")
        if self._steamdb:
            try:
                await self._steamdb.teardown()
            except Exception as e:
                logger.error(f"[SteamCollector] steamdb teardown failed: {_safe_log_text(e)}")
        if self._firecrawl:
            try:
                await self._firecrawl.teardown()
            except Exception as e:
                logger.error(f"[SteamCollector] firecrawl teardown failed: {_safe_log_text(e)}")
        await super().teardown()

    def validate_config(self, config: dict[str, Any] | None = None) -> bool:
        return True


def _list_sources(steam_data: dict | None, steamdb_data: dict | None) -> list[str]:
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


def _safe_log_text(value: Any) -> str:
    return redact_sensitive_text(str(value or ""))


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
