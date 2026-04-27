from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx
from difflib import SequenceMatcher
from loguru import logger

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.config import get as get_config
from src.core.registry import registry


STEAM_APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"
SULLY_SEARCH_URL = "https://sullygnome.com/api/standardsearch/{query}"
SULLY_GAME_URL = "https://sullygnome.com/game/{siteurl}"
SULLY_SUMMARY_URL = f"{SULLY_GAME_URL}/90/summary"
SULLY_CHART_URL = (
    "https://sullygnome.com/api/charts/linecharts/getconfig/"
    "GameViewers/90/0/{game_id}/{game_name}/%20/%20/0/0/%20/0/"
)
PAGEINFO_PATTERN = re.compile(r"var PageInfo = (\{.*?\});", re.DOTALL)
SUPPORTED_METRICS = {"twitch_viewer_trend"}
REMOVED_METRICS = {"player_count", "review_trend", "sales_rank"}
DEFAULT_SULLY_SITEURL_OVERRIDES = {
    "2507950": "delta_force_hawk_ops",
    "delta force": "delta_force_hawk_ops",
    "delta force hawk ops": "delta_force_hawk_ops",
    "delta force: hawk ops": "delta_force_hawk_ops",
    "三角洲行动": "delta_force_hawk_ops",
}


@registry.register("collector", "monitor")
class MonitorCollector(BaseCollector):
    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._client: httpx.AsyncClient | None = None
        self._metric_concurrency = 4

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        await super().setup(config)
        timeout = float(self.config.get("timeout", get_config("collector.request_timeout", 30)))
        user_agent = self.config.get("user_agent") or get_config(
            "collector.user_agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": user_agent,
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "close",
            },
        )
        self._metric_concurrency = int(get_config("monitor.metric_concurrency", 4))

    async def teardown(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        await super().teardown()

    async def collect(self, target: CollectTarget) -> CollectResult:
        if self._client is None:
            raise RuntimeError("Monitor collector client is not initialized")

        app_id = int(str(target.params.get("app_id") or "").strip() or 0)
        if app_id <= 0:
            raise ValueError("monitor target requires a valid app_id")

        metrics = _normalize_metrics(target.params.get("metrics"))
        days = int(target.params.get("days", get_config("monitor.default_days", 30)))
        days = max(7, min(days, 90))
        tz_name = str(target.params.get("timezone", get_config("monitor.timezone", "Asia/Shanghai")))
        twitch_name = _optional_str(target.params.get("twitch_name"))
        siteurl = _optional_str(target.params.get("siteurl"))

        warnings: list[str] = []

        logger.info(f"[Monitor] Start collect: {target.name} (app_id={app_id}) metrics={metrics}")
        metric_payloads = await self._collect_metrics_concurrently(
            metrics=metrics,
            app_id=app_id,
            target_name=target.name,
            days=days,
            tz_name=tz_name,
            twitch_name=twitch_name,
            siteurl=siteurl,
            warnings=warnings,
        )

        successful_metrics = {
            name: payload
            for name, payload in metric_payloads.items()
            if isinstance(payload, dict) and not payload.get("error")
        }
        if not successful_metrics:
            return CollectResult(
                target=target,
                success=False,
                error="monitor collector failed for all requested metrics",
                metadata={"collector": "monitor", "warnings": warnings},
                raw_data=metric_payloads,
            )

        data = {
            "collector": "monitor",
            "game_name": target.name,
            "app_id": app_id,
            "source_meta": {
                "collector": "monitor",
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "metrics": metrics,
                "days": days,
                "timezone": tz_name,
            },
            "monitor_metrics": metric_payloads,
            "snapshot": _build_monitor_snapshot(target.name, app_id, metric_payloads),
        }

        metadata: dict[str, Any] = {
            "collector": "monitor",
            "data_sources": [_metric_source(metric_name) for metric_name in successful_metrics],
        }
        if warnings:
            metadata["warnings"] = warnings

        return CollectResult(
            target=target,
            success=True,
            data=data,
            metadata=metadata,
        )

    async def _collect_metrics_concurrently(
        self,
        *,
        metrics: list[str],
        app_id: int,
        target_name: str,
        days: int,
        tz_name: str,
        twitch_name: str | None,
        siteurl: str | None,
        warnings: list[str],
    ) -> dict[str, Any]:
        semaphore = asyncio.Semaphore(max(1, self._metric_concurrency))

        async def runner(metric_name: str) -> tuple[str, dict[str, Any]]:
            async with semaphore:
                try:
                    if metric_name == "twitch_viewer_trend":
                        payload = await self._collect_twitch_metric(
                            app_id=app_id,
                            target_name=target_name,
                            twitch_name=twitch_name,
                            siteurl=siteurl,
                            days=days,
                        )
                    else:
                        raise ValueError(f"unknown monitor metric: {metric_name}")
                    return metric_name, payload
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"[Monitor] {metric_name} failed for {target_name}: {exc}")
                    warnings.append(f"{metric_name}: {exc}")
                    return metric_name, {
                        "source": _metric_source(metric_name),
                        "error": str(exc),
                        "days": days,
                    }

        results = await asyncio.gather(*(runner(metric_name) for metric_name in metrics))
        return dict(results)

    async def _collect_twitch_metric(
        self,
        *,
        app_id: int,
        target_name: str,
        twitch_name: str | None,
        siteurl: str | None,
        days: int,
    ) -> dict[str, Any]:
        resolved_siteurl = siteurl or _resolve_sully_siteurl_override(app_id, target_name, twitch_name)
        if not resolved_siteurl:
            steam_name = await self._fetch_steam_app_name(app_id)
            search_names = _generate_search_variants(twitch_name, target_name, steam_name)
            candidates = await self._collect_sully_candidates(search_names)
            if not candidates:
                raise ValueError(f'No SullyGnome result found for "{target_name}"')
            resolved_siteurl = _choose_best_sully_siteurl(candidates, search_names)

        summary_html = await self._fetch_text(SULLY_SUMMARY_URL.format(siteurl=resolved_siteurl))
        pageinfo = _parse_pageinfo(summary_html)
        game_id = int(pageinfo["id"])
        game_name = quote(str(pageinfo["name"]), safe="")
        chart_url = SULLY_CHART_URL.format(game_id=game_id, game_name=game_name)
        chart_payload = await self._fetch_json(
            chart_url,
            headers={
                "Timecode": str(pageinfo["timecode"]),
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        daily_rows = _build_twitch_daily_rows(chart_payload)
        if days < len(daily_rows):
            daily_rows = daily_rows[-days:]

        latest_avg = next(
            (row["average_viewers"] for row in reversed(daily_rows) if row["average_viewers"] is not None),
            None,
        )
        latest_peak = next(
            (row["peak_viewers"] for row in reversed(daily_rows) if row["peak_viewers"] is not None),
            None,
        )
        avg_values = [row["average_viewers"] for row in daily_rows if row["average_viewers"] is not None]
        return {
            "source": "sullygnome",
            "days": min(days, len(daily_rows)),
            "siteurl": resolved_siteurl,
            "data_source": SULLY_GAME_URL.format(siteurl=resolved_siteurl),
            "latest_average_viewers": latest_avg,
            "latest_peak_viewers": latest_peak,
            "max_average_viewers": max(avg_values) if avg_values else None,
            "min_average_viewers": min(avg_values) if avg_values else None,
            "daily_rows": daily_rows,
        }

    async def _collect_sully_candidates(self, search_names: list[str]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()
        for name in search_names:
            items = await self._fetch_json(SULLY_SEARCH_URL.format(query=quote(name)))
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict) or item.get("itemtype") != 2:
                    continue
                siteurl = str(item.get("siteurl", "")).strip()
                if not siteurl or siteurl in seen:
                    continue
                seen.add(siteurl)
                candidates.append(item)
        return candidates

    async def _fetch_steam_app_name(self, app_id: int) -> str | None:
        try:
            payload = await self._fetch_json(
                STEAM_APPDETAILS_URL,
                params={"appids": app_id, "l": "english"},
            )
        except Exception:  # noqa: BLE001
            return None
        data = (payload.get(str(app_id)) or {}).get("data") or {}
        name = data.get("name")
        return name.strip() if isinstance(name, str) and name.strip() else None

    async def _fetch_text(self, url: str, *, headers: dict[str, str] | None = None) -> str:
        response = await self._client.get(url, headers=headers)
        response.raise_for_status()
        return response.text

    async def _fetch_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        response = await self._client.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    def validate_config(self, config: dict[str, Any] | None = None) -> bool:
        return True


def _normalize_metrics(raw_metrics: Any) -> list[str]:
    if not isinstance(raw_metrics, list) or not raw_metrics:
        return ["twitch_viewer_trend"]
    requested = {str(metric).strip() for metric in raw_metrics if str(metric).strip()}
    removed = sorted(requested & REMOVED_METRICS)
    if removed:
        raise ValueError(f"removed monitor metrics are no longer supported: {', '.join(removed)}")
    unknown = sorted(requested - SUPPORTED_METRICS)
    if unknown:
        raise ValueError(f"unknown monitor metrics: {', '.join(unknown)}")
    return ["twitch_viewer_trend"]


def _resolve_sully_siteurl_override(app_id: int, target_name: str, twitch_name: str | None) -> str | None:
    configured = get_config("monitor.sully_siteurl_overrides", {})
    overrides = dict(DEFAULT_SULLY_SITEURL_OVERRIDES)
    if isinstance(configured, dict):
        overrides.update({str(key).strip().lower(): str(value).strip() for key, value in configured.items()})

    keys = [str(app_id), target_name, twitch_name or ""]
    for key in keys:
        normalized = str(key or "").strip().lower()
        if normalized and overrides.get(normalized):
            return overrides[normalized]
    return None


def _generate_search_variants(*names: str | None) -> list[str]:
    variants: list[str] = []
    seen: set[str] = set()
    for name in names:
        if not isinstance(name, str) or not name.strip():
            continue
        pending = [name.strip()]
        base = name.strip()
        stripped_number = re.sub(r"\s+\d+$", "", base).strip()
        stripped_dash = re.split(r"\s+-\s+", base, maxsplit=1)[0].strip()
        stripped_colon = re.split(r"\s*:\s*", base, maxsplit=1)[0].strip()
        for variant in (stripped_number, stripped_dash, stripped_colon):
            if variant and variant != base:
                pending.append(variant)
        for variant in pending:
            normalized = _normalize_name(variant)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            variants.append(variant)
    return variants


def _normalize_name(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def _choose_best_sully_siteurl(candidates: list[dict[str, Any]], names: list[str]) -> str:
    normalized_names = [_normalize_name(name) for name in names if _normalize_name(name)]
    if not normalized_names:
        return str(candidates[0]["siteurl"])

    best_item = None
    best_score = 0.0
    for item in candidates:
        display_name = _normalize_name(str(item.get("displaytext", "")))
        siteurl = _normalize_name(str(item.get("siteurl", "")))
        for name in normalized_names:
            score = max(
                SequenceMatcher(None, name, display_name).ratio(),
                SequenceMatcher(None, name, siteurl).ratio(),
            )
            if score > best_score:
                best_score = score
                best_item = item
    if best_item is None:
        raise ValueError("unable to match SullyGnome game")
    return str(best_item["siteurl"])


def _parse_pageinfo(html_text: str) -> dict[str, Any]:
    match = PAGEINFO_PATTERN.search(html_text)
    if match is None:
        raise ValueError("could not find PageInfo JSON in SullyGnome summary page")
    return json.loads(match.group(1))


def _build_twitch_daily_rows(chart_payload: dict[str, Any]) -> list[dict[str, Any]]:
    labels = chart_payload.get("data", {}).get("labels", [])
    avg_values = _find_dataset(chart_payload, "Average viewers")
    peak_values = _find_dataset(chart_payload, "Peak viewers")
    if not labels or len(labels) != len(avg_values) or len(labels) != len(peak_values):
        raise ValueError("invalid SullyGnome chart payload")

    rows: list[dict[str, Any]] = []
    for label, avg_value, peak_value in zip(labels, avg_values, peak_values, strict=True):
        sample_date = datetime.strptime(label, "%Y-%m-%d %H:%M").date()
        rows.append(
            {
                "date": sample_date.isoformat(),
                "average_viewers": None if avg_value is None else int(round(avg_value)),
                "peak_viewers": None if peak_value is None else int(round(peak_value)),
            }
        )
    return rows


def _find_dataset(chart_payload: dict[str, Any], label: str) -> list[float | None]:
    datasets = chart_payload.get("data", {}).get("datasets", [])
    for dataset in datasets:
        if str(dataset.get("label", "")).strip().lower() != label.lower():
            continue
        values = dataset.get("data", [])
        return [float(value) if value is not None else None for value in values]
    raise ValueError(f'missing dataset "{label}"')


def _metric_source(metric_name: str) -> str:
    if metric_name != "twitch_viewer_trend":
        raise ValueError(f"unsupported monitor metric: {metric_name}")
    return "sullygnome"


def _optional_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _build_monitor_snapshot(
    target_name: str,
    app_id: int,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    twitch = metrics.get("twitch_viewer_trend", {})
    return {
        "name": target_name,
        "app_id": app_id,
        "latest_twitch_average_viewers": twitch.get("latest_average_viewers"),
        "latest_twitch_peak_viewers": twitch.get("latest_peak_viewers"),
    }
