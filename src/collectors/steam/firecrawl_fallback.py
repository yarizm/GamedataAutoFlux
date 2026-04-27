"""
Firecrawl fallback collector for SteamDB pages.

When Playwright is blocked by Cloudflare, Firecrawl returns page content as
Markdown. This module extracts the most useful structured fields from that
Markdown, including:

- high level stats
- top-level SteamDB key/value metadata
- monthly online history table
- patch/update history with timestamps
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from loguru import logger


class FirecrawlFallback:
    """Fallback SteamDB scraper backed by Firecrawl."""

    STEAMDB_BASE = "https://steamdb.info"
    HIGHCHARTS_EXTRACT_SCRIPT = """
(() => {
  const charts = (window.Highcharts && window.Highcharts.charts || []).filter(Boolean);
  return JSON.stringify(charts.map(chart => ({
    title: chart.title && chart.title.textStr || "",
    series: (chart.series || []).map(series => ({
      name: series.name || "",
      points: (series.points || []).map(point => ({
        x: point.x,
        y: point.y,
        name: point.name || ""
      })),
      data: (series.options && series.options.data || []).slice(0, 5000)
    }))
  })));
})()
"""

    def __init__(
        self,
        api_key: str = "",
        timeout: int = 30,
    ):
        self._api_key = api_key
        self._timeout = timeout
        self._app = None

    async def setup(self) -> None:
        """Initialize the Firecrawl client."""
        if not self._api_key:
            logger.warning("[Firecrawl] api_key is not configured; fallback is unavailable")
            return

        try:
            from firecrawl import FirecrawlApp

            self._app = FirecrawlApp(api_key=self._api_key)
            logger.info("[Firecrawl] Client initialized")
        except ImportError:
            logger.error("[Firecrawl] firecrawl-py is not installed")
            self._app = None

    async def teardown(self) -> None:
        self._app = None

    async def scrape(self, app_id: str | int, time_slice: str = "monthly_peak_1y") -> dict[str, Any]:
        """Scrape SteamDB charts/info pages and parse Firecrawl Markdown."""
        if not self._app:
            await self.setup()
            if not self._app:
                return {
                    "source": "firecrawl",
                    "error": "Firecrawl is unavailable",
                    "app_id": int(app_id),
                }

        result: dict[str, Any] = {
            "source": "firecrawl",
            "app_id": int(app_id),
            "requested_time_slice": time_slice,
        }

        charts_url = _build_charts_url(self.STEAMDB_BASE, app_id, time_slice)
        charts_md, highcharts_payload = await self._scrape_charts_url(charts_url)
        if charts_md:
            result["charts"] = _parse_steamdb_markdown(
                charts_md,
                chart_url=charts_url,
                requested_time_slice=time_slice,
            )
            _merge_highcharts_payload(result["charts"], highcharts_payload)
            result["charts_raw_preview"] = charts_md[:3000]
        else:
            result["charts"] = None

        info_url = f"{self.STEAMDB_BASE}/app/{app_id}/info/"
        info_md = await self._scrape_url(info_url)
        if info_md:
            result["info"] = _parse_steamdb_markdown(info_md)
            result["info_raw_preview"] = info_md[:3000]
        else:
            result["info"] = None

        return result

    async def _scrape_charts_url(self, url: str) -> tuple[str | None, list[dict[str, Any]]]:
        """Scrape charts Markdown and try to extract rendered Highcharts series."""
        actions = [
            {"type": "wait", "milliseconds": 5000},
            {"type": "executeJavascript", "script": self.HIGHCHARTS_EXTRACT_SCRIPT},
        ]
        markdown, action_payload = await self._scrape_url_with_actions(url, actions=actions)
        return markdown, _extract_highcharts_action_payload(action_payload)

    async def _scrape_url(self, url: str) -> str | None:
        """Call Firecrawl and return Markdown content for a single URL."""
        markdown, _ = await self._scrape_url_with_actions(url, actions=None)
        return markdown

    async def _scrape_url_with_actions(
        self,
        url: str,
        *,
        actions: list[dict[str, Any]] | None,
    ) -> tuple[str | None, Any]:
        """Call Firecrawl and return Markdown plus optional action output."""
        logger.info(f"[Firecrawl] Scrape: {url}")
        try:
            import asyncio

            scrape_fn = getattr(self._app, "scrape", None)
            if callable(scrape_fn):
                kwargs: dict[str, Any] = {"formats": ["markdown"]}
                if actions:
                    kwargs["actions"] = actions
                result = await asyncio.to_thread(
                    scrape_fn,
                    url,
                    **kwargs,
                )
            else:
                legacy_scrape_fn = getattr(self._app, "scrape_url", None)
                if not callable(legacy_scrape_fn):
                    raise AttributeError("Firecrawl client does not expose scrape() or scrape_url()")
                params: dict[str, Any] = {"formats": ["markdown"]}
                if actions:
                    params["actions"] = actions
                result = await asyncio.to_thread(
                    legacy_scrape_fn,
                    url,
                    params=params,
                )

            markdown = _extract_markdown(result)
            action_payload = _extract_actions(result)
            if markdown:
                logger.debug(f"[Firecrawl] Retrieved {len(markdown)} chars")
                return markdown, action_payload

            logger.warning(f"[Firecrawl] Empty response: {url}")
            return None, action_payload
        except Exception as exc:
            logger.error(f"[Firecrawl] Scrape failed: {exc}")
            return None, None


def _parse_steamdb_markdown(
    markdown: str,
    *,
    chart_url: str | None = None,
    requested_time_slice: str | None = None,
) -> dict[str, Any]:
    """Extract structured SteamDB data from Firecrawl Markdown."""
    data: dict[str, Any] = {}
    if chart_url:
        data["chart_url"] = chart_url
    if requested_time_slice:
        data["requested_time_slice"] = requested_time_slice

    normalized = _normalize_markdown(markdown)
    tables = _extract_markdown_tables(markdown)
    table_data = _extract_table_data(tables)

    stats = _extract_stats(normalized)
    if stats:
        data.update(stats)

    if table_data:
        data["table_data"] = table_data

    sections = re.findall(r"^#+\s+(.+)$", markdown, re.MULTILINE)
    if sections:
        data["sections"] = sections[:20]

    online_history = _extract_online_history(tables)
    online_history_daily = _extract_daily_online_history(normalized)
    availability = {
        "monthly_peak_1y": bool(online_history),
        "daily_precise_30d": bool(online_history_daily),
    }
    if online_history:
        data["online_history_1y"] = online_history[:12]
        data["online_history_monthly_peak_1y"] = online_history
    if online_history_daily:
        data["online_history_daily_precise_30d"] = online_history_daily
    data["online_history_availability"] = availability
    unavailable_reasons = {}
    if not availability["daily_precise_30d"]:
        unavailable_reasons["daily_precise_30d"] = (
            "SteamDB page text returned by Firecrawl does not expose exact daily player values. "
            "Current fallback only has chart labels plus monthly breakdown."
        )
    if unavailable_reasons:
        data["online_history_unavailable_reasons"] = unavailable_reasons

    update_history = _extract_update_history(markdown, table_data)
    if update_history:
        data["update_history"] = update_history
        data["latest_update"] = update_history[0]

    for key in ("last_record_update", "first_seen_on_steamdb", "store_asset_modification_time"):
        if table_data.get(key):
            data[key] = table_data[key]

    changenumber = _extract_changenumber(table_data)
    if changenumber:
        data.update(changenumber)

    return data


def _normalize_markdown(markdown: str) -> str:
    """Normalize Firecrawl Markdown to make regex extraction less fragile."""
    normalized = markdown.replace("\r\n", "\n")
    normalized = normalized.replace("\\\n\\\n", " ")
    normalized = normalized.replace("\\\n", " ")
    normalized = normalized.replace("\u2013", " - ")
    normalized = normalized.replace("\u2014", " - ")
    normalized = normalized.replace("\u00a0", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _build_charts_url(base_url: str, app_id: str | int, time_slice: str = "monthly_peak_1y") -> str:
    fragment = "#1m" if time_slice == "daily_precise_30d" else ""
    return f"{base_url}/app/{app_id}/charts/{fragment}"


def _merge_highcharts_payload(charts: dict[str, Any], payload: list[dict[str, Any]]) -> None:
    if not payload:
        return
    from src.collectors.steam.steamdb_scraper import _merge_highcharts_payload as merge_payload

    merge_payload(charts, payload)
    availability = charts.get("online_history_availability")
    if isinstance(availability, dict) and availability.get("daily_precise_30d"):
        reasons = charts.get("online_history_unavailable_reasons")
        if isinstance(reasons, dict):
            reasons.pop("daily_precise_30d", None)
            if not reasons:
                charts.pop("online_history_unavailable_reasons", None)


def _extract_highcharts_action_payload(action_payload: Any) -> list[dict[str, Any]]:
    if not isinstance(action_payload, dict):
        return []

    returns = action_payload.get("javascriptReturns")
    if not isinstance(returns, list):
        return []

    for item in returns:
        if not isinstance(item, dict):
            continue
        value = item.get("value")
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, list):
                return [entry for entry in parsed if isinstance(entry, dict)]
        if isinstance(value, list):
            return [entry for entry in value if isinstance(entry, dict)]

    return []


def _extract_stats(normalized_markdown: str) -> dict[str, Any]:
    stats: dict[str, Any] = {}

    stat_patterns = {
        "all_time_peak": r"all[\s-]*time peak[^0-9]{0,30}([0-9][0-9,]*)",
        "24h_peak": r"24[\s-]*hour peak[^0-9]{0,30}([0-9][0-9,]*)",
        "current_players": r"([0-9][0-9,]*)\s+in-game\b",
        "followers": r"([0-9][0-9,]*)\s+followers\b",
    }
    for key, pattern in stat_patterns.items():
        match = re.search(pattern, normalized_markdown, re.IGNORECASE)
        if not match:
            continue
        parsed = _parse_int(match.group(1))
        if parsed is not None:
            stats[key] = parsed

    review_match = re.search(
        r"([0-9]+(?:\.[0-9]+)?)\s*([KMB])?\s+reviews\b",
        normalized_markdown,
        re.IGNORECASE,
    )
    if review_match:
        review_count = _parse_compact_number(
            review_match.group(1),
            review_match.group(2),
        )
        if review_count is not None:
            stats["total_reviews"] = review_count

    return stats


def _extract_markdown_tables(markdown: str) -> list[dict[str, Any]]:
    """Return Markdown tables as structured header/row data."""
    tables: list[dict[str, Any]] = []
    current_lines: list[str] = []

    for line in markdown.splitlines():
        if line.strip().startswith("|"):
            current_lines.append(line.rstrip())
            continue

        if current_lines:
            parsed = _parse_markdown_table(current_lines)
            if parsed:
                tables.append(parsed)
            current_lines = []

    if current_lines:
        parsed = _parse_markdown_table(current_lines)
        if parsed:
            tables.append(parsed)

    return tables


def _parse_markdown_table(lines: list[str]) -> dict[str, Any] | None:
    rows = [_split_markdown_row(line) for line in lines]
    meaningful_rows = [
        row
        for row in rows
        if row and not all(cell.strip() and re.fullmatch(r"[:\- ]+", cell) for cell in row)
    ]
    if len(meaningful_rows) < 2:
        return None

    headers_raw = meaningful_rows[0]
    header_count = len(headers_raw)
    if header_count < 2:
        return None

    headers = [_strip_markdown(cell) for cell in headers_raw]
    normalized_headers = _make_unique_headers(headers)

    parsed_rows: list[dict[str, str]] = []
    parsed_rows_raw: list[dict[str, str]] = []
    for raw_row in meaningful_rows[1:]:
        cells_raw = list(raw_row[:header_count])
        if len(cells_raw) < header_count:
            cells_raw.extend([""] * (header_count - len(cells_raw)))
        cells_raw = cells_raw[:header_count]

        cells_clean = [_strip_markdown(cell) for cell in cells_raw]
        parsed_rows.append(
            {
                normalized_headers[idx]: cells_clean[idx]
                for idx in range(header_count)
            }
        )
        parsed_rows_raw.append(
            {
                normalized_headers[idx]: cells_raw[idx].strip()
                for idx in range(header_count)
            }
        )

    return {
        "headers": headers,
        "normalized_headers": normalized_headers,
        "rows": parsed_rows,
        "rows_raw": parsed_rows_raw,
    }


def _split_markdown_row(line: str) -> list[str]:
    row = line.strip()
    if row.startswith("|"):
        row = row[1:]
    if row.endswith("|"):
        row = row[:-1]
    return [cell.strip() for cell in row.split("|")]


def _extract_table_data(tables: list[dict[str, Any]]) -> dict[str, str]:
    table_data: dict[str, str] = {}

    for table in tables:
        headers = table["normalized_headers"]
        if len(headers) != 2:
            continue

        for row in table["rows_raw"]:
            key_cell = row.get(headers[0], "").strip()
            value_cell = row.get(headers[1], "").strip()
            key_text = _strip_markdown(key_cell)
            value_text = _strip_markdown(value_cell)
            if not key_text or not value_text:
                continue
            if key_text.startswith("-") or value_text.startswith("-"):
                continue
            safe_key = _normalize_key(key_text)[:80]
            if safe_key:
                table_data[safe_key] = value_text

    return table_data


def _extract_online_history(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract the monthly Steam charts table, usually the last 12 months."""
    for table in tables:
        headers = set(table["normalized_headers"])
        if "month" not in headers or "peak" not in headers:
            continue
        if not ("average" in headers or "avg" in headers or "gain" in headers):
            continue

        history: list[dict[str, Any]] = []
        for row in table["rows"]:
            month = row.get("month", "").strip()
            peak = row.get("peak", "").strip()
            average = row.get("average", row.get("avg", "")).strip()
            gain = row.get("gain", "").strip()
            if not month or not peak:
                continue

            entry = {
                "month": _clean_period_label(month),
                "peak": peak,
                "peak_value": _parse_int(peak),
            }
            if average:
                entry["average"] = average
                entry["average_value"] = _parse_int(average)
            if gain:
                entry["gain"] = gain
                entry["gain_percent"] = _parse_percent(gain)
            history.append(entry)

        if history:
            return history

    return []


def _extract_daily_online_history(normalized_markdown: str) -> list[dict[str, Any]]:
    """
    Try to extract exact daily points from the chart text.

    Firecrawl usually does not expose the actual series values for SteamDB charts,
    only axis labels and the monthly table. Keep the hook here so the collector can
    advertise the capability when the upstream response eventually contains it.
    """
    del normalized_markdown
    return []


def _extract_update_history(markdown: str, table_data: dict[str, str]) -> list[dict[str, Any]]:
    """Extract patch notes history and timestamps from Markdown/table data."""
    updates: dict[str, dict[str, Any]] = {}

    link_pattern = re.compile(
        r"\[(?P<patch_id>\d+)\]\((?P<url>https://steamdb\.info/patchnotes/\d+/?)\)"
        r"\s*(?:\||\)|\s)\s*(?P<timestamp>[^\n|]+)",
        re.IGNORECASE,
    )
    for match in link_pattern.finditer(markdown):
        patch_id = match.group("patch_id")
        timestamp_raw = _clean_timestamp(match.group("timestamp"))
        if not _looks_like_update_timestamp(timestamp_raw):
            continue
        updates[patch_id] = _build_update_entry(
            patch_id=patch_id,
            patchnote_url=match.group("url"),
            timestamp_raw=timestamp_raw,
        )

    if not updates:
        for key, value in table_data.items():
            if "patchnotes" not in key:
                continue
            patch_match = re.search(r"patchnotes(\d+)", key)
            if not patch_match:
                continue
            patch_id = patch_match.group(1)
            timestamp_raw = _clean_timestamp(value)
            if not _looks_like_update_timestamp(timestamp_raw):
                continue
            updates[patch_id] = _build_update_entry(
                patch_id=patch_id,
                patchnote_url=f"https://steamdb.info/patchnotes/{patch_id}/",
                timestamp_raw=timestamp_raw,
            )

    ordered_updates = list(updates.values())
    ordered_updates.sort(
        key=lambda item: item.get("timestamp_unix") or -1,
        reverse=True,
    )
    return ordered_updates


def _build_update_entry(
    *,
    patch_id: str,
    patchnote_url: str,
    timestamp_raw: str,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "patch_id": int(patch_id),
        "patchnote_url": patchnote_url,
        "timestamp_raw": timestamp_raw,
    }

    absolute_timestamp = _extract_absolute_timestamp(timestamp_raw)
    if absolute_timestamp:
        entry["updated_at"] = absolute_timestamp
        parsed_dt = _parse_steamdb_datetime(absolute_timestamp)
        if parsed_dt:
            entry["timestamp_unix"] = int(parsed_dt.timestamp())

    relative_timestamp = _extract_relative_timestamp(timestamp_raw)
    if relative_timestamp:
        entry["updated_at_relative"] = relative_timestamp

    return entry


def _extract_changenumber(table_data: dict[str, str]) -> dict[str, Any]:
    changenumber_value = _find_table_value(table_data, "last_changenumber")
    if not changenumber_value:
        return {}

    match = re.search(
        r"\[(?P<id>\d+)\]\((?P<url>https://steamdb\.info/changelist/\d+/?)\)",
        changenumber_value,
    )
    if not match:
        parsed_id = _parse_int(changenumber_value)
        return {"last_changenumber": parsed_id} if parsed_id is not None else {}

    return {
        "last_changenumber": int(match.group("id")),
        "last_changenumber_url": match.group("url"),
    }


def _find_table_value(table_data: dict[str, str], prefix: str) -> str | None:
    for key, value in table_data.items():
        if key.startswith(prefix):
            return value
    return None


def _strip_markdown(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    text = text.replace("\\_", "_")
    text = re.sub(r"!\[[^\]]*]\([^)]+\)", "", text)
    text = re.sub(r"\[([^\]]+)]\([^)]+\)", r"\1", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"_\(([^)]+)\)_", r"(\1)", text)
    text = text.replace("**", "").replace("__", "")
    text = text.replace("\u2013", " - ").replace("\u2014", " - ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_key(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("\u2013", " ").replace("\u2014", " ")
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text.strip("_")


def _clean_period_label(value: str) -> str:
    text = _strip_markdown(value)
    match = re.match(r"^(Last 30 days|[A-Za-z]+\s+\d{4})", text)
    if match:
        return match.group(1)
    return text


def _make_unique_headers(headers: list[str]) -> list[str]:
    normalized_headers: list[str] = []
    seen: dict[str, int] = {}
    for index, header in enumerate(headers, start=1):
        base = _normalize_key(header) or f"col_{index}"
        count = seen.get(base, 0)
        seen[base] = count + 1
        normalized_headers.append(base if count == 0 else f"{base}_{count + 1}")
    return normalized_headers


def _parse_int(value: str) -> int | None:
    match = re.search(r"[0-9][0-9,]*", value)
    if not match:
        return None
    try:
        return int(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _parse_compact_number(number: str, suffix: str | None) -> int | None:
    try:
        value = float(number)
    except ValueError:
        return None

    multiplier = {
        None: 1,
        "K": 1_000,
        "M": 1_000_000,
        "B": 1_000_000_000,
    }.get((suffix or "").upper() or None, 1)
    return int(value * multiplier)


def _parse_percent(value: str) -> float | None:
    match = re.search(r"([+-]?[0-9]+(?:\.[0-9]+)?)\s*%", value)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _clean_timestamp(value: str) -> str:
    text = _strip_markdown(value)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" |")


def _looks_like_update_timestamp(value: str) -> bool:
    return _extract_absolute_timestamp(value) is not None


def _extract_absolute_timestamp(value: str) -> str | None:
    match = re.search(
        r"(\d{1,2}\s+[A-Za-z]+\s+\d{4}\s+[ -]\s+\d{2}:\d{2}:\d{2}\s+UTC)",
        value,
    )
    if match:
        return re.sub(r"\s+", " ", match.group(1)).replace(" - ", " - ")
    return None


def _extract_relative_timestamp(value: str) -> str | None:
    absolute_timestamp = _extract_absolute_timestamp(value)
    if not absolute_timestamp:
        text = value.strip()
        return text if text else None

    relative = value.replace(absolute_timestamp, "").strip(" -")
    relative = re.sub(r"\s+", " ", relative)
    return relative or None


def _parse_steamdb_datetime(value: str) -> datetime | None:
    match = re.search(
        r"(?P<date>\d{1,2}\s+[A-Za-z]+\s+\d{4})\s+[ -]\s+(?P<time>\d{2}:\d{2}:\d{2})\s+UTC",
        value,
    )
    if not match:
        return None
    try:
        parsed = datetime.strptime(
            f"{match.group('date')} {match.group('time')}",
            "%d %B %Y %H:%M:%S",
        )
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


def _extract_markdown(result: Any) -> str | None:
    """Handle Firecrawl v1/v2 response shapes."""
    if result is None:
        return None
    if isinstance(result, dict):
        markdown = result.get("markdown")
        if isinstance(markdown, str):
            return markdown
        data = result.get("data")
        if isinstance(data, dict) and isinstance(data.get("markdown"), str):
            return data["markdown"]
        return None

    markdown = getattr(result, "markdown", None)
    if isinstance(markdown, str):
        return markdown

    data = getattr(result, "data", None)
    if isinstance(data, dict) and isinstance(data.get("markdown"), str):
        return data["markdown"]

    return None


def _extract_actions(result: Any) -> Any:
    if result is None:
        return None
    if isinstance(result, dict):
        actions = result.get("actions")
        if actions is not None:
            return actions
        data = result.get("data")
        if isinstance(data, dict):
            return data.get("actions")
        return None

    actions = getattr(result, "actions", None)
    if actions is not None:
        return actions

    data = getattr(result, "data", None)
    if isinstance(data, dict):
        return data.get("actions")

    return None
