"""Run a one-off SteamDB CDP collection smoke test."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from src.collectors.steam.steamdb_scraper import SteamDBScraper


async def main() -> None:
    parser = argparse.ArgumentParser(description="SteamDB CDP smoke collector")
    parser.add_argument("--app-id", default="730")
    parser.add_argument("--time-slice", default="daily_precise_90d")
    parser.add_argument("--cdp-port", type=int, default=9222)
    parser.add_argument("--cdp-optional", action="store_true", help="Fall back to a new browser when CDP is not reachable.")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    scraper = SteamDBScraper(
        cdp_enabled=True,
        cdp_required=not args.cdp_optional,
        cdp_port=args.cdp_port,
        request_delay=5.0,
        request_jitter=2.0,
        timeout=45000,
        headless=True,
    )
    try:
        data = await scraper.scrape(args.app_id, time_slice=args.time_slice)
    finally:
        await scraper.teardown()

    output = Path(args.output or f"tmp/steamdb_cdp_smoke_{args.app_id}.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    charts = data.get("charts") or {}
    info = data.get("info") or {}
    review_availability = charts.get("user_reviews_history_availability") or {}
    summary = {
        "source": data.get("source"),
        "requested_time_slice": data.get("requested_time_slice"),
        "chart_url": charts.get("chart_url"),
        "steamdb_signed_in": info.get("steamdb_signed_in"),
        "daily90_count": len(charts.get("online_history_daily_precise_90d") or []),
        "review_history_count": len(charts.get("user_reviews_history_90d") or []),
        "review_history_reason": review_availability.get("reason", ""),
        "followers_count": len(charts.get("followers_history") or []),
        "wishlist_count": len(charts.get("wishlist_history") or []),
        "patchnotes_count": len(((data.get("patchnotes") or {}).get("items")) or []),
        "sales_prices_count": len(((data.get("sales") or {}).get("prices")) or []),
        "top_sellers_rank": ((data.get("top_sellers") or {}).get("rank")),
        "output": str(output),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
