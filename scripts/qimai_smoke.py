"""Run a one-off Qimai collection smoke test."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from src.collectors.base import CollectTarget
from src.collectors.qimai_collector import QimaiCollector


async def main() -> None:
    parser = argparse.ArgumentParser(description="Qimai smoke collector")
    parser.add_argument("--app-id", default="1642894547")
    parser.add_argument("--name", default="Delta Force")
    parser.add_argument("--country", default="cn")
    parser.add_argument("--cdp-port", type=int, default=9222)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    collector = QimaiCollector(
        {
            "cdp_enabled": True,
            "cdp_port": args.cdp_port,
            "request_delay": 2.0,
            "request_jitter": 0.5,
            "click_delay": 0.8,
            "scroll_delay": 1.0,
            "max_api_payloads": 220,
        }
    )
    await collector.setup()
    result = await collector.collect(
        CollectTarget(
            name=args.name,
            url="",
            params={"qimai_app_id": args.app_id, "country": args.country},
        )
    )

    payload = {
        "success": result.success,
        "error": result.error,
        "data": result.data,
        "metadata": result.metadata,
    }
    output = Path(args.output or f"tmp/qimai_smoke_{args.app_id}.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {output}")
    if result.data and isinstance(result.data.get("qimai"), dict):
        qimai = result.data["qimai"]
        for key in (
            "ios_grossing_rank_trend",
            "appstore_review_trend",
            "dau_trend_90d",
            "downloads_trend_90d",
            "revenue_trend_90d",
        ):
            value = qimai.get(key)
            print(f"{key}: {len(value) if isinstance(value, list) else 0}")
        print("api_response_count:", qimai.get("api_response_count"))
        print("api_urls:", qimai.get("api_urls", [])[:20])
        print("export_sources:", qimai.get("qimai_export_sources"))
        print("warnings:", qimai.get("extraction_warnings"))


if __name__ == "__main__":
    asyncio.run(main())
