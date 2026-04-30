from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from pathlib import Path

from src.collectors.base import CollectTarget
from src.collectors.official_site_collector import OfficialSiteCollector


def _summarize(data: dict) -> dict:
    counts = {section: len((data.get(section) or {}).get("items", [])) for section in ("news", "patch_notes", "events")}
    all_items = []
    for section in ("news", "patch_notes", "events"):
        all_items.extend((data.get(section) or {}).get("items", []))
    dates = sorted([item.get("date") for item in all_items if isinstance(item, dict) and item.get("date")])
    samples = []
    for section in ("news", "patch_notes", "events"):
        for item in (data.get(section) or {}).get("items", [])[:3]:
            samples.append(
                {
                    "section": section,
                    "date": item.get("date", ""),
                    "category": item.get("category", ""),
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                }
            )
    return {
        "counts": counts,
        "total": sum(counts.values()),
        "date_range": [dates[0], dates[-1]] if dates else [],
        "pages_discovered": (data.get("source_meta") or {}).get("pages_discovered"),
        "pages_crawled": (data.get("source_meta") or {}).get("pages_crawled"),
        "warnings": (data.get("source_meta") or {}).get("warnings", [])[:5],
        "samples": samples,
    }


async def _run(args: argparse.Namespace) -> dict:
    started = time.time()
    collector = OfficialSiteCollector(
        {
            "request_delay": args.request_delay,
            "timeout": args.timeout,
            "playwright_timeout": args.playwright_timeout,
        }
    )
    target = CollectTarget(
        name=args.name,
        target_type="game",
        params={
            "official_url": args.url,
            "use_playwright": args.use_playwright,
            "since_days": args.since_days,
            "max_pages": args.max_pages,
            "max_depth": args.max_depth,
        },
    )
    result = await asyncio.wait_for(collector.collect(target), timeout=args.per_game_timeout)
    data = result.data or {}
    suffix = hashlib.sha1(f"{args.name}|{args.url}".encode("utf-8")).hexdigest()[:10]
    output = Path("tmp") / f"official_site_validate_{suffix}.json"
    output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = {
        "game": args.name,
        "url": args.url,
        "success": result.success,
        "error": result.error,
        "duration": round(time.time() - started, 1),
        "output": str(output),
    }
    summary.update(_summarize(data))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--since-days", type=int, default=90)
    parser.add_argument("--max-pages", type=int, default=30)
    parser.add_argument("--max-depth", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=25)
    parser.add_argument("--playwright-timeout", type=int, default=25000)
    parser.add_argument("--per-game-timeout", type=int, default=180)
    parser.add_argument("--request-delay", type=float, default=0.0)
    parser.add_argument("--use-playwright", default="auto")
    args = parser.parse_args()

    try:
        summary = asyncio.run(_run(args))
    except Exception as exc:
        summary = {
            "game": args.name,
            "url": args.url,
            "success": False,
            "error": repr(exc),
            "duration": 0,
            "counts": {"news": 0, "patch_notes": 0, "events": 0},
            "total": 0,
            "date_range": [],
            "pages_discovered": 0,
            "pages_crawled": 0,
            "warnings": [],
            "samples": [],
            "output": "",
        }

    print(json.dumps(summary, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
