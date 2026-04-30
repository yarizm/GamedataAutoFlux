from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from src.collectors.base import CollectTarget
from src.collectors.official_site_collector import OfficialSiteCollector


DEFAULT_GAMES = [
    "三角洲行动",
    "永劫无间",
    "Arc Raiders",
    "失落飞船：马拉松",
    "黑夜君临",
    "超自然行动组",
    "第五人格",
    "燕云十六声",
    "命运2",
    "怪物猎人：荒野",
    "星际战甲",
    "碧蓝幻想 relink",
    "FF14",
    "永恒之塔2",
]


def _load_recipes() -> dict[str, dict[str, Any]]:
    settings = yaml.safe_load(Path("config/settings.yaml").read_text(encoding="utf-8")) or {}
    recipes = ((settings.get("official_site") or {}).get("recipes") or {})
    return {str(name): params for name, params in recipes.items() if isinstance(params, dict)}


def _summarize_items(data: dict[str, Any]) -> dict[str, Any]:
    sections = ("news", "patch_notes", "events")
    counts = {section: len((data.get(section) or {}).get("items", [])) for section in sections}
    all_items: list[dict[str, Any]] = []
    for section in sections:
        for item in (data.get(section) or {}).get("items", []):
            if isinstance(item, dict):
                all_items.append(item)

    dates = sorted(str(item.get("date")) for item in all_items if item.get("date"))
    samples = []
    for section in sections:
        for item in (data.get(section) or {}).get("items", [])[:2]:
            if not isinstance(item, dict):
                continue
            samples.append(
                {
                    "section": section,
                    "date": item.get("date", ""),
                    "category": item.get("category") or item.get("type") or "",
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                }
            )

    return {
        "news_count": counts["news"],
        "patch_notes_count": counts["patch_notes"],
        "events_count": counts["events"],
        "total_count": sum(counts.values()),
        "date_start": dates[0] if dates else "",
        "date_end": dates[-1] if dates else "",
        "samples": samples,
    }


async def _collect_one(
    game_name: str,
    recipe: dict[str, Any],
    *,
    output_dir: Path,
    per_game_timeout: int,
    request_delay: float,
    timeout: float,
    playwright_timeout: int,
) -> dict[str, Any]:
    started = time.time()
    collector = OfficialSiteCollector(
        {
            "request_delay": request_delay,
            "timeout": timeout,
            "playwright_timeout": playwright_timeout,
        }
    )
    target = CollectTarget(name=game_name, target_type="game", params={})
    summary: dict[str, Any] = {
        "game": game_name,
        "configured_url": recipe.get("official_url", ""),
        "validation_status": recipe.get("validation_status", ""),
        "validation_notes": recipe.get("validation_notes", ""),
        "success": False,
        "usable": False,
        "error": "",
        "duration_seconds": 0.0,
        "output": "",
        "pages_discovered": 0,
        "pages_crawled": 0,
        "warnings": [],
        "news_count": 0,
        "patch_notes_count": 0,
        "events_count": 0,
        "total_count": 0,
        "date_start": "",
        "date_end": "",
        "samples": [],
    }

    try:
        result = await asyncio.wait_for(collector.collect(target), timeout=per_game_timeout)
        data = result.data if isinstance(result.data, dict) else {}
        suffix = hashlib.sha1(game_name.encode("utf-8")).hexdigest()[:10]
        output_path = output_dir / f"official_site_{suffix}.json"
        output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

        source_meta = data.get("source_meta") or {}
        summary.update(
            {
                "success": bool(result.success),
                "error": result.error or "",
                "output": str(output_path),
                "resolved_url": data.get("official_url", ""),
                "recipe": source_meta.get("recipe", ""),
                "pages_discovered": source_meta.get("pages_discovered", 0),
                "pages_crawled": source_meta.get("pages_crawled", 0),
                "warnings": source_meta.get("warnings", []),
            }
        )
        summary.update(_summarize_items(data))
        summary["usable"] = bool(summary["success"] and summary["total_count"] > 0)
    except Exception as exc:
        summary["error"] = repr(exc)
    finally:
        summary["duration_seconds"] = round(time.time() - started, 1)

    return summary


async def _run(args: argparse.Namespace) -> list[dict[str, Any]]:
    recipes = _load_recipes()
    selected = args.games or DEFAULT_GAMES
    missing = [name for name in selected if name not in recipes]
    if missing:
        raise SystemExit(f"Recipes not found: {', '.join(missing)}")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir) / f"official_site_recipe_smoke_{run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    summaries: list[dict[str, Any]] = []
    for index, game_name in enumerate(selected, start=1):
        print(f"[{index}/{len(selected)}] {game_name}", flush=True)
        summary = await _collect_one(
            game_name,
            recipes[game_name],
            output_dir=output_dir,
            per_game_timeout=args.per_game_timeout,
            request_delay=args.request_delay,
            timeout=args.timeout,
            playwright_timeout=args.playwright_timeout,
        )
        summaries.append(summary)
        print(
            json.dumps(
                {
                    "game": summary["game"],
                    "success": summary["success"],
                    "usable": summary["usable"],
                    "total": summary["total_count"],
                    "date_range": [summary["date_start"], summary["date_end"]],
                    "duration": summary["duration_seconds"],
                    "error": summary["error"],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    summary_json = output_dir / "summary.json"
    summary_json.write_text(json.dumps(summaries, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_csv = output_dir / "summary.csv"
    fieldnames = [
        "game",
        "success",
        "usable",
        "validation_status",
        "configured_url",
        "resolved_url",
        "total_count",
        "news_count",
        "patch_notes_count",
        "events_count",
        "date_start",
        "date_end",
        "pages_discovered",
        "pages_crawled",
        "duration_seconds",
        "error",
        "output",
        "validation_notes",
    ]
    with summary_csv.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for summary in summaries:
            writer.writerow({field: summary.get(field, "") for field in fieldnames})

    print(f"summary_json={summary_json}", flush=True)
    print(f"summary_csv={summary_csv}", flush=True)
    return summaries


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test official_site recipes.")
    parser.add_argument("--games", nargs="*", default=None, help="Optional subset of recipe names.")
    parser.add_argument("--output-dir", default="tmp")
    parser.add_argument("--per-game-timeout", type=int, default=420)
    parser.add_argument("--timeout", type=float, default=25)
    parser.add_argument("--playwright-timeout", type=int, default=25000)
    parser.add_argument("--request-delay", type=float, default=0.0)
    args = parser.parse_args()

    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
