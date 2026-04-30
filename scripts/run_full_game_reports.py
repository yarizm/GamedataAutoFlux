from __future__ import annotations

import argparse
import asyncio
import csv
import importlib
import json
import pkgutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from src.core.config import load_settings
from src.core.pipeline import Pipeline
from src.core.task import Task, TaskStatus, TaskTarget
from src.reporting.generator import ReportGenerator
from src.storage.base import StorageRecord


if sys.platform == "win32" and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


@dataclass(frozen=True)
class SourceSpec:
    name: str
    pipeline_name: str
    target_params: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    reason: str = ""


@dataclass(frozen=True)
class GameSpec:
    name: str
    group_id: str
    report_template: str = "general_game"
    steam_app_id: int | None = None
    steam_name: str | None = None
    twitch_name: str | None = None
    sully_siteurl: str | None = None
    taptap_app_id: str | None = None
    taptap_region: str = "cn"
    qimai_app_id: str | None = None
    qimai_country: str = "cn"
    google_keyword: str | None = None


GAMES: list[GameSpec] = [
    GameSpec("三角洲行动", "delta_force", steam_app_id=2507950, steam_name="Delta Force", twitch_name="Delta Force", sully_siteurl="delta_force_hawk_ops", google_keyword="三角洲行动"),
    GameSpec("永劫无间", "naraka", steam_app_id=1203220, steam_name="NARAKA: BLADEPOINT", twitch_name="NARAKA: BLADEPOINT", google_keyword="永劫无间"),
    GameSpec("Arc Raiders", "arc_raiders", steam_app_id=1808500, steam_name="ARC Raiders", twitch_name="ARC Raiders", google_keyword="ARC Raiders"),
    GameSpec("失落飞船：马拉松", "marathon", steam_app_id=3065800, steam_name="Marathon", twitch_name="Marathon", google_keyword="Marathon game"),
    GameSpec("黑夜君临", "nightreign", steam_app_id=2622380, steam_name="ELDEN RING NIGHTREIGN", twitch_name="ELDEN RING NIGHTREIGN", google_keyword="ELDEN RING NIGHTREIGN"),
    GameSpec("超自然行动组", "chaoziran", google_keyword="超自然行动组"),
    GameSpec("第五人格", "identity_v", google_keyword="第五人格"),
    GameSpec("燕云十六声", "where_winds_meet", steam_app_id=3564740, steam_name="Where Winds Meet", twitch_name="Where Winds Meet", google_keyword="燕云十六声"),
    GameSpec("命运2", "destiny2", steam_app_id=1085660, steam_name="Destiny 2", twitch_name="Destiny 2", google_keyword="命运2"),
    GameSpec("怪物猎人：荒野", "monster_hunter_wilds", steam_app_id=2246340, steam_name="Monster Hunter Wilds", twitch_name="Monster Hunter Wilds", google_keyword="怪物猎人 荒野"),
    GameSpec("星际战甲", "warframe", steam_app_id=230410, steam_name="Warframe", twitch_name="Warframe", google_keyword="Warframe"),
    GameSpec("碧蓝幻想 relink", "granblue_relink", steam_app_id=881020, steam_name="Granblue Fantasy: Relink", twitch_name="Granblue Fantasy Relink", google_keyword="Granblue Fantasy Relink"),
    GameSpec("FF14", "ff14", steam_app_id=39210, steam_name="FINAL FANTASY XIV Online", twitch_name="FINAL FANTASY XIV Online", google_keyword="FF14"),
    GameSpec("永恒之塔2", "aion2", steam_app_id=3393110, steam_name="AION 2", twitch_name="AION 2", google_keyword="AION 2"),
]


PIPELINES: dict[str, Pipeline] = {
    "autoflux_official_site_basic": Pipeline("autoflux_official_site_basic")
    .add_collector("official_site", {})
    .add_processor("cleaner", {})
    .add_storage("local", {}),
    "autoflux_steam_basic": Pipeline("autoflux_steam_basic")
    .add_collector("steam", {"request_delay": 0.5})
    .add_processor("cleaner", {})
    .add_storage("local", {}),
    "autoflux_steam_discussions_basic": Pipeline("autoflux_steam_discussions_basic")
    .add_collector("steam_discussions", {"request_delay": 1.0, "max_pages": 1, "max_topics": 20, "include_replies": False})
    .add_processor("cleaner", {})
    .add_storage("local", {}),
    "autoflux_gtrends_basic": Pipeline("autoflux_gtrends_basic")
    .add_collector("gtrends", {})
    .add_processor("cleaner", {})
    .add_storage("local", {}),
    "autoflux_monitor_basic": Pipeline("autoflux_monitor_basic")
    .add_collector("monitor", {})
    .add_processor("cleaner", {})
    .add_storage("local", {}),
    "autoflux_taptap_basic": Pipeline("autoflux_taptap_basic")
    .add_collector("taptap", {})
    .add_processor("cleaner", {})
    .add_storage("local", {}),
    "autoflux_qimai_basic": Pipeline("autoflux_qimai_basic")
    .add_collector("qimai", {})
    .add_processor("cleaner", {})
    .add_storage("local", {}),
}


def _auto_discover_plugins() -> None:
    for package_name in ("src.collectors", "src.processors", "src.storage"):
        package = importlib.import_module(package_name)
        package_path = Path(package.__file__).parent
        for _, module_name, _ in pkgutil.iter_modules([str(package_path)]):
            if module_name == "base":
                continue
            importlib.import_module(f"{package_name}.{module_name}")


def _load_official_recipes() -> dict[str, dict[str, Any]]:
    settings = yaml.safe_load(Path("config/settings.yaml").read_text(encoding="utf-8")) or {}
    recipes = ((settings.get("official_site") or {}).get("recipes") or {})
    return {str(name): params for name, params in recipes.items() if isinstance(params, dict)}


def _build_sources(game: GameSpec, official_recipes: dict[str, dict[str, Any]], *, skip_heavy_steamdb: bool) -> list[SourceSpec]:
    sources = []
    if game.name in official_recipes:
        sources.append(SourceSpec("official_site", "autoflux_official_site_basic", {}))
    else:
        sources.append(SourceSpec("official_site", "autoflux_official_site_basic", {}, enabled=False, reason="official_site recipe missing"))

    if game.steam_app_id:
        sources.append(
            SourceSpec(
                "steam",
                "autoflux_steam_basic",
                {
                    "app_id": game.steam_app_id,
                    "skip_steamdb": skip_heavy_steamdb,
                    "steamdb_time_slice": "daily_90d",
                    "max_reviews": 200,
                    "review_trend_days": 90,
                    "review_trend_mode": "histogram",
                },
            )
        )
        start_at = (datetime.now() - timedelta(days=14)).date().isoformat()
        sources.append(
            SourceSpec(
                "steam_discussions",
                "autoflux_steam_discussions_basic",
                {
                    "app_id": game.steam_app_id,
                    "start_at": start_at,
                    "max_pages": 1,
                    "max_topics": 20,
                    "include_replies": False,
                },
            )
        )
        sources.append(
            SourceSpec(
                "monitor",
                "autoflux_monitor_basic",
                {
                    "app_id": game.steam_app_id,
                    "metrics": ["twitch_viewer_trend"],
                    "days": 90,
                    "twitch_name": game.twitch_name or game.steam_name or game.name,
                    **({"siteurl": game.sully_siteurl} if game.sully_siteurl else {}),
                },
            )
        )
    else:
        sources.append(SourceSpec("steam", "autoflux_steam_basic", {}, enabled=False, reason="no Steam app_id"))
        sources.append(SourceSpec("steam_discussions", "autoflux_steam_discussions_basic", {}, enabled=False, reason="no Steam app_id"))
        sources.append(SourceSpec("monitor", "autoflux_monitor_basic", {}, enabled=False, reason="no Steam app_id"))

    sources.append(
        SourceSpec(
            "gtrends",
            "autoflux_gtrends_basic",
            {"keyword": game.google_keyword or game.name, "timeframe": "today 12-m", "hl": "zh-CN"},
        )
    )

    if game.taptap_app_id:
        sources.append(
            SourceSpec(
                "taptap",
                "autoflux_taptap_basic",
                {
                    "app_id": game.taptap_app_id,
                    "region": game.taptap_region,
                    "metrics": ["details", "reviews", "updates"],
                    "reviews_pages": 1,
                    "reviews_limit": 20,
                },
            )
        )
    else:
        sources.append(SourceSpec("taptap", "autoflux_taptap_basic", {}, enabled=False, reason="no TapTap app_id/page_url"))

    if game.qimai_app_id:
        sources.append(
            SourceSpec(
                "qimai",
                "autoflux_qimai_basic",
                {"qimai_app_id": game.qimai_app_id, "country": game.qimai_country},
            )
        )
    else:
        sources.append(SourceSpec("qimai", "autoflux_qimai_basic", {}, enabled=False, reason="no Qimai/App Store app_id"))
    return sources


async def _run_task(
    scheduler: Any,
    game: GameSpec,
    source: SourceSpec,
    *,
    run_id: str,
    per_task_timeout: int,
) -> tuple[dict[str, Any], list[StorageRecord]]:
    started = time.time()
    task = Task(
        name=f"[全量采集 {run_id}] {game.name} - {source.name}",
        description=f"Full sequential data collection for {game.name}; source={source.name}",
        pipeline_name=source.pipeline_name,
        collector_name=source.name,
        targets=[
            TaskTarget(
                name=game.name,
                target_type="game",
                params=source.target_params,
            )
        ],
        config={
            "data_group": {"id": f"fullrun_{run_id}_{game.group_id}", "name": f"{game.name}_{run_id}"},
            "full_run": {"run_id": run_id, "game": game.name, "source": source.name},
        },
        max_retries=0,
    )
    await scheduler.submit(task, pipeline_name=source.pipeline_name)
    while True:
        live_task = scheduler.get_task(task.id) or task
        if live_task.is_terminal:
            break
        if time.time() - started > per_task_timeout:
            await scheduler.cancel(task.id)
            break
        await asyncio.sleep(2)

    final_task = scheduler.get_task(task.id) or task
    result = final_task.result
    records = list(getattr(result, "output_records", []) or [])
    status = {
        "game": game.name,
        "source": source.name,
        "task_id": task.id,
        "pipeline": source.pipeline_name,
        "status": final_task.status.value,
        "duration_seconds": round(time.time() - started, 1),
        "records": len(records),
        "error": final_task.error or "; ".join(getattr(result, "errors", []) or []),
    }
    return status, records


async def _run(args: argparse.Namespace) -> list[dict[str, Any]]:
    load_settings()
    _auto_discover_plugins()

    from src.web.app import scheduler, report_generator

    await scheduler.start()
    for pipeline in PIPELINES.values():
        await scheduler.save_pipeline(pipeline)

    official_recipes = _load_official_recipes()
    selected_names = set(args.games or [])
    games = [game for game in GAMES if not selected_names or game.name in selected_names or game.group_id in selected_names]
    if not games:
        raise SystemExit("No matching games selected.")

    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir) / f"full_game_run_{run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_rows: list[dict[str, Any]] = []

    try:
        for index, game in enumerate(games, start=1):
            print(f"\n=== [{index}/{len(games)}] {game.name} ===", flush=True)
            game_records: list[StorageRecord] = []
            sources = _build_sources(game, official_recipes, skip_heavy_steamdb=args.skip_steamdb)
            for source in sources:
                if not source.enabled:
                    row = {
                        "game": game.name,
                        "source": source.name,
                        "task_id": "",
                        "pipeline": source.pipeline_name,
                        "status": "skipped",
                        "duration_seconds": 0,
                        "records": 0,
                        "error": source.reason,
                    }
                    summary_rows.append(row)
                    print(json.dumps(row, ensure_ascii=False), flush=True)
                    continue
                print(f"--> {game.name} / {source.name}", flush=True)
                row, records = await _run_task(
                    scheduler,
                    game,
                    source,
                    run_id=run_id,
                    per_task_timeout=args.per_task_timeout,
                )
                summary_rows.append(row)
                game_records.extend(records)
                print(json.dumps(row, ensure_ascii=False), flush=True)

            report = await report_generator.generate_excel(
                prompt=f"{game.name} 全量数据监测报告（run_id={run_id}）",
                data_source=f"fullrun_{run_id}_{game.group_id}",
                template=game.report_template,
                params={"include_llm_analysis": False},
                records=game_records,
                metadata={
                    "run_id": run_id,
                    "game": game.name,
                    "group_id": f"fullrun_{run_id}_{game.group_id}",
                    "source_task_ids": [row["task_id"] for row in summary_rows if row["game"] == game.name and row.get("task_id")],
                },
            )
            report_row = {
                "game": game.name,
                "source": "report",
                "task_id": "",
                "pipeline": "",
                "status": "success",
                "duration_seconds": 0,
                "records": report.matched_records,
                "error": "",
                "report_id": report.id,
                "excel_path": report.excel_path or "",
            }
            summary_rows.append(report_row)
            print(json.dumps(report_row, ensure_ascii=False), flush=True)
    finally:
        await scheduler.stop()

    summary_json = output_dir / "summary.json"
    summary_json.write_text(json.dumps(summary_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_csv = output_dir / "summary.csv"
    fieldnames = sorted({key for row in summary_rows for key in row.keys()})
    with summary_csv.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)
    print(f"\nsummary_json={summary_json}", flush=True)
    print(f"summary_csv={summary_csv}", flush=True)
    return summary_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Run sequential full data collection and one report per game.")
    parser.add_argument("--games", nargs="*", default=None, help="Optional game names or group ids.")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--output-dir", default="tmp")
    parser.add_argument("--per-task-timeout", type=int, default=900)
    parser.add_argument("--skip-steamdb", action="store_true", help="Skip SteamDB inside steam collector.")
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
