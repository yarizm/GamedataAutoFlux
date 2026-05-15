"""Report template definitions and source validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ReportTemplate:
    id: str
    name: str
    description: str
    required_collectors: tuple[str, ...]
    optional_collectors: tuple[str, ...] = ()
    prompt_instruction: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "required_collectors": list(self.required_collectors),
            "optional_collectors": list(self.optional_collectors),
            "prompt_instruction": self.prompt_instruction,
        }


COLLECTOR_LABELS: dict[str, str] = {
    "steam": "Steam",
    "taptap": "TapTap",
    "gtrends": "Google Trends",
    "monitor": "Monitor",
    "events": "事件数据",
    "steam_discussions": "Steam Community Discussions",
    "official_site": "官方网站",
    "qimai": "七麦数据(AppStore)",
}


REPORT_TEMPLATES: dict[str, ReportTemplate] = {
    "general_game": ReportTemplate(
        id="general_game",
        name="通用游戏模板",
        description="适用于同时具备 Steam、TapTap、Google Trends、Monitor、事件与社区讨论数据的游戏。",
        required_collectors=("steam", "taptap", "gtrends", "monitor", "events", "steam_discussions"),
        optional_collectors=("qimai",),
        prompt_instruction=(
            "按综合游戏分析报告输出，覆盖 Steam 表现、TapTap 口碑、Google Trends 热度、"
            "Monitor 外围指标、关键版本/活动事件和社区讨论，并明确标注缺失数据。"
        ),
    ),
    "taptap_game": ReportTemplate(
        id="taptap_game",
        name="TapTap游戏模板",
        description="适用于仅依赖 TapTap 数据的移动端游戏。",
        required_collectors=("taptap",),
        prompt_instruction="按 TapTap 单源口碑与产品表现报告输出，避免引用不存在的外部数据源。",
    ),
    "steam_game": ReportTemplate(
        id="steam_game",
        name="Steam游戏模板",
        description="适用于具备 Steam、Google Trends、Monitor、事件与社区讨论数据的 Steam 游戏。",
        required_collectors=("steam", "gtrends", "monitor", "events", "steam_discussions"),
        prompt_instruction=(
            "按 Steam 游戏分析报告输出，重点解释在线峰值、搜索趋势、外围监控指标、"
            "关键事件、玩家讨论和风险信号。"
        ),
    ),
}


COLLECTOR_ALIASES: dict[str, str] = {
    "google_trends": "gtrends",
    "pytrends": "gtrends",
    "steam_api": "steam",
    "steamdb": "steam",
    "firecrawl": "steam",
    "event": "events",
    "event_data": "events",
    "steam_news": "events",
    "official_website": "official_site",
    "official": "official_site",
}


def list_report_templates() -> list[dict[str, Any]]:
    return [template.to_dict() for template in REPORT_TEMPLATES.values()]


def get_report_template(template_id: str) -> ReportTemplate | None:
    return REPORT_TEMPLATES.get(template_id)


def is_structured_template(template_id: str) -> bool:
    return template_id in REPORT_TEMPLATES


def normalize_collector(value: str | None) -> str:
    normalized = (value or "unknown").strip().lower()
    return COLLECTOR_ALIASES.get(normalized, normalized)


def validate_template_sources(
    template_id: str,
    source_counts: dict[str, int],
) -> dict[str, Any]:
    template = get_report_template(template_id)
    normalized_counts: dict[str, int] = {}
    for collector, count in source_counts.items():
        normalized = normalize_collector(collector)
        normalized_counts[normalized] = normalized_counts.get(normalized, 0) + int(count or 0)

    if template is None:
        return {
            "template": template_id,
            "known_template": False,
            "status": "unchecked",
            "required_collectors": [],
            "available_collectors": sorted(k for k, v in normalized_counts.items() if v > 0),
            "missing_collectors": [],
            "source_counts": normalized_counts,
        }

    available = sorted(k for k, v in normalized_counts.items() if v > 0)
    missing = [collector for collector in template.required_collectors if collector not in available]
    return {
        "template": template.id,
        "template_name": template.name,
        "known_template": True,
        "status": "complete" if not missing else "partial",
        "required_collectors": list(template.required_collectors),
        "optional_collectors": list(template.optional_collectors),
        "available_collectors": available,
        "missing_collectors": missing,
        "source_counts": normalized_counts,
    }
