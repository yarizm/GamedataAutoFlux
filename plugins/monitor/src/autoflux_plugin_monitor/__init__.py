"""Smart monitor collector plugin."""

from src.core.collector_metadata import (
    CollectorMetadata,
    CollectorTargetField,
    CollectorTargetSchema,
    TargetValidationRule,
)
from src.core.plugin_system import PluginSpec, make_template

plugin = PluginSpec(
    name="autoflux-plugin-monitor",
    version="0.1.0",
    description="Steam, Twitch and external-site monitoring collector.",
    modules=(
        "autoflux_plugin_monitor.collector",
        "autoflux_plugin_monitor.probes",
        "autoflux_plugin_monitor.resolvers",
    ),
    collectors=("monitor",),
    metadata=(
        CollectorMetadata(
            collector_id="monitor",
            display_name="Smart Monitor",
            description=(
                "Collects monitoring snapshots from Steam player metrics, Twitch "
                "channel metrics, and configured external sites."
            ),
            capabilities=["steam_metrics", "twitch_metrics", "site_monitoring"],
            supports_checkpoint=True,
            recovery_level="L1",
            target_schema=CollectorTargetSchema(
                default_params={"days": 30, "metrics": ["twitch_viewer_trend"]},
                fields=[
                    CollectorTargetField(
                        key="name",
                        location="name",
                        label="目标名称",
                        description="用于在任务和结果中识别被监测对象。",
                        placeholder="Counter-Strike 2",
                    ),
                    CollectorTargetField(
                        key="app_id",
                        label="Steam App ID",
                        description="Steam 商店页面中的数字应用 ID；与外部站点标识至少填写一个。",
                        placeholder="730",
                    ),
                    CollectorTargetField(
                        key="twitch_name",
                        label="Twitch 分类名",
                        description="可选的 Twitch 游戏分类名称，用于补充直播趋势。",
                        placeholder="Counter-Strike",
                    ),
                    CollectorTargetField(
                        key="siteurl",
                        label="外部站点标识",
                        description="SullyGnome 等监测站点使用的游戏 URL slug。",
                        placeholder="counter-strike_global_offensive",
                    ),
                    CollectorTargetField(
                        key="days",
                        label="趋势天数",
                        description="需要回溯的趋势时间范围。",
                        input_type="number",
                        default=30,
                        minimum=7,
                        maximum=90,
                    ),
                ],
                required_fields=[
                    "target.params.app_id or target.params.siteurl",
                    "target.params.twitch_name (optional)",
                ],
                rules=[
                    TargetValidationRule(
                        mode="any",
                        fields=["target.params.app_id", "target.params.siteurl"],
                        code="missing_monitor_app_id",
                        field="targets[{index}]",
                        message="Monitor target requires app_id or siteurl.",
                        skip_if_error=False,
                    ),
                    TargetValidationRule(
                        mode="all",
                        fields=["target.params.app_id"],
                        check="regex",
                        pattern=r"\d+",
                        optional=True,
                        level="warning",
                        code="invalid_app_id_format",
                        field="targets[{index}].params.app_id",
                        message="app_id should be numeric.",
                        suggested_action="Use a numeric platform app id.",
                    ),
                ],
            ),
        ),
    ),
    pipeline_templates=(
        make_template(
            "monitor_basic", "Monitor 基础采集", "monitor -> cleaner -> sqlalchemy", "monitor"
        ),
    ),
)
