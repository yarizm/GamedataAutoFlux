"""Google Trends collector plugin."""

from src.core.collector_metadata import (
    CollectorMetadata,
    CollectorTargetField,
    CollectorTargetSchema,
    TargetValidationRule,
)
from src.core.plugin_system import PluginSpec, make_template


plugin = PluginSpec(
    name="autoflux-plugin-gtrends",
    version="0.1.0",
    description="Google Trends time series and related queries collector.",
    modules=(
        "autoflux_plugin_gtrends.collector",
        "autoflux_plugin_gtrends.probes",
        "autoflux_plugin_gtrends.resolvers",
    ),
    collectors=("gtrends",),
    metadata=(
        CollectorMetadata(
            collector_id="gtrends",
            display_name="Google Trends",
            description=(
                "Collects Google Trends interest-over-time series and related "
                "queries for a keyword, region, and time range."
            ),
            capabilities=["trend_timeseries", "related_queries"],
            supports_checkpoint=True,
            recovery_level="L1",
            target_schema=CollectorTargetSchema(
                target_type="keyword",
                fields=[
                    CollectorTargetField(
                        key="name",
                        location="name",
                        label="搜索关键词",
                        description="Google Trends 中要查询的品牌、产品或主题关键词。",
                        required=True,
                        placeholder="Counter-Strike 2",
                    )
                ],
                required_fields=["target.name"],
                rules=[
                    TargetValidationRule(
                        mode="any",
                        fields=["target.name"],
                        code="missing_keyword",
                        field="targets[{index}]",
                        message="Google Trends target needs a keyword name.",
                        skip_if_error=False,
                    )
                ],
            ),
        ),
    ),
    pipeline_templates=(
        make_template(
            "gtrends_basic", "Google Trends 基础采集", "gtrends -> cleaner -> sqlalchemy", "gtrends"
        ),
    ),
)
