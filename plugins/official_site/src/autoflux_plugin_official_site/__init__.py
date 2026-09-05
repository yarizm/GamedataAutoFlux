"""Official-site collector plugin."""

from src.core.collector_metadata import (
    CollectorMetadata,
    CollectorTargetField,
    CollectorTargetSchema,
    CredentialRequirement,
    TargetValidationRule,
)
from src.core.plugin_system import PluginSpec, make_template

plugin = PluginSpec(
    name="autoflux-plugin-official-site",
    version="0.1.0",
    description="Official website news, announcements, events and update collector.",
    modules=(
        "autoflux_plugin_official_site.collector",
        "autoflux_plugin_official_site.probes",
        "autoflux_plugin_official_site.resolvers",
    ),
    collectors=("official_site",),
    metadata=(
        CollectorMetadata(
            collector_id="official_site",
            display_name="Official Site",
            description=(
                "Discovers and collects news, announcements, events, and update "
                "pages from an official website URL."
            ),
            capabilities=["official_news", "announcements", "events", "browser_collection"],
            credential_profiles=["playwright_runtime"],
            credential_requirements=[
                CredentialRequirement(
                    requirement_id="playwright",
                    kind="python_module",
                    module="playwright",
                    status_key="playwright",
                    level="warning",
                    code="missing_playwright",
                    field="playwright",
                    message="Playwright is not importable; browser fallback will be unavailable.",
                    suggested_action="pip install playwright && playwright install chromium",
                )
            ],
            target_schema=CollectorTargetSchema(
                default_params={"use_playwright": "auto"},
                fields=[
                    CollectorTargetField(
                        key="name",
                        location="name",
                        label="站点名称",
                        description="用于在任务和结果中识别该官网。",
                        placeholder="Counter-Strike 2",
                    ),
                    CollectorTargetField(
                        key="official_url",
                        label="官网 URL",
                        description="新闻、公告或更新页面所在官网的完整 http/https 地址。",
                        input_type="url",
                        required=True,
                        placeholder="https://www.counter-strike.net/news",
                    ),
                ],
                required_fields=["target.params.official_url"],
                rules=[
                    TargetValidationRule(
                        mode="any",
                        fields=["target.params.official_url"],
                        code="missing_official_url",
                        field="targets[{index}]",
                        message="Official site target needs official_url.",
                        skip_if_error=False,
                    ),
                    TargetValidationRule(
                        mode="all",
                        fields=["target.params.official_url"],
                        check="absolute_url",
                        code="invalid_official_url",
                        field="targets[{index}].params.official_url",
                        message="official_url must be an absolute URL (http/https).",
                        suggested_action="Provide a full URL including scheme.",
                    ),
                ],
            ),
        ),
    ),
    pipeline_templates=(
        make_template(
            "official_site_basic",
            "游戏官网基础采集",
            "official_site -> cleaner -> sqlalchemy",
            "official_site",
        ),
    ),
)
