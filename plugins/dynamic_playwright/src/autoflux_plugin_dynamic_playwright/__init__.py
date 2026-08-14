"""Generic Playwright collector plugin."""

from src.core.collector_metadata import (
    CollectorMetadata,
    CollectorTargetField,
    CollectorTargetSchema,
    CredentialRequirement,
    TargetValidationRule,
)
from src.core.plugin_system import PluginSpec, make_template


plugin = PluginSpec(
    name="autoflux-plugin-dynamic-playwright",
    version="0.1.0",
    description="Configuration-driven collector for dynamic web pages.",
    modules=(
        "autoflux_plugin_dynamic_playwright.collector",
        "autoflux_plugin_dynamic_playwright.validators",
    ),
    collectors=("dynamic_playwright",),
    metadata=(
        CollectorMetadata(
            collector_id="dynamic_playwright",
            display_name="Dynamic Playwright",
            description=(
                "Runs a configuration-driven Playwright browser flow against a "
                "dynamic page and extracts records with declared selectors."
            ),
            capabilities=["browser_collection", "custom_selectors", "dynamic_pages"],
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
                    message="Playwright is not importable; dynamic collection will fail.",
                    suggested_action="pip install playwright && playwright install chromium",
                )
            ],
            target_schema=CollectorTargetSchema(
                fields=[
                    CollectorTargetField(
                        key="name",
                        location="name",
                        label="目标名称",
                        description="用于标识本次采集目标，例如页面或站点名称。",
                        required=True,
                        placeholder="Example website",
                    )
                ],
                required_fields=["target.name"],
                rules=[
                    TargetValidationRule(
                        mode="any",
                        fields=["target.name"],
                        level="warning",
                        code="missing_target_name",
                        field="targets[{index}]",
                        message="Dynamic Playwright target should have a game name.",
                        skip_if_error=False,
                    )
                ],
            ),
            config_schema={
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": {
                        "type": "string",
                        "format": "uri",
                        "title": "页面 URL 模板",
                        "description": "要访问的页面地址，可使用目标参数占位符，例如 https://example.com/{id}。",
                    },
                    "fields": {
                        "type": "object",
                        "title": "字段提取规则",
                        "description": "CSS 选择器映射；请在高级 JSON 中编辑。",
                    },
                    "wait_for": {
                        "type": "string",
                        "title": "等待选择器",
                        "description": "可选。页面出现该 CSS 选择器后再开始提取。",
                    },
                },
            },
        ),
    ),
    pipeline_templates=(
        make_template(
            "dynamic_playwright_basic",
            "动态浏览器网页采集",
            "dynamic_playwright -> cleaner -> sqlalchemy",
            "dynamic_playwright",
        ),
    ),
)
