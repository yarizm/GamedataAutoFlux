"""TapTap collector plugin."""

from src.core.collector_metadata import (
    CollectorMetadata,
    CollectorTargetField,
    CollectorTargetSchema,
    CredentialRequirement,
    TargetValidationRule,
)
from src.core.dag_nodes import DagOutputField
from src.core.plugin_system import PluginSpec, make_template

plugin = PluginSpec(
    name="autoflux-plugin-taptap",
    version="0.1.0",
    description="TapTap public pages, reviews and update collection.",
    modules=("autoflux_plugin_taptap.collector", "autoflux_plugin_taptap.resolvers"),
    collectors=("taptap",),
    metadata=(
        CollectorMetadata(
            collector_id="taptap",
            display_name="TapTap",
            description=(
                "Collects public TapTap game metadata, ratings, reviews, and "
                "update entries from an app ID or game URL."
            ),
            capabilities=["public_game_page", "reviews", "updates", "browser_collection"],
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
                    message="Playwright is not importable; browser-backed collection may fail.",
                    suggested_action="pip install playwright && playwright install chromium",
                )
            ],
            target_schema=CollectorTargetSchema(
                default_params={
                    "region": "cn",
                    "metrics": ["details", "reviews", "updates"],
                    "reviews_pages": 1,
                    "reviews_limit": 20,
                    "use_playwright": "auto",
                },
                fields=[
                    CollectorTargetField(
                        key="name",
                        location="name",
                        label="游戏名称",
                        description="用于在任务和结果中识别该游戏。",
                        placeholder="原神",
                    ),
                    CollectorTargetField(
                        key="page_url",
                        label="TapTap 公开页 URL",
                        description="TapTap 国内站游戏详情页完整地址；与 App ID 至少填写一个。",
                        input_type="url",
                        placeholder="https://www.taptap.cn/app/57532",
                    ),
                    CollectorTargetField(
                        key="app_id",
                        label="TapTap App ID",
                        description="TapTap 页面路径中的数字应用 ID。",
                        placeholder="57532",
                    ),
                    CollectorTargetField(
                        key="reviews_pages",
                        label="评论页数",
                        description="需要采集的评论页数。",
                        input_type="number",
                        default=1,
                        minimum=1,
                        maximum=5,
                    ),
                    CollectorTargetField(
                        key="reviews_limit",
                        label="评论数量上限",
                        description="每次任务最多保留的评论条数。",
                        input_type="number",
                        default=20,
                        minimum=1,
                        maximum=100,
                    ),
                ],
                required_fields=["target.params.app_id or target.params.page_url"],
                rules=[
                    TargetValidationRule(
                        mode="any",
                        fields=["target.params.app_id", "target.params.page_url"],
                        code="missing_taptap_target",
                        field="targets[{index}]",
                        message="TapTap target needs app_id or url.",
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
            output_fields=[
                DagOutputField(key="game_name", label="游戏名"),
                DagOutputField(key="app_id", label="App ID"),
            ],
        ),
    ),
    pipeline_templates=(
        make_template(
            "taptap_basic", "TapTap 基础采集", "TapTap -> cleaner -> sqlalchemy", "taptap"
        ),
    ),
)
