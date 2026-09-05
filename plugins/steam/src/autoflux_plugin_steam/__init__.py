"""Steam and Steam Community collector plugin."""

from src.core.collector_metadata import (
    CollectorMetadata,
    CollectorTargetField,
    CollectorTargetSchema,
    CredentialRequirement,
    SessionAccountSpec,
    SessionCheckSpec,
    TargetValidationRule,
)
from src.core.dag_nodes import DagOutputField
from src.core.plugin_system import PluginSpec, make_template

plugin = PluginSpec(
    name="autoflux-plugin-steam",
    version="0.1.0",
    description="Steam Store, reviews, SteamDB and Community Discussions collectors.",
    modules=(
        "autoflux_plugin_steam.collector",
        "autoflux_plugin_steam.discussions",
        "autoflux_plugin_steam.probes",
        "autoflux_plugin_steam.resolvers",
    ),
    collectors=("steam", "steam_discussions"),
    metadata=(
        CollectorMetadata(
            collector_id="steam",
            display_name="Steam",
            description=(
                "Collects Steam store details, review summaries, player metrics, "
                "and optional SteamDB metrics for a game name or app ID."
            ),
            capabilities=["steam_store", "steam_reviews", "steam_api", "steamdb_optional_browser"],
            supports_checkpoint=True,
            recovery_level="L1",
            session_config_key="steam.steamdb.session_mode",
            credential_profiles=["steam_api_key", "steamdb_optional_browser_session"],
            credential_requirements=[
                CredentialRequirement(
                    requirement_id="steam_api_key",
                    kind="config_value",
                    config_key="steam.api_key",
                    status_key="steam.api_key",
                    level="warning",
                    code="missing_steam_api_key",
                    field="steam.api_key",
                    message="Steam API Key is missing; official Steam APIs may be unavailable.",
                    suggested_action="Set steam.api_key in settings or .env.",
                ),
                CredentialRequirement(
                    requirement_id="playwright",
                    kind="python_module",
                    module="playwright",
                    status_key="playwright",
                    level="warning",
                    code="missing_playwright",
                    field="playwright",
                    message="Playwright is not importable; SteamDB browser collection may fail.",
                    suggested_action="pip install playwright && playwright install chromium",
                    when_all={"steam.steamdb.enabled": True},
                ),
            ],
            session_accounts=[
                SessionAccountSpec(
                    account_id="local:steamdb_profile",
                    account_kind="local_profile",
                    locator_config_key="steam.steamdb.cdp_profile_dir",
                    default_locator="data/steamdb_profile",
                    locator_label="cdp_profile_dir",
                    when_all={"steam.steamdb.enabled": True},
                )
            ],
            session_checks=[
                SessionCheckSpec(
                    check_id="session:steamdb",
                    kind="notice",
                    level="warning",
                    when_all={
                        "steam.steamdb.enabled": True,
                        "steam.steamdb.cdp_enabled": False,
                    },
                    message=(
                        "SteamDB CDP session is disabled; SteamDB pages may hit captcha or "
                        "rate limits"
                    ),
                    suggested_action="enable_cdp_browser",
                ),
                SessionCheckSpec(
                    check_id="session:steamdb",
                    kind="http_endpoint",
                    config_key="steam.steamdb.cdp_port",
                    default_value=9222,
                    endpoint_template="http://127.0.0.1:{value}/json/version",
                    detail_key="cdp_port",
                    level="warning",
                    when_all={
                        "steam.steamdb.enabled": True,
                        "steam.steamdb.cdp_enabled": True,
                    },
                    message=(
                        "SteamDB CDP browser is not reachable; launch and log in before "
                        "SteamDB collection"
                    ),
                    ok_message="SteamDB CDP browser is reachable",
                    suggested_action="open_steamdb_browser",
                ),
            ],
            target_schema=CollectorTargetSchema(
                default_params={"skip_steamdb": True},
                fields=[
                    CollectorTargetField(
                        key="name",
                        location="name",
                        label="游戏名称",
                        description="Steam 游戏名称，用于识别任务目标并辅助解析 App ID。",
                        placeholder="Counter-Strike 2",
                    ),
                    CollectorTargetField(
                        key="app_id",
                        label="Steam App ID",
                        description="Steam 商店链接中的数字 ID；建议填写以避免同名游戏匹配错误。",
                        placeholder="730",
                    ),
                    CollectorTargetField(
                        key="skip_steamdb",
                        label="仅使用 Steam 官方接口",
                        description="开启后跳过需要浏览器会话的 SteamDB 指标采集。",
                        input_type="boolean",
                        default=True,
                    ),
                    CollectorTargetField(
                        key="steamdb_time_slice",
                        label="SteamDB 历史区间",
                        description="关闭“仅使用官方接口”后，选择 SteamDB 在线人数历史范围。",
                        input_type="select",
                        default="monthly_peak_1y",
                        options=[
                            {"value": "monthly_peak_1y", "label": "近 12 个月月峰值"},
                            {"value": "daily_precise_30d", "label": "近 30 天每日值"},
                            {"value": "daily_precise_90d", "label": "近 90 天每日值"},
                        ],
                    ),
                ],
                required_fields=["target.name", "target.params.app_id (recommended)"],
                rules=[
                    TargetValidationRule(
                        mode="any",
                        fields=["target.name", "target.params.app_id"],
                        code="missing_steam_target",
                        field="targets[{index}]",
                        message="Steam target needs a game name or app_id.",
                        skip_if_error=False,
                    ),
                    TargetValidationRule(
                        mode="any",
                        fields=["target.params.app_id"],
                        level="warning",
                        code="missing_steam_app_id",
                        field="targets[{index}]",
                        message="Steam app_id is recommended to avoid wrong game matches.",
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
            config_schema={
                "type": "object",
                "properties": {
                    "request_delay": {
                        "type": "number",
                        "minimum": 0,
                        "default": 1,
                        "title": "请求间隔（秒）",
                        "description": "连续请求之间的等待时间；遇到限流时可适当调高。",
                    },
                    "collect_timeout_seconds": {
                        "type": "number",
                        "minimum": 0,
                        "default": 300,
                        "title": "单目标超时（秒）",
                        "description": "单个 Steam 目标允许执行的最长时间。",
                    },
                    "collect_retries": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 2,
                        "title": "失败重试次数",
                        "description": "网络错误或临时失败后自动重试的次数。",
                    },
                },
            },
            output_fields=[
                DagOutputField(key="game_name", label="游戏名"),
                DagOutputField(key="app_id", label="App ID"),
            ],
        ),
        CollectorMetadata(
            collector_id="steam_discussions",
            display_name="Steam Community Discussions",
            description=(
                "Collects Steam Community discussion topics and optional replies "
                "for a game forum, with checkpoint-based resume."
            ),
            capabilities=["steam_community", "forum_threads", "discussion_posts"],
            supports_checkpoint=True,
            recovery_level="L1",
            target_schema=CollectorTargetSchema(
                default_params={
                    "max_pages": 50,
                    "max_topics": 1000,
                    "include_replies": True,
                },
                fields=[
                    CollectorTargetField(
                        key="name",
                        location="name",
                        label="游戏名称",
                        description="用于在任务和结果中识别该 Steam 讨论区。",
                        placeholder="Counter-Strike 2",
                    ),
                    CollectorTargetField(
                        key="app_id",
                        label="Steam App ID",
                        description="Steam 商店链接中的数字应用 ID；与论坛 URL 至少填写一个。",
                        placeholder="730",
                    ),
                    CollectorTargetField(
                        key="forum_url",
                        label="讨论区 URL",
                        description="可直接指定 Steam Community 讨论区完整地址。",
                        input_type="url",
                        placeholder="https://steamcommunity.com/app/730/discussions/",
                    ),
                    CollectorTargetField(
                        key="start_time",
                        label="开始日期",
                        description="可选，仅采集该日期之后的讨论。",
                        input_type="date",
                    ),
                    CollectorTargetField(
                        key="end_time",
                        label="结束日期",
                        description="可选，仅采集该日期之前的讨论。",
                        input_type="date",
                    ),
                    CollectorTargetField(
                        key="max_pages",
                        label="最大论坛页数",
                        description="本次任务允许扫描的论坛分页上限。",
                        input_type="number",
                        default=50,
                        minimum=1,
                        maximum=500,
                    ),
                    CollectorTargetField(
                        key="max_topics",
                        label="最大主题数",
                        description="本次任务最多采集的讨论主题数量。",
                        input_type="number",
                        default=1000,
                        minimum=1,
                        maximum=5000,
                    ),
                    CollectorTargetField(
                        key="include_replies",
                        label="采集回复",
                        description="开启后会继续进入主题页面采集回复内容。",
                        input_type="boolean",
                        default=True,
                    ),
                ],
                required_fields=["target.params.app_id or target.params.forum_url"],
                rules=[
                    TargetValidationRule(
                        mode="any",
                        fields=["target.params.app_id", "target.params.forum_url"],
                        code="missing_discussion_target",
                        field="targets[{index}]",
                        message="Steam discussions need app_id or forum_url.",
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
                DagOutputField(key="app_id", label="App ID"),
                DagOutputField(key="title", label="主题标题"),
                DagOutputField(key="author", label="作者"),
                DagOutputField(key="content", label="内容"),
            ],
        ),
    ),
    pipeline_templates=(
        make_template(
            "steam_basic",
            "Steam 基础采集",
            "Steam -> cleaner -> sqlalchemy",
            "steam",
            collector_config={"request_delay": 0.5},
        ),
        make_template(
            "steam_discussions_basic",
            "Steam Community 讨论采集",
            "steam_discussions -> cleaner -> sqlalchemy",
            "steam_discussions",
        ),
    ),
)
