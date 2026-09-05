"""Qimai collector plugin."""

from src.core.collector_metadata import (
    CollectorMetadata,
    CollectorTargetField,
    CollectorTargetSchema,
    CredentialRequirement,
    SessionAccountSpec,
    SessionCheckSpec,
    TargetValidationRule,
)
from src.core.plugin_system import PluginSpec, make_template

plugin = PluginSpec(
    name="autoflux-plugin-qimai",
    version="0.1.0",
    description="Authenticated Qimai App Store metrics collector.",
    modules=(
        "autoflux_plugin_qimai.collector",
        "autoflux_plugin_qimai.probes",
        "autoflux_plugin_qimai.resolvers",
    ),
    collectors=("qimai",),
    metadata=(
        CollectorMetadata(
            collector_id="qimai",
            display_name="Qimai",
            description=(
                "Uses an authenticated Qimai browser session to collect App Store "
                "ranking, rating, and downloadable metrics for an app ID."
            ),
            capabilities=["app_store_rank", "ratings", "download_export", "browser_collection"],
            requires_session=True,
            session_mode="local_profile",
            supported_session_modes=["local_profile", "managed_state"],
            credential_profiles=["playwright_runtime", "local_browser_profile"],
            credential_requirements=[
                CredentialRequirement(
                    requirement_id="playwright",
                    kind="python_module",
                    module="playwright",
                    status_key="playwright",
                    level="warning",
                    code="missing_playwright",
                    field="playwright",
                    message="Playwright is not importable; Qimai collection will fail.",
                    suggested_action="pip install playwright && playwright install chromium",
                )
            ],
            session_accounts=[
                SessionAccountSpec(
                    account_id="local:qimai_profile",
                    account_kind="local_profile",
                    session_modes=["local_profile"],
                    locator_config_key="qimai.user_data_dir",
                    default_locator="data/qimai_profile",
                    locator_label="user_data_dir",
                    readiness_check="session:qimai_profile",
                    worker_capability="session:qimai_profile",
                ),
                SessionAccountSpec(
                    account_id="managed:qimai_storage_state",
                    account_kind="managed_state",
                    session_modes=["managed_state"],
                    locator_config_key="qimai.storage_state_path",
                    default_locator="data/qimai_storage_state.json",
                    locator_label="storage_state_path",
                    readiness_check="session:qimai_storage_state",
                ),
            ],
            session_checks=[
                SessionCheckSpec(
                    check_id="session:qimai_profile",
                    kind="path_directory",
                    session_modes=["local_profile"],
                    config_key="qimai.user_data_dir",
                    default_value="data/qimai_profile",
                    detail_key="profile_dir",
                    required=True,
                    message=(
                        "Browser profile directory is missing; run the login helper before "
                        "collection"
                    ),
                    ok_message="Browser profile directory exists",
                    suggested_action="prepare_local_profile",
                ),
                SessionCheckSpec(
                    check_id="session:qimai_cdp",
                    kind="http_endpoint",
                    session_modes=["local_profile"],
                    config_key="qimai.cdp_port",
                    default_value=9222,
                    endpoint_template="http://127.0.0.1:{value}/json/version",
                    detail_key="cdp_port",
                    required_config_key="qimai.cdp_required",
                    when_all={"qimai.cdp_enabled": True},
                    message="Qimai CDP browser is not reachable",
                    ok_message="Qimai CDP browser is reachable",
                    suggested_action="start_cdp_browser",
                ),
                SessionCheckSpec(
                    check_id="session:qimai_storage_state",
                    kind="path_file",
                    session_modes=["managed_state"],
                    config_key="qimai.storage_state_path",
                    default_value="data/qimai_storage_state.json",
                    detail_key="storage_state_path",
                    required=True,
                    message=(
                        "Storage state file is missing; export a logged-in browser storage "
                        "state before collection"
                    ),
                    ok_message="Storage state file exists",
                    suggested_action="export_storage_state",
                ),
            ],
            target_schema=CollectorTargetSchema(
                fields=[
                    CollectorTargetField(
                        key="name",
                        location="name",
                        label="应用名称",
                        description="用于在任务和结果中识别该应用。",
                        placeholder="Mobile Legends",
                    ),
                    CollectorTargetField(
                        key="app_id",
                        label="App Store App ID",
                        description="App Store 链接中的纯数字应用 ID。",
                        required=True,
                        placeholder="1160056295",
                    ),
                ],
                required_fields=["target.params.app_id"],
                rules=[
                    TargetValidationRule(
                        mode="any",
                        fields=["target.params.app_id"],
                        code="missing_qimai_app_id",
                        field="targets[{index}]",
                        message="Qimai target needs app_id.",
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
            "qimai_basic", "Qimai（七麦）基础采集", "qimai -> cleaner -> sqlalchemy", "qimai"
        ),
    ),
)
