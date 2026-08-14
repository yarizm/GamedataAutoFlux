"""YouTube channel and comment collector plugin."""

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
    name="autoflux-plugin-youtube",
    version="0.1.0",
    description="YouTube Data API channel profile and video comment collectors.",
    modules=(
        "autoflux_plugin_youtube.profiles",
        "autoflux_plugin_youtube.comments",
        "autoflux_plugin_youtube.probes",
    ),
    collectors=("youtube_profiles", "youtube_comments"),
    metadata=(
        CollectorMetadata(
            collector_id="youtube_profiles",
            display_name="YouTube Profiles",
            description=(
                "Collects YouTube channel identity, profile metadata, and channel "
                "statistics with the YouTube Data API."
            ),
            capabilities=["youtube_channel_metadata", "youtube_data_api"],
            supports_checkpoint=True,
            recovery_level="L1",
            credential_profiles=["youtube_api_key"],
            credential_requirements=[
                CredentialRequirement(
                    requirement_id="youtube_api_keys",
                    kind="config_list",
                    config_key="youtube.api_keys",
                    status_key="youtube.api_keys",
                    level="error",
                    code="missing_youtube_api_key",
                    field="youtube.api_keys",
                    message="YouTube API keys are missing; configure youtube.api_keys before running YouTube collectors.",
                    suggested_action="Configure youtube.api_keys in settings.yaml / .env.",
                )
            ],
            target_schema=CollectorTargetSchema(
                target_type="youtube_channel",
                fields=[
                    CollectorTargetField(
                        key="channel_url",
                        label="YouTube 频道链接",
                        description="每行填写一个频道 URL、频道 ID 或 @handle。",
                        input_type="textarea_lines",
                        required=True,
                        multiple=True,
                        placeholder="https://www.youtube.com/@OpenAI",
                    )
                ],
                required_fields=[
                    "target.params.channel_url or target.params.channel_id or target.params.handle"
                ],
                rules=[
                    TargetValidationRule(
                        mode="any",
                        fields=[
                            "target.params.channel_url",
                            "target.params.channel_id",
                            "target.params.handle",
                        ],
                        code="missing_youtube_channel_target",
                        field="targets[{index}]",
                        message="YouTube profiles target needs channel_url, channel_id, or handle.",
                        skip_if_error=False,
                    ),
                    TargetValidationRule(
                        mode="all",
                        fields=["target.params.channel_url"],
                        check="url_host",
                        allowed_hosts=["youtube.com", "youtu.be"],
                        optional=True,
                        level="warning",
                        code="invalid_youtube_url_host",
                        field="targets[{index}].params.channel_url",
                        message="channel_url does not look like a YouTube URL.",
                        suggested_action="Use a youtube.com URL.",
                    ),
                ],
            ),
            output_fields=[
                DagOutputField(key="channel_url", label="频道主页 URL"),
                DagOutputField(key="channel_id", label="频道 ID"),
                DagOutputField(key="author_name", label="作者名"),
                DagOutputField(key="subscriber_count", label="粉丝数"),
                DagOutputField(key="description", label="简介"),
            ],
        ),
        CollectorMetadata(
            collector_id="youtube_comments",
            display_name="YouTube Comments",
            description=(
                "Collects YouTube video comments and replies with pagination and "
                "checkpoint resume through the YouTube Data API."
            ),
            capabilities=["youtube_video_comments", "youtube_data_api"],
            supports_checkpoint=True,
            recovery_level="L1",
            credential_profiles=["youtube_api_key"],
            credential_requirements=[
                CredentialRequirement(
                    requirement_id="youtube_api_keys",
                    kind="config_list",
                    config_key="youtube.api_keys",
                    status_key="youtube.api_keys",
                    level="error",
                    code="missing_youtube_api_key",
                    field="youtube.api_keys",
                    message="YouTube API keys are missing; configure youtube.api_keys before running YouTube collectors.",
                    suggested_action="Configure youtube.api_keys in settings.yaml / .env.",
                )
            ],
            target_schema=CollectorTargetSchema(
                target_type="youtube_video",
                fields=[
                    CollectorTargetField(
                        key="video_url",
                        label="YouTube 视频链接",
                        description="每行填写一个 watch、youtu.be 或 Shorts 视频链接。",
                        input_type="textarea_lines",
                        required=True,
                        multiple=True,
                        placeholder="https://www.youtube.com/watch?v=...",
                    )
                ],
                required_fields=["target.params.video_url"],
                rules=[
                    TargetValidationRule(
                        mode="any",
                        fields=["target.params.video_url"],
                        code="missing_youtube_video_url",
                        field="targets[{index}]",
                        message="YouTube comments target needs video_url.",
                        skip_if_error=False,
                    ),
                    TargetValidationRule(
                        mode="all",
                        fields=["target.params.video_url"],
                        check="url_host",
                        allowed_hosts=["youtube.com", "youtu.be"],
                        optional=True,
                        level="warning",
                        code="invalid_youtube_url_host",
                        field="targets[{index}].params.video_url",
                        message="video_url does not look like a YouTube URL.",
                        suggested_action="Use a youtube.com or youtu.be URL.",
                    ),
                ],
            ),
            output_fields=[
                DagOutputField(key="video_url", label="视频 URL"),
                DagOutputField(key="video_id", label="视频 ID"),
                DagOutputField(key="title", label="标题"),
                DagOutputField(key="channel_id", label="频道 ID"),
                DagOutputField(key="channel_url", label="频道主页 URL"),
                DagOutputField(key="channel_name", label="频道名"),
                DagOutputField(key="subscriber_count", label="粉丝数"),
                DagOutputField(key="view_count", label="播放量"),
                DagOutputField(key="comment_count", label="评论数"),
            ],
        ),
    ),
    pipeline_templates=(
        make_template(
            "youtube_profiles_pipeline",
            "YouTube 频道资料采集",
            "youtube_profiles -> sqlalchemy",
            "youtube_profiles",
            clean=False,
        ),
        make_template(
            "youtube_comments_pipeline",
            "YouTube 评论采集",
            "youtube_comments -> sqlalchemy",
            "youtube_comments",
            clean=False,
        ),
    ),
)
