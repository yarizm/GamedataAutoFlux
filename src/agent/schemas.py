"""Agent 工具的 Pydantic 输入模型定义"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ListTasksInput(BaseModel):
    """列出任务输入"""

    status: str | None = Field(
        default=None,
        description="按状态过滤，可选: pending / running / success / failed / cancelled",
    )


class GetTaskDetailInput(BaseModel):
    """获取任务详情输入"""

    task_id: str = Field(..., description="任务 ID")


class CreateTaskInput(BaseModel):
    """创建任务输入"""

    name: str = Field(..., description="任务名称")
    pipeline_name: str = Field(..., description="Pipeline 模板 ID，如 steam_basic / taptap_basic")
    targets: list[dict] = Field(
        default_factory=list,
        description="采集目标列表，每个目标包含 name(游戏名), target_type(game), params(如 app_id)",
    )
    collector_name: str = Field(default="", description="采集器名称，留空从 Pipeline 推断")
    config: dict = Field(default_factory=dict, description="额外配置，如 report.enabled / data_group")


class CancelTaskInput(BaseModel):
    """取消任务输入"""

    task_id: str = Field(..., description="任务 ID")


class CreatePipelineInput(BaseModel):
    """创建 Pipeline 输入"""

    name: str = Field(..., description="Pipeline 名称")
    steps: list[dict] = Field(
        ...,
        description="步骤列表，每步包含 type(collector/processor/storage), name(组件名), config(可选)",
    )


class DeletePipelineInput(BaseModel):
    """删除 Pipeline 输入"""

    name: str = Field(..., description="Pipeline 名称")


class CreateCronJobInput(BaseModel):
    """创建定时任务输入"""

    name: str = Field(..., description="定时任务名称")
    pipeline_name: str = Field(..., description="关联的 Pipeline 名称或模板 ID")
    cron_expr: str = Field(
        ...,
        description="Cron 表达式（5段），如 '0 8 * * *' 表示每天上午 8 点",
    )
    task_template: dict = Field(
        default_factory=dict,
        description="任务模板，可包含 name, targets, config 等",
    )


class DeleteCronJobInput(BaseModel):
    """删除定时任务输入"""

    name: str = Field(..., description="定时任务名称")


class SearchDataInput(BaseModel):
    """搜索数据输入"""

    query: str = Field(..., description="搜索关键词")
    limit: int = Field(default=20, description="返回数量上限")


class ListDataGamesInput(BaseModel):
    """浏览数据游戏列表输入"""

    limit: int = Field(default=50, description="返回数量上限")


class GenerateReportInput(BaseModel):
    """生成报告输入"""

    prompt: str = Field(..., description="报告提示词，描述需要分析的内容")
    data_source: str = Field(default="", description="数据源过滤标签")
    template: str = Field(default="general_game", description="报告模板名称")
    record_keys: list[str] = Field(
        default_factory=list,
        description="指定数据记录 key 列表，留空则按 data_source 自动选取",
    )


class GetReportContentInput(BaseModel):
    """获取报告内容输入"""

    report_id: str = Field(..., description="报告 ID")


class ResolveSteamAppIdInput(BaseModel):
    """按游戏名搜索 Steam App ID 输入"""

    game_name: str = Field(..., description="游戏名称（中文或英文），支持模糊匹配")


class VerifySteamAppIdInput(BaseModel):
    """验证 Steam App ID 是否有效"""

    app_id: int = Field(..., description="要验证的 Steam App ID")


class SetProviderRequest(BaseModel):
    """切换 LLM provider 请求"""

    provider: str = Field(..., description="Provider key: qwen/deepseek/openai/local")


class ChatRequest(BaseModel):
    """Agent 聊天请求"""

    message: str = Field(..., description="用户消息")
    session_id: str = Field(default="default", description="会话 ID")


class ProviderConfigItem(BaseModel):
    """单个 LLM provider 配置"""

    key: str = Field(..., description="Provider key")
    model: str = Field("", description="模型名称")
    base_url: str = Field("", description="API base URL")
    api_key: str = Field("", description="API key（支持 ${ENV_VAR} 占位符）")
    temperature: float = Field(0.3, description="温度参数")
    max_tokens: int = Field(2000, description="最大 token 数")


class UpdateProviderConfigRequest(BaseModel):
    """批量更新 LLM provider 配置请求"""

    provider: str = Field(..., description="默认 provider")
    items: list[ProviderConfigItem] = Field(..., description="所有 provider 配置项")