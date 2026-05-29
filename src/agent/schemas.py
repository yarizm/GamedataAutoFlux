"""Agent 工具的 Pydantic 输入模型定义"""

from __future__ import annotations

import json
from enum import Enum

from pydantic import BaseModel, Field, field_validator


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
    config: dict = Field(
        default_factory=dict, description="额外配置，如 report.enabled / data_group"
    )


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
    confirm: bool = Field(default=False, description="高风险操作确认，必须为 true 才会执行删除")


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
    confirm: bool = Field(default=False, description="高风险操作确认，必须为 true 才会执行删除")


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


# ==================== 游戏标识符自动发现模型 ====================


class IdentifierConfidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class IdentifierCandidate(BaseModel):
    identifier: str = Field(..., description="标识符值，如 '730' 或 'https://...'")
    identifier_type: str = Field(
        ...,
        description="标识符类型: steam_app_id / taptap_app_id / siteurl / official_url / keyword",
    )
    name: str = Field("", description="该平台显示的名称")
    similarity: float | None = Field(None, description="与输入游戏名的相似度 0-1")
    source: str = Field("", description="数据来源: api / playwright / config / cache")


class IdentifierResult(BaseModel):
    platform: str = Field(
        ..., description="平台名: steam / taptap / qimai / monitor / official_site / gtrends"
    )
    identifier: str = Field("", description="解析出的标识符值")
    identifier_type: str = Field("", description="标识符类型")
    game_name: str = Field("", description="该平台解析出的游戏名称")
    confidence: IdentifierConfidence = IdentifierConfidence.LOW
    source: str = Field("", description="数据来源")
    candidates: list[IdentifierCandidate] = Field(default_factory=list)
    url: str = Field("", description="该平台页面的完整 URL")
    status: str = Field("", description="not_found / found / multiple_candidates")
    detail: str = Field("", description="额外说明")


class GameIdentifiers(BaseModel):
    """一个游戏在所有平台的标识符汇总"""

    game_name: str = Field(..., description="输入的游戏名")
    steam: IdentifierResult | None = Field(None)
    taptap: IdentifierResult | None = Field(None)
    qimai: IdentifierResult | None = Field(None)
    monitor: IdentifierResult | None = Field(None)
    official_site: IdentifierResult | None = Field(None)
    gtrends: IdentifierResult | None = Field(None)

    def found_platforms(self) -> list[str]:
        found = []
        for key in ("steam", "taptap", "qimai", "monitor", "official_site", "gtrends"):
            val = getattr(self, key, None)
            if val is not None:
                found.append(key)
        return found

    def high_confidence(self) -> list[str]:
        result = []
        for key in ("steam", "taptap", "qimai", "monitor", "official_site", "gtrends"):
            val = getattr(self, key, None)
            if val is not None and val.confidence == IdentifierConfidence.HIGH:
                result.append(key)
        return result


# ==================== 新工具输入模型 ====================


class SearchGameIdentifiersInput(BaseModel):
    game_name: str = Field(..., description="游戏名称（中文或英文）")
    platforms: list[str] | None = Field(
        default=None,
        description="要搜索的平台列表；为 None 时搜索所有平台",
    )

    @field_validator("platforms", mode="before")
    @classmethod
    def parse_platforms_string(cls, v):
        if isinstance(v, str):
            try:
                # Agent 经常传 "['official_site']" 这种字符串格式，尝试解析
                # 首先处理可能是单引号的 JSON 不规范写法
                cleaned_str = v.replace("'", '"')
                parsed = json.loads(cleaned_str)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass
        return v


class VerifyGameIdentifierInput(BaseModel):
    platform: str = Field(..., description="平台: steam / taptap / qimai / monitor / official_site")
    identifier: str = Field(..., description="要验证的标识符值")
    game_name: str = Field(..., description="预期的游戏名称，用于交叉验证")


class ReviewCollectionResultsInput(BaseModel):
    task_id: str = Field(..., description="要审查的采集任务 ID")
    auto_retry: bool = Field(default=False, description="如果数据不完整，是否自动创建重试任务")


# ==================== 采集结果复查模型 ====================


class CollectionReviewIssue(BaseModel):
    level: str = Field("info", description="error / warning / info")
    category: str = Field(
        "", description="missing_data / wrong_identifier / empty_result / task_failed"
    )
    message: str = Field("", description="问题描述")


class CollectionReviewResult(BaseModel):
    task_id: str = Field("", description="任务 ID")
    task_name: str = Field("", description="任务名称")
    completeness: str = Field("unknown", description="full / partial / empty")
    issues: list[CollectionReviewIssue] = Field(default_factory=list)
    suggestions: list[str] = Field(default_factory=list)
    identifiers_used: dict | None = Field(None)
    record_count: int = Field(0)


class CreateDynamicPipelineInput(BaseModel):
    """创建动态数据采集 Pipeline 的输入模型"""

    pipeline_name: str = Field(
        ...,
        description="动态 Pipeline 的唯一名称（英文/拼音标识，如 'game_x_site'）",
    )
    url: str = Field(
        ...,
        description="要采集的目标网页 URL（如果包含变量，请使用大括号，例如 https://example.com/games/{app_id}，如果没有变量，直接传入完整URL即可）",
    )
    wait_strategy_type: str = Field(
        default="networkidle",
        description="页面加载等待策略，可选: networkidle (网络空闲), selector (等待某个元素加载), domcontentloaded (DOM加载完成)",
    )
    wait_strategy_selector: str | None = Field(
        default=None,
        description="如果 wait_strategy_type 为 selector，指定的 CSS 选择器，例如 '.comments-list'",
    )
    js_script: str = Field(
        ...,
        description="用于提取数据的 JavaScript 脚本。该脚本必须是一个自执行的 JavaScript 表达式或函数，返回值应当是一个包含所需数据的对象或数组。例如: '() => { return { title: document.title, score: document.querySelector(\".score\")?.innerText }; }'",
    )


