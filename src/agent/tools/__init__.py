"""
Agent 工具包入口
"""

from langchain_core.tools import BaseTool

from .tasks import ListTasksTool, GetTaskDetailTool, CreateTaskTool, CancelTaskTool
from .pipelines import (
    ListPipelineTemplatesTool,
    ListPipelinesTool,
    CreatePipelineTool,
    DeletePipelineTool,
    CreateDynamicPipelineTool,
)
from .cron import ListCronJobsTool, CreateCronJobTool, DeleteCronJobTool
from .data import (
    ListDataGamesTool,
    SearchDataTool,
    ReviewCollectionResultsTool,
    GetDataRecordContentTool,
)
from .reports import GenerateReportTool, GetReportContentTool, ListReportsTool, PrecheckReportTool
from .identifiers import (
    ResolveSteamAppIdTool,
    VerifySteamAppIdTool,
    SearchGameIdentifiersTool,
    VerifyGameIdentifierTool,
)
from .system import (
    CheckCollectorReadinessTool,
    CheckSystemReadinessTool,
    GetAgentStatusTool,
    GetSystemStatsTool,
    LaunchSteamDBBrowserTool,
)
from .semantic_search import SemanticSearchTool

ALL_TOOLS: list[BaseTool] = [
    LaunchSteamDBBrowserTool(),
    ResolveSteamAppIdTool(),
    VerifySteamAppIdTool(),
    ListTasksTool(),
    GetTaskDetailTool(),
    CreateTaskTool(),
    CancelTaskTool(),
    ListPipelineTemplatesTool(),
    ListPipelinesTool(),
    CreatePipelineTool(),
    DeletePipelineTool(),
    CreateDynamicPipelineTool(),
    ListCronJobsTool(),
    CreateCronJobTool(),
    DeleteCronJobTool(),
    ListDataGamesTool(),
    SearchDataTool(),
    PrecheckReportTool(),
    ListReportsTool(),
    GenerateReportTool(),
    GetReportContentTool(),
    GetSystemStatsTool(),
    GetAgentStatusTool(),
    CheckSystemReadinessTool(),
    CheckCollectorReadinessTool(),
    SearchGameIdentifiersTool(),
    VerifyGameIdentifierTool(),
    ReviewCollectionResultsTool(),
    GetDataRecordContentTool(),
    SemanticSearchTool(),
]
