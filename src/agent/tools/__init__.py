"""
Agent 工具包入口
"""

from langchain_core.tools import BaseTool

from .cron import CreateCronJobTool, DeleteCronJobTool, ListCronJobsTool
from .data import (
    GetDataRecordContentTool,
    ListDataGamesTool,
    ReviewCollectionResultsTool,
    SearchDataTool,
)
from .identifiers import (
    ResolveSteamAppIdTool,
    SearchGameIdentifiersTool,
    VerifyGameIdentifierTool,
    VerifySteamAppIdTool,
)
from .pipelines import (
    CreateDynamicPipelineTool,
    CreatePipelineTool,
    DeletePipelineTool,
    ListPipelinesTool,
    ListPipelineTemplatesTool,
)
from .reports import GenerateReportTool, GetReportContentTool, ListReportsTool, PrecheckReportTool
from .semantic_search import SemanticSearchTool
from .system import (
    CheckCollectorReadinessTool,
    CheckSystemReadinessTool,
    GetAgentStatusTool,
    GetSystemStatsTool,
    LaunchSteamDBBrowserTool,
)
from .tasks import CancelTaskTool, CreateTaskTool, GetTaskDetailTool, ListTasksTool

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
