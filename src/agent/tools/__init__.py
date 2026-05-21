"""
Agent 工具包入口
"""
from langchain_core.tools import BaseTool

from .tasks import ListTasksTool, GetTaskDetailTool, CreateTaskTool, CancelTaskTool
from .pipelines import ListPipelineTemplatesTool, ListPipelinesTool, CreatePipelineTool, DeletePipelineTool, CreateDynamicPipelineTool
from .cron import ListCronJobsTool, CreateCronJobTool, DeleteCronJobTool
from .data import ListDataGamesTool, SearchDataTool, ReviewCollectionResultsTool, GetDataRecordContentTool
from .reports import GenerateReportTool, GetReportContentTool
from .identifiers import ResolveSteamAppIdTool, VerifySteamAppIdTool, SearchGameIdentifiersTool, VerifyGameIdentifierTool
from .system import GetSystemStatsTool, LaunchSteamDBBrowserTool

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
    GenerateReportTool(),
    GetReportContentTool(),
    GetSystemStatsTool(),
    SearchGameIdentifiersTool(),
    VerifyGameIdentifierTool(),
    ReviewCollectionResultsTool(),
    GetDataRecordContentTool(),
]
