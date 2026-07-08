"""Shared types for Agent workflow orchestration."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any, Literal, TypedDict

from langgraph.graph.message import add_messages

WorkflowRoute = Literal[
    "general_agent",
    "report_workflow",
    "task_review_workflow",
    "pipeline_workflow",
]
WorkflowAction = Literal["precheck", "generate", "review", "retry", "pipeline"]


class AgentWorkflowState(TypedDict, total=False):
    messages: Annotated[list[Any], add_messages]
    route: WorkflowRoute
    workflow_action: WorkflowAction | None
    workflow_task_id: str
    workflow_template: str
    workflow_prompt: str
    workflow_auto_retry: bool
    workflow_url: str
    workflow_pipeline_name: str
    workflow_wait_strategy_type: str
    workflow_wait_strategy_selector: str | None
    workflow_js_script: str
    task_detail: dict[str, Any] | None
    collection_review: dict[str, Any] | None
    report_precheck: dict[str, Any] | None
    generated_report: dict[str, Any] | None
    dynamic_pipeline_result: dict[str, Any] | None


@dataclass(frozen=True)
class WorkflowConditionalEdgeDefinition:
    source: str
    branch: Callable[[AgentWorkflowState], str]
    targets: dict[str, str]


@dataclass(frozen=True)
class WorkflowToolBridgeDefinition:
    tool_name: str
    build_args: Callable[[AgentWorkflowState], dict[str, Any]]
    output_state_key: str | None = None


@dataclass(frozen=True)
class WorkflowNodeDefinition:
    name: str
    handler: Any
    bridge: WorkflowToolBridgeDefinition | None = None


@dataclass(frozen=True)
class WorkflowGraphDefinition:
    route: WorkflowRoute
    entry_node: str
    resolve: Callable[[str], dict[str, Any] | None]
    nodes: tuple[WorkflowNodeDefinition, ...]
    conditional_edges: tuple[WorkflowConditionalEdgeDefinition, ...] = ()
    edges: tuple[tuple[str, Any], ...] = ()
