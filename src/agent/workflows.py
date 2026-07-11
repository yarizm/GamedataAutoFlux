"""Workflow-oriented LangGraph helpers for structured Agent tasks."""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Literal

from langchain.agents import create_agent
from langchain_core.messages import AIMessage
from langchain_core.tools import BaseTool
from langgraph.graph import END, START, StateGraph

from src.agent.tools.data import ReviewCollectionResultsTool
from src.agent.tools.pipelines import CreateDynamicPipelineTool
from src.agent.tools.reports import GenerateReportTool, PrecheckReportTool
from src.agent.tools.tasks import GetTaskDetailTool
from src.agent.workflow_graphs import (
    build_workflow_graph_definitions,
    register_workflow_graph,
    workflow_entry_nodes,
    workflow_node_bridge_map as build_workflow_node_bridge_map,
)
from src.agent.workflow_matchers import (
    _match_cron_workflow,
    _match_multisource_workflow,
    _match_pipeline_workflow,
    _match_readiness_workflow,
    _match_report_workflow,
    _match_task_review_workflow,
    _workflow_state,
)
from src.agent.workflow_routing import (
    pipeline_prepare_branch,
    precheck_branch,
    report_task_detail_branch,
    resolve_workflow_request,
    review_branch,
    route_branch,
    route_request,
    task_review_detail_branch,
)
from src.agent.workflow_runtime_nodes import (
    apply_cron_action_node,
    apply_multisource_action_node,
    build_multisource_draft_node,
    check_readiness_config_node,
    check_readiness_session_node,
    create_dynamic_pipeline_node,
    generate_report_node,
    load_task_detail_node,
    precheck_report_node,
    prepare_dynamic_pipeline_node,
    resolve_cron_intent_node,
    resolve_cron_schedule_node,
    resolve_multisource_intent_node,
    resolve_readiness_target_node,
    review_collection_results_node,
)
from src.agent.workflow_responses import (
    build_cron_response_with_card,
    build_multisource_response_with_card,
    build_pipeline_response_with_card,
    build_readiness_response_with_card,
    build_report_response_with_card,
    build_task_review_response_with_card,
)
from src.agent.workflow_types import AgentWorkflowState, WorkflowGraphDefinition, WorkflowRoute

_TASK_DETAIL_TOOL = GetTaskDetailTool()
_REVIEW_COLLECTION_RESULTS_TOOL = ReviewCollectionResultsTool()
_PRECHECK_REPORT_TOOL = PrecheckReportTool()
_GENERATE_REPORT_TOOL = GenerateReportTool()
_CREATE_DYNAMIC_PIPELINE_TOOL = CreateDynamicPipelineTool()


@lru_cache(maxsize=1)
def _workflow_graph_definitions() -> tuple[WorkflowGraphDefinition, ...]:
    return build_workflow_graph_definitions(
        match_report_workflow=_match_report_workflow,
        match_task_review_workflow=_match_task_review_workflow,
        match_pipeline_workflow=_match_pipeline_workflow,
        match_readiness_workflow=_match_readiness_workflow,
        match_cron_workflow=_match_cron_workflow,
        match_multisource_workflow=_match_multisource_workflow,
        load_task_detail_handler=_load_task_detail,
        review_collection_results_handler=_review_collection_results,
        precheck_report_handler=_precheck_report,
        generate_report_handler=_generate_report,
        prepare_dynamic_pipeline_handler=_prepare_dynamic_pipeline,
        create_dynamic_pipeline_handler=_create_dynamic_pipeline,
        compose_report_response_handler=_compose_report_response,
        compose_task_review_response_handler=_compose_task_review_response,
        compose_pipeline_response_handler=_compose_pipeline_response,
        resolve_readiness_target_handler=_resolve_readiness_target,
        check_readiness_config_handler=_check_readiness_config,
        check_readiness_session_handler=_check_readiness_session,
        compose_readiness_response_handler=_compose_readiness_response,
        resolve_cron_intent_handler=_resolve_cron_intent,
        resolve_cron_schedule_handler=_resolve_cron_schedule,
        apply_cron_action_handler=_apply_cron_action,
        compose_cron_response_handler=_compose_cron_response,
        resolve_multisource_intent_handler=_resolve_multisource_intent,
        build_multisource_draft_handler=_build_multisource_draft,
        apply_multisource_action_handler=_apply_multisource_action,
        compose_multisource_response_handler=_compose_multisource_response,
        report_task_detail_branch=_report_task_detail_branch,
        task_review_detail_branch=_task_review_detail_branch,
        review_branch=_review_branch,
        precheck_branch=_precheck_branch,
        pipeline_prepare_branch=_pipeline_prepare_branch,
    )


@lru_cache(maxsize=1)
def _workflow_entry_nodes() -> dict[WorkflowRoute, str]:
    return workflow_entry_nodes(_workflow_graph_definitions())


def _resolve_workflow_request(user_text: str) -> dict[str, Any]:
    return resolve_workflow_request(
        user_text,
        _workflow_graph_definitions(),
        fallback_state=_workflow_state("general_agent"),
    )


@lru_cache(maxsize=1)
def workflow_node_bridge_map() -> dict[str, Any]:
    return build_workflow_node_bridge_map(_workflow_graph_definitions())


def _register_workflow_graph(
    graph: StateGraph,
    definition: WorkflowGraphDefinition,
) -> None:
    register_workflow_graph(graph, definition)


def build_langgraph_root_graph(
    *,
    model: Any,
    tools: list[BaseTool],
    system_prompt: str,
    checkpointer: Any,
    debug: bool = False,
    name: str = "GamedataAutoFluxAgent",
) -> Any:
    """Build the root graph with workflow routing plus the general agent path."""
    general_agent = create_agent(
        model=model,
        tools=tools,
        system_prompt=system_prompt or None,
        debug=debug,
        name=name,
    )

    graph = StateGraph(AgentWorkflowState)
    graph.add_node("route_request", _route_request)
    graph.add_node("general_agent", general_agent)
    for definition in _workflow_graph_definitions():
        _register_workflow_graph(graph, definition)

    graph.add_edge(START, "route_request")
    graph.add_conditional_edges(
        "route_request",
        _route_branch,
        _workflow_entry_nodes(),
    )
    graph.add_edge("general_agent", END)

    return graph.compile(
        checkpointer=checkpointer,
        debug=debug,
        name=name,
    )


def _route_request(state: AgentWorkflowState) -> dict[str, Any]:
    return route_request(state, resolve_request=_resolve_workflow_request)


def _route_branch(
    state: AgentWorkflowState,
) -> WorkflowRoute:
    return route_branch(state, entry_nodes=_workflow_entry_nodes())


def _report_task_detail_branch(state: AgentWorkflowState) -> Literal["review", "respond"]:
    return report_task_detail_branch(state)


def _task_review_detail_branch(state: AgentWorkflowState) -> Literal["review", "respond"]:
    return task_review_detail_branch(state)


def _review_branch(state: AgentWorkflowState) -> Literal["precheck", "respond"]:
    return review_branch(state)


def _precheck_branch(state: AgentWorkflowState) -> Literal["generate", "respond"]:
    return precheck_branch(state)


def _pipeline_prepare_branch(state: AgentWorkflowState) -> Literal["create", "respond"]:
    return pipeline_prepare_branch(state)


async def _load_task_detail(state: AgentWorkflowState) -> dict[str, Any]:
    return await load_task_detail_node(
        state,
        invoke_task_detail_tool=_ainvoke_task_detail_tool,
    )


async def _review_collection_results(state: AgentWorkflowState) -> dict[str, Any]:
    return await review_collection_results_node(
        state,
        invoke_review_collection_results_tool=_ainvoke_review_collection_results_tool,
    )


async def _precheck_report(state: AgentWorkflowState) -> dict[str, Any]:
    return await precheck_report_node(
        state,
        invoke_precheck_report_tool=_ainvoke_precheck_report_tool,
    )


async def _generate_report(state: AgentWorkflowState) -> dict[str, Any]:
    return await generate_report_node(
        state,
        invoke_generate_report_tool=_ainvoke_generate_report_tool,
    )


async def _prepare_dynamic_pipeline(state: AgentWorkflowState) -> dict[str, Any]:
    return await prepare_dynamic_pipeline_node(state)


async def _create_dynamic_pipeline(state: AgentWorkflowState) -> dict[str, Any]:
    return await create_dynamic_pipeline_node(
        state,
        invoke_create_dynamic_pipeline_tool=_ainvoke_create_dynamic_pipeline_tool,
    )


async def _ainvoke_task_detail_tool(payload: dict[str, Any]) -> Any:
    return await _TASK_DETAIL_TOOL.ainvoke(payload)


async def _ainvoke_review_collection_results_tool(payload: dict[str, Any]) -> Any:
    return await _REVIEW_COLLECTION_RESULTS_TOOL.ainvoke(payload)


async def _ainvoke_precheck_report_tool(payload: dict[str, Any]) -> Any:
    return await _PRECHECK_REPORT_TOOL.ainvoke(payload)


async def _ainvoke_generate_report_tool(payload: dict[str, Any]) -> Any:
    return await _GENERATE_REPORT_TOOL.ainvoke(payload)


async def _ainvoke_create_dynamic_pipeline_tool(payload: dict[str, Any]) -> Any:
    return await _CREATE_DYNAMIC_PIPELINE_TOOL.ainvoke(payload)


def _compose_report_response(state: AgentWorkflowState) -> dict[str, Any]:
    text, card = build_report_response_with_card(state)
    out: dict[str, Any] = {"messages": [AIMessage(content=text)]}
    if card is not None:
        out["result_card"] = card
    return out


def _compose_task_review_response(state: AgentWorkflowState) -> dict[str, Any]:
    text, card = build_task_review_response_with_card(state)
    out: dict[str, Any] = {"messages": [AIMessage(content=text)]}
    if card is not None:
        out["result_card"] = card
    return out


def _compose_pipeline_response(state: AgentWorkflowState) -> dict[str, Any]:
    text, card = build_pipeline_response_with_card(state)
    out: dict[str, Any] = {"messages": [AIMessage(content=text)]}
    if card is not None:
        out["result_card"] = card
    return out


def _resolve_readiness_target(state: AgentWorkflowState) -> dict[str, Any]:
    return resolve_readiness_target_node(state)


def _check_readiness_config(state: AgentWorkflowState) -> dict[str, Any]:
    return check_readiness_config_node(state)


def _check_readiness_session(state: AgentWorkflowState) -> dict[str, Any]:
    return check_readiness_session_node(state)


def _compose_readiness_response(state: AgentWorkflowState) -> dict[str, Any]:
    text, card = build_readiness_response_with_card(state)
    out: dict[str, Any] = {"messages": [AIMessage(content=text)]}
    if card is not None:
        out["result_card"] = card
    return out


def _resolve_cron_intent(state: AgentWorkflowState) -> dict[str, Any]:
    return resolve_cron_intent_node(state)


def _resolve_cron_schedule(state: AgentWorkflowState) -> dict[str, Any]:
    return resolve_cron_schedule_node(state)


def _apply_cron_action(state: AgentWorkflowState) -> dict[str, Any]:
    return apply_cron_action_node(state)


def _compose_cron_response(state: AgentWorkflowState) -> dict[str, Any]:
    text, card = build_cron_response_with_card(state)
    out: dict[str, Any] = {"messages": [AIMessage(content=text)]}
    if card is not None:
        out["result_card"] = card
    return out


def _resolve_multisource_intent(state: AgentWorkflowState) -> dict[str, Any]:
    return resolve_multisource_intent_node(state)


def _build_multisource_draft(state: AgentWorkflowState) -> dict[str, Any]:
    return build_multisource_draft_node(state)


async def _apply_multisource_action(state: AgentWorkflowState) -> dict[str, Any]:
    return await apply_multisource_action_node(state)


def _compose_multisource_response(state: AgentWorkflowState) -> dict[str, Any]:
    text, card = build_multisource_response_with_card(state)
    out: dict[str, Any] = {"messages": [AIMessage(content=text)]}
    if card is not None:
        out["result_card"] = card
    return out
