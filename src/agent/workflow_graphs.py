"""Graph definition builders for Agent workflows."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from langgraph.graph import END, StateGraph

from src.agent.workflow_support import _review_record_keys
from src.agent.workflow_types import (
    AgentWorkflowState,
    WorkflowConditionalEdgeDefinition,
    WorkflowGraphDefinition,
    WorkflowNodeDefinition,
    WorkflowRoute,
    WorkflowToolBridgeDefinition,
)


def _build_get_task_detail_args(state: AgentWorkflowState) -> dict[str, Any]:
    return {"task_id": str(state.get("workflow_task_id") or "").strip()}


def _build_review_collection_results_args(state: AgentWorkflowState) -> dict[str, Any]:
    return {
        "task_id": str(state.get("workflow_task_id") or "").strip(),
        "auto_retry": bool(state.get("workflow_auto_retry")),
    }


def _build_precheck_report_args(state: AgentWorkflowState) -> dict[str, Any]:
    return {
        "prompt": str(state.get("workflow_prompt") or "").strip(),
        "template": str(state.get("workflow_template") or "general_game").strip(),
        "record_keys": _review_record_keys(state.get("collection_review")),
    }


def _build_generate_report_args(state: AgentWorkflowState) -> dict[str, Any]:
    return {
        "prompt": str(state.get("workflow_prompt") or "").strip(),
        "template": str(state.get("workflow_template") or "general_game").strip(),
        "record_keys": _review_record_keys(state.get("collection_review")),
    }


def _build_create_dynamic_pipeline_args(state: AgentWorkflowState) -> dict[str, Any]:
    return {
        "pipeline_name": str(state.get("workflow_pipeline_name") or "").strip(),
        "url": str(state.get("workflow_url") or "").strip(),
        "wait_strategy_type": str(
            state.get("workflow_wait_strategy_type") or "networkidle"
        ).strip(),
        "wait_strategy_selector": state.get("workflow_wait_strategy_selector"),
        "js_script": str(state.get("workflow_js_script") or "").strip(),
    }


def _tool_bridge(
    tool_name: str,
    build_args: Callable[[AgentWorkflowState], dict[str, Any]],
    *,
    output_state_key: str | None = None,
    step_id: str | None = None,
    step_label: str | None = None,
) -> WorkflowToolBridgeDefinition:
    return WorkflowToolBridgeDefinition(
        tool_name=tool_name,
        build_args=build_args,
        output_state_key=output_state_key,
        step_id=step_id,
        step_label=step_label,
    )


def _workflow_node(
    name: str,
    handler: Any,
    *,
    bridge: WorkflowToolBridgeDefinition | None = None,
) -> WorkflowNodeDefinition:
    return WorkflowNodeDefinition(name=name, handler=handler, bridge=bridge)


def _task_detail_node(name: str, handler: Any) -> WorkflowNodeDefinition:
    return _workflow_node(
        name,
        handler,
        bridge=_tool_bridge(
            "get_task_detail",
            _build_get_task_detail_args,
            output_state_key="task_detail",
            step_id="load_task",
            step_label="加载任务",
        ),
    )


def _review_collection_results_node(name: str, handler: Any) -> WorkflowNodeDefinition:
    return _workflow_node(
        name,
        handler,
        bridge=_tool_bridge(
            "review_collection_results",
            _build_review_collection_results_args,
            output_state_key="collection_review",
            step_id="review",
            step_label="复查采集",
        ),
    )


def _response_node(name: str, handler: Any) -> WorkflowNodeDefinition:
    return _workflow_node(name, handler)


def _conditional_edge(
    source: str,
    branch: Callable[[AgentWorkflowState], str],
    targets: dict[str, str],
) -> WorkflowConditionalEdgeDefinition:
    return WorkflowConditionalEdgeDefinition(source=source, branch=branch, targets=targets)


def _build_readiness_session_args(state: AgentWorkflowState) -> dict[str, Any]:
    return {
        "collector_id": str(state.get("workflow_collector_id") or "").strip(),
        "scope": str(state.get("workflow_readiness_scope") or "system").strip(),
    }


def build_workflow_graph_definitions(
    *,
    match_report_workflow: Callable[[str], dict[str, Any] | None],
    match_task_review_workflow: Callable[[str], dict[str, Any] | None],
    match_pipeline_workflow: Callable[[str], dict[str, Any] | None],
    match_readiness_workflow: Callable[[str], dict[str, Any] | None] | None = None,
    load_task_detail_handler: Any,
    review_collection_results_handler: Any,
    precheck_report_handler: Any,
    generate_report_handler: Any,
    prepare_dynamic_pipeline_handler: Any,
    create_dynamic_pipeline_handler: Any,
    compose_report_response_handler: Any,
    compose_task_review_response_handler: Any,
    compose_pipeline_response_handler: Any,
    resolve_readiness_target_handler: Any | None = None,
    check_readiness_config_handler: Any | None = None,
    check_readiness_session_handler: Any | None = None,
    compose_readiness_response_handler: Any | None = None,
    report_task_detail_branch: Callable[[AgentWorkflowState], str],
    task_review_detail_branch: Callable[[AgentWorkflowState], str],
    review_branch: Callable[[AgentWorkflowState], str],
    precheck_branch: Callable[[AgentWorkflowState], str],
    pipeline_prepare_branch: Callable[[AgentWorkflowState], str],
) -> tuple[WorkflowGraphDefinition, ...]:
    definitions: list[WorkflowGraphDefinition] = [
        WorkflowGraphDefinition(
            route="report_workflow",
            entry_node="load_task_detail_report",
            resolve=match_report_workflow,
            nodes=(
                _task_detail_node("load_task_detail_report", load_task_detail_handler),
                _review_collection_results_node(
                    "review_collection_results_report",
                    review_collection_results_handler,
                ),
                _workflow_node(
                    "precheck_report",
                    precheck_report_handler,
                    bridge=_tool_bridge(
                        "precheck_report",
                        _build_precheck_report_args,
                        output_state_key="report_precheck",
                        step_id="precheck",
                        step_label="报告预检",
                    ),
                ),
                _workflow_node(
                    "generate_report",
                    generate_report_handler,
                    bridge=_tool_bridge(
                        "generate_report",
                        _build_generate_report_args,
                        output_state_key="generated_report",
                        step_id="generate",
                        step_label="生成报告",
                    ),
                ),
                _response_node("compose_report_response", compose_report_response_handler),
            ),
            conditional_edges=(
                _conditional_edge(
                    source="load_task_detail_report",
                    branch=report_task_detail_branch,
                    targets={
                        "review": "review_collection_results_report",
                        "respond": "compose_report_response",
                    },
                ),
                _conditional_edge(
                    source="review_collection_results_report",
                    branch=review_branch,
                    targets={
                        "precheck": "precheck_report",
                        "respond": "compose_report_response",
                    },
                ),
                _conditional_edge(
                    source="precheck_report",
                    branch=precheck_branch,
                    targets={
                        "generate": "generate_report",
                        "respond": "compose_report_response",
                    },
                ),
            ),
            edges=(
                ("generate_report", "compose_report_response"),
                ("compose_report_response", END),
            ),
        ),
        WorkflowGraphDefinition(
            route="task_review_workflow",
            entry_node="load_task_detail_task_review",
            resolve=match_task_review_workflow,
            nodes=(
                _task_detail_node("load_task_detail_task_review", load_task_detail_handler),
                _review_collection_results_node(
                    "review_collection_results_task",
                    review_collection_results_handler,
                ),
                _response_node(
                    "compose_task_review_response",
                    compose_task_review_response_handler,
                ),
            ),
            conditional_edges=(
                _conditional_edge(
                    source="load_task_detail_task_review",
                    branch=task_review_detail_branch,
                    targets={
                        "review": "review_collection_results_task",
                        "respond": "compose_task_review_response",
                    },
                ),
            ),
            edges=(
                ("review_collection_results_task", "compose_task_review_response"),
                ("compose_task_review_response", END),
            ),
        ),
        WorkflowGraphDefinition(
            route="pipeline_workflow",
            entry_node="prepare_dynamic_pipeline",
            resolve=match_pipeline_workflow,
            nodes=(
                _response_node("prepare_dynamic_pipeline", prepare_dynamic_pipeline_handler),
                _workflow_node(
                    "create_dynamic_pipeline",
                    create_dynamic_pipeline_handler,
                    bridge=_tool_bridge(
                        "create_dynamic_pipeline",
                        _build_create_dynamic_pipeline_args,
                        output_state_key="dynamic_pipeline_result",
                        step_id="create_pipeline",
                        step_label="创建Pipeline",
                    ),
                ),
                _response_node("compose_pipeline_response", compose_pipeline_response_handler),
            ),
            conditional_edges=(
                _conditional_edge(
                    source="prepare_dynamic_pipeline",
                    branch=pipeline_prepare_branch,
                    targets={
                        "create": "create_dynamic_pipeline",
                        "respond": "compose_pipeline_response",
                    },
                ),
            ),
            edges=(
                ("create_dynamic_pipeline", "compose_pipeline_response"),
                ("compose_pipeline_response", END),
            ),
        ),
    ]

    if (
        match_readiness_workflow is not None
        and resolve_readiness_target_handler is not None
        and check_readiness_config_handler is not None
        and check_readiness_session_handler is not None
        and compose_readiness_response_handler is not None
    ):
        definitions.append(
            WorkflowGraphDefinition(
                route="readiness_workflow",
                entry_node="resolve_readiness_target",
                resolve=match_readiness_workflow,
                nodes=(
                    _workflow_node(
                        "resolve_readiness_target",
                        resolve_readiness_target_handler,
                        bridge=_tool_bridge(
                            "resolve_readiness_target",
                            lambda state: {
                                "collector_id": str(state.get("workflow_collector_id") or ""),
                                "scope": str(state.get("workflow_readiness_scope") or "system"),
                            },
                            step_id="resolve_target",
                            step_label="识别目标",
                        ),
                    ),
                    _workflow_node(
                        "check_readiness_config",
                        check_readiness_config_handler,
                        bridge=_tool_bridge(
                            "check_system_readiness",
                            lambda state: {},
                            output_state_key="readiness_config",
                            step_id="check_config",
                            step_label="配置检查",
                        ),
                    ),
                    _workflow_node(
                        "check_readiness_session",
                        check_readiness_session_handler,
                        bridge=_tool_bridge(
                            "check_collector_readiness",
                            _build_readiness_session_args,
                            output_state_key="readiness_session",
                            step_id="check_session",
                            step_label="会话/登录态",
                        ),
                    ),
                    _response_node(
                        "compose_readiness_response",
                        compose_readiness_response_handler,
                    ),
                ),
                edges=(
                    ("resolve_readiness_target", "check_readiness_config"),
                    ("check_readiness_config", "check_readiness_session"),
                    ("check_readiness_session", "compose_readiness_response"),
                    ("compose_readiness_response", END),
                ),
            )
        )

    return tuple(definitions)


def workflow_entry_nodes(
    definitions: tuple[WorkflowGraphDefinition, ...],
) -> dict[WorkflowRoute, str]:
    return {
        "general_agent": "general_agent",
        **{definition.route: definition.entry_node for definition in definitions},
    }


def workflow_node_bridge_map(
    definitions: tuple[WorkflowGraphDefinition, ...],
) -> dict[str, WorkflowToolBridgeDefinition]:
    mapping: dict[str, WorkflowToolBridgeDefinition] = {}
    for definition in definitions:
        for node in definition.nodes:
            if node.bridge is not None:
                mapping[node.name] = node.bridge
    return mapping


def register_workflow_graph(
    graph: StateGraph,
    definition: WorkflowGraphDefinition,
) -> None:
    for node in definition.nodes:
        graph.add_node(node.name, node.handler)
    for edge in definition.conditional_edges:
        graph.add_conditional_edges(edge.source, edge.branch, edge.targets)
    for source, target in definition.edges:
        graph.add_edge(source, target)
