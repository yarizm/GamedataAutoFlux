from langchain_core.messages import HumanMessage

from src.agent.workflow_routing import (
    pipeline_prepare_branch,
    precheck_branch,
    report_task_detail_branch,
    resolve_workflow_request,
    review_branch,
    route_branch,
    route_request,
)
from src.agent.workflow_types import WorkflowGraphDefinition


def test_resolve_workflow_request_returns_first_match_and_fallback() -> None:
    definitions = (
        WorkflowGraphDefinition(
            route="report_workflow",
            entry_node="report",
            resolve=lambda text: {"route": "report_workflow", "text": text}
            if "report" in text
            else None,
            nodes=(),
        ),
        WorkflowGraphDefinition(
            route="pipeline_workflow",
            entry_node="pipeline",
            resolve=lambda text: {"route": "pipeline_workflow", "text": text}
            if "pipeline" in text
            else None,
            nodes=(),
        ),
    )

    matched = resolve_workflow_request(
        "create report",
        definitions,
        fallback_state={"route": "general_agent"},
    )
    fallback = resolve_workflow_request(
        "hello",
        definitions,
        fallback_state={"route": "general_agent"},
    )

    assert matched == {"route": "report_workflow", "text": "create report"}
    assert fallback == {"route": "general_agent"}


def test_route_request_uses_last_user_message() -> None:
    resolved_text: list[str] = []

    result = route_request(
        {
            "messages": [
                HumanMessage(content="old"),
                HumanMessage(content="latest"),
            ]
        },
        resolve_request=lambda text: resolved_text.append(text) or {"route": "general_agent"},
    )

    assert resolved_text == ["latest"]
    assert result == {"route": "general_agent"}


def test_route_branch_and_detail_branch_fallbacks() -> None:
    entry_nodes = {
        "general_agent": "general_agent",
        "report_workflow": "report",
        "task_review_workflow": "review",
        "pipeline_workflow": "pipeline",
    }

    assert route_branch({"route": "pipeline_workflow"}, entry_nodes=entry_nodes) == "pipeline_workflow"
    assert route_branch({"route": "unknown"}, entry_nodes=entry_nodes) == "general_agent"
    assert report_task_detail_branch({"task_detail": {"status": "ok"}}) == "review"
    assert report_task_detail_branch({"task_detail": {"status": "error"}}) == "respond"


def test_review_and_precheck_and_pipeline_branches_follow_state() -> None:
    assert review_branch({"collection_review": {"record_summaries": [{"key": "record:1"}]}}) == "precheck"
    assert review_branch({"collection_review": {"record_summaries": []}}) == "respond"

    assert precheck_branch(
        {
            "workflow_action": "generate",
            "report_precheck": {"success": True, "can_generate": True},
        }
    ) == "generate"
    assert precheck_branch(
        {
            "workflow_action": "precheck",
            "report_precheck": {"success": True, "can_generate": True},
        }
    ) == "respond"

    assert pipeline_prepare_branch({"workflow_url": "https://example.com"}) == "create"
    assert pipeline_prepare_branch({"workflow_url": ""}) == "respond"
