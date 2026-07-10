from langchain_core.messages import HumanMessage

from src.agent.workflows import (
    _resolve_workflow_request,
    _route_branch,
    _route_request,
    _workflow_graph_definitions,
    _workflow_entry_nodes,
    workflow_node_bridge_map,
)


def test_resolve_workflow_request_matches_report_workflow() -> None:
    result = _resolve_workflow_request("请帮我预检 task-success 的报告是否可以开始生成")

    assert result["route"] == "report_workflow"
    assert result["workflow_action"] == "precheck"
    assert result["workflow_task_id"] == "task-success"
    assert result["workflow_prompt"] == "请帮我预检 task-success 的报告是否可以开始生成"
    assert result["workflow_template"] == ""


def test_resolve_workflow_request_matches_task_review_workflow() -> None:
    result = _resolve_workflow_request("请直接帮我重试 task-retry")

    assert result["route"] == "task_review_workflow"
    assert result["workflow_action"] == "retry"
    assert result["workflow_task_id"] == "task-retry"
    assert result["workflow_auto_retry"] is True


def test_resolve_workflow_request_matches_pipeline_workflow() -> None:
    result = _resolve_workflow_request("请为 https://example.com/game/cs2 创建动态 pipeline")

    assert result["route"] == "pipeline_workflow"
    assert result["workflow_action"] == "pipeline"
    assert result["workflow_url"] == "https://example.com/game/cs2"
    assert result["workflow_pipeline_name"].startswith("example_com_")
    assert result["workflow_wait_strategy_type"] == "networkidle"
    assert "document.title" in result["workflow_js_script"]


def test_resolve_workflow_request_falls_back_to_general_agent() -> None:
    result = _resolve_workflow_request("你好，帮我总结一下当前系统状态")

    assert result["route"] == "general_agent"
    assert result["workflow_action"] is None
    assert result["workflow_task_id"] == ""
    assert result["workflow_url"] == ""


def test_route_request_uses_last_user_message() -> None:
    state = {
        "messages": [
            HumanMessage(content="你好"),
            HumanMessage(content="请为 https://example.com/game/cs2 创建动态 pipeline"),
        ]
    }

    result = _route_request(state)

    assert result["route"] == "pipeline_workflow"
    assert result["workflow_url"] == "https://example.com/game/cs2"


def test_workflow_entry_nodes_cover_all_known_routes() -> None:
    entry_nodes = _workflow_entry_nodes()

    assert entry_nodes == {
        "general_agent": "general_agent",
        "report_workflow": "load_task_detail_report",
        "task_review_workflow": "load_task_detail_task_review",
        "pipeline_workflow": "prepare_dynamic_pipeline",
        "readiness_workflow": "resolve_readiness_target",
    }


def test_workflow_graph_definitions_register_expected_nodes_and_edges() -> None:
    definitions = {definition.route: definition for definition in _workflow_graph_definitions()}

    report = definitions["report_workflow"]
    assert [node.name for node in report.nodes] == [
        "load_task_detail_report",
        "review_collection_results_report",
        "precheck_report",
        "generate_report",
        "compose_report_response",
    ]
    assert [(edge.source, edge.targets) for edge in report.conditional_edges] == [
        (
            "load_task_detail_report",
            {"review": "review_collection_results_report", "respond": "compose_report_response"},
        ),
        (
            "review_collection_results_report",
            {"precheck": "precheck_report", "respond": "compose_report_response"},
        ),
        (
            "precheck_report",
            {"generate": "generate_report", "respond": "compose_report_response"},
        ),
    ]
    assert report.edges == (
        ("generate_report", "compose_report_response"),
        ("compose_report_response", "__end__"),
    )
    assert [node.bridge.tool_name if node.bridge else None for node in report.nodes] == [
        "get_task_detail",
        "review_collection_results",
        "precheck_report",
        "generate_report",
        None,
    ]

    task_review = definitions["task_review_workflow"]
    assert [node.name for node in task_review.nodes] == [
        "load_task_detail_task_review",
        "review_collection_results_task",
        "compose_task_review_response",
    ]
    assert [(edge.source, edge.targets) for edge in task_review.conditional_edges] == [
        (
            "load_task_detail_task_review",
            {"review": "review_collection_results_task", "respond": "compose_task_review_response"},
        )
    ]
    assert task_review.edges == (
        ("review_collection_results_task", "compose_task_review_response"),
        ("compose_task_review_response", "__end__"),
    )
    assert [node.bridge.tool_name if node.bridge else None for node in task_review.nodes] == [
        "get_task_detail",
        "review_collection_results",
        None,
    ]

    pipeline = definitions["pipeline_workflow"]
    assert [node.name for node in pipeline.nodes] == [
        "prepare_dynamic_pipeline",
        "create_dynamic_pipeline",
        "compose_pipeline_response",
    ]
    assert [(edge.source, edge.targets) for edge in pipeline.conditional_edges] == [
        (
            "prepare_dynamic_pipeline",
            {"create": "create_dynamic_pipeline", "respond": "compose_pipeline_response"},
        )
    ]
    assert pipeline.edges == (
        ("create_dynamic_pipeline", "compose_pipeline_response"),
        ("compose_pipeline_response", "__end__"),
    )
    assert [node.bridge.tool_name if node.bridge else None for node in pipeline.nodes] == [
        None,
        "create_dynamic_pipeline",
        None,
    ]


def test_workflow_node_bridge_map_matches_graph_definitions() -> None:
    bridge_map = workflow_node_bridge_map()

    assert bridge_map["load_task_detail_report"].tool_name == "get_task_detail"
    assert bridge_map["load_task_detail_task_review"].tool_name == "get_task_detail"
    assert bridge_map["review_collection_results_report"].tool_name == "review_collection_results"
    assert bridge_map["review_collection_results_task"].tool_name == "review_collection_results"
    assert bridge_map["precheck_report"].tool_name == "precheck_report"
    assert bridge_map["generate_report"].tool_name == "generate_report"
    assert bridge_map["create_dynamic_pipeline"].tool_name == "create_dynamic_pipeline"


def test_route_branch_uses_registered_routes() -> None:
    assert _route_branch({"route": "report_workflow"}) == "report_workflow"
    assert _route_branch({"route": "task_review_workflow"}) == "task_review_workflow"
    assert _route_branch({"route": "pipeline_workflow"}) == "pipeline_workflow"
    assert _route_branch({"route": "unknown"}) == "general_agent"
