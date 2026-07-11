from src.agent.workflow_events import (
    ENTRY_NODE_TO_WORKFLOW,
    WORKFLOW_META,
    result_card_event,
    workflow_end_event,
    workflow_start_event,
    workflow_step_event,
)
from src.agent.workflow_graphs import (
    build_workflow_graph_definitions,
    workflow_node_bridge_map,
)


def test_workflow_start_shape():
    ev = workflow_start_event(
        "report_workflow",
        "报告链路",
        [{"id": "load_task", "label": "加载任务"}, {"id": "precheck", "label": "预检"}],
    )
    assert ev["type"] == "workflow_start"
    assert ev["workflow_id"] == "report_workflow"
    assert len(ev["steps"]) == 2


def test_workflow_step_and_end():
    assert workflow_step_event("report_workflow", "load_task", "加载任务", "running")["status"] == "running"
    end = workflow_end_event("report_workflow", "failed", reason="缺少 task_id")
    assert end["type"] == "workflow_end"
    assert end["reason"] == "缺少 task_id"


def test_result_card_shape():
    card = result_card_event(
        "report",
        "报告已生成",
        "摘要",
        actions=[{"id": "open", "label": "打开", "kind": "navigate", "href": "reports"}],
        payload={"task_id": "t1"},
    )
    assert card["type"] == "result_card"
    assert card["card_type"] == "report"
    assert card["payload"]["task_id"] == "t1"


def test_workflow_meta_step_ids_align_with_graph_bridges() -> None:
    """Start-event step ids ⊆ bridge step_ids ∪ logical prepare/respond."""
    definitions = build_workflow_graph_definitions(
        match_report_workflow=lambda _t: None,
        match_task_review_workflow=lambda _t: None,
        match_pipeline_workflow=lambda _t: None,
        match_readiness_workflow=lambda _t: None,
        match_cron_workflow=lambda _t: None,
        match_multisource_workflow=lambda _t: None,
        load_task_detail_handler=lambda s: s,
        review_collection_results_handler=lambda s: s,
        precheck_report_handler=lambda s: s,
        generate_report_handler=lambda s: s,
        prepare_dynamic_pipeline_handler=lambda s: s,
        create_dynamic_pipeline_handler=lambda s: s,
        compose_report_response_handler=lambda s: s,
        compose_task_review_response_handler=lambda s: s,
        compose_pipeline_response_handler=lambda s: s,
        resolve_readiness_target_handler=lambda s: s,
        check_readiness_config_handler=lambda s: s,
        check_readiness_session_handler=lambda s: s,
        compose_readiness_response_handler=lambda s: s,
        resolve_cron_intent_handler=lambda s: s,
        resolve_cron_schedule_handler=lambda s: s,
        apply_cron_action_handler=lambda s: s,
        compose_cron_response_handler=lambda s: s,
        resolve_multisource_intent_handler=lambda s: s,
        build_multisource_draft_handler=lambda s: s,
        apply_multisource_action_handler=lambda s: s,
        compose_multisource_response_handler=lambda s: s,
        report_task_detail_branch=lambda _s: "respond",
        task_review_detail_branch=lambda _s: "respond",
        review_branch=lambda _s: "respond",
        precheck_branch=lambda _s: "respond",
        pipeline_prepare_branch=lambda _s: "respond",
    )
    bridge_map = workflow_node_bridge_map(definitions)
    known_step_ids = {
        bridge.step_id or bridge.tool_name for bridge in bridge_map.values()
    }
    # Logical path-bar steps without tool bridges
    known_step_ids.update({"prepare", "respond"})

    for meta in WORKFLOW_META.values():
        step_ids = {step["id"] for step in meta.steps}
        assert step_ids <= known_step_ids, (meta.workflow_id, step_ids - known_step_ids)
        assert meta.entry_node in ENTRY_NODE_TO_WORKFLOW
        assert ENTRY_NODE_TO_WORKFLOW[meta.entry_node] == meta.workflow_id
