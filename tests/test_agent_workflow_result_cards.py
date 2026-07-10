from src.agent.workflow_responses import (
    build_pipeline_response_with_card,
    build_report_response_with_card,
    build_task_review_response_with_card,
)
from src.agent.workflow_result_cards import (
    build_pipeline_result_card,
    build_report_result_card,
    build_task_review_result_card,
)


def test_report_card_success():
    state = {
        "workflow_task_id": "task:1",
        "task_detail": {"status": "ok"},
        "generated_report": {
            "success": True,
            "title": "T",
            "report_id": "r1",
            "download_url": "/x",
        },
        "workflow_action": "generate",
    }
    card = build_report_result_card(state)
    assert card["type"] == "result_card"
    assert card["card_type"] == "report"
    assert card["payload"].get("report_id") == "r1"
    assert any(a.get("kind") == "navigate" for a in card["actions"])
    assert any(a.get("href") == "reports" for a in card["actions"])


def test_report_card_precheck_blocked():
    state = {
        "workflow_task_id": "task:2",
        "task_detail": {"status": "ok"},
        "report_precheck": {
            "status": "empty",
            "can_generate": False,
            "selected_records": 0,
            "usable_records": 0,
        },
        "workflow_action": "precheck",
    }
    card = build_report_result_card(state)
    assert card["type"] == "result_card"
    assert card["card_type"] == "report"
    assert card["payload"].get("can_generate") is False
    assert any(a.get("href") == "tasks" for a in card["actions"])


def test_task_review_card_with_issues():
    state = {
        "workflow_task_id": "task:3",
        "task_detail": {"status": "ok"},
        "collection_review": {
            "completeness": "partial",
            "record_count": 2,
            "issues": [{"level": "warning", "message": "缺源"}],
            "suggestions": ["补采 steam"],
        },
        "workflow_auto_retry": False,
    }
    card = build_task_review_result_card(state)
    assert card["type"] == "result_card"
    assert card["card_type"] == "task_review"
    assert card["payload"].get("task_id") == "task:3"
    assert card["payload"].get("issues")
    assert any(a.get("href") == "tasks" for a in card["actions"])


def test_pipeline_card_success():
    state = {
        "workflow_url": "https://example.com/game",
        "workflow_pipeline_name": "dyn_example",
        "dynamic_pipeline_result": {
            "status": "ok",
            "summary": "created",
            "data": {"pipeline_name": "dyn_example"},
        },
    }
    card = build_pipeline_result_card(state)
    assert card["type"] == "result_card"
    assert card["card_type"] == "dynamic_pipeline"
    assert card["payload"].get("pipeline_name") == "dyn_example"
    assert any(a.get("href") == "pipelines" for a in card["actions"])


def test_response_with_card_keeps_text():
    state = {
        "workflow_task_id": "task:1",
        "task_detail": {"status": "ok"},
        "generated_report": {
            "success": True,
            "title": "T",
            "report_id": "r1",
            "download_url": "/x",
        },
        "workflow_action": "generate",
    }
    text, card = build_report_response_with_card(state)
    assert "r1" in text
    assert card is not None
    assert card["type"] == "result_card"

    review_text, review_card = build_task_review_response_with_card(
        {
            "workflow_task_id": "task:3",
            "task_detail": {"status": "ok"},
            "collection_review": {"completeness": "full", "record_count": 1},
        }
    )
    assert "task:3" in review_text
    assert review_card is not None

    pipe_text, pipe_card = build_pipeline_response_with_card(
        {
            "workflow_url": "https://example.com",
            "workflow_pipeline_name": "p1",
            "dynamic_pipeline_result": {
                "status": "ok",
                "summary": "ok",
                "data": {"pipeline_name": "p1"},
            },
        }
    )
    assert "https://example.com" in pipe_text
    assert pipe_card is not None
