import json

import pytest

from src.agent.workflow_runtime_nodes import (
    create_dynamic_pipeline_node,
    load_task_detail_node,
    precheck_report_node,
    prepare_dynamic_pipeline_node,
)


@pytest.mark.asyncio
async def test_load_task_detail_node_parses_json_payload() -> None:
    async def fake_tool(payload):
        assert payload == {"task_id": "task-001"}
        return json.dumps(
            {
                "status": "ok",
                "data": {"id": "task-001", "name": "Task 001"},
            },
            ensure_ascii=False,
        )

    result = await load_task_detail_node(
        {"workflow_task_id": "task-001"},
        invoke_task_detail_tool=fake_tool,
    )

    assert result == {
        "task_detail": {
            "status": "ok",
            "data": {"id": "task-001", "name": "Task 001"},
        }
    }


@pytest.mark.asyncio
async def test_precheck_report_node_resolves_template_prompt_and_record_keys() -> None:
    async def fake_tool(payload):
        assert payload == {
            "prompt": "请基于任务 Counter-Strike 2 的采集结果生成一份数据分析报告，总结核心指标、趋势变化、用户反馈和潜在风险。",
            "template": "steam_game",
            "record_keys": ["record:steam", "record:gtrends"],
        }
        return json.dumps(
            {
                "success": True,
                "status": "complete",
                "can_generate": True,
            },
            ensure_ascii=False,
        )

    state = {
        "workflow_task_id": "task-002",
        "workflow_template": "",
        "task_detail": {
            "status": "ok",
            "data": {
                "name": "Counter-Strike 2",
                "collector_name": "steam",
            },
        },
        "collection_review": {
            "record_summaries": [
                {"key": "record:steam"},
                {"key": "record:gtrends"},
            ]
        },
    }

    result = await precheck_report_node(
        state,
        invoke_precheck_report_tool=fake_tool,
    )

    assert result == {
        "workflow_template": "steam_game",
        "workflow_prompt": (
            "请基于任务 Counter-Strike 2 的采集结果生成一份数据分析报告，"
            "总结核心指标、趋势变化、用户反馈和潜在风险。"
        ),
        "report_precheck": {
            "success": True,
            "status": "complete",
            "can_generate": True,
        },
    }


@pytest.mark.asyncio
async def test_prepare_dynamic_pipeline_node_derives_default_pipeline_draft() -> None:
    result = await prepare_dynamic_pipeline_node(
        {
            "workflow_url": "https://example.com/game/cs2",
            "workflow_pipeline_name": "",
        }
    )

    assert result["workflow_url"] == "https://example.com/game/cs2"
    assert result["workflow_pipeline_name"].startswith("example_com_")
    assert result["workflow_wait_strategy_type"] == "networkidle"
    assert "document.title" in result["workflow_js_script"]


@pytest.mark.asyncio
async def test_create_dynamic_pipeline_node_parses_tool_result() -> None:
    async def fake_tool(payload):
        assert payload == {
            "pipeline_name": "example_com_page",
            "url": "https://example.com/game/cs2",
            "wait_strategy_type": "networkidle",
            "wait_strategy_selector": None,
            "js_script": "() => ({ title: document.title })",
        }
        return json.dumps(
            {
                "status": "ok",
                "data": {"pipeline_name": "example_com_page"},
            },
            ensure_ascii=False,
        )

    result = await create_dynamic_pipeline_node(
        {
            "workflow_pipeline_name": "example_com_page",
            "workflow_url": "https://example.com/game/cs2",
            "workflow_wait_strategy_type": "networkidle",
            "workflow_wait_strategy_selector": None,
            "workflow_js_script": "() => ({ title: document.title })",
        },
        invoke_create_dynamic_pipeline_tool=fake_tool,
    )

    assert result == {
        "dynamic_pipeline_result": {
            "status": "ok",
            "data": {"pipeline_name": "example_com_page"},
        }
    }
