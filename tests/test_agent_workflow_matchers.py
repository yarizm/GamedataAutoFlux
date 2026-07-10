"""Unit tests for workflow request matchers (keywords, ambiguity, fail-closed)."""

from src.agent.workflow_matchers import (
    _match_pipeline_workflow,
    _match_report_workflow,
    _match_task_review_workflow,
)


def test_report_keywords_expanded() -> None:
    assert _match_report_workflow("给任务 task:abc 出分析报告") is not None


def test_report_requires_task_id_fail_closed() -> None:
    assert _match_report_workflow("出分析报告") is None
    assert _match_report_workflow("给最近完成的任务出报告") is None


def test_review_requires_task_id_fail_closed() -> None:
    assert _match_task_review_workflow("诊断一下失败原因") is None
    assert _match_task_review_workflow("重试上一任务") is None


def test_review_vs_report_ambiguity() -> None:
    # contains both 诊断 and 报告 → report wins if we check report first in resolve order
    # definitions order already report before review in graphs — ensure report matchers fire
    text = "诊断并生成报告 task:abc"
    assert _match_report_workflow(text) is not None
    # both matchers may fire; resolve order prefers report
    assert _match_task_review_workflow(text) is not None


def test_pipeline_bare_url_no_match() -> None:
    assert _match_pipeline_workflow("https://example.com/page") is None


def test_pipeline_url_with_collect_intent() -> None:
    assert _match_pipeline_workflow("采集这个页面 https://example.com/game") is not None


def test_pipeline_url_with_pipeline_keyword() -> None:
    m = _match_pipeline_workflow("请为 https://example.com/game/cs2 创建动态 pipeline")
    assert m is not None
    assert m["route"] == "pipeline_workflow"
    assert m["workflow_url"] == "https://example.com/game/cs2"


def test_auto_retry_only_on_retry_words() -> None:
    m = _match_task_review_workflow("查看任务 task:abc 失败原因")
    assert m and m["workflow_auto_retry"] is False
    m2 = _match_task_review_workflow("重试任务 task:abc")
    assert m2 and m2["workflow_auto_retry"] is True
