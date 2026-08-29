"""Pipeline DAG→legacy 回退语义测试。

验收（修复清单 P0）：
- 默认（fallback 关闭）：DAG 异常直接向上抛出，不静默换引擎重跑；
- 显式开启 `pipeline.legacy_fallback`：允许回退，但结果必须携带
  execution_engine=legacy_fallback 与 fallback_reason；
- 正常路径的 execution_engine 标注正确（dag / legacy）。
"""

from datetime import datetime

import pytest

import src.core.config as config_mod
from src.core.pipeline import Pipeline, PipelineResult
from src.core.task import Task


def _task() -> Task:
    return Task(name="fallback-test", pipeline_name="fb_pipeline")


def _patch_config(monkeypatch, *, use_dag=True, fallback=False):
    values = {
        "pipeline.use_dag_execution": use_dag,
        "pipeline.legacy_fallback": fallback,
    }
    monkeypatch.setattr(
        config_mod, "get", lambda key, default=None: values.get(key, default)
    )


def _make_pipeline() -> Pipeline:
    return Pipeline("fb_pipeline")


async def test_dag_failure_raises_by_default(monkeypatch):
    """默认配置下 DAG 异常必须向上抛出，legacy 不得被执行。"""
    from src.core import pipeline as pipeline_mod

    _patch_config(monkeypatch, use_dag=True, fallback=False)
    legacy_calls = []

    async def _boom_dag(self, task, *, recovery_checkpoint=None):
        raise RuntimeError("dag executor bug")

    async def _spy_legacy(self, task, *, recovery_checkpoint=None):
        legacy_calls.append(1)
        return PipelineResult(pipeline_name=self.name, task_id=task.id)

    monkeypatch.setattr(pipeline_mod.Pipeline, "_execute_via_dag", _boom_dag)
    monkeypatch.setattr(pipeline_mod.Pipeline, "_execute_legacy", _spy_legacy)

    with pytest.raises(RuntimeError, match="dag executor bug"):
        await _make_pipeline().execute(_task())

    assert legacy_calls == []


async def test_explicit_fallback_records_engine_and_reason(monkeypatch):
    """显式开启 fallback 后回退执行，并记录引擎与原因。"""
    from src.core import pipeline as pipeline_mod

    _patch_config(monkeypatch, use_dag=True, fallback=True)
    events = []

    async def _boom_dag(self, task, *, recovery_checkpoint=None):
        raise RuntimeError("dag executor bug")

    async def _fake_legacy(self, task, *, recovery_checkpoint=None):
        result = PipelineResult(pipeline_name=self.name, task_id=task.id, success=True)
        result.execution_engine = "legacy"
        result.completed_at = datetime.now()
        return result

    monkeypatch.setattr(pipeline_mod.Pipeline, "_execute_via_dag", _boom_dag)
    monkeypatch.setattr(pipeline_mod.Pipeline, "_execute_legacy", _fake_legacy)

    pipeline = _make_pipeline().on_event(
        lambda task_id, event_type, level, message, payload: events.append(
            (task_id, event_type, level, message, payload)
        )
    )
    result = await pipeline.execute(_task())

    assert result.success is True
    assert result.execution_engine == "legacy_fallback"
    assert "dag executor bug" in (result.fallback_reason or "")
    assert len(events) == 1
    assert events[0][1] == "pipeline_fallback"
    assert events[0][2] == "warning"
    assert events[0][4]["execution_engine"] == "legacy_fallback"
    assert "dag executor bug" in events[0][4]["fallback_reason"]


async def test_dag_path_engine_is_dag(monkeypatch):
    from src.core import pipeline as pipeline_mod

    _patch_config(monkeypatch, use_dag=True, fallback=True)

    async def _fake_dag(self, task, *, recovery_checkpoint=None):
        result = PipelineResult(pipeline_name=self.name, task_id=task.id, success=True)
        result.completed_at = datetime.now()
        return result

    monkeypatch.setattr(pipeline_mod.Pipeline, "_execute_via_dag", _fake_dag)

    result = await _make_pipeline().execute(_task())
    assert result.execution_engine == "dag"
    assert result.fallback_reason is None


async def test_legacy_direct_path_engine_is_legacy(monkeypatch):
    """use_dag_execution 关闭时走真实 legacy 路径（无步骤早退），引擎标注 legacy。"""
    _patch_config(monkeypatch, use_dag=False, fallback=True)

    events = []
    pipeline = _make_pipeline().on_event(
        lambda task_id, event_type, level, message, payload: events.append(
            (task_id, event_type, level, message, payload)
        )
    )
    result = await pipeline.execute(_task())

    assert result.execution_engine == "legacy"
    assert len(events) == 1
    assert events[0][1] == "pipeline_legacy"
    assert events[0][4]["reason"] == "pipeline.use_dag_execution=false"
