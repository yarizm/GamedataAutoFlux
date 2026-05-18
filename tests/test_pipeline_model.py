"""Tests for Pipeline model and PipelineResult."""

from src.core.pipeline import Pipeline, PipelineResult, PipelineStep, StepType


class TestPipelineConstruction:
    def test_default_name(self):
        p = Pipeline("test")
        assert p.name == "test"
        assert p.steps == []

    def test_add_collector(self):
        p = Pipeline("p")
        p.add_collector("steam", {"delay": 1.0})
        assert len(p.steps) == 1
        assert p.steps[0].step_type == StepType.COLLECTOR
        assert p.steps[0].component_name == "steam"
        assert p.steps[0].config == {"delay": 1.0}

    def test_add_processor(self):
        p = Pipeline("p")
        p.add_processor("cleaner")
        assert p.steps[0].step_type == StepType.PROCESSOR
        assert p.steps[0].component_name == "cleaner"
        assert p.steps[0].config == {}

    def test_add_storage(self):
        p = Pipeline("p")
        p.add_storage("local", {"db_name": "test.db"})
        assert p.steps[0].step_type == StepType.STORAGE
        assert p.steps[0].component_name == "local"

    def test_chaining(self):
        p = Pipeline("p").add_collector("steam").add_processor("cleaner").add_storage("local")
        assert len(p.steps) == 3
        assert [s.step_type for s in p.steps] == [
            StepType.COLLECTOR,
            StepType.PROCESSOR,
            StepType.STORAGE,
        ]

    def test_on_progress(self):
        called = []

        async def cb(tid, prog, msg):
            called.append((tid, prog, msg))

        p = Pipeline("p").on_progress(cb)
        assert p._progress_callback is cb


class TestPipelineStepFilters:
    def test_get_collectors(self, pipeline_basic):
        cs = pipeline_basic._get_collectors()
        assert len(cs) == 1
        assert cs[0].component_name == "steam"

    def test_get_processors(self, pipeline_basic):
        ps = pipeline_basic._get_processors()
        assert len(ps) == 1
        assert ps[0].component_name == "cleaner"

    def test_get_storages(self, pipeline_basic):
        ss = pipeline_basic._get_storages()
        assert len(ss) == 1
        assert ss[0].component_name == "local"


class TestPipelineConfig:
    def test_to_config(self, pipeline_basic):
        cfg = pipeline_basic.to_config()
        assert cfg["name"] == "test_pipeline"
        assert len(cfg["steps"]) == 3
        assert cfg["steps"][0]["type"] == "collector"
        assert cfg["steps"][0]["name"] == "steam"
        assert cfg["steps"][1]["type"] == "processor"
        assert cfg["steps"][2]["type"] == "storage"

    def test_from_config(self):
        cfg = {
            "name": "restored",
            "steps": [
                {"type": "collector", "name": "taptap", "config": {"delay": 2.0}},
                {"type": "storage", "name": "local"},
            ],
        }
        p = Pipeline.from_config(cfg)
        assert p.name == "restored"
        assert len(p.steps) == 2
        assert p.steps[0].component_name == "taptap"
        assert p.steps[0].config == {"delay": 2.0}

    def test_roundtrip(self, pipeline_basic):
        cfg = pipeline_basic.to_config()
        restored = Pipeline.from_config(cfg)
        assert restored.name == pipeline_basic.name
        assert len(restored.steps) == len(pipeline_basic.steps)
        for a, b in zip(restored.steps, pipeline_basic.steps):
            assert a.step_type == b.step_type
            assert a.component_name == b.component_name
            assert a.config == b.config


class TestPipelineResult:
    def test_default_values(self):
        r = PipelineResult(pipeline_name="p", task_id="t1")
        assert r.pipeline_name == "p"
        assert r.task_id == "t1"
        assert r.success is True
        assert r.collect_results == []
        assert r.process_results == []
        assert r.output_records == []
        assert r.storage_count == 0
        assert r.errors == []

    def test_duration_none_before_complete(self):
        r = PipelineResult(pipeline_name="p", task_id="t1")
        assert r.duration_seconds is None

    def test_errors_default_empty(self):
        r = PipelineResult(pipeline_name="p", task_id="t1")
        r.errors.append("something went wrong")
        assert r.errors == ["something went wrong"]


class TestPipelineStep:
    def test_defaults(self):
        s = PipelineStep(step_type=StepType.COLLECTOR, component_name="steam")
        assert s.step_type == StepType.COLLECTOR
        assert s.component_name == "steam"
        assert s.config == {}
        assert s.instance is None

    def test_with_config(self):
        s = PipelineStep(step_type=StepType.STORAGE, component_name="local", config={"db": "x.db"})
        assert s.config == {"db": "x.db"}
