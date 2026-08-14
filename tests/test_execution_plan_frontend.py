"""Static integration checks for the execution-plan UX contract."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_task_wizard_is_driven_by_pipeline_and_plugin_metadata() -> None:
    tasks = _read("src/web/src/pages/tasks/index.js")
    target_form = _read("src/web/src/core/targetForm.js")
    template = _read("src/web/templates/index.html")

    assert "describePipeline(pipelineName)" in tasks
    assert "renderMetadataTargetForm('task', targetMetadata)" in tasks
    assert "buildTargetsFromMetadata('task', targetMetadata)" in tasks
    assert "task-pipeline-preview" in template
    assert "task-target-guide" in template
    assert "schema.default_params" in target_form
    assert "textarea_lines" in target_form


def test_pipeline_page_explains_and_launches_execution_plan() -> None:
    page = _read("src/web/src/pages/pipelines/index.js")
    template = _read("src/web/templates/pages/pipelines.html")

    assert "pipelines.concept.plan" in template
    assert "pipelines.concept.task" in template
    assert "pipelines.concept.result" in template
    assert "data-run-plan" in page
    assert "descriptor.description" in page
    assert "pipelines.taskInput" in page


def test_dag_inspector_exposes_usage_and_schema_fields() -> None:
    inspector = _read("src/web/src/pages/dag/inspector.js")
    schemas = _read("src/web/src/pages/dag/schemas.js")
    template = _read("src/web/templates/pages/dag.html")

    assert "renderNodeUsage" in inspector
    assert "renderConfigFields" in inspector
    assert "definition?.config_schema" in inspector
    assert "schema.fields" in schemas
    assert "dag-onboarding" in template
