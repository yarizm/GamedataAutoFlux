# Dynamic Report Templates & Agent Exploration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement custom YAML report templates and an `auto` template mode for Agent data-driven exploration.

**Architecture:** We will replace the hardcoded dictionary of templates with a `TemplateManager` that loads built-in templates and custom YAML templates from `data/templates/`. We will add a `custom_prompt` field to the generator and handle `template="auto"` by synthesizing dynamic prompts based on available data sources. The Excel exporter will naturally handle `auto` by falling back to its dynamic sheet generation.

**Tech Stack:** Python 3.12, FastAPI, PyYAML, Pytest

---

### Task 1: Update ReportTemplate and Create TemplateManager

**Files:**
- Modify: `src/reporting/report_templates.py`
- Modify: `src/core/config.py` (to export `get_data_dir`) - wait, already there.
- Create: `tests/test_template_manager.py`

- [ ] **Step 1: Write failing tests for TemplateManager**

```python
import pytest
from pathlib import Path
import yaml
from src.reporting.report_templates import TemplateManager, ReportTemplate, get_report_template, validate_template_sources

def test_template_manager_custom_yaml(tmp_path):
    # Setup mock templates dir
    template_dir = tmp_path / "templates"
    template_dir.mkdir(parents=True, exist_ok=True)
    yaml_file = template_dir / "custom_test.yaml"
    yaml_content = {
        "name": "Test Custom",
        "description": "Test Desc",
        "required_collectors": ["steam"],
        "prompt_instruction": "Test instruction"
    }
    with open(yaml_file, "w", encoding="utf-8") as f:
        yaml.dump(yaml_content, f)

    manager = TemplateManager(template_dir=template_dir)
    manager.load_all()
    
    # Should contain built-ins + custom
    template = manager.get_template("custom_test")
    assert template is not None
    assert template.name == "Test Custom"
    assert template.is_custom is True
    
    # Check built-in
    assert manager.get_template("general_game") is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_template_manager.py -v`
Expected: FAIL (TemplateManager not imported)

- [ ] **Step 3: Implement TemplateManager and update module functions**

Modify `src/reporting/report_templates.py` to add `TemplateManager` and update functions. Add `import yaml`, `from pathlib import Path`, `from src.core.config import get_data_dir`.

```python
import yaml
from pathlib import Path
from src.core.config import get_data_dir
from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True)
class ReportTemplate:
    id: str
    name: str
    description: str
    required_collectors: tuple[str, ...]
    optional_collectors: tuple[str, ...] = ()
    prompt_instruction: str = ""
    is_custom: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "required_collectors": list(self.required_collectors),
            "optional_collectors": list(self.optional_collectors),
            "prompt_instruction": self.prompt_instruction,
            "is_custom": self.is_custom,
        }

# (Keep COLLECTOR_LABELS and COLLECTOR_ALIASES intact)

# Keep the original REPORT_TEMPLATES dict as BUILTIN_TEMPLATES
BUILTIN_TEMPLATES = {
    # ... existing dictionary content ...
}

class TemplateManager:
    def __init__(self, template_dir: Path | None = None):
        self.template_dir = template_dir or (get_data_dir() / "templates")
        self._templates: dict[str, ReportTemplate] = {}

    def load_all(self):
        self._templates.clear()
        for k, v in BUILTIN_TEMPLATES.items():
            self._templates[k] = v

        if self.template_dir.exists():
            for yaml_file in self.template_dir.glob("*.yaml"):
                try:
                    with open(yaml_file, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f) or {}
                    if not data.get("name"):
                        continue
                    template_id = yaml_file.stem
                    self._templates[template_id] = ReportTemplate(
                        id=template_id,
                        name=data.get("name", template_id),
                        description=data.get("description", ""),
                        required_collectors=tuple(data.get("required_collectors", [])),
                        optional_collectors=tuple(data.get("optional_collectors", [])),
                        prompt_instruction=data.get("prompt_instruction", ""),
                        is_custom=True,
                    )
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).error(f"Failed to load template {yaml_file}: {e}")

    def get_template(self, template_id: str) -> ReportTemplate | None:
        return self._templates.get(template_id)

    def list_templates(self) -> list[ReportTemplate]:
        return list(self._templates.values())

    def save_template(self, template_id: str, data: dict[str, Any]):
        self.template_dir.mkdir(parents=True, exist_ok=True)
        yaml_file = self.template_dir / f"{template_id}.yaml"
        with open(yaml_file, "w", encoding="utf-8") as f:
            yaml.dump(data, f, allow_unicode=True, sort_keys=False)
        self.load_all()

    def delete_template(self, template_id: str) -> bool:
        if template_id in BUILTIN_TEMPLATES:
            return False
        yaml_file = self.template_dir / f"{template_id}.yaml"
        if yaml_file.exists():
            yaml_file.unlink()
            self.load_all()
            return True
        return False

# Global instance
_manager = TemplateManager()
_manager.load_all()

def list_report_templates() -> list[dict[str, Any]]:
    return [t.to_dict() for t in _manager.list_templates()]

def get_report_template(template_id: str) -> ReportTemplate | None:
    return _manager.get_template(template_id)

def is_structured_template(template_id: str) -> bool:
    t = get_report_template(template_id)
    return t is not None and not t.is_custom

def validate_template_sources(
    template_id: str,
    source_counts: dict[str, int],
) -> dict[str, Any]:
    # (Keep original validate logic, but note get_report_template is now dynamic)
    template = get_report_template(template_id)
    normalized_counts = {}
    for collector, count in source_counts.items():
        normalized = normalize_collector(collector)
        normalized_counts[normalized] = normalized_counts.get(normalized, 0) + int(count or 0)

    # If it's "auto" or unknown
    if template is None:
        return {
            "template": template_id,
            "known_template": False,
            "status": "unchecked" if template_id != "auto" else "complete",
            "required_collectors": [],
            "available_collectors": sorted(k for k, v in normalized_counts.items() if v > 0),
            "missing_collectors": [],
            "source_counts": normalized_counts,
        }

    available = sorted(k for k, v in normalized_counts.items() if v > 0)
    missing = [c for c in template.required_collectors if c not in available]
    return {
        "template": template.id,
        "template_name": template.name,
        "known_template": True,
        "status": "complete" if not missing else "partial",
        "required_collectors": list(template.required_collectors),
        "optional_collectors": list(template.optional_collectors),
        "available_collectors": available,
        "missing_collectors": missing,
        "source_counts": normalized_counts,
    }
```
*Note: Make sure to wrap `yaml` module import and add `pyyaml` to requirements if needed, but wait, pyyaml might already be installed since `settings.yaml` is parsed by `config.py` using pyyaml. So `yaml` is available.*

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_template_manager.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/reporting/report_templates.py tests/test_template_manager.py
git commit -m "feat(reporting): add TemplateManager for yaml custom templates"
```

---

### Task 2: Add custom_prompt and auto exploration logic to Generator

**Files:**
- Modify: `src/reporting/generator.py`
- Create: `tests/test_report_generator_prompts.py`

- [ ] **Step 1: Write the failing test**

```python
import pytest
from src.reporting.generator import ReportGenerator

def test_build_template_prompt_auto():
    generator = ReportGenerator()
    validation = {
        "available_collectors": ["steam", "qimai"]
    }
    
    prompt = generator._build_template_prompt("Analyze this game", "auto", validation, custom_prompt="Focus on CN market")
    
    assert "Analyze this game" in prompt
    assert "Available data sources: steam, qimai" in prompt
    assert "Focus on CN market" in prompt
    assert "Dynamically structure" in prompt
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_report_generator_prompts.py -v`
Expected: FAIL (missing `custom_prompt` parameter)

- [ ] **Step 3: Modify Generator to support custom_prompt and auto**

In `src/reporting/generator.py`:
1. Add `custom_prompt: str = ""` to `generate` and `generate_excel` signatures.
2. Pass `custom_prompt` down to `_render_report`.
3. Update `_build_template_prompt` signature to accept `custom_prompt`:

```python
    def _build_template_prompt(
        self,
        prompt: str,
        template: str,
        template_validation: dict[str, Any],
        custom_prompt: str = "",
    ) -> str:
        if template == "auto":
            available = template_validation.get("available_collectors") or []
            avail_text = ", ".join(available) if available else "none"
            instruction = (
                f"{prompt}\\n\\n"\
                f"Report template: Auto Exploration\\n"\
                f"Available data sources: {avail_text}\\n"\
                "Analyze strictly from the provided JSON records. Dynamically structure the report "\
                "based ONLY on the available data sources. Create appropriate chapters for the data found, "\
                "and ignore missing sources without mentioning them.\\n"
            )
            if custom_prompt:
                instruction += f"\\nAdditional constraints/focus: {custom_prompt}\\n"
            return instruction

        template_def = get_report_template(template)
        if template_def is None:
            return f"{prompt}\\n\\n{custom_prompt}" if custom_prompt else prompt

        missing = template_validation.get("missing_collectors") or []
        missing_text = ", ".join(missing) if missing else "none"
        base_prompt = (
            f"{prompt}\\n\\n"\
            f"Report template: {template_def.name}\\n"\
            f"Template requirements: {template_def.prompt_instruction}\\n"\
            f"Missing data sources: {missing_text}\\n"\
            "Analyze strictly from the provided JSON records. If a source is missing, "\
            "state the gap instead of inventing data."
        )
        if custom_prompt:
            base_prompt += f"\\n\\nAdditional constraints/focus: {custom_prompt}"
        return base_prompt
```

Update references to `_build_template_prompt` in `generate` and `generate_excel` to pass `custom_prompt`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_report_generator_prompts.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/reporting/generator.py tests/test_report_generator_prompts.py
git commit -m "feat(reporting): support custom_prompt and auto exploration in generator"
```

---

### Task 3: API Endpoints for Template Management

**Files:**
- Modify: `src/web/routes/reports.py` (Assuming this is where report APIs live, or create `src/web/routes/templates.py` and register it if not. Wait, `src/web/routes/reports.py` is missing from context? No, it's typically in `src/web/routes/`. Let's create an API endpoint in `src/web/routes/api_reports.py` or similar. I'll use `src/web/routes/reports.py` but let subagent check actual filename)

- [ ] **Step 1: Write test for API endpoints**

```python
# Create tests/test_templates_api.py
from fastapi.testclient import TestClient
# (Assume app is imported from src.web.app)
# This step might be skipped or adapted by the agent since exact router name might vary.
# If router is known:
```

- [ ] **Step 2: Implement Endpoints in `src/web/routes/reports.py` (or `api.py`)**

Update the Request model in the file handling `/api/reports/generate`:
```python
from pydantic import BaseModel, Field

class ReportGenerateRequest(BaseModel):
    prompt: str = Field(..., description="Prompt instruction")
    data_source: str = ""
    template: str = "default"
    custom_prompt: str = ""  # ADDED
    provider: str = ""
    use_vector: bool = True
    params: dict = Field(default_factory=dict)
```

Add template management endpoints:
```python
from fastapi import APIRouter, HTTPException
from src.reporting.report_templates import _manager

# ... existing router ...

@router.get("/templates")
async def list_templates():
    return [t.to_dict() for t in _manager.list_templates()]

from typing import Any

class TemplateSaveRequest(BaseModel):
    name: str
    description: str = ""
    required_collectors: list[str] = []
    optional_collectors: list[str] = []
    prompt_instruction: str = ""

@router.post("/templates/{template_id}")
async def save_template(template_id: str, req: TemplateSaveRequest):
    data = req.model_dump()
    _manager.save_template(template_id, data)
    return {"status": "success", "id": template_id}

@router.delete("/templates/{template_id}")
async def delete_template(template_id: str):
    success = _manager.delete_template(template_id)
    if not success:
        raise HTTPException(status_code=400, detail="Cannot delete built-in or missing template")
    return {"status": "deleted"}
```
*(Make sure to apply these to the actual file containing the `reports` APIRouter).*

- [ ] **Step 3: Modify Generator Calls**
Make sure the endpoint that calls `ReportGenerator().generate()` passes `custom_prompt=request.custom_prompt`.

- [ ] **Step 4: Commit**

```bash
git add src/web/routes/ # all changed files
git commit -m "feat(api): add template management CRUD endpoints"
```
