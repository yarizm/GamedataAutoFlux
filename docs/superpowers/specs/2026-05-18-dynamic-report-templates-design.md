# Dynamic Report Templates & Agent Exploration Design

## 1. Overview
This document outlines the design for allowing users to upload custom report templates and enabling the Agent to explore and dynamically generate reports based on available data.

## 2. Core Architecture

### 2.1 Template Storage & Management
- **Storage**: User templates will be stored as YAML files in the `data/templates/` directory.
- **Model Update**: 
  - `ReportTemplate` dataclass (in `src/reporting/report_templates.py`) will be updated to include an `is_custom: bool` flag to differentiate between built-in and user-uploaded templates.
- **TemplateManager**:
  - The hardcoded `REPORT_TEMPLATES` dict will be replaced by a `TemplateManager` class.
  - **`load_templates()`**: Merges built-in templates with the YAML files read from `data/templates/`.
  - **`save_template(id, yaml_content)`**: Validates and writes custom templates to disk.
  - **`delete_template(id)`**: Deletes a custom template YAML file.

### 2.2 API & Data Flow
- **Report Generation Request**:
  - Add an optional `custom_prompt: str` field to allow users to inject temporary instructions (e.g., "Focus on PC platform metrics") without creating a whole new template.
- **Template CRUD Endpoints**:
  - Provide REST endpoints to list, upload/create, and delete YAML templates via the `TemplateManager`.

### 2.3 Agent Auto Exploration (`template="auto"`)
- **Dynamic Context Injection**:
  - When a user selects the `auto` template, `ReportGenerator._build_template_prompt` will dynamically synthesize a System Prompt.
  - It will analyze `available_collectors` from the fetched records and instruct the LLM to dynamically structure the report based *only* on the data found (e.g., "We found Steam and Qimai data. Please structure the report appropriately").
- **Combining Prompts**:
  - The final prompt sent to the LLM will combine the base template instruction (or the auto-exploration instruction) with the user's `custom_prompt`.
- **Excel Compatibility**:
  - The `excel_exporter.py` will handle the `auto` template by bypassing strict predefined structured sheets (like "A在运营产品监测").
  - Instead, it will use its dynamic sheet generation logic to create tabs based on whatever arrays (`ExtractedData.trends`, `ExtractedData.reviews`, etc.) were successfully extracted from the available data.

## 3. Data Flow Example (Auto Mode)
1. User requests a report with `template="auto"` and `custom_prompt="Compare iOS vs PC"`.
2. Generator fetches records and identifies sources: `["steam", "qimai"]`.
3. Generator builds prompt: "You are a data analyst. Available sources: [Steam, Qimai]. Dynamically create report sections for these sources. Additional instructions: Compare iOS vs PC."
4. LLM analyzes data and generates dynamic Markdown.
5. Excel exporter parses the records and writes dynamic tabs (Reviews, Trends, Peaks) without forcing the built-in rigid summary sheets.

## 4. Error Handling
- If a YAML template is malformed, `TemplateManager` will log an error and skip it during `load_templates()`.
- If a custom template requests non-existent data, the generator will append a warning about missing sources, as it currently does for built-in templates.
