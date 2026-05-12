"""Task-related shared Pydantic schemas."""

from pydantic import BaseModel, Field


class TaskPrecheckIssue(BaseModel):
    level: str
    code: str
    field: str
    message: str


class TaskPrecheckResponse(BaseModel):
    status: str
    can_submit: bool
    pipeline_name: str
    collector_name: str = ""
    required_fields: list[str] = Field(default_factory=list)
    issues: list[TaskPrecheckIssue] = Field(default_factory=list)
    credential_status: dict[str, str] = Field(default_factory=dict)
    data_source_status: dict[str, str] = Field(default_factory=dict)
