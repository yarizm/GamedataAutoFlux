"""Service layer — business logic shared between API routes and Agent tools."""

from src.services._utils import (
    build_record_summary,
    compute_record_completeness,
    extract_record_identity,
    first_str,
    max_iso,
    nested_get,
    normalize_key,
    record_group,
    roll_time_params,
    source_label,
)
from src.services.data_management_service import DataManagementService
from src.services.session_registry import SessionRegistryEntry
from src.services.task_service import TaskService
from src.services.game_resolver import GameIdentifierResolver

__all__ = [
    "DataManagementService",
    "SessionRegistryEntry",
    "TaskService",
    "GameIdentifierResolver",
    "build_record_summary",
    "compute_record_completeness",
    "extract_record_identity",
    "first_str",
    "max_iso",
    "nested_get",
    "normalize_key",
    "record_group",
    "roll_time_params",
    "source_label",
]
