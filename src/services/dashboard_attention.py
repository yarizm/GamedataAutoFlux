"""Server-side Dashboard attention aggregation (failed tasks + health issues)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from src.core.sensitive import redact_sensitive_text
from src.core.task import Task, TaskStatus


def _as_utc_aware(value: datetime | None) -> datetime:
    """Normalize naive/aware datetimes so sort never raises TypeError."""
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _task_sort_key(task: Task) -> datetime:
    return _as_utc_aware(task.completed_at or task.created_at)


def build_failed_task_digests(
    tasks: Iterable[Task],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Build compact digests for recently failed tasks with structured error fields."""
    failed = [t for t in tasks if t.status == TaskStatus.FAILED]
    failed.sort(key=_task_sort_key, reverse=True)
    digests: list[dict[str, Any]] = []
    for task in failed[: max(0, limit)]:
        pres = task.derived_error_presentation()
        digests.append(
            {
                "id": task.id,
                "name": redact_sensitive_text(task.name),
                "status": task.status.value,
                "error": redact_sensitive_text(task.error) if task.error else None,
                "error_code": pres.get("error_code"),
                "error_title": pres.get("error_title"),
                "error_suggestion": pres.get("error_suggestion"),
                "phase": task.phase,
                "current_step": (
                    redact_sensitive_text(task.current_step) if task.current_step else None
                ),
                "created_at": task.created_at.isoformat() if task.created_at else None,
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            }
        )
    return digests


def build_health_attention_items(
    health: dict[str, Any] | None = None,
    diagnostics: dict[str, Any] | None = None,
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """
    Normalize health + diagnostics into attention issues.

    Mirrors the intent of frontend collectHealthAttentionItems, but is the
    server source of truth for Dashboard when present on stats.summary.
    """
    items: list[dict[str, Any]] = []
    seen: set[str] = set()

    def push(
        issue_id: str,
        name: str,
        status: str,
        message: str,
        severity: str,
        code: str | None = None,
    ) -> None:
        key = f"{issue_id}|{status}|{message}|{code or ''}"
        if key in seen:
            return
        seen.add(key)
        items.append(
            {
                "id": issue_id,
                "name": name,
                "status": status,
                "message": message,
                "severity": severity,
                "code": code,
            }
        )

    overall = str(
        (diagnostics or {}).get("status") or (health or {}).get("status") or ""
    ).lower()
    if overall and overall not in {"ok", "healthy", "up"}:
        severity = "warning" if overall in {"warning", "degraded", "warn"} else "error"
        push(
            "overall",
            "整体状态",
            overall,
            f"系统状态为 {overall}",
            severity,
        )

    checks: list[Any] = []
    if isinstance((diagnostics or {}).get("checks"), list):
        checks.extend(diagnostics["checks"])  # type: ignore[index]
    if isinstance((health or {}).get("checks"), list):
        checks.extend(health["checks"])  # type: ignore[index]

    for check in checks:
        if not isinstance(check, dict):
            continue
        st = str(check.get("status") or "").lower()
        if not st or st in {"ok", "pass", "passed", "skipped"}:
            continue
        severity = "warning" if st in {"warning", "warn"} else "error"
        name = str(check.get("name") or check.get("id") or check.get("check_id") or "check")
        message = str(check.get("message") or check.get("error") or st)
        code = check.get("error_code") or check.get("code")
        code_s = str(code).strip() if code else None
        push(str(check.get("id") or name), name, st, message, severity, code_s)

    return items[: max(0, limit)]


def build_dashboard_attention(
    tasks: Iterable[Task],
    *,
    health: dict[str, Any] | None = None,
    diagnostics: dict[str, Any] | None = None,
    failed_limit: int = 5,
    health_limit: int = 5,
) -> dict[str, Any]:
    """Aggregate attention payload for GET /tasks/stats/summary."""
    return {
        "failed_tasks": build_failed_task_digests(tasks, limit=failed_limit),
        "health_issues": build_health_attention_items(
            health,
            diagnostics,
            limit=health_limit,
        ),
    }
