"""Safety helpers for high-risk API operations."""

from __future__ import annotations

from fastapi import HTTPException


def require_explicit_confirmation(confirm: bool, operation: str) -> None:
    """Require API callers to opt in before destructive or scheduled actions."""
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail=f"Explicit confirmation required for {operation}; pass confirm=true.",
        )
