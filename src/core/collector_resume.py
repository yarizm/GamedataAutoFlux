"""Shared collector resume cursor helpers (L1)."""

from __future__ import annotations

import copy
import json
from typing import Any

CURSOR_SCHEMA_VERSION = 1
PARTIAL_ITEM_CAP = 500
PARTIAL_BYTES_CAP = 512 * 1024


def build_collector_cursor(
    *,
    collector_id: str,
    target_key: str,
    stage: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": CURSOR_SCHEMA_VERSION,
        "collector_id": str(collector_id or "").strip(),
        "target_key": str(target_key or "").strip(),
        "stage": str(stage or "").strip(),
        "payload": dict(payload or {}),
    }


def cursor_has_deep_payload(cursor: dict[str, Any] | None) -> bool:
    if not isinstance(cursor, dict):
        return False
    payload = cursor.get("payload")
    if not isinstance(payload, dict) or not payload:
        return False
    # deep if any pagination / stage progress key present
    deep_keys = (
        "review_cursor",
        "page_token",
        "partial_reviews",
        "partial_comments",
        "collected_count",
        "scanned_count",
        "completed_stages",
    )
    return any(k in payload for k in deep_keys)


def parse_recovery_cursor(
    recovery: dict[str, Any] | None,
    *,
    collector_id: str,
    target_key: str,
) -> dict[str, Any] | None:
    if not isinstance(recovery, dict):
        return None
    cursor = recovery.get("cursor")
    if not isinstance(cursor, dict):
        return None
    try:
        version = int(cursor.get("schema_version") or 0)
    except (TypeError, ValueError):
        return None
    if version != CURSOR_SCHEMA_VERSION:
        return None
    if str(cursor.get("collector_id") or "").strip() != str(collector_id or "").strip():
        return None
    if str(cursor.get("target_key") or "").strip() != str(target_key or "").strip():
        return None
    return cursor


def cap_partial_list(items: list[Any]) -> tuple[list[Any], bool]:
    if not items:
        return [], False
    truncated = False
    kept = list(items)
    if len(kept) > PARTIAL_ITEM_CAP:
        kept = kept[:PARTIAL_ITEM_CAP]
        truncated = True
    try:
        raw = json.dumps(kept, default=str)
        if len(raw.encode("utf-8")) > PARTIAL_BYTES_CAP:
            # binary search shrink
            lo, hi = 0, len(kept)
            while lo < hi:
                mid = (lo + hi) // 2
                chunk = kept[: mid + 1]
                size = len(json.dumps(chunk, default=str).encode("utf-8"))
                if size <= PARTIAL_BYTES_CAP:
                    lo = mid + 1
                else:
                    hi = mid
            kept = kept[:lo]
            truncated = True
    except (TypeError, ValueError):
        kept = kept[: min(len(kept), 50)]
        truncated = True
    return kept, truncated


def select_preferred_checkpoint(checkpoints: list[Any]) -> Any | None:
    if not checkpoints:
        return None
    for cp in checkpoints:
        cursor = getattr(cp, "cursor", None)
        if isinstance(cursor, dict) and cursor_has_deep_payload(cursor):
            return cp
    for cp in checkpoints:
        state = getattr(cp, "state", None)
        if (
            isinstance(state, dict)
            and isinstance(state.get("target_order"), list)
            and state["target_order"]
        ):
            return cp
    return checkpoints[0]


def _state_has_target_order(state: Any) -> bool:
    return (
        isinstance(state, dict)
        and isinstance(state.get("target_order"), list)
        and bool(state["target_order"])
    )


def _with_checkpoint_state(checkpoint: Any, state: dict[str, Any]) -> Any:
    """Return checkpoint-like object with replaced state; do not mutate original."""
    if hasattr(checkpoint, "model_copy"):
        return checkpoint.model_copy(update={"state": state})
    clone = copy.copy(checkpoint)
    try:
        clone.state = state
    except (AttributeError, TypeError):
        return checkpoint
    return clone


def compose_recovery_checkpoint(checkpoints: list[Any]) -> Any | None:
    """Prefer deep cursor; attach best non-empty target_order state if preferred lacks it.

    Mid-progress emits often carry deep cursor with empty ``state``. Collect-complete
    carries honest multi-target ``state`` but shallow cursor. Recovery must combine both
    so multi-target resume does not re-collect successful targets.

    Expects ``checkpoints`` newest-first (same contract as ``select_preferred_checkpoint``).
    """
    preferred = select_preferred_checkpoint(checkpoints)
    if preferred is None:
        return None

    preferred_state = getattr(preferred, "state", None)
    if _state_has_target_order(preferred_state):
        return preferred

    best_state: dict[str, Any] | None = None
    for cp in checkpoints:
        state = getattr(cp, "state", None)
        if _state_has_target_order(state):
            best_state = dict(state)
            break
    if best_state is None:
        return preferred

    return _with_checkpoint_state(preferred, best_state)


def merge_checkpoint_state(
    *,
    target_order: list[str],
    previous: dict[str, Any] | None,
    collect_results: list[Any],
) -> dict[str, Any]:
    order = [str(n).strip() for n in target_order if str(n or "").strip()]
    success_names: list[str] = []
    failed_names: list[str] = []
    for result in collect_results or []:
        target = getattr(result, "target", None)
        name = str(getattr(target, "name", "") or "").strip()
        if not name:
            continue
        if bool(getattr(result, "success", False)):
            if name not in success_names:
                success_names.append(name)
        else:
            if name not in failed_names:
                failed_names.append(name)

    prev = previous if isinstance(previous, dict) else {}
    prev_success = [
        str(n).strip() for n in (prev.get("successful_targets") or []) if str(n or "").strip()
    ]
    all_success = []
    for name in order:
        if name in success_names or name in prev_success:
            if name not in failed_names and name not in all_success:
                all_success.append(name)

    next_index = 0
    for idx, name in enumerate(order):
        if name in failed_names:
            next_index = idx
            break
        if name not in all_success:
            next_index = idx
            break
        next_index = idx + 1

    return {
        "target_order": order,
        "next_target_index": min(next_index, len(order)),
        "completed_targets": order[: min(next_index, len(order))],
        "successful_targets": [n for n in order if n in all_success],
        "failed_targets": [n for n in order if n in failed_names],
    }
