"""Checkpointer helpers for LangGraph-friendly Agent runtimes."""

from __future__ import annotations

import base64
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from langgraph.checkpoint.memory import InMemorySaver
from loguru import logger

from src.core.config import get as get_config


@dataclass(slots=True)
class AgentCheckpointerBundle:
    """Resolved runtime checkpointer configuration."""

    backend_name: str
    checkpointer: Any | None
    storage_path: str | None = None

    @property
    def enabled(self) -> bool:
        return self.checkpointer is not None


class JsonFileCheckpointSaver(InMemorySaver):
    """Durable LangGraph checkpointer stored as JSON on disk."""

    def __init__(self, file_path: str | Path) -> None:
        self._file_path = Path(file_path)
        self._persist_lock = threading.RLock()
        super().__init__()
        self._load_from_disk()

    def _load_from_disk(self) -> None:
        if not self._file_path.exists():
            return

        try:
            payload = json.loads(self._file_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to load Agent checkpoints from {}: {}", self._file_path, exc)
            return

        with self._persist_lock:
            self.storage.clear()
            self.writes.clear()
            self.blobs.clear()

            for entry in payload.get("storage", []):
                checkpoint_pair = _decode_typed_pair(entry["checkpoint"])
                metadata_pair = _decode_typed_pair(entry["metadata"])
                self.storage[entry["thread_id"]][entry.get("checkpoint_ns", "")][
                    entry["checkpoint_id"]
                ] = (
                    checkpoint_pair,
                    metadata_pair,
                    entry.get("parent_checkpoint_id"),
                )

            for entry in payload.get("writes", []):
                serialized_pair = _decode_typed_pair(entry["serialized"])
                self.writes[
                    (
                        entry["thread_id"],
                        entry.get("checkpoint_ns", ""),
                        entry["checkpoint_id"],
                    )
                ][(entry["task_id"], int(entry["task_index"]))] = (
                    entry["task_id"],
                    entry["channel"],
                    serialized_pair,
                    entry.get("task_path", ""),
                )

            for entry in payload.get("blobs", []):
                self.blobs[
                    (
                        entry["thread_id"],
                        entry.get("checkpoint_ns", ""),
                        entry["channel"],
                        entry["version"],
                    )
                ] = _decode_typed_pair(entry["payload"])

    def _persist_to_disk(self) -> None:
        with self._persist_lock:
            payload = {
                "storage": [],
                "writes": [],
                "blobs": [],
            }

            for thread_id in sorted(self.storage.keys()):
                ns_map = self.storage[thread_id]
                for checkpoint_ns in sorted(ns_map.keys()):
                    checkpoint_map = ns_map[checkpoint_ns]
                    for checkpoint_id in sorted(checkpoint_map.keys()):
                        checkpoint, metadata, parent_checkpoint_id = checkpoint_map[checkpoint_id]
                        payload["storage"].append(
                            {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                                "checkpoint": _encode_typed_pair(checkpoint),
                                "metadata": _encode_typed_pair(metadata),
                                "parent_checkpoint_id": parent_checkpoint_id,
                            }
                        )

            for outer_key in sorted(self.writes.keys()):
                thread_id, checkpoint_ns, checkpoint_id = outer_key
                writes_map = self.writes[outer_key]
                for (task_id, task_index) in sorted(writes_map.keys(), key=lambda item: (item[0], item[1])):
                    _, channel, serialized, task_path = writes_map[(task_id, task_index)]
                    payload["writes"].append(
                        {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint_id,
                            "task_id": task_id,
                            "task_index": task_index,
                            "channel": channel,
                            "serialized": _encode_typed_pair(serialized),
                            "task_path": task_path,
                        }
                    )

            for thread_id, checkpoint_ns, channel, version in sorted(self.blobs.keys()):
                payload["blobs"].append(
                    {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "channel": channel,
                        "version": version,
                        "payload": _encode_typed_pair(self.blobs[(thread_id, checkpoint_ns, channel, version)]),
                    }
                )

            self._file_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._file_path.parent / f"{self._file_path.name}.tmp"
            tmp_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            tmp_path.replace(self._file_path)

    def put(self, config, checkpoint, metadata, new_versions):  # type: ignore[override]
        result = super().put(config, checkpoint, metadata, new_versions)
        try:
            self._persist_to_disk()
        except Exception as exc:
            logger.warning("Failed to persist Agent checkpoints to {}: {}", self._file_path, exc)
        return result

    def put_writes(self, config, writes, task_id, task_path=""):  # type: ignore[override]
        result = super().put_writes(config, writes, task_id, task_path)
        try:
            self._persist_to_disk()
        except Exception as exc:
            logger.warning("Failed to persist Agent checkpoint writes to {}: {}", self._file_path, exc)
        return result

    def delete_thread(self, thread_id: str) -> None:  # type: ignore[override]
        super().delete_thread(thread_id)
        try:
            self._persist_to_disk()
        except Exception as exc:
            logger.warning("Failed to persist Agent checkpoint deletion to {}: {}", self._file_path, exc)


def create_agent_checkpointer() -> AgentCheckpointerBundle:
    """Build the configured Agent checkpointer backend."""
    backend = str(get_config("agent.langgraph_checkpointer.backend", "disabled") or "").strip()
    if not backend or backend == "disabled":
        return AgentCheckpointerBundle(backend_name="disabled", checkpointer=None)
    if backend == "memory":
        try:
            from langgraph.checkpoint.memory import MemorySaver
        except ImportError as exc:
            raise RuntimeError(
                "agent.langgraph_checkpointer.backend=memory 需要 langgraph MemorySaver 依赖"
            ) from exc
        return AgentCheckpointerBundle(backend_name="memory", checkpointer=MemorySaver())
    if backend == "file":
        file_path = str(
            get_config("agent.langgraph_checkpointer.file_path", "data/agent_checkpoints.json")
        )
        return AgentCheckpointerBundle(
            backend_name="file",
            checkpointer=JsonFileCheckpointSaver(file_path),
            storage_path=file_path,
        )
    raise ValueError(
        f"Unsupported agent.langgraph_checkpointer.backend: {backend}"
    )


def _encode_typed_pair(value: tuple[str, bytes]) -> dict[str, str]:
    type_name, payload = value
    return {
        "type": type_name,
        "data": base64.b64encode(payload).decode("ascii"),
    }


def _decode_typed_pair(value: dict[str, Any]) -> tuple[str, bytes]:
    return str(value["type"]), base64.b64decode(str(value["data"]))
