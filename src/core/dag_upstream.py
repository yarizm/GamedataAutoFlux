"""DAG 上游记录 → 下游 collector targets 映射。

支持 collector 串联场景，例如：
  youtube_comments(视频) → youtube_profiles(频道)

节点 config 示例::

    {
      "from_upstream": {
        "auto": true,                 # 自动抽取常见字段（默认）
        "only_success": true,
        "name_from": "channel_name",  # 可选
        "dedupe_by": ["channel_id", "channel_url"],
        # 或显式字段映射（优先于 auto）:
        # "map": {"channel_url": "channel_url", "channel_id": "channel_id"}
      }
    }
"""
from __future__ import annotations

from typing import Any

from src.collectors.base import CollectResult, CollectTarget

# auto 模式会按顺序尝试这些 data 字段，写入 target.params
_AUTO_PARAM_FIELDS = (
    "channel_url",
    "channel_id",
    "handle",
    "video_url",
    "app_id",
    "url",
    "official_url",
)

_AUTO_NAME_FIELDS = (
    "channel_name",
    "channel_url",
    "channel_id",
    "title",
    "game_name",
    "name",
    "video_url",
    "url",
)


def _dig(data: dict[str, Any], path: str) -> Any:
    """支持 a.b.c 点路径取值。"""
    cur: Any = data
    for part in str(path).split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _record_data(item: Any) -> dict[str, Any] | None:
    if isinstance(item, CollectResult):
        if isinstance(item.data, dict):
            return item.data
        return None
    if isinstance(item, dict):
        # 兼容已经是裸 dict 的端口数据
        if "data" in item and isinstance(item["data"], dict):
            return item["data"]
        return item
    return None


def _record_success(item: Any) -> bool:
    if isinstance(item, CollectResult):
        return bool(item.success)
    if isinstance(item, dict) and "success" in item:
        return bool(item["success"])
    return True


def targets_from_upstream_records(
    records: list[Any],
    from_upstream: dict[str, Any] | bool | None,
) -> list[CollectTarget]:
    """把上游 records 映射为 CollectTarget 列表。"""
    if from_upstream is None or from_upstream is False:
        return []
    if from_upstream is True:
        cfg: dict[str, Any] = {"auto": True}
    elif isinstance(from_upstream, dict):
        cfg = from_upstream
    else:
        return []

    only_success = bool(cfg.get("only_success", True))
    field_map = cfg.get("map") if isinstance(cfg.get("map"), dict) else {}
    auto = bool(cfg.get("auto", not field_map))
    name_from = cfg.get("name_from")
    dedupe_by = cfg.get("dedupe_by")
    if not isinstance(dedupe_by, list):
        dedupe_by = list(field_map.keys()) if field_map else ["channel_id", "channel_url", "video_url", "url"]

    targets: list[CollectTarget] = []
    seen: set[str] = set()

    for item in records:
        if only_success and not _record_success(item):
            continue
        data = _record_data(item)
        if not data:
            continue

        params: dict[str, Any] = {}
        if field_map:
            for target_key, source_key in field_map.items():
                val = _dig(data, str(source_key))
                if val is None or val == "":
                    continue
                params[str(target_key)] = val
        elif auto:
            for key in _AUTO_PARAM_FIELDS:
                val = data.get(key)
                if val is not None and val != "":
                    params[key] = val

        if not params:
            continue

        if name_from:
            name_val = _dig(data, str(name_from))
        else:
            name_val = None
            for key in _AUTO_NAME_FIELDS:
                if data.get(key):
                    name_val = data[key]
                    break
        name = str(name_val or next(iter(params.values()), "upstream"))

        dedupe_parts = []
        for key in dedupe_by:
            v = params.get(key) or data.get(key)
            if v:
                dedupe_parts.append(f"{key}={v}")
        dedupe_key = "|".join(dedupe_parts) if dedupe_parts else f"name={name}|{sorted(params.items())}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        targets.append(
            CollectTarget(
                name=name,
                target_type=str(cfg.get("target_type") or "upstream"),
                params=params,
            )
        )

    return targets


def resolve_collector_targets(
    *,
    task_targets: list[CollectTarget],
    upstream_records: list[Any],
    node_config: dict[str, Any],
) -> list[CollectTarget]:
    """决定 collector 使用任务 targets 还是上游映射 targets。

    - 有 ``from_upstream`` 且上游端口有数据 → 只用上游映射结果
    - 有 ``from_upstream`` 但上游为空 → 返回空（不回退任务 targets，避免误采）
    - 无 ``from_upstream`` → 用任务 targets（根 collector）
    """
    cfg = node_config or {}
    from_upstream = cfg.get("from_upstream")
    if from_upstream is None or from_upstream is False:
        return list(task_targets)

    # 显式配置了上游依赖：只吃上游
    return targets_from_upstream_records(upstream_records, from_upstream)
