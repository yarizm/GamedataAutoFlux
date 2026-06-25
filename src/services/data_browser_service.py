"""Shared data browsing helpers for API routes and future agent/service use."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from typing import Any

from src.storage.base import BaseStorage

RecordSummaryFn = Callable[[Any], Any | None]
SourceMatchFn = Callable[[Any, str], str]
RecordIdentityFn = Callable[[Any], dict[str, str] | None]
RecordGroupFn = Callable[[Any], dict[str, Any]]
NormalizeKeyFn = Callable[[str], str]
RedactTextFn = Callable[[str], str]
MaxIsoFn = Callable[[str | None, str | None], str | None]
FilterRecordsBySourceFn = Callable[[list[Any], str], list[Any]]
MergeAppIdFn = Callable[[str | None, str | None], str]


class DataBrowserService:
    """Encapsulates paginated source-data browsing over a storage backend."""

    def __init__(
        self,
        *,
        record_summary: RecordSummaryFn,
        record_source_match_kind: SourceMatchFn,
        extract_record_identity: RecordIdentityFn,
        record_group: RecordGroupFn,
        normalize_key: NormalizeKeyFn,
        redact_text: RedactTextFn,
        max_iso: MaxIsoFn,
        filter_records_by_data_source: FilterRecordsBySourceFn,
        merge_app_id: MergeAppIdFn,
        source_filter_scan_page_size: int = 1000,
    ) -> None:
        self._record_summary = record_summary
        self._record_source_match_kind = record_source_match_kind
        self._extract_record_identity = extract_record_identity
        self._record_group = record_group
        self._normalize_key = normalize_key
        self._redact_text = redact_text
        self._max_iso = max_iso
        self._filter_records_by_data_source = filter_records_by_data_source
        self._merge_app_id = merge_app_id
        self._source_filter_scan_page_size = max(1, int(source_filter_scan_page_size))

    def list_games(self, records: list[Any]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}

        for record in records:
            identity = self._extract_record_identity(record)
            if not identity:
                continue
            group = self._record_group(record)
            safe_game_name = self._redact_text(identity["game_name"])
            safe_app_id = self._redact_text(identity.get("app_id") or "") or None
            safe_game_key = (
                f"name:{self._normalize_key(safe_game_name)}" if safe_game_name else f"app:{safe_app_id}"
            )
            safe_group_id = self._redact_text(group.get("group_id", ""))
            safe_group_name = self._redact_text(group.get("group_name", ""))
            grouped_key = f"group:{safe_group_id}" if safe_group_id else safe_game_key
            game = grouped.setdefault(
                grouped_key,
                {
                    "game_key": grouped_key,
                    "game_name": safe_group_name or safe_game_name,
                    "app_id": safe_app_id,
                    "total_records": 0,
                    "latest_stored_at": None,
                    "group_id": safe_group_id,
                    "group_name": safe_group_name,
                    "sources": defaultdict(
                        lambda: {"name": "", "collector": "", "count": 0, "latest_stored_at": None}
                    ),
                },
            )
            game["total_records"] += 1
            game["latest_stored_at"] = self._max_iso(
                game["latest_stored_at"], record.stored_at.isoformat()
            )
            if group.get("group_id"):
                game["app_id"] = self._merge_app_id(game.get("app_id"), safe_app_id)
            elif safe_app_id and not game.get("app_id"):
                game["app_id"] = safe_app_id

            source_bucket = game["sources"][identity["data_source"]]
            source_bucket["name"] = self._redact_text(identity["data_source"])
            source_bucket["collector"] = self._redact_text(identity["collector"])
            source_bucket["count"] += 1
            source_bucket["latest_stored_at"] = self._max_iso(
                source_bucket["latest_stored_at"], record.stored_at.isoformat()
            )

        response: list[dict[str, Any]] = []
        for game in grouped.values():
            sources = sorted(
                game["sources"].values(),
                key=lambda item: item.get("latest_stored_at") or "",
                reverse=True,
            )
            response.append({**game, "sources": sources})

        response.sort(key=lambda item: item.get("latest_stored_at") or "", reverse=True)
        return response

    def list_game_overview(self, records: list[Any], *, limit: int) -> list[dict[str, Any]]:
        games: dict[str, list[str]] = {}
        for record in records:
            identity = self._extract_record_identity(record)
            if not identity:
                continue
            name = identity["game_name"]
            source = identity["data_source"]
            if name not in games:
                games[name] = []
            if source not in games[name]:
                games[name].append(source)
        return [{"game": game, "sources": sources} for game, sources in sorted(games.items())[:limit]]

    def search_record_overview(self, records: list[Any], *, query: str, limit: int) -> dict[str, Any]:
        needle = query.strip().lower()
        if not needle:
            return {"items": [], "total": 0}

        matched_records: list[Any] = []
        for record in records:
            identity = self._extract_record_identity(record)
            haystack = " ".join(
                str(value)
                for value in (
                    getattr(record, "key", ""),
                    getattr(record, "source", ""),
                    identity.get("game_name", "") if identity else "",
                    identity.get("app_id", "") if identity else "",
                    identity.get("collector", "") if identity else "",
                    identity.get("data_source", "") if identity else "",
                )
                if value
            ).lower()
            if needle in haystack:
                matched_records.append(record)

        summaries = []
        for record in matched_records[:limit]:
            identity = self._extract_record_identity(record)
            summaries.append(
                {
                    "key": record.key,
                    "source": record.source,
                    "game": identity.get("game_name", "") if identity else "",
                    "app_id": identity.get("app_id", "") if identity else "",
                    "stored_at": str(record.stored_at) if record.stored_at else "",
                }
            )
        return {"items": summaries, "total": len(matched_records)}

    def list_groups(self, records: list[Any]) -> list[dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        for record in records:
            group = self._record_group(record)
            if not group.get("group_id"):
                continue
            safe_group_id = self._redact_text(group["group_id"])
            safe_group_name = self._redact_text(group.get("group_name") or group["group_id"])
            bucket = groups.setdefault(
                safe_group_id,
                {
                    "group_id": safe_group_id,
                    "group_name": safe_group_name,
                    "count": 0,
                    "latest_stored_at": None,
                },
            )
            bucket["count"] += 1
            bucket["latest_stored_at"] = self._max_iso(
                bucket["latest_stored_at"], record.stored_at.isoformat()
            )
        return sorted(
            groups.values(),
            key=lambda item: item.get("latest_stored_at") or "",
            reverse=True,
        )

    def search_records(self, records: list[Any], query: str) -> list[Any]:
        needle = query.strip().lower()
        if not needle:
            return []

        results: list[Any] = []
        for record in records:
            summary = self._record_summary(record)
            if not summary:
                continue
            haystack = " ".join(
                str(value)
                for value in (
                    summary.key,
                    summary.game_name,
                    summary.app_id,
                    summary.data_source,
                    summary.collector,
                    summary.group_id,
                    summary.group_name,
                    summary.task_id,
                    summary.task_name,
                    record.source,
                )
                if value
            ).lower()
            if needle in haystack:
                results.append(summary)
        results.sort(key=lambda item: item.stored_at, reverse=True)
        return results

    def list_game_record_page(
        self,
        records: list[Any],
        *,
        game_key: str,
        source: str | None,
        page: int,
        page_size: int,
        sort_order: str,
    ) -> dict[str, Any]:
        summaries: list[Any] = []
        for record in records:
            summary = self._record_summary(record)
            if not summary or summary.game_key != game_key:
                continue
            if source and not self._filter_records_by_data_source([record], source):
                continue
            summaries.append(summary)

        reverse = sort_order == "desc"
        summaries.sort(key=lambda item: item.stored_at, reverse=reverse)
        total = len(summaries)
        offset = (page - 1) * page_size
        page_items = summaries[offset : offset + page_size]
        return {
            "items": page_items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "has_more": offset + page_size < total,
        }

    async def list_record_page(
        self,
        store: BaseStorage,
        *,
        query_text: str,
        source_filter: str,
        page: int,
        page_size: int,
        sort_order: str,
        filter_kwargs: dict[str, str],
    ) -> dict[str, Any]:
        offset = (page - 1) * page_size
        if source_filter:
            return await self._load_source_filtered_record_page(
                store,
                query_text=query_text,
                source_filter=source_filter,
                page=page,
                page_size=page_size,
                sort_order=sort_order,
                filter_kwargs=filter_kwargs,
            )

        result = await store.query(
            query_text,
            limit=page_size,
            offset=offset,
            order=sort_order,
            **filter_kwargs,
        )
        items = [
            summary
            for record in result.records
            if (summary := self._record_summary(record)) is not None
        ]
        return {
            "items": items,
            "total": result.total,
            "page": page,
            "page_size": page_size,
            "has_more": offset + len(result.records) < result.total,
        }

    async def _load_source_filtered_record_page(
        self,
        store: BaseStorage,
        *,
        query_text: str,
        source_filter: str,
        page: int,
        page_size: int,
        sort_order: str,
        filter_kwargs: dict[str, str],
    ) -> dict[str, Any]:
        """Scan storage pages so source filtering reports exact totals beyond one query page."""
        offset = (page - 1) * page_size
        exact_items: list[Any] = []
        relaxed_items: list[Any] = []
        exact_total = 0
        relaxed_total = 0
        scan_offset = 0

        while True:
            result = await store.query(
                query_text,
                limit=self._source_filter_scan_page_size,
                offset=scan_offset,
                order=sort_order,
                **filter_kwargs,
            )
            records = result.records
            if not records:
                break

            for record in records:
                match_kind = self._record_source_match_kind(record, source_filter)
                if not match_kind:
                    continue
                summary = self._record_summary(record)
                if summary is None:
                    continue
                if match_kind == "exact":
                    if exact_total >= offset and len(exact_items) < page_size:
                        exact_items.append(summary)
                    exact_total += 1
                else:
                    if relaxed_total >= offset and len(relaxed_items) < page_size:
                        relaxed_items.append(summary)
                    relaxed_total += 1

            scan_offset += len(records)
            if scan_offset >= result.total:
                break

        if exact_total:
            items = exact_items
            total = exact_total
        else:
            items = relaxed_items
            total = relaxed_total

        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "has_more": offset + page_size < total,
        }
