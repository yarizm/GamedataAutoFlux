from datetime import datetime

import pytest

from src.agent.tools.reports import (
    _filter_records_by_data_source,
    _filter_records_by_keywords,
    _load_candidate_records,
    _record_keyword_score,
)
from src.storage.base import QueryResult, StorageRecord


def test_report_record_filter_matches_metadata_group_name() -> None:
    record = StorageRecord(
        key="record:1",
        source="steam",
        data={"collector": "steam", "app_id": "730"},
        metadata={"group_name": "三角洲行动"},
        stored_at=datetime(2026, 1, 1, 12, 0, 0),
    )

    assert _record_keyword_score(record, ["三角洲"]) > 0
    assert _filter_records_by_keywords([record], ["三角洲"]) == [record]


def test_report_record_filter_prioritizes_exact_game_name() -> None:
    exact = StorageRecord(
        key="record:exact",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
    )
    loose = StorageRecord(
        key="record:loose-counter-strike",
        source="steam",
        data={"collector": "steam", "game_name": "Other Game"},
    )

    matched = _filter_records_by_keywords([loose, exact], ["Counter-Strike 2"])

    assert matched[0].key == "record:exact"


def test_report_record_filter_matches_top_level_task_metadata() -> None:
    record = StorageRecord(
        key="record:task",
        source="steam",
        data={"collector": "steam", "game_name": "Task Game"},
        metadata={"task_id": "task-123", "task_name": "Launch Review"},
    )

    assert _record_keyword_score(record, ["launch"]) > 0
    assert _filter_records_by_keywords([record], ["task-123"]) == [record]


def test_report_record_filter_matches_source_task_pipeline_and_collector() -> None:
    record = StorageRecord(
        key="record:official",
        source="cleaner",
        data={"collector": "official_site", "game_name": "Example Game"},
        metadata={
            "source_task": {
                "pipeline_name": "official_site_basic",
                "collector_name": "official_site",
            }
        },
    )

    assert _filter_records_by_keywords([record], ["official_site_basic"]) == [record]
    assert _filter_records_by_keywords([record], ["official_site"]) == [record]


def test_report_record_filter_matches_tags() -> None:
    record = StorageRecord(
        key="record:tagged",
        source="steam",
        data={"collector": "steam", "game_name": "Tagged Game"},
        tags=["launch-window", "priority"],
    )

    assert _filter_records_by_keywords([record], ["launch-window"]) == [record]


def test_report_record_filter_matches_data_source_label_exactly() -> None:
    steam = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
    )
    discussions = StorageRecord(
        key="record:discussions",
        source="steam_discussions",
        data={"collector": "steam_discussions", "game_name": "Counter-Strike 2"},
    )

    matched = _filter_records_by_data_source([discussions, steam], "Steam")

    assert [record.key for record in matched] == ["record:steam"]


def test_report_record_filter_does_not_relax_short_source_keys() -> None:
    discussions = StorageRecord(
        key="record:discussions",
        source="steam_discussions",
        data={"collector": "steam_discussions", "game_name": "Counter-Strike 2"},
    )

    matched = _filter_records_by_data_source([discussions], "Steam")

    assert matched == []


def test_report_record_filter_relaxes_long_source_labels() -> None:
    discussions = StorageRecord(
        key="record:discussions",
        source="steam_discussions",
        data={"collector": "steam_discussions", "game_name": "Counter-Strike 2"},
    )

    matched = _filter_records_by_data_source([discussions], "Steam Community")

    assert matched == [discussions]


def test_report_record_filter_accepts_source_label_aliases() -> None:
    record = StorageRecord(
        key="record:official",
        source="cleaner",
        data={"collector": "official_site", "game_name": "Example Game"},
        metadata={"source_task": {"collector_name": "official_site"}},
    )

    matched = _filter_records_by_data_source([record], "official website")

    assert matched == [record]


def test_report_record_filter_matches_nested_data_sources() -> None:
    record = StorageRecord(
        key="record:steamdb",
        source="steam",
        data={
            "collector": "steam",
            "game_name": "Example Game",
            "source_meta": {"data_sources": ["steamdb"]},
        },
    )

    matched = _filter_records_by_data_source([record], "steamdb")

    assert matched == [record]


def test_report_record_source_filter_excludes_report_history() -> None:
    report_history = StorageRecord(
        key="report:steam-history",
        source="reporting",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
        metadata={"kind": "report", "data_source": "steam"},
    )
    source_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
    )

    matched = _filter_records_by_data_source([report_history, source_record], "steam")

    assert matched == [source_record]


@pytest.mark.asyncio
async def test_load_candidate_records_falls_back_when_keywords_only_match_report_history() -> None:
    report_history = StorageRecord(
        key="report:steam-history",
        source="reporting",
        data={"title": "Counter-Strike 2 report"},
        metadata={"kind": "report", "data_source": "steam"},
    )
    source_record = StorageRecord(
        key="record:steam",
        source="steam",
        data={"collector": "steam", "game_name": "Counter-Strike 2"},
    )
    store = _FakeCandidateStore(
        {
            "Counter-Strike 2": [report_history],
            "key:": [report_history, source_record],
        }
    )

    records = await _load_candidate_records(store, ["Counter-Strike 2"])

    assert [record.key for record in records] == ["record:steam"]
    assert store.queries == ["Counter-Strike 2", "key:"]


class _FakeCandidateStore:
    def __init__(self, records_by_query: dict[str, list[StorageRecord]]) -> None:
        self.records_by_query = records_by_query
        self.queries: list[str] = []

    async def query(self, query: str, limit: int = 10, **kwargs) -> QueryResult:
        self.queries.append(query)
        records = list(self.records_by_query.get(query, []))[:limit]
        return QueryResult(records=records, total=len(records), query=query)
