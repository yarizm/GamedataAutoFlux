import re
from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient
from openpyxl import load_workbook

import src.web.routes.youtube_export as youtube_export
from src.storage.base import StorageRecord
from src.storage.factory import get_storage
from src.web.app import app

EXPORT_URL_PREFIX = "/api/files/export/"


def _local_client() -> TestClient:
    # 未配置 api_key 时只放行真本机请求：TCP 来源和 Host 头都必须是 loopback。
    return TestClient(app, base_url="http://127.0.0.1", client=("127.0.0.1", 50000))


class _FrozenDatetime(datetime):
    """把导出文件名里的时间戳冻结在同一秒。"""

    @classmethod
    def now(cls, tz=None):
        return cls(2026, 1, 1, 0, 0, 0, tzinfo=tz)


def test_youtube_export_matches_pipeline_record_keys(tmp_path, monkeypatch) -> None:
    task_id = "8a49f1c40413"
    record_key = f"{task_id}:https://www.youtube.com/@doudoureviews/videos:3"
    _save_record(
        StorageRecord(
            key=record_key,
            source="https://www.youtube.com/@doudoureviews/videos",
            data={
                "collector": "youtube_profiles",
                "input_url": "https://www.youtube.com/@doudoureviews/videos",
                "author_name": "Doudou Reviews",
                "channel_id": "UC-example",
                "channel_url": "https://www.youtube.com/channel/UC-example",
                "subscriber_count": "12345",
                "description": "reviews",
                "resolution_method": "handle",
                "resolution_status": "success",
            },
            metadata={
                "collector": "youtube_profiles",
                "task_id": task_id,
                "source_task": {
                    "task_id": task_id,
                    "collector_name": "youtube_profiles",
                },
            },
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )
    _save_record(
        StorageRecord(
            key="other-task:https://www.youtube.com/@other/videos:0",
            source="https://www.youtube.com/@other/videos",
            data={"collector": "youtube_profiles", "input_url": "https://www.youtube.com/@other/videos"},
            metadata={"collector": "youtube_profiles", "task_id": "other-task"},
            stored_at=datetime(2026, 1, 1, 12, 1, 0),
        )
    )

    monkeypatch.setattr("src.web.routes.youtube_export.EXPORT_DIR", tmp_path)

    with _local_client() as client:
        response = client.post(
            "/api/data/export/youtube",
            json={
                "collector": "youtube_profiles",
                "task_ids": [task_id],
                "format": "xlsx",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["record_count"] == 1
    filename = payload["download_url"].rsplit("/", 1)[-1]
    workbook_path = tmp_path / filename
    assert workbook_path.exists()

    workbook = load_workbook(workbook_path)
    sheet = workbook["博主信息"]
    assert sheet.max_row == 2
    assert sheet.cell(row=2, column=1).value == "https://www.youtube.com/@doudoureviews/videos"
    assert sheet.cell(row=2, column=2).value == "Doudou Reviews"


def test_youtube_export_can_fallback_to_task_id_from_record_key(tmp_path, monkeypatch) -> None:
    task_id = "legacy-task"
    _save_record(
        StorageRecord(
            key=f"{task_id}:https://www.youtube.com/watch?v=abc123DEF45:0",
            source="https://www.youtube.com/watch?v=abc123DEF45",
            data={
                "collector": "youtube_comments",
                "video_url": "https://www.youtube.com/watch?v=abc123DEF45",
                "title": "Video",
                "comments": [{"like_count": 7, "text": "hello", "published_at": "2026-01-01"}],
            },
            metadata={"collector": "youtube_comments"},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )

    monkeypatch.setattr("src.web.routes.youtube_export.EXPORT_DIR", tmp_path)

    with _local_client() as client:
        response = client.post(
            "/api/data/export/youtube",
            json={
                "collector": "youtube_comments",
                "task_ids": [task_id],
                "format": "xlsx",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["record_count"] == 1
    workbook_path = tmp_path / Path(payload["download_url"]).name
    workbook = load_workbook(workbook_path)
    assert workbook["视频信息"].cell(row=2, column=1).value == "https://www.youtube.com/watch?v=abc123DEF45"
    assert workbook["评论信息"].cell(row=2, column=3).value == "hello"


def test_youtube_export_then_download_returns_distinct_valid_workbooks(tmp_path, monkeypatch) -> None:
    """POST 导出 → 拿 download_url → GET 下载；同一秒两次导出不能同名互相覆盖。"""
    task_id = "roundtrip-task"
    input_url = "https://www.youtube.com/@roundtrip/videos"
    _save_record(
        StorageRecord(
            key=f"{task_id}:{input_url}:0",
            source=input_url,
            data={
                "collector": "youtube_profiles",
                "input_url": input_url,
                "author_name": "Round Trip",
                "channel_id": "UC-roundtrip",
            },
            metadata={"collector": "youtube_profiles", "task_id": task_id},
            stored_at=datetime(2026, 1, 1, 12, 0, 0),
        )
    )
    export_dir = tmp_path / "exports" / "youtube"  # 不预先创建：导出接口负责 mkdir
    monkeypatch.setattr(youtube_export, "EXPORT_DIR", export_dir)
    monkeypatch.setattr(youtube_export, "datetime", _FrozenDatetime)
    request = {"collector": "youtube_profiles", "task_ids": [task_id], "format": "xlsx"}

    with _local_client() as client:
        exports = [client.post("/api/data/export/youtube", json=request) for _ in range(2)]
        assert [response.status_code for response in exports] == [200, 200]
        urls = [response.json()["download_url"] for response in exports]
        downloads = [client.get(url) for url in urls]

    assert all(response.json()["record_count"] == 1 for response in exports)
    filenames = [url.removeprefix(EXPORT_URL_PREFIX) for url in urls]
    assert all(url.startswith(EXPORT_URL_PREFIX) for url in urls)
    assert filenames[0] != filenames[1]
    for filename in filenames:
        assert re.fullmatch(r"export_youtube_profiles_20260101_000000_[0-9a-f]{8}\.xlsx", filename)
    assert sorted(path.name for path in export_dir.iterdir()) == sorted(filenames)

    for filename, response in zip(filenames, downloads):
        assert response.status_code == 200
        assert response.headers["content-type"] == youtube_export.XLSX_MEDIA_TYPE
        assert filename in response.headers["content-disposition"]
        sheet = load_workbook(BytesIO(response.content))["博主信息"]
        assert sheet.max_row == 2
        assert sheet.cell(row=2, column=1).value == input_url
        assert sheet.cell(row=2, column=2).value == "Round Trip"


def test_youtube_export_download_only_serves_xlsx_directly_in_export_dir(tmp_path, monkeypatch) -> None:
    export_dir = tmp_path / "exports"
    export_dir.mkdir()
    (export_dir / "export_ok.xlsx").write_bytes(b"ok")
    (export_dir / "notes.json").write_text("{}", encoding="utf-8")
    (export_dir / "folder.xlsx").mkdir()
    (export_dir / "nested").mkdir()
    (export_dir / "nested" / "inner.xlsx").write_bytes(b"nested")
    outside = tmp_path / "outside.xlsx"
    outside.write_bytes(b"outside")
    monkeypatch.setattr(youtube_export, "EXPORT_DIR", export_dir)

    rejected = [
        "notes.json",  # 目录内的非 xlsx 文件
        "folder.xlsx",  # xlsx 后缀但是目录
        "missing.xlsx",  # 不存在
        "nested%2Finner.xlsx",  # 不是直接子文件
        "nested%5Cinner.xlsx",
        "..%2Foutside.xlsx",  # 路径穿越到目录外
        "..%5Coutside.xlsx",  # Windows 反斜杠穿越：能匹配路由参数，必须由接口自己挡
        "%2E%2E%5Coutside.xlsx",
        quote(str(outside), safe=""),  # 目录外文件的绝对路径
        "..%2F..%2Fpyproject.toml",
    ]
    with _local_client() as client:
        ok = client.get(f"{EXPORT_URL_PREFIX}export_ok.xlsx")
        statuses = {name: client.get(f"{EXPORT_URL_PREFIX}{name}").status_code for name in rejected}

    assert ok.status_code == 200
    assert ok.content == b"ok"
    assert statuses == {name: 404 for name in rejected}


def test_youtube_export_download_rejects_symlink_escaping_export_dir(tmp_path, monkeypatch) -> None:
    export_dir = tmp_path / "exports"
    export_dir.mkdir()
    outside = tmp_path / "outside.xlsx"
    outside.write_bytes(b"outside")
    link = export_dir / "export_link.xlsx"
    try:
        link.symlink_to(outside)
    except OSError as exc:  # Windows 未开启开发者模式时没有创建符号链接的权限
        pytest.skip(f"cannot create symlink: {exc}")
    monkeypatch.setattr(youtube_export, "EXPORT_DIR", export_dir)

    with _local_client() as client:
        response = client.get(f"{EXPORT_URL_PREFIX}export_link.xlsx")

    assert response.status_code == 404


def _save_record(record: StorageRecord) -> None:
    import asyncio

    async def run() -> None:
        store = get_storage()
        await store.initialize()
        try:
            await store.save(record)
        finally:
            await store.close()

    asyncio.run(run())
