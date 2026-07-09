from fastapi.testclient import TestClient

from src.web.app import app


def test_import_youtube_channel_targets_from_txt() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/import-targets",
            data={
                "collector_name": "youtube_profiles",
                "target_type": "youtube_channel",
            },
            files={
                "file": (
                    "channels.txt",
                    b"# comment\nhttps://www.youtube.com/@example\n/channel/UC123\n",
                    "text/plain",
                )
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["targets"][0]["target_type"] == "youtube_channel"
    assert payload["targets"][0]["params"]["channel_url"] == (
        "https://www.youtube.com/@example"
    )
    assert payload["targets"][1]["params"]["channel_url"] == (
        "https://www.youtube.com/channel/UC123"
    )


def test_import_youtube_video_targets_from_txt() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/import-targets",
            data={
                "collector_name": "youtube_comments",
                "target_type": "youtube_video",
            },
            files={
                "file": (
                    "videos.txt",
                    b"youtu.be/abc123DEF45\n/shorts/xyz987ZYX65\n",
                    "text/plain",
                )
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["targets"][0]["target_type"] == "youtube_video"
    assert payload["targets"][0]["params"]["video_url"] == "https://youtu.be/abc123DEF45"
    assert payload["targets"][1]["params"]["video_url"] == (
        "https://www.youtube.com/shorts/xyz987ZYX65"
    )


def test_import_targets_rejects_non_youtube_collectors() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks/import-targets",
            data={"collector_name": "steam", "target_type": "youtube_video"},
            files={"file": ("targets.txt", b"https://example.com\n", "text/plain")},
        )

    assert response.status_code == 400
