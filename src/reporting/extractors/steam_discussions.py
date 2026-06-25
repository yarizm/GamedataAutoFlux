"""Steam Community Discussions reporting extractor."""

from __future__ import annotations

from typing import Any

from src.reporting.extractors.common import extract_time, safe_int, truncate


def extract_steam_discussions(data: dict[str, Any], result: Any) -> None:
    game_name = data.get("game_name", "")
    snapshot = data.get("snapshot", {}) if isinstance(data.get("snapshot"), dict) else {}
    discussions = data.get("discussions", {}) if isinstance(data.get("discussions"), dict) else {}
    topics = discussions.get("topics", []) if isinstance(discussions.get("topics"), list) else []

    result.overview.append(
        {
            "游戏名": game_name or snapshot.get("name", "未知"),
            "数据来源": "Steam Community",
            "App ID": snapshot.get("app_id", data.get("app_id", "")),
            "讨论主题数": safe_int(discussions.get("topic_count", len(topics))),
            "帖子总数": safe_int(discussions.get("post_count", snapshot.get("post_count"))),
            "最新讨论时间": snapshot.get("latest_topic_at", ""),
            "采集时间": extract_time(data),
        }
    )

    for topic in topics[:200]:
        if not isinstance(topic, dict):
            continue
        posts = topic.get("posts", []) if isinstance(topic.get("posts"), list) else []
        first_post = posts[0] if posts and isinstance(posts[0], dict) else {}
        result.reviews.append(
            {
                "游戏名": game_name or snapshot.get("name", ""),
                "数据来源": "Steam Community",
                "作者": first_post.get("author", ""),
                "评分": "讨论",
                "评论内容": truncate(first_post.get("content", ""), 500),
                "点赞数": "",
                "日期": topic.get("created_at") or first_post.get("published_at", ""),
                "主题标题": topic.get("title", ""),
                "主题URL": topic.get("url", ""),
                "回复数": max(len(posts) - 1, 0),
            }
        )
        result.community_discussions.append(
            {
                "游戏名": game_name or snapshot.get("name", ""),
                "App ID": snapshot.get("app_id", data.get("app_id", "")),
                "主题标题": topic.get("title", ""),
                "主题URL": topic.get("url", ""),
                "发帖时间": topic.get("created_at") or first_post.get("published_at", ""),
                "作者": first_post.get("author", ""),
                "首帖内容": truncate(first_post.get("content", ""), 800),
                "回复数": max(len(posts) - 1, 0),
                "帖子数": len(posts),
            }
        )
