from __future__ import annotations

import html
import re
from typing import Any
from urllib.parse import urljoin, urlparse


DATE_RE = re.compile(
    r"^(?:"
    r"\d{2}/\d{2}/\d{4}|"
    r"\d{4}-\d{2}-\d{2}|"
    r"\d{4}/\d{2}/\d{2}|"
    r"\d{4}年\d{1,2}月\d{1,2}日|"
    r"[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}|"
    r"[A-Z][a-z]{2,8}\s+\d{1,2},\s+\d{4}"
    r")$"
)
COUNT_RE = re.compile(r"^\d+(?:\.\d+)?[KMB]?$", re.IGNORECASE)
REGION_TOKENS = {
    "Global",
    "US",
    "JP",
    "KR",
    "SEA",
    "TW",
    "HK",
    "International",
}
STOP_SECTION_HEADERS = {
    "Related Games",
    "More Games From This Dev",
    "Download TapTap Games worth discovering",
    "Similar Games",
    "What's happening",
    "What鈥檚 happening",
    "Rate this game",
    "\u76f8\u5173\u6e38\u620f",
    "\u66f4\u591a\u6765\u81ea\u8be5\u5382\u5546\u7684\u6e38\u620f",
    "\u53d1\u73b0\u66f4\u591a\u597d\u6e38\u620f",
    "\u731c\u4f60\u559c\u6b22",
}
ABOUT_HEADINGS = {
    "About the Game",
    "\u5173\u4e8e\u8fd9\u6b3e\u6e38\u620f",
    "\u6e38\u620f\u4ecb\u7ecd",
}
REVIEWS_HEADINGS = {
    "Ratings & Reviews",
    "\u8bc4\u5206\u4e0e\u8bc4\u4ef7",
    "\u8bc4\u5206\u53ca\u8bc4\u4ef7",
    "\u8bc4\u4ef7",
}
UPDATES_HEADINGS = {
    "Announcements",
    "What's new",
    "Whats new",
    "What鈥檚 new",
    "\u516c\u544a",
    "\u66f4\u65b0",
    "\u66f4\u65b0\u5185\u5bb9",
    "\u66f4\u65b0\u8bb0\u5f55",
    "\u66f4\u65b0\u65e5\u5fd7",
}
ABOUT_LABELS = {
    "Provider": "provider",
    "Developer": "developer",
    "Publisher": "publisher",
    "Followers": "followers",
    "Downloads": "downloads",
    "Release date": "release_date",
    "Last Updated on": "last_updated_at",
    "Current Version": "current_version",
    "Size": "size",
    "Languages": "languages",
    "System Requirements": "requirements",
    "Content Rating": "content_rating",
    "In-app Purchases": "in_app_purchases",
    "Network Connection": "network_connection",
    "Platform": "platforms_label",
    "\u5382\u5546": "provider",
    "\u4f9b\u5e94\u5546": "provider",
    "\u5f00\u53d1\u5546": "developer",
    "\u53d1\u884c\u5546": "publisher",
    "\u5173\u6ce8": "followers",
    "\u4e0b\u8f7d": "downloads",
    "\u4e0a\u7ebf\u65f6\u95f4": "release_date",
    "\u4e0a\u7ebf\u65e5\u671f": "release_date",
    "\u66f4\u65b0\u65f6\u95f4": "last_updated_at",
    "\u5f53\u524d\u7248\u672c": "current_version",
    "\u6700\u65b0\u7248\u672c": "current_version",
    "\u7248\u672c": "current_version",
    "\u5927\u5c0f": "size",
    "\u8bed\u8a00": "languages",
    "\u7cfb\u7edf\u8981\u6c42": "requirements",
    "\u5185\u5bb9\u5206\u7ea7": "content_rating",
}
INLINE_LABEL_PATTERNS = {
    "provider": re.compile(r"^Provider\s+(.+)$", re.IGNORECASE),
    "developer": re.compile(r"^Developer\s+(.+)$", re.IGNORECASE),
    "publisher": re.compile(r"^Publisher\s+(.+)$", re.IGNORECASE),
    "current_version": re.compile(r"^Current Version\s+(.+)$", re.IGNORECASE),
    "size": re.compile(r"^Size\s+(.+)$", re.IGNORECASE),
    "last_updated_at": re.compile(r"^Last Updated on\s+(.+)$", re.IGNORECASE),
    "release_date": re.compile(r"^Release date\s+(.+)$", re.IGNORECASE),
    "content_rating": re.compile(r"^Content Rating\s+(.+)$", re.IGNORECASE),
    "provider_cn": re.compile(r"^(?:\u5382\u5546|Provider)[\s:]+(.+)$", re.IGNORECASE),
    "supplier_cn": re.compile(r"^(?:\u4f9b\u5e94\u5546)[\s:]+(.+)$", re.IGNORECASE),
    "developer_cn": re.compile(r"^(?:\u5f00\u53d1\u5546|Developer)[\s:]+(.+)$", re.IGNORECASE),
    "publisher_cn": re.compile(r"^(?:\u53d1\u884c\u5546|Publisher)[\s:]+(.+)$", re.IGNORECASE),
    "current_version_current_cn": re.compile(r"^(?:\u5f53\u524d\u7248\u672c)[\s:]+(.+)$", re.IGNORECASE),
    "current_version_cn": re.compile(r"^(?:\u6700\u65b0\u7248\u672c|\u7248\u672c|Current Version)[\s:]+(.+)$", re.IGNORECASE),
    "size_cn": re.compile(r"^(?:\u5927\u5c0f|Size)[\s:]+(.+)$", re.IGNORECASE),
    "last_updated_at_cn": re.compile(r"^(?:\u66f4\u65b0\u65f6\u95f4|Last Updated on)[\s:]+(.+)$", re.IGNORECASE),
    "release_date_cn": re.compile(r"^(?:\u4e0a\u7ebf\u65f6\u95f4|Release date)[\s:]+(.+)$", re.IGNORECASE),
    "content_rating_cn": re.compile(r"^(?:\u5185\u5bb9\u5206\u7ea7|Content Rating)[\s:]+(.+)$", re.IGNORECASE),
}
INLINE_FIELD_ALIASES = {
    "provider_cn": "provider",
    "supplier_cn": "provider",
    "developer_cn": "developer",
    "publisher_cn": "publisher",
    "current_version_current_cn": "current_version",
    "current_version_cn": "current_version",
    "size_cn": "size",
    "last_updated_at_cn": "last_updated_at",
    "release_date_cn": "release_date",
    "content_rating_cn": "content_rating",
}


def parse_taptap_page(
    markup: str,
    *,
    page_url: str,
    source_format: str = "html",
    review_limit: int = 20,
    include_game: bool = True,
    include_reviews: bool = True,
    include_updates: bool = True,
) -> dict[str, Any]:
    text, lines, links = _extract_content(markup, source_format=source_format, base_url=page_url)
    title_hint = _extract_title_hint(markup, source_format=source_format)

    game = _parse_game(lines, text, links, page_url, title_hint=title_hint) if include_game else {}
    reviews_summary, review_items = (
        _parse_reviews(lines, review_limit=review_limit) if include_reviews else ({"has_next_page": False}, [])
    )
    updates = _parse_updates(lines) if include_updates else []
    sections = [line for line in lines if line in REVIEWS_HEADINGS | UPDATES_HEADINGS | ABOUT_HEADINGS]

    return {
        "game": game,
        "reviews_summary": reviews_summary,
        "reviews": {
            "score": reviews_summary.get("score"),
            "score_scale": reviews_summary.get("score_scale"),
            "ratings_count": reviews_summary.get("ratings_count"),
            "items": review_items,
        },
        "updates": {"items": updates},
        "raw_snapshots": {
            "text_preview": text[:2000],
            "line_count": len(lines),
            "sections": sections,
            "links": links[:20],
        },
    }


def merge_taptap_payloads(*payloads: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "game": {},
        "reviews_summary": {},
        "reviews": {"items": []},
        "updates": {"items": []},
        "raw_snapshots": {"segments": []},
    }
    seen_reviews: set[tuple[str, str, str]] = set()
    seen_updates: set[tuple[str, str]] = set()

    for payload in payloads:
        if not payload:
            continue

        game = payload.get("game") or {}
        for key, value in game.items():
            if value not in (None, "", [], {}):
                merged["game"].setdefault(key, value)

        reviews_summary = payload.get("reviews_summary") or {}
        for key, value in reviews_summary.items():
            if value not in (None, "", [], {}):
                merged["reviews_summary"][key] = value

        reviews = payload.get("reviews") or {}
        if reviews.get("score") is not None:
            merged["reviews"]["score"] = reviews.get("score")
        if reviews.get("score_scale") is not None:
            merged["reviews"]["score_scale"] = reviews.get("score_scale")
        if reviews.get("ratings_count") is not None:
            merged["reviews"]["ratings_count"] = reviews.get("ratings_count")

        for item in reviews.get("items", []):
            identity = (
                str(item.get("author", "")),
                str(item.get("published_at", "")),
                str(item.get("content", "")),
            )
            if identity in seen_reviews:
                continue
            seen_reviews.add(identity)
            merged["reviews"]["items"].append(item)

        updates = payload.get("updates") or {}
        for item in updates.get("items", []):
            identity = (str(item.get("published_at", "")), str(item.get("summary", "")))
            if identity in seen_updates:
                continue
            seen_updates.add(identity)
            merged["updates"]["items"].append(item)

        snapshot = payload.get("raw_snapshots") or {}
        if snapshot:
            merged["raw_snapshots"]["segments"].append(snapshot)

    merged["reviews_summary"].setdefault("has_next_page", False)
    if "score" not in merged["reviews"]:
        merged["reviews"]["score"] = merged["reviews_summary"].get("score")
    if "score_scale" not in merged["reviews"]:
        merged["reviews"]["score_scale"] = merged["reviews_summary"].get("score_scale")
    if "ratings_count" not in merged["reviews"]:
        merged["reviews"]["ratings_count"] = merged["reviews_summary"].get("ratings_count")
    return merged


def _extract_content(markup: str, *, source_format: str, base_url: str) -> tuple[str, list[str], list[dict[str, str]]]:
    links = _extract_links(markup, base_url=base_url) if source_format == "html" else []
    text = _html_to_text(markup) if source_format == "html" else _markdown_to_text(markup)
    return text, _normalize_lines(text), links


def _html_to_text(markup: str) -> str:
    cleaned = re.sub(r"(?is)<(script|style|noscript|svg).*?>.*?</\1>", " ", markup)
    cleaned = re.sub(
        r"(?i)</?(div|section|article|header|footer|main|aside|nav|ul|ol|li|tr|td|th|h1|h2|h3|h4|h5|h6|p|br|a|button|span)[^>]*>",
        "\n",
        cleaned,
    )
    cleaned = re.sub(r"(?i)<[^>]+>", " ", cleaned)
    return html.unescape(cleaned)


def _markdown_to_text(markdown: str) -> str:
    text = markdown.replace("\r\n", "\n")
    text = re.sub(r"!\[[^\]]*]\([^)]+\)", "", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", text)
    text = re.sub(r"^[#>\-\*\s]+", "", text, flags=re.MULTILINE)
    return html.unescape(text)


def _normalize_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw in text.splitlines():
        line = raw.replace("\xa0", " ").strip()
        line = re.sub(r"\s+", " ", line)
        if not line:
            continue
        if line in {"TapTap", "Developer", "Legal", "Terms", "Privacy", "Hop in."}:
            continue
        lines.append(line)
    return lines


def _extract_links(markup: str, *, base_url: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    pattern = re.compile(r'<a[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<label>.*?)</a>', re.IGNORECASE | re.DOTALL)
    for match in pattern.finditer(markup):
        href = html.unescape(match.group("href")).strip()
        label = _strip_tags(match.group("label"))
        if not href or href.startswith("javascript:"):
            continue
        links.append({"label": label, "url": urljoin(base_url, href)})
    return links


def _strip_tags(value: str) -> str:
    value = re.sub(r"(?is)<[^>]+>", " ", value)
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def _parse_game(
    lines: list[str],
    text: str,
    links: list[dict[str, str]],
    page_url: str,
    *,
    title_hint: str | None,
) -> dict[str, Any]:
    game: dict[str, Any] = {
        "page_url": page_url,
        "title": _extract_title(lines, page_url, title_hint=title_hint),
        "provider": None,
        "developer": None,
        "publisher": None,
        "description": _extract_description(lines),
        "genres": [],
        "tags": [],
        "platforms": _extract_platforms(text, links),
        "release_date": None,
        "last_updated_at": None,
        "current_version": None,
        "size": None,
        "languages": [],
        "requirements": None,
        "content_rating": None,
        "followers": None,
        "downloads": None,
        "official_website": None,
        "google_play_url": None,
        "app_store_url": None,
        "region": _extract_region(lines),
    }

    genres = _extract_genres(lines)
    if genres:
        game["genres"] = genres
        game["tags"] = genres[:]

    about = _extract_about(lines)
    for label, field in ABOUT_LABELS.items():
        value = about.get(label)
        if value in (None, "") or _looks_like_noise(value):
            continue
        if field in {"followers", "downloads"}:
            game[field] = _parse_compact_int(value)
        elif field == "languages":
            game[field] = [item.strip() for item in re.split(r",| and |\u3001", value) if item.strip()]
        else:
            game[field] = value

    inline_pairs = _extract_labeled_pairs(lines)
    for label, field in ABOUT_LABELS.items():
        if field in game and game[field] not in (None, "", [], {}):
            continue
        value = inline_pairs.get(label)
        if value in (None, "") or _looks_like_noise(value):
            continue
        if field in {"followers", "downloads"}:
            game[field] = _parse_compact_int(value)
        elif field == "languages":
            game[field] = [item.strip() for item in re.split(r",| and |\u3001", value) if item.strip()]
        else:
            game[field] = value

    inline_values = _extract_inline_values(lines)
    for field, value in inline_values.items():
        if field in {"followers", "downloads"}:
            game[field] = game.get(field) or _parse_compact_int(value)
        else:
            game[field] = game.get(field) or value

    provider = game.get("provider") or _extract_provider(lines, page_url=page_url)
    if provider:
        game["provider"] = provider
        game["developer"] = game.get("developer") or provider
        game["publisher"] = game.get("publisher") or provider
        if game.get("title") in _generic_titles():
            game["title"] = provider
    else:
        game.pop("provider", None)
        game.pop("developer", None)
        game.pop("publisher", None)

    game.update(_select_store_links(links))
    return {key: value for key, value in game.items() if value not in (None, "", [], {})}


def _extract_title(lines: list[str], page_url: str, *, title_hint: str | None = None) -> str:
    if title_hint and title_hint not in _generic_titles():
        return title_hint
    for index, line in enumerate(lines):
        if line in {"Details", "\u8be6\u60c5"} and index + 1 < len(lines):
            candidate = lines[index + 1]
            if candidate not in _generic_titles():
                return candidate
    for line in lines[:15]:
        if line in _generic_titles():
            continue
        if "TapTap" in line and "for Android" in line:
            continue
        if "Games, Posts" in line or "Home > Games >" in line:
            continue
        if len(line) > 3 and not line.startswith("Home"):
            return line
    path = urlparse(page_url).path.rstrip("/").split("/")
    return path[-1] if path else page_url


def _extract_region(lines: list[str]) -> str | None:
    for index, line in enumerate(lines[:12]):
        if line == "Details" and index + 2 < len(lines):
            candidate = lines[index + 2]
            if candidate in REGION_TOKENS:
                return candidate
    for line in lines[:10]:
        if line in REGION_TOKENS:
            return line
    return None


def _extract_genres(lines: list[str]) -> list[str]:
    title = _extract_title(lines, "")
    region = _extract_region(lines)
    provider = _extract_provider(lines)
    start = lines.index(title) + 1 if title in lines else 0
    for line in lines[start:start + 5]:
        if line in {region, provider, "Download", "\u4e0b\u8f7d"}:
            continue
        if len(line) > 60:
            continue
        tokens = re.findall(r"[A-Z][A-Za-z&+.'-]*(?:\s+[A-Z][A-Za-z&+.'-]*)*", line)
        filtered = [token for token in tokens if token not in REGION_TOKENS and len(token) > 1]
        if len(filtered) >= 2:
            return filtered[:8]
    return []


def _extract_provider(lines: list[str], *, page_url: str = "") -> str | None:
    inline_values = _extract_inline_values(lines)
    provider = inline_values.get("provider")
    if provider and not _looks_like_noise(provider):
        return provider

    if "taptap.cn" in page_url or any(_contains_cjk(line) for line in lines):
        return None

    title = _extract_title(lines, "")
    title_index = lines.index(title) if title in lines else 0
    for line in lines[title_index:title_index + 8]:
        if line in {"Download", "Details", "Reviews", "Ratings & Reviews", "Follow", "About"} or line in REGION_TOKENS:
            continue
        if len(line) > 80 or line == title or _looks_like_noise(line):
            continue
        if not DATE_RE.match(line) and not line.endswith("Ratings") and not line.endswith("/10"):
            return line
    return None


def _extract_description(lines: list[str]) -> str | None:
    start = _find_line(lines, "Download", "\u4e0b\u8f7d")
    if start is None:
        return None

    chunks: list[str] = []
    for line in lines[start + 1:]:
        if line in REVIEWS_HEADINGS | UPDATES_HEADINGS | ABOUT_HEADINGS | STOP_SECTION_HEADERS:
            break
        if line in {"Write a review", "\u5199\u8bc4\u4ef7"} or len(line) < 2:
            continue
        chunks.append(line.removeprefix("More ").strip())
    return " ".join(chunks).strip() or None


def _extract_platforms(text: str, links: list[dict[str, str]]) -> list[str]:
    platforms: list[str] = []
    lowered = text.lower()
    if "android" in lowered or "\u5b89\u5353" in text:
        platforms.append("Android")
    if "ios" in lowered or "\u82f9\u679c" in text:
        platforms.append("iOS")
    if "pc" in lowered or "\u7535\u8111" in text:
        platforms.append("PC")
    if not platforms:
        for link in links:
            host = urlparse(link["url"]).netloc.lower()
            if "play.google.com" in host and "Android" not in platforms:
                platforms.append("Android")
            if "apps.apple.com" in host and "iOS" not in platforms:
                platforms.append("iOS")
    return platforms


def _extract_about(lines: list[str]) -> dict[str, str]:
    about_index = _find_line_ordered(lines, ["About the Game", "\u5173\u4e8e\u8fd9\u6b3e\u6e38\u620f", "\u6e38\u620f\u4ecb\u7ecd"])
    if about_index is None:
        return {}

    result: dict[str, str] = {}
    i = about_index + 1
    while i < len(lines):
        line = lines[i]
        if line in STOP_SECTION_HEADERS or line in REVIEWS_HEADINGS | UPDATES_HEADINGS:
            break
        if line in ABOUT_LABELS and i + 1 < len(lines):
            result[line] = lines[i + 1]
            i += 2
            continue
        i += 1
    return result


def _parse_reviews(lines: list[str], *, review_limit: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    summary: dict[str, Any] = {
        "score": None,
        "score_scale": 10,
        "ratings_count": None,
        "has_next_page": any(line in {"Next Page", "\u4e0b\u4e00\u9875"} for line in lines),
    }
    items: list[dict[str, Any]] = []

    index = _find_line_ordered(lines, ["Ratings & Reviews", "\u8bc4\u5206\u4e0e\u8bc4\u4ef7", "\u8bc4\u5206\u53ca\u8bc4\u4ef7", "\u8bc4\u4ef7"])
    if index is None:
        return summary, items

    for line in lines[index:index + 12]:
        score_match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)/10", line)
        if score_match:
            summary["score"] = float(score_match.group(1))
            continue
        ratings_match = re.fullmatch(r"(.+)\s+Ratings", line)
        if ratings_match:
            summary["ratings_count"] = _parse_compact_int(ratings_match.group(1))
            continue
        ratings_cn_match = re.fullmatch(r"(.+?)(?:\u4eba\u8bc4\u4ef7|\u4eba\u8bc4\u5206|\u4e2a\u8bc4\u4ef7|\u4e2a\u8bc4\u5206)", line)
        if ratings_cn_match:
            summary["ratings_count"] = _parse_compact_int(ratings_cn_match.group(1))
            continue
        score_cn_match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)", line)
        if score_cn_match and summary["score"] is None:
            score_value = float(score_cn_match.group(1))
            if 0 <= score_value <= 10:
                summary["score"] = score_value

    stop_index = len(lines)
    for marker in UPDATES_HEADINGS | ABOUT_HEADINGS:
        marker_index = _find_line(lines, marker)
        if marker_index is not None and marker_index > index:
            stop_index = min(stop_index, marker_index)

    review_lines = lines[index + 1:stop_index]
    for pos, line in enumerate(review_lines):
        if not DATE_RE.match(line):
            continue

        author = review_lines[pos - 1].strip() if pos >= 1 else ""
        content = review_lines[pos + 1].strip() if pos + 1 < len(review_lines) else ""
        likes = review_lines[pos + 2].strip() if pos + 2 < len(review_lines) else ""

        if pos >= 2 and COUNT_RE.match(content) and not likes:
            content = review_lines[pos - 2].strip()
            likes = review_lines[pos + 1].strip() if pos + 1 < len(review_lines) else ""
        if pos >= 2 and not content:
            possible_content = review_lines[pos - 2].strip()
            if possible_content and not DATE_RE.match(possible_content):
                content = possible_content

        if not author or author in {"Most Helpful", "Hot", "Latest", "\u6700\u70ed", "\u6700\u65b0"}:
            continue
        if not content or content in {"Most Helpful", "Hot", "Latest", "See All Reviews", "\u67e5\u770b\u5168\u90e8\u8bc4\u4ef7"}:
            continue

        items.append(
            {
                "author": author,
                "published_at": line,
                "rating_text_or_score": None,
                "content": content,
                "likes": _parse_compact_int(likes),
                "reply_count": None,
            }
        )
        if len(items) >= review_limit:
            break

    return summary, items


def _parse_updates(lines: list[str]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str]] = set()
    stop_lines = {
        "View",
        "View update history",
        "Details",
        "Official Website",
        "Download",
        "Download App",
        "App Store Download",
        "Android APK Download",
        "Download PC Version",
        "TapTap Mobile App",
        "TapTap PC Version",
        "Privacy Policy",
        "Required Permissions",
        "\u67e5\u770b",
        "\u67e5\u770b\u66f4\u65b0\u5386\u53f2",
        "\u8be6\u7ec6\u4fe1\u606f",
        "\u5b98\u7f51",
        "\u4e0b\u8f7d",
        "\u4e0b\u8f7dApp",
        "\u4e0b\u8f7d\u624b\u673a APP",
        "\u8bba\u575b",
        "\u6d3b\u52a8",
        "\u9886\u793c\u5305",
        "App Store \u4e0b\u8f7d",
        "Android APK \u4e0b\u8f7d",
        "\u4e0b\u8f7d PC \u7248",
        "TapTap \u624b\u673a APP",
        "TapTap PC \u7248",
        "\u9690\u79c1\u653f\u7b56",
        "\u6240\u9700\u6743\u9650",
    }

    for heading in ("Announcements", "What's new", "Whats new", "What鈥檚 new", "\u516c\u544a", "\u66f4\u65b0\u5185\u5bb9", "\u66f4\u65b0\u8bb0\u5f55", "\u66f4\u65b0\u65e5\u5fd7"):
        index = _find_line(lines, heading)
        if index is None:
            continue

        i = index + 1
        while i < len(lines):
            line = lines[i]
            if line in ABOUT_HEADINGS | REVIEWS_HEADINGS | UPDATES_HEADINGS | STOP_SECTION_HEADERS:
                break

            published_at: str | None = None
            if DATE_RE.match(line):
                published_at = line
                i += 1

            summary_parts: list[str] = []
            while i < len(lines):
                next_line = lines[i]
                if next_line in ABOUT_HEADINGS | REVIEWS_HEADINGS | UPDATES_HEADINGS | STOP_SECTION_HEADERS:
                    break
                if next_line in stop_lines or next_line in ABOUT_LABELS:
                    break
                if published_at and DATE_RE.match(next_line):
                    break
                summary_parts.append(next_line)
                i += 1

            summary = " ".join(summary_parts).strip()
            if not summary or _looks_like_update_noise(summary):
                if published_at is None:
                    i += 1
                continue

            identity = (published_at, summary)
            if identity in seen:
                continue
            seen.add(identity)
            items.append(
                {
                    "title": _guess_update_title(summary),
                    "published_at": published_at,
                    "summary": summary,
                    "platforms": _extract_platform_mentions(summary),
                    "kind": _classify_update_kind(summary),
                    "event_type": _classify_event_type(summary),
                    "version": _extract_update_version(summary),
                    "importance": _infer_update_importance(summary),
                }
            )
            if len(items) >= 20:
                break

    if not items:
        update_index = _find_line(lines, "\u66f4\u65b0")
        if update_index is not None:
            i = update_index + 1
            summary_parts: list[str] = []
            published_at: str | None = None
            while i < len(lines):
                next_line = lines[i]
                if next_line in ABOUT_HEADINGS | REVIEWS_HEADINGS | UPDATES_HEADINGS | STOP_SECTION_HEADERS:
                    break
                if next_line in stop_lines or next_line in ABOUT_LABELS:
                    break
                if DATE_RE.match(next_line):
                    if published_at is None:
                        published_at = next_line
                        i += 1
                        continue
                    break
                if not summary_parts and re.fullmatch(r"\d+(?:\.\d+)+(?:\s*\(\d+\))?", next_line):
                    i += 1
                    continue
                summary_parts.append(next_line)
                i += 1
            summary = " ".join(summary_parts).strip()
            if summary and not _looks_like_update_noise(summary):
                items.append(
                    {
                        "title": _guess_update_title(summary),
                        "published_at": published_at,
                        "summary": summary,
                        "platforms": _extract_platform_mentions(summary),
                        "kind": _classify_update_kind(summary),
                        "event_type": _classify_event_type(summary),
                        "version": _extract_update_version(summary),
                        "importance": _infer_update_importance(summary),
                    }
                )

    return items


def _looks_like_update_noise(summary: str) -> bool:
    compact = re.sub(r"\s+", " ", summary).strip()
    lowered = compact.lower()
    if compact in {
        "View",
        "扫码下载",
        "论坛",
        "活动",
        "领礼包",
        "下载",
    }:
        return True
    if lowered in {"download", "view", "forum", "event"}:
        return True
    if compact.startswith("扫码下载"):
        return True
    noisy_fragments = (
        "Windows 版下载",
        "Windows 客户端下载",
        "主题设置",
        "关于 TapTap",
        "关于我们",
        "开发者",
        "工作机会",
        "产品建议和反馈",
        "状态页",
        "品牌资源",
        "推广中心",
        "资源置换服务",
        "侵权投诉",
        "服务协议",
        "营业执照",
        "沪 ICP",
        "网文",
        "增值电信业务经营许可证",
        "聚光灯计划",
        "kefu@taptap.com",
        "加载中",
    )
    if any(fragment in compact for fragment in noisy_fragments):
        return True
    if re.fullmatch(r"版本[:：]\s*v?", compact, re.IGNORECASE):
        return True
    return False


def _guess_update_title(summary: str) -> str:
    sentence = re.split(r"[.!?。！？]", summary, maxsplit=1)[0].strip()
    return sentence if len(sentence) <= 80 else summary[:77].rstrip() + "..."


def _classify_update_kind(summary: str) -> str:
    lowered = summary.lower()
    if "scheduled to release" in lowered or "\u9884\u8ba1\u4e8e" in summary or "\u5b9a\u6863" in summary:
        return "scheduled_release"
    if summary:
        return "announcement"
    return "unknown"


def _classify_event_type(summary: str) -> str:
    lowered = summary.lower()
    if "scheduled to release" in lowered or "\u9884\u8ba1\u4e8e" in summary or "\u5b9a\u6863" in summary:
        return "scheduled_release"
    if (
        "public beta" in lowered
        or "open beta" in lowered
        or "\u516c\u6d4b" in summary
        or "\u4e0a\u7ebf" in summary
        or "\u6b63\u5f0f\u5f00\u542f" in summary
    ):
        return "launch_event"
    if (
        "version" in lowered
        or re.search(r"\b\d+(?:\.\d+){1,3}\b", summary)
        or "\u7248\u672c" in summary
        or "\u66f4\u65b0\u4e8e" in summary
    ):
        return "version_update"
    if (
        "\u6d3b\u52a8" in summary
        or "\u7b7e\u5230" in summary
        or "\u9650\u65f6" in summary
        or "\u5956\u52b1" in summary
        or "event" in lowered
    ):
        return "major_event"
    if summary:
        return "announcement"
    return "unknown"


def _extract_update_version(summary: str) -> str | None:
    cn_match = re.search(r"\u7248\u672c[^\d]*([0-9]+(?:\.[0-9]+){1,3})", summary)
    if cn_match:
        return cn_match.group(1)
    generic_match = re.search(r"\bv(?:ersion)?\s*([0-9]+(?:\.[0-9]+){1,3})\b", summary, re.IGNORECASE)
    if generic_match:
        return generic_match.group(1)
    bare_match = re.search(r"\b([0-9]+(?:\.[0-9]+){1,3})\b", summary)
    if bare_match and ("\u66f4\u65b0" in summary or "version" in summary.lower()):
        return bare_match.group(1)
    return None


def _infer_update_importance(summary: str) -> str:
    lowered = summary.lower()
    if (
        "\u516c\u6d4b" in summary
        or "\u6b63\u5f0f\u5f00\u542f" in summary
        or "\u4e0a\u7ebf" in summary
        or "\u5b9a\u6863" in summary
        or "open beta" in lowered
        or "public beta" in lowered
        or "scheduled to release" in lowered
        or "launch" in lowered
        or "release" in lowered
    ):
        return "high"
    if (
        "\u7248\u672c" in summary
        or "\u66f4\u65b0" in summary
        or "\u6d3b\u52a8" in summary
        or "\u7b7e\u5230" in summary
        or "event" in lowered
        or "update" in lowered
    ):
        return "medium"
    return "low"


def _extract_platform_mentions(summary: str) -> list[str]:
    platforms: list[str] = []
    lowered = summary.lower()
    if "android" in lowered or "\u5b89\u5353" in summary:
        platforms.append("Android")
    if "ios" in lowered or "\u82f9\u679c" in summary:
        platforms.append("iOS")
    if "pc" in lowered or "\u7535\u8111" in summary:
        platforms.append("PC")
    return platforms


def _extract_inline_values(lines: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in lines:
        compact = re.sub(r"\s+", " ", line).strip()
        for field, pattern in INLINE_LABEL_PATTERNS.items():
            match = pattern.match(compact)
            if match and not _looks_like_noise(match.group(1)):
                values[INLINE_FIELD_ALIASES.get(field, field)] = match.group(1).strip()

        downloads_match = re.search(r"([0-9][0-9,\.KMB]*)\s+Downloads\b", compact, re.IGNORECASE)
        downloads_reverse_match = re.search(r"\bDownloads\s+([0-9][0-9,\.KMB]*)", compact, re.IGNORECASE)
        downloads_cn_match = re.search(r"(?:\u4e0b\u8f7d|Downloads)[\s:]+([0-9][0-9,\.KMB]*)", compact, re.IGNORECASE)
        followers_match = re.search(r"([0-9][0-9,\.KMB]*)\s+Followers\b", compact, re.IGNORECASE)
        followers_reverse_match = re.search(r"\bFollowers\s+([0-9][0-9,\.KMB]*)", compact, re.IGNORECASE)
        followers_cn_match = re.search(r"(?:\u5173\u6ce8|Followers)[\s:]+([0-9][0-9,\.KMB]*)", compact, re.IGNORECASE)

        for field, match in (
            ("downloads", downloads_match),
            ("downloads", downloads_reverse_match),
            ("downloads", downloads_cn_match),
            ("followers", followers_match),
            ("followers", followers_reverse_match),
            ("followers", followers_cn_match),
        ):
            if match and field not in values:
                values[field] = match.group(1)
    return values


def _extract_labeled_pairs(lines: list[str]) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for index, line in enumerate(lines[:-1]):
        if line not in ABOUT_LABELS:
            continue
        next_line = lines[index + 1].strip()
        if not next_line or _looks_like_noise(next_line):
            continue
        pairs.setdefault(line, next_line)
    return pairs


def _select_store_links(links: list[dict[str, str]]) -> dict[str, str | None]:
    selected = {"official_website": None, "google_play_url": None, "app_store_url": None}
    for link in links:
        host = urlparse(link["url"]).netloc.lower()
        if "play.google.com" in host:
            selected["google_play_url"] = selected["google_play_url"] or link["url"]
        elif "apps.apple.com" in host:
            selected["app_store_url"] = selected["app_store_url"] or link["url"]
        elif host and "taptap.io" not in host and "taptap.cn" not in host and "developer.taptap.io" not in host and "developer.taptap.cn" not in host:
            selected["official_website"] = selected["official_website"] or link["url"]
    return selected


def _find_line(lines: list[str], *needles: str) -> int | None:
    for index, line in enumerate(lines):
        if line in needles:
            return index
    return None


def _find_line_ordered(lines: list[str], needles: list[str]) -> int | None:
    for needle in needles:
        index = _find_line(lines, needle)
        if index is not None:
            return index
    return None


def _extract_title_hint(markup: str, *, source_format: str) -> str | None:
    if source_format == "markdown":
        match = re.search(r"^#\s+(.+)$", markup, re.MULTILINE)
        if not match:
            return None
        title = match.group(1).strip()
        return title if title not in _generic_titles() else None

    match = re.search(r"<h1[^>]*>(.*?)</h1>", markup, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    title = _strip_tags(match.group(1))
    return title if title and title not in _generic_titles() else None


def _generic_titles() -> set[str]:
    return {
        "Details",
        "Reviews",
        "Review",
        "Download",
        "Games",
        "Top Charts",
        "Editor's Choice",
        "Ratings & Reviews",
        "Announcements",
        "About the Game",
        "\u8be6\u60c5",
        "\u8bc4\u4ef7",
        "\u4e0b\u8f7d",
        "\u516c\u544a",
        "\u66f4\u65b0\u5185\u5bb9",
        "\u66f4\u65b0\u8bb0\u5f55",
        "\u66f4\u65b0\u65e5\u5fd7",
        "\u5173\u4e8e\u8fd9\u6b3e\u6e38\u620f",
        "\u8bc4\u5206\u4e0e\u8bc4\u4ef7",
        "\u8bc4\u5206\u53ca\u8bc4\u4ef7",
    }


def _looks_like_noise(value: str) -> bool:
    lowered = value.lower()
    return (
        lowered.startswith("[](")
        or "taptap.io/auth" in lowered
        or "taptap.cn/auth" in lowered
        or lowered in {
            "home",
            "login",
            "sign in",
            "\u9996\u9875",
            "\u4e3b\u9875",
            "\u6392\u884c\u699c",
            "\u53d1\u73b0",
            "\u4e91\u6e38\u620f",
            "\u5b98\u65b9\u5165\u9a7b",
        }
        or "\u4eba\u6c14\u51fa\u54c1" in value
        or value.startswith("· ")
        or lowered in {item.lower() for item in _generic_titles()}
        or lowered.startswith("home > games >")
        or "games, posts and people" in lowered
        or "games, posts, and people" in lowered
    )


def _contains_cjk(value: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in value)


def _parse_compact_int(value: str | None) -> int | None:
    if not value:
        return None
    normalized = value.replace(",", "").strip()
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*([KMB万亿])?", normalized, re.IGNORECASE)
    if not match:
        return None
    number = float(match.group(1))
    suffix = (match.group(2) or "").upper()
    multiplier = {
        "": 1,
        "K": 1_000,
        "M": 1_000_000,
        "B": 1_000_000_000,
        "万": 10_000,
        "亿": 100_000_000,
    }[suffix]
    return int(number * multiplier)
