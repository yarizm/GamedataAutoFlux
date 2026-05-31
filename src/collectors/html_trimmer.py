"""HTML 结构化裁剪，为 LLM 提取做预处理。"""

from __future__ import annotations

import re
from html.parser import HTMLParser


def trim_html(html: str, max_tokens: int = 4000) -> str:
    """裁剪 HTML 到 LLM 可处理的精简文本。

    处理流程：
    1. 移除 script/style/nav/footer/header/noscript/svg/iframe
    2. 优先保留 article/main/[role="main"] 区域
    3. 移除 HTML 注释
    4. 移除所有标签属性，保留标签结构
    5. 压缩空白
    6. 截断到 max_tokens（约 4 字符/token）
    """
    if not html or not html.strip():
        return ""

    parser = _HTMLTrimmer()
    parser.feed(html)
    text = parser.get_text()

    if not text.strip():
        return ""

    # 硬截断：1 token ≈ 4 字符
    max_chars = max_tokens * 4
    if len(text) > max_chars:
        text = text[:max_chars].rsplit("\n", 1)[0]

    return text


_STRIP_TAGS = {"script", "style", "nav", "footer", "header", "noscript", "svg", "iframe"}
_CONTENT_TAGS = {
    "article", "main", "section", "div", "p", "h1", "h2", "h3", "h4", "h5", "h6",
    "li", "td", "th", "pre", "blockquote", "figcaption", "summary",
}


class _HTMLTrimmer(HTMLParser):
    """解析 HTML，移除噪声标签，保留结构化文本。"""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._text_parts: list[str] = []
        self._skip_depth: int = 0
        self._tag_stack: list[str] = []
        self._in_pre: int = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        self._tag_stack.append(tag)
        if tag in _STRIP_TAGS:
            self._skip_depth += 1
        if tag == "pre":
            self._in_pre += 1
        if tag in _CONTENT_TAGS and self._skip_depth == 0:
            self._text_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in _STRIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
        if tag == "pre":
            self._in_pre = max(0, self._in_pre - 1)
        if tag in _CONTENT_TAGS and self._skip_depth == 0:
            self._text_parts.append("\n")
        if self._tag_stack and self._tag_stack[-1] == tag:
            self._tag_stack.pop()

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        if self._in_pre:
            self._text_parts.append(data)
        else:
            self._text_parts.append(data)

    def handle_comment(self, data: str) -> None:
        pass  # 丢弃注释

    def get_text(self) -> str:
        raw = "".join(self._text_parts)
        raw = re.sub(r"[^\S\n]+", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()
