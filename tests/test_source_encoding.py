"""源码编码守卫：防止 UTF-8 文本被按 GBK 解码后写回（乱码）。

曾经发生过：`steamdb_scraper.py` 的价格正则 `€|£|¥` 变成乱码后再也匹配不到非美元价格，
`taptap/parser.py` 的 `What’s new` 标题同样失效。这类损坏不会报语法错误，只能靠静态扫描发现。
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
_SOURCE_GLOBS = (
    ("src", "*.py"),
    ("plugins", "*.py"),
    ("scripts", "*.py"),
    ("src/web/src", "*.js"),
)
_SKIP_PARTS = {"__pycache__", "node_modules", "dist", "data", "build"}
_CJK_RUN = re.compile(r"[ -⁯€　-〿一-鿿＀-￯]+")


def _mojibake_fragment(run: str) -> str | None:
    """返回疑似乱码片段：两个字符的 GBK 字节能拼成一个合法的 3 字节 UTF-8 字符。

    UTF-8 中文的首字节是 0xE2-0xEF，按 GBK 解码后落在罕用的二级字库，正常中文极少出现
    这种字节组合，因此误报率很低。
    """
    for current, following in zip(run, run[1:]):
        try:
            lead = current.encode("cp936")
            trail = following.encode("cp936")
        except UnicodeEncodeError:
            continue
        if len(lead) != 2 or not 0xE2 <= lead[0] <= 0xEF or not 0x80 <= lead[1] <= 0xBF:
            continue
        if not 0x80 <= trail[0] <= 0xBF:
            continue
        try:
            bytes((lead[0], lead[1], trail[0])).decode("utf-8")
        except UnicodeDecodeError:
            continue
        return current + following
    return None


def _source_files() -> list[Path]:
    files: set[Path] = set()
    for directory, pattern in _SOURCE_GLOBS:
        for path in (ROOT / directory).rglob(pattern):
            if not _SKIP_PARTS.intersection(path.relative_to(ROOT).parts):
                files.add(path)
    return sorted(files)


@pytest.mark.parametrize(
    ("garbled", "original"),
    [
        ("What鈥檚 new", "What’s new"),
        ("閲囬泶澶辫触", "采集失败"),
    ],
)
def test_detector_flags_known_mojibake(garbled: str, original: str) -> None:
    assert any(_mojibake_fragment(run.group()) for run in _CJK_RUN.finditer(garbled))
    assert not any(_mojibake_fragment(run.group()) for run in _CJK_RUN.finditer(original))


def test_sources_have_no_mojibake() -> None:
    findings: list[str] = []
    for path in _source_files():
        text = path.read_text(encoding="utf-8")
        for number, line in enumerate(text.splitlines(), 1):
            for run in _CJK_RUN.finditer(line):
                fragment = _mojibake_fragment(run.group())
                if fragment:
                    relative = path.relative_to(ROOT).as_posix()
                    findings.append(f"{relative}:{number}: {fragment!r}")
                    break
    assert not findings, "疑似乱码（UTF-8 被按 GBK 解码）:\n" + "\n".join(findings)
