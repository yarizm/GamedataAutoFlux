from src.collectors.html_trimmer import trim_html


def test_removes_script_and_style():
    html = "<html><head><style>.x{}</style><script>alert(1)</script></head><body><p>Hello</p></body></html>"
    result = trim_html(html)
    assert "alert" not in result
    assert ".x{}" not in result
    assert "Hello" in result


def test_removes_nav_footer_header():
    html = (
        "<html><body><nav>Nav</nav><main><p>Content</p></main><footer>Foot</footer></body></html>"
    )
    result = trim_html(html)
    assert "Nav" not in result
    assert "Foot" not in result
    assert "Content" in result


def test_prefers_article_over_body():
    html = "<html><body><p>Body text</p><article><h1>Article Title</h1><p>Article body</p></article></body></html>"
    result = trim_html(html)
    assert "Article Title" in result
    assert "Article body" in result


def test_falls_back_to_body_when_no_article():
    html = "<html><body><p>Just body content here</p></body></html>"
    result = trim_html(html)
    assert "Just body content here" in result


def test_compresses_whitespace():
    html = "<html><body><p>  lots   of    spaces  </p></body></html>"
    result = trim_html(html)
    assert "  " not in result.replace("\n", "")
    assert "lots of spaces" in result


def test_strips_html_comments():
    html = "<html><body><!-- comment --><p>Visible</p></body></html>"
    result = trim_html(html)
    assert "comment" not in result
    assert "Visible" in result


def test_removes_all_attributes():
    html = '<html><body><p class="foo" id="bar" data-x="y">Text</p></body></html>'
    result = trim_html(html)
    assert "class" not in result
    assert "data-x" not in result
    assert "Text" in result


def test_truncates_to_max_tokens():
    long_text = "word " * 10000
    html = f"<html><body><p>{long_text}</p></body></html>"
    result = trim_html(html, max_tokens=100)
    assert len(result) < 2000


def test_empty_html():
    assert trim_html("") == ""
    assert trim_html("<html></html>") == ""
