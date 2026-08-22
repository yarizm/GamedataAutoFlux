"""数据库/连接 URL 凭据脱敏测试。"""

from src.core.sensitive import redact_url_credentials


def test_password_in_userinfo_is_masked():
    url = "postgresql+asyncpg://postgres:secret123@localhost:5432/autoflux"
    assert redact_url_credentials(url) == "postgresql+asyncpg://postgres:***@localhost:5432/autoflux"


def test_user_without_password_unchanged():
    url = "postgresql+asyncpg://postgres@db.internal:5432/autoflux"
    assert redact_url_credentials(url) == url


def test_url_without_credentials_unchanged():
    url = "sqlite+aiosqlite:///./data/autoflux.db"
    assert redact_url_credentials(url) == url


def test_ipv6_host_password_masked():
    url = "postgresql://user:p%40ss@[2001:db8::1]:5432/autoflux"
    masked = redact_url_credentials(url)
    assert "p%40ss" not in masked
    assert masked.startswith("postgresql://user:***@[2001:db8::1]:5432/autoflux")


def test_sensitive_query_params_masked():
    url = "mysql+aiomysql://root@host:3306/db?password=hunter2&charset=utf8"
    masked = redact_url_credentials(url)
    assert "hunter2" not in masked
    assert "password=***" in masked
    assert "charset=utf8" in masked


def test_non_sensitive_query_params_untouched():
    url = "postgresql://host:5432/db?sslmode=require&connect_timeout=10"
    assert redact_url_credentials(url) == url


def test_empty_and_none_inputs():
    assert redact_url_credentials("") == ""
    assert redact_url_credentials(None) == ""
