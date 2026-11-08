"""WebUI 登录会话 token：服务端随机密钥签名，可吊销。

token 格式 ``v1.<issued_at>.<nonce>.<sig>``，其中
``sig = HMAC-SHA256(server_secret, "v1|<issued_at>|<nonce>|<sha256(api_key) 的 hex>")``：

- ``server_secret`` 是首次使用时生成并持久化的 32 字节随机数。拿到一枚 Cookie 也没法离线
  暴力破解弱 api_key；单凭 api_key 也伪造不了 token。
- 签名里带 api_key 的哈希：更换 ``server.api_key`` 后旧 Cookie 立即失效。
- ``nonce`` 随机：登出时写入吊销表，重放已登出的 Cookie 无效。

文件放 ``data/auth/``，``AUTOFLUX_AUTH_DIR`` 可覆盖（相对路径以项目根目录为基准，与
``AUTOFLUX_PLUGIN_MANAGER_DIR`` 一致）。能读这个目录的人可以伪造会话，按密钥文件管理权限。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
from pathlib import Path
from typing import NamedTuple

from loguru import logger

from src.core.config import get_data_dir

AUTH_DIR_ENV = "AUTOFLUX_AUTH_DIR"
AUTH_DIRNAME = "auth"
SESSION_TTL_SECONDS = 7 * 24 * 3600
_CLOCK_SKEW_SECONDS = 60
_SECRET_FILENAME = "session_secret"
_REVOKED_FILENAME = "revoked_sessions.json"
_TOKEN_VERSION = "v1"
# 严格格式：签发时间只认 ASCII 十进制且最多 12 位（str.isdigit() 会放过 '²'，超长数字 int() 会抛异常），
# nonce 是 token_urlsafe 字符集，签名是小写 hex。不匹配一律视为无效 token。
_TOKEN_RE = re.compile(r"v1\.([0-9]{1,12})\.([A-Za-z0-9_-]{16,64})\.([0-9a-f]{64})")

# 同步鉴权依赖跑在 FastAPI 线程池里，文件读写与缓存更新需要线程锁。
_lock = threading.Lock()
_secret_cache: tuple[Path, bytes] | None = None
_revoked_cache: tuple[Path, tuple[int, int] | None, dict[str, int]] | None = None


class SessionToken(NamedTuple):
    issued_at: int
    nonce: str


def get_auth_dir() -> Path:
    """会话密钥与吊销表所在目录（与 plugin-manager 目录同样的解析规则）。"""
    configured = os.getenv(AUTH_DIR_ENV, "").strip()
    if not configured:
        return get_data_dir() / AUTH_DIRNAME
    candidate = Path(configured)
    if not candidate.is_absolute():
        candidate = get_data_dir().parent / candidate
    return candidate


def issue_session_token(api_key: str, *, now: float | None = None) -> str:
    issued_at = int(time.time() if now is None else now)
    nonce = secrets.token_urlsafe(18)
    return f"{_TOKEN_VERSION}.{issued_at}.{nonce}.{_signature(issued_at, nonce, api_key)}"


def verify_session_token(
    value: str | None, api_key: str, *, now: float | None = None
) -> SessionToken | None:
    """校验格式、签名、有效期和吊销表；任何一项不通过都返回 None，不抛异常。"""
    token = _parse_signed(value, api_key, now=now)
    if token is None or token.nonce in _revoked_nonces():
        return None
    return token


def is_valid_session_token(value: str | None, api_key: str, *, now: float | None = None) -> bool:
    return verify_session_token(value, api_key, now=now) is not None


def revoke_session_token(value: str | None, api_key: str) -> bool:
    """登出：把 token 的 nonce 写进吊销表，保留到 token 本身过期为止。

    只吊销签名有效的 token，伪造的 Cookie 不会触发写盘。
    """
    token = _parse_signed(value, api_key)
    if token is None:
        return False
    now = int(time.time())
    path = get_auth_dir() / _REVOKED_FILENAME
    with _lock:
        revoked = {nonce: exp for nonce, exp in _read_revoked(path).items() if exp > now}
        revoked[token.nonce] = token.issued_at + SESSION_TTL_SECONDS + _CLOCK_SKEW_SECONDS
        _atomic_write(path, json.dumps(revoked, separators=(",", ":")))
        _store_revoked_cache(path, revoked)
    return True


def _parse_signed(
    value: str | None, api_key: str, *, now: float | None = None
) -> SessionToken | None:
    if not value or not api_key:
        return None
    match = _TOKEN_RE.fullmatch(value)
    if match is None:
        return None
    issued_raw, nonce, signature = match.groups()
    issued_at = int(issued_raw)
    current = time.time() if now is None else now
    if issued_at > current + _CLOCK_SKEW_SECONDS or current - issued_at > SESSION_TTL_SECONDS:
        return None
    if not hmac.compare_digest(signature, _signature(issued_at, nonce, api_key)):
        return None
    return SessionToken(issued_at, nonce)


def _signature(issued_at: int, nonce: str, api_key: str) -> str:
    key_digest = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
    message = f"{_TOKEN_VERSION}|{issued_at}|{nonce}|{key_digest}".encode("ascii")
    return hmac.new(_server_secret(), message, hashlib.sha256).hexdigest()


def _server_secret() -> bytes:
    global _secret_cache
    path = get_auth_dir() / _SECRET_FILENAME
    with _lock:
        if _secret_cache is not None and _secret_cache[0] == path:
            return _secret_cache[1]
        secret = _read_secret(path)
        if secret is None:
            secret = secrets.token_bytes(32)
            _atomic_write(path, secret.hex())
        _secret_cache = (path, secret)
        return secret


def _read_secret(path: Path) -> bytes | None:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    try:
        secret = bytes.fromhex(raw.decode("ascii").strip())
    except ValueError:
        secret = b""
    if len(secret) != 32:
        logger.warning(f"会话密钥文件内容无效，重新生成（已签发的登录 Cookie 全部失效）: {path}")
        return None
    return secret


def _revoked_nonces() -> dict[str, int]:
    """吊销表按文件 (mtime, size) 缓存：每次只 stat 一下，文件变了才重读（多进程也能看到登出）。"""
    global _revoked_cache
    path = get_auth_dir() / _REVOKED_FILENAME
    with _lock:
        signature = _stat_signature(path)
        cached = _revoked_cache
        if cached is not None and cached[0] == path and cached[1] == signature:
            return cached[2]
        revoked = _read_revoked(path) if signature is not None else {}
        _revoked_cache = (path, signature, revoked)
        return revoked


def _store_revoked_cache(path: Path, revoked: dict[str, int]) -> None:
    global _revoked_cache
    _revoked_cache = (path, _stat_signature(path), revoked)


def _stat_signature(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_mtime_ns, stat.st_size


def _read_revoked(path: Path) -> dict[str, int]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    try:
        payload = json.loads(raw)
    except ValueError:
        payload = None
    if not isinstance(payload, dict):
        # 本模块只用原子替换写这个文件；内容坏了说明被外部改过——能改它的人同样能改
        # session_secret，按空表处理不会比现状更不安全。
        logger.warning(f"会话吊销表内容无效，按空表处理: {path}")
        return {}
    return {
        nonce: expires_at
        for nonce, expires_at in payload.items()
        if isinstance(expires_at, int) and not isinstance(expires_at, bool)
    }


def _atomic_write(path: Path, text: str) -> None:
    """写临时文件再 os.replace：读者永远看不到半截内容。新文件权限尽量 0600（Windows 上忽略）。"""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(6)}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
