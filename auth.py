"""JWT（HS256）登录凭证。

只用标准库 hmac / hashlib / base64 / json，不引入 PyJWT 或 python-jose，
与 security.py 一样避免 Railway 构建时的额外依赖与版本兼容问题。

签名密钥来自环境变量 JWT_SECRET（也接受 SECRET_KEY）。**生产必须配置固定值**：
没配置时用启动时随机密钥，服务每次重启都会让所有已签发的令牌失效。

令牌载荷：{"sub": 用户id字符串, "account": 账户名, "iat": 签发时间, "exp": 过期时间}
受保护接口通过 auth.require_owner 依赖校验：签名有效 + 未过期 + sub 等于路径 user_id。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from typing import Any

from fastapi import Header, HTTPException, Request, status

log = logging.getLogger("fate-auth")

ALGORITHM = "HS256"

_raw_secret = os.getenv("JWT_SECRET") or os.getenv("SECRET_KEY")
if _raw_secret:
    SECRET = _raw_secret.encode("utf-8")
else:
    SECRET = secrets.token_bytes(32)
    log.warning("未配置 JWT_SECRET，已使用临时随机密钥：服务重启后旧令牌会失效")

try:
    EXPIRE_SECONDS = int(os.getenv("JWT_EXPIRE_MINUTES", "10080")) * 60  # 默认 7 天
except ValueError:
    EXPIRE_SECONDS = 10080 * 60


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _sign(signing_input: str) -> str:
    # 用 utf-8 而非 ascii：恶意令牌里出现中文时也不会抛 UnicodeEncodeError 变成 500
    digest = hmac.new(SECRET, signing_input.encode("utf-8"), hashlib.sha256).digest()
    return _b64(digest)


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def create_access_token(
    user_id: int, account: str, expires_seconds: int | None = None
) -> str:
    """登录 / 注册成功后签发。expires_seconds 仅供测试构造过期令牌。"""
    now = int(time.time())
    ttl = EXPIRE_SECONDS if expires_seconds is None else expires_seconds
    header = {"alg": ALGORITHM, "typ": "JWT"}
    payload: dict[str, Any] = {
        "sub": str(user_id),
        "account": account,
        "iat": now,
        "exp": now + ttl,
    }
    head = _b64(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    body = _b64(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    signing_input = head + "." + body
    return signing_input + "." + _sign(signing_input)


def decode_access_token(token: str) -> dict:
    """校验签名、算法、有效期与 sub；任何问题都统一抛 401，不泄露细节。"""
    try:
        head, body, signature = token.split(".")
    except (ValueError, AttributeError):
        raise _unauthorized("登录凭证格式不正确")
    try:
        valid = hmac.compare_digest(
            signature.encode("utf-8"), _sign(head + "." + body).encode("utf-8")
        )
    except (AttributeError, UnicodeError):
        valid = False
    if not valid:
        raise _unauthorized("登录凭证签名无效")
    try:
        header = json.loads(_unb64(head))
        payload = json.loads(_unb64(body))
    except Exception:
        raise _unauthorized("登录凭证内容无法解析")
    if header.get("alg") != ALGORITHM:
        raise _unauthorized("不支持的凭证算法")
    exp = payload.get("exp")
    if not isinstance(exp, (int, float)) or exp < time.time():
        raise _unauthorized("登录已过期，请重新登录")
    sub = payload.get("sub")
    if not isinstance(sub, str) or not sub.isdigit():
        raise _unauthorized("登录凭证缺少用户标识")
    return payload


def bearer_token(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise _unauthorized("缺少登录凭证，请先登录")
    token = authorization[7:].strip()
    if not token:
        raise _unauthorized("缺少登录凭证，请先登录")
    return token


def require_owner(request: Request, authorization: str | None = Header(None)) -> dict:
    """FastAPI 依赖：必须是本人令牌，才能读写这个 user_id 的数据。

    路径里没有 user_id 的接口不要用这个依赖；越权返回 403，
    无凭证 / 凭证无效 / 过期返回 401。
    """
    claims = decode_access_token(bearer_token(authorization))
    try:
        path_uid = int(request.path_params.get("user_id"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="用户序号不合法")
    if int(claims["sub"]) != path_uid:
        raise HTTPException(status_code=403, detail="无权访问其他玩家的数据")
    return claims
