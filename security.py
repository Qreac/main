"""密码哈希。

用标准库 PBKDF2-HMAC-SHA256 + 每用户独立随机盐，不引入 bcrypt/argon2，
避免 Railway 构建时的二进制依赖与 passlib/bcrypt 版本兼容问题。
存储格式：pbkdf2_sha256$<迭代次数>$<盐hex>$<哈希hex>

以后若要换成 argon2/bcrypt，只需替换本文件的两个函数，
数据库字段与接口都不用改。
"""
from __future__ import annotations

import hashlib
import hmac
import secrets

_ALGO = "pbkdf2_sha256"
_ITERATIONS = 260_000
_SALT_BYTES = 16


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(_SALT_BYTES)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _ITERATIONS)
    return f"{_ALGO}${_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        algo, iterations, salt_hex, digest_hex = stored.split("$")
        if algo != _ALGO:
            return False
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            bytes.fromhex(salt_hex),
            int(iterations),
        )
        # compare_digest 避免时序侧信道
        return hmac.compare_digest(digest.hex(), digest_hex)
    except (ValueError, TypeError):
        return False
