"""PostgreSQL 连接层（Railway）。

连接串来源优先级：
    1. DATABASE_URL        —— Railway PostgreSQL 插件自动注入（推荐）
    2. DATABASE_PUBLIC_URL —— 部分项目暴露的公网连接串
    3. 都没有时传空串，让 libpq 读取 Railway 注入的
       PGHOST / PGPORT / PGUSER / PGPASSWORD / PGDATABASE

本模块只负责「连库 + 建表」，不写业务 SQL；业务 SQL 全部在 main.py，
并且一律使用 %s 参数占位符，不拼字符串。
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Iterator

from dotenv import load_dotenv
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

load_dotenv()

log = logging.getLogger("fate-api.db")

_pool: ConnectionPool | None = None


def conninfo() -> str:
    """把 Railway 给的连接串整理成 psycopg3 可直接使用的形式。"""
    url = (os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL") or "").strip()
    if url.startswith("postgres://"):
        # psycopg3 / SQLAlchemy 统一用 postgresql:// 前缀
        url = "postgresql://" + url[len("postgres://") :]

    sslmode = (os.getenv("DB_SSLMODE") or "").strip()
    if url and sslmode and "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=" + sslmode
    return url  # 空字符串 = 交给 libpq 读 PG* 环境变量


def init_pool() -> None:
    """在 FastAPI lifespan 里调用一次。"""
    global _pool
    if _pool is not None:
        return
    _pool = ConnectionPool(
        conninfo=conninfo(),
        min_size=1,
        max_size=max(1, int(os.getenv("DB_POOL_MAX", "5"))),
        timeout=10,
        kwargs={"row_factory": dict_row},
        open=True,
    )
    log.info("数据库连接池已创建（max_size=%s）", os.getenv("DB_POOL_MAX", "5"))


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def connection() -> Iterator[Connection]:
    """借一条连接；正常退出自动 commit，异常自动 rollback 并归还连接池。"""
    if _pool is None:
        raise RuntimeError("数据库连接池尚未初始化")
    with _pool.connection() as conn:
        yield conn


# ---------------------------------------------------------------------------
# 建表：序号 id 为唯一主键；account 为登录账户名（大小写不敏感唯一）；
# nickname 为用户昵称（默认允许重复，如需唯一，见文件末尾注释）；
# password_hash 只存哈希，明文密码绝不落库；
# win_rate 用 GENERATED ... STORED 生成列，由数据库保证永远不会和胜负场次不一致。
# ---------------------------------------------------------------------------
# 每条语句单独 execute：psycopg3 用扩展查询协议时不支持一次执行多条 SQL，
# 拆开可以避免不同驱动版本的行为差异。
SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS users (
        id            BIGSERIAL    PRIMARY KEY,
        account       VARCHAR(64)  NOT NULL UNIQUE,
        nickname      VARCHAR(64)  NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        points        INTEGER      NOT NULL DEFAULT 0 CHECK (points >= 0),
        total_games   INTEGER      NOT NULL DEFAULT 0 CHECK (total_games >= 0),
        wins          INTEGER      NOT NULL DEFAULT 0 CHECK (wins >= 0),
        losses        INTEGER      NOT NULL DEFAULT 0 CHECK (losses >= 0),
        win_rate      NUMERIC(5,2) GENERATED ALWAYS AS (
            CASE WHEN total_games > 0
                 THEN ROUND(wins::NUMERIC * 100 / total_games, 2)
                 ELSE 0.00
            END
        ) STORED,
        created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT chk_users_record CHECK (wins + losses <= total_games)
    )
    """,
    # 账户名大小写不敏感唯一（Alice / alice 视为同一个人）
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_users_account_ci ON users (LOWER(account))",
    # 如果以后要求「昵称也不能重复」，取消下面这行注释即可，业务逻辑无需改动：
    # "CREATE UNIQUE INDEX IF NOT EXISTS uq_users_nickname_ci ON users (LOWER(nickname))",
)


def init_schema() -> None:
    """启动时幂等建表，首次部署不需要手工跑 migration。"""
    with connection() as conn:
        with conn.cursor() as cur:
            for statement in SCHEMA_STATEMENTS:
                cur.execute(statement)
    log.info("users 表已就绪")
