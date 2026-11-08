"""PostgreSQL 连接层（Railway）。

连接串来源优先级：
    1. DATABASE_URL        —— Railway PostgreSQL 插件自动注入（推荐）
    2. DATABASE_PUBLIC_URL —— 部分项目暴露的公网连接串
    3. 都没有时传空串，让 libpq 读取 Railway 注入的
       PGHOST / PGPORT / PGUSER / PGPASSWORD / PGDATABASE

本模块负责「连库 + 建表 + 读取卡片种子」；业务 SQL 全部在 main.py，
并且一律使用 %s 参数占位符，不拼字符串。
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator

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
    log.info(f"准备使用的数据库连接串前缀: {url[:20]}...") 
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
    #
    # 卡片表：与前端 config.js 的卡池一一对应。卡图暂时不入库，image 默认空串。
    # id 用前端的卡片 id（当前就是卡名），因此用 TEXT 主键而不是自增序号。
    # effect 用 JSONB 保存完整效果配置（含数值参数），无效果卡为 NULL。
    """
    CREATE TABLE IF NOT EXISTS cards (
        id          VARCHAR(128) PRIMARY KEY,
        name        VARCHAR(128) NOT NULL,
        atk         INTEGER      NOT NULL DEFAULT 0 CHECK (atk >= 0),
        "def"       INTEGER      NOT NULL DEFAULT 0 CHECK ("def" >= 0),
        attr        VARCHAR(8)   NOT NULL,
        rarity      VARCHAR(4)   NOT NULL,
        effect      JSONB,
        image       TEXT         NOT NULL DEFAULT '',
        created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT chk_cards_rarity CHECK (rarity IN ('N', 'R', 'SR', 'SSR'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_cards_rarity ON cards (rarity)",
)

# 卡片种子文件由前端 config.js 导出（js/cards-sync.js 的 collect()），
# 与本项目一起提交到 GitHub；启动时幂等 upsert，首次部署无需手工导入。
SEED_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cards_seed.json")


def load_seed_cards() -> list[dict[str, Any]]:
    """读取 cards_seed.json；文件缺失或格式不对时返回空列表，不影响服务启动。"""
    if not os.path.exists(SEED_FILE):
        log.warning("未找到卡片种子文件 %s，跳过启动时导入", SEED_FILE)
        return []
    try:
        with open(SEED_FILE, encoding="utf-8") as fp:
            data = json.load(fp)
        cards = data.get("cards", []) if isinstance(data, dict) else data
        if not isinstance(cards, list):
            raise ValueError("cards 字段应为数组")
        return [c for c in cards if isinstance(c, dict) and c.get("id")]
    except Exception as exc:  # 种子是可选数据，坏文件不该让服务起不来
        log.warning("卡片种子文件解析失败，跳过导入: %s", exc)
        return []


def init_schema() -> None:
    """启动时幂等建表，首次部署不需要手工跑 migration。"""
    with connection() as conn:
        with conn.cursor() as cur:
            for statement in SCHEMA_STATEMENTS:
                cur.execute(statement)
    log.info("users / cards 表已就绪")
