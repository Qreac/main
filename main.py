"""FATE DRAW 玩家数据 API —— FastAPI + Railway PostgreSQL。

启动（本地）：
    uvicorn main:app --reload --port 8000
启动（Railway，见 Procfile / railway.toml）：
    uvicorn main:app --host 0.0.0.0 --port $PORT

接口一览：
    GET  /health                     健康检查（Railway 用）
    GET  /users/check-account        写入前检查账户名是否已被占用
    POST /users                      写入新用户（先查重，再插入）
    GET  /users/{user_id}            按序号查询用户
    GET  /users/by-account/{account} 按账户名查询用户
    GET  /users                      分页 / 搜索用户列表
    PATCH /users/{user_id}           修改昵称 / 积分
    PUT  /users/{user_id}/stats      整份战绩覆盖写入（同步本地存档）
    POST /users/{user_id}/match-result  记录一局对战，胜率由数据库自动重算
    POST /users/verify-password      校验账户名 + 密码
    GET  /docs                       Swagger 交互文档
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager, contextmanager
from typing import Iterator

import psycopg
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from psycopg import Connection
from psycopg.errors import UniqueViolation
from psycopg_pool import PoolTimeout

import db
import security
from schemas import (
    MatchResult,
    PasswordCheck,
    UserCreate,
    UserListOut,
    UserOut,
    UserStatsIn,
    UserUpdate,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("fate-api")

# 所有查询都显式列出字段，password_hash 只在校验密码时单独读取
COLS = (
    "id, account, nickname, points, total_games, wins, losses, "
    "win_rate, created_at, updated_at"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_pool()
    db.init_schema()  # 幂等建表，首次部署无需手工跑 SQL
    yield
    db.close_pool()


app = FastAPI(
    title="FATE DRAW 玩家数据 API",
    version="0.1.0",
    description="用户序号为唯一索引；账户名写入前查重；密码只存 PBKDF2 哈希；胜率由数据库生成列自动维护。",
    lifespan=lifespan,
)

_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins or ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@contextmanager
def db_session() -> Iterator[Connection]:
    """统一把数据库异常翻译成 HTTP 状态码，不把内部错误细节暴露给客户端。"""
    try:
        with db.connection() as conn:
            yield conn
    except HTTPException:
        raise
    except UniqueViolation as exc:
        # 并发下两个请求同时通过查重时，由唯一索引兜底
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="账户名已被其他玩家使用",
        ) from exc
    except (RuntimeError, PoolTimeout) as exc:
        log.exception("连接池未就绪或借连接超时: %s", exc)
        raise HTTPException(status_code=503, detail="数据库连接未就绪，请稍后重试") from exc
    except psycopg.Error as exc:
        log.exception("PostgreSQL 操作失败: %s", exc)
        raise HTTPException(status_code=503, detail="数据库暂不可用，请稍后重试") from exc


def _row_or_404(row: dict | None) -> dict:
    if row is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    return row


# ---------------------------------------------------------------------------
# 健康检查
# ---------------------------------------------------------------------------
@app.get("/health", tags=["系统"])
def health() -> dict:
    with db_session() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 AS ok")
        alive = cur.fetchone()["ok"] == 1
    return {"status": "ok" if alive else "degraded", "database": "postgresql"}


@app.get("/", tags=["系统"], include_in_schema=False)
def root() -> dict:
    return {"service": "fate-draw-api", "docs": "/docs", "health": "/health"}


# ---------------------------------------------------------------------------
# 写入前查重 / 写入
# ---------------------------------------------------------------------------
@app.get("/users/check-account", tags=["用户"])
def check_account(
    account: str = Query(..., min_length=2, max_length=64, description="待检查的账户名"),
) -> dict:
    """前端注册页可以先调这个接口做实时校验；POST /users 内部也会再查一次。"""
    name = account.strip()
    with db_session() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE LOWER(account) = LOWER(%s)", (name,))
        used = cur.fetchone() is not None
    return {"account": name, "available": not used}


@app.post("/users", response_model=UserOut, status_code=status.HTTP_201_CREATED, tags=["用户"])
def create_user(payload: UserCreate) -> dict:
    """写入新用户。账户名先查重，唯一索引再兜底并发冲突。"""
    with db_session() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM users WHERE LOWER(account) = LOWER(%s)",
            (payload.account,),
        )
        if cur.fetchone() is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="账户名已被其他玩家使用",
            )

        cur.execute(
            f"""
            INSERT INTO users
                (account, nickname, password_hash, points, total_games, wins, losses)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING {COLS}
            """,
            (
                payload.account,
                payload.nickname,
                security.hash_password(payload.password),
                payload.points,
                payload.total_games,
                payload.wins,
                payload.losses,
            ),
        )
        return _row_or_404(cur.fetchone())


# ---------------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------------
@app.get("/users/by-account/{account}", response_model=UserOut, tags=["用户"])
def get_user_by_account(account: str) -> dict:
    with db_session() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT {COLS} FROM users WHERE LOWER(account) = LOWER(%s)",
            (account.strip(),),
        )
        return _row_or_404(cur.fetchone())


@app.post("/users/verify-password", response_model=UserOut, tags=["用户"])
def verify_password(payload: PasswordCheck) -> dict:
    """登录校验。账户不存在与密码错误返回同一个 401，避免账户名被枚举。"""
    with db_session() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT {COLS}, password_hash FROM users WHERE LOWER(account) = LOWER(%s)",
            (payload.account,),
        )
        row = cur.fetchone()
    if row is None or not security.verify_password(payload.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="账户名或密码错误")
    row.pop("password_hash", None)
    return row


@app.get("/users", response_model=UserListOut, tags=["用户"])
def list_users(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    q: str | None = Query(None, max_length=64, description="按账户名 / 昵称模糊搜索"),
) -> dict:
    where, params = "", []
    if q and q.strip():
        where = "WHERE account ILIKE %s OR nickname ILIKE %s"
        like = f"%{q.strip()}%"
        params = [like, like]

    with db_session() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM users {where}", params)
        total = cur.fetchone()["n"]
        cur.execute(
            f"SELECT {COLS} FROM users {where} ORDER BY id ASC LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        items = cur.fetchall()
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/users/{user_id}", response_model=UserOut, tags=["用户"])
def get_user(user_id: int) -> dict:
    """按序号（主键）查询。"""
    with db_session() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {COLS} FROM users WHERE id = %s", (user_id,))
        return _row_or_404(cur.fetchone())


# ---------------------------------------------------------------------------
# 更新
# ---------------------------------------------------------------------------
@app.patch("/users/{user_id}", response_model=UserOut, tags=["用户"])
def update_user(user_id: int, payload: UserUpdate) -> dict:
    if payload.nickname is None and payload.points is None:
        raise HTTPException(status_code=422, detail="没有需要更新的字段")
    with db_session() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE users SET
                nickname = COALESCE(%s, nickname),
                points   = COALESCE(%s, points),
                updated_at = NOW()
            WHERE id = %s
            RETURNING {COLS}
            """,
            (payload.nickname, payload.points, user_id),
        )
        return _row_or_404(cur.fetchone())


@app.put("/users/{user_id}/stats", response_model=UserOut, tags=["用户"])
def overwrite_stats(user_id: int, payload: UserStatsIn) -> dict:
    """整份战绩覆盖写入，用于把本地存档 / 旧系统数据同步上来。
    win_rate 不需要传，数据库生成列会自动按新的胜负场次重算。"""
    with db_session() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE users SET
                total_games = %s,
                wins        = %s,
                losses      = %s,
                points      = %s,
                updated_at  = NOW()
            WHERE id = %s
            RETURNING {COLS}
            """,
            (
                payload.total_games,
                payload.wins,
                payload.losses,
                payload.points,
                user_id,
            ),
        )
        return _row_or_404(cur.fetchone())


@app.post("/users/{user_id}/match-result", response_model=UserOut, tags=["用户"])
def record_match(user_id: int, payload: MatchResult) -> dict:
    """记录一局结果。用 SQL 原子自增，避免「先读后写」的并发覆盖；
    win_rate 是生成列，数据库会在 UPDATE 时自动重算，无需应用层计算。"""
    win = 1 if payload.result == "win" else 0
    loss = 1 if payload.result == "loss" else 0
    with db_session() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE users SET
                total_games = total_games + 1,
                wins        = wins + %s,
                losses      = losses + %s,
                points      = GREATEST(points + %s, 0),
                updated_at  = NOW()
            WHERE id = %s
            RETURNING {COLS}
            """,
            (win, loss, payload.points_delta, user_id),
        )
        return _row_or_404(cur.fetchone())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
    )
