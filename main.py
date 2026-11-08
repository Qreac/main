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
    GET  /users/{user_id}/points     只取积分（前端登录后刷新用）
    POST /users/{user_id}/draw       抽卡扣分（单抽 10 / 十连 90，服务端原子扣减）
    GET  /users/by-account/{account} 按账户名查询用户
    GET  /users                      分页 / 搜索用户列表
    PATCH /users/{user_id}           修改昵称 / 积分
    PUT  /users/{user_id}/stats      整份战绩覆盖写入（同步本地存档）
    POST /users/{user_id}/match-result  记录一局对战（胜 +10 / 负或平 +5，胜率自动重算）
    POST /users/verify-password      校验账户名 + 密码
    POST /cards/import               批量写入 / 更新卡片（幂等 upsert，前端卡池同步用）
    GET  /cards                      卡片列表（可按稀有度 / 名称筛选）
    GET  /cards/{card_id}            按卡片 id 查询单张
    GET  /docs                       Swagger 交互文档
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Iterator

import psycopg
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from psycopg import Connection
from psycopg.errors import UniqueViolation
from psycopg_pool import PoolTimeout

import db
import security
from schemas import (
    CardImportIn,
    CardImportOut,
    CardListOut,
    CardOut,
    DrawIn,
    DrawOut,
    MatchResult,
    PasswordCheck,
    PointsOut,
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

# 积分规则只在服务端定义，前端不允许自己决定增减多少
DRAW_COST = {"single": 10, "ten": 90}
DRAW_COUNT = {"single": 1, "ten": 10}
BATTLE_REWARD = {"win": 10, "loss": 5, "draw": 5}

# 卡片表字段（"def" 加双引号，因为 def 在部分 SQL 方言里是关键字）
CARD_COLS = 'id, name, atk, "def", attr, rarity, effect, image, created_at, updated_at'
CARD_UPSERT = """
    INSERT INTO cards (id, name, atk, "def", attr, rarity, effect, image)
    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    ON CONFLICT (id) DO UPDATE SET
        name       = EXCLUDED.name,
        atk        = EXCLUDED.atk,
        "def"      = EXCLUDED."def",
        attr       = EXCLUDED.attr,
        rarity     = EXCLUDED.rarity,
        effect     = EXCLUDED.effect,
        image      = EXCLUDED.image,
        updated_at = NOW()
    RETURNING id
"""


def card_params(card: dict) -> tuple:
    """把一条卡片字典整理成 SQL 参数；effect 用 JSON 字符串交给 ::jsonb 转换。"""
    effect = card.get("effect")
    effect_json = (
        json.dumps(effect, ensure_ascii=False) if isinstance(effect, dict)
        else (effect if isinstance(effect, str) and effect.strip() else None)
    )
    return (
        str(card["id"])[:128],
        str(card["name"])[:128],
        int(card.get("atk") or 0),
        int(card.get("def") or 0),
        str(card.get("attr") or "")[:8],
        str(card.get("rarity") or "N").upper()[:4],
        effect_json,
        str(card.get("image") or "")[:512],  # 卡图暂时留空
    )


def row_to_card(row: dict) -> dict:
    """PostgreSQL 的 jsonb 直接回 dict；其它驱动可能回 JSON 字符串，这里统一。"""
    out = dict(row)
    effect = out.get("effect")
    if isinstance(effect, str):
        try:
            out["effect"] = json.loads(effect) if effect.strip() else None
        except json.JSONDecodeError:
            out["effect"] = None
    return out


def seed_cards() -> None:
    """启动时把 cards_seed.json（由前端 config.js 导出）幂等写进 cards 表。
    种子是可选数据，任何异常都只告警，不影响服务启动。"""
    cards = db.load_seed_cards()
    if not cards:
        return
    try:
        with db.connection() as conn, conn.cursor() as cur:
            for card in cards:
                cur.execute(CARD_UPSERT, card_params(card))
                cur.fetchone()
        log.info("卡片种子导入完成：%d 张", len(cards))
    except Exception as exc:
        log.warning("卡片种子导入失败（不影响服务）: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_pool()
    db.init_schema()  # 幂等建表，首次部署无需手工跑 SQL
    seed_cards()      # 首次部署自动把当前卡池写进 cards 表，重复启动不会重复插入
    yield
    db.close_pool()


app = FastAPI(
    title="FATE DRAW 玩家数据 API",
    version="0.1.0",
    description="用户序号为唯一索引；账户名写入前查重；密码只存 PBKDF2 哈希；胜率由数据库生成列自动维护。",
    lifespan=lifespan,
)

# CORS 白名单：正式前端是部署在 Netlify 的静态站，其余为本地调试用。
# "null" 是 file:// 双击打开本地 HTML 时浏览器发出的 Origin。
# Railway 上可再用环境变量 ALLOWED_ORIGINS 覆盖（多个用英文逗号分隔）。
DEFAULT_ORIGINS = ",".join([
    "https://sparkly-bombolone-ceabfb.netlify.app",
    "http://localhost:8000", "http://127.0.0.1:8000",
    "http://localhost:5500", "http://127.0.0.1:5500",
    "http://localhost:3000", "http://127.0.0.1:3000",
    "null",
])
_origins = [
    o.strip()
    for o in os.getenv("ALLOWED_ORIGINS", DEFAULT_ORIGINS).split(",")
    if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=False,             # 本项目不用 cookie，保持 False 才能用 "*" 类宽松来源
    allow_methods=["GET", "POST", "PATCH", "PUT", "OPTIONS"],
    allow_headers=["Content-Type", "Accept", "Authorization"],
    max_age=600,
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


@app.get("/users/{user_id}/points", response_model=PointsOut, tags=["用户"])
def get_points(user_id: int) -> dict:
    """前端登录成功后的轻量积分查询，只回 4 个字段，不含战绩和密码。"""
    with db_session() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, account, nickname, points FROM users WHERE id = %s",
            (user_id,),
        )
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


@app.post("/users/{user_id}/draw", response_model=DrawOut, tags=["用户"])
def draw_cards(user_id: int, payload: DrawIn) -> dict:
    """抽卡扣分。卡池与发牌仍在前端，服务端只做「有多少钱、扣多少、扣完剩多少」，
    用 UPDATE ... WHERE points >= cost 保证并发下不会被扣成负数。"""
    cost = DRAW_COST[payload.kind]
    with db_session() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, account, points FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="用户不存在")
        if row["points"] < cost:
            raise HTTPException(
                status_code=400,
                detail=f"积分不足：需要 {cost} 点，当前只有 {row['points']} 点",
            )
        cur.execute(
            """
            UPDATE users SET points = points - %s, updated_at = NOW()
            WHERE id = %s AND points >= %s
            RETURNING id, account, points
            """,
            (cost, user_id, cost),
        )
        updated = cur.fetchone()
        if updated is None:
            raise HTTPException(status_code=400, detail="积分刚被其他请求扣除，请刷新后重试")
        return {
            "id": updated["id"],
            "account": updated["account"],
            "points": updated["points"],
            "cost": cost,
            "kind": payload.kind,
            "count": DRAW_COUNT[payload.kind],
        }


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
    win_rate 是生成列，数据库会在 UPDATE 时自动重算，无需应用层计算。
    不传 points_delta 时由服务端按胜 +10 / 负或平 +5 发放。"""
    win = 1 if payload.result == "win" else 0
    loss = 1 if payload.result == "loss" else 0
    delta = BATTLE_REWARD[payload.result] if payload.points_delta is None else payload.points_delta
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
            (win, loss, delta, user_id),
        )
        return _row_or_404(cur.fetchone())


# ---------------------------------------------------------------------------
# 卡片：由前端 config.js 导出的卡池，卡图暂时不入库
# ---------------------------------------------------------------------------
@app.post("/cards/import", response_model=CardImportOut, tags=["卡片"])
def import_cards(payload: CardImportIn) -> dict:
    """批量写入卡片。按 id 幂等 upsert：新增或覆盖，重复调用不会插入重复行。
    前端 js/cards-sync.js 会在页面加载时把当前全部卡片推一次上来。"""
    cards = [c.model_dump(by_alias=True) for c in payload.cards]
    with db_session() as conn, conn.cursor() as cur:
        for card in cards:
            cur.execute(CARD_UPSERT, card_params(card))
            cur.fetchone()
        cur.execute("SELECT COUNT(*) AS n FROM cards")
        total = cur.fetchone()["n"]
    return {"imported": len(cards), "total": total}


@app.get("/cards", response_model=CardListOut, tags=["卡片"])
def list_cards(
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    rarity: str | None = Query(None, description="按稀有度筛选：N / R / SR / SSR"),
    q: str | None = Query(None, max_length=64, description="按卡名 / id 模糊搜索"),
) -> dict:
    conds: list[str] = []
    params: list[Any] = []
    if rarity and rarity.strip():
        conds.append("rarity = %s")
        params.append(rarity.strip().upper())
    if q and q.strip():
        conds.append("(name ILIKE %s OR id ILIKE %s)")
        like = f"%{q.strip()}%"
        params += [like, like]
    where = "WHERE " + " AND ".join(conds) if conds else ""

    with db_session() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM cards {where}", params)
        total = cur.fetchone()["n"]
        cur.execute(
            f"SELECT {CARD_COLS} FROM cards {where} ORDER BY id ASC LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        items = [row_to_card(r) for r in cur.fetchall()]
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/cards/{card_id}", response_model=CardOut, tags=["卡片"])
def get_card(card_id: str) -> dict:
    """按卡片 id 查询（中文卡名需 URL 编码，浏览器会自动处理）。"""
    with db_session() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {CARD_COLS} FROM cards WHERE id = %s", (card_id,))
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="卡片不存在")
    return row_to_card(row)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
    )
