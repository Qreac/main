"""双真人对战：房间大厅 + WebSocket 中继（战斗由房主客户端计算）。

架构约定（用户选定）：沿用前端 js/battle.js 的规则引擎，房主客户端执行
出牌 / 结束回合并把最新棋盘快照发给服务器；服务器负责房间、消息转发、
30 秒回合计时、掉线判负、真人局积分（胜 +50 / 负 +10），不重算战斗规则。

房间状态放在进程内存里：Railway 单实例足够，服务重启后房间自动消失。
积分写库用参数化 SQL 原子更新，每个房间只会结算一次（status=finished 兜底）。

消息协议见各 handler 的 send_json；REST 前缀 /pvp，WS 路径 /pvp/ws/{room_id}。
"""
from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, status

import auth
import db

log = logging.getLogger("fate-pvp")

TURN_SECONDS = 30          # 每次出牌的思考时间
HEARTBEAT_TIMEOUT = 18     # 超过这么久没心跳视为掉线
TIMEOUT_GRACE = 8          # 超时后给房主执行「自动结束回合」的宽限
PVP_WIN = 50               # 真人对战胜利奖励
PVP_LOSS = 10              # 真人对战失败奖励（人机仍是 +10 / +5）
FINISHED_TTL = 120         # 结算后房间保留时间

router = APIRouter(prefix="/pvp", tags=["双人对战"])
ROOMS: dict[str, "Room"] = {}
_bg_task: asyncio.Task | None = None


@dataclass
class Room:
    id: str
    host_id: int
    host_name: str
    guest_id: int | None = None
    guest_name: str | None = None
    status: str = "waiting"            # waiting | playing | finished
    created_at: float = field(default_factory=time.time)
    first: str | None = None           # 'player'(房主) | 'ai'(客端)
    turn_key: str = ""
    turn_side: str | None = None
    turn_no: int = 0
    deadline: float | None = None
    timeout_key: str | None = None
    finished_at: float | None = None
    sockets: dict[int, WebSocket] = field(default_factory=dict)
    last_seen: dict[int, float] = field(default_factory=dict)
    decks: dict[int, list[str]] = field(default_factory=dict)  # 卡名数组，房主建局要用
    result: dict | None = None


def room_view(room: Room) -> dict:
    return {
        "id": room.id,
        "host": {"id": room.host_id, "name": room.host_name},
        "guest": ({"id": room.guest_id, "name": room.guest_name}
                  if room.guest_id is not None else None),
        "status": room.status,
        "players": 1 + (1 if room.guest_id is not None else 0),
        "first": room.first,
        "turn_seconds": TURN_SECONDS,
        "created_at": room.created_at,
    }


def _new_room_id() -> str:
    for _ in range(20):
        rid = secrets.token_hex(3).upper()   # 6 位十六进制房间号
        if rid not in ROOMS:
            return rid
    raise RuntimeError("房间号生成失败")


def _close_stale_rooms_for(uid: int) -> None:
    """一个玩家同时只保留一个房间，避免重复开房占列表。"""
    for rid, room in list(ROOMS.items()):
        if room.status == "waiting" and room.host_id == uid and not room.sockets:
            ROOMS.pop(rid, None)


def _award_points(winner_id: int, loser_id: int) -> dict:
    """用一条 UPDATE 原子加分 + 记战绩；win_rate 由数据库生成列自动重算。"""
    out: dict[str, Any] = {}
    with db.connection() as conn, conn.cursor() as cur:
        for uid, delta, win, loss in (
            (winner_id, PVP_WIN, 1, 0),
            (loser_id, PVP_LOSS, 0, 1),
        ):
            cur.execute(
                """
                UPDATE users SET
                    points      = points + %s,
                    total_games = total_games + 1,
                    wins        = wins + %s,
                    losses      = losses + %s,
                    updated_at  = NOW()
                WHERE id = %s
                RETURNING id, points
                """,
                (delta, win, loss, uid),
            )
            row = cur.fetchone()
            out[str(uid)] = {
                "delta": delta,
                "points": row["points"] if row else None,
            }
    return out


def _award_draw(uid_a: int, uid_b: int) -> dict:
    """平局：双方各拿失败档 +10，各记 1 场但不计胜 / 负。"""
    out: dict[str, Any] = {}
    with db.connection() as conn, conn.cursor() as cur:
        for uid in (uid_a, uid_b):
            cur.execute(
                """
                UPDATE users SET points = points + %s,
                    total_games = total_games + 1, updated_at = NOW()
                WHERE id = %s RETURNING id, points
                """,
                (PVP_LOSS, uid),
            )
            row = cur.fetchone()
            out[str(uid)] = {"delta": PVP_LOSS,
                             "points": row["points"] if row else None}
    return out


def _view_state(state: dict, viewer_side: str) -> dict:
    """转发给观战方时隐藏对手手牌 / 牌库内容，只保留张数。"""
    try:
        out = json.loads(json.dumps(state, ensure_ascii=False))
    except (TypeError, ValueError):
        return state
    players = out.get("players") or {}
    for side in ("player", "ai"):
        if side == viewer_side:
            continue
        p = players.get(side) or {}
        for key in ("hand", "library"):
            arr = p.get(key)
            if isinstance(arr, list):
                p[key] = [{"hidden": True} for _ in arr]
    return out


async def _send(ws: WebSocket | None, payload: dict) -> bool:
    if ws is None:
        return False
    try:
        await ws.send_json(payload)
        return True
    except Exception:
        return False


async def _broadcast(room: Room, payload: dict, exclude: int | None = None) -> None:
    dead: list[int] = []
    for uid, sock in list(room.sockets.items()):
        if uid == exclude:
            continue
        if not await _send(sock, payload):
            dead.append(uid)
    for uid in dead:
        await _on_disconnect(room, uid, "write_failed")


async def _finish(room: Room, winner_id: int, loser_id: int, reason: str) -> None:
    """结算：只发一次积分，广播结果并把房间标记为 finished。"""
    if room.status != "playing":
        return
    room.status = "finished"
    room.finished_at = time.time()
    room.deadline = None
    try:
        if winner_id == 0:  # 平局
            points = _award_draw(room.host_id, room.guest_id)
            loser_id = 0
        else:
            points = _award_points(winner_id, loser_id)
    except Exception as exc:
        log.exception("真人局积分结算失败: %s", exc)
        points = {}
    payload = {
        "type": "result",
        "winner": winner_id,
        "loser": loser_id,
        "reason": reason,
        "points": points,
        "rewards": {"win": PVP_WIN, "loss": PVP_LOSS},
    }
    room.result = payload
    await _broadcast(room, payload)


async def _on_disconnect(room: Room, uid: int, reason: str = "disconnect") -> None:
    """掉线判负：对局中断线者直接判负，另一方获胜并领取 +50。"""
    if uid not in room.sockets and uid not in room.last_seen:
        return
    room.sockets.pop(uid, None)
    room.last_seen.pop(uid, None)
    if room.status == "playing":
        winner = room.guest_id if uid == room.host_id else room.host_id
        loser = uid
        await _broadcast(room, {"type": "opponent_left", "user_id": uid, "reason": reason})
        if winner is not None:
            await _finish(room, winner, loser, "disconnect")
    elif room.status == "waiting":
        if uid == room.host_id:
            await _broadcast(room, {"type": "room_closed", "reason": "房主已离开"})
            ROOMS.pop(room.id, None)
        elif uid == room.guest_id:
            room.guest_id = None
            room.guest_name = None
            await _broadcast(room, {"type": "presence", "room": room_view(room)})


async def _tick() -> None:
    """后台巡检：心跳掉线、30 秒超时、过期房间清理。"""
    now = time.time()
    for room in list(ROOMS.values()):
        for uid, last in list(room.last_seen.items()):
            if now - last > HEARTBEAT_TIMEOUT:
                ws = room.sockets.pop(uid, None)
                if ws is not None:
                    try:
                        await ws.close(code=1001, reason="心跳超时")
                    except Exception:
                        pass
                await _on_disconnect(room, uid, "heartbeat_timeout")
        if room.status == "playing" and room.deadline and now >= room.deadline:
            if room.timeout_key != room.turn_key:
                room.timeout_key = room.turn_key
                await _broadcast(room, {
                    "type": "timeout", "side": room.turn_side, "turn": room.turn_no,
                    "message": "思考超时，自动结束回合",
                })
            room.deadline = now + TIMEOUT_GRACE
        if room.status == "finished" and room.finished_at and now - room.finished_at > FINISHED_TTL:
            ROOMS.pop(room.id, None)


async def _watchdog() -> None:
    while True:
        try:
            await asyncio.sleep(1)
            await _tick()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("PVP 巡检异常: %s", exc)


def start_background_tasks() -> None:
    global _bg_task
    if _bg_task is None or _bg_task.done():
        _bg_task = asyncio.create_task(_watchdog())


async def stop_background_tasks() -> None:
    global _bg_task
    if _bg_task is not None:
        _bg_task.cancel()
        try:
            await _bg_task
        except asyncio.CancelledError:
            pass
        _bg_task = None


# ---------------------------------------------------------------------------
# REST：房间大厅
# ---------------------------------------------------------------------------
@router.get("/rooms")
def list_rooms(claims: dict = Depends(auth.require_auth)) -> dict:
    items = [room_view(r) for r in ROOMS.values() if r.status != "finished"]
    items.sort(key=lambda x: x["created_at"], reverse=True)
    return {"items": items, "total": len(items), "turn_seconds": TURN_SECONDS,
            "rewards": {"win": PVP_WIN, "loss": PVP_LOSS}}


@router.post("/rooms", status_code=status.HTTP_201_CREATED)
def create_room(claims: dict = Depends(auth.require_auth)) -> dict:
    uid = int(claims["sub"])
    _close_stale_rooms_for(uid)
    room = Room(id=_new_room_id(), host_id=uid,
                host_name=str(claims.get("account") or uid))
    ROOMS[room.id] = room
    log.info("房间 %s 由 %s 创建", room.id, room.host_name)
    return {"room": room_view(room)}


@router.get("/rooms/{room_id}")
def get_room(room_id: str, claims: dict = Depends(auth.require_auth)) -> dict:
    room = ROOMS.get(room_id.upper())
    if room is None:
        raise HTTPException(status_code=404, detail="房间不存在或已结束")
    return {"room": room_view(room)}


@router.post("/rooms/{room_id}/join")
def join_room(room_id: str, claims: dict = Depends(auth.require_auth)) -> dict:
    room = ROOMS.get(room_id.upper())
    if room is None:
        raise HTTPException(status_code=404, detail="房间不存在或已结束")
    uid = int(claims["sub"])
    if room.host_id == uid:
        raise HTTPException(status_code=400, detail="不能加入自己开的房间")
    if room.guest_id is not None and room.guest_id != uid:
        raise HTTPException(status_code=409, detail="房间已经坐满了")
    if room.status != "waiting":
        raise HTTPException(status_code=409, detail="对局已经开始")
    room.guest_id = uid
    room.guest_name = str(claims.get("account") or uid)
    return {"room": room_view(room)}


@router.post("/rooms/{room_id}/start")
async def start_room(room_id: str, claims: dict = Depends(auth.require_auth)) -> dict:
    room = ROOMS.get(room_id.upper())
    if room is None:
        raise HTTPException(status_code=404, detail="房间不存在或已结束")
    if int(claims["sub"]) != room.host_id:
        raise HTTPException(status_code=403, detail="只有房主可以开始对局")
    if room.guest_id is None:
        raise HTTPException(status_code=409, detail="等待另一位玩家加入")
    if room.status != "waiting":
        raise HTTPException(status_code=409, detail="对局已经开始")
    if room.host_id not in room.sockets or room.guest_id not in room.sockets:
        raise HTTPException(status_code=409, detail="还有玩家没有连接就绪")
    if room.host_id not in room.decks or room.guest_id not in room.decks:
        raise HTTPException(status_code=409, detail="还有玩家没有提交卡组")

    room.status = "playing"
    room.first = secrets.choice(["player", "ai"])  # 服务端随机先手，房主=player
    room.turn_key = ""
    room.turn_side = room.first
    room.turn_no = 1
    room.deadline = time.time() + TURN_SECONDS
    room.timeout_key = None
    payload = {
        "type": "start", "room": room_view(room), "first": room.first,
        "host": {"id": room.host_id, "name": room.host_name},
        "guest": {"id": room.guest_id, "name": room.guest_name},
        "decks": {"player": room.decks[room.host_id], "ai": room.decks[room.guest_id]},
        "deadline_in": TURN_SECONDS,
    }
    await _broadcast(room, payload)
    return {"room": room_view(room), "first": room.first}


@router.delete("/rooms/{room_id}")
async def leave_room(room_id: str, claims: dict = Depends(auth.require_auth)) -> dict:
    room = ROOMS.get(room_id.upper())
    if room is None:
        return {"ok": True}
    uid = int(claims["sub"])
    ws = room.sockets.get(uid)
    if ws is not None:
        try:
            await ws.close()
        except Exception:
            pass
    await _on_disconnect(room, uid, "leave")
    return {"ok": True}


# ---------------------------------------------------------------------------
# WebSocket：鉴权 → 中继 → 掉线处理
# ---------------------------------------------------------------------------
@router.websocket("/ws/{room_id}")
async def room_socket(ws: WebSocket, room_id: str) -> None:
    room = ROOMS.get(room_id.upper())
    uid = 0
    try:
        claims = auth.decode_access_token(ws.query_params.get("token") or "")
        uid = int(claims["sub"])
        raw_uid = ws.query_params.get("uid")
        if raw_uid and int(raw_uid) != uid:
            raise ValueError("身份不匹配")
        if room is None:
            raise ValueError("房间不存在或已结束")
        if uid not in (room.host_id, room.guest_id):
            raise ValueError("你不在这个房间里")
    except Exception as exc:
        await ws.accept()
        await ws.send_json({"type": "error", "message": str(exc) or "连接校验失败"})
        await ws.close()
        return

    await ws.accept()
    room.sockets[uid] = ws
    room.last_seen[uid] = time.time()
    await _broadcast(room, {"type": "presence", "room": room_view(room)})
    if room.status == "playing" and room.result is None:
        await _send(ws, {"type": "state_resume", "room": room_view(room),
                         "first": room.first, "deadline_in": TURN_SECONDS})

    try:
        while True:
            raw = await ws.receive_text()
            room.last_seen[uid] = time.time()
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if not isinstance(data, dict):
                continue
            t = data.get("type")

            if t == "ping":
                await _send(ws, {"type": "pong"})
                continue

            if t == "deck":
                cards = data.get("cards")
                if isinstance(cards, list):
                    room.decks[uid] = [str(x)[:128] for x in cards[:40]]
                await _send(ws, {"type": "deck_ack",
                                 "count": len(room.decks.get(uid, []))})
                continue

            if t == "leave":
                await _on_disconnect(room, uid, "leave")
                break

            if t == "action":
                if room.status != "playing":
                    await _send(ws, {"type": "error", "message": "对局未在进行中"})
                    continue
                side = "player" if uid == room.host_id else "ai"
                if room.turn_side and side != room.turn_side:
                    await _send(ws, {"type": "error", "message": "还没轮到你出牌"})
                    continue
                host_ws = room.sockets.get(room.host_id)
                if not await _send(host_ws, {"type": "action", "from": uid,
                                             "side": side, "action": data.get("action")}):
                    await _on_disconnect(room, room.host_id, "write_failed")
                continue

            if t == "state":
                if uid != room.host_id:
                    await _send(ws, {"type": "error", "message": "只有房主能同步棋盘"})
                    continue
                if room.status != "playing":
                    continue
                st = data.get("state")
                if not isinstance(st, dict):
                    continue
                # 每次出牌（每次 state 快照）都重新给 30 秒：
                # 玩家在自己回合内连续出牌不会挤占彼此的思考时间。
                room.turn_key = str(st.get("turn")) + ":" + str(st.get("side"))
                room.turn_side = str(st.get("side") or room.turn_side)
                room.turn_no = int(st.get("turn") or room.turn_no)
                room.deadline = time.time() + TURN_SECONDS
                room.timeout_key = None
                deadline_in = (max(1, int(room.deadline - time.time()))
                               if room.deadline else TURN_SECONDS)
                await _broadcast(room, {"type": "state_ack", "deadline_in": deadline_in,
                                        "turn": room.turn_no, "side": room.turn_side})
                await _send(room.sockets.get(room.guest_id),
                            {"type": "state", "state": _view_state(st, "ai"),
                             "deadline_in": deadline_in})
                continue

            if t == "result":
                if uid != room.host_id:
                    await _send(ws, {"type": "error", "message": "只有房主能提交结果"})
                    continue
                if room.status != "playing":
                    continue
                try:
                    winner = int(data.get("winner"))
                except (TypeError, ValueError):
                    continue
                if winner not in (0, room.host_id, room.guest_id):
                    await _send(ws, {"type": "error", "message": "获胜者不在本房间"})
                    continue
                if winner == 0:
                    await _finish(room, 0, 0, "draw")
                    continue
                loser = room.guest_id if winner == room.host_id else room.host_id
                await _finish(room, winner, loser, str(data.get("reason") or "ko"))
                continue

            if t == "chat":
                text = str(data.get("text") or "")[:120]
                await _broadcast(room, {"type": "chat", "from": uid, "text": text},
                                 exclude=uid)
                continue

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.warning("房间 %s 的 WebSocket 异常: %s", room.id, exc)
    finally:
        await _on_disconnect(room, uid, "disconnect")
