"""请求 / 响应模型。

注意：UserOut 里故意没有 password_hash 字段，任何查询接口都不会把密码哈希吐出去。
卡片字段名与前端 config.js 保持一致；因为 def 是 Python 关键字，用别名 def 收发，
Python 内部访问时用 defense。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _strip(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("不能为空")
    return value


class StatsMixin(BaseModel):
    """战绩字段的公共约束：胜 + 负 不能超过总场数（差额即平局）。"""
    total_games: int = Field(default=0, ge=0)
    wins: int = Field(default=0, ge=0)
    losses: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def check_record(self):
        if self.wins + self.losses > self.total_games:
            raise ValueError("胜场 + 败场不能大于总游戏场数")
        return self


class UserCreate(StatsMixin):
    account: str = Field(min_length=2, max_length=64, description="用户账户名 / 登录名，唯一")
    nickname: str = Field(min_length=1, max_length=64, description="用户昵称，默认可重复")
    password: str = Field(min_length=8, max_length=128, description="明文密码，仅在传输时出现")
    points: int = Field(default=0, ge=0, description="初始积分，默认 0")

    @field_validator("account", "nickname")
    @classmethod
    def strip_text(cls, v: str) -> str:
        return _strip(v)

    @field_validator("account")
    @classmethod
    def no_space_in_account(cls, v: str) -> str:
        if any(ch.isspace() for ch in v):
            raise ValueError("账户名不能包含空格")
        return v


class UserStatsIn(StatsMixin):
    """整份战绩覆盖写入（用于把本地存档同步到服务器）。"""
    total_games: int = Field(ge=0)
    wins: int = Field(ge=0)
    losses: int = Field(ge=0)
    points: int = Field(default=0, ge=0)


class UserUpdate(BaseModel):
    nickname: str | None = Field(default=None, min_length=1, max_length=64)
    points: int | None = Field(default=None, ge=0)

    @field_validator("nickname")
    @classmethod
    def strip_nickname(cls, v: str | None) -> str | None:
        return _strip(v) if v is not None else None


class MatchResult(BaseModel):
    result: Literal["win", "loss", "draw"] = Field(description="胜 / 负 / 平（平局也计入总场数）")
    points_delta: int | None = Field(
        default=None, ge=-100000, le=100000,
        description="本局积分增减；不传时由服务端发放：胜 +10 / 负或平 +5",
    )


class DrawIn(BaseModel):
    kind: Literal["single", "ten"] = Field(description="单抽 / 十连")


class DrawOut(BaseModel):
    id: int
    account: str
    points: int
    cost: int
    kind: str
    count: int


class PasswordCheck(BaseModel):
    account: str = Field(min_length=2, max_length=64)
    password: str = Field(min_length=1, max_length=128)

    @field_validator("account")
    @classmethod
    def strip_account(cls, v: str) -> str:
        return _strip(v)


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="序号，唯一索引")
    account: str
    nickname: str
    points: int
    total_games: int
    wins: int
    losses: int
    win_rate: float = Field(description="百分比，例如 66.67 表示 66.67%")
    created_at: datetime
    updated_at: datetime


class TokenOut(UserOut):
    """登录 / 注册返回用户资料 + JWT 凭证；其它用户查询接口仍只返回 UserOut。"""
    access_token: str = Field(description="JWT，后续请求放在 Authorization: Bearer <token>")
    token_type: str = "Bearer"
    expires_in: int = Field(description="凭证有效期（秒）")


class PointsOut(BaseModel):
    """登录后的轻量积分响应。"""
    id: int
    account: str
    nickname: str
    points: int


class UserListOut(BaseModel):
    items: list[UserOut]
    total: int
    limit: int
    offset: int


# ---------------------------------------------------------------------------
# 卡片：id 用前端的卡片 id（当前等于卡名），image 暂时留空
# ---------------------------------------------------------------------------
class CardIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(min_length=1, max_length=128, description="卡片 id，前端 config.js 里的唯一标识")
    name: str = Field(min_length=1, max_length=128, description="卡片名字")
    atk: int = Field(default=0, ge=0, description="攻击力")
    defense: int = Field(default=0, ge=0, alias="def", description="防御力（JSON 字段名是 def）")
    attr: str = Field(min_length=1, max_length=8, description="属性：光 / 暗 / 炎 / 水 / 地 / 风 / 神")
    rarity: Literal["N", "R", "SR", "SSR"] = Field(description="稀有度")
    effect: dict[str, Any] | None = Field(default=None, description="专属效果完整配置；无效果卡为 null")
    image: str = Field(default="", max_length=512, description="卡图地址，暂不存图，统一留空")

    @field_validator("id", "name")
    @classmethod
    def strip_text(cls, v: str) -> str:
        return _strip(v)


class CardOut(CardIn):
    created_at: datetime
    updated_at: datetime


class CardImportIn(BaseModel):
    cards: list[CardIn] = Field(min_length=1, max_length=500, description="要写入的卡片列表")


class CardImportOut(BaseModel):
    imported: int = Field(description="本次提交并写入的卡片张数")
    total: int = Field(description="数据库里卡片总数")


class CardListOut(BaseModel):
    items: list[CardOut]
    total: int
    limit: int
    offset: int


# ---------------------------------------------------------------------------
# 玩家卡组：按用户存整份快照，数组元素是前端 config.js 的卡片 id（卡名）
# ---------------------------------------------------------------------------
class DeckSaveIn(BaseModel):
    """整份覆盖写入：卡组 + 备用牌库。前端每次编辑 / 抽卡后调用一次。"""
    deck: list[str] = Field(default_factory=list, max_length=500,
                            description="卡组卡名数组，前端上限 40 张")
    collection: list[str] = Field(default_factory=list, max_length=5000,
                                  description="备用牌库卡名数组（拥有但没编入卡组的卡）")

    @field_validator("deck", "collection")
    @classmethod
    def clean_ids(cls, v: list[str]) -> list[str]:
        return [s.strip() for s in v if isinstance(s, str) and s.strip()]


class DeckOut(BaseModel):
    """exists=false 表示这个用户还没有云端卡组，前端应上传本地存档作为初始化。"""
    user_id: int
    exists: bool
    deck: list[str]
    collection: list[str]
    version: int
    updated_at: str | None = None
