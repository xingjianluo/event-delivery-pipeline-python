"""领域模型与 API 数据结构。"""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class EventState(str, Enum):
    PENDING = "PENDING"       # 已接收，待投递
    PROCESSING = "PROCESSING" # 已被投递/认领
    SUCCEEDED = "SUCCEEDED"   # 终态：成功（副作用仅产生一次）
    FAILED = "FAILED"         # 终态：业务拒绝，不可重试
    DEAD = "DEAD"             # 终态：重试耗尽/无法归类，可人工处置
    DISCARDED = "DISCARDED"   # 终态：人工处置丢弃


class EventIn(BaseModel):
    event_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    payload: dict


class AcceptResult(BaseModel):
    event_id: str
    state: EventState
    duplicate: bool
    correlation_id: str


class HistoryItem(BaseModel):
    id: int
    event_id: str
    correlation_id: str
    at: float
    from_state: EventState | None
    to_state: EventState
    attempt: int
    reason: str
    actor: str


class EventView(BaseModel):
    event_id: str
    event_type: str
    payload: dict
    content_hash: str
    state: EventState
    attempts: int
    max_attempts: int
    correlation_id: str
    next_attempt_at: float | None
    last_error: str | None
    failure_kind: str | None
    created_at: float
    updated_at: float


class Stats(BaseModel):
    total: int
    by_state: dict[str, int]
    side_effect_count: int
