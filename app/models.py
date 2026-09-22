"""领域模型：事件状态、失败分类与 API 数据结构。"""
from __future__ import annotations

import enum
import uuid
from typing import Any

from pydantic import BaseModel, Field


class EventStatus(str, enum.Enum):
    """事件生命周期状态。

    PENDING  -> RETRYING -> DELIVERED
                         -> DEAD
    PENDING  -> DEAD（业务拒绝 / 不可归类错误，首次即终态）
    PENDING  -> DELIVERED
    """

    PENDING = "PENDING"
    RETRYING = "RETRYING"
    DELIVERED = "DELIVERED"
    DEAD = "DEAD"


TERMINAL_STATUSES = frozenset({EventStatus.DELIVERED, EventStatus.DEAD})


class FailureKind(str, enum.Enum):
    """失败分类。"""

    NONE = "NONE"            # 尚未失败 / 成功
    TRANSIENT = "TRANSIENT"  # 可恢复瞬时错误（自动重试）
    BUSINESS = "BUSINESS"    # 业务性拒绝（不重试，直接终态）
    UNKNOWN = "UNKNOWN"      # 无法归类（不重试，进入死信）


class EventIn(BaseModel):
    """事件提交请求体。event_id 由调用方提供以支持幂等去重。"""

    event_id: str = Field(min_length=1, max_length=128)
    type: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, max_length=200)


class ReceiveResult(BaseModel):
    accepted: bool
    event_id: str
    status: EventStatus
    duplicate: bool
    conflict: bool = False
    correlation_id: str
    """本次请求的关联标识（回显 X-Correlation-ID 或新生成）。"""
    original_correlation_id: str | None = None
    """重复提交时，首次接收该事件所用的关联标识，便于调用方对账。"""
    reason: str | None = None


class AttemptView(BaseModel):
    attempt_no: int
    outcome: str            # DELIVERED / TRANSIENT / BUSINESS / UNKNOWN
    http_status: int | None = None
    error_code: str | None = None
    detail: str | None = None
    correlation_id: str
    happened_at: str


class EventView(BaseModel):
    event_id: str
    type: str
    payload: dict[str, Any]
    status: EventStatus
    idempotency_key: str | None
    content_hash: str
    attempts: int
    max_attempts: int
    failure_kind: FailureKind
    last_error_code: str | None
    last_error_detail: str | None
    receive_correlation_id: str
    created_at: str
    updated_at: str


class HistoryItem(BaseModel):
    seq: int
    stage: str
    from_status: EventStatus | None
    to_status: EventStatus | None
    detail: str | None
    correlation_id: str
    happened_at: str


class EventDetail(BaseModel):
    event: EventView
    attempts_history: list[AttemptView]
    lifecycle: list[HistoryItem]
    side_effects: int


class StatsView(BaseModel):
    total: int
    pending: int
    retrying: int
    delivered: int
    dead: int
    total_attempts: int
    transient_failures: int
    business_rejections: int
    unknown_failures: int
    side_effects: int


def new_correlation_id(prefix: str = "cid") -> str:
    return f"{prefix}_{uuid.uuid4().hex}"
