"""领域模型与 API 数据结构。"""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field

import app.tracing as tracing


class EventState(str, Enum):
    PENDING = "PENDING"       # 已接收，待投递
    PROCESSING = "PROCESSING" # 已被投递/认领
    SUCCEEDED = "SUCCEEDED"   # 终态：成功（副作用仅产生一次）
    FAILED = "FAILED"         # 终态：业务拒绝，不可重试
    DEAD = "DEAD"             # 终态：重试耗尽/无法归类，可人工处置
    DISCARDED = "DISCARDED"   # 终态：人工处置丢弃


# 允许人工批量/单条处置的状态（其他状态一律跳过或拒绝）
DISPOSABLE_STATES = (EventState.DEAD.value, EventState.FAILED.value)


class EventIn(BaseModel):
    event_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    payload: dict
    # 调用方自带追踪标识（可选）；与 X-Correlation-ID 头等价，头优先。
    # 不做 pydantic 约束，统一交给 tracing.validate_correlation_id 校验，
    # 以便返回语义明确的 400 而不是通用 422。
    correlation_id: str | None = None


class AcceptResult(BaseModel):
    event_id: str
    state: EventState
    duplicate: bool
    # 返回给调用方的追踪标识：首次提交即调用方带进来的原值（归一化后），
    # 重复提交沿用首次持久化的值，绝不悄悄替换。
    correlation_id: str
    # 重复提交且调用方本次带的标识与首次不同时，回显首次持久化的标识，
    # 让调用方能对得上“自己带进来的值”和“系统实际使用的值”。
    submitted_correlation_id: str | None = None
    correlation_id_mismatch: bool = False


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


class EventList(BaseModel):
    """按条件查询的结果：稳定顺序（created_at, event_id），带回筛选条件。"""
    items: list[EventView]
    count: int
    states: list[EventState] | None = None
    correlation_id: str | None = None
    event_type: str | None = None
    limit: int
    offset: int


class BatchDisposeRequest(BaseModel):
    """批量人工处置请求。

    选择器三选一/可组合（取交集）：
    - event_ids：明确指定一批事件；
    - states：按状态筛选（仅 DEAD/FAILED 可被处置，其余即使命中也跳过）；
    - correlation_id：按调用方追踪标识筛选。
    什么都不给（空选择器）一律拒绝，避免误处置整库。
    ``idempotency_key`` 由调用方提供；同一 key 重复提交直接返回首次结果，
    不重复产生副作用、不重复写处置历史。
    """
    event_ids: list[str] | None = None
    states: list[EventState] | None = None
    correlation_id: str | None = None
    reason: str = Field(default="batch disposal", max_length=500)
    idempotency_key: str = Field(min_length=1, max_length=128)


class BatchDisposeResult(BaseModel):
    batch_id: int
    action: str               # "requeue" | "discard"
    idempotency_key: str
    changed: list[str]        # 本次实际发生状态迁移的事件
    skipped: list[str]        # 命中选择器但当前状态不可处置（含已完成/已处置）
    not_found: list[str]      # 显式 event_ids 中不存在的 id
    changed_count: int
    skipped_count: int
    replayed: bool = False    # True 表示这是幂等重放，返回的是首次结果
    at: float


class Stats(BaseModel):
    total: int
    by_state: dict[str, int]
    side_effect_count: int


def normalize_states(states: list[EventState] | None) -> list[EventState] | None:
    if states is None:
        return None
    # 去重并保持稳定顺序
    seen: set[EventState] = set()
    out: list[EventState] = []
    for s in states:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out or None


def validate_optional_correlation_id(value: str | None) -> str | None:
    """对查询/批量接口中的可选 cid 参数复用同一套校验。"""
    return tracing.validate_correlation_id(value)
