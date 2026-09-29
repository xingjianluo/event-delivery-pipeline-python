"""领域模型与 API 数据结构。"""
from __future__ import annotations

from enum import Enum
from typing import Literal

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
    # 调用方追踪标识（与 X-Correlation-ID 头等价；两者都给时以头为准）
    trace_id: str | None = None


class AcceptResult(BaseModel):
    event_id: str
    state: EventState
    duplicate: bool
    # 事件实际持久化的追踪标识；correlation_id 为兼容字段，二者永远相同。
    # 重复提交即使携带了不同的标识，也返回首次提交时持久化的值，绝不悄悄替换。
    trace_id: str
    correlation_id: str
    # 本次提交携带的追踪标识与持久化值是否不一致（仅提示，不改变存储）
    trace_id_mismatch: bool = False


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
    batch_id: str | None = None


class EventView(BaseModel):
    event_id: str
    event_type: str
    payload: dict
    content_hash: str
    state: EventState
    attempts: int
    max_attempts: int
    trace_id: str
    correlation_id: str
    next_attempt_at: float | None
    last_error: str | None
    failure_kind: str | None
    created_at: float
    updated_at: float


class TraceView(BaseModel):
    """按调用方追踪标识反查的结果：一次业务操作关联了哪些事件、各走到哪一步。"""
    trace_id: str
    event_count: int
    by_state: dict[str, int]
    events: list[EventView]
    side_effect_count: int


class BatchActionRequest(BaseModel):
    action: Literal["requeue", "discard"]
    # 显式指定要处置的事件；与 filter 至少给一个
    event_ids: list[str] | None = None
    # 按条件圈定：目前支持 state（可多值）与 trace_id；只会圈定可人工处置的状态
    filter: BatchFilter | None = None
    dry_run: bool = False


class BatchFilter(BaseModel):
    state: list[EventState] | None = None
    trace_id: str | None = None


class BatchActionResult(BaseModel):
    batch_id: str
    action: str
    dry_run: bool
    # 匹配到的候选（满足筛选条件且处于可处置状态）
    matched: int
    # 实际发生状态迁移的事件；幂等重复执行时除第一次外均为 0
    changed: list[str]
    unchanged: list[str]
    # 因筛选条件命中但状态不可处置而跳过的事件（已完成的事件永远不会被卷入）
    skipped_terminal: list[str]
    not_found: list[str]


class Stats(BaseModel):
    total: int
    by_state: dict[str, int]
    side_effect_count: int
