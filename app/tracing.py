"""关联标识（correlation / trace id）：跨请求与后台阶段传递。

- 调用方提交事件时可携带自己的追踪标识（``X-Correlation-ID`` 头或请求体
  ``trace_id`` 字段）；它会原样持久化在事件行上（events.correlation_id），
  接收、落库、后台投递、重试、人工处置与历史查询全程沿用，重启也不变；
- 一次业务操作可以包含多个事件，它们允许共用同一个追踪标识，因此该列**非唯一**、
  带索引，支持按标识反查关联事件；
- 调用方未提供时，服务端生成一个 ``cid_`` 前缀的标识，保证该字段始终可查；
- ContextVar 仅用于在同一进程的日志中带出当前处理的关联标识；后台每轮投递另用
  一次性 cid 记录 actor/lease_owner，与事件持久化的追踪标识互不覆盖。
"""
from __future__ import annotations

import uuid
from contextvars import ContextVar
from typing import Any

TRACE_ID_MAX_LENGTH = 128

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def new_correlation_id() -> str:
    return "cid_" + uuid.uuid4().hex


def new_batch_id() -> str:
    return "batch_" + uuid.uuid4().hex


def normalize_trace_id(raw: str | None) -> str | None:
    """校验并规范化调用方追踪标识。

    - 去首尾空白；为空视为未提供（返回 None，由服务端生成）；
    - 仅允许可打印字符（拒绝控制字符，避免日志注入/串号），长度上限 128；
    - 不合法时抛 ``ValueError``，由 API 层转成 400。
    """
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None
    if len(value) > TRACE_ID_MAX_LENGTH:
        raise ValueError(f"trace_id too long (max {TRACE_ID_MAX_LENGTH} chars)")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        raise ValueError("trace_id must not contain control characters")
    return value


def current_correlation_id() -> str | None:
    return _correlation_id.get()


class _Bind:
    def __init__(self, token: Any):
        self._token = token

    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc: object) -> None:
        _correlation_id.reset(self._token)


def use_correlation_id(cid: str) -> _Bind:
    token = _correlation_id.set(cid)
    return _Bind(token)
