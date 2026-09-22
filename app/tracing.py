"""关联标识（correlation id）：跨请求与后台阶段传递。

- 接收事件时生成一个 correlation id，随事件持久化（events.correlation_id），
  后续所有后台投递、重试、人工处置都沿用同一个 id，因此进程重启后也不会串号；
- 每次 HTTP 请求可携带 X-Correlation-ID 作为调用方侧标识；
- ContextVar 仅用于在同一进程的日志中带出当前处理的关联标识。
"""
from __future__ import annotations

import uuid
from contextvars import ContextVar
from typing import Any

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def new_correlation_id() -> str:
    return "cid_" + uuid.uuid4().hex


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
