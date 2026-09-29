"""关联标识（correlation id）：跨请求与后台阶段传递。

- 调用方可在提交事件时自带追踪标识（HTTP 头 ``X-Correlation-ID`` 或请求体
  ``correlation_id`` 字段）；未提供时由服务端生成一个 ``cid_`` 前缀的 id。
  该值在接收时校验后随事件持久化（events.correlation_id），后续接收确认、
  落库、后台投递、重试回收、人工处置与历史查询整条链路都沿用同一个值，
  进程重启、重复提交、并发提交都不会被替换或串号；
- ContextVar 仅用于在同一进程的日志中带出当前处理的关联标识（后台处理沿用
  事件持久化的 id，而不是 worker 自己生成的临时 id）。
"""
from __future__ import annotations

import re
import uuid
from contextvars import ContextVar
from typing import Any

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)

# 允许调用方追踪标识使用的字符集：与 HTTP 头/JSON 都安全兼容的可见 ASCII，
# 不含空白，长度受限，保证原样存储、原样返回、可在日志/SQL 中安全使用。
_CORRELATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:\-]{0,127}$")


class InvalidCorrelationId(ValueError):
    """调用方提供的追踪标识不合法（为空、超长或含非法字符）。"""


def new_correlation_id() -> str:
    return "cid_" + uuid.uuid4().hex


def validate_correlation_id(value: str | None) -> str | None:
    """校验并归一化调用方追踪标识。

    - None：调用方未提供，由上层决定是否生成；
    - 去掉首尾空白后校验：长度 1..128，仅允许字母/数字/``. _ : -``，
      且首字符为字母或数字（避免 ``-``/``:`` 这类容易和分隔符混淆的开头）。
    校验失败抛 InvalidCorrelationId，服务端不会悄悄替换成另一个值。
    """
    if value is None:
        return None
    cid = value.strip()
    if not _CORRELATION_ID_RE.match(cid):
        raise InvalidCorrelationId(
            "correlation_id must be 1-128 chars of letters/digits/'.', '_', ':', '-' "
            "and start with a letter or digit"
        )
    return cid


def resolve_correlation_id(value: str | None) -> str:
    """校验调用方标识；未提供时生成服务端 id。返回最终持久化使用的值。"""
    cid = validate_correlation_id(value)
    return cid if cid is not None else new_correlation_id()


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
