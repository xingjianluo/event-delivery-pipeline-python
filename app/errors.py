"""错误分类与领域异常定义。"""
from __future__ import annotations

from enum import Enum


class FailureKind(str, Enum):
    TRANSIENT = "transient"   # 可恢复瞬时错误：允许重试
    BUSINESS = "business"     # 业务性拒绝：不可重试
    UNKNOWN = "unknown"       # 无法归类：不可重试，进死信


class EventError(Exception):
    """领域异常基类。"""

    def __init__(self, message: str, kind: FailureKind):
        super().__init__(message)
        self.kind = kind


class TransientError(EventError):
    def __init__(self, message: str = "transient failure"):
        super().__init__(message, FailureKind.TRANSIENT)


class BusinessRejected(EventError):
    def __init__(self, message: str = "business rejected"):
        super().__init__(message, FailureKind.BUSINESS)


class UnknownFailure(EventError):
    def __init__(self, message: str = "unknown failure"):
        super().__init__(message, FailureKind.UNKNOWN)


class ContentConflictError(Exception):
    """同一事件 ID 但内容不一致。"""


def classify(exc: BaseException) -> FailureKind:
    """把下游异常归类为瞬时/业务/未知。

    - 领域异常自带类别（TRANSIENT / BUSINESS / UNKNOWN）；
    - 任何未预期的异常一律视为 UNKNOWN：保守起见不可重试，进死信，
      由人工处置，绝不静默丢弃。
    """
    if isinstance(exc, EventError):
        return exc.kind
    return FailureKind.UNKNOWN
