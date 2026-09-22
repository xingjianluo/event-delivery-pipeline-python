"""模拟下游处理：以本地 side_effects 表代表真实外部副作用。

失败通过 payload 中的字段注入，便于测试：
- payload["simulate"] == "transient"：前 N 次投递瞬时失败，之后成功；
- payload["simulate"] == "business"：始终业务拒绝（4xx 语义，不重试）；
- payload["simulate"] == "unknown"：始终不可归类错误（5xx，不重试，直接死信）；
- 其它情况：成功。

副作用幂等键 = f"{event_id}:done"，同一条事件即使在重启后被重投递，
副作用表 UNIQUE 约束也保证真实世界语义下的“只执行一次”。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from .models import FailureKind
from .storage import utcnow_iso

logger = logging.getLogger("event_pipeline.downstream")


class DownstreamError(Exception):
    """下游失败基类，携带分类与 HTTP 语义状态码。"""

    kind: FailureKind = FailureKind.UNKNOWN
    http_status: int = 500

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


class TransientDownstreamError(DownstreamError):
    kind = FailureKind.TRANSIENT
    http_status = 503


class BusinessRejection(DownstreamError):
    kind = FailureKind.BUSINESS
    http_status = 422


class UnknownDownstreamError(DownstreamError):
    kind = FailureKind.UNKNOWN
    http_status = 500


@dataclass(frozen=True)
class DeliveryResult:
    downstream_ref: str


class DownstreamHandler:
    def __init__(self, storage, transient_sim_attempts: int = 2) -> None:
        self.storage = storage
        self.transient_sim_attempts = max(1, transient_sim_attempts)

    def deliver(
        self,
        event_id: str,
        attempt_no: int,
        event_type: str,
        payload: dict[str, Any],
        correlation_id: str,
    ) -> DeliveryResult:
        """执行一次投递。成功则幂等写入副作用；失败抛出带分类的异常。

        任何未预期的异常统一归为 UNKNOWN，保证事件不会被静默丢弃。
        """
        mode = str(payload.get("simulate", "ok"))
        try:
            if mode == "transient" and attempt_no <= self.transient_sim_attempts:
                raise TransientDownstreamError(
                    "DOWNSTREAM_UNAVAILABLE",
                    f"simulated transient failure on attempt {attempt_no} "
                    f"(recovers after {self.transient_sim_attempts} attempts)",
                )
            if mode == "business":
                raise BusinessRejection(
                    "BUSINESS_RULE_REJECTED",
                    "simulated permanent business rejection; must not be retried",
                )
            if mode == "unknown":
                raise UnknownDownstreamError(
                    "UNCLASSIFIABLE_FAILURE",
                    "simulated failure that cannot be classified",
                )

            # 真实副作用在此发生；用持久化的幂等键代表“外部世界已执行”。
            downstream_ref = f"{event_id}:done"
            if self.storage.side_effect_exists(event_id, downstream_ref):
                # 崩溃恢复场景：副作用此前已生效，只是交付状态未来得及推进。
                # 按幂等成功处理，绝不再执行一次副作用。
                logger.warning(
                    "side effect already applied, treating delivery as idempotent "
                    "success: event=%s attempt=%s cid=%s",
                    event_id, attempt_no, correlation_id,
                )
                return DeliveryResult(downstream_ref=downstream_ref)
            inserted = self.storage.record_side_effect(
                event_id=event_id,
                downstream_ref=downstream_ref,
                correlation_id=correlation_id,
                at=utcnow_iso(),
            )
            if not inserted:
                # 极端并发下的兜底（理论上被 claim 锁排除）。
                logger.warning(
                    "side effect insert raced, idempotent success: event=%s cid=%s",
                    event_id, correlation_id,
                )
            return DeliveryResult(downstream_ref=downstream_ref)
        except DownstreamError:
            raise
        except Exception as exc:  # noqa: BLE001 - 未预期错误必须显式归类
            raise UnknownDownstreamError("UNEXPECTED_EXCEPTION", repr(exc)) from exc
