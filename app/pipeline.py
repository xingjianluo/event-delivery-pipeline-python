"""核心流转：接收去重 + 投递处理 + 分类重试，串起存储/下游/重试/追踪。"""
from __future__ import annotations

import hashlib
import json
import logging
import time

from app.config import Settings
from app.downstream import DownstreamHandler
from app.errors import classify, FailureKind
from app.models import AcceptResult
from app.retry import RetryPolicy
from app.storage import Storage
from app.tracing import new_correlation_id, normalize_trace_id, use_correlation_id

log = logging.getLogger("event_pipeline.pipeline")


class ContentConflict(Exception):
    """同一 event_id 但内容不一致（哈希冲突）。"""

    def __init__(self, event_id: str, existing_hash: str, incoming_hash: str):
        super().__init__(
            f"event_id={event_id} already exists with different content "
            f"(existing={existing_hash}, incoming={incoming_hash})"
        )
        self.existing_hash = existing_hash
        self.incoming_hash = incoming_hash


class InvalidTraceId(ValueError):
    """调用方追踪标识不合法（控制字符/超长）。"""


def content_hash(event_type: str, payload: dict) -> str:
    raw = json.dumps(
        {"event_type": event_type, "payload": payload},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    )
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


class Pipeline:
    def __init__(
        self,
        storage: Storage,
        handler: DownstreamHandler,
        retry: RetryPolicy,
        settings: Settings,
    ):
        self.storage = storage
        self.handler = handler
        self.retry = retry
        self.settings = settings

    # --------------------------------------------------------------- receive
    def receive(
        self,
        event_id: str,
        event_type: str,
        payload: dict,
        now: float | None = None,
        trace_id: str | None = None,
    ) -> AcceptResult:
        """接收一个事件。

        ``trace_id`` 为调用方自己的追踪标识（已由入口规范化）；未提供时服务端生成。
        追踪标识在**首次写入**时持久化，此后接收去重、并发提交、进程重启都不会改变它：
        重复提交即使携带了不同的标识，也返回首次持久化的值（``trace_id_mismatch=True``
        仅作提示），绝不悄悄替换，因此调用方总能对上自己首次带进系统的那个值。
        """
        now = time.time() if now is None else now
        try:
            requested = normalize_trace_id(trace_id)
        except ValueError as exc:
            raise InvalidTraceId(str(exc)) from exc
        chash = content_hash(event_type, payload)
        cid = requested or new_correlation_id()
        created, conflict = self.storage.insert_event_if_absent(
            event_id, event_type, payload, chash, cid, now
        )
        if conflict is not None:
            log.warning(
                "content conflict rejected event_id=%s cid=%s existing=%s incoming=%s",
                event_id, cid, conflict, chash,
            )
            raise ContentConflict(event_id, conflict, chash)
        view = self.storage.get_event(event_id)
        assert view is not None
        mismatch = (not created) and requested is not None and requested != view.correlation_id
        if mismatch:
            log.warning(
                "duplicate submit with different trace_id event_id=%s "
                "persisted=%s incoming=%s -> keeping persisted",
                event_id, view.correlation_id, requested,
            )
        log.info(
            "receive event_id=%s cid=%s duplicate=%s state=%s mismatch=%s",
            event_id, view.correlation_id, not created, view.state.value, mismatch,
        )
        return AcceptResult(
            event_id=event_id, state=view.state, duplicate=not created,
            trace_id=view.correlation_id, correlation_id=view.correlation_id,
            trace_id_mismatch=mismatch,
        )

    # -------------------------------------------------------------- delivery
    def process_once(
        self,
        event_id: str,
        correlation_id: str,
        now: float | None = None,
    ) -> None:
        """处理一个已被认领（PROCESSING）的事件：调用下游并写终态/调度重试。

        调用方（worker）应先 claim_next 得到事件与 attempt；这里重新读取行，
        以存储中的 attempts 为准，确保重启/回收后状态归属不串。
        """
        now = time.time() if now is None else now
        view = self.storage.get_event(event_id)
        if view is None:
            return
        with use_correlation_id(view.correlation_id):
            attempt = view.attempts
            try:
                self.handler.handle(event_id, view.event_type, view.payload)
            except Exception as exc:  # noqa: BLE001 - 任何异常都必须被归类，不允许逃逸丢事件
                kind = classify(exc)
                self._record_failure(event_id, attempt, kind, exc, correlation_id, now)
            else:
                applied = self.storage.finish_success(event_id, attempt, correlation_id, now)
                log.info(
                    "processed event_id=%s cid=%s attempt=%s applied=%s",
                    event_id, view.correlation_id, attempt, applied,
                )

    def _record_failure(
        self,
        event_id: str,
        attempt: int,
        kind: FailureKind,
        exc: BaseException,
        correlation_id: str,
        now: float,
    ) -> None:
        if kind is FailureKind.TRANSIENT:
            delay = self.retry.next_delay(attempt)
            next_at = None if delay is None else now + delay
        elif kind is FailureKind.BUSINESS:
            delay = None  # 业务拒绝：永不重试
            next_at = None
        else:
            delay = None  # 未知错误：保守不重试，进死信待人工处置
            next_at = None
        self.storage.finish_failure(
            event_id, attempt, kind.value, str(exc), next_at, correlation_id, now
        )
        log.warning(
            "failure recorded event_id=%s cid=%s attempt=%s kind=%s retry_after=%s err=%s",
            event_id, self.storage.get_event(event_id).correlation_id, attempt,
            kind.value, delay, exc,
        )
