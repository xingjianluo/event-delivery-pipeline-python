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
from app.tracing import (
    resolve_correlation_id,
    use_correlation_id,
    validate_correlation_id,
)

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
        correlation_id: str | None = None,
    ) -> AcceptResult:
        """接收一个事件。

        ``correlation_id`` 为调用方自带追踪标识：
        - 首次提交：校验后原样作为该事件的关联标识持久化（不提供则服务端生成）；
        - 重复提交：**沿用首次持久化的标识**，绝不被本次提交悄悄替换；若本次
          带了不同的值，在响应中以 ``correlation_id_mismatch`` 明确告知，同时
          回显本次提交值与首次值，让调用方能够对得上账。
        """
        now = time.time() if now is None else now
        submitted = validate_correlation_id(correlation_id)
        chash = content_hash(event_type, payload)
        # 首次写入使用调用方标识（缺省时生成）；重复提交沿用行上的既有标识
        cid = resolve_correlation_id(submitted)
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
        mismatch = bool(submitted is not None and submitted != view.correlation_id)
        log.info(
            "receive event_id=%s cid=%s submitted_cid=%s duplicate=%s mismatch=%s state=%s",
            event_id, view.correlation_id, submitted, not created, mismatch, view.state.value,
        )
        return AcceptResult(
            event_id=event_id, state=view.state, duplicate=not created,
            correlation_id=view.correlation_id,
            submitted_correlation_id=submitted if mismatch else None,
            correlation_id_mismatch=mismatch,
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
