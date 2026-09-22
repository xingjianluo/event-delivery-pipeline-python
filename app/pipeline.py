"""编排层：组装存储、下游处理器与 worker 池，提供接收/查询/人工处置入口。"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from .config import Settings, settings as default_settings
from .downstream import DownstreamHandler
from .models import (
    AttemptView,
    EventDetail,
    EventStatus,
    EventView,
    FailureKind,
    HistoryItem,
    ReceiveResult,
    StatsView,
    new_correlation_id,
)
from .storage import Storage, utcnow_iso
from .worker import RetryWorkerPool

logger = logging.getLogger("event_pipeline.pipeline")


def compute_content_hash(
    event_type: str, payload: dict[str, Any], idempotency_key: str | None
) -> str:
    """对“业务内容”做稳定哈希：相同事件标识但内容不同 -> 哈希不同 -> 拒绝。"""
    canonical = json.dumps(
        {"type": event_type, "payload": payload, "idempotency_key": idempotency_key},
        sort_keys=True, ensure_ascii=False, separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _row_to_event(row) -> EventView:
    return EventView(
        event_id=row["event_id"],
        type=row["type"],
        payload=json.loads(row["payload"]),
        status=EventStatus(row["status"]),
        idempotency_key=row["idempotency_key"],
        content_hash=row["content_hash"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        failure_kind=FailureKind(row["failure_kind"]),
        last_error_code=row["last_error_code"],
        last_error_detail=row["last_error_detail"],
        receive_correlation_id=row["receive_correlation_id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class EventPipeline:
    def __init__(self, settings: Settings | None = None, *, start_workers: bool = True) -> None:
        self.settings = settings or default_settings
        self.storage = Storage(self.settings)
        self.downstream = DownstreamHandler(
            self.storage,
            transient_sim_attempts=self.settings.transient_simulation_attempts,
        )
        self.pool = RetryWorkerPool(self.storage, self.downstream, self.settings)
        # 重启恢复：解锁崩溃时在途的事件（状态不回退，副作用幂等）。
        recovered = self.storage.recover_stuck(utcnow_iso())
        if recovered:
            logger.warning("recovered %d in-flight events after restart", recovered)
        if start_workers:
            self.pool.start()

    # ---------- 接收 ----------
    def receive(
        self,
        event_id: str,
        event_type: str,
        payload: dict[str, Any],
        idempotency_key: str | None,
        correlation_id: str,
    ) -> ReceiveResult:
        content_hash = compute_content_hash(event_type, payload, idempotency_key)
        now = utcnow_iso()
        token, canonical_id = self.storage.insert_event_if_absent(
            event_id=event_id,
            event_type=event_type,
            payload_json=json.dumps(payload, ensure_ascii=False, sort_keys=True),
            idempotency_key=idempotency_key,
            content_hash=content_hash,
            correlation_id=correlation_id,
            now=now,
        )

        if token == "inserted":
            logger.info(
                "event accepted event=%s recv_cid=%s idempotency_key=%r",
                event_id, correlation_id, idempotency_key,
            )
            self.pool.wake()
            return ReceiveResult(
                accepted=True, event_id=event_id, status=EventStatus.PENDING,
                duplicate=False, correlation_id=correlation_id,
                reason="event accepted for first time",
            )

        existing = self.storage.get_event_row(canonical_id)
        if token == "conflict":
            # 相同标识、不同内容：明确拒绝，绝不覆盖、绝不按重复静默处理。
            detail = (
                "event identifier already exists with DIFFERENT content "
                f"(existing content_hash={existing['content_hash'][:12]}..., "
                f"submitted content_hash={content_hash[:12]}...)"
            )
            self.storage.add_history(
                event_id=canonical_id, stage="CONTENT_CONFLICT_REJECTED",
                from_status=existing["status"], to_status=existing["status"],
                detail=detail, correlation_id=correlation_id, now=utcnow_iso(),
            )
            logger.error(
                "content conflict rejected event=%s recv_cid=%s original_cid=%s",
                canonical_id, correlation_id, existing["receive_correlation_id"],
            )
            return ReceiveResult(
                accepted=False, event_id=canonical_id,
                status=EventStatus(existing["status"]),
                duplicate=False, conflict=True, correlation_id=correlation_id,
                original_correlation_id=existing["receive_correlation_id"],
                reason="same event identifier submitted with inconsistent content; rejected",
            )

        # 真正的重复提交：显式反馈，不再次执行任何处理。
        logger.info(
            "duplicate submission event=%s recv_cid=%s original_cid=%s status=%s",
            canonical_id, correlation_id,
            existing["receive_correlation_id"], existing["status"],
        )
        self.storage.add_history(
            event_id=canonical_id, stage="DUPLICATE_SUBMISSION",
            from_status=existing["status"], to_status=existing["status"],
            detail=f"duplicate submission echoed via cid={correlation_id}; no reprocessing",
            correlation_id=correlation_id, now=utcnow_iso(),
        )
        return ReceiveResult(
            accepted=False, event_id=canonical_id,
            status=EventStatus(existing["status"]),
            duplicate=True, correlation_id=correlation_id,
            original_correlation_id=existing["receive_correlation_id"],
            reason="duplicate submission; original processing result is preserved",
        )

    # ---------- 查询 ----------
    def get_detail(self, event_id: str) -> EventDetail | None:
        row = self.storage.get_event_row(event_id)
        if row is None:
            return None
        attempts = [
            AttemptView(
                attempt_no=r["attempt_no"],
                outcome=r["outcome"],
                http_status=r["http_status"],
                error_code=r["error_code"],
                detail=r["detail"],
                correlation_id=r["correlation_id"],
                happened_at=r["happened_at"],
            )
            for r in self.storage.list_attempts(event_id)
            if r["outcome"] != "RUNNING"
        ]
        lifecycle = [
            HistoryItem(
                seq=r["seq"],
                stage=r["stage"],
                from_status=EventStatus(r["from_status"]) if r["from_status"] else None,
                to_status=EventStatus(r["to_status"]) if r["to_status"] else None,
                detail=r["detail"],
                correlation_id=r["correlation_id"],
                happened_at=r["happened_at"],
            )
            for r in self.storage.list_history(event_id)
        ]
        return EventDetail(
            event=_row_to_event(row),
            attempts_history=attempts,
            lifecycle=lifecycle,
            side_effects=self.storage.count_side_effects(event_id),
        )

    def get_stats(self) -> StatsView:
        return StatsView(**self.storage.stats())

    def list_dead(self) -> list[EventView]:
        return [_row_to_event(r) for r in self.storage.list_dead_events()]

    # ---------- 人工处置 ----------
    def replay_dead(self, event_id: str, correlation_id: str,
                    extra_budget: int | None = None) -> str:
        budget = extra_budget or self.settings.max_attempts
        result = self.storage.replay_dead_event(
            event_id, correlation_id, utcnow_iso(), extra_budget=budget
        )
        if result == "requeued":
            logger.warning(
                "dead event manually re-queued event=%s op_cid=%s",
                event_id, correlation_id,
            )
            self.pool.wake()
        return result

    def shutdown(self) -> None:
        self.pool.stop()
