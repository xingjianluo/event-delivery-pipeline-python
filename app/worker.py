"""后台投递 worker：领取到期事件 -> 调用下游 -> 按失败分类推进状态。

重试策略（有上限）：
- TRANSIENT（5xx 语义，可恢复）：指数退避重试，总尝试次数 <= max_attempts；
  最后一次仍失败 -> DEAD（RETRIES_EXHAUSTED），可查询、可人工 replay；
- BUSINESS（4xx 语义，业务拒绝）：不重试，首次即 DEAD（MOVED_TO_DEAD）；
- UNKNOWN（无法归类）：不重试，首次即 DEAD；
- 成功：DELIVERED。每次尝试都有独立 attempt 关联标识并完整落历史。
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta

from .config import Settings
from .downstream import DownstreamError
from .models import FailureKind, new_correlation_id
from .storage import utcnow_iso

logger = logging.getLogger("event_pipeline.worker")


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


class RetryWorkerPool:
    def __init__(self, storage, downstream, settings: Settings) -> None:
        self.storage = storage
        self.downstream = downstream
        self.settings = settings
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._threads: list[threading.Thread] = []

    # ---------- 调度 ----------
    def backoff_delay(self, attempts_done: int) -> float:
        delay = self.settings.retry_base_delay * (2 ** max(0, attempts_done - 1))
        return min(delay, self.settings.retry_max_delay)

    def _next_run_at(self, attempts_done: int, now_iso: str) -> str:
        at = _parse_iso(now_iso) + timedelta(seconds=self.backoff_delay(attempts_done))
        return at.isoformat(timespec="microseconds")

    # ---------- 生命周期 ----------
    def start(self) -> None:
        if self._threads:
            return
        self._stop.clear()
        for i in range(self.settings.worker_count):
            t = threading.Thread(
                target=self._run, args=(f"worker-{i + 1}",),
                name=f"edp-worker-{i + 1}", daemon=True,
            )
            t.start()
            self._threads.append(t)
        logger.info("worker pool started: %d workers", self.settings.worker_count)

    def wake(self) -> None:
        """有新事件 / 死信被 replay 时唤醒轮询。"""
        self._wake.set()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        for t in self._threads:
            t.join(timeout=timeout)
        self._threads = []

    # ---------- 核心处理（不依赖线程，可在测试中直接调用） ----------
    def process_one(self, worker_id: str, row) -> bool:
        """处理一条已被本 worker 领取的事件。返回是否完成终态/等待重试。"""
        event_id = row["event_id"]
        attempt_no = row["locked_attempt"]
        max_attempts = row["max_attempts"]
        receive_cid = row["receive_correlation_id"]
        attempt_cid = new_correlation_id("att")
        now = utcnow_iso()

        logger.info(
            "[%s] dispatch start event=%s attempt=%d/%d recv_cid=%s att_cid=%s",
            worker_id, event_id, attempt_no, max_attempts, receive_cid, attempt_cid,
        )
        self.storage.start_attempt(event_id, attempt_no, attempt_cid, now)

        import json

        payload = json.loads(row["payload"])
        try:
            result = self.downstream.deliver(
                event_id=event_id,
                attempt_no=attempt_no,
                event_type=row["type"],
                payload=payload,
                correlation_id=attempt_cid,
            )
        except DownstreamError as exc:
            return self._handle_failure(
                worker_id, row, attempt_no, max_attempts, attempt_cid, exc
            )
        except Exception as exc:  # noqa: BLE001 - 兜底：任何意外都必须可归类
            from .downstream import UnknownDownstreamError

            wrapped = UnknownDownstreamError("UNEXPECTED_EXCEPTION", repr(exc))
            return self._handle_failure(
                worker_id, row, attempt_no, max_attempts, attempt_cid, wrapped
            )

        now = utcnow_iso()
        ok = self.storage.advance_to_delivered(
            event_id=event_id, worker_id=worker_id, attempt_no=attempt_no,
            attempts_done=attempt_no, correlation_id=attempt_cid, now=now,
        )
        if ok:
            logger.info(
                "[%s] DELIVERED event=%s attempt=%d ref=%s att_cid=%s",
                worker_id, event_id, attempt_no, result.downstream_ref, attempt_cid,
            )
        else:
            logger.warning(
                "[%s] delivery advance rejected (already terminal or lock lost): "
                "event=%s attempt=%d att_cid=%s",
                worker_id, event_id, attempt_no, attempt_cid,
            )
        return ok

    def _handle_failure(self, worker_id, row, attempt_no, max_attempts,
                        attempt_cid, exc: DownstreamError) -> bool:
        event_id = row["event_id"]
        now = utcnow_iso()
        kind = exc.kind
        logger.warning(
            "[%s] dispatch failed event=%s attempt=%d/%d kind=%s code=%s att_cid=%s detail=%s",
            worker_id, event_id, attempt_no, max_attempts,
            kind.value, exc.code, attempt_cid, exc.detail,
        )

        if kind is FailureKind.TRANSIENT and attempt_no < max_attempts:
            next_at = self._next_run_at(attempt_no, now)
            ok = self.storage.advance_to_retry(
                event_id=event_id, worker_id=worker_id, attempt_no=attempt_no,
                attempts_done=attempt_no, next_run_at=next_at,
                error_code=exc.code, detail=exc.detail,
                correlation_id=attempt_cid, now=now,
            )
            if ok:
                logger.info(
                    "[%s] RETRYING event=%s next_attempt=%d next_run_at=%s att_cid=%s",
                    worker_id, event_id, attempt_no + 1, next_at, attempt_cid,
                )
            return ok

        exhausted = kind is FailureKind.TRANSIENT and attempt_no >= max_attempts
        ok = self.storage.advance_to_dead(
            event_id=event_id, worker_id=worker_id, attempt_no=attempt_no,
            attempts_done=attempt_no, failure_kind=kind.value,
            outcome=kind.value, http_status=exc.http_status,
            error_code=exc.code, detail=exc.detail,
            correlation_id=attempt_cid, now=now, exhausted=exhausted,
        )
        if ok:
            logger.error(
                "[%s] DEAD event=%s kind=%s exhausted=%s code=%s att_cid=%s "
                "requires manual disposition",
                worker_id, event_id, kind.value, exhausted, exc.code, attempt_cid,
            )
        return ok

    # ---------- 线程循环 ----------
    def _run(self, worker_id: str) -> None:
        while not self._stop.is_set():
            try:
                row = self.storage.claim_next_event(worker_id, utcnow_iso())
                if row is not None:
                    self.process_one(worker_id, row)
                    continue
                self._wake.wait(timeout=self.settings.poll_interval)
                self._wake.clear()
            except Exception:  # noqa: BLE001 - worker 线程绝不因异常退出
                logger.exception("[%s] worker loop error", worker_id)
                self._stop.wait(timeout=self.settings.poll_interval)
