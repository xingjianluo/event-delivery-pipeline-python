"""后台轮询投递 worker：认领到期事件并调用 pipeline。

- 单线程循环，一次只认领处理一个事件；认领、终态写入均为数据库原子事务，
  因此即使人为启动多个 worker，也不会对同一 attempt 重复落终态/副作用；
- 进程崩溃/重启后，留在 PROCESSING 的事件在租约超时（lease_timeout）后
  会被 ``claim_next`` 自动回收（attempt+1 并留痕），不会卡死或丢失；
- 后台处理沿用事件持久化的 correlation_id，跨重启不串号。
"""
from __future__ import annotations

import logging
import threading
import time

from app.pipeline import Pipeline
from app.storage import Storage
from app.tracing import new_correlation_id, use_correlation_id

log = logging.getLogger("event_pipeline.worker")


class Worker:
    def __init__(
        self,
        pipeline: Pipeline,
        storage: Storage,
        poll_interval: float,
        worker_id: str | None = None,
        clock=time.time,
    ):
        self.pipeline = pipeline
        self.storage = storage
        self.poll_interval = poll_interval
        self.worker_id = worker_id or f"worker-{threading.get_ident()}-{id(self):x}"
        self._clock = clock
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def recover_stale(self, now: float | None = None) -> int:
        """统计崩溃/重启遗留的超时租约（它们会在轮询中被自动回收）。"""
        now = self._clock() if now is None else now
        stale_before = now - self.pipeline.settings.lease_timeout
        rows = self.storage.find_stale_leases(stale_before)
        for r in rows:
            log.warning(
                "stale lease detected on startup: event_id=%s cid=%s attempts=%s "
                "leased_at=%s -> will be reclaimed",
                r["event_id"], r["correlation_id"], r["attempts"], r["leased_at"],
            )
        return len(rows)

    def run_forever(self) -> None:
        log.info("worker %s started", self.worker_id)
        while not self._stop.is_set():
            now = self._clock()
            cid = new_correlation_id()
            with use_correlation_id(cid):
                event_id = self.storage.claim_next(now, cid)
            if event_id is None:
                self._stop.wait(self.poll_interval)
                continue
            log.info("worker %s claimed event_id=%s", self.worker_id, event_id)
            try:
                self.pipeline.process_once(event_id, cid, now=now)
            except Exception:
                log.exception("worker loop error on event_id=%s", event_id)
        log.info("worker %s stopped", self.worker_id)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self.run_forever, name=f"event-worker-{self.worker_id}", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
