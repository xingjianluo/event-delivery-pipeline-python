"""测试辅助：临时库配置、状态等待、可读日志输出。"""
from __future__ import annotations

import logging
import os
import tempfile
import time

from app.config import Settings
from app.models import EventStatus


def make_settings(*, max_attempts: int = 3, workers: int = 4,
                  transient_sim: int = 2, retry_base: float = 0.02) -> tuple[Settings, str]:
    tmp = tempfile.mkdtemp(prefix="edp-test-")
    db_path = os.path.join(tmp, "pipeline.db")
    s = Settings(
        db_path=db_path,
        max_attempts=max_attempts,
        retry_base_delay=retry_base,
        retry_max_delay=1.0,
        worker_count=workers,
        poll_interval=0.02,
        transient_simulation_attempts=transient_sim,
    )
    return s, tmp


def wait_for(pl, event_id, statuses, timeout=10.0, poll=0.03):
    deadline = time.time() + timeout
    detail = None
    while time.time() < deadline:
        detail = pl.get_detail(event_id)
        if detail is not None and detail.event.status in statuses:
            return detail
        time.sleep(poll)
    raise AssertionError(
        f"event {event_id} did not reach {statuses}; "
        f"last={detail.event.status if detail else None}"
    )


def print_trace(pl, event_id: str, title: str) -> None:
    d = pl.get_detail(event_id)
    print(f"\n===== {title}: event={event_id} =====")
    if d is None:
        print("  <not found>")
        return
    print(f"  status={d.event.status.value} attempts={d.event.attempts}/"
          f"{d.event.max_attempts} failure_kind={d.event.failure_kind.value} "
          f"last_error={d.event.last_error_code} side_effects={d.side_effects}")
    print(f"  receive_correlation_id={d.event.receive_correlation_id}")
    for a in d.attempts_history:
        print(f"  attempt#{a.attempt_no} -> {a.outcome} (http={a.http_status}, "
              f"code={a.error_code}) cid={a.correlation_id}")
    for h in d.lifecycle:
        frm = h.from_status.value if h.from_status else "-"
        to = h.to_status.value if h.to_status else "-"
        print(f"  [{h.seq}] {h.happened_at} {frm} -> {to} via {h.stage} "
              f"cid={h.correlation_id} :: {h.detail or ''}")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )
