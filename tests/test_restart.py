"""进程重启恢复：崩溃遗留在 PROCESSING 的事件在租约超时后被回收，继续投递。

断言：不丢事件、不重复副作用、correlation id 不变、attempts 单调递增。
"""
from __future__ import annotations

import logging

from app.config import Settings
from app.downstream import DownstreamHandler
from app.models import EventState
from app.pipeline import Pipeline
from app.retry import RetryPolicy
from app.storage import Storage
from app.tracing import new_correlation_id
from app.worker import Worker


def _fresh_pipeline(settings, handler):
    """模拟进程重启：关闭旧连接，用新的 Storage/Pipeline 指向同一个 DB 文件。"""
    storage = Storage(settings)
    storage.connect()
    pipeline = Pipeline(storage, handler, RetryPolicy(settings), settings)
    return storage, pipeline


def test_recovery_after_crash_reclaims_and_succeeds_once(settings, handler, drive, clock):
    storage, pipeline = _fresh_pipeline(settings, handler)
    event_id = "evt-crash-1"
    pipeline.receive(event_id, "order.created", {"order_id": "o-9"}, now=clock.now())
    cid_before = storage.get_event(event_id).correlation_id

    # worker 认领后、写终态前“崩溃”：事件停留在 PROCESSING
    claimed = storage.claim_next(clock.now(), "w-old")
    assert claimed == event_id
    assert storage.get_event(event_id).attempts == 1
    storage.close()  # 进程退出

    # —— 进程重启 ——
    storage2, pipeline2 = _fresh_pipeline(settings, handler)
    worker2 = Worker(pipeline2, storage2, settings.poll_interval, clock=clock.now)

    # 刚重启时租约未超时，不应回收
    assert worker2.recover_stale(now=clock.now()) == 0
    assert storage2.claim_next(clock.now(), "w-new") is None

    # 租约超时后：检测到 1 个遗留事件并自动回收
    clock.advance(settings.lease_timeout + 0.01)
    assert worker2.recover_stale(now=clock.now()) == 1
    reclaimed = storage2.claim_next(clock.now(), "w-new")
    assert reclaimed == event_id
    v = storage2.get_event(event_id)
    assert v.state is EventState.PROCESSING and v.attempts == 2, "回收后 attempt 递增"
    assert v.correlation_id == cid_before, "重启后关联标识不串号"

    pipeline2.process_once(event_id, "w-new", now=clock.now())
    v = storage2.get_event(event_id)
    assert v.state is EventState.SUCCEEDED
    assert storage2.side_effect_count(event_id) == 1, "崩溃前未完成的副作用只能产生一次"

    logging.getLogger("test").info(
        "recovery event_id=%s cid=%s attempts=%s", event_id, v.correlation_id, v.attempts
    )
    for h in storage2.history(event_id):
        logging.getLogger("test").info(
            "    hist event_id=%s %s->%s attempt=%s actor=%s reason=%s",
            event_id, h.from_state, h.to_state.value, h.attempt, h.actor, h.reason,
        )
    storage2.close()


def test_recovery_preserves_data_and_stats(settings, handler, drive, clock):
    storage, pipeline = _fresh_pipeline(settings, handler)
    # 一条已成功 + 一条崩溃在途 + 一条等待
    pipeline.receive("done", "t", {}, now=clock.now())
    drive(storage, pipeline, new_correlation_id, clock)
    assert storage.get_event("done").state is EventState.SUCCEEDED

    pipeline.receive("stuck", "t", {}, now=clock.now())
    storage.claim_next(clock.now(), "w-old")
    pipeline.receive("waiting", "t", {}, now=clock.now())
    storage.close()

    storage2, pipeline2 = _fresh_pipeline(settings, handler)
    stats_before = storage2.stats()
    assert stats_before["total"] == 3
    assert stats_before["by_state"]["SUCCEEDED"] == 1

    clock.advance(settings.lease_timeout + 1)
    drive(storage2, pipeline2, new_correlation_id, clock, rounds=100)
    stats = storage2.stats()
    assert stats["by_state"]["SUCCEEDED"] == 3, "重启后在途与等待事件都被处理，无丢失"
    assert stats["side_effect_count"] == 3
    storage2.close()
