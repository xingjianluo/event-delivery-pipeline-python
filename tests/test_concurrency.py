"""并发压力：多线程同时接收 + 多个投递 worker 并发跑。

断言：每个事件恰好一次副作用；所有事件到达终态；统计 total == 唯一事件数。
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from app.models import EventState
from app.tracing import new_correlation_id
from app.worker import Worker


def test_many_workers_no_double_side_effects(pipeline, storage, settings, clock):
    event_ids = [f"evt-bulk-{i}" for i in range(40)]
    # 一半事件注入一次瞬时失败
    payloads = {
        eid: ({"simulate": {"mode": "transient", "fail_times": 1}} if i % 2 == 0 else {})
        for i, eid in enumerate(event_ids)
    }

    # 多线程并发接收（每个事件也重复提交 3 次，制造重复压力）
    submissions = [(eid, k) for eid in event_ids for k in range(3)]

    def submit(task):
        eid, _ = task
        return pipeline.receive(eid, "bulk", payloads[eid], now=time.time())

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(submit, submissions))
    firsts = [r for r in results if not r.duplicate]
    assert len(firsts) == len(event_ids)

    # 3 个 worker 线程并发投递
    stop = threading.Event()

    def loop():
        while not stop.is_set():
            cid = new_correlation_id()
            eid = storage.claim_next(time.time(), cid)
            if eid is None:
                stop.wait(0.005)
                continue
            pipeline.process_once(eid, cid, now=time.time())

    threads = [threading.Thread(target=loop, daemon=True) for _ in range(3)]
    [t.start() for t in threads]
    deadline = time.time() + 10
    while time.time() < deadline:
        st = storage.stats()
        if st["by_state"]["PENDING"] == 0 and st["by_state"]["PROCESSING"] == 0:
            break
        time.sleep(0.02)
    stop.set()
    [t.join(2) for t in threads]

    stats = storage.stats()
    assert stats["total"] == len(event_ids)
    assert stats["by_state"]["SUCCEEDED"] == len(event_ids)
    for eid in event_ids:
        assert storage.side_effect_count(eid) == 1, f"{eid} 副作用次数必须为1"
        assert pipeline.handler.side_effect_count(eid) == 1, f"{eid} handler 内存计数必须为1"
    assert stats["side_effect_count"] == len(event_ids)
    logging.getLogger("test").info("bulk stats=%s", stats)


def test_worker_thread_recovers_and_exhausts(pipeline, storage, settings):
    # 瞬时失败次数超过上限 -> DEAD，验证后台线程 Worker 也走完整流程
    pipeline.receive("w-dead", "t", {"simulate": {"mode": "transient", "fail_times": 99}},
                     now=time.time())
    worker = Worker(pipeline, storage, poll_interval=0.005)
    worker.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        if storage.get_event("w-dead").state is EventState.DEAD:
            break
        time.sleep(0.02)
    worker.stop()
    v = storage.get_event("w-dead")
    assert v.state is EventState.DEAD and v.attempts == settings.max_attempts
    assert storage.side_effect_count("w-dead") == 0
