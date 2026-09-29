"""极端参数下的有限收敛：租约超时=0、重试上限极小、退避=0、崩溃循环。

异常循环必须在有限次数内落到可查询、可人工处理的终态，不能无限重新认领，
attempts/状态也不得回退。
"""
from __future__ import annotations

import threading
import time

from app.config import Settings
from app.downstream import DownstreamHandler
from app.models import EventState
from app.pipeline import Pipeline
from app.retry import RetryPolicy
from app.storage import Storage
from app.tracing import new_correlation_id
from app.worker import Worker


def _fresh(settings, handler=None):
    s = Storage(settings)
    s.connect()
    p = Pipeline(s, handler or DownstreamHandler(), RetryPolicy(settings), settings)
    return s, p


def test_zero_lease_crash_loop_forces_dead_within_max_attempts(tmp_path):
    # lease_timeout=0：任何 PROCESSING 在下一轮立刻被视为超时；
    # max_attempts=3：回收两次后（attempt=3）必须强制 DEAD，不再投递。
    settings = Settings(
        db_path=str(tmp_path / "c.db"), lease_timeout=0.0, max_attempts=3,
        base_backoff=0.0, backoff_factor=2.0, poll_interval=0.0,
    )
    storage, pipeline = _fresh(settings)

    class CrashHandler(DownstreamHandler):
        """模拟每次投递时进程都在写终态前死掉：什么都不返回（由测试驱动认领）。"""
        def handle(self, event_id, event_type, payload):  # type: ignore[override]
            raise AssertionError("被强制收敛路径不应真正调用下游")

    storage2, pipeline2 = _fresh(settings, CrashHandler())
    pipeline2.receive("loop", "t", {}, now=0.0, correlation_id="trace-loop")

    # 第 1、2、3 轮：认领 attempt 1、两次回收 attempt 2/3，每次都在写终态前崩
    assert storage2.claim_next(1.0, "w1") == "loop"
    v1 = storage2.get_event("loop")
    assert v1.attempts == 1 and v1.state is EventState.PROCESSING
    assert storage2.claim_next(2.0, "w2") == "loop"  # 回收 -> 2（reclaim_count=1）
    assert storage2.get_event("loop").attempts == 2
    assert storage2.claim_next(3.0, "w3") == "loop"  # 回收 -> 3（reclaim_count=2）
    assert storage2.get_event("loop").attempts == 3

    # 第 4 轮：3 次投递机会已全部用在崩溃循环上，不再投递，直接强制 DEAD
    assert storage2.claim_next(4.0, "w4") is None
    v = storage2.get_event("loop")
    assert v.state is EventState.DEAD
    assert v.attempts == 3, "attempts 用满即止，绝不超过 max_attempts"
    hist = storage2.history("loop")
    assert hist[-1].to_state is EventState.DEAD
    assert "exhausted" in hist[-1].reason
    # 之后无论再轮询多少轮，终态不回退、不重新认领
    for t in range(5, 20):
        assert storage2.claim_next(float(t), f"w{t}") is None
    assert storage2.get_event("loop").state is EventState.DEAD
    assert storage2.get_event("loop").attempts == 3
    storage2.close()


def test_max_attempts_one_succeeds_or_dead_without_reclaim_loops(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "o.db"), lease_timeout=0.0, max_attempts=1,
        base_backoff=0.0, poll_interval=0.0,
    )
    storage, pipeline = _fresh(settings)
    # 成功路径：一次成功
    pipeline.receive("ok", "t", {}, now=0.0, correlation_id="c-ok")
    assert storage.claim_next(1.0, "w") == "ok"
    pipeline.process_once("ok", "w", now=1.0)
    assert storage.get_event("ok").state is EventState.SUCCEEDED

    # 持续瞬时失败路径：attempt=1 即上限，直接 DEAD，不进入 PENDING 重试
    pipeline.receive("bad", "t", {"simulate": {"mode": "transient", "fail_times": 99}},
                     now=0.0, correlation_id="c-bad")
    assert storage.claim_next(2.0, "w") == "bad"
    pipeline.process_once("bad", "w", now=2.0)
    v = storage.get_event("bad")
    assert v.state is EventState.DEAD and v.attempts == 1
    # 崩溃回收路径：PROCESSING(attempt=1) 超时后不得再投递，直接 DEAD
    pipeline.receive("crash", "t", {}, now=0.0, correlation_id="c-crash")
    assert storage.claim_next(3.0, "w") == "crash"  # 唯一一次投递机会
    assert storage.get_event("crash").attempts == 1
    # 越过最小处理宽限后回收：唯一一次机会也已崩溃 -> 直接 DEAD，不再投递
    assert storage.claim_next(3.0 + settings.min_lease_grace + 0.01, "w") is None
    assert storage.get_event("crash").state is EventState.DEAD
    assert storage.get_event("crash").attempts == 1
    storage.close()


def test_zero_backoff_retries_happen_immediately_and_converge(tmp_path):
    # base_backoff=0：失败后立刻可重试；持续失败仍严格在 max_attempts 次后 DEAD
    settings = Settings(
        db_path=str(tmp_path / "z.db"), lease_timeout=30.0, max_attempts=4,
        base_backoff=0.0, poll_interval=0.0,
    )
    storage, pipeline = _fresh(settings)
    pipeline.receive("z", "t", {"simulate": {"mode": "transient", "fail_times": 99}},
                     now=0.0, correlation_id="c-z")
    for tick in range(1, 6):
        eid = storage.claim_next(float(tick), f"w{tick}")
        if eid:
            pipeline.process_once(eid, f"w{tick}", now=float(tick))
    v = storage.get_event("z")
    assert v.state is EventState.DEAD and v.attempts == 4
    # 每次认领 attempt 严格 +1，历史中 PROCESSING 恰好 4 次，无回退
    procs = [h for h in storage.history("z") if h.to_state is EventState.PROCESSING]
    assert [h.attempt for h in procs] == [1, 2, 3, 4]
    storage.close()


def test_background_worker_converges_under_extreme_params(tmp_path):
    """真实后台线程 + 零租约 + 零退避：坏事件必须收敛 DEAD，好事件照常成功。"""
    settings = Settings(
        db_path=str(tmp_path / "w.db"), lease_timeout=0.0, max_attempts=2,
        base_backoff=0.0, poll_interval=0.005, worker_concurrency=4,
    )
    storage = Storage(settings)
    storage.connect()
    pipeline = Pipeline(storage, DownstreamHandler(), RetryPolicy(settings), settings)
    pipeline.receive("bad", "t", {"simulate": {"mode": "transient", "fail_times": 99}},
                     correlation_id="c-w")
    for i in range(5):
        pipeline.receive(f"good-{i}", "t", {}, correlation_id=f"c-g{i}")

    worker = Worker(pipeline, storage, poll_interval=0.005, concurrency=4)
    worker.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        st = storage.stats()
        if st["by_state"]["DEAD"] >= 1 and st["by_state"]["SUCCEEDED"] == 5 \
                and st["by_state"]["PENDING"] == 0 and st["by_state"]["PROCESSING"] == 0:
            break
        time.sleep(0.02)
    worker.stop()
    st = storage.stats()
    assert st["by_state"]["DEAD"] == 1
    assert st["by_state"]["SUCCEEDED"] == 5
    assert st["side_effect_count"] == 5
    v = storage.get_event("bad")
    assert v.state is EventState.DEAD and v.attempts == 2 and v.correlation_id == "c-w"
    storage.close()
