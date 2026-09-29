"""极端参数下的有限收敛：异常循环必须进入可查询、可人工处理的终态，
不能无限重认领，也不能状态回退。
"""
from __future__ import annotations

import pytest

from app.config import Settings, validate_settings
from app.downstream import DownstreamHandler
from app.models import EventState
from app.pipeline import Pipeline
from app.retry import RetryPolicy
from app.storage import Storage
from app.tracing import new_correlation_id


def _pipeline(settings):
    st = Storage(settings)
    st.connect()
    pl = Pipeline(st, DownstreamHandler(), RetryPolicy(settings), settings)
    return st, pl


def test_max_attempts_one_immediate_dead_no_retry(tmp_db):
    s = Settings(db_path=tmp_db, max_attempts=1, lease_timeout=30.0,
                 base_backoff=0.01, poll_interval=0.01)
    st, pl = _pipeline(s)
    pl.receive("m1", "t", {"simulate": {"mode": "transient", "fail_times": 99}}, trace_id="tr1")
    cid = new_correlation_id()
    assert st.claim_next(1.0, cid) == "m1"
    pl.process_once("m1", cid, now=1.0)
    v = st.get_event("m1")
    assert v.state is EventState.DEAD and v.attempts == 1
    assert v.trace_id == "tr1"
    # 没有任何到期事件可再认领
    assert st.claim_next(1000.0, new_correlation_id()) is None
    st.close()


def test_zero_lease_timeout_crash_loop_is_quarantined(tmp_db):
    """lease_timeout=0 + worker 每轮都在写终态前崩溃：认领循环必须被上限截断，
    事件进入 DEAD 终态而不是被无限重认领。"""
    s = Settings(db_path=tmp_db, max_attempts=3, lease_timeout=0.0,
                 base_backoff=0.0, poll_interval=0.001)
    st, pl = _pipeline(s)
    pl.receive("loop", "t", {}, trace_id="tr-loop")

    # 模拟 worker 反复：认领 -> 还没写终态就“崩溃”（租约立即超时）-> 被回收
    claims = 0
    for t in range(1, 20):
        eid = st.claim_next(float(t), f"w{t}")
        if eid is None:
            break
        claims += 1
        # 不调用 process_once：进程在终态前被杀
    v = st.get_event("loop")
    assert v.state is EventState.DEAD, \
        "崩溃循环必须收敛到可查询、可人工处理的 DEAD，而不是卡在 PROCESSING"
    assert v.attempts == s.max_attempts, "认领次数不得超过上限"
    assert claims <= s.max_attempts, "不会无限重认领"
    assert v.trace_id == "tr-loop", "追踪标识在回收/隔离中不变"

    # 终态可人工处置，requeue 后 attempts 清零可再投递
    assert st.requeue("loop", "ops", 100.0) is True
    v2 = st.get_event("loop")
    assert v2.state is EventState.PENDING and v2.attempts == 0
    cid = new_correlation_id()
    assert st.claim_next(101.0, cid) == "loop"
    pl.process_once("loop", cid, now=101.0)
    assert st.get_event("loop").state is EventState.SUCCEEDED
    assert st.side_effect_count("loop") == 1
    st.close()


def test_stale_reclaim_still_honors_retry_budget(tmp_db):
    """正常崩溃（未到上限）仍会被回收重投，attempts 单调递增；最后一次回收
    若已达上限则隔离，整个过程状态不回退。"""
    s = Settings(db_path=tmp_db, max_attempts=2, lease_timeout=0.0,
                 base_backoff=0.0, poll_interval=0.001)
    st, pl = _pipeline(s)
    pl.receive("rec", "t", {"simulate": {"mode": "transient", "fail_times": 99}},
               trace_id="tr-rec")

    # 第一次认领后崩溃 -> 回收（attempt 2，最后一次机会仍真正执行）处理失败 -> DEAD
    assert st.claim_next(1.0, "w1") == "rec"
    assert st.get_event("rec").attempts == 1
    # 崩溃后立即回收（lease_timeout=0），attempts 仍未超上限，允许再投一次
    assert st.claim_next(2.0, "w2") == "rec"
    assert st.get_event("rec").attempts == 2
    pl.process_once("rec", "w2", now=2.0)
    v = st.get_event("rec")
    assert v.state is EventState.DEAD and v.attempts == 2
    # 此后再无候选
    assert st.claim_next(3.0, "w3") is None
    states = [h.to_state for h in st.history("rec")]
    assert states[-1] is EventState.DEAD

    # 若最后一次尝试也崩溃（无人写终态），下一轮扫描把它隔离到 DEAD（attempts 不超上限）
    pl.receive("rec2", "t", {}, trace_id="tr-rec2")
    assert st.claim_next(10.0, "a") == "rec2"   # attempt 1 -> 崩溃
    assert st.claim_next(11.0, "b") == "rec2"   # attempt 2 -> 崩溃
    assert st.claim_next(12.0, "c") is None     # 已隔离
    v2 = st.get_event("rec2")
    assert v2.state is EventState.DEAD and v2.attempts == 2
    st.close()


def test_terminal_and_failure_results_never_regress_state(tmp_db):
    """迟到/失效的处理结果不能把已被回收或已处置的事件状态往回写。"""
    s = Settings(db_path=tmp_db, max_attempts=2, lease_timeout=0.0,
                 base_backoff=0.0, poll_interval=0.001)
    st, pl = _pipeline(s)
    pl.receive("fence", "t", {"simulate": {"mode": "transient", "fail_times": 99}},
               trace_id="tr-fence")

    cid1 = "w1"
    assert st.claim_next(1.0, cid1) == "fence"  # attempt=1，租约随即超时
    cid2 = "w2"
    assert st.claim_next(2.0, cid2) == "fence"  # attempt=2，旧租约失效

    # 旧 worker（attempt=1）迟到的成功/失败结果必须被 attempt 围栏拒绝
    assert st.finish_success("fence", 1, cid1, 3.0) is False
    assert st.finish_failure("fence", 1, "transient", "late", None, cid1, 3.0) is False
    v = st.get_event("fence")
    assert v.state is EventState.PROCESSING and v.attempts == 2, "状态不得回退"
    assert st.side_effect_count("fence") == 0

    # 当前属主（attempt=2，已到上限）瞬态失败：重试耗尽 -> DEAD，状态不回退
    pl.process_once("fence", cid2, now=3.0)
    assert st.get_event("fence").state is EventState.DEAD
    assert st.side_effect_count("fence") == 0
    st.close()


def test_invalid_settings_rejected_fast():
    with pytest.raises(ValueError):
        validate_settings(Settings(db_path=":memory:", max_attempts=0))
    with pytest.raises(ValueError):
        validate_settings(Settings(db_path=":memory:", lease_timeout=-1))
    with pytest.raises(ValueError):
        validate_settings(Settings(db_path=":memory:", base_backoff=-1))
    with pytest.raises(ValueError):
        validate_settings(Settings(db_path=":memory:", backoff_factor=0.5))
    with pytest.raises(ValueError):
        validate_settings(Settings(db_path=":memory:", poll_interval=0))
