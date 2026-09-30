"""按条件批量处置的 fail-closed 边界。

筛选条件圈不出“可人工处置（DEAD/FAILED）”事件时：
- 一个事件都不能动（含与本次筛选无关、追踪标识不同的死信）；
- 不产生任何状态迁移历史、副作用；审计若落库也必须是 changed_count=0 的真实记录；
- 重复提交同一请求、与后台投递并发时同样成立。
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.models import EventState
from app.tracing import new_correlation_id


def _drive_to_terminal(storage, pipeline, clock, rounds=100):
    t = clock.t
    for _ in range(rounds):
        t += 1
        eid = storage.claim_next(t, new_correlation_id())
        if eid:
            pipeline.process_once(eid, new_correlation_id(), now=t)
    clock.t = t


def _seed_mixed_states(settings, storage, pipeline, clock):
    """造齐全部状态；唯一的 DEAD/FAILED 受害者都在 trace grp 下。"""
    pipeline.receive("ok", "t", {}, now=clock.now(), trace_id="grp")
    pipeline.receive("biz", "t", {"simulate": {"mode": "business"}}, now=clock.now(),
                     trace_id="grp")
    pipeline.receive("dead", "t", {"simulate": {"mode": "unknown"}}, now=clock.now(),
                     trace_id="grp")
    pipeline.receive("disc", "t", {"simulate": {"mode": "unknown"}}, now=clock.now(),
                     trace_id="grp")
    pipeline.receive("req", "t", {"simulate": {"mode": "unknown"}}, now=clock.now(),
                     trace_id="grp")
    _drive_to_terminal(storage, pipeline, clock)
    # 尚未驱动的接收：保持 PENDING（在途前）
    pipeline.receive("pen", "t", {}, now=clock.now(), trace_id="grp")
    # 认领但不结束：PROCESSING（投递途中）
    eid = storage.claim_next(clock.t + 1, new_correlation_id())
    assert eid == "pen"
    # 已人工丢弃 / 已重入队（两者都已不是可处置状态）
    assert storage.discard("disc", "seed", clock.t)
    assert storage.requeue("req", "seed", clock.t)
    return ["ok", "biz", "dead", "disc", "req", "pen"]


def _snapshot(storage, ids):
    return {eid: (storage.get_event(eid).state, ) for eid in ids}


def _history_count(storage, ids):
    return {eid: len(storage.history(eid)) for eid in ids}


@pytest.mark.parametrize(
    "flt",
    [
        {"state": ["SUCCEEDED"]},
        {"state": ["PENDING"]},
        {"state": ["PROCESSING"]},
        {"state": ["DISCARDED"]},
        {"state": ["SUCCEEDED", "PENDING", "PROCESSING", "DISCARDED"]},
        {"state": []},  # 空状态列表也不得退化为“全量”
        {"state": ["SUCCEEDED"], "trace_id": "grp"},  # grp 下确有 SUCCEEDED 但不可处置
        {"state": ["DEAD", "FAILED"], "trace_id": "trace-does-not-exist"},
        {"state": ["SUCCEEDED"], "trace_id": "trace-does-not-exist"},
        {"trace_id": "trace-does-not-exist"},
    ],
)
def test_batch_filter_with_no_disposable_match_touches_nothing(
    settings, storage, pipeline, clock, flt
):
    ids = _seed_mixed_states(settings, storage, pipeline, clock)
    before_states = _snapshot(storage, ids)
    before_hist = _history_count(storage, ids)
    before_side = storage.stats()["side_effect_count"]
    assert storage.get_event("dead").state is EventState.DEAD
    assert storage.get_event("biz").state is EventState.FAILED

    with TestClient(create_app(settings, start_worker=False)) as c:
        r = c.post("/events/batch", json={"action": "discard", "filter": flt})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["matched"] == 0
        assert body["changed"] == []
        # 真实审计：即使留行，也必须声明 0 条变更
        audit = c.get(f"/batch-actions/{body['batch_id']}").json()
        assert audit["matched"] == 0 and audit["changed_count"] == 0
        assert audit["changed_ids"] == []

    # 所有事件状态一条不动；与筛选无关、trace 不同也一样（这里全部 trace=grp 即受害者群）
    assert _snapshot(storage, ids) == before_states
    assert storage.get_event("dead").state is EventState.DEAD
    assert storage.get_event("biz").state is EventState.FAILED
    # 没有任何新的状态迁移历史、没有副作用
    assert _history_count(storage, ids) == before_hist
    assert storage.stats()["side_effect_count"] == before_side


def test_batch_filter_unrelated_trace_does_not_swallow_other_dead_letter(
    settings, storage, pipeline, clock
):
    """用户实测复现：只带 SUCCEEDED 的条件，绝不能命中 trace 完全不同的死信。"""
    pipeline.receive("victim", "t", {"simulate": {"mode": "unknown"}}, now=clock.now(),
                     trace_id="trace-victim")
    pipeline.receive("other-ok", "t", {}, now=clock.now(), trace_id="trace-requester")
    _drive_to_terminal(storage, pipeline, clock)
    assert storage.get_event("victim").state is EventState.DEAD
    assert storage.get_event("other-ok").state is EventState.SUCCEEDED

    with TestClient(create_app(settings, start_worker=False)) as c:
        r = c.post("/events/batch", json={
            "action": "discard",
            "filter": {"state": ["SUCCEEDED"], "trace_id": "trace-requester"},
        })
        assert r.status_code == 200
        assert r.json()["changed"] == []

    v = storage.get_event("victim")
    assert v.state is EventState.DEAD, "无关死信被真丢掉了"
    assert v.trace_id == "trace-victim"
    dead_hist = storage.history("victim")
    assert all(h.to_state is not EventState.DISCARDED for h in dead_hist)


def test_batch_dry_run_no_match_writes_no_audit(settings, storage, pipeline, clock):
    _seed_mixed_states(settings, storage, pipeline, clock)
    with TestClient(create_app(settings, start_worker=False)) as c:
        r = c.post("/events/batch", json={
            "action": "discard", "dry_run": True,
            "filter": {"state": ["SUCCEEDED"]},
        })
        assert r.status_code == 200
        body = r.json()
        assert body["matched"] == 0 and body["changed"] == []
        assert c.get(f"/batch-actions/{body['batch_id']}").status_code == 404


def test_batch_filter_resubmit_no_extra_transition(settings, storage, pipeline, clock):
    _seed_mixed_states(settings, storage, pipeline, clock)
    flt = {"state": ["DEAD", "FAILED"], "trace_id": "grp"}
    with TestClient(create_app(settings, start_worker=False)) as c:
        first = c.post("/events/batch", json={"action": "discard", "filter": flt})
        assert sorted(first.json()["changed"]) == ["biz", "dead"]
        # 完全相同的批量请求重复提交：不再有可处置对象
        second = c.post("/events/batch", json={"action": "discard", "filter": flt})
        assert second.json()["matched"] == 0
        assert second.json()["changed"] == []
        third = c.post("/events/batch", json={"action": "discard", "filter": flt})
        assert third.json()["changed"] == []

        a1 = c.get(f"/batch-actions/{first.json()['batch_id']}").json()
        a2 = c.get(f"/batch-actions/{second.json()['batch_id']}").json()
        a3 = c.get(f"/batch-actions/{third.json()['batch_id']}").json()
        assert a1["changed_count"] == 2 and sorted(a1["changed_ids"]) == ["biz", "dead"]
        assert a2["changed_count"] == 0 and a2["changed_ids"] == []
        assert a3["changed_count"] == 0 and a3["changed_ids"] == []

    # 每个事件恰好一次 DISCARDED 迁移，重复提交未产生额外迁移
    for eid in ("biz", "dead"):
        discards = [h for h in storage.history(eid) if h.to_state is EventState.DISCARDED]
        assert len(discards) == 1
    # 其他状态都没被波及
    assert storage.get_event("ok").state is EventState.SUCCEEDED
    assert storage.get_event("disc").state is EventState.DISCARDED
    assert storage.get_event("req").state is EventState.PENDING
    assert storage.get_event("pen").state is EventState.PROCESSING
    assert storage.stats()["side_effect_count"] == 1  # 只有 ok 的一次下游副作用


def test_batch_no_match_concurrent_with_worker(settings, storage, pipeline):
    """后台投递在跑 + 并发重复提交“圈不到可处置事件”的批量：死信永不被丢弃。"""
    import time as _t

    pipeline.receive("bc", "t", {"simulate": {"mode": "unknown"}}, trace_id="tr-bc")

    def worker_rounds():
        end = _t.time() + 3
        while _t.time() < end:
            cid = new_correlation_id()
            eid = storage.claim_next(_t.time(), cid)
            if eid:
                pipeline.process_once(eid, cid)
            else:
                _t.sleep(0.002)

    with TestClient(create_app(settings, start_worker=False)) as c:
        t = threading.Thread(target=worker_rounds, daemon=True)
        t.start()
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = []
            for _ in range(40):
                futs.append(pool.submit(lambda: c.post("/events/batch", json={
                    "action": "discard",
                    "filter": {"state": ["SUCCEEDED"]},
                })))
            results = [f.result() for f in futs]
        t.join(timeout=5)

    for r in results:
        assert r.status_code == 200
        assert r.json()["changed"] == [] and r.json()["matched"] == 0
    v = storage.get_event("bc")
    assert v.state is EventState.DEAD, "只圈 SUCCEEDED 的批量绝不能把并发下产生的死信丢掉"
    assert storage.side_effect_count("bc") == 0


def test_list_events_empty_states_is_fail_closed(storage, pipeline, clock):
    pipeline.receive("d", "t", {"simulate": {"mode": "unknown"}}, now=clock.now(),
                     trace_id="grp")
    _drive_to_terminal(storage, pipeline, clock)
    assert storage.list_events(states=[]) == [], "空 states 不得解释为无状态约束"
    assert {e.event_id for e in storage.list_events(states=["DEAD"])} == {"d"}
