"""按条件筛选 + 批量人工处置：幂等、可审计、不卷入已完成事件。"""
from __future__ import annotations

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


def test_batch_discard_by_state_only_touches_dead_failed(settings, storage, pipeline, clock):
    # 1 成功、1 业务失败、1 未知进死信、1 重新入队等待 —— 后两者用于批量
    pipeline.receive("ok", "t", {}, now=clock.now(), trace_id="grp")
    pipeline.receive("biz", "t", {"simulate": {"mode": "business"}}, now=clock.now(), trace_id="grp")
    pipeline.receive("unk", "t", {"simulate": {"mode": "unknown"}}, now=clock.now(), trace_id="grp")
    _drive_to_terminal(storage, pipeline, clock)
    # 驱动结束后再接收一个事件，保持 PENDING，不应被批量卷入
    pipeline.receive("pending", "t", {}, now=clock.now(), trace_id="grp2")
    assert storage.get_event("ok").state is EventState.SUCCEEDED
    assert storage.get_event("pending").state is EventState.PENDING

    res = storage.batch_dispose(
        action="discard",
        event_ids=["biz", "unk", "ok", "pending", "missing"],
        correlation_id="ops-1", batch_id="batch-test-1",
        selector='{"event_ids":["biz","unk","ok","pending","missing"]}',
        dry_run=False, now=clock.now(),
    )
    assert sorted(res["changed"]) == ["biz", "unk"]
    assert sorted(res["skipped_terminal"]) == ["ok", "pending"], "成功/在途事件绝不卷入"
    assert res["not_found"] == ["missing"]

    assert storage.get_event("biz").state is EventState.DISCARDED
    assert storage.get_event("unk").state is EventState.DISCARDED
    assert storage.get_event("ok").state is EventState.SUCCEEDED
    assert storage.side_effect_count("ok") == 1

    # 审计落库，历史迁移带 batch_id
    audit = storage.get_batch("batch-test-1")
    assert audit is not None and audit["action"] == "discard"
    assert sorted(audit["changed_ids"]) == ["biz", "unk"]
    for eid in ("biz", "unk"):
        tagged = [h for h in storage.history(eid) if h.batch_id == "batch-test-1"]
        assert tagged and tagged[-1].to_state is EventState.DISCARDED
        assert tagged[-1].actor == "manual:ops-1"


def test_batch_is_idempotent(settings, storage, pipeline, clock):
    pipeline.receive("d1", "t", {"simulate": {"mode": "unknown"}}, now=clock.now())
    pipeline.receive("d2", "t", {"simulate": {"mode": "unknown"}}, now=clock.now())
    _drive_to_terminal(storage, pipeline, clock)

    ids = ["d1", "d2"]
    first = storage.batch_dispose(
        action="discard", event_ids=ids, correlation_id="ops",
        batch_id="batch-ida", selector="ids", dry_run=False, now=clock.now(),
    )
    assert sorted(first["changed"]) == ["d1", "d2"]

    # 第二次：没有可处置对象，不产生新迁移/新历史/新副作用
    second = storage.batch_dispose(
        action="discard", event_ids=ids, correlation_id="ops",
        batch_id="batch-idb", selector="ids", dry_run=False, now=clock.now(),
    )
    assert second["changed"] == [] and second["matched"] == 0
    assert sorted(second["skipped_terminal"]) == ["d1", "d2"]
    # 第二次也留审计（changed_count=0），但事件历史没有新增 DISCARDED 迁移
    assert storage.get_batch("batch-idb")["changed_count"] == 0
    for eid in ids:
        discards = [h for h in storage.history(eid) if h.to_state is EventState.DISCARDED]
        assert len(discards) == 1
    assert storage.stats()["side_effect_count"] == 0


def test_batch_requeue_then_delivered_once(settings, storage, pipeline, clock):
    from app.downstream import DownstreamHandler
    from app.pipeline import Pipeline
    from app.retry import RetryPolicy

    class FixedHandler(DownstreamHandler):
        def handle(self, event_id, event_type, payload):  # type: ignore[override]
            # 模拟运维修复下游：忽略原 unknown 脚本，直接成功
            with self._lock:
                self.side_effects[event_id] = self.side_effects.get(event_id, 0) + 1

    pipeline.receive("rq", "t", {"simulate": {"mode": "unknown"}}, now=clock.now())
    _drive_to_terminal(storage, pipeline, clock)
    assert storage.get_event("rq").state is EventState.DEAD

    res = storage.batch_dispose(
        action="requeue", event_ids=["rq"], correlation_id="ops",
        batch_id="batch-rq", selector="ids", dry_run=False, now=clock.now(),
    )
    assert res["changed"] == ["rq"]
    v = storage.get_event("rq")
    assert v.state is EventState.PENDING and v.attempts == 0

    fixed = Pipeline(storage, FixedHandler(), RetryPolicy(settings), settings)
    _drive_to_terminal(storage, fixed, clock)
    assert storage.get_event("rq").state is EventState.SUCCEEDED
    assert storage.side_effect_count("rq") == 1


def test_batch_dry_run_changes_nothing(settings, storage, pipeline, clock):
    pipeline.receive("dr", "t", {"simulate": {"mode": "unknown"}}, now=clock.now())
    _drive_to_terminal(storage, pipeline, clock)
    res = storage.batch_dispose(
        action="discard", event_ids=["dr"], correlation_id="ops",
        batch_id="batch-dry", selector="ids", dry_run=True, now=clock.now(),
    )
    assert res["changed"] == ["dr"] and res["dry_run"] is True
    assert storage.get_event("dr").state is EventState.DEAD, "dry-run 不得改变状态"
    assert storage.get_batch("batch-dry") is None, "dry-run 不写审计"


def test_batch_endpoints_filter_and_validation(settings, storage, pipeline, clock):
    pipeline.receive("t-a", "t", {"simulate": {"mode": "unknown"}}, now=clock.now(),
                     trace_id="trace-x")
    pipeline.receive("t-b", "t", {"simulate": {"mode": "business"}}, now=clock.now(),
                     trace_id="trace-x")
    pipeline.receive("t-c", "t", {}, now=clock.now(), trace_id="trace-x")
    _drive_to_terminal(storage, pipeline, clock)

    with TestClient(create_app(settings, start_worker=False)) as c:
        # 空 selector -> 400
        assert c.post("/events/batch", json={"action": "discard"}).status_code == 400

        # filter 只圈定 DEAD/FAILED（即使 filter 里写了 SUCCEEDED 也不会卷入成功事件）
        r = c.post("/events/batch", json={
            "action": "discard",
            "filter": {"state": ["DEAD", "FAILED", "SUCCEEDED"], "trace_id": "trace-x"},
        })
        assert r.status_code == 200
        assert sorted(r.json()["changed"]) == ["t-a", "t-b"]
        assert storage.get_event("t-c").state is EventState.SUCCEEDED
        assert c.get(f"/batch-actions/{r.json()['batch_id']}").status_code == 200

        # GET /events 条件筛选
        listed = c.get("/events", params=[("state", "DISCARDED")]).json()
        assert {e["event_id"] for e in listed} == {"t-a", "t-b"}
        by_trace = c.get("/events", params={"trace_id": "trace-x"}).json()
        assert len(by_trace) == 3


def test_batch_concurrent_with_worker_and_resubmit(settings, storage, pipeline):
    """批量 discard 与后台投递/重复提交并发：不重复副作用、不报错、终态稳定。"""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    pipeline.receive("bc", "t", {"simulate": {"mode": "unknown"}}, trace_id="tr-bc")

    def worker_rounds():
        import time as _t
        end = _t.time() + 3
        while _t.time() < end:
            cid = new_correlation_id()
            eid = storage.claim_next(_t.time(), cid)
            if eid:
                pipeline.process_once(eid, cid)
            else:
                _t.sleep(0.002)

    t = threading.Thread(target=worker_rounds, daemon=True)
    t.start()
    # 同时反复重复提交（同内容）+ 反复批量 discard
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = []
        for _ in range(20):
            futs.append(pool.submit(
                lambda: pipeline.receive("bc", "t",
                                         {"simulate": {"mode": "unknown"}},
                                         trace_id="tr-bc")))
        for _ in range(20):
            futs.append(pool.submit(lambda: storage.batch_dispose(
                action="discard", event_ids=["bc"], correlation_id="ops",
                batch_id=new_correlation_id().replace("cid_", "batch_"),
                selector="ids", dry_run=False, now=0.0)))
        [f.result() for f in futs]
    t.join(timeout=5)

    v = storage.get_event("bc")
    # 终态必为 DEAD（无批量）或 DISCARDED（某个批量先到），绝不可能回到在途/成功
    assert v.state in (EventState.DEAD, EventState.DISCARDED)
    assert v.trace_id == "tr-bc"
    assert storage.side_effect_count("bc") == 0, "失败事件在任何并发交叉下都不产生副作用"
    # 成功路径事件在并发下也只产生一次副作用
    pipeline.receive("ok", "t", {}, trace_id="tr-ok")
    cid = new_correlation_id()
    eid = storage.claim_next(100.0, cid)
    assert eid == "ok"
    pipeline.process_once("ok", cid, now=100.0)
    assert storage.side_effect_count("ok") == 1


def _snapshot(storage):
    """记录全部事件的状态/updated_at 与全局历史行数、副作用总数，用于断言零改动。"""
    return {
        e.event_id: (e.state, e.updated_at)
        for e in storage.list_events(limit=500)
    }


def _history_total(storage) -> int:
    with storage._write_lock:
        return storage.conn.execute("SELECT COUNT(*) AS n FROM event_history").fetchone()["n"]


def _seed_mixed_terminal_states(storage, pipeline, clock):
    """构造一个各状态混杂的库：成功、业务失败、死信、在途(PENDING)各一条，trace 分离。"""
    pipeline.receive("ok", "t", {}, now=clock.now(), trace_id="trace-ok")
    pipeline.receive("biz", "t", {"simulate": {"mode": "business"}},
                     now=clock.now(), trace_id="trace-dead")
    pipeline.receive("unk", "t", {"simulate": {"mode": "unknown"}},
                     now=clock.now(), trace_id="trace-dead")
    _drive_to_terminal(storage, pipeline, clock)
    pipeline.receive("pending", "t", {}, now=clock.now(), trace_id="trace-fly")
    assert storage.get_event("ok").state is EventState.SUCCEEDED
    assert storage.get_event("biz").state is EventState.FAILED
    assert storage.get_event("unk").state is EventState.DEAD
    assert storage.get_event("pending").state is EventState.PENDING
    return ["ok", "biz", "unk", "pending"]


def _assert_totally_untouched(storage, before_states, before_hist, ids):
    """零命中批次的核心断言：状态/updated_at/历史行数/副作用全部不变。"""
    after_states = _snapshot(storage)
    assert after_states == before_states, "零命中筛选不得改动任何事件（含 updated_at）"
    assert _history_total(storage) == before_hist, "零命中筛选不得写任何状态迁移历史"
    # 死信/失败仍在原处，没有被误改成 DISCARDED
    assert storage.get_event("unk").state is EventState.DEAD
    assert storage.get_event("biz").state is EventState.FAILED
    assert storage.get_event("ok").state is EventState.SUCCEEDED
    assert storage.get_event("pending").state is EventState.PENDING
    assert storage.side_effect_count("ok") == 1
    for eid in ids:
        assert [h for h in storage.history(eid) if h.batch_id is not None] == [], \
            "零命中批次不得给任何事件挂 batch_id 迁移记录"


def test_filter_with_only_non_disposable_state_matches_nothing(
        settings, storage, pipeline, clock):
    """回归：filter.state 只给不可处置状态（如 SUCCEEDED）时，绝不能退化为全表处置。

    这是线上踩坑场景：只带 SUCCEEDED 的批量丢弃曾把库中无关死信/失败事件真丢掉。
    """
    ids = _seed_mixed_terminal_states(storage, pipeline, clock)
    before_states, before_hist = _snapshot(storage), _history_total(storage)

    with TestClient(create_app(settings, start_worker=False)) as c:
        r = c.post("/events/batch",
                   json={"action": "discard", "filter": {"state": ["SUCCEEDED"]}})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["matched"] == 0 and body["changed"] == []
        assert body["skipped_terminal"] == [] and body["not_found"] == []

    _assert_totally_untouched(storage, before_states, before_hist, ids)

    # 审计行即便写入，也必须如实记录 changed_count=0（不允许"改了 N 条"的假审计）
    audit = storage.get_batch(body["batch_id"])
    assert audit is not None and audit["changed_count"] == 0
    assert audit["changed_ids"] == [] and audit["matched"] == 0

    # 其它不可处置状态同样安全（PENDING/PROCESSING/DISCARDED，单值与多值）
    for states in (["PENDING"], ["PROCESSING"], ["DISCARDED"],
                   ["SUCCEEDED", "PENDING", "PROCESSING", "DISCARDED"]):
        before_hist2 = _history_total(storage)
        with TestClient(create_app(settings, start_worker=False)) as c:
            rr = c.post("/events/batch",
                        json={"action": "discard", "filter": {"state": states}})
            assert rr.status_code == 200
            assert rr.json()["matched"] == 0 and rr.json()["changed"] == []
        assert _history_total(storage) == before_hist2
        assert storage.get_event("unk").state is EventState.DEAD
        assert storage.get_event("biz").state is EventState.FAILED


def test_filter_state_and_trace_without_intersection_matches_nothing(
        settings, storage, pipeline, clock):
    """state（可处置）与 trace_id 组合后没有交集：一个都不能动。"""
    ids = _seed_mixed_terminal_states(storage, pipeline, clock)
    before_states, before_hist = _snapshot(storage), _history_total(storage)

    with TestClient(create_app(settings, start_worker=False)) as c:
        # 可处置状态存在，但它们属于 trace-dead；拿成功事件的 trace 去圈 -> 空交集
        r = c.post("/events/batch", json={
            "action": "discard",
            "filter": {"state": ["DEAD", "FAILED"], "trace_id": "trace-ok"},
        })
        assert r.status_code == 200
        assert r.json()["matched"] == 0 and r.json()["changed"] == []
        _assert_totally_untouched(storage, before_states, before_hist, ids)

        # 不存在的 trace_id
        before_states2, before_hist2 = _snapshot(storage), _history_total(storage)
        r2 = c.post("/events/batch", json={
            "action": "discard",
            "filter": {"state": ["DEAD", "FAILED"], "trace_id": "no-such-trace"},
        })
        assert r2.json()["matched"] == 0 and r2.json()["changed"] == []
        _assert_totally_untouched(storage, before_states2, before_hist2, ids)

        # 不可处置状态 + 无关 trace（两边都空）
        r3 = c.post("/events/batch", json={
            "action": "discard",
            "filter": {"state": ["SUCCEEDED"], "trace_id": "trace-dead"},
        })
        assert r3.json()["matched"] == 0 and r3.json()["changed"] == []
        assert _snapshot(storage) == before_states


def test_empty_state_list_is_explicit_empty_match(settings, storage, pipeline, clock):
    """filter.state=[] 是显式空集（什么都圈不到），不是"无状态条件"。"""
    ids = _seed_mixed_terminal_states(storage, pipeline, clock)
    before_states, before_hist = _snapshot(storage), _history_total(storage)

    with TestClient(create_app(settings, start_worker=False)) as c:
        r = c.post("/events/batch",
                   json={"action": "discard", "filter": {"state": []}})
        assert r.status_code == 200
        assert r.json()["matched"] == 0 and r.json()["changed"] == []
        # 空 state + trace 同样是空集（trace 无法把空状态集合"救活"）
        r2 = c.post("/events/batch", json={
            "action": "discard",
            "filter": {"state": [], "trace_id": "trace-dead"},
        })
        assert r2.json()["matched"] == 0 and r2.json()["changed"] == []
    _assert_totally_untouched(storage, before_states, before_hist, ids)


def test_zero_match_filter_repeated_is_side_effect_free(
        settings, storage, pipeline, clock):
    """重复提交同一个零命中批量请求：每次都 0 改动，不累积任何迁移/审计假数据。"""
    ids = _seed_mixed_terminal_states(storage, pipeline, clock)
    before_states = _snapshot(storage)

    payload = {"action": "requeue", "filter": {"state": ["SUCCEEDED"]}}
    with TestClient(create_app(settings, start_worker=False)) as c:
        for _ in range(5):
            r = c.post("/events/batch", json=payload)
            assert r.status_code == 200
            assert r.json()["matched"] == 0 and r.json()["changed"] == []

    assert _snapshot(storage) == before_states
    # 只有 5 行 changed_count=0 的诚实审计；没有任何事件历史被写入
    with storage._write_lock:
        tagged = storage.conn.execute(
            "SELECT COUNT(*) AS n FROM event_history WHERE batch_id IS NOT NULL"
        ).fetchone()["n"]
        fake = storage.conn.execute(
            "SELECT COUNT(*) AS n FROM manual_batches WHERE changed_count <> 0"
        ).fetchone()["n"]
    assert tagged == 0 and fake == 0
    assert storage.get_event("unk").state is EventState.DEAD
    assert storage.get_event("biz").state is EventState.FAILED
    _ = ids


def test_zero_match_filter_while_worker_running(settings, storage, pipeline):
    """零命中批量与后台投递并发：始终只返回真正满足条件且可处置的事件。"""
    import threading
    import time as _t
    from concurrent.futures import ThreadPoolExecutor

    pipeline.receive("fly", "t", {"simulate": {"mode": "business"}},
                     trace_id="tr-fly")
    pipeline.receive("goner", "t", {"simulate": {"mode": "unknown"}},
                     trace_id="tr-gone")

    stop = threading.Event()

    def worker_rounds():
        end = _t.time() + 2
        while _t.time() < end and not stop.is_set():
            cid = new_correlation_id()
            eid = storage.claim_next(_t.time(), cid)
            if eid:
                pipeline.process_once(eid, cid)
            else:
                _t.sleep(0.001)

    t = threading.Thread(target=worker_rounds, daemon=True)
    t.start()
    try:
        with ThreadPoolExecutor(max_workers=6) as pool, \
                TestClient(create_app(settings, start_worker=False)) as c:
            futs = []
            # 只用不可处置状态反复圈选；无论 worker 把事件带到什么状态都不得卷入
            for states in (["SUCCEEDED"], ["PENDING"], ["PROCESSING"],
                           ["DISCARDED"], ["SUCCEEDED", "PENDING"]):
                for _ in range(10):
                    futs.append(pool.submit(
                        lambda ss=states: c.post(
                            "/events/batch",
                            json={"action": "discard", "filter": {"state": ss}}
                        ).json()))
            for f in futs:
                assert f.result()["changed"] == []
    finally:
        stop.set()
        t.join(timeout=5)

    # goner 最终 DEAD（没被批量误伤）；没有事件被这些批量置为 DISCARDED
    assert storage.get_event("goner").state is EventState.DEAD
    with storage._write_lock:
        disc = storage.conn.execute(
            "SELECT COUNT(*) AS n FROM events WHERE state='DISCARDED'"
        ).fetchone()["n"]
        tagged = storage.conn.execute(
            "SELECT COUNT(*) AS n FROM event_history WHERE to_state='DISCARDED'"
        ).fetchone()["n"]
    assert disc == 0 and tagged == 0
    assert storage.stats()["side_effect_count"] == 0


def test_list_events_empty_states_returns_empty_without_full_scan(settings, storage,
                                                                  pipeline, clock):
    """存储层防御：states=[]（空集）必须返回空，而不是像 states=None 那样全表列出。"""
    _seed_mixed_terminal_states(storage, pipeline, clock)
    assert storage.list_events(states=[]) == []
    assert storage.list_events(states=[], trace_id="trace-dead") == []
    # None 仍是无条件全列（GET /events 不带 state 时的语义）
    assert {e.event_id for e in storage.list_events()} == {
        "ok", "biz", "unk", "pending"}


def test_explicit_ids_still_disposed_when_filter_matches_nothing(
        settings, storage, pipeline, clock):
    """显式 event_ids 与 filter 并存：filter 零命中不影响显式给出的可处置事件被处置；
    显式给出的不可处置事件仍只进 skipped_terminal，绝不被卷入。"""
    ids = _seed_mixed_terminal_states(storage, pipeline, clock)
    with TestClient(create_app(settings, start_worker=False)) as c:
        r = c.post("/events/batch", json={
            "action": "discard",
            "event_ids": ["unk", "ok", "missing"],
            "filter": {"state": ["SUCCEEDED"]},  # 零命中，不得扩大圈选
        })
        body = r.json()
        assert body["changed"] == ["unk"], body
        assert body["skipped_terminal"] == ["ok"], body
        assert body["not_found"] == ["missing"], body
    assert storage.get_event("unk").state is EventState.DISCARDED
    assert storage.get_event("biz").state is EventState.FAILED, "filter 不得把 biz 卷进来"
    assert storage.get_event("ok").state is EventState.SUCCEEDED
    assert storage.side_effect_count("ok") == 1
    _ = ids
