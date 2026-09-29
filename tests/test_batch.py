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
