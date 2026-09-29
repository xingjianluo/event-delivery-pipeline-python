"""批量人工处置：条件筛选、幂等、可审计、不误伤已完成事件。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from app.models import EventState
from app.storage import BatchIdempotencyConflict
from app.tracing import new_correlation_id


def _make_mixed_states(pipeline, storage, drive, clock):
    """造 2 DEAD、2 FAILED、1 SUCCEEDED、1 PENDING；cid 分两组。"""
    def recv(eid, payload, cid):
        pipeline.receive(eid, "t", payload, now=clock.now(), correlation_id=cid)
    recv("dead-a", {"simulate": {"mode": "transient", "fail_times": 99}}, "grp-A")
    recv("dead-b", {"simulate": {"mode": "unknown"}}, "grp-A")
    recv("fail-a", {"simulate": {"mode": "business"}}, "grp-B")
    recv("fail-b", {"simulate": {"mode": "business"}}, "grp-B")
    recv("done-a", {}, "grp-A")
    drive(storage, pipeline, new_correlation_id, clock, rounds=100)
    # 终态就位后再补一个尚未投递的 PENDING（避免被上面的 drive 顺手处理）
    recv("pend-a", {}, "grp-B")
    states = {eid: storage.get_event(eid).state for eid in
              ["dead-a", "dead-b", "fail-a", "fail-b", "done-a", "pend-a"]}
    assert states["dead-a"] is EventState.DEAD
    assert states["dead-b"] is EventState.DEAD
    assert states["fail-a"] is EventState.FAILED
    assert states["fail-b"] is EventState.FAILED
    assert states["done-a"] is EventState.SUCCEEDED
    assert states["pend-a"] is EventState.PENDING


def test_batch_discard_by_states_skips_non_disposable(pipeline, storage, drive, clock):
    _make_mixed_states(pipeline, storage, drive, clock)
    res = storage.batch_dispose(
        action="discard", idempotency_key="k-1",
        event_ids=None,
        states=[EventState.DEAD.value, EventState.FAILED.value,
                EventState.SUCCEEDED.value, EventState.PENDING.value],
        correlation_id=None, reason="ops cleanup", actor="ops-1", now=clock.now(),
    )
    assert set(res["changed"]) == {"dead-a", "dead-b", "fail-a", "fail-b"}
    assert set(res["skipped"]) == {"done-a", "pend-a"}, "已成功/待投递绝不被处置"
    for eid in res["changed"]:
        assert storage.get_event(eid).state is EventState.DISCARDED
    assert storage.get_event("done-a").state is EventState.SUCCEEDED
    assert storage.get_event("pend-a").state is EventState.PENDING


def test_batch_requeue_by_correlation_then_succeeds_once(pipeline, storage, drive, clock):
    _make_mixed_states(pipeline, storage, drive, clock)
    res = storage.batch_dispose(
        action="requeue", idempotency_key="k-2", event_ids=None,
        states=[EventState.DEAD.value, EventState.FAILED.value],
        correlation_id="grp-A", reason="retry after fix", actor="ops-2", now=clock.now(),
    )
    # grp-A 下只有 dead-a/dead-b 可处置；done-a 是 SUCCEEDED 不在 states 选择器里
    assert set(res["changed"]) == {"dead-a", "dead-b"}
    for eid in res["changed"]:
        v = storage.get_event(eid)
        assert v.state is EventState.PENDING and v.attempts == 0
        assert v.correlation_id == "grp-A"

    # 再次投递（handler 对这两个仍会失败），它们重新走完整分类逻辑后仍到终态
    drive(storage, pipeline, new_correlation_id, clock, rounds=100)
    assert storage.get_event("dead-a").state is EventState.DEAD
    assert storage.get_event("dead-b").state is EventState.DEAD
    assert storage.side_effect_count("dead-a") == 0
    assert storage.side_effect_count("dead-b") == 0


def test_batch_idempotent_replay_no_double_transition(pipeline, storage, drive, clock):
    _make_mixed_states(pipeline, storage, drive, clock)
    kwargs = dict(
        event_ids=["dead-a"], states=None, correlation_id=None,
        reason="x", actor="ops-3", now=clock.now(),
    )
    r1 = storage.batch_dispose(action="discard", idempotency_key="dup-1", **kwargs)
    assert r1["changed"] == ["dead-a"] and r1["replayed"] is False
    first_batch = r1["batch_id"]

    # 同一 key 重复提交（即使时间不同/调用者不同）：返回首次结果，不新增历史
    r2 = storage.batch_dispose(
        action="discard", idempotency_key="dup-1",
        event_ids=["dead-a"], states=None, correlation_id=None,
        reason="different reason", actor="ops-other", now=clock.now() + 10,
    )
    assert r2["replayed"] is True
    assert r2["batch_id"] == first_batch
    assert r2["changed"] == ["dead-a"]
    discards = [h for h in storage.history("dead-a")
                if h.to_state is EventState.DISCARDED]
    assert len(discards) == 1, "幂等重放不得产生第二次迁移/副作用"
    assert discards[0].actor == f"batch#{first_batch}:ops-3"

    # 同一 key 用于不同动作 -> 冲突报错
    try:
        storage.batch_dispose(
            action="requeue", idempotency_key="dup-1",
            event_ids=["dead-a"], states=None, correlation_id=None,
            reason="x", actor="ops-3", now=clock.now(),
        )
    except BatchIdempotencyConflict:
        pass
    else:
        raise AssertionError("same key different action must conflict")


def test_batch_explicit_ids_not_found_and_already_disposed(pipeline, storage, drive, clock):
    _make_mixed_states(pipeline, storage, drive, clock)
    r = storage.batch_dispose(
        action="discard", idempotency_key="k-3",
        event_ids=["dead-a", "ghost-1", "done-a", "fail-a"],
        states=None, correlation_id=None, reason="x", actor="ops-4", now=clock.now(),
    )
    assert r["not_found"] == ["ghost-1"]
    assert set(r["changed"]) == {"dead-a", "fail-a"}
    assert r["skipped"] == ["done-a"]

    # 再次对已 DISCARDED 的事件处置：幂等 key 不同，但状态已不可处置 -> skipped，
    # 不产生第二次状态迁移，也不报错（天然幂等）
    r2 = storage.batch_dispose(
        action="discard", idempotency_key="k-4",
        event_ids=["dead-a", "fail-a"], states=None, correlation_id=None,
        reason="x", actor="ops-4", now=clock.now(),
    )
    assert r2["changed"] == [] and set(r2["skipped"]) == {"dead-a", "fail-a"}
    assert r2["replayed"] is False


def test_batch_empty_selector_rejected_at_storage(storage):
    try:
        storage.batch_dispose(
            action="discard", idempotency_key="k-empty",
            event_ids=None, states=None, correlation_id=None,
            reason="x", actor="o", now=1.0,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("empty selector must be rejected")


def test_batch_audit_rows_are_queryable(pipeline, storage, drive, clock):
    _make_mixed_states(pipeline, storage, drive, clock)
    r = storage.batch_dispose(
        action="requeue", idempotency_key="audit-1",
        event_ids=None, states=[EventState.FAILED.value], correlation_id=None,
        reason="fix rule and replay", actor="ops-5", now=clock.now(),
    )
    for eid in r["changed"]:
        hist = storage.history(eid)
        row = [h for h in hist if h.actor.startswith(f"batch#{r['batch_id']}:")]
        assert len(row) == 1
        assert row[0].to_state is EventState.PENDING
        assert "fix rule and replay" in row[0].reason
        assert row[0].correlation_id == storage.get_event(eid).correlation_id


def test_concurrent_batch_same_key_applies_once(pipeline, storage, drive, clock):
    _make_mixed_states(pipeline, storage, drive, clock)

    def call(_):
        return storage.batch_dispose(
            action="discard", idempotency_key="race-key",
            event_ids=["dead-a", "dead-b"], states=None, correlation_id=None,
            reason="race", actor="ops-6", now=clock.now(),
        )
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(call, range(10)))
    batch_ids = {r["batch_id"] for r in results}
    assert len(batch_ids) == 1, "同一幂等键并发只能产生一个批次记录"
    assert sum(1 for r in results if not r["replayed"]) == 1
    assert sum(1 for r in results if r["replayed"]) == 9
    for eid in ["dead-a", "dead-b"]:
        discards = [h for h in storage.history(eid)
                    if h.to_state is EventState.DISCARDED]
        assert len(discards) == 1


# --------------------------------------------------------------- HTTP 层
from fastapi.testclient import TestClient  # noqa: E402

from app.config import Settings as _Settings  # noqa: E402
from app.main import create_app  # noqa: E402


def _drive_http(storage, pipeline):
    import time as _t
    from app.tracing import new_correlation_id as _nid
    t = 100.0
    for _ in range(100):
        t += 1
        eid = storage.claim_next(t, _nid())
        if eid is None:
            continue
        pipeline.process_once(eid, _nid(), now=t)


def test_batch_endpoints_http(settings: _Settings):
    app = create_app(settings, start_worker=False)
    with TestClient(app) as c:
        storage = app.state.storage
        pipeline = app.state.pipeline
        # 造 2 DEAD（同 cid）+ 1 已成功
        for eid in ("d1", "d2"):
            r = c.post("/events", json={
                "event_id": eid, "event_type": "t",
                "payload": {"simulate": {"mode": "unknown"}},
                "correlation_id": "batch-trace",
            })
            assert r.status_code == 201
        c.post("/events", json={"event_id": "ok", "event_type": "t",
                                "payload": {}, "correlation_id": "batch-trace"})
        _drive_http(storage, pipeline)
        assert storage.get_event("d1").state is EventState.DEAD
        assert storage.get_event("ok").state is EventState.SUCCEEDED

        # 按 cid 批量重放（SUCCEEDED 的 ok 不在 DEAD/FAILED states 内，天然不卷入）
        r = c.post("/events/batch-requeue", json={
            "correlation_id": "batch-trace", "states": ["DEAD"],
            "reason": "downstream fixed", "idempotency_key": "http-1",
        }, headers={"X-Correlation-ID": "ops-user"})
        assert r.status_code == 200, r.text
        assert r.json()["changed"] == ["d1", "d2"]
        assert r.json()["replayed"] is False

        # 幂等：同 key 再来一次，replayed=true，不新增迁移
        r2 = c.post("/events/batch-requeue", json={
            "correlation_id": "batch-trace", "states": ["DEAD"],
            "reason": "different", "idempotency_key": "http-1",
        })
        assert r2.status_code == 200 and r2.json()["replayed"] is True

        # 空选择器 400；含 SUCCEEDED 状态 400；同 key 换动作 409
        assert c.post("/events/batch-discard",
                      json={"idempotency_key": "http-x"}).status_code == 400
        assert c.post("/events/batch-discard", json={
            "states": ["SUCCEEDED"], "idempotency_key": "http-y",
        }).status_code == 400
        conflict = c.post("/events/batch-discard", json={
            "states": ["DEAD"], "idempotency_key": "http-1",
        })
        assert conflict.status_code == 409
        assert conflict.json()["detail"]["error"] == "idempotency_key_conflict"

        # 批量丢弃：event_ids 显式指定，含一个不存在 id 与一个已成功 id
        _drive_http(storage, pipeline)  # d1/d2 重放后仍 unknown -> 再次 DEAD
        r3 = c.post("/events/batch-discard", json={
            "event_ids": ["d1", "ghost", "ok"], "idempotency_key": "http-2",
        })
        assert r3.status_code == 200
        assert r3.json()["changed"] == ["d1"]
        assert r3.json()["not_found"] == ["ghost"]
        assert r3.json()["skipped"] == ["ok"]
        assert storage.get_event("ok").state is EventState.SUCCEEDED
        # 副作用总数不因任何批量操作增加
        assert storage.stats()["side_effect_count"] == 1
