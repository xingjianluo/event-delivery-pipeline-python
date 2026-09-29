"""调用方追踪标识闭环：自带标识端到端流转、重复/并发不串号、按标识反查。"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.models import EventState
from app.tracing import InvalidCorrelationId, new_correlation_id, validate_correlation_id


def test_caller_correlation_id_persisted_and_returned(pipeline, storage, drive, clock):
    r = pipeline.receive("t-1", "t", {}, now=clock.now(), correlation_id="biz-trace-001")
    assert r.correlation_id == "biz-trace-001"
    assert r.duplicate is False and r.correlation_id_mismatch is False

    drive(storage, pipeline, new_correlation_id, clock)
    v = storage.get_event("t-1")
    assert v.state is EventState.SUCCEEDED
    # 整条链路（接收/认领/成功）的历史都用调用方带进来的标识
    hist = storage.history("t-1")
    assert {h.correlation_id for h in hist} == {"biz-trace-001"}
    assert all("biz-trace-001" in h.actor or h.actor.startswith("worker:") for h in hist)


def test_duplicate_keeps_first_correlation_id_and_flags_mismatch(pipeline, storage, clock):
    r1 = pipeline.receive("d-1", "t", {"a": 1}, now=clock.now(), correlation_id="first-cid")
    assert r1.correlation_id == "first-cid"
    # 同内容重复提交，不带标识：沿用首次值，无 mismatch
    r2 = pipeline.receive("d-1", "t", {"a": 1}, now=clock.now())
    assert r2.duplicate is True
    assert r2.correlation_id == "first-cid" and r2.correlation_id_mismatch is False
    # 同内容重复提交但带不同标识：不替换持久化值，明确回显冲突
    r3 = pipeline.receive("d-1", "t", {"a": 1}, now=clock.now(), correlation_id="second-cid")
    assert r3.duplicate is True
    assert r3.correlation_id == "first-cid", "系统实际使用的仍是首次标识"
    assert r3.correlation_id_mismatch is True
    assert r3.submitted_correlation_id == "second-cid", "本次提交值要回显便于对账"
    # 带相同标识：无 mismatch
    r4 = pipeline.receive("d-1", "t", {"a": 1}, now=clock.now(), correlation_id="first-cid")
    assert r4.correlation_id_mismatch is False
    # 存储层始终只有一个标识，未被任何一次提交替换
    assert storage.get_event("d-1").correlation_id == "first-cid"


def test_concurrent_same_submission_same_caller_cid_wins(pipeline, storage, clock):
    """20 个线程并发首次提交同一事件、都带同一个调用方标识：只写入一次且不串号。"""
    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(
            lambda _: pipeline.receive("c-1", "t", {"v": 1},
                                       now=clock.now(), correlation_id="race-cid"),
            range(20),
        ))
    assert {r.correlation_id for r in results} == {"race-cid"}
    assert len([r for r in results if not r.duplicate]) == 1
    assert storage.get_event("c-1").correlation_id == "race-cid"


def test_concurrent_first_submissions_with_different_cids_single_winner(pipeline, storage, clock):
    """并发首次提交各带不同标识：只有一个成为持久化值，其他人都拿到同一个赢家值。"""
    def submit(i):
        return pipeline.receive("c-2", "t", {"v": 1},
                                now=clock.now(), correlation_id=f"cid-cand-{i}")
    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(submit, range(20)))
    stored = storage.get_event("c-2").correlation_id
    assert stored.startswith("cid-cand-")
    firsts = [r for r in results if not r.duplicate]
    assert len(firsts) == 1
    assert firsts[0].correlation_id == stored
    # 输家全部明确告知 mismatch，且回显的系统值一致
    losers = [r for r in results if r.duplicate]
    assert len(losers) == 19
    assert all(r.correlation_id == stored for r in losers)
    assert all(r.correlation_id_mismatch and r.submitted_correlation_id != stored
               for r in losers)


def test_invalid_correlation_id_rejected():
    for bad in ["", " ", "has space", "-leading-dash", "x" * 129, "中文字符", "a/b"]:
        with pytest.raises(InvalidCorrelationId):
            validate_correlation_id(bad)
    # 合法值（含常见分隔符）与首尾空白归一化
    assert validate_correlation_id("  Trace_1.2:3-ABC ") == "Trace_1.2:3-ABC"


def test_reverse_lookup_by_correlation_across_events(pipeline, storage, drive, clock):
    # 一次业务操作关联 3 个事件，另有 1 个无关事件
    for i in range(3):
        pipeline.receive(f"biz-{i}", "order", {"i": i},
                         now=clock.now(), correlation_id="op-777")
    pipeline.receive("other-1", "other", {}, now=clock.now(), correlation_id="op-other")
    drive(storage, pipeline, new_correlation_id, clock)

    views = storage.find_events(correlation_id="op-777")
    assert {v.event_id for v in views} == {"biz-0", "biz-1", "biz-2"}
    assert all(v.state is EventState.SUCCEEDED for v in views)

    hist = storage.history_by_correlation("op-777")
    # 每个事件 3 行历史（PENDING/PROCESSING/SUCCEEDED），全部挂在 op-777 下
    assert {h.event_id for h in hist} == {"biz-0", "biz-1", "biz-2"}
    assert len(hist) == 9
    assert all(h.correlation_id == "op-777" for h in hist)

    # 状态 + 标识组合筛选
    dead_views = storage.find_events(
        states=[EventState.DEAD.value], correlation_id="op-777")
    assert dead_views == []
    only_other = storage.find_events(correlation_id="op-other")
    assert [v.event_id for v in only_other] == ["other-1"]


def test_cid_survives_retries_and_manual_disposal(pipeline, storage, drive, clock):
    pipeline.receive("m-1", "t", {"simulate": {"mode": "transient", "fail_times": 99}},
                     now=clock.now(), correlation_id="trace-dead")
    drive(storage, pipeline, new_correlation_id, clock, rounds=100)
    v = storage.get_event("m-1")
    assert v.state is EventState.DEAD and v.correlation_id == "trace-dead"

    # 人工重放：cid 仍不变，历史里出现 manual 操作者
    assert storage.requeue("m-1", "ops-alice", clock.now()) is True
    drive(storage, pipeline, new_correlation_id, clock, rounds=100)
    assert storage.get_event("m-1").state is EventState.DEAD
    assert storage.get_event("m-1").correlation_id == "trace-dead"
    assert storage.discard("m-1", "ops-bob", clock.now()) is True
    v = storage.get_event("m-1")
    assert v.state is EventState.DISCARDED and v.correlation_id == "trace-dead"
    cids = {h.correlation_id for h in storage.history("m-1")}
    assert cids == {"trace-dead"}, "失败/重试/重放/丢弃全程追踪标识不变"
    actors = [h.actor for h in storage.history("m-1")]
    assert any(a.startswith("manual:ops-alice") for a in actors)
    assert any(a.startswith("manual:ops-bob") for a in actors)


def test_http_header_and_body_cid_and_lookup(settings):
    with TestClient(create_app(settings, start_worker=False)) as c:
        # 请求体携带
        r = c.post("/events", json={
            "event_id": "h1", "event_type": "t", "payload": {},
            "correlation_id": "body-cid",
        })
        assert r.status_code == 201 and r.json()["correlation_id"] == "body-cid"
        # 头优先于请求体
        r2 = c.post("/events", json={
            "event_id": "h2", "event_type": "t", "payload": {},
            "correlation_id": "body-cid-2",
        }, headers={"X-Correlation-ID": "header-cid"})
        assert r2.json()["correlation_id"] == "header-cid"
        # 非法标识 400（头与体各一）
        assert c.post("/events", json={
            "event_id": "h3", "event_type": "t", "payload": {},
            "correlation_id": "bad cid",
        }).status_code == 400
        assert c.post(
            "/events",
            json={"event_id": "h4", "event_type": "t", "payload": {}},
            headers={"X-Correlation-ID": "bad cid"},
        ).status_code == 400
        # 反查端点
        ids = {e["event_id"] for e in c.get("/correlations/body-cid/events").json()["items"]}
        assert ids == {"h1"}
        assert c.get("/correlations/bad cid/events").status_code == 400
        # 列表筛选
        rl = c.get("/events", params=[("state", "PENDING")]).json()
        assert {e["event_id"] for e in rl["items"]} == {"h1", "h2"}
        rl2 = c.get("/events", params=[("state", "PENDING"), ("correlation_id", "header-cid")]).json()
        assert [e["event_id"] for e in rl2["items"]] == ["h2"]
    logging.getLogger("test").info("tracing http flows ok")
