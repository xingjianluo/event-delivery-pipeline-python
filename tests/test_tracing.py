"""调用方追踪标识（trace_id / X-Correlation-ID）端到端闭环与反查。

覆盖：
- 调用方标识被原样持久化并贯穿接收/投递/重试/历史；
- 多个事件可共用一个业务标识；可按标识反查关联事件及各自状态；
- 重复提交（含并发）不替换首次标识，返回值总能对上首次带入的值；
- 标识缺失时服务端生成；非法标识被 400 拒绝。
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.models import EventState
from app.pipeline import InvalidTraceId
from app.tracing import new_correlation_id


def test_caller_trace_id_persisted_and_propagated(pipeline, storage, drive, clock):
    event_id = "evt-trace-1"
    result = pipeline.receive(
        event_id, "order.created",
        {"simulate": {"mode": "transient", "fail_times": 1}},
        now=clock.now(), trace_id="biz-order-42",
    )
    assert result.trace_id == "biz-order-42"
    assert result.correlation_id == "biz-order-42"
    assert result.trace_id_mismatch is False

    drive(storage, pipeline, new_correlation_id, clock)
    v = storage.get_event(event_id)
    assert v.state is EventState.SUCCEEDED
    assert v.trace_id == "biz-order-42" and v.correlation_id == "biz-order-42"

    # 历史中每一行（接收/认领/重试/成功）都沿用调用方标识，不串号
    hist = storage.history(event_id)
    assert {h.correlation_id for h in hist} == {"biz-order-42"}

    # 反查
    tv = storage.trace_view("biz-order-42")
    assert tv is not None
    assert tv["event_count"] == 1
    assert tv["by_state"]["SUCCEEDED"] == 1
    assert tv["side_effect_count"] == 1
    assert storage.trace_view("does-not-exist") is None


def test_multiple_events_share_one_trace_id_and_reverse_lookup(
    pipeline, storage, drive, clock
):
    trace = "checkout-abc"
    for i, payload in enumerate(
        [
            {},
            {"simulate": {"mode": "business", "reason": "r"}},
            {"simulate": {"mode": "unknown", "reason": "u"}},
        ]
    ):
        r = pipeline.receive(f"evt-grp-{i}", "t", payload, now=clock.now(), trace_id=trace)
        assert r.trace_id == trace
    drive(storage, pipeline, new_correlation_id, clock, rounds=100)

    tv = storage.trace_view(trace)
    assert tv is not None and tv["event_count"] == 3
    assert tv["by_state"]["SUCCEEDED"] == 1
    assert tv["by_state"]["FAILED"] == 1
    assert tv["by_state"]["DEAD"] == 1
    assert {e.event_id for e in tv["events"]} == {"evt-grp-0", "evt-grp-1", "evt-grp-2"}
    assert {e.trace_id for e in tv["events"]} == {trace}

    # 按状态 + 追踪标识组合筛选
    only_dead = storage.list_events(states=["DEAD"], trace_id=trace)
    assert [e.event_id for e in only_dead] == ["evt-grp-2"]


def test_duplicate_submit_keeps_first_trace_id(pipeline, storage, clock):
    r1 = pipeline.receive("evt-dup", "t", {}, now=clock.now(), trace_id="first-wins")
    assert r1.duplicate is False and r1.trace_id == "first-wins"

    # 重复提交携带了不同的标识：明确提示 mismatch，但持久化值绝不被替换
    r2 = pipeline.receive("evt-dup", "t", {}, now=clock.now(), trace_id="later-value")
    assert r2.duplicate is True
    assert r2.trace_id == "first-wins"
    assert r2.trace_id_mismatch is True
    assert storage.get_event("evt-dup").correlation_id == "first-wins"

    # 重复提交带相同标识：无 mismatch
    r3 = pipeline.receive("evt-dup", "t", {}, now=clock.now(), trace_id="first-wins")
    assert r3.duplicate is True and r3.trace_id_mismatch is False


def test_concurrent_submits_same_trace_never_mix(pipeline, storage, clock):
    event_id = "evt-conc-trace"
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(
            lambda i: pipeline.receive(
                event_id, "t", {}, now=clock.now(),
                trace_id=f"trace-{i}" if i else "trace-winner",
            ),
            range(16),
        ))
    persisted = storage.get_event(event_id).correlation_id
    assert all(r.trace_id == persisted for r in results), \
        "并发重复提交返回的追踪标识必须全部等于持久化的那一个，绝不串号"
    assert results[0].trace_id == "trace-winner"  # 首个写入者（map 保序提交，同事件同内容）


def test_missing_trace_id_is_generated_and_invalid_rejected(pipeline):
    r = pipeline.receive("evt-auto", "t", {})
    assert r.trace_id.startswith("cid_")
    with pytest.raises(InvalidTraceId):
        pipeline.receive("evt-bad", "t", {}, trace_id="bad\tinjection")
    with pytest.raises(InvalidTraceId):
        pipeline.receive("evt-bad2", "t", {}, trace_id="x" * 200)


def test_trace_endpoints_via_http(settings):
    with TestClient(create_app(settings, start_worker=False)) as c:
        # 头与请求体两种携带方式；头优先
        c.post("/events", json={
            "event_id": "h1", "event_type": "t", "payload": {},
            "trace_id": "body-trace",
        }, headers={"X-Correlation-ID": "header-trace"})
        c.post("/events", json={
            "event_id": "h2", "event_type": "t", "payload": {},
            "trace_id": "body-only",
        })
        assert c.get("/events/h1").json()["trace_id"] == "header-trace"
        assert c.get("/events/h2").json()["trace_id"] == "body-only"

        tv = c.get("/traces/header-trace").json()
        assert tv["event_count"] == 1 and tv["events"][0]["event_id"] == "h1"
        assert c.get("/traces/nope").status_code == 404

        bad = c.post("/events", json={
            "event_id": "h3", "event_type": "t", "payload": {},
            "trace_id": "has\x00null",
        })
        assert bad.status_code == 400
        assert bad.json()["detail"]["error"] == "invalid_trace_id"
