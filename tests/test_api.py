"""HTTP 层：接收/重复/冲突的可区分反馈、历史可查询、统计口径、人工处置。"""
from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def _client(settings: Settings, start_worker: bool = True):
    app = create_app(settings, start_worker=start_worker)
    return TestClient(app)


def test_accept_duplicate_conflict_status(settings):
    with _client(settings) as c:
        r1 = c.post("/events", json={"event_id": "E1", "event_type": "t", "payload": {"a": 1}})
        assert r1.status_code == 201 and r1.json()["duplicate"] is False
        cid = r1.json()["correlation_id"]

        r2 = c.post("/events", json={"event_id": "E1", "event_type": "t", "payload": {"a": 1}})
        assert r2.status_code == 200, "重复提交必须以可区分状态反馈"
        assert r2.json()["duplicate"] is True
        assert r2.json()["correlation_id"] == cid, "重复提交沿用原关联标识"

        r3 = c.post("/events", json={"event_id": "E1", "event_type": "t", "payload": {"a": 2}})
        assert r3.status_code == 409, "同 id 不同内容必须明确拒绝"
        assert r3.json()["detail"]["error"] == "content_conflict"


def test_correlation_header_propagated_and_history_queryable(settings):
    with _client(settings) as c:
        r = c.post(
            "/events",
            json={"event_id": "E2", "event_type": "t", "payload": {}},
            headers={"X-Correlation-ID": "cid-trace-1"},
        )
        assert r.status_code in (200, 201)

        hist = c.get("/events/E2/history").json()
        assert hist, "完整流转历史可按事件查询"
        # 后台 worker 的每一步历史都沿用事件自身持久化的 cid，不串号
        assert {h["correlation_id"] for h in hist} == {r.json()["correlation_id"]}
        assert any(h["to_state"] == "PENDING" for h in hist)


def test_full_flow_via_api_and_stats(settings):
    with _client(settings) as c:
        c.post("/events", json={
            "event_id": "OK", "event_type": "t",
            "payload": {"simulate": {"mode": "transient", "fail_times": 2}},
        })
        c.post("/events", json={
            "event_id": "BIZ", "event_type": "t",
            "payload": {"simulate": {"mode": "business", "reason": "rejected"}},
        })

        import time
        deadline = time.time() + 5
        while time.time() < deadline:
            s = c.get("/stats").json()
            if s["by_state"]["PENDING"] == 0 and s["by_state"]["PROCESSING"] == 0:
                break
            time.sleep(0.02)

        assert c.get("/events/OK").json()["state"] == "SUCCEEDED"
        assert c.get("/events/BIZ").json()["state"] == "FAILED"
        stats = c.get("/stats").json()
        assert stats["total"] == 2
        assert stats["by_state"]["SUCCEEDED"] == 1
        assert stats["by_state"]["FAILED"] == 1
        assert stats["side_effect_count"] == 1, "统计口径：仅成功事件产生副作用"

        logging.getLogger("test").info("OK history=%s", c.get("/events/OK/history").json())


def test_manual_requeue_then_success(settings):
    with _client(settings, start_worker=False) as c:
        # 先让业务拒绝事件进入 FAILED
        c.post("/events", json={
            "event_id": "FIX", "event_type": "t",
            "payload": {"simulate": {"mode": "business", "reason": "temp rule"}},
        })
        pipeline = c.app.state.pipeline
        storage = c.app.state.storage
        from app.tracing import new_correlation_id

        def pump(rounds=10):
            t = 1.0
            for _ in range(rounds):
                t += 1
                eid = storage.claim_next(t, new_correlation_id())
                if eid is None:
                    continue
                pipeline.process_once(eid, new_correlation_id(), now=t)

        pump()
        assert storage.get_event("FIX").state.value == "FAILED"

        # 非法状态迁移要被拒绝
        ok = c.post("/events/FIX/discard")
        assert ok.status_code == 200 and ok.json()["state"] == "DISCARDED"
        again = c.post("/events/FIX/requeue")
        assert again.status_code == 409

        assert c.get("/events/nope").status_code == 404
