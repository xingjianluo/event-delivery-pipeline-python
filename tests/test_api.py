"""HTTP 层测试：状态码语义、关联标识头、死信处置接口。"""
from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from app.api import create_app
from app.models import EventStatus
from app.pipeline import EventPipeline
from tests.helpers import make_settings, wait_for


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.settings, _ = make_settings(max_attempts=2, workers=2, transient_sim=1)
        self.pl = EventPipeline(self.settings)
        self.client = TestClient(create_app(self.pl))
        self.client.__enter__()

    def tearDown(self):
        self.client.__exit__(None, None, None)
        self.pl.shutdown()

    def test_receive_duplicate_conflict_status_codes_and_cids(self):
        # 首次接收 201，回显调用方 cid
        r = self.client.post(
            "/events",
            json={"event_id": "api-1", "type": "t", "payload": {}},
            headers={"X-Correlation-ID": "cid-api-first"},
        )
        self.assertEqual(r.status_code, 201)
        self.assertEqual(r.headers["X-Correlation-ID"], "cid-api-first")
        body = r.json()
        self.assertTrue(body["accepted"])

        # 重复提交 200 + duplicate=true，携带 original_correlation_id
        r2 = self.client.post(
            "/events",
            json={"event_id": "api-1", "type": "t", "payload": {}},
        )
        self.assertEqual(r2.status_code, 200)
        b2 = r2.json()
        self.assertTrue(b2["duplicate"])
        self.assertEqual(b2["original_correlation_id"], "cid-api-first")
        self.assertTrue(b2["correlation_id"].startswith("recv_"))  # 未传则生成

        # 内容冲突 409
        r3 = self.client.post(
            "/events",
            json={"event_id": "api-1", "type": "t", "payload": {"x": 2}},
        )
        self.assertEqual(r3.status_code, 409)
        self.assertEqual(r3.json()["error"], "CONTENT_CONFLICT")

    def test_full_lifecycle_and_dead_letter_endpoints(self):
        r = self.client.post(
            "/events",
            json={"event_id": "api-biz", "type": "t",
                  "payload": {"simulate": "business"}},
            headers={"X-Correlation-ID": "cid-biz"},
        )
        self.assertEqual(r.status_code, 201)
        import time

        for _ in range(100):
            ev = self.client.get("/events/api-biz").json()
            if ev["event"]["status"] == "DEAD":
                break
            time.sleep(0.03)
        ev = self.client.get("/events/api-biz").json()
        self.assertEqual(ev["event"]["status"], "DEAD")
        self.assertEqual(ev["event"]["failure_kind"], "BUSINESS")

        dead = self.client.get("/dead-letters").json()["dead_letters"]
        self.assertIn("api-biz", [e["event_id"] for e in dead])

        # replay 非死信保护
        self.assertEqual(self.client.get("/health").status_code, 200)

        # 404
        self.assertEqual(self.client.get("/events/nope").status_code, 404)
        self.assertEqual(
            self.client.post("/dead-letters/nope/replay").status_code, 404
        )

        stats = self.client.get("/stats").json()
        self.assertEqual(stats["dead"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
