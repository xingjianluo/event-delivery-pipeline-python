"""并发压力测试：重复提交去重、事件归属不串号、副作用恰好一次、统计口径一致。"""
from __future__ import annotations

import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from app.models import EventStatus, new_correlation_id
from app.pipeline import EventPipeline
from tests.helpers import make_settings, print_trace, wait_for

N_EVENTS = 30
N_DUP = 8  # 每个事件并发提交次数


class ConcurrencyTests(unittest.TestCase):
    def test_concurrent_duplicate_submissions_processed_once(self):
        settings, _ = make_settings(max_attempts=2, workers=4, transient_sim=1)
        pl = EventPipeline(settings)
        try:
            barrier = threading.Barrier(N_DUP)
            lock = threading.Lock()
            results: dict[str, list] = {f"ev-{i:03d}": [] for i in range(N_EVENTS)}

            def submit(event_id: str, idx: int) -> None:
                cid = new_correlation_id("recv")
                barrier.wait(timeout=30)  # 让所有线程同时撞上同一条事件
                r = pl.receive(
                    event_id, "order.created",
                    {"simulate": "transient", "n": int(event_id.split("-")[1])},
                    None, cid,
                )
                with lock:
                    results[event_id].append(r)

            tasks = []
            with ThreadPoolExecutor(max_workers=N_DUP) as pool:
                for i in range(N_EVENTS):
                    eid = f"ev-{i:03d}"
                    for j in range(N_DUP):
                        tasks.append(pool.submit(submit, eid, j))
                for t in tasks:
                    t.result(timeout=30)

            accepted = 0
            for eid, rs in results.items():
                self.assertEqual(len(rs), N_DUP, msg=eid)
                acc = [r for r in rs if r.accepted]
                dup = [r for r in rs if r.duplicate]
                conf = [r for r in rs if r.conflict]
                self.assertEqual(len(acc), 1, msg=f"{eid} accepted={len(acc)}")
                self.assertEqual(len(dup), N_DUP - 1, msg=eid)
                self.assertEqual(len(conf), 0, msg=eid)
                accepted += 1
                # 所有重复反馈必须指向同一个首次接收 cid
                first_cid = acc[0].correlation_id
                self.assertTrue(all(r.original_correlation_id == first_cid for r in dup),
                                msg=eid)
            self.assertEqual(accepted, N_EVENTS)

            # 等待全部投递完成（瞬时失败后重试成功）
            for i in range(N_EVENTS):
                wait_for(pl, f"ev-{i:03d}", {EventStatus.DELIVERED}, timeout=30)

            # 全局一致性断言
            st = pl.get_stats()
            print(f"\n[{datetime.now().isoformat()}] stats after concurrent run: {st.model_dump()}")
            self.assertEqual(st.total, N_EVENTS)
            self.assertEqual(st.delivered, N_EVENTS)
            self.assertEqual(st.side_effects, N_EVENTS)          # 无重复副作用
            self.assertEqual(st.dead + st.pending + st.retrying, 0)

            # 逐事件核对：接收 cid 不串号、副作用唯一、历史只接收一次
            for i in range(N_EVENTS):
                eid = f"ev-{i:03d}"
                d = pl.get_detail(eid)
                self.assertEqual(d.event.event_id, eid)
                self.assertEqual(d.event.payload["n"], i)        # 内容归属无错配
                self.assertEqual(d.side_effects, 1, msg=eid)
                stages = [h.stage for h in d.lifecycle]
                self.assertEqual(stages.count("RECEIVED"), 1, msg=eid)
                self.assertEqual(stages.count("DUPLICATE_SUBMISSION"), N_DUP - 1,
                                 msg=eid)
                # 历史中的 cid 要么是接收 cid 要么是该事件自己的 att cid
                recv_cid = d.event.receive_correlation_id
                for h in d.lifecycle:
                    if h.stage == "RECEIVED":
                        self.assertEqual(h.correlation_id, recv_cid)
                    if h.stage in ("RETRY_SCHEDULED", "DELIVERED"):
                        self.assertTrue(h.correlation_id.startswith("att_"))
                for a in d.attempts_history:
                    self.assertTrue(a.correlation_id.startswith("att_"))
            print_trace(pl, "ev-000", "sample concurrent event")
        finally:
            pl.shutdown()


if __name__ == "__main__":
    unittest.main(verbosity=2)
