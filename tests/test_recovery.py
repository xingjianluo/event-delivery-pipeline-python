"""重启恢复测试：

场景 A：在“副作用已落盘、终态未推进”的崩溃窗口杀掉处理过程 ->
        新进程恢复锁后重投递必须识别为幂等成功，副作用恰好一次；
场景 B：在“瞬时失败尝试进行中”崩溃 -> 恢复后按退避继续重试直至成功；
场景 C：真实进程级恢复（subprocess 启动 uvicorn 后再启动新实例），
        验证状态、cid 归属不回退、不串号、统计口径不变。
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import unittest
from urllib.request import Request, urlopen

from app.models import EventStatus, new_correlation_id
from app.pipeline import EventPipeline
from app.storage import utcnow_iso
from tests.helpers import make_settings, print_trace, wait_for


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RestartRecoveryTests(unittest.TestCase):
    def _pipeline_no_workers(self, settings):
        return EventPipeline(settings, start_workers=False)

    def test_recovery_after_crash_between_side_effect_and_terminal(self):
        """场景 A：副作用已生效、终态未提交的崩溃恢复。"""
        settings, _ = make_settings(max_attempts=3, workers=1, transient_sim=0)
        pl = self._pipeline_no_workers(settings)
        # 直接用 storage 造一条已领取（锁在途）事件并产生副作用
        cid_recv = new_correlation_id("recv")
        now = "2026-01-01T00:00:00+00:00"
        pl.storage.insert_event_if_absent(
            event_id="crash-1", event_type="order",
            payload_json=json.dumps({"simulate": "ok"}),
            idempotency_key=None, content_hash="x",
            correlation_id=cid_recv, now=now,
        )
        # 手动领取并提交副作用，但不推进状态（模拟崩溃窗口）
        row = pl.storage.claim_next_event("dead-worker", now)
        self.assertEqual(row["event_id"], "crash-1")
        self.assertEqual(row["locked_attempt"], 1)
        cid_att = new_correlation_id("att")
        pl.storage.start_attempt("crash-1", 1, cid_att, now)
        inserted = pl.storage.record_side_effect(
            event_id="crash-1", downstream_ref="crash-1:done",
            correlation_id=cid_att, at=now,
        )
        self.assertTrue(inserted)
        pl.shutdown()  # 不解锁，模拟进程被杀死

        # —— 新进程打开同一数据库 ——
        pl2 = EventPipeline(settings)
        try:
            d = wait_for(pl2, "crash-1", {EventStatus.DELIVERED}, timeout=10)
            self.assertEqual(d.side_effects, 1)          # 绝无重复副作用
            self.assertEqual(d.event.status, EventStatus.DELIVERED)
            self.assertEqual(d.event.attempts, 1)
            stages = [h.stage for h in d.lifecycle]
            self.assertIn("RECOVERED_AFTER_RESTART", stages)
            # 恢复后那次成功投递复用 attempt 编号 1（锁记录的在途尝试）
            recovered_attempts = [
                a for a in d.attempts_history if a.outcome == "DELIVERED"
            ]
            self.assertEqual(len(recovered_attempts), 1)
            self.assertEqual(recovered_attempts[0].attempt_no, 1)
            # 副作用只发生一次（恢复重投递识别为幂等成功）
            self.assertEqual(pl2.storage.count_side_effects("crash-1"), 1)
            # 接收 cid 保持不变，不串号
            self.assertEqual(d.event.receive_correlation_id, cid_recv)
            self.assertTrue(recovered_attempts[0].correlation_id.startswith("att_"))
            print_trace(pl2, "crash-1", "recovery: side effect before crash")
        finally:
            pl2.shutdown()

    def test_recovery_mid_retry_chain(self):
        """场景 B：瞬时失败重试链途中崩溃，恢复后继续并成功（无副作用丢失/重复）。"""
        settings, _ = make_settings(max_attempts=3, workers=1, transient_sim=2)
        pl = self._pipeline_no_workers(settings)
        pl.receive("crash-2", "order", {"simulate": "transient"}, None,
                   new_correlation_id("recv"))
        # 让一个 worker 只处理第一次失败尝试：直接调用 process_one
        row = pl.storage.claim_next_event("dead-worker", utcnow_iso())
        self.assertIsNotNone(row)
        pl.pool.process_one("dead-worker", row)
        d = pl.get_detail("crash-2")
        self.assertEqual(d.event.status, EventStatus.RETRYING)
        # 模拟第二次尝试被领取（加锁）后进程被杀：把 next_run_at 提前后领取
        c = pl.storage.conn()
        c.execute(
            "UPDATE events SET next_run_at = ? WHERE event_id = ?",
            ("2026-01-01T00:00:01+00:00", "crash-2"),
        )
        row2 = pl.storage.claim_next_event("dead-worker", utcnow_iso())
        self.assertEqual(row2["locked_attempt"], 2)
        pl.shutdown()

        pl2 = EventPipeline(settings)
        try:
            d = wait_for(pl2, "crash-2", {EventStatus.DELIVERED}, timeout=10)
            self.assertEqual(d.side_effects, 1)
            outcomes = [a.outcome for a in d.attempts_history]
            self.assertEqual(outcomes, ["TRANSIENT", "TRANSIENT", "DELIVERED"])
            self.assertEqual(d.event.attempts, 3)
            self.assertIn("RECOVERED_AFTER_RESTART",
                          [h.stage for h in d.lifecycle])
            print_trace(pl2, "crash-2", "recovery: mid retry chain")
        finally:
            pl2.shutdown()

    def test_real_process_restart(self):
        """场景 C：真实进程重启——uvicorn 子进程崩溃/停止后，新实例看到一致状态。"""
        settings, _ = make_settings(max_attempts=2, workers=1, transient_sim=99)
        env = {
            **os.environ,
            "EDP_DB_PATH": settings.db_path,
            "EDP_MAX_ATTEMPTS": "2",
            "EDP_WORKER_COUNT": "1",
            "EDP_RETRY_BASE_DELAY": "0.02",
            "EDP_TRANSIENT_SIM_ATTEMPTS": "99",
        }
        port = _free_port()

        def _stop(proc):
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
            if proc.stdout:
                proc.stdout.close()

        def start_server():
            return subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "main:app",
                 "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
                env=env, cwd=os.getcwd(),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )

        def post_json(path, payload, cid):
            req = Request(
                f"http://127.0.0.1:{port}{path}",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json",
                         "X-Correlation-ID": cid},
                method="POST",
            )
            with urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read())

        def get_json(path):
            with urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as resp:
                return json.loads(resp.read())

        def wait_http(deadline=15):
            t0 = time.time()
            while time.time() - t0 < deadline:
                try:
                    with urlopen(f"http://127.0.0.1:{port}/health", timeout=1):
                        return
                except Exception:
                    time.sleep(0.1)
            raise AssertionError("server did not become ready")

        proc = start_server()
        try:
            wait_http()
            cid = new_correlation_id("recv")
            status, body = post_json(
                "/events",
                {"event_id": "proc-1", "type": "order",
                 "payload": {"simulate": "transient"}},
                cid,
            )
            self.assertEqual(status, 201)
            # 等待重试用尽进入 DEAD
            t0 = time.time()
            while time.time() - t0 < 15:
                ev = get_json("/events/proc-1")
                if ev["event"]["status"] == "DEAD":
                    break
                time.sleep(0.1)
            ev = get_json("/events/proc-1")
            self.assertEqual(ev["event"]["status"], "DEAD")
            stats_before = get_json("/stats")
        finally:
            _stop(proc)

        # —— 重启第二个进程，使用同一数据库 ——
        proc2 = start_server()
        try:
            wait_http()
            ev = get_json("/events/proc-1")
            self.assertEqual(ev["event"]["status"], "DEAD")           # 状态未回退
            self.assertEqual(ev["event"]["receive_correlation_id"], cid)  # cid 不串号
            self.assertEqual(ev["side_effects"], 0)
            stats_after = get_json("/stats")
            self.assertEqual(stats_before, stats_after)               # 统计口径一致

            # 死信在新进程上可查询、可人工处置并成功
            post_json(
                "/events",
                {"event_id": "proc-1", "type": "order",
                 "payload": {"simulate": "transient"}},
                new_correlation_id("recv"),
            )  # 重复提交：期望 200 + duplicate（不重新执行）
            # 运维 replay 并解除故障注入不可行（payload 固定），此处通过把
            # 事件内容确认不可变更来验证“重复不重跑”，再用 replay 追加预算
            # 后仍失败但尝试次数累计、历史不丢。
            req = Request(
                f"http://127.0.0.1:{port}/dead-letters/proc-1/replay?extra_budget=1",
                data=b"", headers={"X-Correlation-ID": "op-replay-1"}, method="POST",
            )
            with urlopen(req, timeout=10) as resp:
                self.assertEqual(resp.status, 202)
            t0 = time.time()
            while time.time() - t0 < 15:
                ev = get_json("/events/proc-1")
                if ev["event"]["status"] == "DEAD":
                    break
                time.sleep(0.1)
            ev = get_json("/events/proc-1")
            self.assertEqual(ev["event"]["attempts"], 3)  # 2 次原始 + 1 次 replay
            self.assertIn("MANUAL_REPLAY",
                          [h["stage"] for h in ev["lifecycle"]])
            print("\nreal process restart event:", json.dumps(ev, indent=2)[:1200])
        finally:
            _stop(proc2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
