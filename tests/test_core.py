"""核心行为测试：去重、内容冲突、失败分类、重试上限、终态规则、统计口径。"""
from __future__ import annotations

import unittest

from app.models import EventStatus, FailureKind, new_correlation_id
from app.pipeline import EventPipeline
from tests.helpers import configure_logging, make_settings, print_trace, wait_for

configure_logging()


class CoreBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.settings, self.tmp = make_settings(max_attempts=3, transient_sim=2)
        self.pl = EventPipeline(self.settings)

    def tearDown(self):
        self.pl.shutdown()

    def _submit(self, eid, mode="ok", event_type="order", extra=None):
        payload = {"simulate": mode}
        if extra:
            payload.update(extra)
        return self.pl.receive(eid, event_type, payload, None, new_correlation_id("recv"))

    def test_duplicate_submission_is_distinct_and_not_reprocessed(self):
        """重复提交显式反馈 duplicate=True，关联到首次 cid，状态不变、不重复处理。"""
        r1 = self._submit("dup-1")
        self.assertTrue(r1.accepted)
        self.assertFalse(r1.duplicate)
        d = wait_for(self.pl, "dup-1", {EventStatus.DELIVERED})

        r2 = self._submit("dup-1")
        r3 = self._submit("dup-1")
        self.assertFalse(r2.accepted)
        self.assertTrue(r2.duplicate)
        self.assertTrue(r3.duplicate)
        # 重复请求回各自的 cid，同时保留首次接收 cid 以便对账
        self.assertEqual(r2.original_correlation_id, r1.correlation_id)
        self.assertNotEqual(r2.correlation_id, r1.correlation_id)

        d2 = self.pl.get_detail("dup-1")
        self.assertEqual(d2.event.status, EventStatus.DELIVERED)
        self.assertEqual(d2.side_effects, 1)
        stages = [h.stage for h in d2.lifecycle]
        self.assertEqual(stages.count("RECEIVED"), 1)
        self.assertEqual(stages.count("DUPLICATE_SUBMISSION"), 2)
        print_trace(self.pl, "dup-1", "duplicate submission")

    def test_same_id_different_payload_is_rejected(self):
        """相同事件标识但内容不一致：409 语义拒绝，且绝不覆盖原事件。"""
        r1 = self._submit("conflict-1", mode="ok", extra={"v": 1})
        self.assertTrue(r1.accepted)
        wait_for(self.pl, "conflict-1", {EventStatus.DELIVERED})

        r2 = self.pl.receive(
            "conflict-1", "order", {"simulate": "ok", "v": 2}, None,
            new_correlation_id("recv"),
        )
        self.assertTrue(r2.conflict)
        self.assertFalse(r2.accepted)
        self.assertEqual(r2.original_correlation_id, r1.correlation_id)

        d = self.pl.get_detail("conflict-1")
        self.assertEqual(d.event.payload, {"simulate": "ok", "v": 1})
        self.assertEqual(d.side_effects, 1)
        self.assertIn("CONTENT_CONFLICT_REJECTED", [h.stage for h in d.lifecycle])
        print_trace(self.pl, "conflict-1", "content conflict")

    def test_idempotency_key_dedup_with_different_event_id(self):
        """event_id 不同但 idempotency_key 相同且内容一致 -> 视为重复，返回首次事件。"""
        r1 = self.pl.receive("a-1", "order", {"x": 1}, "idem-77", new_correlation_id("recv"))
        r2 = self.pl.receive("a-2", "order", {"x": 1}, "idem-77", new_correlation_id("recv"))
        self.assertTrue(r1.accepted)
        self.assertTrue(r2.duplicate)
        self.assertEqual(r2.event_id, "a-1")
        wait_for(self.pl, "a-1", {EventStatus.DELIVERED})
        self.assertEqual(self.pl.get_stats().side_effects, 1)

    def test_business_rejection_is_terminal_without_retry(self):
        """业务性拒绝不重试，首次即 DEAD，失败原因可区分。"""
        self._submit("biz-1", mode="business")
        d = wait_for(self.pl, "biz-1", {EventStatus.DEAD})
        self.assertEqual(d.event.attempts, 1)
        self.assertEqual(d.event.failure_kind, FailureKind.BUSINESS)
        self.assertEqual(d.event.last_error_code, "BUSINESS_RULE_REJECTED")
        self.assertEqual(len(d.attempts_history), 1)
        stages = [h.stage for h in d.lifecycle]
        self.assertIn("MOVED_TO_DEAD", stages)
        self.assertNotIn("RETRY_SCHEDULED", stages)
        self.assertEqual(d.side_effects, 0)
        print_trace(self.pl, "biz-1", "business rejection")

    def test_unknown_failure_is_terminal_without_retry(self):
        """无法归类的错误不重试，首次即 DEAD（UNKNOWN），不静默丢弃。"""
        self._submit("unk-1", mode="unknown")
        d = wait_for(self.pl, "unk-1", {EventStatus.DEAD})
        self.assertEqual(d.event.attempts, 1)
        self.assertEqual(d.event.failure_kind, FailureKind.UNKNOWN)
        self.assertEqual(d.event.last_error_code, "UNCLASSIFIABLE_FAILURE")
        self.assertEqual(d.side_effects, 0)
        print_trace(self.pl, "unk-1", "unknown failure")

    def test_transient_retries_then_delivers_with_single_side_effect(self):
        """瞬时错误按上限重试，恢复后成功；副作用恰好一次，尝试与 cid 完整可查。"""
        self._submit("tr-1", mode="transient")
        d = wait_for(self.pl, "tr-1", {EventStatus.DELIVERED})
        self.assertEqual(d.event.attempts, 3)
        outcomes = [a.outcome for a in d.attempts_history]
        self.assertEqual(outcomes, ["TRANSIENT", "TRANSIENT", "DELIVERED"])
        cids = [a.correlation_id for a in d.attempts_history]
        self.assertEqual(len(set(cids)), 3)            # 每次尝试独立 cid
        self.assertTrue(all(c.startswith("att_") for c in cids))
        self.assertEqual(d.side_effects, 1)
        print_trace(self.pl, "tr-1", "transient then delivered")

    def test_transient_retries_exhausted_goes_dead(self):
        """始终瞬时失败：达到 max_attempts 后进入 DEAD，阶段为 RETRIES_EXHAUSTED。"""
        # transient_sim=99 -> 每次都瞬时失败
        self.settings, self.tmp = make_settings(max_attempts=3, transient_sim=99)
        self.pl.shutdown()
        self.pl = EventPipeline(self.settings)
        self._submit("exh-1", mode="transient")
        d = wait_for(self.pl, "exh-1", {EventStatus.DEAD})
        self.assertEqual(d.event.attempts, 3)
        self.assertEqual(d.event.failure_kind, FailureKind.TRANSIENT)
        self.assertEqual(d.event.last_error_code, "DOWNSTREAM_UNAVAILABLE")
        self.assertEqual([a.outcome for a in d.attempts_history],
                         ["TRANSIENT", "TRANSIENT", "TRANSIENT"])
        self.assertIn("RETRIES_EXHAUSTED", [h.stage for h in d.lifecycle])
        self.assertEqual(d.side_effects, 0)
        print_trace(self.pl, "exh-1", "retries exhausted")

    def test_manual_replay_dead_event_branches(self):
        """死信人工 replay 的分支：成功入队后可恢复投递、非死信拒绝、不存在拒绝。"""
        import json

        # 用“重试用尽”的瞬时事件模拟，replay 后解除故障注入 -> 可成功交付
        self.settings, self.tmp = make_settings(max_attempts=2, transient_sim=99)
        self.pl.shutdown()
        self.pl = EventPipeline(self.settings)
        self._submit("rep-1", mode="transient")
        wait_for(self.pl, "rep-1", {EventStatus.DEAD})

        # 运维确认故障排除：清除故障注入后 replay
        c = self.pl.storage.conn()
        c.execute(
            "UPDATE events SET payload = ? WHERE event_id = ?",
            (json.dumps({"simulate": "ok"}), "rep-1"),
        )
        token = self.pl.replay_dead("rep-1", new_correlation_id("op"))
        self.assertEqual(token, "requeued")
        d2 = wait_for(self.pl, "rep-1", {EventStatus.DELIVERED})
        stages = [h.stage for h in d2.lifecycle]
        self.assertIn("MANUAL_REPLAY", stages)
        self.assertEqual(d2.side_effects, 1)
        print_trace(self.pl, "rep-1", "manual replay recovered")

        self.assertEqual(self.pl.replay_dead("rep-1", "op-x"), "not_dead")
        self.assertEqual(self.pl.replay_dead("missing", "op-x"), "not_found")

    def test_stats_consistency(self):
        """统计口径：各状态之和=总数；副作用数=成功事件数。"""
        self._submit("s-1", mode="ok")
        self._submit("s-2", mode="business")
        self._submit("s-3", mode="unknown")
        wait_for(self.pl, "s-1", {EventStatus.DELIVERED})
        wait_for(self.pl, "s-2", {EventStatus.DEAD})
        wait_for(self.pl, "s-3", {EventStatus.DEAD})
        st = self.pl.get_stats()
        self.assertEqual(st.total, 3)
        self.assertEqual(st.delivered + st.dead + st.pending + st.retrying, st.total)
        self.assertEqual(st.delivered, 1)
        self.assertEqual(st.dead, 2)
        self.assertEqual(st.side_effects, 1)
        self.assertGreaterEqual(st.business_rejections, 1)
        self.assertGreaterEqual(st.unknown_failures, 1)
        print("\nstats:", st.model_dump())


if __name__ == "__main__":
    unittest.main(verbosity=2)
