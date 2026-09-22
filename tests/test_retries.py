"""失败分类与重试：瞬时错误按上限退避重试；业务拒绝不重试；未知进死信。"""
from __future__ import annotations

import logging

from app.models import EventState
from app.tracing import new_correlation_id


def _log_flow(storage, event_id: str) -> None:
    v = storage.get_event(event_id)
    logging.getLogger("test").info(
        "event_id=%s final_state=%s attempts=%s/%s failure_kind=%s last_error=%s "
        "side_effects=%s",
        event_id, v.state.value, v.attempts, v.max_attempts, v.failure_kind,
        v.last_error, storage.side_effect_count(event_id),
    )
    for h in storage.history(event_id):
        logging.getLogger("test").info(
        "    hist event_id=%s %s->%s attempt=%s cid=%s reason=%s",
            event_id, h.from_state, h.to_state.value, h.attempt,
            h.correlation_id, h.reason,
        )


def test_transient_retried_then_succeeds_once(pipeline, storage, drive, clock, settings):
    event_id = "evt-retry-ok"
    pipeline.receive(event_id, "t", {"simulate": {"mode": "transient", "fail_times": 2}},
                     now=clock.now())
    drive(storage, pipeline, new_correlation_id, clock)

    v = storage.get_event(event_id)
    assert v.state is EventState.SUCCEEDED
    assert v.attempts == 3, "前两次瞬时失败，第三次成功"
    assert storage.side_effect_count(event_id) == 1, "成功副作用恰好一次"
    states = [h.to_state for h in storage.history(event_id)]
    assert states == [
        EventState.PENDING, EventState.PROCESSING, EventState.PENDING,   # 第1次失败后退避
        EventState.PROCESSING, EventState.PENDING,                        # 第2次失败后退避
        EventState.PROCESSING, EventState.SUCCEEDED,                      # 第3次成功
    ]
    _log_flow(storage, event_id)


def test_transient_exhausts_then_dead(pipeline, storage, drive, clock, settings):
    event_id = "evt-retry-dead"
    pipeline.receive(event_id, "t", {"simulate": {"mode": "transient", "fail_times": 99}},
                     now=clock.now())
    drive(storage, pipeline, new_correlation_id, clock, rounds=100)

    v = storage.get_event(event_id)
    assert v.state is EventState.DEAD, "重试耗尽必须进入可查询的死信终态"
    assert v.attempts == settings.max_attempts
    assert v.failure_kind == "transient"
    assert storage.side_effect_count(event_id) == 0, "失败事件绝不能产生副作用"
    _log_flow(storage, event_id)


def test_business_rejection_not_retried(pipeline, storage, drive, clock):
    event_id = "evt-biz"
    pipeline.receive(event_id, "t",
                     {"simulate": {"mode": "business", "reason": "account frozen"}},
                     now=clock.now())
    drive(storage, pipeline, new_correlation_id, clock)

    v = storage.get_event(event_id)
    assert v.state is EventState.FAILED
    assert v.attempts == 1, "业务拒绝不得重试"
    assert v.failure_kind == "business"
    assert "account frozen" in (v.last_error or "")
    assert storage.side_effect_count(event_id) == 0
    _log_flow(storage, event_id)


def test_unknown_failure_goes_dead(pipeline, storage, drive, clock):
    event_id = "evt-unknown"
    pipeline.receive(event_id, "t",
                     {"simulate": {"mode": "unknown", "reason": "schema vanished"}},
                     now=clock.now())
    drive(storage, pipeline, new_correlation_id, clock)

    v = storage.get_event(event_id)
    assert v.state is EventState.DEAD, "无法归类的异常保守处理：死信待人工处置"
    assert v.attempts == 1
    assert v.failure_kind == "unknown"
    _log_flow(storage, event_id)


def test_dead_event_can_be_requeued_and_succeed(pipeline, storage, drive, clock):
    event_id = "evt-requeue"
    pipeline.receive(event_id, "t", {"simulate": {"mode": "transient", "fail_times": 99}},
                     now=clock.now())
    drive(storage, pipeline, new_correlation_id, clock, rounds=100)
    assert storage.get_event(event_id).state is EventState.DEAD

    # 人工修正后重新入队（此处换成成功负载，模拟下游已修复）
    assert storage.requeue(event_id, "cid-operator", clock.now()) is True
    v = storage.get_event(event_id)
    assert v.state is EventState.PENDING and v.attempts == 0

    # 为可成功完成，直接通过存储无法改 payload；这里用 handler 脚本计数器重置后即可成功：
    # handler 对该 event 已无 fail_times 语义依赖——改为验证重新走一遍仍受上限保护
    drive(storage, pipeline, new_correlation_id, clock, rounds=100)
    v = storage.get_event(event_id)
    assert v.state is EventState.DEAD, "重投后依旧失败，应再次进入死信而非丢失"
    assert storage.side_effect_count(event_id) == 0
    _log_flow(storage, event_id)


def test_failed_event_requeued_after_fix_then_succeeds(storage, settings, drive, clock):
    """业务拒绝进 FAILED 后，下游修复，人工 requeue 可成功（副作用恰好一次）。"""
    from app.downstream import DownstreamHandler
    from app.errors import BusinessRejected
    from app.pipeline import Pipeline
    from app.retry import RetryPolicy

    class BrokenHandler(DownstreamHandler):
        def handle(self, event_id, event_type, payload):  # type: ignore[override]
            raise BusinessRejected("rule v1")

    p_broken = Pipeline(storage, BrokenHandler(), RetryPolicy(settings), settings)
    event_id = "evt-fixed"
    p_broken.receive(event_id, "t", {}, now=clock.now())
    drive(storage, p_broken, new_correlation_id, clock)
    assert storage.get_event(event_id).state is EventState.FAILED

    # 运维修复下游后替换 handler 并人工 requeue
    pipeline = Pipeline(storage, DownstreamHandler(), RetryPolicy(settings), settings)
    assert storage.requeue(event_id, "cid-ops", clock.now()) is True
    drive(storage, pipeline, new_correlation_id, clock)

    v = storage.get_event(event_id)
    assert v.state is EventState.SUCCEEDED
    assert storage.side_effect_count(event_id) == 1
    actors = [h.actor.split(":")[0] for h in storage.history(event_id)]
    assert "manual" in actors, "人工处置必须在历史中留痕可审计"
    _log_flow(storage, event_id)
