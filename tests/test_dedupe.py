"""并发重复提交去重：只处理一次；重复可区分；内容不一致被拒绝。"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.models import EventState
from app.tracing import new_correlation_id


def _log_flow(storage, event_id: str, title: str) -> None:
    v = storage.get_event(event_id)
    logging.getLogger("test").info(
        "[%s] event_id=%s state=%s cid=%s attempts=%s side_effects=%s",
        title, event_id, v.state.value, v.correlation_id, v.attempts,
        storage.side_effect_count(event_id),
    )
    for h in storage.history(event_id):
        logging.getLogger("test").info(
            "    hist event_id=%s %s->%s attempt=%s cid=%s actor=%s reason=%s",
            event_id, h.from_state, h.to_state.value, h.attempt,
            h.correlation_id, h.actor, h.reason,
        )


def test_concurrent_duplicate_submit_processed_once(pipeline, storage, drive, clock):
    event_id = "evt-concurrent-1"
    payload = {"order_id": "o-1", "amount": 100}

    # 20 个线程并发提交同一事件（同一内容）
    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(
            lambda _: pipeline.receive(event_id, "order.created", payload, now=clock.now()),
            range(20),
        ))

    created = [r for r in results if not r.duplicate]
    duplicates = [r for r in results if r.duplicate]
    assert len(created) == 1, "只有一次首次写入"
    assert len(duplicates) == 19, "其余必须明确标记为 duplicate"
    cids = {r.correlation_id for r in results}
    assert cids == {created[0].correlation_id}, "重复提交必须沿用首次的 correlation id"

    # 投递并处理：只能处理一次，副作用一次
    drive(storage, pipeline, new_correlation_id, clock)
    v = storage.get_event(event_id)
    assert v.state is EventState.SUCCEEDED
    assert v.attempts == 1
    assert storage.side_effect_count(event_id) == 1
    _log_flow(storage, event_id, "concurrent-duplicate")


def test_same_id_different_content_rejected(pipeline, storage, clock):
    from app.pipeline import ContentConflict

    r1 = pipeline.receive("evt-x", "order.created", {"v": 1}, now=clock.now())
    assert r1.duplicate is False

    with pytest.raises(ContentConflict) as ei:
        pipeline.receive("evt-x", "order.created", {"v": 2}, now=clock.now())
    assert ei.value.existing_hash != ei.value.incoming_hash

    # 内容不一致绝不覆盖既有事件，且不会产生任何处理
    v = storage.get_event("evt-x")
    assert v.payload == {"v": 1}
    assert storage.history("evt-x")[0].to_state is EventState.PENDING
    logging.getLogger("test").info(
        "content conflict rejected event_id=evt-x existing=%s incoming=%s",
        ei.value.existing_hash, ei.value.incoming_hash,
    )

