"""测试夹具：临时 SQLite 文件、确定性时钟、不跑后台线程的同步驱动。"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest

from app.config import Settings
from app.downstream import DownstreamHandler
from app.pipeline import Pipeline
from app.retry import RetryPolicy
from app.storage import Storage


@pytest.fixture()
def tmp_db(tmp_path):
    return str(tmp_path / "events.db")


@pytest.fixture()
def settings(tmp_db):
    return Settings(
        db_path=tmp_db,
        lease_timeout=0.5,
        max_attempts=3,
        base_backoff=0.02,
        backoff_factor=2.0,
        poll_interval=0.01,
        recovery_on_startup=True,
    )


@pytest.fixture()
def storage(settings):
    s = Storage(settings)
    s.connect()
    yield s
    s.close()


@pytest.fixture()
def handler():
    return DownstreamHandler()


@pytest.fixture()
def pipeline(storage, handler, settings):
    return Pipeline(storage, handler, RetryPolicy(settings), settings)


@dataclass
class Clock:
    t: float = 1000.0

    def now(self) -> float:
        return self.t

    def advance(self, dt: float) -> float:
        self.t += dt
        return self.t


@pytest.fixture()
def clock():
    return Clock()


def pump(storage: Storage, pipeline: Pipeline, cid_factory, clock, rounds=50):
    """同步驱动：反复认领并处理到期事件，直到没有可投递事件或达到上限。"""
    delivered = []
    for _ in range(rounds):
        cid = cid_factory()
        event_id = storage.claim_next(clock.now(), cid)
        if event_id is None:
            clock.advance(0.03)
            continue
        pipeline.process_once(event_id, cid, now=clock.now())
        delivered.append(event_id)
    return delivered


@pytest.fixture()
def drive():
    return pump


@pytest.fixture(autouse=True)
def _caplog_level(caplog):
    caplog.set_level(logging.INFO, logger="event_pipeline")
