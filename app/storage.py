"""基于 SQLite 的本地持久化存储（单文件、事务、重启后可恢复）。

并发模型：
- 单进程内所有写操作以一把可重入锁串行化；SQLite 置为 autocommit +
  显式 ``BEGIN IMMEDIATE``，状态迁移与历史/副作用在同一事务内原子提交；
- WAL 日志保证重启不丢已提交数据；``busy_timeout`` 应对多进程短暂竞争。

状态机：
    PENDING ──claim──▶ PROCESSING ──成功──▶ SUCCEEDED（终态，副作用已记录）
       ▲                  │
       │              瞬时错误且未达上限
       └──────────────────┤
                          ├─业务拒绝──────────▶ FAILED（终态）
                          ├─未知错误/重试耗尽─▶ DEAD（终态，可人工处置）
    DEAD/FAILED ─requeue─▶ PENDING（attempts 清零，留痕）
    DEAD/FAILED ─discard─▶ DISCARDED（终态）
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from app.config import Settings
from app.models import EventState, EventView, HistoryItem

log = logging.getLogger("event_pipeline.storage")

_TERMINAL = {EventState.SUCCEEDED, EventState.FAILED, EventState.DEAD, EventState.DISCARDED}


class Storage:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._conn: sqlite3.Connection | None = None
        self._write_lock = threading.RLock()

    # ------------------------------------------------------------------ setup
    def connect(self) -> None:
        if self._conn is not None:
            return
        db_path = self.settings.db_path
        parent = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(parent, exist_ok=True)
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10)
        conn.isolation_level = None  # autocommit；事务显式管理
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=10000")
        self._conn = conn
        self._init_schema()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("storage not connected")
        return self._conn

    def _init_schema(self) -> None:
        with self._tx() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    event_id       TEXT PRIMARY KEY,
                    event_type     TEXT NOT NULL,
                    payload        TEXT NOT NULL,
                    content_hash   TEXT NOT NULL,
                    state          TEXT NOT NULL,
                    attempts       INTEGER NOT NULL DEFAULT 0,
                    max_attempts   INTEGER NOT NULL,
                    correlation_id TEXT NOT NULL,
                    next_attempt_at REAL,
                    lease_owner    TEXT,
                    leased_at      REAL,
                    last_error     TEXT,
                    failure_kind   TEXT,
                    disposition    TEXT,
                    created_at     REAL NOT NULL,
                    updated_at     REAL NOT NULL
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS event_history (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id       TEXT NOT NULL,
                    correlation_id TEXT NOT NULL,
                    at             REAL NOT NULL,
                    from_state     TEXT,
                    to_state       TEXT NOT NULL,
                    attempt        INTEGER NOT NULL,
                    reason         TEXT NOT NULL,
                    actor          TEXT NOT NULL
                )
                """
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_event ON event_history(event_id, id)"
            )
            # 下游副作用登记表：event_id 为主键，保证每个事件至多登记一次
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS side_effects (
                    event_id TEXT PRIMARY KEY,
                    at       REAL NOT NULL
                )
                """
            )

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            c = self.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                yield c
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise

    def _history(
        self,
        c: sqlite3.Connection,
        *,
        event_id: str,
        correlation_id: str,
        at: float,
        from_state: str | None,
        to_state: EventState,
        attempt: int,
        reason: str,
        actor: str,
    ) -> None:
        c.execute(
            """INSERT INTO event_history
               (event_id, correlation_id, at, from_state, to_state, attempt, reason, actor)
               VALUES (?,?,?,?,?,?,?,?)""",
            (event_id, correlation_id, at, from_state, to_state.value, attempt, reason, actor),
        )
        log.info(
            "transition event_id=%s cid=%s %s->%s attempt=%s actor=%s reason=%s",
            event_id, correlation_id, from_state, to_state.value, attempt, actor, reason,
        )

    # ------------------------------------------------------- receive & dedupe
    def insert_event_if_absent(
        self,
        event_id: str,
        event_type: str,
        payload: dict,
        content_hash: str,
        correlation_id: str,
        now: float,
    ) -> tuple[bool, str | None]:
        """原子插入。返回 (是否首次写入, 冲突的既有content_hash或None)。

        并发提交同一 event_id 时，靠主键约束只有一个事务能成为首次写入；
        内容哈希不一致则明确返回冲突，调用方据此拒绝（409）。
        """
        with self._tx() as c:
            row = c.execute(
                "SELECT content_hash FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
            if row is not None:
                existing = row["content_hash"]
                return False, (None if existing == content_hash else existing)
            c.execute(
                """INSERT INTO events
                   (event_id, event_type, payload, content_hash, state, attempts,
                    max_attempts, correlation_id, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    event_id, event_type, json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    content_hash, EventState.PENDING.value, 0, self.settings.max_attempts,
                    correlation_id, now, now,
                ),
            )
            self._history(
                c, event_id=event_id, correlation_id=correlation_id, at=now,
                from_state=None, to_state=EventState.PENDING, attempt=0,
                reason="accepted", actor=f"api:{correlation_id}",
            )
            return True, None

    def get_event(self, event_id: str) -> EventView | None:
        with self._write_lock:
            row = self.conn.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
        return None if row is None else self._to_view(row)

    def _to_view(self, row: sqlite3.Row) -> EventView:
        return EventView(
            event_id=row["event_id"],
            event_type=row["event_type"],
            payload=json.loads(row["payload"]),
            content_hash=row["content_hash"],
            state=EventState(row["state"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            correlation_id=row["correlation_id"],
            next_attempt_at=row["next_attempt_at"],
            last_error=row["last_error"],
            failure_kind=row["failure_kind"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # ------------------------------------------------------------- delivery
    def claim_next(self, now: float, correlation_id: str) -> str | None:
        """认领一个到期的待投递事件，或回收一个租约超时的 PROCESSING 事件。

        attempts 在认领时 +1（即投递尝试次数，含首次与崩溃后回收），
        状态迁移到 PROCESSING。返回事件 id；没有可投递事件返回 None。
        """
        stale_before = now - self.settings.lease_timeout
        with self._tx() as c:
            row = c.execute(
                """
                SELECT event_id, state, attempts FROM events
                WHERE (state = 'PENDING'
                       AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                   OR (state = 'PROCESSING' AND leased_at IS NOT NULL
                       AND leased_at <= ?)
                ORDER BY created_at, event_id
                LIMIT 1
                """,
                (now, stale_before),
            ).fetchone()
            if row is None:
                return None
            event_id = row["event_id"]
            prev = EventState(row["state"])
            new_attempt = row["attempts"] + 1
            c.execute(
                """UPDATE events
                   SET state='PROCESSING', attempts=?, lease_owner=?, leased_at=?,
                       next_attempt_at=NULL, updated_at=?
                   WHERE event_id=?""",
                (new_attempt, correlation_id, now, now, event_id),
            )
            reason = "claimed for delivery" if prev is EventState.PENDING \
                else "reclaimed after stale lease (worker crash/restart)"
            self._history(
                c, event_id=event_id, correlation_id=self._event_cid(c, event_id),
                at=now, from_state=prev.value, to_state=EventState.PROCESSING,
                attempt=new_attempt, reason=reason, actor=f"worker:{correlation_id}",
            )
            return event_id

    @staticmethod
    def _event_cid(c: sqlite3.Connection, event_id: str) -> str:
        r = c.execute("SELECT correlation_id FROM events WHERE event_id=?", (event_id,)).fetchone()
        return r["correlation_id"]

    def _claim_still_ours(self, c: sqlite3.Connection, event_id: str, attempt: int) -> sqlite3.Row | None:
        row = c.execute(
            "SELECT * FROM events WHERE event_id=?", (event_id,)
        ).fetchone()
        if row is None:
            return None
        if row["state"] != EventState.PROCESSING.value or row["attempts"] != attempt:
            # 租约已失效（崩溃后被他人回收，或已被人工处置），本次结果作废
            return None
        return row

    def finish_success(self, event_id: str, attempt: int, correlation_id: str, now: float) -> bool:
        with self._tx() as c:
            row = self._claim_still_ours(c, event_id, attempt)
            if row is None:
                return False
            c.execute(
                """UPDATE events
                   SET state='SUCCEEDED', lease_owner=NULL, leased_at=NULL,
                       next_attempt_at=NULL, last_error=NULL, failure_kind=NULL,
                       updated_at=?
                   WHERE event_id=?""",
                (now, event_id),
            )
            # 副作用与终态在同一事务登记；主键保证一个事件至多一次
            c.execute(
                "INSERT OR IGNORE INTO side_effects(event_id, at) VALUES (?,?)",
                (event_id, now),
            )
            self._history(
                c, event_id=event_id, correlation_id=row["correlation_id"], at=now,
                from_state=EventState.PROCESSING.value, to_state=EventState.SUCCEEDED,
                attempt=attempt, reason="delivered; downstream side effect recorded exactly once",
                actor=f"worker:{correlation_id}",
            )
            return True

    def finish_failure(
        self,
        event_id: str,
        attempt: int,
        kind: str,
        reason: str,
        next_attempt_at: float | None,
        correlation_id: str,
        now: float,
    ) -> bool:
        with self._tx() as c:
            row = self._claim_still_ours(c, event_id, attempt)
            if row is None:
                return False
            if kind == "business":
                # 业务性拒绝：不可重试，直接 FAILED
                target = EventState.FAILED
                full_reason = f"{kind}: {reason} (permanent business rejection)"
            elif next_attempt_at is None:
                target = EventState.DEAD
                tail = "retries exhausted" if kind == "transient" else "unclassifiable failure"
                full_reason = f"{kind}: {reason} ({tail})"
            else:
                target = EventState.PENDING
                full_reason = f"{kind}: {reason} (retry scheduled at {next_attempt_at:.3f})"
            c.execute(
                """UPDATE events
                   SET state=?, lease_owner=NULL, leased_at=NULL, next_attempt_at=?,
                       last_error=?, failure_kind=?, updated_at=?
                   WHERE event_id=?""",
                (target.value, next_attempt_at, reason, kind, now, event_id),
            )
            self._history(
                c, event_id=event_id, correlation_id=row["correlation_id"], at=now,
                from_state=EventState.PROCESSING.value, to_state=target,
                attempt=attempt, reason=full_reason,
                actor=f"worker:{correlation_id}",
            )
            return True

    # ------------------------------------------------------- manual disposal
    def requeue(self, event_id: str, correlation_id: str, now: float) -> bool:
        with self._tx() as c:
            row = c.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
            if row is None or EventState(row["state"]) not in {EventState.DEAD, EventState.FAILED}:
                return False
            c.execute(
                """UPDATE events
                   SET state='PENDING', attempts=0, lease_owner=NULL, leased_at=NULL,
                       next_attempt_at=NULL, disposition='requeued', updated_at=?
                   WHERE event_id=?""",
                (now, event_id),
            )
            self._history(
                c, event_id=event_id, correlation_id=row["correlation_id"], at=now,
                from_state=row["state"], to_state=EventState.PENDING, attempt=0,
                reason="manual requeue after review",
                actor=f"manual:{correlation_id}",
            )
            return True

    def discard(self, event_id: str, correlation_id: str, now: float) -> bool:
        with self._tx() as c:
            row = c.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
            if row is None or EventState(row["state"]) not in {EventState.DEAD, EventState.FAILED}:
                return False
            c.execute(
                "UPDATE events SET state='DISCARDED', disposition='discarded', updated_at=? "
                "WHERE event_id=?",
                (now, event_id),
            )
            self._history(
                c, event_id=event_id, correlation_id=row["correlation_id"], at=now,
                from_state=row["state"], to_state=EventState.DISCARDED,
                attempt=row["attempts"], reason="manual discard after review",
                actor=f"manual:{correlation_id}",
            )
            return True

    # ------------------------------------------------------------- queries
    def history(self, event_id: str) -> list[HistoryItem]:
        with self._write_lock:
            rows = self.conn.execute(
                "SELECT * FROM event_history WHERE event_id=? ORDER BY id", (event_id,)
            ).fetchall()
        return [
            HistoryItem(
                id=r["id"], event_id=r["event_id"], correlation_id=r["correlation_id"],
                at=r["at"], from_state=r["from_state"], to_state=EventState(r["to_state"]),
                attempt=r["attempt"], reason=r["reason"],
                actor=r["actor"],
            )
            for r in rows
        ]

    def find_stale_leases(self, stale_before: float) -> list[sqlite3.Row]:
        with self._write_lock:
            return list(self.conn.execute(
                "SELECT event_id, correlation_id, attempts, leased_at FROM events "
                "WHERE state='PROCESSING' AND leased_at IS NOT NULL AND leased_at <= ?",
                (stale_before,),
            ).fetchall())

    def side_effect_count(self, event_id: str) -> int:
        with self._write_lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM side_effects WHERE event_id=?", (event_id,)
            ).fetchone()
        return int(row["n"])

    def stats(self) -> dict[str, Any]:
        with self._write_lock:
            by_state = {s.value: 0 for s in EventState}
            for r in self.conn.execute("SELECT state, COUNT(*) AS n FROM events GROUP BY state"):
                by_state[r["state"]] = r["n"]
            total = sum(by_state.values())
            side = self.conn.execute("SELECT COUNT(*) AS n FROM side_effects").fetchone()["n"]
        return {"total": total, "by_state": by_state, "side_effect_count": int(side)}
