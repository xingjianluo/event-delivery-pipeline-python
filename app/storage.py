"""基于 SQLite 的本地持久化层。

设计要点：
- WAL + busy_timeout，单文件、零外部依赖；每个线程持有独立连接；
- 领取（claim）使用 BEGIN IMMEDIATE 事务 + 条件 UPDATE，保证多线程下
  同一条事件只会被一个 worker 拿到；
- 所有状态推进均为带前置条件（属主锁 + 非终态）的单条 UPDATE（CAS），
  进程重启 / 并发下不会出现状态回退或重复终态写入；
- 下游副作用与事件同库，副作用表 UNIQUE(event_id, downstream_ref)，
  即使崩溃恢复后重投递也不可能产生第二条副作用。
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from .models import EventStatus

_DISPATCHABLE = (EventStatus.PENDING.value, EventStatus.RETRYING.value)
_TERMINAL = (EventStatus.DELIVERED.value, EventStatus.DEAD.value)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class Storage:
    def __init__(self, settings: Settings | None = None) -> None:
        from .config import settings as default_settings

        self.settings = settings or default_settings
        self._tls = threading.local()
        self._init_lock = threading.Lock()
        self.init_db()

    # ---------- 连接 ----------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.settings.db_path,
            timeout=30,
            isolation_level=None,  # 手工事务
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def conn(self) -> sqlite3.Connection:
        c = getattr(self._tls, "conn", None)
        if c is None:
            c = self._connect()
            self._tls.conn = c
        return c

    def init_db(self) -> None:
        with self._init_lock:
            import os

            os.makedirs(os.path.dirname(os.path.abspath(self.settings.db_path)), exist_ok=True)
            c = self.conn()
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS events (
                    event_id      TEXT PRIMARY KEY,
                    type          TEXT NOT NULL,
                    payload       TEXT NOT NULL,
                    idempotency_key TEXT,
                    content_hash  TEXT NOT NULL,
                    status        TEXT NOT NULL,
                    attempts      INTEGER NOT NULL DEFAULT 0,
                    max_attempts  INTEGER NOT NULL,
                    failure_kind  TEXT NOT NULL DEFAULT 'NONE',
                    last_error_code TEXT,
                    last_error_detail TEXT,
                    receive_correlation_id TEXT NOT NULL,
                    next_run_at   TEXT NOT NULL,
                    locked_by     TEXT,
                    locked_attempt INTEGER,
                    created_at    TEXT NOT NULL,
                    updated_at    TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_events_dispatch
                    ON events(status, next_run_at);

                CREATE TABLE IF NOT EXISTS attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    outcome TEXT NOT NULL,
                    http_status INTEGER,
                    error_code TEXT,
                    detail TEXT,
                    correlation_id TEXT NOT NULL,
                    happened_at TEXT NOT NULL,
                    UNIQUE(event_id, attempt_no)
                );

                CREATE TABLE IF NOT EXISTS event_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    stage TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT,
                    detail TEXT,
                    correlation_id TEXT NOT NULL,
                    happened_at TEXT NOT NULL,
                    UNIQUE(event_id, seq)
                );

                CREATE TABLE IF NOT EXISTS side_effects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    downstream_ref TEXT NOT NULL,
                    correlation_id TEXT NOT NULL,
                    happened_at TEXT NOT NULL,
                    UNIQUE(event_id, downstream_ref)
                );
                """
            )

    def close(self) -> None:
        c = getattr(self._tls, "conn", None)
        if c is not None:
            c.close()
            self._tls.conn = None

    # ---------- 接收 / 去重 ----------
    def insert_event_if_absent(
        self,
        *,
        event_id: str,
        event_type: str,
        payload_json: str,
        idempotency_key: str | None,
        content_hash: str,
        correlation_id: str,
        now: str,
    ) -> tuple[str, str | None]:
        """幂等插入。

        返回 (token, canonical_event_id)：
        - ('inserted', event_id)：首次接收；
        - ('duplicate', 已存在事件 id)：同一事件（event_id 或 idempotency_key
          命中）且内容哈希一致；
        - ('conflict', 已存在事件 id)：标识命中但内容哈希不一致。
        """
        c = self.conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute(
                "SELECT event_id, content_hash FROM events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if row is None and idempotency_key:
                row = c.execute(
                    "SELECT event_id, content_hash FROM events WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()

            if row is not None:
                token = "duplicate" if row["content_hash"] == content_hash else "conflict"
                c.commit()
                return token, row["event_id"]

            c.execute(
                """INSERT INTO events(event_id, type, payload, idempotency_key,
                       content_hash, status, attempts, max_attempts, failure_kind,
                       receive_correlation_id, next_run_at, created_at, updated_at)
                   VALUES (?,?,?,?,?, 'PENDING', 0, ?, 'NONE', ?, ?, ?, ?)""",
                (
                    event_id,
                    event_type,
                    payload_json,
                    idempotency_key,
                    content_hash,
                    self.settings.max_attempts,
                    correlation_id,
                    now,
                    now,
                    now,
                ),
            )
            self._insert_history(
                c,
                event_id,
                "RECEIVED",
                None,
                EventStatus.PENDING.value,
                f"event accepted; idempotency_key={idempotency_key!r}",
                correlation_id,
                now,
            )
            c.commit()
            return "inserted", event_id
        except Exception:
            c.rollback()
            raise

    def get_event_row(self, event_id: str) -> sqlite3.Row | None:
        return self.conn().execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()

    # ---------- 投递领取 / 状态推进 ----------
    def claim_next_event(self, worker_id: str, now: str) -> sqlite3.Row | None:
        """原子领取一条到期且未锁定的事件。无则返回 None。"""
        c = self.conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute(
                """SELECT event_id FROM events
                   WHERE status IN (?, ?) AND locked_by IS NULL AND next_run_at <= ?
                   ORDER BY next_run_at ASC LIMIT 1""",
                (*_DISPATCHABLE, now),
            ).fetchone()
            if row is None:
                c.commit()
                return None
            event_id = row["event_id"]
            cur = c.execute(
                """UPDATE events
                      SET locked_by = ?,
                          locked_attempt = attempts + 1,
                          updated_at = ?
                    WHERE event_id = ? AND locked_by IS NULL
                      AND status IN (?, ?)""",
                (worker_id, now, event_id, *_DISPATCHABLE),
            )
            if cur.rowcount != 1:
                c.commit()
                return None
            c.commit()
            return self.get_event_row(event_id)
        except Exception:
            c.rollback()
            raise

    def start_attempt(self, event_id: str, attempt_no: int, correlation_id: str, now: str) -> None:
        """记录一次投递尝试开始（便于观测在途尝试；非状态推进）。"""
        c = self.conn()
        c.execute(
            """INSERT INTO attempts(event_id, attempt_no, outcome, correlation_id, happened_at)
               VALUES (?, ?, 'RUNNING', ?, ?)
               ON CONFLICT(event_id, attempt_no) DO NOTHING""",
            (event_id, attempt_no, correlation_id, now),
        )

    def record_attempt(
        self,
        c: sqlite3.Connection,
        *,
        event_id: str,
        attempt_no: int,
        outcome: str,
        http_status: int | None,
        error_code: str | None,
        detail: str | None,
        correlation_id: str,
        now: str,
    ) -> None:
        """落盘尝试结果（由调用方在事务内执行，与状态推进原子提交）。"""
        c.execute(
            """UPDATE attempts
                  SET outcome = ?, http_status = ?, error_code = ?, detail = ?,
                      correlation_id = ?, happened_at = ?
                WHERE event_id = ? AND attempt_no = ?""",
            (outcome, http_status, error_code, detail, correlation_id, now,
             event_id, attempt_no),
        )

    def _finish(
        self,
        *,
        event_id: str,
        worker_id: str,
        attempt_no: int,
        to_status: EventStatus,
        attempts_done: int,
        failure_kind: str,
        outcome: str,
        http_status: int | None,
        error_code: str | None,
        detail: str | None,
        next_run_at: str,
        stage: str,
        correlation_id: str,
        now: str,
    ) -> bool:
        """带属主与在途尝试校验的状态推进（CAS），尝试结果与历史同事务提交。"""
        c = self.conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute(
                "SELECT status, attempts FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if row is None:
                c.rollback()
                return False
            if row["status"] in _TERMINAL:
                # 已终态：绝不重复推进，也不重复副作用
                c.rollback()
                return False
            cur = c.execute(
                """UPDATE events
                      SET status = ?, attempts = ?, failure_kind = ?,
                          last_error_code = ?, last_error_detail = ?,
                          next_run_at = ?, locked_by = NULL, locked_attempt = NULL,
                          updated_at = ?
                    WHERE event_id = ? AND locked_by = ? AND locked_attempt = ?
                      AND status NOT IN (?, ?)""",
                (
                    to_status.value,
                    attempts_done,
                    failure_kind,
                    error_code,
                    detail,
                    next_run_at,
                    now,
                    event_id,
                    worker_id,
                    attempt_no,
                    *_TERMINAL,
                ),
            )
            if cur.rowcount != 1:
                c.rollback()
                return False
            self.record_attempt(
                c, event_id=event_id, attempt_no=attempt_no, outcome=outcome,
                http_status=http_status, error_code=error_code, detail=detail,
                correlation_id=correlation_id, now=now,
            )
            self._insert_history(
                c, event_id, stage, row["status"], to_status.value,
                detail if detail else error_code, correlation_id, now
            )
            c.commit()
            return True
        except Exception:
            c.rollback()
            raise

    def advance_to_delivered(
        self, *, event_id: str, worker_id: str, attempt_no: int,
        attempts_done: int, correlation_id: str, now: str,
    ) -> bool:
        return self._finish(
            event_id=event_id, worker_id=worker_id, attempt_no=attempt_no,
            to_status=EventStatus.DELIVERED, attempts_done=attempts_done,
            failure_kind="NONE", outcome="DELIVERED", http_status=200,
            error_code=None, detail=None,
            next_run_at=now, stage="DELIVERED", correlation_id=correlation_id, now=now,
        )

    def advance_to_retry(
        self, *, event_id: str, worker_id: str, attempt_no: int,
        attempts_done: int, next_run_at: str, error_code: str,
        detail: str | None, correlation_id: str, now: str,
    ) -> bool:
        return self._finish(
            event_id=event_id, worker_id=worker_id, attempt_no=attempt_no,
            to_status=EventStatus.RETRYING, attempts_done=attempts_done,
            failure_kind="TRANSIENT", outcome="TRANSIENT", http_status=503,
            error_code=error_code, detail=detail,
            next_run_at=next_run_at, stage="RETRY_SCHEDULED",
            correlation_id=correlation_id, now=now,
        )

    def advance_to_dead(
        self, *, event_id: str, worker_id: str, attempt_no: int,
        attempts_done: int, failure_kind: str, outcome: str,
        http_status: int, error_code: str,
        detail: str | None, correlation_id: str, now: str,
        exhausted: bool,
    ) -> bool:
        stage = "RETRIES_EXHAUSTED" if exhausted else "MOVED_TO_DEAD"
        return self._finish(
            event_id=event_id, worker_id=worker_id, attempt_no=attempt_no,
            to_status=EventStatus.DEAD, attempts_done=attempts_done,
            failure_kind=failure_kind, outcome=outcome, http_status=http_status,
            error_code=error_code, detail=detail,
            next_run_at=now, stage=stage, correlation_id=correlation_id, now=now,
        )

    # ---------- 历史 / 副作用 ----------
    def _insert_history(self, c, event_id, stage, from_status, to_status,
                        detail, correlation_id, now) -> None:
        c.execute(
            """INSERT INTO event_history(event_id, seq, stage, from_status,
                   to_status, detail, correlation_id, happened_at)
               SELECT ?, COALESCE(MAX(seq), 0) + 1, ?, ?, ?, ?, ?, ?
                 FROM event_history WHERE event_id = ?""",
            (event_id, stage, from_status, to_status, detail,
             correlation_id, now, event_id),
        )

    def add_history(self, *, event_id: str, stage: str, from_status: str | None,
                    to_status: str | None, detail: str | None,
                    correlation_id: str, now: str) -> None:
        c = self.conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            self._insert_history(
                c, event_id, stage, from_status, to_status, detail, correlation_id, now
            )
            c.commit()
        except Exception:
            c.rollback()
            raise

    def list_history(self, event_id: str) -> list[sqlite3.Row]:
        return list(self.conn().execute(
            "SELECT * FROM event_history WHERE event_id = ? ORDER BY seq ASC",
            (event_id,),
        ))

    def list_attempts(self, event_id: str) -> list[sqlite3.Row]:
        return list(self.conn().execute(
            "SELECT * FROM attempts WHERE event_id = ? ORDER BY attempt_no ASC",
            (event_id,),
        ))

    def record_side_effect(self, *, event_id: str, downstream_ref: str,
                           correlation_id: str, at: str) -> bool:
        """幂等记录副作用；已存在同一 ref 时返回 False（绝不重复产生副作用）。"""
        cur = self.conn().execute(
            """INSERT OR IGNORE INTO side_effects
                   (event_id, downstream_ref, correlation_id, happened_at)
               VALUES (?, ?, ?, ?)""",
            (event_id, downstream_ref, correlation_id, at),
        )
        return cur.rowcount == 1

    def count_side_effects(self, event_id: str | None = None) -> int:
        c = self.conn()
        if event_id is None:
            return c.execute("SELECT COUNT(*) AS n FROM side_effects").fetchone()["n"]
        return c.execute(
            "SELECT COUNT(*) AS n FROM side_effects WHERE event_id = ?", (event_id,)
        ).fetchone()["n"]

    def side_effect_exists(self, event_id: str, downstream_ref: str) -> bool:
        return self.conn().execute(
            "SELECT 1 FROM side_effects WHERE event_id = ? AND downstream_ref = ?",
            (event_id, downstream_ref),
        ).fetchone() is not None

    # ---------- 死信 / 统计 / 恢复 ----------
    def list_dead_events(self) -> list[sqlite3.Row]:
        return list(self.conn().execute(
            "SELECT * FROM events WHERE status = 'DEAD' ORDER BY updated_at ASC"
        ))

    def replay_dead_event(self, event_id: str, correlation_id: str, now: str,
                          extra_budget: int) -> str:
        """人工处置：把死信重新入队。

        不清零任何历史：attempts 保留（尝试编号累计递增，历史 attempts 行不被覆盖），
        仅在原预算之上追加 extra_budget 次投递机会。
        """
        c = self.conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute(
                "SELECT status, attempts, max_attempts FROM events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if row is None:
                c.rollback()
                return "not_found"
            if row["status"] != EventStatus.DEAD.value:
                c.rollback()
                return "not_dead"
            c.execute(
                """UPDATE events
                      SET status = 'PENDING', failure_kind = 'NONE',
                          last_error_code = NULL, last_error_detail = NULL,
                          next_run_at = ?, locked_by = NULL, locked_attempt = NULL,
                          max_attempts = attempts + ?,
                          updated_at = ?
                    WHERE event_id = ? AND status = 'DEAD'""",
                (now, extra_budget, now, event_id),
            )
            self._insert_history(
                c, event_id, "MANUAL_REPLAY",
                EventStatus.DEAD.value, EventStatus.PENDING.value,
                f"dead event re-queued by operator; +{extra_budget} attempt budget",
                correlation_id, now,
            )
            c.commit()
            return "requeued"
        except Exception:
            c.rollback()
            raise

    def stats(self) -> dict[str, int]:
        c = self.conn()
        rows = {
            r["status"]: r["n"]
            for r in c.execute("SELECT status, COUNT(*) AS n FROM events GROUP BY status")
        }
        out = {
            "total": sum(rows.values()),
            "pending": rows.get("PENDING", 0),
            "retrying": rows.get("RETRYING", 0),
            "delivered": rows.get("DELIVERED", 0),
            "dead": rows.get("DEAD", 0),
        }
        out["total_attempts"] = c.execute(
            "SELECT COUNT(*) AS n FROM attempts WHERE outcome != 'RUNNING'"
        ).fetchone()["n"]
        for kind, col in (
            ("TRANSIENT", "transient_failures"),
            ("BUSINESS", "business_rejections"),
            ("UNKNOWN", "unknown_failures"),
        ):
            out[col] = c.execute(
                "SELECT COUNT(*) AS n FROM attempts WHERE outcome = ?", (kind,)
            ).fetchone()["n"]
        out["side_effects"] = c.execute(
            "SELECT COUNT(*) AS n FROM side_effects"
        ).fetchone()["n"]
        return out

    def recover_stuck(self, now: str) -> int:
        """重启恢复：把崩溃时仍被锁定的非终态事件解锁并提前到期。

        - attempts / failure_kind 保留，不回退任何已完成事实；
        - 若已达最大尝试次数，worker 重新领取后会立即将其判定为 DEAD；
        - 副作用表不受影响，重投递天然幂等。
        """
        c = self.conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            stuck = c.execute(
                """SELECT event_id, status FROM events
                   WHERE locked_by IS NOT NULL AND status NOT IN (?, ?)""",
                _TERMINAL,
            ).fetchall()
            for r in stuck:
                c.execute(
                    """UPDATE events SET locked_by = NULL, locked_attempt = NULL,
                           next_run_at = ?, updated_at = ?
                         WHERE event_id = ?""",
                    (now, now, r["event_id"]),
                )
                self._insert_history(
                    c, r["event_id"], "RECOVERED_AFTER_RESTART",
                    r["status"], r["status"],
                    "worker lock cleared during process recovery",
                    "recovery", now,
                )
            c.commit()
            return len(stuck)
        except Exception:
            c.rollback()
            raise
