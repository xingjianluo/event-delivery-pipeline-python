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

崩溃循环收敛：回收超时租约时，若该事件 attempts 已达 max_attempts，不再重新
投递，直接迁移到 DEAD 终态——因此无论 lease_timeout 多极端，attempts 都不会
超过上限，事件必然在有限步内落到可查询、可人工处置的终态。
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


class BatchIdempotencyConflict(Exception):
    """同一 idempotency_key 被用于不同的批量动作（action）。"""

    def __init__(self, key: str, existing_action: str, requested_action: str):
        super().__init__(
            f"idempotency_key={key!r} already used with action={existing_action!r}, "
            f"cannot reuse for {requested_action!r}"
        )
        self.key = key
        self.existing_action = existing_action
        self.requested_action = requested_action


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
                    reclaim_count  INTEGER NOT NULL DEFAULT 0,
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
            # 条件查询索引：按追踪标识反查、按状态筛选运维列表
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_correlation "
                "ON events(correlation_id)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_state_created "
                "ON events(state, created_at, event_id)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_correlation "
                "ON event_history(correlation_id, id)"
            )
            # 批量处置审计表：idempotency_key 唯一，保证同一批重复提交不重复处置
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS batch_ops (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    action          TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    result_json     TEXT NOT NULL,
                    at              REAL NOT NULL
                )
                """
            )
            # 旧库补列：reclaim_count 记录在途租约被回收的次数
            cols = {r["name"] for r in c.execute("PRAGMA table_info(events)").fetchall()}
            if "reclaim_count" not in cols:
                c.execute(
                    "ALTER TABLE events ADD COLUMN reclaim_count INTEGER NOT NULL DEFAULT 0"
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

        收敛保证：回收超时租约时，对已用满投递次数（attempts >=
        max_attempts，再回收即超额）的在途事件直接落 DEAD 终态并留痕，不再
        投递。为避免 ``lease_timeout=0`` 时把“正常处理中但尚未写终态”的健康
        事件误判成超时在途，实际可回收时刻不早于认领后 ``min_lease_grace``
        秒（默认 50ms，足以完成本地下游调用）。崩溃循环最多产生
        max_attempts 次投递后必然收敛到可人工处置的终态，attempts 不超上限。
        """
        stale_before = now - max(
            self.settings.lease_timeout, self.settings.min_lease_grace
        )
        with self._tx() as c:
            # 先把已用满尝试次数的超时在途事件强制收敛到 DEAD（同一事务内），
            # 再选下一条可投递事件，避免无限重新认领。
            while True:
                row = c.execute(
                    """
                    SELECT event_id, state, attempts, last_error, failure_kind,
                           correlation_id, leased_at, reclaim_count
                    FROM events
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
                if EventState(row["state"]) is EventState.PROCESSING \
                        and row["attempts"] >= self.settings.max_attempts:
                    self._force_dead_after_stale_attempts(c, row, now, correlation_id)
                    continue  # 该事件已收敛，选下一条
                break

            event_id = row["event_id"]
            prev = EventState(row["state"])
            new_attempt = row["attempts"] + 1
            new_reclaims = row["reclaim_count"] + (
                1 if prev is EventState.PROCESSING else 0
            )
            c.execute(
                """UPDATE events
                   SET state='PROCESSING', attempts=?, lease_owner=?, leased_at=?,
                       next_attempt_at=NULL, reclaim_count=?, updated_at=?
                   WHERE event_id=?""",
                (new_attempt, correlation_id, now, new_reclaims, now, event_id),
            )
            reason = "claimed for delivery" if prev is EventState.PENDING \
                else f"reclaimed after stale lease (worker crash/restart), " \
                     f"reclaim_no={new_reclaims}"
            self._history(
                c, event_id=event_id, correlation_id=row["correlation_id"],
                at=now, from_state=prev.value, to_state=EventState.PROCESSING,
                attempt=new_attempt, reason=reason, actor=f"worker:{correlation_id}",
            )
            return event_id

    def _force_dead_after_stale_attempts(
        self,
        c: sqlite3.Connection,
        row: sqlite3.Row,
        now: float,
        actor_cid: str,
    ) -> None:
        """崩溃循环已耗尽尝试次数：PROCESSING -> DEAD（不再投递、attempts 不回退）。

        UPDATE 带 state/attempts/leased_at 守卫并检查 rowcount：并发回收时
        只有一个线程能落终态并写历史，其他人静默跳过（该事件随后被重新查询
        时已是 DEAD，不会再进入候选）。
        """
        event_id = row["event_id"]
        attempts = row["attempts"]
        kind = row["failure_kind"] or "transient"
        last_error = row["last_error"] or "delivery never completed before lease expired"
        cur = c.execute(
            """UPDATE events
               SET state='DEAD', lease_owner=NULL, leased_at=NULL,
                   next_attempt_at=NULL, failure_kind=?, last_error=?,
                   disposition='dead_after_stale_attempts', updated_at=?
               WHERE event_id=? AND state='PROCESSING' AND attempts=?
                 AND leased_at=? AND reclaim_count=?""",
            (kind, last_error, now, event_id, attempts,
             row["leased_at"], row["reclaim_count"]),
        )
        if cur.rowcount == 0:
            return  # 已被其他回收线程迁移
        self._history(
            c, event_id=event_id, correlation_id=row["correlation_id"], at=now,
            from_state=EventState.PROCESSING.value, to_state=EventState.DEAD,
            attempt=attempts,
            reason=f"{kind}: {last_error} (stale lease; attempts {attempts} "
                   f"exhausted, moved to dead letter instead of reclaiming)",
            actor=f"worker:{actor_cid}",
        )

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
                       next_attempt_at=NULL, reclaim_count=0,
                       disposition='requeued', updated_at=?
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

    def batch_dispose(
        self,
        *,
        action: str,
        idempotency_key: str,
        event_ids: list[str] | None,
        states: list[str] | None,
        correlation_id: str | None,
        reason: str,
        actor: str,
        now: float,
    ) -> dict[str, Any]:
        """幂等批量人工处置（requeue/discard）。

        - 整个选择 + 逐行状态迁移 + 审计登记在**单个事务**内完成；
        - 只处置 DEAD/FAILED：SUCCEEDED/PENDING/PROCESSING/DISCARDED 即使命中
          选择器也一律进入 skipped，绝不把正常完成的事件卷进去；
        - 幂等：idempotency_key 唯一，重复提交直接返回首次结果（replayed=true），
          不重复迁移、不重复写历史、不产生额外副作用；
        - 每个被处置的事件都单独写一行 event_history（含 batch 与操作者）。
        """
        if action not in {"requeue", "discard"}:
            raise ValueError("action must be 'requeue' or 'discard'")
        ids_filter = list(dict.fromkeys(event_ids)) if event_ids else None
        disposable = (EventState.DEAD.value, EventState.FAILED.value)

        with self._tx() as c:
            existing = c.execute(
                "SELECT id, action, result_json FROM batch_ops WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["action"] != action:
                    raise BatchIdempotencyConflict(
                        idempotency_key, existing["action"], action
                    )
                result = json.loads(existing["result_json"])
                result["replayed"] = True
                return result

            # 1) 显式 id 中不存在的（不受其他筛选条件影响）
            not_found: list[str] = []
            if ids_filter:
                placeholders = ",".join("?" for _ in ids_filter)
                found = {
                    r["event_id"] for r in c.execute(
                        f"SELECT event_id FROM events WHERE event_id IN ({placeholders})",
                        ids_filter,
                    ).fetchall()
                }
                not_found = [eid for eid in ids_filter if eid not in found]

            # 2) 命中选择器的候选行
            clauses: list[str] = []
            params: list[Any] = []
            if ids_filter:
                clauses.append(f"event_id IN ({','.join('?' for _ in ids_filter)})")
                params.extend(ids_filter)
            if states:
                clauses.append(f"state IN ({','.join('?' for _ in states)})")
                params.extend(states)
            if correlation_id is not None:
                clauses.append("correlation_id = ?")
                params.append(correlation_id)
            if not clauses:
                raise ValueError("empty selector is not allowed for batch disposal")
            rows = c.execute(
                f"SELECT * FROM events WHERE {' AND '.join(clauses)} "
                "ORDER BY created_at, event_id",
                params,
            ).fetchall()

            changed: list[str] = []
            skipped: list[str] = []
            cur = c.execute(
                "INSERT INTO batch_ops(action, idempotency_key, result_json, at) "
                "VALUES (?,?,?,?)",
                (action, idempotency_key, "{}", now),
            )
            batch_id = int(cur.lastrowid)

            for row in rows:
                eid = row["event_id"]
                if row["state"] not in disposable:
                    skipped.append(eid)
                    continue
                if action == "requeue":
                    c.execute(
                        """UPDATE events
                           SET state='PENDING', attempts=0, lease_owner=NULL,
                               leased_at=NULL, next_attempt_at=NULL, reclaim_count=0,
                               disposition='requeued', updated_at=?
                           WHERE event_id=?""",
                        (now, eid),
                    )
                    self._history(
                        c, event_id=eid, correlation_id=row["correlation_id"], at=now,
                        from_state=row["state"], to_state=EventState.PENDING, attempt=0,
                        reason=f"batch#{batch_id} requeue: {reason}",
                        actor=f"batch#{batch_id}:{actor}",
                    )
                else:
                    c.execute(
                        "UPDATE events SET state='DISCARDED', disposition='discarded', "
                        "updated_at=? WHERE event_id=?",
                        (now, eid),
                    )
                    self._history(
                        c, event_id=eid, correlation_id=row["correlation_id"], at=now,
                        from_state=row["state"], to_state=EventState.DISCARDED,
                        attempt=row["attempts"],
                        reason=f"batch#{batch_id} discard: {reason}",
                        actor=f"batch#{batch_id}:{actor}",
                    )
                changed.append(eid)

            result = {
                "batch_id": batch_id,
                "action": action,
                "idempotency_key": idempotency_key,
                "changed": changed,
                "skipped": skipped,
                "not_found": not_found,
                "changed_count": len(changed),
                "skipped_count": len(skipped),
                "replayed": False,
                "at": now,
            }
            c.execute(
                "UPDATE batch_ops SET result_json=? WHERE id=?",
                (json.dumps(result, ensure_ascii=False, sort_keys=True), batch_id),
            )
            return result

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

    def history_by_correlation(self, correlation_id: str) -> list[HistoryItem]:
        """按调用方追踪标识反查其关联的全部流转记录（跨事件、按时间顺序）。"""
        with self._write_lock:
            rows = self.conn.execute(
                "SELECT * FROM event_history WHERE correlation_id=? ORDER BY id",
                (correlation_id,),
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

    def find_events(
        self,
        *,
        states: list[str] | None = None,
        correlation_id: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[EventView]:
        """按条件筛选事件：状态（可多个）、追踪标识、事件类型；稳定排序。"""
        clauses: list[str] = []
        params: list[Any] = []
        if states:
            clauses.append(f"state IN ({','.join('?' for _ in states)})")
            params.extend(states)
        if correlation_id is not None:
            clauses.append("correlation_id = ?")
            params.append(correlation_id)
        if event_type is not None:
            clauses.append("event_type = ?")
            params.append(event_type)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.extend([max(1, min(limit, 500)), max(0, offset)])
        with self._write_lock:
            rows = self.conn.execute(
                f"SELECT * FROM events {where} "
                "ORDER BY created_at, event_id LIMIT ? OFFSET ?",
                params,
            ).fetchall()
        return [self._to_view(r) for r in rows]

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
