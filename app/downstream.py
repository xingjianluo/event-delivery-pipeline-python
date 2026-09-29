"""模拟下游消费者：按 event_type / payload 脚本产生确定性结果。

不访问任何外部服务。失败脚本约定（payload.simulate）：
- 缺省 / "succeed"：成功；
- {"mode": "transient", "fail_times": N}：前 N 次抛 TransientError，之后成功；
- {"mode": "business", "reason": ...}：始终业务拒绝；
- {"mode": "unknown", "reason": ...}：始终抛不可归类异常；
- {"mode": "hang", "marker": M, "timeout": S}：阻塞在处理中途（事件已认领、
  终态未写），用于在**独立进程**里真实模拟 worker 崩溃/被 kill：用文件锁
  ``./data/hang_<marker>.lock`` 作为跨进程同步点，测试端等锁出现后直接杀掉
  进程，事件即确定性地停在 PROCESSING。仅用于本地演练/测试，不访问网络。

内存中的 success 计数仅用于演示/测试观测；副作用的**权威持久记录**
是存储层 side_effects 表（与 SUCCEEDED 同事务，恰好一次）。
"""
from __future__ import annotations

import os
import threading
import time

from app.errors import BusinessRejected, TransientError, UnknownFailure


class DownstreamHandler:
    def __init__(self, hang_dir: str | None = None) -> None:
        self.side_effects: dict[str, int] = {}
        self._attempts: dict[str, int] = {}
        self._lock = threading.Lock()
        self._hang_dir = hang_dir

    def handle(self, event_id: str, event_type: str, payload: dict) -> None:
        """成功则副作用恰好记录一次；失败抛出可归类异常。"""
        sim = payload.get("simulate")
        mode = sim if isinstance(sim, str) else (sim or {}).get("mode", "succeed")

        if mode == "business":
            reason = sim.get("reason", "business rejected") if isinstance(sim, dict) else "business rejected"
            raise BusinessRejected(reason)
        if mode == "unknown":
            reason = sim.get("reason", "unclassifiable failure") if isinstance(sim, dict) else "unclassifiable failure"
            raise UnknownFailure(reason)

        with self._lock:
            seen = self._attempts.get(event_id, 0) + 1
            self._attempts[event_id] = seen
            if mode == "transient":
                fail_times = int(sim.get("fail_times", 1)) if isinstance(sim, dict) else 1
                if seen <= fail_times:
                    raise TransientError(
                        f"simulated transient failure {seen}/{fail_times}"
                    )

        # hang 在锁外阻塞（不占用 handler 锁），且每次进入下游都会挂住——
        # 模拟“调用一直不返回”的下游，供进程被杀/租约回收演练使用。
        if mode == "hang" and isinstance(sim, dict):
            self._hang(sim)

        with self._lock:
            # 成功：产生（模拟）下游副作用
            self.side_effects[event_id] = self.side_effects.get(event_id, 0) + 1

    def _hang(self, sim: dict) -> None:
        """在处理中途阻塞：创建标记锁文件后睡到 timeout（或被进程杀掉）。

        若同目录存在 ``hang_<marker>.release`` 文件，视为“下游已恢复”，
        立即放行（调用随即成功），用于模拟崩溃回收后下游恢复的场景。
        """
        marker = str(sim.get("marker", "default"))
        timeout = float(sim.get("timeout", 30.0))
        directory = self._hang_dir or os.path.join(".", "data")
        os.makedirs(directory, exist_ok=True)
        release = os.path.join(directory, f"hang_{marker}.release")
        if os.path.exists(release):
            return
        path = os.path.join(directory, f"hang_{marker}.lock")
        # 创建标记文件作为“已进入下游、终态尚未写”的跨进程信号；
        # 文件可能是上一个被杀进程的残留线程留下的，重写即可（不要求独占）。
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"pid={os.getpid()} at={time.time()}")
            f.flush()
        # 每 50ms 复查一次释放信号，避免等到 timeout
        deadline = time.time() + timeout
        while time.time() < deadline:
            if os.path.exists(release):
                return
            time.sleep(0.05)

    def side_effect_count(self, event_id: str) -> int:
        with self._lock:
            return self.side_effects.get(event_id, 0)

    def reset(self) -> None:
        with self._lock:
            self.side_effects.clear()
            self._attempts.clear()
