"""模拟下游消费者：按 event_type / payload 脚本产生确定性结果。

不访问任何外部服务。失败脚本约定（payload.simulate）：
- 缺省 / "succeed"：成功；
- {"mode": "transient", "fail_times": N}：前 N 次抛 TransientError，之后成功；
- {"mode": "business", "reason": ...}：始终业务拒绝；
- {"mode": "unknown", "reason": ...}：始终抛不可归类异常；
- {"mode": "slow", "delay": 秒数}：处理期间确定性睡眠（用于在真实进程上制造
  “认领后、写终态前被 kill”的崩溃在途窗口），睡眠结束后成功。

内存中的 success 计数仅用于演示/测试观测；副作用的**权威持久记录**
是存储层 side_effects 表（与 SUCCEEDED 同事务，恰好一次）。
"""
from __future__ import annotations

import threading
import time

from app.errors import BusinessRejected, TransientError, UnknownFailure


class DownstreamHandler:
    def __init__(self) -> None:
        self.side_effects: dict[str, int] = {}
        self._attempts: dict[str, int] = {}
        self._lock = threading.Lock()

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
        if mode == "slow":
            simd = sim if isinstance(sim, dict) else {}
            delay = float(simd.get("delay", 1.0))
            then = simd.get("then", "succeed")
            time.sleep(max(0.0, delay))
            if then == "crash_fail":
                # 模拟“慢处理之后仍失败”（供测试）；默认睡完即走成功路径
                raise TransientError(f"slow handler failed after {delay}s")
            # 睡眠结束后落到下面的成功路径

        with self._lock:
            seen = self._attempts.get(event_id, 0) + 1
            self._attempts[event_id] = seen
            if mode == "transient":
                fail_times = int(sim.get("fail_times", 1)) if isinstance(sim, dict) else 1
                if seen <= fail_times:
                    raise TransientError(
                        f"simulated transient failure {seen}/{fail_times}"
                    )
            # 成功：产生（模拟）下游副作用
            self.side_effects[event_id] = self.side_effects.get(event_id, 0) + 1

    def side_effect_count(self, event_id: str) -> int:
        with self._lock:
            return self.side_effects.get(event_id, 0)

    def reset(self) -> None:
        with self._lock:
            self.side_effects.clear()
            self._attempts.clear()
