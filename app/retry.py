"""重试策略：有上限的指数退避。

约定：attempt 从 1 开始计数（与存储层认领时自增的 attempts 一致），
``max_attempts`` 含首次尝试。第 attempt 次失败后：
- attempt >= max_attempts：返回 None，表示不再调度，进入终态；
- 否则延迟 = base_backoff * backoff_factor ** (attempt - 1)。
"""
from __future__ import annotations

from app.config import Settings


class RetryPolicy:
    def __init__(self, settings: Settings):
        self.settings = settings

    def next_delay(self, attempt: int) -> float | None:
        """第 attempt 次（从1计）失败后，下次尝试前的延迟；不再重试返回 None。"""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        if attempt >= self.settings.max_attempts:
            return None
        delay = self.settings.base_backoff * (
            self.settings.backoff_factor ** (attempt - 1)
        )
        return max(0.0, delay)
