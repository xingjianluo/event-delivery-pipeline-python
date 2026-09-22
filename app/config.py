"""可配置项。

所有配置均可通过环境变量覆盖（前缀 EDP_，大写），例如
EDP_DB_PATH、EDP_MAX_ATTEMPTS、EDP_LEASE_TIMEOUT。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class Settings:
    db_path: str = "./data/events.db"
    lease_timeout: float = 30.0        # PROCESSING 卡住超过该秒数可被重新认领
    max_attempts: int = 3              # 含首次在内的最大尝试次数
    base_backoff: float = 0.05         # 指数退避基数（秒）
    backoff_factor: float = 2.0
    poll_interval: float = 0.05        # 后台轮询间隔（秒）
    recovery_on_startup: bool = True


def get_settings() -> Settings:
    kwargs: dict[str, object] = {}
    for f in fields(Settings):
        env = os.environ.get(f"EDP_{f.name.upper()}")
        if env is None:
            continue
        default = f.default
        if isinstance(default, bool):
            kwargs[f.name] = env.strip().lower() in {"1", "true", "yes", "on"}
        else:
            kwargs[f.name] = type(default)(env)
    return Settings(**kwargs)
