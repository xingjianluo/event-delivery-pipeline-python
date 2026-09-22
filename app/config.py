"""可配置项：均通过环境变量覆盖，无需任何外部服务。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    db_path: str = os.environ.get("EDP_DB_PATH", "data/event_pipeline.db")
    max_attempts: int = _env_int("EDP_MAX_ATTEMPTS", 4)          # 含首次投递
    retry_base_delay: float = _env_float("EDP_RETRY_BASE_DELAY", 0.2)
    retry_max_delay: float = _env_float("EDP_RETRY_MAX_DELAY", 5.0)
    worker_count: int = _env_int("EDP_WORKER_COUNT", 4)
    poll_interval: float = _env_float("EDP_POLL_INTERVAL", 0.05)
    transient_simulation_attempts: int = _env_int("EDP_TRANSIENT_SIM_ATTEMPTS", 2)


settings = Settings()
