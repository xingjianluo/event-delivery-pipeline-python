"""服务入口：uvicorn main:app

环境变量配置见 README.md（EDP_*）。本地存储默认位于 data/event_pipeline.db。
"""
from __future__ import annotations

import logging

from app.api import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = create_app()
