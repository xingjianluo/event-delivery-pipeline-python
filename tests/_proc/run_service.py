"""子进程入口：用环境变量配置启动真实 uvicorn 服务（供进程崩溃/重启测试）。

环境变量：
  EDP_TEST_DB            数据库文件路径（与父进程共享同一份本地库）
  EDP_TEST_PORT          父进程预分配的空闲端口
  EDP_TEST_POLL          worker 轮询间隔
  EDP_TEST_LEASE         租约超时秒数
  EDP_TEST_MAX_ATTEMPTS  最大尝试次数
  EDP_TEST_BASE_BACKOFF  退避基数

父进程把 socket fd 经 stdin 传入？不——这里直接 bind 预分配端口（父进程先
占坑拿到空闲端口后释放，存在理论竞争，测试环境可接受）。
"""
from __future__ import annotations

import os
import sys

# 确保以仓库根为导入根（python tests/_proc/run_service.py 时 __package__ 为空）
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import uvicorn  # noqa: E402

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402


def main() -> None:
    port = int(os.environ["EDP_TEST_PORT"])
    settings = Settings(
        db_path=os.environ["EDP_TEST_DB"],
        lease_timeout=float(os.environ.get("EDP_TEST_LEASE", "30")),
        max_attempts=int(os.environ.get("EDP_TEST_MAX_ATTEMPTS", "3")),
        base_backoff=float(os.environ.get("EDP_TEST_BASE_BACKOFF", "0.02")),
        backoff_factor=2.0,
        poll_interval=float(os.environ.get("EDP_TEST_POLL", "0.02")),
        worker_concurrency=int(os.environ.get("EDP_TEST_CONCURRENCY", "2")),
        recovery_on_startup=True,
    )
    app = create_app(settings, start_worker=True)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    main()
