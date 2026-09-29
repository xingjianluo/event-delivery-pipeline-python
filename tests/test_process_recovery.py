"""真实进程级崩溃自愈：起一个 uvicorn 服务进程跑一轮，处理中途 kill -9，
再用同一个本地 SQLite 库重新拉起，验证状态/追踪标识/尝试次数/统计
不回退、不重复、不丢。最后再验多实例（两个进程同时跑同一个库）。

不依赖任何外部服务或凭据；HTTP 探测仅用标准库 urllib。
"""
from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.http_probe import request

ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ServerProc:
    def __init__(self, db_path: str, port: int, env_extra: dict | None = None):
        self.db_path = db_path
        self.port = port
        env = os.environ.copy()
        env.update({
            "EDP_DB_PATH": db_path,
            "EDP_LEASE_TIMEOUT": "0.5",
            "EDP_POLL_INTERVAL": "0.02",
            "EDP_BASE_BACKOFF": "0.02",
            "EDP_MAX_ATTEMPTS": "3",
            "PYTHONPATH": str(ROOT),
        })
        if env_extra:
            env.update(env_extra)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "main:app",
             "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
            cwd=str(ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def wait_ready(self, timeout: float = 15.0) -> None:
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out = self.proc.stdout.read() if self.proc.stdout else ""
                raise AssertionError(f"server exited early code={self.proc.returncode}\n{out}")
            try:
                status, _ = request("GET", self.base + "/", timeout=1.0)
                if status == 200:
                    return
            except Exception as exc:  # noqa: BLE001
                last_err = exc
            time.sleep(0.1)
        raise AssertionError(f"server not ready: {last_err}")

    def hard_kill(self) -> None:
        """SIGKILL：不给优雅停机机会，模拟进程在写终态前被直接干掉。"""
        self.proc.send_signal(signal.SIGKILL)
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)

    def stop(self) -> None:
        if self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=5)
            except Exception:  # noqa: BLE001
                self.hard_kill()


def _wait_state(base, event_id, states, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, body = request("GET", f"{base}/events/{event_id}", timeout=2.0)
        if status == 200 and body["state"] in states:
            return body
        time.sleep(0.05)
    raise AssertionError(f"event {event_id} never reached {states}; last={body}")


def test_real_process_kill_and_restart_same_db(tmp_path):
    db_path = str(tmp_path / "events.db")
    port1 = _free_port()
    port2 = _free_port()

    srv = ServerProc(db_path, port1)
    try:
        srv.wait_ready()
        # 一个慢事件（处理耗时 3s）+ 一个普通事件，共用调用方追踪标识
        status, r1 = request("POST", srv.base + "/events", {
            "event_id": "kill-me", "event_type": "t",
            "payload": {"simulate": {"mode": "slow", "delay": 1.0}},
        }, headers={"X-Correlation-ID": "biz-kill-1"})
        assert status == 201 and r1["trace_id"] == "biz-kill-1"
        request("POST", srv.base + "/events", {
            "event_id": "normal", "event_type": "t", "payload": {},
        }, headers={"X-Correlation-ID": "biz-kill-1"})

        # 慢事件一旦被认领进入 PROCESSING 就立刻杀（此时它还在 1s 睡眠中，
        # 终态未写）；不要等 normal，避免把慢事件“等到自然完成”。
        proc_view = _wait_state(srv.base, "kill-me", {"PROCESSING"})
        assert proc_view["attempts"] == 1
        time.sleep(0.2)  # 确保它正卡在下游睡眠里，而非刚认领完的瞬间
        srv.hard_kill()
    finally:
        srv.hard_kill()

    # 确保端口/进程已完全退出后重启
    time.sleep(0.3)

    # —— 用同一个本地库重新拉起服务进程 ——
    srv2 = ServerProc(db_path, port2)
    try:
        srv2.wait_ready()
        # 租约 0.5s 超时后遗留的 PROCESSING 被回收（attempts=2）。第二进程启动时
        # 被杀的那轮早已随 SIGKILL 消失，因此这里先确认确实发生了一次回收，再等成功。
        deadline = time.time() + 12
        reclaimed = False
        view = None
        while time.time() < deadline:
            _, view = request("GET", srv2.base + "/events/kill-me")
            _, hist = request("GET", srv2.base + "/events/kill-me/history")
            reclaimed = reclaimed or any(
                "reclaimed after stale lease" in h["reason"] for h in hist
            )
            if view["state"] == "SUCCEEDED":
                break
            time.sleep(0.05)
        assert view is not None and view["state"] == "SUCCEEDED"
        assert reclaimed, "重启后必须有一次崩溃租约回收留痕"
        assert view["trace_id"] == "biz-kill-1", "重启后追踪标识不串号"
        assert view["attempts"] == 2, "崩溃前 1 次 + 重启回收后 1 次"

        # 统计不回退、不重复、不丢
        _, stats = request("GET", srv2.base + "/stats")
        assert stats["total"] == 2
        assert stats["by_state"]["SUCCEEDED"] == 2
        assert stats["side_effect_count"] == 2, "崩溃在途事件副作用恰好一次"

        # 追踪反查：两个事件都能凭同一个调用方标识找到
        _, tv = request("GET", srv2.base + "/traces/biz-kill-1")
        assert tv["event_count"] == 2
        assert {e["state"] for e in tv["events"]} == {"SUCCEEDED"}
        assert tv["side_effect_count"] == 2

        # 历史里有一次“崩溃后回收”留痕，且每行 cid 都是调用方标识
        _, hist = request("GET", srv2.base + "/events/kill-me/history")
        assert {h["correlation_id"] for h in hist} == {"biz-kill-1"}
        assert any("reclaimed after stale lease" in h["reason"] for h in hist)
    finally:
        srv2.stop()


def test_two_processes_same_db_no_double_delivery(tmp_path):
    """多实例：两个服务进程同时打开同一个库并发投递，副作用不重复。"""
    db_path = str(tmp_path / "events.db")
    n = 12
    port_a = _free_port()
    port_b = _free_port()
    a = ServerProc(db_path, port_a)
    b = ServerProc(db_path, port_b)
    try:
        a.wait_ready()
        b.wait_ready()
        for i in range(n):
            payload = {"simulate": {"mode": "transient", "fail_times": 1}} if i % 3 == 0 else {}
            status, _ = request("POST", a.base + "/events", {
                "event_id": f"multi-{i}", "event_type": "t", "payload": payload,
            })
            assert status == 201

        deadline = time.time() + 15
        while time.time() < deadline:
            _, stats = request("GET", a.base + "/stats")
            if stats["by_state"]["SUCCEEDED"] == n and stats["total"] == n:
                break
            time.sleep(0.1)
        else:
            raise AssertionError(f"not all delivered: {stats}")

        _, stats = request("GET", b.base + "/stats")
        assert stats["by_state"]["SUCCEEDED"] == n
        assert stats["side_effect_count"] == n, "两个进程并发下每个事件副作用恰好一次"
        for i in range(n):
            _, ev = request("GET", a.base + f"/events/multi-{i}")
            assert ev["state"] == "SUCCEEDED"
    finally:
        a.hard_kill()
        b.hard_kill()


@pytest.mark.parametrize("extra", [
    {"EDP_MAX_ATTEMPTS": "1"},
    {"EDP_LEASE_TIMEOUT": "0", "EDP_MAX_ATTEMPTS": "2"},
])
def test_extreme_settings_real_process_converges(tmp_path, extra):
    """极端参数下真实进程也必须在有限时间内把持续失败事件送到 DEAD 终态。"""
    db_path = str(tmp_path / f"events-{extra['EDP_MAX_ATTEMPTS']}.db")
    port = _free_port()
    srv = ServerProc(db_path, port, env_extra=extra)
    try:
        srv.wait_ready()
        status, _ = request("POST", srv.base + "/events", {
            "event_id": "always-bad", "event_type": "t",
            "payload": {"simulate": {"mode": "transient", "fail_times": 999}},
        }, headers={"X-Correlation-ID": "biz-extreme"})
        assert status == 201
        view = _wait_state(srv.base, "always-bad", {"DEAD"}, timeout=12)
        assert view["attempts"] == int(extra["EDP_MAX_ATTEMPTS"])
        assert view["trace_id"] == "biz-extreme"
        # 再等一段时间确认它稳定停在 DEAD，不被无限重认领/回退
        time.sleep(0.6)
        _, again = request("GET", srv.base + "/events/always-bad")
        assert again["state"] == "DEAD" and again["attempts"] == view["attempts"]
    finally:
        srv.stop()
