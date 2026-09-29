"""真实进程级崩溃自愈：起 uvicorn 子进程 -> 运行中 kill -9 -> 同库重启。

与 test_restart.py（同进程内换连接模拟重启）互补，这里验证的是：
- 真正的独立 OS 进程被 SIGKILL 后，WAL 中的已提交数据不丢、统计不回退；
- 停在 PROCESSING 的事件在租约超时后被新进程回收，cid/attempts 不串号；
- 两个真实服务实例 + 两个后台 worker 同时跑一份库，副作用仍然恰好一次；
- 极端参数（lease_timeout≈0、max_attempts=2、backoff=0）下崩溃循环收敛
  到 DEAD，而不是无限重新认领或状态回退。
"""
from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "tests" / "_proc" / "run_service.py"


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Service:
    def __init__(self, db_path: str, port: int, env_extra: dict[str, str] | None = None):
        self.db_path = db_path
        self.port = port
        env = os.environ.copy()
        env["EDP_TEST_DB"] = db_path
        env["EDP_TEST_PORT"] = str(port)
        env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        if env_extra:
            env.update(env_extra)
        self.proc = subprocess.Popen(  # noqa: S603
            [sys.executable, str(RUNNER)],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def wait_ready(self, timeout: float = 15.0) -> None:
        deadline = time.time() + timeout
        last_exc: Exception | None = None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out = self.proc.stdout.read().decode("utf-8", "replace") if self.proc.stdout else ""
                raise AssertionError(f"service exited early code={self.proc.returncode}\n{out}")
            try:
                r = httpx.get(f"{self.base}/stats", timeout=1.0)
                if r.status_code == 200:
                    return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
            time.sleep(0.1)
        raise AssertionError(f"service not ready: {last_exc}")

    def kill(self) -> None:
        """SIGKILL 整个进程组：模拟 kill -9，没有任何优雅收尾。"""
        if self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGKILL)
        self.proc.wait(timeout=10)

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.kill()


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "events.db")


def _wait_state(base: str, event_id: str, states, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = httpx.get(f"{base}/events/{event_id}", timeout=2.0).json()
        if v["state"] in states:
            return v
        time.sleep(0.1)
    raise AssertionError(f"event {event_id} never reached {states}")


def _wait_no_inflight(base: str, total: int, timeout: float = 25.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = httpx.get(f"{base}/stats", timeout=2.0).json()
        if s["total"] == total and s["by_state"]["PENDING"] == 0 \
                and s["by_state"]["PROCESSING"] == 0:
            return s
        time.sleep(0.15)
    raise AssertionError("events did not all reach terminal states")


def _wait_marker(db_path: str, marker: str, timeout: float = 10.0) -> str:
    path = str(Path(db_path).parent / f"hang_{marker}.lock")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return path
        time.sleep(0.03)
    raise AssertionError(f"hang marker {path} never appeared")


def test_real_process_kill_and_restart_recovers_once(db_path):
    port1, port2 = _free_port(), _free_port()
    svc = Service(db_path, port1, {"EDP_TEST_LEASE": "3.0", "EDP_TEST_POLL": "0.02",
                                   "EDP_TEST_CONCURRENCY": "1"})
    svc.wait_ready()
    try:
        # 一条正常事件、一条将在下游处理中途被杀、一条等待中的事件
        with httpx.Client(base_url=svc.base, timeout=5.0) as cli:
            r = cli.post("/events", json={
                "event_id": "ok-1", "event_type": "t", "payload": {},
                "correlation_id": "trace-restart",
            })
            assert r.status_code == 201 and r.json()["correlation_id"] == "trace-restart"
            cli.post("/events", json={
                "event_id": "hang-1", "event_type": "t",
                "payload": {"simulate": {"mode": "hang", "marker": "m1", "timeout": 120}},
                "correlation_id": "trace-restart",
            })
            cli.post("/events", json={
                "event_id": "wait-1", "event_type": "t", "payload": {},
                "correlation_id": "trace-other",
            })

        marker_path = _wait_marker(db_path, "m1")
        # 确认它停在 PROCESSING（attempts=1），此时直接 SIGKILL
        deadline = time.time() + 5
        while time.time() < deadline:
            v = httpx.get(f"{svc.base}/events/hang-1", timeout=2.0).json()
            if v["state"] == "PROCESSING" and v["attempts"] == 1:
                break
            time.sleep(0.03)
        assert v["state"] == "PROCESSING" and v["attempts"] == 1, v
        svc.kill()
    finally:
        if svc.proc.poll() is None:
            svc.kill()

    # 清掉被杀进程残留的 hang 标记（其残留线程仍持有文件句柄，但写操作是
    # 截断重写，不影响新进程；删掉可确保“标记出现”一定代表新进程已进入下游）
    stale_lock = Path(db_path).parent / "hang_m1.lock"
    if stale_lock.exists():
        stale_lock.unlink()

    # —— 用同一个本地库重新起一个真实进程 ——
    # 模拟“下游故障已恢复”：放置 release 信号，回收后的投递立即成功
    release_path = Path(db_path).parent.joinpath("hang_m1.release")
    assert not release_path.exists()
    release_path.write_text("recovered", encoding="utf-8")
    svc2 = Service(db_path, port2, {"EDP_TEST_LEASE": "3.0", "EDP_TEST_POLL": "0.02",
                                    "EDP_TEST_CONCURRENCY": "2"})
    svc2.wait_ready()
    try:
        # 已提交数据不丢、统计不回退：三条都在；ok-1 已成功且副作用已落库
        s0 = httpx.get(f"{svc2.base}/stats", timeout=2.0).json()
        assert s0["total"] == 3
        assert s0["by_state"]["SUCCEEDED"] >= 1
        ok1 = httpx.get(f"{svc2.base}/events/ok-1", timeout=2.0).json()
        assert ok1["state"] == "SUCCEEDED" and ok1["correlation_id"] == "trace-restart"
        # hang-1 仍是崩溃前的在途状态：attempts 未回退、未冒进、cid 未变
        hv = httpx.get(f"{svc2.base}/events/hang-1", timeout=2.0).json()
        assert hv["state"] == "PROCESSING" and hv["attempts"] == 1, hv

        v = _wait_state(svc2.base, "hang-1", ["SUCCEEDED"], timeout=20)
        assert v["correlation_id"] == "trace-restart", "重启后追踪标识不串号"
        assert v["attempts"] == 2, "回收后第二次尝试成功，attempts 单调不回退"

        w = _wait_state(svc2.base, "wait-1", ["SUCCEEDED"])
        assert w["correlation_id"] == "trace-other"

        s = _wait_no_inflight(svc2.base, total=3)
        assert s["by_state"]["SUCCEEDED"] == 3
        assert s["side_effect_count"] == 3, "崩溃前后合计副作用恰好一次/事件"

        # 按调用方追踪标识反查：一次业务操作关联的两个事件都能对上
        linked = httpx.get(f"{svc2.base}/correlations/trace-restart/events", timeout=2.0).json()
        assert {e["event_id"] for e in linked["items"]} == {"ok-1", "hang-1"}
        assert all(e["state"] == "SUCCEEDED" for e in linked["items"])
    finally:
        svc2.stop()


def test_two_real_instances_concurrent_no_duplicates(db_path):
    port_a, port_b = _free_port(), _free_port()
    common = {"EDP_TEST_LEASE": "30", "EDP_TEST_POLL": "0.01"}
    a = Service(db_path, port_a, common)
    b = Service(db_path, port_b, common)
    a.wait_ready()
    b.wait_ready()
    n = 30
    try:
        # 事件交替提交到两个实例；1/3 带一次瞬时失败
        with httpx.Client(timeout=5.0) as cli:
            for i in range(n):
                base = a.base if i % 2 == 0 else b.base
                payload = (
                    {"simulate": {"mode": "transient", "fail_times": 1}}
                    if i % 3 == 0 else {}
                )
                r = cli.post(f"{base}/events", json={
                    "event_id": f"mi-{i}", "event_type": "bulk",
                    "payload": payload, "correlation_id": f"biz-op-{i % 5}",
                })
                assert r.status_code == 201

        s = _wait_no_inflight(a.base, total=n, timeout=40)
        assert s["by_state"]["SUCCEEDED"] == n
        assert s["side_effect_count"] == n, "两个实例并发投递，副作用仍恰好一次/事件"

        # 追踪反查：biz-op-0 关联 i=0,5,10,... 共 6 个事件
        linked = httpx.get(f"{a.base}/correlations/biz-op-0/events", timeout=2.0).json()
        assert linked["count"] == 6
        assert {e["event_id"] for e in linked["items"]} == {f"mi-{i}" for i in range(0, n, 5)}
    finally:
        a.stop()
        b.stop()


def test_extreme_params_crash_loop_converges_to_dead(db_path):
    # lease≈0、退避=0、上限 2 次：连续两轮“认领后被杀”后必须收敛到 DEAD。
    extra = {"EDP_TEST_LEASE": "0.05", "EDP_TEST_POLL": "0.01",
             "EDP_TEST_MAX_ATTEMPTS": "2", "EDP_TEST_BASE_BACKOFF": "0.0",
             "EDP_TEST_CONCURRENCY": "2"}
    p1, p2, p3 = _free_port(), _free_port(), _free_port()

    s1 = Service(db_path, p1, extra)
    s1.wait_ready()
    try:
        with httpx.Client(base_url=s1.base, timeout=5.0) as cli:
            cli.post("/events", json={
                "event_id": "loop-1", "event_type": "t",
                "payload": {"simulate": {"mode": "hang", "marker": "m2", "timeout": 120}},
                "correlation_id": "trace-extreme",
            })
        _wait_marker(db_path, "m2")
        deadline = time.time() + 5
        while time.time() < deadline:
            v = httpx.get(f"{s1.base}/events/loop-1", timeout=2.0).json()
            if v["state"] == "PROCESSING" and v["attempts"] == 1:
                break
            time.sleep(0.02)
        assert v["state"] == "PROCESSING" and v["attempts"] == 1
        s1.kill()
    finally:
        if s1.proc.poll() is None:
            s1.kill()

    # 第二实例在租约超时后回收（attempt 2）；多 worker 下它可能：
    # a) 回收后挂在 hang 上（PROCESSING/2），或
    # b) 立即被另一线程判定尝试耗尽，直接收敛 DEAD/2。
    # 两条路径都合法；a 时杀进程，b 时最终收敛由后续断言保证。
    if os.path.exists(_wait_marker(db_path, "m2")):
        os.remove(_wait_marker(db_path, "m2"))
    s2 = Service(db_path, p2, extra)
    s2.wait_ready()
    converged_early = False
    try:
        deadline = time.time() + 8
        while time.time() < deadline:
            v = httpx.get(f"{s2.base}/events/loop-1", timeout=2.0).json()
            if v["attempts"] == 2:
                break
            time.sleep(0.03)
        assert v["attempts"] == 2, v
        if v["state"] == "DEAD":
            converged_early = True
        else:
            assert v["state"] == "PROCESSING", v
            s2.kill()
    finally:
        if s2.proc.poll() is None:
            s2.kill()

    # 第三实例：若上一程已收敛则复核终态；否则 attempts 已达上限，
    # 不得再认领投递，直接强制收敛到 DEAD。
    if not converged_early and os.path.exists(_wait_marker(db_path, "m2")):
        os.remove(_wait_marker(db_path, "m2"))
    s3 = Service(db_path, p3, extra)
    s3.wait_ready()
    try:
        v = _wait_state(s3.base, "loop-1", ["DEAD"], timeout=15)
        assert v["attempts"] == 2, "attempts 不得超过 max_attempts"
        assert v["correlation_id"] == "trace-extreme"
        hist = httpx.get(f"{s3.base}/events/loop-1/history", timeout=2.0).json()
        assert hist[-1]["to_state"] == "DEAD" and "exhausted" in hist[-1]["reason"]
        # 收敛后保持终态：再等两个租约周期也不得回退/重新认领
        time.sleep(0.5)
        v2 = httpx.get(f"{s3.base}/events/loop-1", timeout=2.0).json()
        assert v2["state"] == "DEAD", "终态不得回退到 PROCESSING/PENDING"
        assert v2["attempts"] == 2
        # DEAD 仍可查询、可人工处置
        r = httpx.post(f"{s3.base}/events/loop-1/requeue",
                       headers={"X-Correlation-ID": "ops-user"}, timeout=2.0)
        assert r.status_code == 200 and r.json()["state"] == "PENDING"
        assert r.json()["attempts"] == 0
    finally:
        s3.stop()
