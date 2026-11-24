"""作业进程归属登记与核实测试（普通文件模拟 procfs / cgroup）。"""
from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.jobs import JobError, JobNotFoundError, JobRegistry, ProcIdentity
from app.main import create_app
from app.procfs import read_proc_stat
from app.service import TransferService

A, B = "groupA", "groupB"


# ---------------------------------------------------------------- 真实 procfs


def test_parse_real_proc_stat_of_running_process():
    """解析真实 /proc：对照内核给出的 pid/ppid/starttime。"""
    st = read_proc_stat("/proc", os.getpid())
    assert st is not None
    assert st.pid == os.getpid()
    assert st.ppid == os.getppid()
    assert st.start_time > 0
    assert st.alive


def test_real_proc_parent_chain_verified():
    """用真实父子进程验证：子进程经父链核实到根，根身份不符则不确认。"""
    proc = Path("/proc")
    # 子进程自行退出（受限环境里可能不允许 kill 子进程）
    child = subprocess.Popen(["sleep", "2"])
    try:
        parent = read_proc_stat(proc, os.getpid())
        assert parent is not None
        child_stat = read_proc_stat(proc, child.pid)
        assert child_stat is not None and child_stat.ppid == os.getpid()

        registry = JobRegistry(
            cgroup_root=Path("/nonexistent-cgroup"), proc_root=proc
        )
        root = ProcIdentity(parent.pid, parent.start_time)
        assert registry._verify_descendant(root, child.pid, set()) == ProcIdentity(
            child.pid, child_stat.start_time
        )
        # 根进程身份不符（start_time 被改）→ 不确认
        wrong_root = ProcIdentity(parent.pid, parent.start_time + 1)
        assert registry._verify_descendant(wrong_root, child.pid, set()) is None
    finally:
        child.wait(timeout=10)


# ---------------------------------------------------------------- 模拟环境


class FakeHost:
    """用普通文件模拟 cgroup.procs 与 /proc/<pid>/stat。"""

    def __init__(self, tmp_path: Path):
        self.cgroup = tmp_path / "cgroup"
        self.proc = tmp_path / "proc"
        self.state = tmp_path / "state"
        for g in (A, B):
            (self.cgroup / g).mkdir(parents=True, exist_ok=True)
            (self.cgroup / g / "cgroup.procs").write_text("")
        self.proc.mkdir(parents=True)

    def add_proc(self, pid: int, ppid: int, start_time: int, state: str = "S") -> None:
        d = self.proc / str(pid)
        d.mkdir(parents=True, exist_ok=True)
        self._write(d / "stat", self._stat(pid, ppid, start_time, state))

    def remove_proc(self, pid: int) -> None:
        (self.proc / str(pid) / "stat").unlink(missing_ok=True)

    def set_group(self, group: str, pids: list[int]) -> None:
        (self.cgroup / group / "cgroup.procs").write_text(
            "".join(f"{p}\n" for p in pids)
        )

    def registry(self, interval: float = 0.05, state_dir: bool = False) -> JobRegistry:
        return JobRegistry(
            cgroup_root=self.cgroup,
            proc_root=self.proc,
            state_dir=self.state if state_dir else None,
            scan_interval=interval,
        )

    @staticmethod
    def _stat(pid: int, ppid: int, start_time: int, state: str) -> str:
        # 第 3 字段 state、第 4 字段 ppid、第 22 字段 starttime，其余补 0
        rest = [state, str(ppid)] + ["0"] * 17 + [str(start_time)]
        rest += ["0"] * 30
        return f"{pid} (fake-proc) {' '.join(rest)}\n"

    @staticmethod
    def _write(path: Path, text: str) -> None:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(text)
        tmp.replace(path)


@pytest.fixture
def host(tmp_path):
    return FakeHost(tmp_path)


@pytest.fixture
def registry(host):
    reg = host.registry()
    yield reg
    reg.stop()


# ---------------------------------------------------------------- 基本核实


def test_confirms_root_and_descendants(host, registry):
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.add_proc(1003, 1002, 300)
    host.set_group(A, [1001, 1002, 1003])
    registry.register("job-1", A, 1001, 100)

    assert registry.scan_once() == {"job-1": 3}
    view = registry.get("job-1")
    assert view["processes"] == [
        {"pid": 1001, "start_time": 100},
        {"pid": 1002, "start_time": 200},
        {"pid": 1003, "start_time": 300},
    ]
    assert view["pending"] == []


def test_process_from_other_tree_not_confirmed(host, registry):
    host.add_proc(1001, 1, 100)
    host.add_proc(2001, 1, 200)  # 同组但父链到不了根
    host.add_proc(2002, 2001, 300)
    host.set_group(A, [1001, 2001, 2002])
    registry.register("job-1", A, 1001, 100)
    registry.scan_once()
    assert [p["pid"] for p in registry.get("job-1")["processes"]] == [1001]


def test_root_start_time_mismatch_skipped(host, registry):
    host.add_proc(1001, 1, 999)  # 登记的是 100，根 PID 已被复用
    host.add_proc(1002, 1001, 200)
    host.set_group(A, [1001, 1002])
    registry.register("job-1", A, 1001, 100)
    registry.scan_once()
    assert registry.get("job-1")["processes"] == []


def test_parent_exit_midchain_skips_new_descendant(host, registry):
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.add_proc(1003, 1002, 300)
    host.set_group(A, [1001, 1002, 1003])
    registry.register("job-1", A, 1001, 100)

    # 扫描进行到一半父进程退出：模拟扫描时 1002 已消失（组文件仍是旧快照）
    host.remove_proc(1002)
    registry.scan_once()
    view = registry.get("job-1")
    # 1003 的父链无法核实：不确认
    assert view["processes"] == [{"pid": 1001, "start_time": 100}]


def test_pid_reuse_midchain_skips_new_descendant(host, registry):
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 250)
    host.add_proc(1003, 1002, 300)
    host.set_group(A, [1001, 1002, 1003])
    registry.register("job-1", A, 1001, 100)
    registry.scan_once()
    assert 1003 in [p["pid"] for p in registry.get("job-1")["processes"]]

    # 1003 退出后 PID 被一个无关键程复用（start_time 变了、父进程不是 1002）
    host.remove_proc(1003)
    host.add_proc(1003, 1, 900)
    registry.scan_once()
    view = registry.get("job-1")
    # 旧身份不再存活 → pending(pid_reused)；新身份核实不到根 → 不确认
    assert 1003 not in [p["pid"] for p in view["processes"]]
    assert {"pid": 1003, "start_time": 300, "reason": "pid_reused"} in view["pending"]


def test_confirmed_process_reparented_still_attributed(host, registry):
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.add_proc(1003, 1002, 300)
    host.set_group(A, [1001, 1002, 1003])
    registry.register("job-1", A, 1001, 100)
    registry.scan_once()

    # 1002、1003 被 reparent 到 1（父链已断），但身份未变、仍在原组
    host.add_proc(1002, 1, 200)
    host.add_proc(1003, 1, 300)
    registry.scan_once()
    view = registry.get("job-1")
    assert [p["pid"] for p in view["processes"]] == [1001, 1002, 1003]
    assert view["pending"] == []


def test_parent_pid_reused_midchain_skips_new_descendant(host, registry):
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.set_group(A, [1001, 1002])
    registry.register("job-1", A, 1001, 100)
    registry.scan_once()
    assert 1002 in [p["pid"] for p in registry.get("job-1")["processes"]]

    # 1002 退出后 PID 被无关键程复用，新进程 1006 仍以 1002 为父进程
    host.add_proc(1002, 1, 900)
    host.add_proc(1006, 1002, 600)
    host.set_group(A, [1001, 1002, 1006])
    registry.scan_once()
    view = registry.get("job-1")
    # 新后代 1006 的父链核实失败：跳过
    assert 1006 not in [p["pid"] for p in view["processes"]]
    # 旧的已确认身份 1002 不再存活：列入待核实
    assert {"pid": 1002, "start_time": 200, "reason": "pid_reused"} in view["pending"]


def test_new_child_of_reparented_confirmed_process_confirmed(host, registry):
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.set_group(A, [1001, 1002])
    registry.register("job-1", A, 1001, 100)
    registry.scan_once()

    # 1002 换父进程后 fork 出 1004：父链断在已确认的 1002 处，仍归属本作业
    host.add_proc(1002, 1, 200)
    host.add_proc(1004, 1002, 400)
    host.set_group(A, [1001, 1002, 1004])
    registry.scan_once()
    assert [p["pid"] for p in registry.get("job-1")["processes"]] == [1001, 1002, 1004]


# ---------------------------------------------------------------- 结束与查询


def test_end_does_not_require_scan(host, registry):
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.set_group(A, [1001, 1002])
    registry.register("job-1", A, 1001, 100)
    view = registry.end("job-1")  # 结束上报本身不触发扫描
    assert view["ended"] is True
    assert view["ended_at"]
    assert view["processes"] == []  # 尚未采样确认过

    registry.scan_once()  # 结束后的扫描不再添到这笔作业
    assert registry.get("job-1")["processes"] == []


def test_late_scan_result_discarded_after_end(host, registry):
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.set_group(A, [1001, 1002])
    registry.register("job-1", A, 1001, 100)

    started = threading.Event()
    release = threading.Event()
    orig = registry._verify_descendant

    def slow_verify(root, pid, confirmed):
        started.set()
        release.wait(5)
        return orig(root, pid, confirmed)

    registry._verify_descendant = slow_verify  # type: ignore[method-assign]
    t = threading.Thread(target=registry.scan_once)
    t.start()
    assert started.wait(5)

    # 扫描还没完成，结束上报立即返回（不等扫描）
    t0 = time.monotonic()
    registry.end("job-1")
    assert time.monotonic() - t0 < 1.0

    release.set()
    t.join(5)
    # 迟到的扫描结果整体丢弃
    assert registry.get("job-1")["processes"] == []
    assert registry.get("job-1")["pending"] == []


def test_query_lists_alive_in_group_and_pending_others(host, registry):
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.add_proc(1003, 1001, 300)
    host.add_proc(1004, 1001, 400)
    host.add_proc(1005, 1001, 500)
    host.set_group(A, [1001, 1002, 1003, 1004, 1005])
    registry.register("job-1", A, 1001, 100)
    registry.scan_once()

    view = registry.end("job-1")
    assert [p["pid"] for p in view["processes"]] == [1001, 1002, 1003, 1004, 1005]

    # 1002 退出（stat 消失）；1003 变成僵尸；1004 离开组；1005 的 PID 被复用
    host.remove_proc(1002)
    host.add_proc(1003, 1001, 300, state="Z")
    host.set_group(A, [1001, 1003, 1005])
    host.add_proc(1005, 1, 999)

    view = registry.get("job-1")
    assert [p["pid"] for p in view["processes"]] == [1001]
    reasons = {p["pid"]: p["reason"] for p in view["pending"]}
    assert reasons == {
        1002: "process_missing",
        1003: "process_dead",
        1004: "left_group",
        1005: "pid_reused",
    }


def test_unknown_job_raises(registry):
    with pytest.raises(JobNotFoundError):
        registry.end("nope")
    with pytest.raises(JobNotFoundError):
        registry.get("nope")


def test_registration_validation(host, registry):
    with pytest.raises(JobError, match="未知执行器组"):
        registry.register("j", "groupC", 1, 1)
    with pytest.raises(JobError, match="PID"):
        registry.register("j", A, 0, 1)
    with pytest.raises(JobError, match="启动时刻"):
        registry.register("j", A, 1, -1)
    registry.register("j", A, 1, 1)
    # 幂等重放
    registry.register("j", A, 1, 1)
    # 同号不同根进程：拒绝
    with pytest.raises(JobError, match="已登记"):
        registry.register("j", A, 2, 2)


def test_background_scanner_discovers(host):
    reg = host.registry(interval=0.02)
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.set_group(A, [1001, 1002])
    reg.register("job-1", A, 1001, 100)
    reg.start()
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if len(reg.get("job-1")["processes"]) == 2:
                break
            time.sleep(0.02)
        assert [p["pid"] for p in reg.get("job-1")["processes"]] == [1001, 1002]
    finally:
        reg.stop()


def test_persistence_roundtrip(host):
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.set_group(A, [1001, 1002])
    reg = host.registry(state_dir=True)
    reg.register("job-1", A, 1001, 100)
    reg.scan_once()
    reg.end("job-1")
    reg.stop()

    reg2 = host.registry(state_dir=True)
    assert reg2.load_persisted() == 1
    view = reg2.get("job-1")
    assert view["ended"] is True
    assert [p["pid"] for p in view["processes"]] == [1001, 1002]


# ---------------------------------------------------------------- HTTP API


@pytest.fixture
def api(host):
    cg = host.cgroup
    for name, q in ((A, 100000), (B, 50000)):
        (cg / name / "cpu.max").write_text(f"{q} 100000\n")
    svc = TransferService(
        {A: cg / A / "cpu.max", B: cg / B / "cpu.max"}, host.state, 20000
    )
    jobs = host.registry(interval=0.02)
    app = create_app(svc, jobs)
    with TestClient(app) as c:
        yield c, jobs
    svc.close()


def test_api_job_lifecycle(host, api):
    c, _ = api
    host.add_proc(1001, 1, 100)
    host.add_proc(1002, 1001, 200)
    host.set_group(A, [1001, 1002])

    r = c.post(
        "/jobs",
        json={"job_id": "build-1", "group": A, "root_pid": 1001, "root_start_time": 100},
    )
    assert r.status_code == 201, r.text

    # 等后台采样确认
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        body = c.get("/jobs/build-1").json()
        if len(body["processes"]) == 2:
            break
        time.sleep(0.02)
    assert [p["pid"] for p in body["processes"]] == [1001, 1002]

    # 更换父进程 + PID 复用，再上报结束
    host.add_proc(1002, 1, 200)
    host.add_proc(1003, 1001, 300)  # 新后代，但父链正常 → 会被确认
    host.set_group(A, [1001, 1002, 1003])
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if len(c.get("/jobs/build-1").json()["processes"]) == 3:
            break
        time.sleep(0.02)

    host.add_proc(1003, 1, 999)  # 1003 的 PID 被复用，旧身份不再存在
    r = c.post("/jobs/build-1/end")
    assert r.status_code == 200
    body = r.json()
    assert body["ended"] is True
    assert [p["pid"] for p in body["processes"]] == [1001, 1002]  # 1002 换父进程仍归属
    assert {"pid": 1003, "start_time": 300, "reason": "pid_reused"} in body["pending"]

    # 结束后的扫描不会再把 1003 的新身份记进来
    time.sleep(0.15)
    body = c.get("/jobs/build-1").json()
    assert 1003 not in [p["pid"] for p in body["processes"]]


def test_api_errors(host, api):
    c, _ = api
    r = c.post(
        "/jobs",
        json={"job_id": "x", "group": "groupC", "root_pid": 1, "root_start_time": 1},
    )
    assert r.status_code == 400
    r = c.post(
        "/jobs",
        json={"job_id": "x", "group": A, "root_pid": 0, "root_start_time": 1},
    )
    assert r.status_code == 422
    assert c.post("/jobs/nope/end").status_code == 404
    assert c.get("/jobs/nope").status_code == 404
    assert c.get("/jobs").json() == {"jobs": []}
