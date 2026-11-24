"""作业进程归属登记与核实。

CI 调度端登记“作业号 + 执行器组 + 根进程 PID + 启动时刻”，服务在后台
读取该组 cgroup 的 ``cgroup.procs`` 与 ``/proc/<pid>/stat``，核实进程身份
与父子链路后，把该作业名下的进程记录下来；作业结束（上报）后，
按作业号查询“结束后仍存活、且仍在原组”的进程。

术语：
  * 采样（scan）：一次从 cgroup.procs 出发、沿父链向根进程核实的遍历；
  * 确认（confirmed）：采样中完整核实过身份与父链的进程；
  * 待核实（pending）：此前确认过、但查询时取不到证据的进程（已退出、
    PID 被复用、已离开原组，或证据文件读不到）。

不变式（与采样并发时的行为）：
  1. 进程身份 = (pid, start_time)；PID 被复用（start_time 变了）即不是同一进程；
  2. 新后代归属只在“本次采样完整核实了从该进程到根进程的父链”时成立；
     父链读到一半遇到进程退出 / PID 复用，无法核实的新后代跳过；
  3. 已确认进程后来换了父进程，仍按原确认身份归属（不再要求原父链）；
  4. 结束上报立即生效、不等扫描；结束之后完成的扫描结果不再写入该作业；
  5. 查询只返回“采样曾确认、查询时仍活着、且仍在原组”的进程 PID 与启动时刻；
     曾确认但这次取不到证据的单列为 pending，并注明原因。
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .journal import atomic_write_json
from .procfs import ProcStat, read_cgroup_procs, read_proc_stat


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobError(Exception):
    """作业登记请求非法（未知组、pid 非法、作业号冲突等），对应 HTTP 400。"""


class JobNotFoundError(Exception):
    """作业号不存在，对应 HTTP 404。"""


@dataclass(frozen=True)
class ProcIdentity:
    """进程身份：PID 与内核记录的启动时刻（start_time）。"""

    pid: int
    start_time: int


@dataclass
class Job:
    job_id: str
    group: str
    root_pid: int
    root_start_time: int
    registered_at: str
    # 采样曾确认过的全部身份。同一 PID 被复用后可能同时存在两个身份。
    confirmed: set[ProcIdentity] = field(default_factory=set)
    scan_count: int = 0
    ended: bool = False
    ended_at: str | None = None
    # 上次成功落盘的摘要，避免无变化时反复写状态文件
    _persisted_fingerprint: tuple | None = None

    @property
    def root_identity(self) -> ProcIdentity:
        return ProcIdentity(self.root_pid, self.root_start_time)


class JobRegistry:
    """线程安全的作业登记表 + 后台采样器。

    ``proc_root`` 与 ``cgroup_root`` 可替换：交付环境用普通文件模拟
    procfs 与 cgroup（每个组一个 ``cgroup.procs``，每行一个 PID；
    每个进程一个 ``<pid>/stat``，内容格式与真实 procfs 一致）。
    """

    def __init__(
        self,
        cgroup_root: Path | str,
        proc_root: Path | str = "/proc",
        state_dir: Path | str | None = None,
        scan_interval: float = 0.2,
        log: Callable[[str], None] | None = None,
    ):
        self.cgroup_root = Path(cgroup_root)
        self.proc_root = Path(proc_root)
        self.state_dir = Path(state_dir) if state_dir is not None else None
        self.scan_interval = float(scan_interval)
        self._log = log or (lambda _msg: None)
        self._lock = threading.RLock()
        self._scan_lock = threading.Lock()
        self._jobs: dict[str, Job] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._persist_dir = self.state_dir / "jobs" if self.state_dir else None

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def start(self) -> None:
        """启动后台采样线程（幂等）。"""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, name="job-registry-scanner", daemon=True
            )
            self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout)
        self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self.scan_interval):
            try:
                self.scan_once()
            except Exception as e:  # 后台线程不能因单次采样异常退出
                self._log(f"作业采样失败: {e}")

    # ------------------------------------------------------------------
    # 登记 / 结束 / 查询
    # ------------------------------------------------------------------
    def register(
        self, job_id: str, group: str, root_pid: int, root_start_time: int
    ) -> dict:
        job_id = (job_id or "").strip()
        if not job_id:
            raise JobError("作业号不能为空")
        if len(job_id) > 200 or any(c in job_id for c in ("/", "\\", "\x00")):
            raise JobError("作业号过长或包含路径分隔符")
        if job_id in (".", ".."):
            raise JobError("作业号非法")
        if not isinstance(group, str) or not self._group_exists(group):
            raise JobError(f"未知执行器组 {group!r}")
        if isinstance(root_pid, bool) or not isinstance(root_pid, int) or root_pid <= 0:
            raise JobError("根进程 PID 必须为正整数")
        if (
            isinstance(root_start_time, bool)
            or not isinstance(root_start_time, int)
            or root_start_time < 0
        ):
            raise JobError("根进程启动时刻必须为非负整数")
        with self._lock:
            existing = self._jobs.get(job_id)
            if existing is not None:
                if (
                    existing.group != group
                    or existing.root_pid != root_pid
                    or existing.root_start_time != root_start_time
                ):
                    raise JobError(f"作业号 {job_id!r} 已登记为其他根进程/组")
                created = existing  # 幂等重放
            else:
                created = Job(
                    job_id=job_id,
                    group=group,
                    root_pid=root_pid,
                    root_start_time=root_start_time,
                    registered_at=_now(),
                )
                self._jobs[job_id] = created
                self._persist_locked(created)
                self._log(
                    f"登记作业 {job_id} group={group} pid={root_pid} "
                    f"start_time={root_start_time}"
                )
        # 视图要在锁外构造（会读 procfs / cgroup.procs，不能阻塞采样提交）
        return self._view(created)

    def end(self, job_id: str) -> dict:
        """上报作业结束：只置结束标记，不扫描、不等待后台采样。"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            if not job.ended:
                job.ended = True
                job.ended_at = _now()
                self._persist_locked(job)
                self._log(f"作业 {job_id} 已结束")
        return self._view(job)

    def get(self, job_id: str) -> dict:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFoundError(job_id)
        return self._view(job)

    def list_jobs(self) -> list[dict]:
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda j: j.registered_at)
            return [
                {
                    "job_id": j.job_id,
                    "group": j.group,
                    "root_pid": j.root_pid,
                    "root_start_time": j.root_start_time,
                    "ended": j.ended,
                    "ended_at": j.ended_at,
                    "scans": j.scan_count,
                }
                for j in jobs
            ]

    # ------------------------------------------------------------------
    # 采样
    # ------------------------------------------------------------------
    def scan_once(self) -> dict:
        """对所有未结束作业各采样一次；返回 {job_id: 本次新确认数}。"""
        with self._scan_lock:
            with self._lock:
                jobs = [j for j in self._jobs.values() if not j.ended]
            result: dict[str, int] = {}
            for job in jobs:
                result[job.job_id] = self._scan_job(job)
            return result

    def _scan_job(self, job: Job) -> int:
        # 先在锁内取快照，所有文件读取都在锁外：end 不会被慢扫描阻塞
        with self._lock:
            if job.ended:
                return 0
            group = job.group
            root = job.root_identity
            known = set(job.confirmed)

        pids = self._read_cgroup_procs(group)
        if pids is None:
            # 组文件读不到：保持上次状态，不把已确认进程误判为“取不到证据”
            self._log(f"作业 {job.job_id}: 读不到 {group} 的 cgroup.procs，跳过本次")
            return 0

        verified: list[ProcIdentity] = []
        for pid in pids:
            ident = self._verify_descendant(root, pid, known)
            if ident is None:
                continue  # 无法核实的新后代：跳过
            verified.append(ident)

        with self._lock:
            if job.ended:
                # 结束上报先到：本次迟到的扫描结果整体丢弃，不写入该作业
                self._log(f"作业 {job.job_id} 已结束，丢弃迟到扫描结果")
                return 0
            job.scan_count += 1
            new_count = 0
            for ident in verified:
                if ident not in job.confirmed:
                    new_count += 1
                job.confirmed.add(ident)
            self._persist_locked(job)
            return new_count

    def _verify_descendant(
        self,
        root: ProcIdentity,
        pid: int,
        confirmed: set[ProcIdentity],
    ) -> ProcIdentity | None:
        """核实 ``pid`` 是否属于该作业，成功返回其身份，否则 None。

        从 pid 出发沿父链向上。任一时刻出现进程不存在、已退出（僵尸）、
        或父链上的 PID 已被复用，即无法核实、返回 None。

        到达以下任一节点即核实成功：
          * 根进程，且当前身份与登记的 (pid, start_time) 完全一致；
          * 父链上某个此前已确认的身份（该祖先仍存活且身份未变）；
          * 候选进程自身的身份此前已确认 —— 它即便换了父进程仍按原身份归属。

        成功前会重读一次候选进程：若其间候选退出、被 reparent 或身份变了，
        说明核实所依据的链路已过期，本候选跳过（下一轮可再核实）。
        """
        stat = read_proc_stat(self.proc_root, pid)
        if stat is None or not stat.alive:
            return None
        candidate = ProcIdentity(stat.pid, stat.start_time)
        if candidate == root:
            return candidate  # 根进程本身在组里，身份与登记一致
        if candidate in confirmed:
            # 曾确认、仍存活、仍在原组：换了父进程也仍按原身份归属
            return candidate

        by_pid: dict[int, set[int]] = {}
        for ident in confirmed:
            by_pid.setdefault(ident.pid, set()).add(ident.start_time)

        current: ProcStat = stat
        visited: set[int] = {candidate.pid}
        while True:
            ppid = current.ppid
            if ppid <= 0 or ppid in visited:
                return None  # 到不了根 / 父链成环：无法核实
            parent = read_proc_stat(self.proc_root, ppid)
            if parent is None or not parent.alive:
                return None  # 父链读到一半父进程退出/消失
            if parent.start_time > current.start_time:
                # 父进程不可能晚于子进程启动：PID 在读取间隙被复用
                return None
            if ppid == root.pid:
                if ProcIdentity(parent.pid, parent.start_time) != root:
                    return None  # 根的 PID 被复用
                break
            known_starts = by_pid.get(ppid)
            if known_starts is not None:
                if parent.start_time not in known_starts:
                    return None  # 祖先进程的 PID 被复用，链路无法核实
                break
            visited.add(ppid)
            current = parent

        # 重读候选：确认这次核实所依据的 ppid/身份在遍历后仍然成立
        latest = read_proc_stat(self.proc_root, pid)
        if (
            latest is None
            or not latest.alive
            or latest.start_time != candidate.start_time
            or latest.ppid != stat.ppid
        ):
            return None
        return candidate

    # ------------------------------------------------------------------
    # 查询视图
    # ------------------------------------------------------------------
    def _view(self, job: Job) -> dict:
        """只列活着且仍在原组的已确认进程；其余已确认身份列入 pending。"""
        with self._lock:
            identities = sorted(
                job.confirmed, key=lambda i: (i.pid, i.start_time)
            )
            meta = {
                "job_id": job.job_id,
                "group": job.group,
                "root_pid": job.root_pid,
                "root_start_time": job.root_start_time,
                "registered_at": job.registered_at,
                "ended": job.ended,
                "ended_at": job.ended_at,
                "scans": job.scan_count,
            }
        root_stat = read_proc_stat(self.proc_root, job.root_pid)
        meta["root_verified"] = (
            root_stat is not None
            and root_stat.start_time == job.root_start_time
            and root_stat.alive
        )
        group_pids = self._read_cgroup_procs(job.group)
        in_group = set(group_pids) if group_pids is not None else None

        processes: list[dict] = []
        pending: list[dict] = []
        for ident in identities:
            stat = read_proc_stat(self.proc_root, ident.pid)
            if stat is not None and stat.start_time != ident.start_time:
                reason = "pid_reused"
            elif stat is None:
                reason = "process_missing"
            elif not stat.alive:
                reason = "process_dead"
            elif in_group is None:
                reason = "group_unreadable"
            elif ident.pid not in in_group:
                reason = "left_group"
            else:
                processes.append({"pid": ident.pid, "start_time": ident.start_time})
                continue
            pending.append(
                {"pid": ident.pid, "start_time": ident.start_time, "reason": reason}
            )
        return {**meta, "processes": processes, "pending": pending}

    # ------------------------------------------------------------------
    # 文件 / 持久化
    # ------------------------------------------------------------------
    def _group_path(self, group: str) -> Path:
        return self.cgroup_root / group / "cgroup.procs"

    def _group_exists(self, group: str) -> bool:
        if not group or "/" in group or group in (".", ".."):
            return False
        return (self.cgroup_root / group).is_dir()

    def _read_cgroup_procs(self, group: str) -> list[int] | None:
        return read_cgroup_procs(self._group_path(group))

    def _persist_locked(self, job: Job) -> None:
        """登记 / 结束 / 已确认集合变化时原子落盘（调用方需持有 self._lock）。"""
        if self._persist_dir is None:
            return
        fingerprint = (
            job.ended,
            job.ended_at,
            tuple(sorted((i.pid, i.start_time) for i in job.confirmed)),
        )
        if fingerprint == job._persisted_fingerprint:
            return
        payload = {
            "job_id": job.job_id,
            "group": job.group,
            "root_pid": job.root_pid,
            "root_start_time": job.root_start_time,
            "registered_at": job.registered_at,
            "ended": job.ended,
            "ended_at": job.ended_at,
            "scans": job.scan_count,
            "confirmed": [
                {"pid": i.pid, "start_time": i.start_time}
                for i in sorted(job.confirmed, key=lambda i: (i.pid, i.start_time))
            ],
        }
        atomic_write_json(self._persist_dir / f"{job.job_id}.json", payload)
        job._persisted_fingerprint = fingerprint

    def load_persisted(self) -> int:
        """从状态目录恢复作业登记与已确认身份；返回加载的作业数。"""
        if self._persist_dir is None or not self._persist_dir.exists():
            return 0
        count = 0
        with self._lock:
            for path in sorted(self._persist_dir.glob("*.json")):
                try:
                    data = json.loads(path.read_text())
                    job = Job(
                        job_id=str(data["job_id"]),
                        group=str(data["group"]),
                        root_pid=int(data["root_pid"]),
                        root_start_time=int(data["root_start_time"]),
                        registered_at=str(data.get("registered_at") or _now()),
                        scan_count=int(data.get("scans", 0)),
                        ended=bool(data.get("ended", False)),
                        ended_at=data.get("ended_at"),
                        confirmed={
                            ProcIdentity(int(c["pid"]), int(c["start_time"]))
                            for c in data.get("confirmed", [])
                        },
                    )
                    if not job.job_id:
                        continue
                except (OSError, ValueError, KeyError, TypeError):
                    continue
                if job.job_id in self._jobs:
                    continue
                self._jobs[job.job_id] = job
                count += 1
        return count


__all__ = [
    "Job",
    "JobError",
    "JobNotFoundError",
    "JobRegistry",
    "ProcIdentity",
]
