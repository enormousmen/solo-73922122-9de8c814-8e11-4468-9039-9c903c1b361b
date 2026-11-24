"""读取进程身份与 cgroup 成员：``/proc/<pid>/stat`` 与 ``cgroup.procs``。

进程身份用 ``(pid, start_time)`` 表示。``start_time`` 是内核记录的进程启动
时刻（自开机起的时钟滴答数，stat 第 22 个字段）；同一个 PID 被复用后该值
必然改变，因此可用它判断“这还是不是当初那个进程”。

交付环境允许把 ``PROC_ROOT`` 指向一个用普通文件模拟的目录树，
每个 ``<pid>/stat`` 的内容与真实 procfs 一致。
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

# 僵尸 / 已退出：进程条目可能还在，但已经不再“活着”占用执行器。
DEAD_STATES = frozenset({"Z", "X", "x"})


class ProcStatError(ValueError):
    """stat 内容格式非法。"""


class CgroupProcsError(ValueError):
    """cgroup.procs 内容非法。"""


@dataclass(frozen=True)
class ProcStat:
    pid: int
    state: str
    ppid: int
    start_time: int

    @property
    def alive(self) -> bool:
        return self.state not in DEAD_STATES


def parse_proc_stat(text: str, expected_pid: int | None = None) -> ProcStat:
    """解析 stat 内容。

    comm（第 2 字段）允许包含空格和括号，所以从最后一个 ``)`` 处切分：
    其后 token[0] 是 state（第 3 字段），token[1] 是 ppid（第 4 字段），
    token[19] 是 starttime（第 22 字段）。
    """
    left = text.find("(")
    right = text.rfind(")")
    if left < 0 or right < left:
        raise ProcStatError(f"stat 格式非法: {text!r}")
    try:
        pid = int(text[:left].strip())
    except ValueError as e:
        raise ProcStatError(f"stat 中的 pid 非法: {text!r}") from e
    rest = text[right + 1 :].split()
    if len(rest) < 20:
        raise ProcStatError(f"stat 字段不足: {text!r}")
    try:
        state = rest[0]
        ppid = int(rest[1])
        start_time = int(rest[19])
    except ValueError as e:
        raise ProcStatError(f"stat 字段非法: {text!r}") from e
    if expected_pid is not None and pid != expected_pid:
        raise ProcStatError(f"读取 {expected_pid} 时拿到 pid={pid} 的 stat")
    return ProcStat(pid=pid, state=state, ppid=ppid, start_time=start_time)


def read_proc_stat(proc_root: Path | str, pid: int) -> ProcStat | None:
    """读 ``<proc_root>/<pid>/stat``；进程不存在或内容不可解析时返回 None。"""
    path = Path(proc_root) / str(pid) / "stat"
    try:
        text = path.read_text()
    except OSError:
        return None
    try:
        return parse_proc_stat(text, expected_pid=pid)
    except ProcStatError:
        return None


def parse_cgroup_procs(text: str) -> list[int]:
    """解析 cgroup.procs：每行一个 PID，忽略空行；非法内容抛 CgroupProcsError。"""
    pids: list[int] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            pid = int(line)
        except ValueError as e:
            raise CgroupProcsError(f"cgroup.procs 含非法行: {line!r}") from e
        if pid not in pids:
            pids.append(pid)
    return pids


def read_cgroup_procs(path: Path | str) -> list[int] | None:
    """读组内进程列表；文件不存在 / 不可读 / 内容非法时返回 None。"""
    try:
        text = Path(path).read_text()
    except OSError:
        return None
    try:
        return parse_cgroup_procs(text)
    except CgroupProcsError:
        return None
