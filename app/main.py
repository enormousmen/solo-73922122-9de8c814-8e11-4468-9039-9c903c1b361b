"""FastAPI 入口：配额划转 + 作业进程归属登记。"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .jobs import JobError, JobNotFoundError, JobRegistry
from .service import (
    RecoveryPendingError,
    TransferError,
    TransferService,
    WriteFailureError,
)

log = logging.getLogger("quota-transfer")


def build_service_from_env() -> TransferService:
    crash_hook = None
    if os.environ.get("CRASH_AFTER_DONOR_WRITE") == "1":
        # 演示用：模拟“让出方降额后、接收方升额前进程崩溃退出”
        def crash_hook(_stage: str) -> None:
            os._exit(42)

    return TransferService(
        group_paths={
            "groupA": Path(os.environ.get("GROUP_A_CPU_MAX", "/cgroup/groupA/cpu.max")),
            "groupB": Path(os.environ.get("GROUP_B_CPU_MAX", "/cgroup/groupB/cpu.max")),
        },
        state_dir=Path(os.environ.get("STATE_DIR", "/state")),
        min_quota=int(os.environ.get("MIN_QUOTA", "20000")),
        crash_hook=crash_hook,
    )


def build_jobs_from_env() -> JobRegistry:
    return JobRegistry(
        cgroup_root=Path(os.environ.get("CGROUP_ROOT", "/cgroup")),
        proc_root=Path(os.environ.get("PROC_ROOT", "/proc")),
        state_dir=Path(os.environ.get("STATE_DIR", "/state")),
        scan_interval=float(os.environ.get("JOB_SCAN_INTERVAL", "0.2")),
        log=lambda msg: log.info("jobs: %s", msg),
    )


class TransferRequest(BaseModel):
    donor: str = Field(description="让出方组名（groupA 或 groupB）")
    amount: int = Field(gt=0, description="划转量（cpu.max quota 单位，微秒/周期）")


class TransferResponse(BaseModel):
    action: str
    result: str
    transfer_id: str
    donor: str
    receiver: str
    amount: int
    post: dict
    at: str


class JobRegisterRequest(BaseModel):
    job_id: str = Field(min_length=1, description="作业号（调度端唯一标识）")
    group: str = Field(description="执行器组名（cgroup 目录名，如 groupA）")
    root_pid: int = Field(gt=0, description="作业根进程 PID")
    root_start_time: int = Field(ge=0, description="根进程启动时刻（/proc/<pid>/stat 第 22 字段）")


class ProcessItem(BaseModel):
    pid: int
    start_time: int


class PendingItem(BaseModel):
    pid: int
    start_time: int
    reason: str


class JobResponse(BaseModel):
    job_id: str
    group: str
    root_pid: int
    root_start_time: int
    registered_at: str
    ended: bool
    ended_at: str | None = None
    scans: int
    root_verified: bool
    processes: list[ProcessItem]
    pending: list[PendingItem]


def create_app(
    service: TransferService, jobs: JobRegistry | None = None
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 启动时先恢复：回退未登记成功的转移，保留已登记成功的新配额。
        # 恢复完成前（含恢复失败）recovered=False，新转移一律被拒绝。
        try:
            result = service.recover()
            log.info("startup recovery: %s", result)
        except Exception:
            log.exception("启动恢复失败；恢复完成前将拒绝新转移")
        if jobs is not None:
            loaded = jobs.load_persisted()
            jobs.start()
            log.info("作业登记表已启动，恢复 %d 个作业", loaded)
        try:
            yield
        finally:
            if jobs is not None:
                jobs.stop()

    app = FastAPI(title="CPU Quota Transfer & Job Process Service", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True}

    @app.get("/quotas")
    def get_quotas() -> dict:
        """查询两组当前配额。"""
        return service.quotas()

    @app.get("/status")
    def get_status() -> dict:
        """查询恢复状态与最近一次处理结果。"""
        return service.status()

    @app.post("/transfers", response_model=TransferResponse)
    def post_transfer(req: TransferRequest) -> dict:
        """发起一笔划转：donor 组让出 amount 给另一组。"""
        try:
            return service.transfer(req.donor, req.amount)
        except RecoveryPendingError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except TransferError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except WriteFailureError as e:
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # 作业进程归属
    # ------------------------------------------------------------------
    def require_jobs() -> JobRegistry:
        if jobs is None:
            raise HTTPException(status_code=503, detail="作业登记未启用")
        return jobs

    @app.post("/jobs", response_model=JobResponse, status_code=201)
    def register_job(req: JobRegisterRequest) -> dict:
        """登记一笔作业：作业号、执行器组、根进程 PID 与启动时刻。"""
        try:
            return require_jobs().register(
                req.job_id, req.group, req.root_pid, req.root_start_time
            )
        except JobError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/jobs/{job_id}/end", response_model=JobResponse)
    def end_job(job_id: str) -> dict:
        """上报作业结束：立即生效，不等后台采样。"""
        try:
            return require_jobs().end(job_id)
        except JobNotFoundError:
            raise HTTPException(status_code=404, detail=f"作业 {job_id!r} 不存在")

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    def get_job(job_id: str) -> dict:
        """按作业号查询结束后的存活后代进程。"""
        try:
            return require_jobs().get(job_id)
        except JobNotFoundError:
            raise HTTPException(status_code=404, detail=f"作业 {job_id!r} 不存在")

    @app.get("/jobs")
    def list_jobs() -> dict:
        return {"jobs": require_jobs().list_jobs()}

    return app


def app_factory() -> FastAPI:
    """uvicorn --factory 入口：延迟到真正启动时才读取环境、触碰文件系统。"""
    return create_app(build_service_from_env(), build_jobs_from_env())
