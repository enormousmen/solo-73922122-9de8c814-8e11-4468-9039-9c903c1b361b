#!/usr/bin/env bash
# 作业进程归属 API 演示（cgroup.procs 与 /proc 均用普通文件模拟）：
#   1) 登记作业：作业号、执行器组、根进程 PID、启动时刻
#   2) 后台采样核实父子链，确认根进程与后代
#   3) 已确认后代换父进程（reparent），仍按原身份归属
#   4) 已确认 PID 被复用：旧身份单列待核实；复用者及其子树无法核实，跳过
#   5) 父链读不完整（中间父进程不存在）的新后代跳过
#   6) 上报结束：立即返回，不等扫描；结束后的新进程不再归属该作业
#   7) 按作业号查询：只列活着且仍在原组的后代，返回 PID 与启动时刻
#
# 用法：
#   examples/jobs_demo.sh docker   # 用 Docker Compose 启动（默认）
#   examples/jobs_demo.sh local    # 本机直接跑（需 uvicorn）
set -euo pipefail

MODE="${1:-docker}"
BASE_URL="${BASE_URL:-http://127.0.0.1:${PORT:-8000}}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CGROUP_DIR="${CGROUP_DIR:-$ROOT/cgroup}"
STATE_DIR="${STATE_DIR:-$ROOT/state}"
PROC_DIR="${PROC_DIR:-$ROOT/examples/runtime/proc}"
UVICORN="${UVICORN:-uvicorn}"
SERVER_PID=""
JOB_ID="build-9001"
PORT="${PORT:-8000}"

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
show() { printf '  %s\n' "$*"; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "缺少命令: $1" >&2; exit 1; }; }

# 从 GET /jobs/<id> 的响应里取 processes 数组中的 PID（不含 pending）。
# 末尾的 || true：processes 为空时 grep 退出码为 1，不能触发 set -e。
proc_pids() {
  sed -n 's/.*"processes":\[\([^]]*\)\].*/\1/p' \
    | grep -o '"pid":[0-9]*' | cut -d: -f2 | sort -n | tr '\n' ' ' || true
}

# ---------------------------------------------------------------- 模拟文件
# stat 第 3 字段 state、第 4 字段 ppid、第 22 字段 starttime，其余补 0。
stat_line() {  # $1 pid, $2 ppid, $3 start_time, $4 state
  local rest="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"  # 第 5..21 字段
  echo "$1 (ci-$1) $4 $2 $rest $3 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"
}

add_proc() {  # $1 pid, $2 ppid, $3 start_time, $4 state（默认 S）
  mkdir -p "$PROC_DIR/$1"
  stat_line "$1" "$2" "$3" "${4:-S}" > "$PROC_DIR/$1/stat.tmp"
  mv "$PROC_DIR/$1/stat.tmp" "$PROC_DIR/$1/stat"
}

del_proc() { rm -rf "${PROC_DIR:?}/$1"; }

set_group() {  # $1 组名，其余参数为 PID 列表
  local group="$1"; shift
  : > "$CGROUP_DIR/$group/cgroup.procs"
  for pid in "$@"; do echo "$pid" >> "$CGROUP_DIR/$group/cgroup.procs"; done
}

reset_sim() {
  mkdir -p "$CGROUP_DIR/groupA" "$CGROUP_DIR/groupB" "$STATE_DIR" "$PROC_DIR"
  echo "100000 100000" > "$CGROUP_DIR/groupA/cpu.max"
  echo "50000 100000"  > "$CGROUP_DIR/groupB/cpu.max"
  rm -rf "$STATE_DIR/jobs"
  rm -f "$STATE_DIR"/transfer_journal.json "$STATE_DIR"/last_result.json
  find "${PROC_DIR:?}" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  # pid 1：所有“无关键程”的父进程
  add_proc 1 0 1
  # 作业 build-9001 的进程树：4200 → 4201 → 4202
  add_proc 4200 1 1000
  add_proc 4201 4200 1100
  add_proc 4202 4201 1200
  set_group groupA 4200 4201 4202
}

# ---------------------------------------------------------------- 服务
start_service() {
  if [ "$MODE" = docker ]; then
    need docker
    docker compose -f "$ROOT/docker-compose.yml" up -d --build >/dev/null
  else
    if curl -fsS "$BASE_URL/healthz" >/dev/null 2>&1; then
      echo "端口 $PORT 上已有服务在响应（可能是上次演示残留）；请换 PORT= 或先停掉它" >&2
      exit 1
    fi
    ( cd "$ROOT" && \
      GROUP_A_CPU_MAX="$CGROUP_DIR/groupA/cpu.max" \
      GROUP_B_CPU_MAX="$CGROUP_DIR/groupB/cpu.max" \
      CGROUP_ROOT="$CGROUP_DIR" \
      PROC_ROOT="$PROC_DIR" \
      STATE_DIR="$STATE_DIR" \
      MIN_QUOTA=20000 \
      JOB_SCAN_INTERVAL=0.2 \
      exec "$UVICORN" --factory app.main:app_factory --host 127.0.0.1 --port "$PORT" \
    ) >/tmp/jobs-demo.log 2>&1 &
    SERVER_PID=$!
  fi
  wait_ready
}

stop_service() {
  if [ "$MODE" = docker ]; then
    docker compose -f "$ROOT/docker-compose.yml" down >/dev/null 2>&1 || true
  elif [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$SERVER_PID" 2>/dev/null || break
      sleep 0.25
    done
    if kill -0 "$SERVER_PID" 2>/dev/null; then
      kill -9 "$SERVER_PID" 2>/dev/null || true
      echo "警告: 服务进程 $SERVER_PID 无法终止（受限环境？），可能仍占用端口" >&2
    else
      wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
  fi
}

wait_ready() {
  for _ in $(seq 1 50); do
    if curl -fsS "$BASE_URL/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  echo "服务未就绪（日志见 /tmp/jobs-demo.log 或 docker compose logs）" >&2
  exit 1
}

register_job() {
  curl -sS -X POST "$BASE_URL/jobs" -H 'Content-Type: application/json' \
    -d "{\"job_id\":\"$JOB_ID\",\"group\":\"groupA\",\"root_pid\":4200,\"root_start_time\":1000}"
  echo
}

show_job() {
  printf '  GET /jobs/%s →\n' "$JOB_ID"
  curl -fsS "$BASE_URL/jobs/$JOB_ID" | sed 's/^/  /'
  echo
}

wait_for_pids() {  # $@ 期望出现在 processes 里的 PID；最多等 5 秒
  local body got want
  want="$(printf '%s\n' "$@" | sort -n | tr '\n' ' ')"
  for _ in $(seq 1 50); do
    body="$(curl -fsS "$BASE_URL/jobs/$JOB_ID")"
    got="$(printf '%s\n' "$body" | proc_pids)"
    [ "$got" = "$want" ] && return 0
    sleep 0.1
  done
  show "警告: 等待进程 [$want] 确认超时，当前 processes=[$got]"
  return 1
}

trap 'stop_service' EXIT

# ---------------------------------------------------------------- 0. 启动
say "0. 重置模拟文件并启动服务（$MODE，后台采样间隔 0.2s）"
stop_service; reset_sim; start_service
show "groupA/cgroup.procs: $(tr '\n' ' ' < "$CGROUP_DIR/groupA/cgroup.procs")"
show "模拟进程树: 4200(ppid=1,start=1000) → 4201(ppid=4200,start=1100) → 4202(ppid=4201,start=1200)"

# ---------------------------------------------------------------- 1. 登记
say "1. 调度端登记作业（作业号 $JOB_ID，执行器组 groupA，根 PID 4200，启动时刻 1000）"
register_job

say "2. 后台采样核实父子链后，查询作业"
wait_for_pids 4200 4201 4202
show_job
show "三个进程的 (pid, start_time) 均被采样确认"

# ---------------------------------------------------------------- 2. 换父进程
say "3. 更换父进程：4201 被 reparent 到 pid 1（身份不变、仍在 groupA）"
add_proc 4201 1 1100
sleep 0.6   # 等至少一次后台采样
show_job
show "4201 的父链已断，但身份 (4201, 1100) 曾确认，仍归属本作业"

say "4. 换父进程的 4201 又 fork 出 4203：父链断在已确认祖先处，仍可确认"
add_proc 4203 4201 1300
set_group groupA 4200 4201 4202 4203
wait_for_pids 4200 4201 4202 4203
show_job

# ---------------------------------------------------------------- 3. PID 复用
say "5. PID 复用：4202 退出后 PID 被无关键程复用（start_time 1200 → 9000）"
del_proc 4202
add_proc 4202 1 9000
# 复用者 4202 又带了一个新后代 4204
add_proc 4204 4202 1400
# 另有一个父进程根本不存在的 4205：父链读不完整
add_proc 4205 4999 1500
set_group groupA 4200 4201 4202 4203 4204 4205
sleep 0.6
show_job
show "4202 的旧身份 (4202, 1200) 已取不到证据 → pending / pid_reused"
show "4204（父链经过被复用的 PID）与 4205（父进程不存在）无法核实 → 不确认"

# ---------------------------------------------------------------- 4. 结束
say "6. 上报作业结束（立即返回，不等扫描）"
curl -sS -X POST "$BASE_URL/jobs/$JOB_ID/end" | sed 's/^/  /'
echo
show "结束后再加入 4206（父进程 4200，父链完全正常），之后完成的扫描也不得归入本作业"
add_proc 4206 4200 1600
set_group groupA 4200 4201 4202 4203 4204 4205 4206
sleep 0.8
show_job
show "4206 未出现在 processes / pending 中"

say "7. 结束后的查询语义：只列采样曾确认、仍活着且仍在原组的进程"
show "返回值中 processes 即归属进程（PID + 启动时刻）；pending 为待核实的旧身份"

say "演示完成"
