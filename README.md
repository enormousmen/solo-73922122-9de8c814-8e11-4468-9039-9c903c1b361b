# CPU 配额划转服务（cgroup v2 cpu.max）+ 作业进程归属登记

本服务有两部分：

1. **CPU 配额划转**：独占管理两组 cgroup v2 `cpu.max`，保证转移/回退全程
   配额不变式；
2. **作业进程归属登记**：调度端登记作业的根进程，服务在后台核实父子链，
   把后代进程记到作业名下；作业结束后仍可查询哪些进程还占着执行器。

本仓库用普通文件模拟 cgroup 与 procfs（`cgroup/`、`examples/runtime/proc/`），
代码只依赖文件路径，接入真实构建机时指向 `/sys/fs/cgroup` 与 `/proc` 即可。

## 一、CPU 配额划转

同一台构建机上，一组 CI 执行器忙不过来时，值班员通过本服务把另一组的部分
CPU 配额划给它。服务独占管理两组的 cgroup v2 `cpu.max`（本仓库用普通文件模拟，
接口与 `/sys/fs/cgroup/<group>/cpu.max` 完全一致），并保证转移/回退全程的
配额不变式。

### 保证的不变式

转移或回退的任意中间时刻：

1. **两组 quota 之和不超过本次转移前的总额** —— 转移本身保持总额不变
   （让出方减多少、接收方加多少），且所有两组写入都“先降后升”：
   正常转移先写让出方（降）再写接收方（升）；回退先写需要降的组再写需要升的组。
2. **任一组 quota 不低于设定下限**（`MIN_QUOTA`）—— 划转前校验让出方
   划转后的值；回退目标即转移前的合法值。

其他规则：

- **写入失败即回退**：任一 `cpu.max` 写失败，立即把两组恢复到本次转移前的配额。
- **重启恢复**：启动时回退“未登记成功”的转移（日志 `in_progress`），
  保留“已登记成功”的新配额（日志 `registered`）。
- **恢复完成前拒绝新转移**（HTTP 503），查询接口不受影响。
- **独占管理**：进程内互斥锁 + 状态目录 `flock`，拒绝第二个实例同时管理。

## 目录结构

```
app/
  cgroups.py   # cpu.max 解析/读写（写后 fsync）
  journal.py   # 转移日志（WAL）与原子写 JSON
  service.py   # 配额划转核心：转移、失败回退、启动恢复、不变式
  procfs.py    # 读取 /proc/<pid>/stat 与 cgroup.procs（可指向模拟目录）
  jobs.py      # 作业进程归属：登记、后台采样核实、结束上报、查询
  main.py      # FastAPI 入口与依赖装配
cgroup/        # 普通文件模拟：两组 cpu.max + cgroup.procs（组内进程列表）
state/         # 运行期状态：transfer_journal.json、last_result.json、jobs/<作业号>.json、service.lock
examples/
  demo.sh        # 配额划转端到端演示（docker / local）
  jobs_demo.sh   # 作业进程归属演示：换父进程、PID 复用、结束上报
  runtime/proc/  # jobs_demo.sh 生成的模拟 procfs（<pid>/stat）
tests/         # pytest：配额不变式、崩溃恢复、API、作业归属核实
```

## 启动

```bash
docker compose up -d --build
```

环境变量（compose 中已配好默认值）：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `GROUP_A_CPU_MAX` / `GROUP_B_CPU_MAX` | `/cgroup/groupA/cpu.max` … | 两组 cpu.max 路径 |
| `STATE_DIR` | `/state` | 日志、结果、作业登记目录 |
| `MIN_QUOTA` | `20000` | 任一组 quota 下限 |
| `CRASH_AFTER_DONOR_WRITE` | `0` | 演示用：置 `1` 模拟“让出方降额后进程崩溃” |
| `CGROUP_ROOT` | `/cgroup` | 执行器组根目录（每组一个 `cgroup.procs`） |
| `PROC_ROOT` | `/sim-proc` | procfs 根目录（每个进程一个 `<pid>/stat`）；真实机器用 `/proc` |
| `JOB_SCAN_INTERVAL` | `0.2` | 后台采样间隔（秒） |

## 一、配额 API 示例

```bash
# 发起划转：groupB 让出 20000 给 groupA
curl -X POST http://127.0.0.1:8000/transfers \
  -H 'Content-Type: application/json' \
  -d '{"donor": "groupB", "amount": 20000}'
# => {"result":"success","receiver":"groupA","post":{"groupA":"120000 100000","groupB":"30000 100000"},...}

# 查询两组配额
curl http://127.0.0.1:8000/quotas
# => {"groupA":{"quota":"120000","period":100000,...},"groupB":{"quota":"30000",...}}

# 查询恢复状态与最近一次处理结果
curl http://127.0.0.1:8000/status
# => {"recovered":true,"min_quota":20000,"last_result":{"action":"transfer","result":"success",...}}
```

错误码：`400` 请求非法（未知组名、低于下限、period 不一致、quota 为 max 等）；
`422` 参数校验失败；`503` 启动恢复未完成；`500` 写文件失败（已回退）。

## 二、作业进程归属登记

构建结束后，后台子进程可能还占着 CI 执行器。调度端用这组 API 登记作业，
服务在后台把“该作业的后代进程”记到作业名下，作业结束后仍可查询哪些进程
还在占着执行器。

### 语义

1. **身份 = `(pid, start_time)`**。`start_time` 取自 `/proc/<pid>/stat` 第 22
   字段；PID 被复用后该值必然变化，据此识别“不是当初那个进程”。
2. **登记**：作业号、执行器组、根进程 PID、根进程启动时刻。同一作业号用同样
   参数重复登记是幂等的（返回 201 + 当前视图）；换参数则 400。
3. **后台采样**：每 `JOB_SCAN_INTERVAL` 秒读一次该组 `cgroup.procs`，对其中每个
   PID 沿 `/proc/<pid>/stat` 的父链向上核实，**完整核实到根进程（且根的
   `start_time` 与登记一致）或核实到某个此前已确认的祖先进程**才记为确认。
   核实成功前会重读一次候选进程，确认遍历期间它没有退出、被 reparent 或
   换了身份。
4. **父链读不完整的新后代跳过**：父链读到一半发现父进程不存在（已退出）、
   已退出（`Z`）、或 PID 已被复用（`start_time` 对不上 / 比子进程还新），
   该候选本次不确认；下一轮若链路完整才补上。
5. **已确认进程换了父进程仍归属**：身份此前被采样确认过、且当前存活且仍在
   原组，则无论它后来被 reparent 到哪里都仍归本作业；它之后 fork 的新子进程
   其父链断在该已确认祖先进程处，也可确认。
6. **结束上报不等扫描**：`POST /jobs/{id}/end` 只置结束标记立即返回；与结束
   并发的采样若在结束之后才拿锁提交，整批结果丢弃（迟到结果不得再添到该作业）。
7. **结束后查询**：只返回“采样曾确认、查询时仍活着（非僵尸、`start_time`
   未变）且仍在原组”的进程，字段为 `pid` 与 `start_time`。曾确认但这次取不到
   证据的进程单列在 `pending`，带原因：
   `pid_reused` / `process_missing` / `process_dead` / `left_group` /
   `group_unreadable`。

### API

```bash
# 登记作业（201）
curl -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"job_id":"build-9001","group":"groupA","root_pid":4200,"root_start_time":1000}'
# => {"job_id":"build-9001","group":"groupA","root_pid":4200,"root_start_time":1000,
#     "ended":false,"scans":0,"root_verified":true,"processes":[],"pending":[]}

# 查询（每 0.2s 采样一次，几秒后后代已确认）
curl http://127.0.0.1:8000/jobs/build-9001
# => {...,"scans":3,"processes":[{"pid":4200,"start_time":1000},
#                                {"pid":4201,"start_time":1100}],"pending":[]}

# 上报结束（立即返回，不等扫描）
curl -X POST http://127.0.0.1:8000/jobs/build-9001/end
# => {...,"ended":true,"ended_at":"...","processes":[{"pid":4200,"start_time":1000}],
#     "pending":[{"pid":4201,"start_time":1100,"reason":"process_dead"}]}

# 列出所有作业（不含进程明细）
curl http://127.0.0.1:8000/jobs
# => {"jobs":[{"job_id":"build-9001",...}]}
```

错误码：`400` 作业号空/含路径分隔符、未知执行器组、PID 非法、作业号已登记为
其他根进程；`404` 作业不存在；`422` 参数校验失败（如 `root_pid<=0`）。

### 演示（普通文件模拟 procfs / cgroup）

```bash
examples/jobs_demo.sh docker   # 或 examples/jobs_demo.sh local
```

脚本在 `examples/runtime/proc/<pid>/stat` 与 `cgroup/<组>/cgroup.procs` 里造出
模拟进程树，再通过 API 走完整流程：

1. 登记作业 `build-9001`（组 `groupA`，根 PID 4200，启动时刻 1000），
   后台采样确认 4200 → 4201 → 4202；
2. **更换父进程**：把 4201 的 ppid 改成 1（reparent），查询仍归属本作业；
   它之后 fork 的 4203 也因父链断在已确认的 4201 处而被确认；
3. **PID 复用**：4202 退出后同 PID 以新 `start_time` 出现，旧身份
   `(4202, 1200)` 进入 `pending/pid_reused`；复用者带来的 4204 与父进程
   不存在的 4205 都无法核实 → 不确认；
4. **结束上报**：立即返回；结束后再加入父链完全正常的 4206，之后完成的
   扫描也不会把它记进本作业；
5. 查询结果只列仍活着且仍在原组的后代，给出 PID 与启动时刻。

> 若在受限环境（如本仓库的自测环境）里进程无法被 `kill`，脚本退出时只会
> 打印警告；换一个 `PORT`/`STATE_DIR` 即可重跑。

## 三、配额划转演示（让出方降额后进程退出 → 重启恢复）

```bash
examples/demo.sh docker   # 或 examples/demo.sh local（本机直接跑 uvicorn）
```

演示流程：

1. 正常划转 `groupB --20000--> groupA`：两组变为 `120000 / 30000`，总额恒为 150000；
2. 重启服务：已登记成功的转移**保留**新配额；
3. `CRASH_AFTER_DONOR_WRITE=1` 时发起划转：让出方降额后进程退出，
   现场为 `groupB=20000`（已降）、`groupA=120000`（未升）、日志 `in_progress`；
4. 正常重启：启动恢复**回退**未登记成功的转移，`groupB` 恢复为 `30000`，
   `/status` 显示 `rolled_back`；
5. 下限校验：让出 15000 使 groupB 低于下限 20000，被 400 拒绝。

## 四、测试

```bash
pip install -r requirements-dev.txt
pytest
```

39 个用例：
- 配额 20 个：总额不变式（转移与回退全程逐次校验）、下限边界、写失败回退、
  崩溃后重启回退、已登记保留、恢复失败持续拒绝、结果持久化、独占锁、API 各端点；
- 作业归属 19 个：真实 /proc 解析与父子链核实、模拟环境下确认根与后代、
  组内无关进程不确认、根 `start_time` 不符跳过、父进程中途退出跳过、
  父链/候选 PID 复用跳过、已确认进程 reparent 仍归属、reparent 后新子进程
  可确认、结束不等扫描、迟到扫描结果丢弃、结束后查询只列存活且在原组
  （含四种 pending 原因）、后台线程自动发现、持久化往返、HTTP 全流程。

## 五、已知限制 / 未完成

- 配额划转仅支持数值 quota；任一组为 `max` 时拒绝划转（返回 400）。
- 两组 period 必须相同，否则拒绝划转；服务不修改 period。
- 配额登记（`registered`）写盘失败的极端情况下，转移已生效但未登记，
  按规则将在下次重启时被回退（与“未登记成功即回退”语义一致）。
- 单实例部署：flock 防止同机多实例，但不支持跨机并发管理同一组文件。
- 本仓库用普通文件模拟 cpu.max、cgroup.procs 与 procfs；接入真实构建机时把
  `GROUP_*_CPU_MAX`、`CGROUP_ROOT`、`PROC_ROOT` 指向真实路径并保证容器有读权限。
- 作业登记目前只保存在进程内存 + `state/jobs/<作业号>.json`，服务重启后
  会重新加载；但**不校验**作业是否仍在校验队列中，也没有“过期作业自动清理”。
- 采样不做全局一致性快照：一次采样内多个进程的 stat 是逐个读取的，读取期间
  发生 reparent 只会让对应的新候选本轮跳过，不会产生错误归属。
- 未接入认证/鉴权：假设调度端与执行器在同一可信网络内。

