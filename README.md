# Event Delivery Pipeline（本地版）

一个仅依赖本地存储（SQLite）与仓库内 Python 依赖（FastAPI）的事件接收、投递与消费演示服务。
目标是在**重复投递、失败重试、并发压力与进程重启**下，让事件状态保持一致、可解释、可追溯，
并具备**调用方追踪标识闭环、按条件筛选、幂等批量人工处置、真实进程级崩溃自愈**等可上生产的运维能力。

- 不依赖任何外部服务（无消息队列、无 Redis、无外部数据库），不需要任何凭据；
- 持久化仅使用进程工作目录下的一个 SQLite 文件（默认 `./data/events.db`，WAL 模式）；
- 下游用确定性的内存脚本模拟（`app/downstream.py`），可按需编排成功 / 瞬时失败 / 业务拒绝 / 未知异常 / 中途挂起。

## 快速开始

```bash
uv sync                       # 安装依赖（含开发依赖 pytest）
uv run pytest -q              # 运行全部测试（39 个，含真实进程崩溃/多实例/极端参数场景）
uv run uvicorn main:app       # 启动服务（默认 http://127.0.0.1:8000，后台 worker 自动投递）
```

提交事件（携带调用方自己的追踪标识）：

```bash
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: biz-order-20260929-0001' \
  -d '{"event_id":"evt-1","event_type":"order.created","payload":{"order_id":"o-1"}}'
```

## API

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /events` | 接收事件。首次 `201 duplicate=false`；内容完全相同的重复提交 `200 duplicate=true`（不再次处理，沿用首次 `correlation_id`）；同一 `event_id` 但内容哈希不同 `409 content_conflict`；非法追踪标识 `400 invalid_correlation_id` |
| `GET /events` | 按条件筛选事件：`state`（可重复，如 `?state=DEAD&state=FAILED`）、`correlation_id`、`event_type`、`limit`/`offset`；按 `created_at,event_id` 稳定排序 |
| `GET /events/{id}` | 查询事件当前状态、尝试次数、下次重试时间、最近错误与失败分类 |
| `GET /events/{id}/history` | 该事件完整状态流转历史（每次迁移一行，含 cid、attempt、原因、操作者） |
| `GET /correlations/{cid}/events` | **按调用方追踪标识反查**其关联的全部事件及当前状态（一次业务操作串起的多个事件一次查全） |
| `GET /correlations/{cid}/history` | 按调用方追踪标识反查跨事件的完整流转历史 |
| `GET /stats` | 按状态计数的统计口径，以及已产生副作用的事件总数 |
| `POST /events/{id}/requeue` | 人工处置：把 `DEAD/FAILED` 事件重新入队（attempts 与回收计数清零，留痕） |
| `POST /events/{id}/discard` | 人工处置：把 `DEAD/FAILED` 事件标记为 `DISCARDED`（留痕，终态） |
| `POST /events/batch-requeue` | 批量重放（选择器 + 幂等键，仅 `DEAD/FAILED`） |
| `POST /events/batch-discard` | 批量丢弃（选择器 + 幂等键，仅 `DEAD/FAILED`） |

### 调用方追踪标识（correlation id）闭环

- 提交时两种携带方式等价（**头优先**）：HTTP 头 `X-Correlation-ID`，或请求体字段
  `correlation_id`；都不带时服务端生成 `cid_` 前缀的标识。
- 合法字符：字母/数字/`. _ : -`，长度 1–128，首字符为字母或数字；非法值返回
  `400 invalid_correlation_id`，**绝不静默替换成另一个值**。
- 标识在首次接收时随事件持久化，此后接收确认、落库、后台投递、租约回收、重试、
  人工重放/丢弃、历史与反查全程沿用同一个值：进程重启、重复提交、并发提交都不会
  串号、错配或被替换。
- **重复提交对账**：重复提交一律返回首次持久化的标识。若本次带了不同的标识，响应中
  `correlation_id_mismatch=true` 并回显 `submitted_correlation_id`，让调用方既能对得上
  “自己带进来的值”，也能看到“系统实际使用的值”。
- **反查**：拿这个标识调用 `/correlations/{cid}/events` 即可知道它关联了哪些事件、
  每个事件走到了哪一步；`/correlations/{cid}/history` 给出跨事件的时间线。

请求体示例：

```json
{"event_id": "evt-2", "event_type": "order.paid", "payload": {}, "correlation_id": "biz-777"}
```

### 按条件筛选与批量人工处置

```bash
# 查所有死信/业务失败事件
curl -s 'localhost:8000/events?state=DEAD&state=FAILED&limit=50'
# 查某次业务操作关联的全部事件
curl -s localhost:8000/correlations/biz-777/events

# 批量丢弃某个追踪标识下的所有死信（必须带幂等键）
curl -s -X POST localhost:8000/events/batch-discard \
  -H 'X-Correlation-ID: ops-alice' -H 'Content-Type: application/json' \
  -d '{"correlation_id":"biz-777","states":["DEAD"],"reason":"downstream retired","idempotency_key":"cleanup-20260929-01"}'

# 或按显式 id 批量重放
curl -s -X POST localhost:8000/events/batch-requeue \
  -H 'Content-Type: application/json' \
  -d '{"event_ids":["evt-a","evt-b"],"reason":"fixed config and retry","idempotency_key":"replay-0930-02"}'
```

批量操作语义：

- **选择器**：`event_ids` / `states` / `correlation_id` 三选一或组合取交集；
  空选择器一律 `400`，避免误处置整库；
- **只处置 `DEAD/FAILED`**：选择器里写其他状态直接 `400`；显式 id 命中但当前状态
  不可处置（已成功/在途/已丢弃）会计入 `skipped`，**已正常完成的事件绝不卷入**；
- **幂等**：`idempotency_key` 唯一。同键重复提交直接返回首次结果（`replayed=true`），
  不重复迁移、不重复写历史、不产生额外副作用；同一键用于不同动作返回 `409`；
- **可审计**：每个被处置的事件单独写一行历史，`reason` 带批次号与操作原因，
  `actor=batch#<id>:<操作者>`；批次记录（含完整结果）落在 `batch_ops` 表；
- 响应区分 `changed`（本次真正迁移）、`skipped`（命中但状态不允许）、
  `not_found`（显式 id 不存在）。

## 投递语义

- **接收侧去重（exactly-once 接收）**：以 `event_id` 为主键，插入在 `BEGIN IMMEDIATE`
  事务内完成。并发重复提交时，靠主键约束只有一个事务成为"首次写入"，其余明确返回
  `duplicate=true`，既不静默忽略，也不会再次执行。
- **内容一致性校验**：对 `event_type + payload` 做规范化 JSON 的 SHA-256 哈希；
  `event_id` 相同但哈希不同一律 `409` 拒绝，绝不覆盖既有事件。
- **投递侧 at-least-once 尝试 + 副作用 exactly-once**：认领（claim）使状态原子迁移到
  `PROCESSING` 并使 `attempts+1`；成功时 `SUCCEEDED` 终态与副作用登记
  （`side_effects` 表，`event_id` 主键）在**同一事务**提交。失效认领（崩溃后租约已被
  回收）回写的结果会被状态+attempt 校验拒绝，因此副作用至多一次；失败路径从不登记副作用。

## 状态机

```
PENDING ──claim(attempts+1)──▶ PROCESSING ──成功────────────▶ SUCCEEDED（终态，副作用恰好一次）
   ▲                              │
   │                              ├─瞬时错误且未达上限(退避)─▶ PENDING（等待 next_attempt_at）
   │                              ├─业务拒绝────────────────▶ FAILED（终态，不重试）
   │                              ├─未知错误 / 瞬时重试耗尽─▶ DEAD（终态，可人工处置）
   │                              └─租约超时且 attempts 已满─▶ DEAD（崩溃循环强制收敛，不再投递）
   │
   └──────── requeue(DEAD/FAILED，attempts/回收计数清零)
   DEAD/FAILED ──discard──▶ DISCARDED（终态）
```

## 失败分类与终态规则

| 分类 (`failure_kind`) | 含义 | 是否重试 | 终态 |
| --- | --- | --- | --- |
| `transient` | 可恢复瞬时错误（超时、临时不可用等） | 是，有上限的指数退避 | 重试耗尽 → `DEAD` |
| `business` | 业务性拒绝（非法账户、规则拒绝等） | **永不重试** | `FAILED` |
| `unknown` | 无法归类的异常（防御性兜底） | **不重试**，避免未知错误无限放大 | `DEAD`（等待人工研判） |

- 任何异常都不会逃逸出处理循环、不会静默丢弃；未被识别的异常一律归入 `unknown`；
- `DEAD/FAILED` 均可通过 `GET /events/{id}` 与 `/history` 查询，并通过
  `requeue` / `discard`（或批量接口）人工处置，处置动作同样写历史
  （`actor=manual:<cid>` 或 `actor=batch#<id>:<cid>`）；
- 重试上限 `max_attempts` **含首次尝试**（默认 3，即最多 1 次首投 + 2 次重试）；
- 崩溃恢复时对超时租约的回收也会计入 `attempts`（每次认领/回收即一次投递尝试，
  回收次数另记在 `reclaim_count` 上），因此“崩溃循环”同样受上限约束：事件最多经历
  `max_attempts` 次投递即进入 `DEAD`，而不是被无限重认领或丢失。

## 重启恢复与崩溃循环收敛

后台 worker 认领事件时记录 `leased_at`、`lease_owner` 与回收计数 `reclaim_count`。
若进程在写终态前崩溃，事件停留在 `PROCESSING`；重启后超过租约时间仍未完成的租约会被
自动回收：状态回到 `PROCESSING`（attempt+1，历史留痕 `reclaimed after stale lease`）
并重新投递。WAL 日志保证已提交数据不丢；事件持久化的 `correlation_id` 保证跨重启不
串号、不错配，`attempts` 单调不回退。

**极端参数也收敛**：当一行超时在途记录的 `attempts` 已达到 `max_attempts`（再回收
就会产生超额投递）时，不再投递，直接由认领事务原子迁移到 `DEAD`
（`disposition=dead_after_stale_attempts`，历史原因含 `exhausted`）。该迁移带
`state/attempts/leased_at/reclaim_count` 守卫，多线程或多实例并发回收时只有一个能落
终态。为防止把 `lease_timeout=0` 下“正常处理中、只是终态还没写完”的健康事件误判为
超时，可回收时刻不早于认领后的 `min_lease_grace` 秒（默认 50ms，足以完成本地下游
调用）——即极端配置下也不会无限重认领或让状态回退。

**多实例**：可以对同一个数据库文件启动多个服务进程（各自带后台 worker，
`EDP_WORKER_CONCURRENCY` 控制单进程内的并发投递线程数，默认 2）。认领是
`BEGIN IMMEDIATE` 原子事务，终态写入带认领代次校验，因此多实例/真并发下副作用仍恰好
一次（见下方“真进程验证”）。

## 可观测性

- 每次状态迁移都向 `event_history` 写一行：`from/to`、`attempt`、`reason`、
  `actor`（`api:` / `worker:` / `manual:`）、事件 `correlation_id`、时间戳；
- 服务日志同样按事件打印 `event_id`、cid、状态流转依据与失败原因；
- `GET /stats` 提供与历史一致的统计：`total`、各状态计数、副作用总数。

## 可配置项（环境变量，前缀 `EDP_`）

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `EDP_DB_PATH` | `./data/events.db` | SQLite 文件路径 |
| `EDP_MAX_ATTEMPTS` | `3` | 最大尝试次数（含首次） |
| `EDP_BASE_BACKOFF` | `0.05` | 指数退避基数（秒） |
| `EDP_BACKOFF_FACTOR` | `2.0` | 退避倍数；第 n 次失败后延迟 `base * factor^(n-1)` |
| `EDP_LEASE_TIMEOUT` | `30.0` | PROCESSING 租约超时（秒），超时后重启/轮询可回收 |
| `EDP_MIN_LEASE_GRACE` | `0.05` | 即使租约超时配成 0，一次在途投递至少受保护的秒数 |
| `EDP_POLL_INTERVAL` | `0.05` | 后台 worker 轮询间隔（秒） |
| `EDP_WORKER_CONCURRENCY` | `2` | 单进程后台并发投递线程数（多实例各自独立认领） |
| `EDP_RECOVERY_ON_STARTUP` | `true` | 启动时是否扫描并告警遗留租约 |

## 本地验证要点

```bash
uv run pytest -q -s                 # 日志打印 event_id、cid 与每次状态流转依据
uv run pytest tests/test_process_restart.py -q   # 仅跑真实进程级用例（约 18 秒）
```

测试覆盖（`tests/`，共 39 个）：

1. **并发重复提交**（`test_dedupe.py`、`test_concurrency.py`）：20 线程并发提交同一事件、
   40 事件 × 3 次重复 + 3 个 worker 并发投递；断言仅一次首次处理、每个事件副作用恰好 1 次；
2. **瞬时失败重试**（`test_retries.py`）：前 N 次瞬时失败后成功（恰好一次副作用）、
   持续瞬时失败在 `max_attempts` 后进入 `DEAD`、业务拒绝不重试、未知异常进死信；
3. **重启恢复**（`test_restart.py`）：用“关闭连接再以新连接打开同一 DB 文件”模拟进程重启，
   断言超时租约被回收、数据不丢、副作用不重复、cid 与 attempts 不回退不串号；
4. **真实进程级崩溃自愈**（`test_process_restart.py`，起 uvicorn 子进程）：
   - 服务运行中 `SIGKILL` 整个进程组（无优雅收尾），用**同一个本地库**重新起进程，
     验证 WAL 数据不丢、统计不回退、在途事件租约超时后回收、cid/attempts 不串号、
     副作用恰好一次，并按调用方追踪标识反查整条业务链路；
   - 两个真实服务实例（各自 2 个投递线程）并发写同一库，30 个事件（含瞬时失败）
     全部恰好一次成功；
   - 极端参数（`lease_timeout=0.05`、`max_attempts=2`、`base_backoff=0`）下连续两轮
     “认领后即杀”，第三实例不再投递，直接收敛 `DEAD`，attempts 不超上限、终态不回退、
     仍可查询与人工 requeue；
   - 崩溃注入使用下游 `{"mode":"hang"}` 脚本配合 `hang_<marker>.lock/.release`
     标记文件做跨进程同步，不访问任何网络或外部服务；
5. **追踪标识闭环**（`test_tracing.py`）：自带标识端到端不串号、重复/并发提交的赢家
   标识一致与 mismatch 回显、非法标识拒绝、按 cid 跨事件反查事件与历史、人工处置全程
   cid 不变、HTTP 头/请求体两种携带方式与头优先；
6. **极端参数收敛**（`test_convergence.py`）：零租约崩溃循环、`max_attempts=1`、
   零退避即时重试、4 并发后台线程等场景下全部在有限步内进入终态且不产生重复副作用；
7. **批量处置**（`test_batch_ops.py`）：按状态/cid/id 选择、跳过不可处置事件、
   幂等键并发只产生一个批次、幂等重放不重复迁移、空选择器/跨动作键拒绝、
   每个事件独立审计留痕、HTTP 端到端；
8. **HTTP 语义**（`test_api.py`）：201/200/409 可区分、历史可查、人工处置与非法迁移拒绝。

## 模拟下游脚本（payload.simulate）

```json
{"simulate": "succeed"}
{"simulate": {"mode": "transient", "fail_times": 2}}
{"simulate": {"mode": "business", "reason": "account frozen"}}
{"simulate": {"mode": "unknown", "reason": "unexpected schema"}}
{"simulate": {"mode": "hang", "marker": "m1", "timeout": 120}}
```

`hang` 仅用于本地崩溃演练：下游进入处理中途后在 DB 同目录创建
`hang_<marker>.lock` 标记文件并阻塞；出现 `hang_<marker>.release` 文件后放行成功。
测试据此实现“起进程 → 提交 → 等标记出现（事件确定停在 PROCESSING）→ kill -9 →
同库重启 → 放 release → 验证回收成功”的真实进程级验证。

## 代码结构

```
app/
  config.py      # 可配置项（环境变量覆盖），含 worker 并发数与最小租约宽限
  errors.py      # 失败分类：transient / business / unknown 与异常类型
  tracing.py     # 调用方 correlation id 校验/归一化/生成与上下文传递
  models.py      # 状态枚举与 API 模型（接收、列表、批量处置等）
  storage.py     # SQLite：events/event_history/side_effects/batch_ops，事务状态机
                 #   + 条件查询 + 幂等批量处置 + 崩溃循环强制收敛
  downstream.py  # 确定性脚本化的下游消费者模拟（含 hang 崩溃注入）
  retry.py       # 有上限的指数退避
  pipeline.py    # 接收去重 + 调用方标识透传 + 投递处理 + 分类重试（核心编排）
  worker.py      # 多线程后台轮询投递与崩溃租约回收
  api.py         # FastAPI 路由（接收、条件查询、追踪反查、单条/批量处置）
  main.py        # 应用装配与生命周期（启动 worker、关闭存储）
tests/
  _proc/run_service.py      # 真实进程测试用的 uvicorn 子进程入口
  test_tracing.py           # 调用方追踪标识端到端闭环与反查
  test_convergence.py       # 极端参数下的有限收敛
  test_batch_ops.py         # 条件筛选 + 幂等批量处置（含 HTTP）
  test_process_restart.py   # 真实进程 kill -9 / 同库重启 / 多实例并发
  test_dedupe.py / test_concurrency.py / test_retries.py /
  test_restart.py / test_api.py   # 既有能力回归
```

## 兼容性说明

- `POST /events` 的 `correlation_id` 请求体字段为可选新增字段；不带时行为与旧版一致，
  只是标识改由服务端生成。旧客户端无需任何改动。
- SQLite 旧库首次启动会自动补列（`reclaim_count`）并新增索引与 `batch_ops` 表，
  无需手工迁移；历史事件按 `reclaim_count=0` 处理。
- `GET /events`、`/correlations/...`、`/events/batch-*` 均为新增端点，不改变既有路由
  语义；`POST /events/{id}/requeue|discard` 的请求/响应结构保持不变。
