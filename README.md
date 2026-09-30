# Event Delivery Pipeline（本地版）

一个仅依赖本地存储（SQLite）与仓库内 Python 依赖（FastAPI）的事件接收、投递与消费演示服务。
目标是在**重复投递、失败重试、并发压力与进程重启**下，让事件状态保持一致、可解释、可追溯。

- **调用方追踪标识闭环**：提交时可携带自己的标识（头或请求体），接收、落库、后台投递、
  重试、崩溃回收、人工处置到历史查询全程沿用，并可凭它反查关联事件与各自状态；重复/并发/
  重启都不串号、不替换；
- **可运维**：按状态/追踪标识筛选事件，批量重放或批量丢弃（幂等、可审计、只动死信/失败）；
- **崩溃自愈与有限收敛**：真实进程被 `SIGKILL` 后用同一本地库重启可自动回收在途事件，
  极端参数下异常循环也会在有限次数内进入可查询、可人工处理的终态；
- 不依赖任何外部服务（无消息队列、无 Redis、无外部数据库），不需要任何凭据；
- 持久化仅使用进程工作目录下的一个 SQLite 文件（默认 `./data/events.db`，WAL 模式，并对旧库轻量迁移）；
- 下游用确定性的内存脚本模拟（`app/downstream.py`），可按需编排成功 / 瞬时失败 / 业务拒绝 / 未知异常 / 处理中崩溃。

## 快速开始

```bash
uv sync                       # 安装依赖（含开发依赖 pytest）
uv run pytest -q              # 运行全部测试（37 个，含真实进程 kill/重启、并发/重试/收敛场景）
uv run uvicorn main:app       # 启动服务（默认 http://127.0.0.1:8000，后台 worker 自动投递）
```

提交事件（可携带调用方自己的追踪标识）：

```bash
# 方式一：X-Correlation-ID 头
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: biz-order-42' \
  -d '{"event_id":"evt-1","event_type":"order.created","payload":{"order_id":"o-1"}}'

# 方式二：请求体 trace_id 字段（两者同时给时以头为准）
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-1","event_type":"order.created","payload":{},"trace_id":"biz-order-42"}'

# 拿着标识反查这次业务操作关联了哪些事件、各自走到哪一步
curl -s localhost:8000/traces/biz-order-42
```

## API

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /events` | 接收事件。首次 `201 duplicate=false`；内容完全相同的重复提交 `200 duplicate=true`（沿用首次持久化的追踪标识，不再次处理）；同一 `event_id` 但内容哈希不同 `409 content_conflict`；追踪标识含控制字符或超长 `400 invalid_trace_id` |
| `GET /events/{id}` | 查询事件当前状态、尝试次数、下次重试时间、最近错误与失败分类 |
| `GET /events/{id}/history` | 该事件完整状态流转历史（每次迁移一行，含追踪标识、attempt、原因、操作者、批量批次号） |
| `GET /events` | 按条件筛选事件：`state`（可重复，如 `?state=DEAD&state=FAILED`）、`trace_id`、`limit`、`offset` |
| `GET /traces/{trace_id}` | 按调用方追踪标识反查：关联事件清单、各状态计数、副作用总数；标识不存在返回 404 |
| `GET /stats` | 按状态计数的统计口径，以及已产生副作用的事件总数 |
| `POST /events/{id}/requeue` | 人工处置：把 `DEAD/FAILED` 事件重新入队（attempts 清零，留痕） |
| `POST /events/{id}/discard` | 人工处置：把 `DEAD/FAILED` 事件标记为 `DISCARDED`（留痕，终态） |
| `POST /events/batch` | 批量人工处置（`requeue`/`discard`），按 `event_ids` 或 `filter`（`state`/`trace_id`）圈选，支持 `dry_run`；幂等、可审计，只动 `DEAD/FAILED` |
| `GET /batch-actions/{batch_id}` | 查询一次批量处置的审计记录（动作、选择器、改了哪些事件、操作者、时间） |

### 追踪标识（trace_id）闭环

- 调用方提交事件时可携带**自己的追踪标识**（`X-Correlation-ID` 头或请求体 `trace_id`，
  头优先）；未提供时服务端生成 `cid_` 前缀标识。该标识在**首次接收时持久化**，
  此后接收去重、落库、后台投递、重试、崩溃回收、人工处置、历史查询全程沿用同一个值；
- **一次业务操作可包含多个事件**，它们允许共用同一个追踪标识（该列非唯一、带索引）。
  用 `GET /traces/{trace_id}` 可一次反查该操作关联的所有事件及每个事件的当前状态；
- **重复提交 / 并发提交 / 进程重启都不会替换或串号**：重复提交即使携带了不同标识，
  存储与响应也始终返回首次持久化的那个值（响应额外带 `trace_id_mismatch=true` 仅作
  提示），调用方总能对上自己首次带进系统的值；
- 校验：去除首尾空白；空值视为未提供；拒绝控制字符（防日志注入）；长度上限 128；
- 响应中的 `trace_id` 与 `correlation_id` 永远是同一个值（后者为兼容字段保留）。

### 批量处置

`POST /events/batch` 请求体：

```json
{
  "action": "discard",
  "filter": {"state": ["DEAD", "FAILED"], "trace_id": "biz-order-42"},
  "dry_run": false
}
```

也可用 `"event_ids": ["e1","e2"]` 显式指定（`event_ids` 与 `filter` 至少给一个，否则
`400 empty_selector`）。返回 `batch_id`、`changed`、`unchanged`、`skipped_terminal`、
`not_found`。保证：

- **只处置 `DEAD/FAILED`**：`SUCCEEDED/PENDING/PROCESSING/DISCARDED` 一律进入
  `skipped_terminal`，已正常完成的事件绝不会被卷入，因此不可能产生第二次副作用；
  即使 `filter.state` 里写了 `SUCCEEDED`，也会被收敛到可处置集合；
- **圈不到就一个都不动（fail-closed，绝不漏成全量）**：`filter.state` 显式给值时，
  只取它与 `{DEAD, FAILED}` 的交集；交集为空——例如只传 `["SUCCEEDED"]`（或
  `PENDING/PROCESSING/DISCARDED` 等任何不可人工处置的组合）、空列表 `[]`，或
  `state` 与 `trace_id` 组合后没有交集——本次请求的候选集合就是空：**不会改动任何
  事件**，也不会波及追踪标识不同、与筛选完全无关的死信/失败事件，不产生任何状态
  迁移历史与副作用；响应 `matched=0, changed=[]`。非 dry-run 时审计行如实记录
  `matched=0, changed_count=0, changed_ids=[]`（可查但绝不虚报条数），dry-run 不落
  审计；
- **幂等**：圈选与迁移在同一个 `BEGIN IMMEDIATE` 事务内完成；重复执行第二次
  `changed` 为空，不产生新的状态迁移与副作用；多实例 / 后台投递并发交叉、或重复
  提交同一个批量请求时，上述边界同样成立；
- **可审计**：非 `dry_run` 在 `manual_batches` 落一行，每条历史迁移带 `batch_id`，
  可用 `GET /batch-actions/{batch_id}` 回查；`dry_run` 只预演不落库、不改状态。

请求可携带 `X-Correlation-ID` 头作为调用方侧关联标识；事件自身的追踪标识
在首次接收时确定并持久化，后续每次后台投递/重试/人工处置都沿用它。

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
   │                              └─最后一次尝试也崩溃(租约超时且已达上限)─▶ DEAD（隔离，可人工处置）
   │
   └──────── requeue(DEAD/FAILED，attempts 清零；单条或批量)
   DEAD/FAILED ──discard(单条或批量)──▶ DISCARDED（终态）
```

## 失败分类与终态规则

| 分类 (`failure_kind`) | 含义 | 是否重试 | 终态 |
| --- | --- | --- | --- |
| `transient` | 可恢复瞬时错误（超时、临时不可用等） | 是，有上限的指数退避 | 重试耗尽 → `DEAD` |
| `business` | 业务性拒绝（非法账户、规则拒绝等） | **永不重试** | `FAILED` |
| `unknown` | 无法归类的异常（防御性兜底） | **不重试**，避免未知错误无限放大 | `DEAD`（等待人工研判） |

- 任何异常都不会逃逸出处理循环、不会静默丢弃；未被识别的异常一律归入 `unknown`；
- `DEAD/FAILED` 均可通过 `GET /events/{id}` 与 `/history` 查询，并通过
  `requeue` / `discard` 人工处置，处置动作同样写历史（`actor=manual:<cid>`）；
- 重试上限 `max_attempts` **含首次尝试**（默认 3，即最多 1 次首投 + 2 次重试）；
- 崩溃恢复时对超时租约的回收也会计入 `attempts`（每次认领/回收即一次投递尝试）；
- **崩溃循环有限收敛**：若一条 `PROCESSING` 事件的**最后一次**投递尝试也以
  worker 崩溃/租约超时告终（没有预算再回收重投），下一次扫描会把它**原子隔离到
  `DEAD`**（历史留痕 `stale lease after final attempt crashed; quarantined ...`），
  而不是无限重认领。`attempts` 精确停在 `max_attempts`，事件进入可查询、可人工
  requeue 的终态。这样即使把 `EDP_LEASE_TIMEOUT` 调到 0、worker 每轮都被杀，
  单条事件的认领次数也不会超过上限，状态只进不退。最后一次尝试机会仍会被真正
  执行（它完全可能成功），不会因前序崩溃被提前剥夺。

## 重启恢复

后台 worker 认领事件时记录 `leased_at` 与 `lease_owner`。若进程在写终态前崩溃
（测试中用 `SIGKILL` 真杀进程验证），事件停留在 `PROCESSING`；用**同一个 SQLite
文件**重新拉起服务后，超过 `lease_timeout` 仍未完成的租约会被自动回收：
状态回到 `PROCESSING`（attempt+1，历史留痕 `reclaimed after stale lease`）并重新投递。
WAL 日志保证已提交数据不丢；事件持久化的追踪标识保证跨重启不串号、不错配，
attempts 与副作用计数不回退、不重复。多个服务进程同时打开同一个库时，认领与终态
写入的原子事务 + attempt 围栏同样保证每个事件副作用至多一次（见多实例测试）。

## 可观测性

- 每次状态迁移都向 `event_history` 写一行：`from/to`、`attempt`、`reason`、
  `actor`（`api:` / `worker:` / `manual:`）、事件追踪标识、时间戳、`batch_id`
  （人工批量处置时关联审计批次）；
- 服务日志同样按事件打印 `event_id`、追踪标识、状态流转依据与失败原因；
- `GET /stats` 提供与历史一致的统计：`total`、各状态计数、副作用总数；
- `GET /traces/{trace_id}` 提供按调用方标识的整条链路视图；
  `GET /batch-actions/{batch_id}` 提供批量处置审计。

## 存储与兼容范围

- 仅用本地单个 SQLite 文件（WAL），表：`events`、`event_history`、`side_effects`、
  `manual_batches`；新增了 `events(correlation_id)`、`events(state)` 等索引；
- **对旧库轻量迁移、不丢数据**：打开旧版本库时会自动给 `event_history` 补
  `batch_id` 列、补建索引与 `manual_batches` 表（`CREATE TABLE IF NOT EXISTS` +
  `PRAGMA table_info` 探测后 `ALTER TABLE ADD COLUMN`），老数据可正常查询与处置；
- 响应保留 `correlation_id` 字段（与新增 `trace_id` 同值），旧调用方无需改动；
- 不引入任何新依赖，不访问外部服务或凭据。

## 可配置项（环境变量，前缀 `EDP_`）

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `EDP_DB_PATH` | `./data/events.db` | SQLite 文件路径 |
| `EDP_MAX_ATTEMPTS` | `3` | 最大尝试次数（含首次，必须 ≥ 1） |
| `EDP_BASE_BACKOFF` | `0.05` | 指数退避基数（秒，≥ 0） |
| `EDP_BACKOFF_FACTOR` | `2.0` | 退避倍数（≥ 1）；第 n 次失败后延迟 `base * factor^(n-1)` |
| `EDP_LEASE_TIMEOUT` | `30.0` | PROCESSING 租约超时（秒，≥ 0；设 0 表示立即视为崩溃在途） |
| `EDP_POLL_INTERVAL` | `0.05` | 后台 worker 轮询间隔（秒，必须 > 0） |
| `EDP_RECOVERY_ON_STARTUP` | `true` | 启动时是否扫描并告警遗留租约 |

参数在启动时校验，非法取值（如 `MAX_ATTEMPTS=0`、`POLL_INTERVAL=0`、负退避）会快速
失败而不是在后台循环里表现为无限重认领。

## 本地验证要点

```bash
uv run pytest -q -s          # 测试日志打印 event_id、追踪标识与每次状态流转依据

# 只跑本次 fail-closed 边界（圈不到可处置事件的各种写法）
uv run pytest tests/test_batch_empty_match.py -q
```

手工复现/验证“只带不可处置状态的批量丢弃不会波及无关死信”：

```bash
uv run uvicorn app.main:app --port 8000 &
# 造一个一定会进 DEAD 的事件（unknown 失败，追踪标识 trace-victim）
curl -s -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"event_id":"victim","event_type":"t","payload":{"simulate":{"mode":"unknown"}},"trace_id":"trace-victim"}'
sleep 1  # 等后台投递把它送到 DEAD
# 只按 SUCCEEDED 圈选批量丢弃 —— 修复前会把 victim 误丢，修复后必须 matched=0
curl -s -X POST localhost:8000/events/batch -H 'content-type: application/json' \
  -d '{"action":"discard","filter":{"state":["SUCCEEDED"]}}'
# 期望：{"matched":0,"changed":[] ...}；victim 仍是 DEAD，且没有 DISCARDED 历史
curl -s localhost:8000/events/victim | python -c 'import sys,json;print(json.load(sys.stdin)["state"])'
curl -s localhost:8000/events/victim/history
```

测试覆盖（`tests/`，共 52 个）：

1. **并发重复提交**（`test_dedupe.py`、`test_concurrency.py`）：20 线程并发提交同一事件、
   40 事件 × 3 次重复 + 3 个 worker 并发投递；断言仅一次首次处理、每个事件副作用恰好 1 次；
2. **瞬时失败重试**（`test_retries.py`）：前 N 次瞬时失败后成功（恰好一次副作用）、
   持续瞬时失败在 `max_attempts` 后进入 `DEAD`、业务拒绝不重试、未知异常进死信；
3. **连接级重启恢复**（`test_restart.py`）：用“关闭连接再以新连接打开同一 DB 文件”模拟重启，
   断言超时租约被回收、数据不丢、副作用不重复、追踪标识与 attempts 不回退不串号；
4. **真实进程崩溃自愈**（`test_process_recovery.py`，用标准库子进程起 uvicorn，无外部依赖）：
   真实起服务进程投递慢事件、处理中途 `SIGKILL`，再用同一个库重新拉起，断言回收留痕、
   attempts=2、追踪标识与统计不回退不重复；两个进程同时打开同一库的多实例并发；
   `MAX_ATTEMPTS=1`、`LEASE_TIMEOUT=0` 等极端参数下真实进程也在有限时间收敛到 `DEAD`；
5. **追踪标识闭环**（`test_tracing.py`）：调用方标识原样贯穿接收/投递/重试/历史、
   多事件共用一标识并反查、重复/并发提交不替换标识（`trace_id_mismatch` 提示）、
   缺失自动生成、非法标识 400、HTTP 头/请求体两种携带方式；
6. **按条件筛选 + 批量处置**（`test_batch.py`）：只动 DEAD/FAILED、成功/在途事件不卷入、
   幂等无重复迁移与副作用、审计落库且历史带 batch_id、dry-run 不改状态、按 state/trace 圈选、
   空选择器 400，以及批量与 worker/重复提交并发的压测；
   - **圈不到可处置事件时 fail-closed**（`test_batch_empty_match.py`）：参数化覆盖
     `state` 只给 `SUCCEEDED/PENDING/PROCESSING/DISCARDED` 及其组合、空状态列表、
     `state`+`trace_id` 无交集、不存在的 trace 等写法，断言所有事件状态与历史条数不变、
     无副作用、审计如实为 0；并复现“只圈 SUCCEEDED 却丢掉无关 trace 死信”的事故场景
     断言受害者保持 DEAD；另含 dry-run 不写审计、同一批量请求重复提交零额外迁移、
     与后台投递并发 40 次重复提交死信仍保持 DEAD、以及 `list_events(states=[])`
     fail-closed 的底层防护；
7. **极端参数收敛**（`test_convergence.py`）：`max_attempts=1` 立即死信、`lease_timeout=0`
   崩溃循环被上限隔离到 DEAD、attempts 围栏拒绝迟到结果防状态回退、非法配置启动即失败；
8. **HTTP 语义**（`test_api.py`）：201/200/409 可区分、历史可查、人工处置与非法迁移拒绝。

## 模拟下游脚本（payload.simulate）

```json
{"simulate": "succeed"}
{"simulate": {"mode": "transient", "fail_times": 2}}
{"simulate": {"mode": "business", "reason": "account frozen"}}
{"simulate": {"mode": "unknown", "reason": "unexpected schema"}}
{"simulate": {"mode": "slow", "delay": 1.0, "then": "succeed"}}
```

`slow` 模式会在处理期间确定性睡眠（默认睡完成功），用于在真实进程上稳定制造
“认领后、写终态前被 kill”的崩溃在途窗口；`then` 设为 `crash_fail` 时睡完抛瞬时错误。

## 代码结构

```
app/
  config.py      # 可配置项（环境变量覆盖）+ 启动参数校验
  errors.py      # 失败分类：transient / business / unknown 与异常类型
  tracing.py     # 追踪标识：调用方标识规范化/校验、生成与上下文传递
  models.py      # 状态枚举与 API 模型（含 trace 反查、批量处置模型）
  storage.py     # SQLite：events / event_history / side_effects / manual_batches，
                 #   事务状态机、认领上限隔离、条件查询、批量处置、旧库轻量迁移
  downstream.py  # 确定性脚本化的下游消费者模拟（含 slow 崩溃窗口模式）
  retry.py       # 有上限的指数退避
  pipeline.py    # 接收去重 + 调用方追踪标识 + 投递处理 + 分类重试（核心编排）
  worker.py      # 后台轮询投递与崩溃租约回收
  api.py         # FastAPI 路由（接收/筛选/追踪反查/单条与批量人工处置/审计）
  main.py        # 应用装配与生命周期（启动 worker、关闭存储）
tests/           # 并发 / 重试 / 连接级与真实进程重启 / 追踪 / 批量 / 收敛 / HTTP 测试
```
