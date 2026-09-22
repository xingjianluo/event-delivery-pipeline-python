# Event Delivery Pipeline（本地版）

一个仅依赖本地存储（SQLite）与仓库内 Python 依赖（FastAPI）的事件接收、投递与消费演示服务。
目标是在**重复投递、失败重试、并发压力与进程重启**下，让事件状态保持一致、可解释、可追溯。

- 不依赖任何外部服务（无消息队列、无 Redis、无外部数据库），不需要任何凭据；
- 持久化仅使用进程工作目录下的一个 SQLite 文件（默认 `./data/events.db`，WAL 模式）；
- 下游用确定性的内存脚本模拟（`app/downstream.py`），可按需编排成功 / 瞬时失败 / 业务拒绝 / 未知异常。

## 快速开始

```bash
uv sync                       # 安装依赖（含开发依赖 pytest）
uv run pytest -q              # 运行全部测试（16 个，含并发/重试/重启恢复场景）
uv run uvicorn main:app       # 启动服务（默认 http://127.0.0.1:8000，后台 worker 自动投递）
```

提交事件：

```bash
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-1","event_type":"order.created","payload":{"order_id":"o-1"}}'
```

## API

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /events` | 接收事件。首次 `201 duplicate=false`；内容完全相同的重复提交 `200 duplicate=true`（沿用原 `correlation_id`，不再次处理）；同一 `event_id` 但内容哈希不同 `409 content_conflict` |
| `GET /events/{id}` | 查询事件当前状态、尝试次数、下次重试时间、最近错误与失败分类 |
| `GET /events/{id}/history` | 该事件完整状态流转历史（每次迁移一行，含 cid、attempt、原因、操作者） |
| `GET /stats` | 按状态计数的统计口径，以及已产生副作用的事件总数 |
| `POST /events/{id}/requeue` | 人工处置：把 `DEAD/FAILED` 事件重新入队（attempts 清零，留痕） |
| `POST /events/{id}/discard` | 人工处置：把 `DEAD/FAILED` 事件标记为 `DISCARDED`（留痕，终态） |

请求可携带 `X-Correlation-ID` 头作为调用方侧关联标识；事件自身的 `correlation_id`
在首次接收时生成并持久化，后续每次后台投递/重试/人工处置都沿用它。

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
   │                              └─未知错误 / 瞬时重试耗尽─▶ DEAD（终态，可人工处置）
   │
   └──────── requeue(DEAD/FAILED，attempts 清零)
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
  `requeue` / `discard` 人工处置，处置动作同样写历史（`actor=manual:<cid>`）；
- 重试上限 `max_attempts` **含首次尝试**（默认 3，即最多 1 次首投 + 2 次重试）；
- 崩溃恢复时对超时租约的回收也会计入 `attempts`（每次认领/回收即一次投递尝试），
  因此"崩溃循环"同样受上限约束，事件最终进入 `DEAD` 而不是被无限重试或丢失。

## 重启恢复

后台 worker 认领事件时记录 `leased_at` 与 `lease_owner`。若进程在写终态前崩溃，
事件停留在 `PROCESSING`；重启后超过 `lease_timeout` 仍未完成的租约会被自动回收：
状态回到 `PROCESSING`（attempt+1，历史留痕 `reclaimed after stale lease`）并重新投递。
WAL 日志保证已提交数据不丢；事件持久化的 `correlation_id` 保证跨重启不串号、不错配。

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
| `EDP_POLL_INTERVAL` | `0.05` | 后台 worker 轮询间隔（秒） |
| `EDP_RECOVERY_ON_STARTUP` | `true` | 启动时是否扫描并告警遗留租约 |

## 本地验证要点

```bash
uv run pytest -q -s          # 测试日志打印 event_id、cid 与每次状态流转依据
```

测试覆盖（`tests/`）：

1. **并发重复提交**（`test_dedupe.py`、`test_concurrency.py`）：20 线程并发提交同一事件、
   40 事件 × 3 次重复 + 3 个 worker 并发投递；断言仅一次首次处理、每个事件副作用恰好 1 次；
2. **瞬时失败重试**（`test_retries.py`）：前 N 次瞬时失败后成功（恰好一次副作用）、
   持续瞬时失败在 `max_attempts` 后进入 `DEAD`、业务拒绝不重试、未知异常进死信；
3. **重启恢复**（`test_restart.py`）：用"关闭连接再以新连接打开同一 DB 文件"模拟进程重启，
   断言超时租约被回收、数据不丢、副作用不重复、cid 与 attempts 不回退不串号；
4. **HTTP 语义**（`test_api.py`）：201/200/409 可区分、历史可查、人工处置与非法迁移拒绝。

## 模拟下游脚本（payload.simulate）

```json
{"simulate": "succeed"}
{"simulate": {"mode": "transient", "fail_times": 2}}
{"simulate": {"mode": "business", "reason": "account frozen"}}
{"simulate": {"mode": "unknown", "reason": "unexpected schema"}}
```

## 代码结构

```
app/
  config.py      # 可配置项（环境变量覆盖）
  errors.py      # 失败分类：transient / business / unknown 与异常类型
  tracing.py     # correlation id 生成与上下文传递
  models.py      # 状态枚举与 API 模型
  storage.py     # SQLite：events / event_history / side_effects，事务状态机
  downstream.py  # 确定性脚本化的下游消费者模拟
  retry.py       # 有上限的指数退避
  pipeline.py    # 接收去重 + 投递处理 + 分类重试（核心编排）
  worker.py      # 后台轮询投递与崩溃租约回收
  api.py         # FastAPI 路由
  main.py        # 应用装配与生命周期（启动 worker、关闭存储）
tests/           # 并发 / 重试 / 重启恢复 / HTTP 测试
```
