# Event Delivery Pipeline（本地版）

一套仅依赖**本地存储（SQLite）与仓库已有依赖（FastAPI/httpx/pydantic）**的事件接收、投递与消费流程，
不连接任何外部服务、不需要任何凭据。重点解决：

- 并发重复提交下去重与“只处理一次”；
- 消费失败的**分类处理**（瞬时错误有上限重试 / 业务拒绝不重试 / 无法归类进死信）；
- 跨请求与后台阶段的**关联标识（correlation id）**与完整流转历史；
- 进程重启后的状态恢复（不回退、不串号、副作用不重复）。

## 1. 快速开始

```bash
uv sync                                   # 安装已锁定依赖（含 fastapi/httpx）
.venv/bin/python -m uvicorn main:app --port 8000
# 另一个终端：
curl -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: cid-demo-1' \
  -d '{"event_id":"e-1","type":"order.created","payload":{"simulate":"transient"}}'
curl localhost:8000/events/e-1
curl localhost:8000/stats
```

事件处理由后台 worker 线程池异步完成（接收即持久化、立刻返回 `PENDING`）。

### 运行测试

```bash
.venv/bin/python -m unittest discover -s tests -v
```

测试共 4 组 15 个用例，覆盖：

| 文件 | 覆盖内容 |
| --- | --- |
| `tests/test_core.py` | 重复提交显式反馈、内容冲突拒绝、幂等键去重、三类失败分类、重试上限、死信终态、人工 replay、统计口径 |
| `tests/test_concurrency.py` | 30 个事件 × 8 线程并发重复提交（Barrier 同时撞击），断言仅 1 次接收、1 次副作用、cid 不串号 |
| `tests/test_recovery.py` | “副作用已落盘但终态未推进”崩溃恢复、重试链途中崩溃恢复、真实 uvicorn 子进程重启后状态/统计一致 |
| `tests/test_api.py` | HTTP 状态码语义（201/200/409/404）、`X-Correlation-ID` 回显、死信接口 |

测试日志会打印每个关键事件的事件标识、尝试次数、失败分类、状态流转依据与关联标识
（见 `tests/helpers.py::print_trace`）。

## 2. HTTP API

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /events` | 接收事件。首次 `201`；重复 `200` 且 `duplicate=true`；同 id 不同内容 `409 CONTENT_CONFLICT` |
| `GET /events/{event_id}` | 查询事件状态、每次尝试明细、完整生命周期历史、副作用计数 |
| `GET /dead-letters` | 列出全部死信（终态、可人工处置） |
| `POST /dead-letters/{event_id}/replay?extra_budget=N` | 人工把死信重新入队，追加 N 次投递预算（默认等于 `EDP_MAX_ATTEMPTS`） |
| `GET /stats` | 各状态计数、尝试次数、按分类的失败次数、副作用总数 |
| `GET /health` | 健康检查 |

请求体：

```json
{
  "event_id": "evt-001",          // 必填，由调用方保证稳定，用作幂等主键
  "type": "order.created",        // 必填，事件类型（参与内容哈希）
  "payload": {"simulate": "..."},// 业务内容；simulate 字段用于注入下游故障（见第 5 节）
  "idempotency_key": "idem-xxx"   // 可选；存在时同样参与唯一去重
}
```

所有响应回显请求头 `X-Correlation-ID`；未传时服务端生成 `recv_*`。
后台每次投递尝试生成独立的 `att_*` 关联标识，人工操作生成 `op_*`，
它们共同出现在 `event_history` / `attempts` 中，可跨请求与后台阶段串联对账。

## 3. 投递语义

### 3.1 接收去重（幂等接收，处理只发生一次）

- 主键：`event_id`；若提供 `idempotency_key`，它同样受唯一约束保护。
- 接收在 `BEGIN IMMEDIATE` 事务内先查后插：并发提交同一标识时，**只有一个请求插入成功**；
  其余请求得到 `duplicate=true` 的显式反馈（HTTP 200），携带 `original_correlation_id`
  指向首次接收，绝不静默忽略，也绝不再次执行处理。
- 内容一致性：对 `{type, payload, idempotency_key}` 做规范化 JSON + SHA-256 得到 `content_hash`。
  标识相同但哈希不同 → `409 CONTENT_CONFLICT`，明确拒绝，原事件不被覆盖，冲突事件进入历史。
- 重复/冲突只写一条观测历史（`DUPLICATE_SUBMISSION` / `CONTENT_CONFLICT_REJECTED`），
  不改变事件状态。

### 3.2 至少一次投递 + 幂等副作用 ≈ 有效一次

- worker 通过条件 `UPDATE ... WHERE locked_by IS NULL AND status IN (PENDING, RETRYING)`
  原子领取事件；同一事件在任一时刻只可能被一个 worker 处理。
- 状态推进全部是**带前置条件（属主锁 + 非终态 + 在途尝试号）的单条 UPDATE（CAS）**，
  并与尝试结果、历史在同一事务提交。重复/迟到的推进一律 0 行生效并被拒绝。
- 下游副作用写入 `side_effects` 表，`UNIQUE(event_id, downstream_ref)`。
  若进程在“副作用生效之后、DELIVERED 提交之前”崩溃，恢复后重投递会检测到副作用已存在，
  按**幂等成功**收敛到 `DELIVERED`，不会第二次产生副作用。

### 3.3 状态机

```
                 ┌─────────────── TRANSIENT 且仍有预算 ───────────────┐
                 ▼                                                     │
PENDING ──► RETRYING ──成功──► DELIVERED（终态）                        │
  │            │                                                        │
  │            ├──TRANSIENT 且预算用尽──► DEAD（RETRIES_EXHAUSTED，终态）│
  │            ├──BUSINESS（任意一次）───► DEAD（MOVED_TO_DEAD，终态）   │
  │            └──UNKNOWN（任意一次）────► DEAD（MOVED_TO_DEAD，终态）   │
  └──首次即 BUSINESS/UNKNOWN───────────► DEAD（MOVED_TO_DEAD，终态）    │
                                                                         │
DEAD ──人工 replay（追加预算）──► PENDING（历史保留、尝试编号累计递增）──┘
```

终态只有两个：`DELIVERED`、`DEAD`。进入终态后任何路径都不能再改变或重复产生副作用。

## 4. 失败分类与重试规则

| 分类 | 触发（模拟） | HTTP 语义 | 是否重试 | 去向 |
| --- | --- | --- | --- | --- |
| `TRANSIENT` | 下游暂时不可用、超时等可恢复错误 | 503 | 是，指数退避，**总尝试次数 ≤ `EDP_MAX_ATTEMPTS`** | 成功→`DELIVERED`；用尽→`DEAD/RETRIES_EXHAUSTED` |
| `BUSINESS` | 业务规则永久拒绝（载荷不合法等） | 422 | **否** | 首次即 `DEAD/MOVED_TO_DEAD` |
| `UNKNOWN` | 无法归类 / 处理器抛出未预期异常 | 500 | **否**（避免对未知错误盲目重试） | 首次即 `DEAD/MOVED_TO_DEAD` |

- 退避：`delay = min(EDP_RETRY_BASE_DELAY * 2**(attempts_done-1), EDP_RETRY_MAX_DELAY)`。
- 每次尝试独立记录 `attempts`（尝试号、分类、错误码、http 状态、详情、`att_*` cid），
  失败原因通过 `failure_kind` + `last_error_code` 可区分、可查询。
- 死信**永不被静默丢弃**：可经 `GET /dead-letters` 查询，并通过 replay 接口人工处置；
  replay 不清零历史，仅追加预算，尝试编号累计。

## 5. 本地模拟的下游

`app/downstream.py` 用 payload 中的 `simulate` 字段注入故障，无任何外部调用：

| 值 | 行为 |
| --- | --- |
| 缺省 / `ok` / `flaky` | 成功，写入一条幂等副作用 |
| `transient` | 前 `EDP_TRANSIENT_SIM_ATTEMPTS` 次投递抛 `503 DOWNSTREAM_UNAVAILABLE`，之后成功 |
| `business` | 始终抛 `422 BUSINESS_RULE_REJECTED` |
| `unknown` | 始终抛 `500 UNCLASSIFIABLE_FAILURE` |

## 6. 进程重启恢复

`EventPipeline` 初始化时执行 `recover_stuck`：

- 找出 `locked_by IS NOT NULL` 且非终态的事件（崩溃时在途），清除锁并把 `next_run_at`
  提前到当前时刻，写一条 `RECOVERED_AFTER_RESTART` 历史（cid=`recovery`）；
- **不回退**任何已完成事实（attempts、失败分类、副作用全部保留）；
- 已达预算上限的事件被重新领取后会立即判死；副作用已存在的重投递按幂等成功收敛。
- `tests/test_recovery.py::test_real_process_restart` 用真实 uvicorn 子进程验证：
  重启后状态不回退、`receive_correlation_id` 不串号、`/stats` 前后完全一致。

## 7. 可配置项（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `EDP_DB_PATH` | `data/event_pipeline.db` | SQLite 文件路径（自动建目录） |
| `EDP_MAX_ATTEMPTS` | `4` | 每事件最大尝试次数（含首次） |
| `EDP_RETRY_BASE_DELAY` | `0.2` | 指数退避基准秒数 |
| `EDP_RETRY_MAX_DELAY` | `5.0` | 退避上限秒数 |
| `EDP_WORKER_COUNT` | `4` | 后台 worker 线程数 |
| `EDP_POLL_INTERVAL` | `0.05` | worker 空轮询间隔（新事件会主动唤醒） |
| `EDP_TRANSIENT_SIM_ATTEMPTS` | `2` | `simulate=transient` 时前几次失败 |

SQLite 固定开启 `WAL`、`busy_timeout=30s`，每个工作线程独立连接。

## 8. 代码结构

```
main.py              # uvicorn 入口
app/models.py        # 状态/失败分类枚举、Pydantic 数据结构、cid 生成
app/config.py        # 环境变量配置
app/storage.py       # SQLite schema、幂等接收、原子领取、CAS 状态推进、历史/副作用/统计/恢复
app/downstream.py    # 模拟下游：失败分类异常 + 幂等副作用
app/worker.py        # worker 线程池、退避调度、失败分类驱动的状态机
app/pipeline.py      # 编排层：接收去重、查询组装、死信 replay、重启恢复
app/api.py           # FastAPI 路由与状态码语义
tests/               # unittest 测试（标准库 + fastapi TestClient + uvicorn 子进程）
```
