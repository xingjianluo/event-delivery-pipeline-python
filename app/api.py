"""FastAPI 路由：接收、查询、历史、统计、追踪反查、人工处置（单条/批量）。

响应语义（重复提交可区分、不静默、不重复执行）：
- POST /events 首次接收           -> 201, duplicate=false
- POST /events 完全相同的重复提交 -> 200, duplicate=true（不再次处理，沿用原追踪标识）
- POST /events 同 id 不同内容     -> 409 content_conflict
- 追踪标识非法（控制字符/超长）   -> 400 invalid_trace_id
- 终态人工处置：POST /events/{id}/requeue | /discard
- 批量处置：POST /events/batch（仅 DEAD/FAILED，幂等、可审计）
"""
from __future__ import annotations

import json
import time

from fastapi import APIRouter, HTTPException, Query, Request, Response

from app.models import (
    AcceptResult,
    BatchActionRequest,
    BatchActionResult,
    EventIn,
    EventState,
    EventView,
    HistoryItem,
    Stats,
    TraceView,
)
from app.pipeline import ContentConflict, InvalidTraceId
from app.tracing import new_batch_id, new_correlation_id

router = APIRouter()

_DISPOSABLE_STATES = {EventState.DEAD, EventState.FAILED}


def _request_cid(request: Request) -> str:
    cid = request.headers.get("x-correlation-id")
    return cid or new_correlation_id()


def _trace_id_from(request: Request, body: EventIn) -> str | None:
    # 显式头优先；否则用请求体字段；都没有则 None（服务端生成）
    header = request.headers.get("x-correlation-id")
    return header if header is not None else body.trace_id


@router.post("/events", response_model=AcceptResult, status_code=201)
def receive_event(body: EventIn, request: Request, response: Response) -> AcceptResult:
    pipeline = request.app.state.pipeline
    trace_id = _trace_id_from(request, body)
    try:
        result = pipeline.receive(
            body.event_id, body.event_type, body.payload, trace_id=trace_id
        )
    except InvalidTraceId as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_trace_id", "message": str(exc)},
        ) from exc
    except ContentConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "content_conflict",
                "event_id": body.event_id,
                "existing_content_hash": exc.existing_hash,
                "incoming_content_hash": exc.incoming_hash,
                "message": "same event_id with different content is rejected",
            },
        ) from exc
    if result.duplicate:
        response.status_code = 200
    return result


@router.get("/events", response_model=list[EventView])
def list_events(
    request: Request,
    state: list[EventState] | None = Query(default=None),
    trace_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[EventView]:
    """按状态（可重复参数，如 ?state=DEAD&state=FAILED）与/或追踪标识筛选。"""
    storage = request.app.state.storage
    states = [s.value for s in state] if state else None
    return storage.list_events(
        states=states, trace_id=trace_id, limit=limit, offset=offset
    )


@router.get("/traces/{trace_id}", response_model=TraceView)
def get_trace(trace_id: str, request: Request) -> TraceView:
    """拿着调用方追踪标识反查：关联了哪些事件、每个走到哪一步、副作用计数。"""
    storage = request.app.state.storage
    data = storage.trace_view(trace_id)
    if data is None:
        raise HTTPException(
            status_code=404, detail={"error": "trace_not_found", "trace_id": trace_id}
        )
    return TraceView(**data)


@router.post("/events/batch", response_model=BatchActionResult)
def batch_action(body: BatchActionRequest, request: Request) -> BatchActionResult:
    """批量人工处置（requeue/discard）。

    - 只处置 DEAD/FAILED；SUCCEEDED/PENDING/PROCESSING/DISCARDED 进入 skipped_terminal；
    - **fail-closed 圈选**：filter.state 显式给值时，只取其与 {DEAD,FAILED} 的交集；
      交集为空（例如只给 SUCCEEDED）就当“圈不到任何可处置事件”，一个都不选、
      绝不会退化为全库扫描波及无关死信；state=[]（空列表）同样按空集合处理；
    - 幂等：重复执行第二次 changed 为空，不产生新的状态迁移与副作用；
    - 非 dry-run 在 manual_batches 留审计（0 命中时 changed_count=0，绝不虚报条数），
      历史迁移带 batch_id；
    - 可用 event_ids 显式指定，或用 filter（state/trace_id）圈定，二者至少给一个。
    """
    storage = request.app.state.storage
    actor_cid = _request_cid(request)

    explicit_ids = body.event_ids or []
    # None 表示 filter 未给 state 约束（可在 DEAD/FAILED 全量圈定）；
    # set（含空集）表示调用方显式给了 state 列表——空交集必须按“圈不到”处理，
    # 绝不允许退化为无状态约束的全量扫描（fail-closed）。
    filter_states: set[str] | None = None
    filter_trace = body.filter.trace_id if body.filter else None
    if body.filter and body.filter.state is not None:
        filter_states = {s.value for s in body.filter.state}

    if not explicit_ids and filter_states is None and filter_trace is None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "empty_selector",
                "message": "provide event_ids or filter (state and/or trace_id)",
            },
        )

    selected: list[str] = list(dict.fromkeys(explicit_ids))
    if filter_states is not None or filter_trace is not None:
        allowed = {s.value for s in _DISPOSABLE_STATES}
        wanted = allowed if filter_states is None else filter_states & allowed
        # 显式 state 与可处置状态没有交集（例如只给了 SUCCEEDED）：
        # 条件本身圈不出任何可处置事件，直接空集合，一个都不查/不选。
        if wanted:
            views = storage.list_events(
                states=sorted(wanted), trace_id=filter_trace, limit=500
            )
            for v in views:
                if v.event_id not in selected:
                    selected.append(v.event_id)

    batch_id = new_batch_id()
    selector = json.dumps(
        {"event_ids": body.event_ids or [], "filter": body.filter.model_dump() if body.filter else None},
        ensure_ascii=False, sort_keys=True,
    )
    result = storage.batch_dispose(
        action=body.action,
        event_ids=selected,
        correlation_id=actor_cid,
        batch_id=batch_id,
        selector=selector,
        dry_run=body.dry_run,
        now=time.time(),
    )
    return BatchActionResult(**result)


@router.get("/batch-actions/{batch_id}")
def get_batch_action(batch_id: str, request: Request) -> dict:
    storage = request.app.state.storage
    data = storage.get_batch(batch_id)
    if data is None:
        raise HTTPException(
            status_code=404, detail={"error": "batch_not_found", "batch_id": batch_id}
        )
    return data


@router.get("/events/{event_id}", response_model=EventView)
def get_event(event_id: str, request: Request) -> EventView:
    storage = request.app.state.storage
    view = storage.get_event(event_id)
    if view is None:
        raise HTTPException(status_code=404, detail={"error": "not_found", "event_id": event_id})
    return view


@router.get("/events/{event_id}/history", response_model=list[HistoryItem])
def get_history(event_id: str, request: Request) -> list[HistoryItem]:
    storage = request.app.state.storage
    if storage.get_event(event_id) is None:
        raise HTTPException(status_code=404, detail={"error": "not_found", "event_id": event_id})
    return storage.history(event_id)


@router.get("/stats", response_model=Stats)
def get_stats(request: Request) -> Stats:
    return Stats(**request.app.state.storage.stats())


@router.post("/events/{event_id}/requeue", response_model=EventView)
def requeue_event(event_id: str, request: Request) -> EventView:
    storage = request.app.state.storage
    cid = _request_cid(request)
    if not storage.requeue(event_id, cid, time.time()):
        view = storage.get_event(event_id)
        if view is None:
            raise HTTPException(404, detail={"error": "not_found", "event_id": event_id})
        raise HTTPException(
            409,
            detail={"error": "invalid_transition", "event_id": event_id, "state": view.state.value},
        )
    return storage.get_event(event_id)  # type: ignore[return-value]


@router.post("/events/{event_id}/discard", response_model=EventView)
def discard_event(event_id: str, request: Request) -> EventView:
    storage = request.app.state.storage
    cid = _request_cid(request)
    if not storage.discard(event_id, cid, time.time()):
        view = storage.get_event(event_id)
        if view is None:
            raise HTTPException(404, detail={"error": "not_found", "event_id": event_id})
        raise HTTPException(
            409,
            detail={"error": "invalid_transition", "event_id": event_id, "state": view.state.value},
        )
    return storage.get_event(event_id)  # type: ignore[return-value]
