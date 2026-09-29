"""FastAPI 路由：接收、查询、历史、统计、人工处置（含条件筛选与批量处置）。

响应语义（重复提交可区分、不静默、不重复执行）：
- POST /events 首次接收           -> 201, duplicate=false
- POST /events 完全相同的重复提交 -> 200, duplicate=true（不再次处理，沿用原 cid）
- POST /events 同 id 不同内容     -> 409 ContentConflict
- 调用方追踪标识：X-Correlation-ID 头（优先）或请求体 correlation_id 字段，
  原样持久化、原样返回；非法格式 -> 400；
- 终态人工处置：POST /events/{id}/requeue | /discard，
  批量幂等处置：POST /events/batch-requeue | /batch-discard。
"""
from __future__ import annotations

import time

from fastapi import APIRouter, HTTPException, Query, Request, Response

from app.models import (
    AcceptResult,
    BatchDisposeRequest,
    BatchDisposeResult,
    EventIn,
    EventList,
    EventState,
    EventView,
    HistoryItem,
    Stats,
    normalize_states,
)
from app.pipeline import ContentConflict
from app.storage import BatchIdempotencyConflict
from app.tracing import InvalidCorrelationId, validate_correlation_id

router = APIRouter()


def _request_cid(request: Request) -> str | None:
    """调用方追踪标识：头优先；未提供返回 None（由服务端生成）。

    校验在统一入口完成，非法值返回 400 而不是被悄悄替换。
    """
    raw = request.headers.get("x-correlation-id")
    if raw is None:
        return None
    try:
        return validate_correlation_id(raw)
    except InvalidCorrelationId as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_correlation_id", "message": str(exc)},
        ) from exc


def _resolve_incoming_cid(body: EventIn, request: Request) -> str | None:
    header_cid = _request_cid(request)
    if header_cid is not None:
        return header_cid
    try:
        return validate_correlation_id(body.correlation_id)
    except InvalidCorrelationId as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_correlation_id", "message": str(exc)},
        ) from exc


@router.post("/events", response_model=AcceptResult, status_code=201)
def receive_event(body: EventIn, request: Request, response: Response) -> AcceptResult:
    pipeline = request.app.state.pipeline
    cid = _resolve_incoming_cid(body, request)
    try:
        result = pipeline.receive(
            body.event_id, body.event_type, body.payload, correlation_id=cid
        )
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
    except InvalidCorrelationId as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_correlation_id", "message": str(exc)},
        ) from exc
    if result.duplicate:
        response.status_code = 200
    return result


@router.get("/events", response_model=EventList)
def list_events(
    request: Request,
    state: list[EventState] | None = Query(default=None),
    correlation_id: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> EventList:
    """按状态（可重复传参，如 ?state=DEAD&state=FAILED）、追踪标识、事件类型筛选。"""
    storage = request.app.state.storage
    try:
        cid = validate_correlation_id(correlation_id)
    except InvalidCorrelationId as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_correlation_id", "message": str(exc)},
        ) from exc
    states = normalize_states(state)
    items = storage.find_events(
        states=[s.value for s in states] if states else None,
        correlation_id=cid,
        event_type=event_type,
        limit=limit,
        offset=offset,
    )
    return EventList(
        items=items,
        count=len(items),
        states=states,
        correlation_id=cid,
        event_type=event_type,
        limit=limit,
        offset=offset,
    )


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


@router.get("/correlations/{correlation_id}/events", response_model=EventList)
def events_by_correlation(
    correlation_id: str,
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> EventList:
    """拿调用方追踪标识反查它关联的全部事件及当前状态。"""
    storage = request.app.state.storage
    try:
        cid = validate_correlation_id(correlation_id)
    except InvalidCorrelationId as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_correlation_id", "message": str(exc)},
        ) from exc
    assert cid is not None
    items = storage.find_events(correlation_id=cid, limit=limit, offset=offset)
    return EventList(
        items=items, count=len(items), correlation_id=cid, limit=limit, offset=offset
    )


@router.get("/correlations/{correlation_id}/history", response_model=list[HistoryItem])
def history_by_correlation(correlation_id: str, request: Request) -> list[HistoryItem]:
    """拿调用方追踪标识反查其关联事件的完整流转历史（跨事件）。"""
    storage = request.app.state.storage
    try:
        cid = validate_correlation_id(correlation_id)
    except InvalidCorrelationId as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_correlation_id", "message": str(exc)},
        ) from exc
    assert cid is not None
    return storage.history_by_correlation(cid)


@router.get("/stats", response_model=Stats)
def get_stats(request: Request) -> Stats:
    return Stats(**request.app.state.storage.stats())


@router.post("/events/{event_id}/requeue", response_model=EventView)
def requeue_event(event_id: str, request: Request) -> EventView:
    storage = request.app.state.storage
    cid = _request_cid(request) or "anonymous"
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
    cid = _request_cid(request) or "anonymous"
    if not storage.discard(event_id, cid, time.time()):
        view = storage.get_event(event_id)
        if view is None:
            raise HTTPException(404, detail={"error": "not_found", "event_id": event_id})
        raise HTTPException(
            409,
            detail={"error": "invalid_transition", "event_id": event_id, "state": view.state.value},
        )
    return storage.get_event(event_id)  # type: ignore[return-value]


def _batch_dispose(action: str, body: BatchDisposeRequest, request: Request) -> BatchDisposeResult:
    storage = request.app.state.storage
    actor = _request_cid(request) or "anonymous"
    if not body.event_ids and not body.states and not body.correlation_id:
        raise HTTPException(
            400,
            detail={
                "error": "empty_selector",
                "message": "provide event_ids and/or states and/or correlation_id",
            },
        )
    # 只允许处置 DEAD/FAILED：即使调用方传了其他状态，也在入口显式拒绝，
    # 避免“静默跳过”让人误以为已正常完成的事件会被卷入。
    if body.states and any(
        s not in (EventState.DEAD, EventState.FAILED) for s in body.states
    ):
        raise HTTPException(
            400,
            detail={
                "error": "invalid_batch_states",
                "message": "batch disposal only allows DEAD and FAILED states",
                "allowed": ["DEAD", "FAILED"],
            },
        )
    try:
        cid = validate_correlation_id(body.correlation_id)
        result = storage.batch_dispose(
            action=action,
            idempotency_key=body.idempotency_key,
            event_ids=body.event_ids,
            states=[s.value for s in body.states] if body.states else None,
            correlation_id=cid,
            reason=body.reason,
            actor=actor,
            now=time.time(),
        )
    except BatchIdempotencyConflict as exc:
        raise HTTPException(
            409,
            detail={
                "error": "idempotency_key_conflict",
                "idempotency_key": exc.key,
                "existing_action": exc.existing_action,
                "requested_action": exc.requested_action,
            },
        ) from exc
    except InvalidCorrelationId as exc:
        raise HTTPException(
            400,
            detail={"error": "invalid_correlation_id", "message": str(exc)},
        ) from exc
    return BatchDisposeResult(**result)


@router.post("/events/batch-requeue", response_model=BatchDisposeResult)
def batch_requeue(body: BatchDisposeRequest, request: Request) -> BatchDisposeResult:
    """批量重放（仅 DEAD/FAILED；幂等；每个事件留痕，已完成事件不卷入）。"""
    return _batch_dispose("requeue", body, request)


@router.post("/events/batch-discard", response_model=BatchDisposeResult)
def batch_discard(body: BatchDisposeRequest, request: Request) -> BatchDisposeResult:
    """批量丢弃（仅 DEAD/FAILED；幂等；每个事件留痕，已完成事件不卷入）。"""
    return _batch_dispose("discard", body, request)
