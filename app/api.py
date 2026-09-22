"""FastAPI 路由：接收、查询、历史、统计、人工处置。

响应语义（重复提交可区分、不静默、不重复执行）：
- POST /events 首次接收           -> 201, duplicate=false
- POST /events 完全相同的重复提交 -> 200, duplicate=true（不再次处理，沿用原 cid）
- POST /events 同 id 不同内容     -> 409 ContentConflict
- 终态人工处置：POST /events/{id}/requeue | /discard
"""
from __future__ import annotations

import time

from fastapi import APIRouter, HTTPException, Request, Response

from app.models import AcceptResult, EventIn, EventView, HistoryItem, Stats
from app.pipeline import ContentConflict
from app.tracing import new_correlation_id

router = APIRouter()


def _request_cid(request: Request) -> str:
    cid = request.headers.get("x-correlation-id")
    return cid or new_correlation_id()


@router.post("/events", response_model=AcceptResult, status_code=201)
def receive_event(body: EventIn, request: Request, response: Response) -> AcceptResult:
    pipeline = request.app.state.pipeline
    cid = _request_cid(request)
    try:
        result = pipeline.receive(body.event_id, body.event_type, body.payload)
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
