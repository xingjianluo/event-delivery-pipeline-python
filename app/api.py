"""HTTP API：接收、查询、死信处置、统计。

关联标识：
- 请求可携带 X-Correlation-ID（用于接收阶段），缺省由服务生成；
- 响应始终回显 X-Correlation-ID；每次后台投递尝试另有 att_* 关联标识，
  并在事件流转历史中与接收标识串联，可跨请求与后台阶段对账。
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Query, Response

from .models import EventIn, new_correlation_id
from .pipeline import EventPipeline

logger = logging.getLogger("event_pipeline.api")


def create_app(pipeline: EventPipeline | None = None) -> FastAPI:
    state: dict[str, EventPipeline] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        state["pipeline"] = pipeline or EventPipeline()
        try:
            yield
        finally:
            state["pipeline"].shutdown()

    app = FastAPI(
        title="Event Delivery Pipeline (local-only)",
        version="0.1.0",
        lifespan=lifespan,
    )

    def _pipeline() -> EventPipeline:
        return state["pipeline"]

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/events", status_code=201)
    def receive_event(event: EventIn, response: Response,
                      x_correlation_id: str | None = Header(default=None)):
        cid = x_correlation_id or new_correlation_id("recv")
        result = _pipeline().receive(
            event_id=event.event_id,
            event_type=event.type,
            payload=event.payload,
            idempotency_key=event.idempotency_key,
            correlation_id=cid,
        )
        response.headers["X-Correlation-ID"] = cid
        if result.conflict:
            # 相同标识、不同内容：明确拒绝
            response.status_code = 409
            return {
                "error": "CONTENT_CONFLICT",
                "message": result.reason,
                "event_id": result.event_id,
                "current_status": result.status.value,
                "correlation_id": cid,
                "original_correlation_id": result.original_correlation_id,
            }
        if result.duplicate:
            # 重复提交：显式反馈（200 + duplicate=true），不静默忽略、不再执行
            response.status_code = 200
        return result.model_dump()

    @app.get("/events/{event_id}")
    def get_event(event_id: str):
        detail = _pipeline().get_detail(event_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="event not found")
        return detail.model_dump()

    @app.get("/dead-letters")
    def list_dead_letters():
        return {"dead_letters": [e.model_dump() for e in _pipeline().list_dead()]}

    @app.post("/dead-letters/{event_id}/replay", status_code=202)
    def replay_dead(event_id: str, response: Response,
                    x_correlation_id: str | None = Header(default=None),
                    extra_budget: int | None = Query(default=None, ge=1, le=100)):
        cid = x_correlation_id or new_correlation_id("op")
        token = _pipeline().replay_dead(event_id, cid, extra_budget=extra_budget)
        response.headers["X-Correlation-ID"] = cid
        if token == "not_found":
            raise HTTPException(status_code=404, detail="event not found")
        if token == "not_dead":
            raise HTTPException(
                status_code=409,
                detail="event is not in DEAD state; only dead letters can be replayed",
            )
        return {
            "event_id": event_id,
            "replayed": True,
            "status": "PENDING",
            "correlation_id": cid,
        }

    @app.get("/stats")
    def stats():
        return _pipeline().get_stats().model_dump()

    return app
