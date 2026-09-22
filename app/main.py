"""应用装配：存储、pipeline、worker、FastAPI 生命周期。"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import router
from app.config import Settings, get_settings
from app.downstream import DownstreamHandler
from app.pipeline import Pipeline
from app.retry import RetryPolicy
from app.storage import Storage
from app.worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s [%(filename)s:%(lineno)d] %(message)s",
)


def create_app(settings: Settings | None = None, start_worker: bool = True) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        storage = Storage(settings)
        storage.connect()
        handler = DownstreamHandler()
        retry = RetryPolicy(settings)
        pipeline = Pipeline(storage, handler, retry, settings)
        app.state.settings = settings
        app.state.storage = storage
        app.state.handler = handler
        app.state.pipeline = pipeline

        worker: Worker | None = None
        if start_worker:
            worker = Worker(pipeline, storage, settings.poll_interval)
            if settings.recovery_on_startup:
                stale = worker.recover_stale()
                if stale:
                    logging.getLogger("event_pipeline").warning(
                        "%d stale in-flight event(s) recovered on startup", stale
                    )
            worker.start()
            app.state.worker = worker
        try:
            yield
        finally:
            if worker is not None:
                worker.stop()
            storage.close()

    app = FastAPI(title="Event Delivery Pipeline", lifespan=lifespan)
    app.include_router(router)

    @app.get("/")
    def read_root():
        return {"service": "event-delivery-pipeline", "status": "ok"}

    return app


app = create_app()
