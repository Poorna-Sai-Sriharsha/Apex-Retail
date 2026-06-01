"""
FastAPI application entrypoint.

Features:
- Lifespan: DB init + logging setup on startup
- Middleware: trace_id injection, latency tracking, structlog request logging
- Routes: ingest, metrics, funnel, heatmap, anomalies, health
- WebSocket: /ws/stores/{store_id}/live — pushes metric updates on each ingest
- Dashboard: GET /dashboard/{store_id} — serves single-page WebSocket dashboard
- Error handlers: 503 on DB failure, never raw stack traces
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.exc import OperationalError

from app.logging_config import configure_logging, get_logger
from app.anomalies import router as anomalies_router
from app.config_api import router as config_router
from app.funnel import router as funnel_router
from app.heatmap import router as heatmap_router
from app.health import router as health_router
from app.ingestion import router as ingest_router
from app.metrics import router as metrics_router
from app.models import init_db

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
configure_logging(LOG_LEVEL)
logger = get_logger(__name__)


# ── WebSocket connection manager ───────────────────────────────────────────
class ConnectionManager:
    """
    Manages active WebSocket connections per store.
    WHY: Instead of having the dashboard poll the database every second (which 
    would overload PostgreSQL with 40+ stores), we maintain persistent WS connections.
    When a new event batch is ingested, we push the recalculated metrics directly 
    to all subscribed clients.
    """
    def __init__(self) -> None:
        # store_id → set of active WebSocket connections
        self._connections: dict[str, set[WebSocket]] = {}

    async def connect(self, store_id: str, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.setdefault(store_id, set()).add(ws)

    def disconnect(self, store_id: str, ws: WebSocket) -> None:
        self._connections.get(store_id, set()).discard(ws)

    async def broadcast(self, store_id: str, message: dict) -> None:
        dead: list[WebSocket] = []
        for ws in list(self._connections.get(store_id, set())):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(store_id, ws)


manager = ConnectionManager()


# ── Lifespan ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application startup/shutdown hook.
    WHY: By initialising the DB schema here, we guarantee the tables exist before 
    the API accepts any traffic. This prevents race conditions on container restart 
    where the API might try to serve metrics before the DB is ready.
    """
    logger.info("startup", message="Initialising database schema")
    await init_db()
    logger.info("startup", message="Store Intelligence API ready")
    yield
    logger.info("shutdown", message="Store Intelligence API shutting down")


app = FastAPI(
    title="Apex Retail — Store Intelligence API",
    version="1.0.0",
    description=(
        "End-to-end retail analytics: visitor detection, funnel analysis, "
        "zone heatmaps, anomaly detection, and real-time dashboard."
    ),
    lifespan=lifespan,
)

# CORS — allow dashboard and local clients
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ────────────────────────────────────────────────────────────────
app.include_router(ingest_router, tags=["Ingestion"])
app.include_router(metrics_router, tags=["Metrics"])
app.include_router(funnel_router, tags=["Funnel"])
app.include_router(heatmap_router, tags=["Heatmap"])
app.include_router(anomalies_router, tags=["Anomalies"])
app.include_router(health_router, tags=["Health"])
app.include_router(config_router, tags=["Config"])


# ── Middleware: trace_id + latency + structured logging ────────────────────
@app.middleware("http")
async def logging_middleware(request: Request, call_next) -> Response:
    """
    WHY: In a distributed system with high event throughput, debugging a single 
    failed request is difficult. We inject a unique `trace_id` at the edge and bind it 
    to the structlog context so that all downstream logs (including DB queries) 
    share the same ID.
    """
    trace_id = str(uuid.uuid4())
    request.state.trace_id = trace_id

    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(trace_id=trace_id)

    start_ms = time.monotonic() * 1000
    try:
        response: Response = await call_next(request)
    except Exception as exc:
        logger.error("unhandled_exception", error=str(exc), exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "INTERNAL_ERROR", "message": "An unexpected error occurred"},
        )

    latency_ms = round(time.monotonic() * 1000 - start_ms, 2)

    # Extract store_id from path if present
    path_parts = request.url.path.split("/")
    store_id = None
    if "stores" in path_parts:
        idx = path_parts.index("stores")
        if idx + 1 < len(path_parts):
            store_id = path_parts[idx + 1]

    logger.info(
        "request",
        trace_id=trace_id,
        store_id=store_id,
        endpoint=request.url.path,
        method=request.method,
        latency_ms=latency_ms,
        status_code=response.status_code,
    )

    response.headers["X-Trace-Id"] = trace_id
    return response


# ── Error handlers ─────────────────────────────────────────────────────────
@app.exception_handler(OperationalError)
async def db_error_handler(request: Request, exc: OperationalError) -> JSONResponse:
    logger.error("db_unavailable", error=str(exc))
    return JSONResponse(
        status_code=503,
        content={
            "error": "SERVICE_UNAVAILABLE",
            "message": "Database connection failed",
            "retry_after": 30,
        },
    )


@app.exception_handler(Exception)
async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("unhandled_error", error=str(exc), exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "INTERNAL_ERROR", "message": "An unexpected error occurred"},
    )


# ── WebSocket: live metric push ────────────────────────────────────────────
@app.websocket("/ws/stores/{store_id}/live")
async def websocket_live(ws: WebSocket, store_id: str) -> None:
    """
    WebSocket endpoint for real-time metric streaming.
    Clients subscribe and receive metric updates every time events are ingested.
    Also sends a heartbeat ping every 5s to keep the connection alive.
    """
    await manager.connect(store_id, ws)
    logger.info("ws_connect", store_id=store_id)
    try:
        while True:
            # Keep connection alive — clients can also send pings
            await asyncio.sleep(5)
            await ws.send_json({"type": "heartbeat", "store_id": store_id})
    except WebSocketDisconnect:
        manager.disconnect(store_id, ws)
        logger.info("ws_disconnect", store_id=store_id)


@app.get("/dashboard/{store_id}", response_class=HTMLResponse)
async def get_dashboard(store_id: str) -> HTMLResponse:
    """Serve the live Store Intelligence dashboard."""
    index_path = Path(__file__).parent / "dashboard.html"
    if index_path.exists():
        return HTMLResponse(content=index_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Dashboard missing</h1>", status_code=404)


# ── Ingest hook: broadcast to WebSocket subscribers ───────────────────────
async def _broadcast_after_ingest(store_id: str, metrics_summary: dict) -> None:
    """Called after a successful ingest to notify WebSocket subscribers."""
    await manager.broadcast(store_id, {"type": "metrics_update", **metrics_summary})
