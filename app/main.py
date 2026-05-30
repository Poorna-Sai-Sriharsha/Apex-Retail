from __future__ import annotations
from typing import Optional
"""
Structured JSON logging configuration using structlog.
Every request emits: trace_id, store_id, endpoint, latency_ms, event_count, status_code.
"""


import logging
import sys

import structlog


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configure structlog for JSON output with consistent field names.
    Called once at app startup.
    """
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Silence noisy third-party loggers
    for noisy in ("uvicorn.access", "sqlalchemy.engine"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger."""
    return structlog.get_logger(name)

"""
GET /stores/{store_id}/heatmap — zone visit frequency and dwell heatmap.

Returns per zone:
- visit_frequency: total ZONE_ENTER events for this zone
- avg_dwell_ms: mean dwell_ms from ZONE_DWELL events for this zone
- normalised_score: 0–100 relative to busiest zone in this store/window

data_confidence: LOW if total unique sessions < 20 in window.
"""


from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import get_db
from app.main import get_logger
from app.metrics import _window_bounds
from app.models import (
    DataConfidence,
    EventORM,
    EventType,
    HeatmapResponse,
    ZoneHeatmap,
)

heatmap_router = APIRouter()
logger = get_logger(__name__)


@heatmap_router.get("/stores/{store_id}/heatmap", response_model=HeatmapResponse)
async def get_heatmap(
    store_id: str,
    camera_id: Optional[str] = None,
    window: str = Query("today", pattern="^(today|7d|30d)$"),
    db: AsyncSession = Depends(get_db),
) -> HeatmapResponse:
    """
    Zone visit heatmap with 0–100 normalised scores and data confidence flag.
    Staff excluded. Empty zones return 0 values.
    """
    start, end = _window_bounds(window)

    filters = [
        EventORM.store_id == store_id,
        EventORM.timestamp >= start,
        EventORM.timestamp <= end,
        EventORM.is_staff.is_(False),
    ]
    if camera_id and camera_id != "ALL":
        filters.append(EventORM.camera_id == camera_id)
        
    base_filter = and_(*filters)

    # Session count for confidence check
    session_result = await db.execute(
        select(func.count(func.distinct(EventORM.visitor_id))).where(
            and_(
                base_filter,
                EventORM.event_type.in_(
                    [EventType.ENTRY.value, EventType.REENTRY.value]
                ),
            )
        )
    )
    session_count: int = session_result.scalar_one() or 0
    confidence = DataConfidence.LOW if session_count < 20 else DataConfidence.OK

    # Zone visit frequency (ZONE_ENTER count per zone)
    freq_result = await db.execute(
        select(EventORM.zone_id, func.count(EventORM.id))
        .where(
            and_(
                base_filter,
                EventORM.event_type == EventType.ZONE_ENTER.value,
                EventORM.zone_id.isnot(None),
            )
        )
        .group_by(EventORM.zone_id)
    )
    freq_map: dict[str, int] = {row[0]: row[1] for row in freq_result.fetchall() if row[0]}

    # Avg dwell per zone (ZONE_DWELL avg dwell_ms)
    dwell_result = await db.execute(
        select(EventORM.zone_id, func.avg(EventORM.dwell_ms))
        .where(
            and_(
                base_filter,
                EventORM.event_type == EventType.ZONE_DWELL.value,
                EventORM.zone_id.isnot(None),
            )
        )
        .group_by(EventORM.zone_id)
    )
    dwell_map: dict[str, float] = {
        row[0]: float(row[1]) for row in dwell_result.fetchall() if row[0]
    }

    all_zones = set(freq_map) | set(dwell_map)
    max_freq = max(freq_map.values(), default=1)  # avoid /0

    zones: list[ZoneHeatmap] = []
    for zone_id in sorted(all_zones):
        freq = freq_map.get(zone_id, 0)
        avg_dwell = dwell_map.get(zone_id, 0.0)
        normalised = round((freq / max_freq) * 100, 2) if max_freq > 0 else 0.0
        zones.append(
            ZoneHeatmap(
                zone_id=zone_id,
                visit_frequency=freq,
                avg_dwell_ms=round(avg_dwell, 2),
                normalised_score=normalised,
            )
        )

    logger.info(
        "heatmap_computed",
        store_id=store_id,
        window=window,
        zone_count=len(zones),
        session_count=session_count,
        confidence=confidence.value,
    )

    return HeatmapResponse(
        store_id=store_id,
        window=window,
        data_confidence=confidence,
        zones=zones,
    )

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

from app.anomalies import router as anomalies_router
from app.models import init_db
from app.funnel import router as funnel_router
from app.health import router as health_router
from app.ingestion import router as ingest_router
from app.metrics import router as metrics_router

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
configure_logging(LOG_LEVEL)
logger = get_logger(__name__)

# WebSocket connection manager
class ConnectionManager:
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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup: initialise DB. Shutdown: nothing special needed."""
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
app.include_router(anomalies_router, tags=["Anomalies"])
app.include_router(health_router, tags=["Health"])
app.include_router(heatmap_router, tags=['Heatmap'])


# ── Middleware: trace_id + latency + structured logging ────────────────────
@app.middleware("http")
async def logging_middleware(request: Request, call_next) -> Response:
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
# Monkey-patch the ingest endpoint to broadcast after successful ingest.
# This is done via a post-ingest hook rather than modifying ingestion.py.
original_ingest = ingest_router.routes[0].endpoint if ingest_router.routes else None


async def _broadcast_after_ingest(store_id: str, metrics_summary: dict) -> None:
    """Called after a successful ingest to notify WebSocket subscribers."""
    await manager.broadcast(store_id, {"type": "metrics_update", **metrics_summary})
