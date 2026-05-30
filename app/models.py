from __future__ import annotations
"""
Database engine, session factory, and base for SQLAlchemy async ORM.
Supports both SQLite (dev) and PostgreSQL (prod) via DATABASE_URL env var.
"""


import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

DATABASE_URL: str = os.getenv(
    "DATABASE_URL", "sqlite+aiosqlite:///./store_intelligence.db"
)

# Engine creation — SQLite gets special connect args for WAL mode
_connect_args: dict = {}
if DATABASE_URL.startswith("sqlite"):
    _connect_args = {"check_same_thread": False}

engine: AsyncEngine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    connect_args=_connect_args,
)

# Enable WAL mode for SQLite to allow concurrent reads during writes
if DATABASE_URL.startswith("sqlite"):

    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):  # type: ignore[misc]
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""

    pass


async def init_db() -> None:
    """Create all tables on startup."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: yields an async DB session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_db_context() -> AsyncGenerator[AsyncSession, None]:
    """Context manager variant for non-FastAPI code."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

"""
Pydantic v2 request/response schemas and SQLAlchemy ORM models.
The EventSchema exactly matches the required output schema from the spec.
"""


import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column



# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class EventType(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    ZONE_ENTER = "ZONE_ENTER"
    ZONE_EXIT = "ZONE_EXIT"
    ZONE_DWELL = "ZONE_DWELL"
    BILLING_QUEUE_JOIN = "BILLING_QUEUE_JOIN"
    BILLING_QUEUE_ABANDON = "BILLING_QUEUE_ABANDON"
    REENTRY = "REENTRY"


class AnomalySeverity(str, Enum):
    INFO = "INFO"
    WARN = "WARN"
    CRITICAL = "CRITICAL"


class DataConfidence(str, Enum):
    LOW = "LOW"
    OK = "OK"


# ---------------------------------------------------------------------------
# Pydantic Schemas — exactly matching spec
# ---------------------------------------------------------------------------


class EventMetadata(BaseModel):
    queue_depth: Optional[int] = Field(None, ge=0)
    sku_zone: Optional[str] = None
    session_seq: int = Field(0, ge=0)

    model_config = {"extra": "allow"}


class EventSchema(BaseModel):
    """
    Full event schema as per Apex Retail spec.
    All fields are required. event_id must be UUIDv4.
    confidence is never suppressed — low values are kept as-is.
    """

    event_id: str = Field(..., description="Globally unique UUIDv4")
    store_id: str = Field(..., min_length=1)
    camera_id: str = Field(..., min_length=1)
    visitor_id: str = Field(..., pattern=r"^VIS_[0-9a-f]{6}$")
    event_type: EventType
    timestamp: datetime
    zone_id: Optional[str] = None
    dwell_ms: int = Field(0, ge=0)
    is_staff: bool = False
    confidence: float = Field(..., ge=0.0, le=1.0)
    metadata: EventMetadata = Field(default_factory=EventMetadata)

    @field_validator("event_id")
    @classmethod
    def validate_uuid(cls, v: str) -> str:
        try:
            parsed = uuid.UUID(v, version=4)
            if str(parsed) != v:
                raise ValueError("event_id must be a valid UUIDv4 string")
        except (ValueError, AttributeError):
            raise ValueError(f"event_id '{v}' is not a valid UUIDv4")
        return v

    @model_validator(mode="after")
    def validate_zone_rules(self) -> "EventSchema":
        # ENTRY and EXIT must have null zone_id
        if self.event_type in (EventType.ENTRY, EventType.EXIT, EventType.REENTRY):
            if self.zone_id is not None:
                raise ValueError(
                    f"zone_id must be null for event_type={self.event_type}"
                )
        # Zone events must have a zone_id
        if self.event_type in (
            EventType.ZONE_ENTER,
            EventType.ZONE_EXIT,
            EventType.ZONE_DWELL,
            EventType.BILLING_QUEUE_JOIN,
            EventType.BILLING_QUEUE_ABANDON,
        ):
            if self.zone_id is None:
                raise ValueError(
                    f"zone_id is required for event_type={self.event_type}"
                )
        # BILLING_QUEUE_JOIN must have queue_depth
        if self.event_type == EventType.BILLING_QUEUE_JOIN:
            if self.metadata.queue_depth is None:
                raise ValueError(
                    "metadata.queue_depth is required for BILLING_QUEUE_JOIN"
                )
        return self

    model_config = {"use_enum_values": True}


# ---------------------------------------------------------------------------
# Ingest request / response
# ---------------------------------------------------------------------------


class IngestRequest(BaseModel):
    events: list[Any] = Field(..., max_length=500)


class IngestError(BaseModel):
    index: int
    event_id: Optional[str] = None
    reason: str


class IngestResponse(BaseModel):
    accepted: int
    rejected: int
    errors: list[IngestError]


# ---------------------------------------------------------------------------
# Metrics response
# ---------------------------------------------------------------------------

class CameraMetric(BaseModel):
    camera_id: str
    unique_visitors: int

class CameraMetricsResponse(BaseModel):
    store_id: str
    window: str = "today"
    cameras: list[CameraMetric]


class MetricsResponse(BaseModel):
    store_id: str
    window: str = "today"
    unique_visitors: int
    conversion_rate: float
    avg_dwell_by_zone: dict[str, float]
    queue_depth: int
    abandonment_rate: float


# ---------------------------------------------------------------------------
# Funnel response
# ---------------------------------------------------------------------------


class FunnelStage(BaseModel):
    stage: str
    count: int
    drop_off_pct: float  # 0.0 if first stage


class FunnelResponse(BaseModel):
    store_id: str
    window: str = "today"
    stages: list[FunnelStage]


# ---------------------------------------------------------------------------
# Heatmap response
# ---------------------------------------------------------------------------


class ZoneHeatmap(BaseModel):
    zone_id: str
    visit_frequency: int
    avg_dwell_ms: float
    normalised_score: float  # 0–100


class HeatmapResponse(BaseModel):
    store_id: str
    window: str = "today"
    data_confidence: DataConfidence
    zones: list[ZoneHeatmap]


# ---------------------------------------------------------------------------
# Anomaly response
# ---------------------------------------------------------------------------


class AnomalyDetail(BaseModel):
    anomaly_type: str
    severity: AnomalySeverity
    suggested_action: str
    timestamp: datetime
    details: dict[str, Any]


class AnomaliesResponse(BaseModel):
    store_id: str
    anomalies: list[AnomalyDetail]


# ---------------------------------------------------------------------------
# Health response
# ---------------------------------------------------------------------------


class StoreHealthStatus(BaseModel):
    store_id: str
    last_event_timestamp: Optional[datetime]
    stale_feed: bool
    lag_minutes: Optional[float]


class HealthResponse(BaseModel):
    status: str  # "OK" | "DEGRADED"
    checked_at: datetime
    stores: list[StoreHealthStatus]


# ---------------------------------------------------------------------------
# Error response
# ---------------------------------------------------------------------------


class ErrorResponse(BaseModel):
    error: str
    message: str
    retry_after: Optional[int] = None


# ---------------------------------------------------------------------------
# SQLAlchemy ORM Models
# ---------------------------------------------------------------------------


class EventORM(Base):
    """Persisted event record — mirrors EventSchema exactly."""

    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False)
    store_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    camera_id: Mapped[str] = mapped_column(String(50), nullable=False)
    visitor_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    zone_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    dwell_ms: Mapped[int] = mapped_column(Integer, default=0)
    is_staff: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    meta_queue_depth: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    meta_sku_zone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    meta_session_seq: Mapped[int] = mapped_column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint("event_id", name="uq_event_id"),
        Index("ix_store_timestamp", "store_id", "timestamp"),
        Index("ix_store_visitor", "store_id", "visitor_id"),
        Index("ix_store_type_timestamp", "store_id", "event_type", "timestamp"),
    )
