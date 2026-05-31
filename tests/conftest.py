# PROMPT: Generate a shared pytest conftest.py for an async FastAPI app using SQLAlchemy async
# with aiosqlite (in-memory SQLite). Provide fixtures for: async DB session, FastAPI test client
# with DB override, pre-built event factory that generates valid EventSchema dicts for all
# 8 event types. Include helpers for seeding store data quickly.
#
# CHANGES MADE:
# - Added store_id parameter to event_factory for multi-store tests
# - Fixed AsyncSessionLocal override to use in-memory URL (AI used file-based)
# - Added explicit timezone=UTC to all datetime fixtures (AI omitted tzinfo)
# - Added conftest-level anyio_backend fixture required by pytest-asyncio
# - Patched init_db in lifespan to prevent writing production SQLite during tests


from __future__ import annotations

"""
Shared pytest fixtures for the Store Intelligence test suite.
"""


import uuid
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import Base, get_db
from app.main import app
from app.models import EventORM  # noqa: F401 (imported for table registration side-effect)

from sqlalchemy.pool import StaticPool

# ── In-memory SQLite engine (never touches disk) ─────────────────────────────
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(
    TEST_DATABASE_URL, 
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestSessionLocal = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture(scope="function")
async def db() -> AsyncGenerator[AsyncSession, None]:
    """Fresh in-memory database for each test function."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("DELETE FROM events"))

    async with TestSessionLocal() as session:
        yield session
        await session.rollback()

    async with test_engine.begin() as conn:
        await conn.execute(text("DELETE FROM events"))
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(scope="function")
async def client(db: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """
    Async HTTP test client with DB dependency overridden to in-memory SQLite.
    Patches init_db so the app lifespan does NOT write a production SQLite file.
    """
    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        yield db

    app.dependency_overrides[get_db] = override_get_db

    # Patch init_db so the lifespan startup doesn't touch the production DB engine
    with patch("app.main.init_db", new_callable=AsyncMock):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as ac:
            yield ac

    app.dependency_overrides.clear()


# ── Event factory ─────────────────────────────────────────────────────────────


def make_event(
    event_type: str = "ENTRY",
    store_id: str = "ST1008",
    camera_id: str = "CAM_ENTRY_01",
    visitor_id: str | None = None,
    zone_id: str | None = None,
    is_staff: bool = False,
    confidence: float = 0.92,
    dwell_ms: int = 0,
    queue_depth: int | None = None,
    timestamp: datetime | None = None,
    session_seq: int = 1,
) -> dict:
    """
    Build a valid event dict for all 8 event types.
    Handles zone_id and queue_depth constraints automatically.
    """
    if visitor_id is None:
        visitor_id = f"VIS_{uuid.uuid4().hex[:6]}"

    if timestamp is None:
        timestamp = datetime.now(timezone.utc) - timedelta(minutes=5)

    ts_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Zone rules
    zone_events = {
        "ZONE_ENTER", "ZONE_EXIT", "ZONE_DWELL",
        "BILLING_QUEUE_JOIN", "BILLING_QUEUE_ABANDON"
    }
    no_zone_events = {"ENTRY", "EXIT", "REENTRY"}

    if event_type in no_zone_events:
        zone_id = None
    elif event_type in zone_events and zone_id is None:
        zone_id = "SKINCARE"

    if event_type == "BILLING_QUEUE_JOIN" and zone_id is None:
        zone_id = "BILLING"
    if event_type == "BILLING_QUEUE_ABANDON" and zone_id is None:
        zone_id = "BILLING"
    if event_type == "BILLING_QUEUE_JOIN" and queue_depth is None:
        queue_depth = 2

    return {
        "event_id": str(uuid.uuid4()),
        "store_id": store_id,
        "camera_id": camera_id,
        "visitor_id": visitor_id,
        "event_type": event_type,
        "timestamp": ts_str,
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": is_staff,
        "confidence": confidence,
        "metadata": {
            "queue_depth": queue_depth,
            "sku_zone": None,
            "session_seq": session_seq,
        },
    }


def make_session_events(
    visitor_id: str | None = None,
    store_id: str = "ST1008",
    base_time: datetime | None = None,
    is_staff: bool = False,
    go_to_billing: bool = True,
    abandon: bool = False,
) -> list[dict]:
    """Generate a complete session (ENTRY → ZONE events → BILLING → EXIT)."""
    if visitor_id is None:
        visitor_id = f"VIS_{uuid.uuid4().hex[:6]}"
    if base_time is None:
        base_time = datetime.now(timezone.utc) - timedelta(hours=1)

    events = [
        make_event("ENTRY", store_id=store_id, visitor_id=visitor_id,
                   timestamp=base_time, is_staff=is_staff),
        make_event("ZONE_ENTER", store_id=store_id, visitor_id=visitor_id, zone_id="SKINCARE",
                   timestamp=base_time + timedelta(minutes=1), is_staff=is_staff),
        make_event("ZONE_DWELL", store_id=store_id, visitor_id=visitor_id, zone_id="SKINCARE",
                   dwell_ms=30000, timestamp=base_time + timedelta(minutes=1, seconds=30), is_staff=is_staff),
        make_event("ZONE_EXIT", store_id=store_id, visitor_id=visitor_id, zone_id="SKINCARE",
                   dwell_ms=60000, timestamp=base_time + timedelta(minutes=2), is_staff=is_staff),
    ]

    if go_to_billing:
        events += [
            make_event("BILLING_QUEUE_JOIN", store_id=store_id, visitor_id=visitor_id, zone_id="BILLING",
                       queue_depth=2, timestamp=base_time + timedelta(minutes=3), is_staff=is_staff),
        ]
        if abandon:
            events.append(
                make_event("BILLING_QUEUE_ABANDON", store_id=store_id, visitor_id=visitor_id, zone_id="BILLING",
                           timestamp=base_time + timedelta(minutes=4), is_staff=is_staff)
            )

    events.append(
        make_event("EXIT", store_id=store_id, visitor_id=visitor_id,
                   timestamp=base_time + timedelta(minutes=10), is_staff=is_staff)
    )
    return events


@pytest.fixture
def sample_store_id() -> str:
    return "ST1008"


@pytest.fixture
def base_time() -> datetime:
    return datetime.now(timezone.utc).replace(hour=10, minute=0, second=0, microsecond=0)
