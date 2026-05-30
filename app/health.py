"""
GET /health — service health with per-store event lag monitoring.

Returns STALE_FEED for any store where last event is >STALE_FEED_MINUTES old.
This is the first endpoint on-call engineers check — must be accurate and fast.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import get_db
from app.main import get_logger
from app.models import EventORM, HealthResponse, StoreHealthStatus

router = APIRouter()
logger = get_logger(__name__)

STORE_IDS: list[str] = os.getenv(
    "STORE_IDS",
    "STORE_BLR_001,STORE_BLR_002,STORE_BLR_003,STORE_BLR_004,STORE_BLR_005",
).split(",")

STALE_FEED_MINUTES = int(os.getenv("STALE_FEED_MINUTES", "10"))


@router.get("/health", response_model=HealthResponse)
async def health_check(
    db: AsyncSession = Depends(get_db),
) -> HealthResponse:
    """
    Returns overall service status and per-store feed freshness.
    STALE_FEED is set if any store's last event is >10 min old (or no events today).
    """
    now = datetime.now(timezone.utc)
    stale_threshold = now - timedelta(minutes=STALE_FEED_MINUTES)

    store_statuses: list[StoreHealthStatus] = []
    any_stale = False

    for store_id in STORE_IDS:
        result = await db.execute(
            select(func.max(EventORM.timestamp)).where(
                EventORM.store_id == store_id
            )
        )
        last_ts: datetime | None = result.scalar_one()

        if last_ts is None:
            stale = True
            lag_minutes = None
        else:
            # Ensure timezone-aware comparison
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=timezone.utc)
            lag_minutes = round((now - last_ts).total_seconds() / 60, 1)
            stale = last_ts < stale_threshold

        if stale:
            any_stale = True

        store_statuses.append(
            StoreHealthStatus(
                store_id=store_id,
                last_event_timestamp=last_ts,
                stale_feed=stale,
                lag_minutes=lag_minutes,
            )
        )

    overall_status = "DEGRADED" if any_stale else "OK"

    logger.info(
        "health_checked",
        status=overall_status,
        stale_stores=[s.store_id for s in store_statuses if s.stale_feed],
    )

    return HealthResponse(
        status=overall_status,
        checked_at=now,
        stores=store_statuses,
    )
