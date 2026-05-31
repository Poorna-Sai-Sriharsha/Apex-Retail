"""
GET /stores/{store_id}/heatmap — zone visit frequency and dwell heatmap.

Returns per zone:
- visit_frequency: total ZONE_ENTER events for this zone
- avg_dwell_ms: mean dwell_ms from ZONE_DWELL events for this zone
- normalised_score: 0–100 relative to busiest zone in this store/window

data_confidence: LOW if total unique sessions < 20 in window.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging_config import get_logger
from app.metrics import _window_bounds
from app.models import (
    DataConfidence,
    EventORM,
    EventType,
    HeatmapResponse,
    ZoneHeatmap,
    get_db,
)

router = APIRouter()
logger = get_logger(__name__)


@router.get("/stores/{store_id}/heatmap", response_model=HeatmapResponse)
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
    max_ts_result = await db.execute(
        select(func.max(EventORM.timestamp)).where(EventORM.store_id == store_id)
    )
    max_ts = max_ts_result.scalar_one_or_none()
    now = max_ts if max_ts else datetime.now(timezone.utc)

    start, end = _window_bounds(window, now)

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
    freq_map: dict[str, int] = {
        row[0]: row[1] for row in freq_result.fetchall() if row[0]
    }

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
