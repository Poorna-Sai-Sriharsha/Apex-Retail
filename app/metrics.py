"""
GET /stores/{store_id}/metrics — real-time store metrics.

Computes:
- unique_visitors: distinct visitor_ids with is_staff=False
- conversion_rate: converted_visitors / unique_visitors (0.0 if no visitors or purchases)
- avg_dwell_by_zone: mean dwell_ms per zone from ZONE_DWELL events
- queue_depth: current depth from latest BILLING_QUEUE_JOIN minus BILLING_QUEUE_ABANDON
- abandonment_rate: abandon events / (join + abandon) for billing queue

Window: calendar day UTC (configurable via ?window=today|7d|30d).
Staff events (is_staff=True) are excluded from all calculations.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import get_db
from app.main import get_logger
from app.models import EventORM, EventType, MetricsResponse

router = APIRouter()
logger = get_logger(__name__)


def _window_bounds(window: str) -> tuple[datetime, datetime]:
    """Return (start, end) UTC datetimes for the requested window."""
    now = datetime.now(timezone.utc)
    if window == "7d":
        start = now - timedelta(days=7)
    elif window == "30d":
        start = now - timedelta(days=30)
    else:  # today
        start = datetime.combine(now.date(), datetime.min.time(), tzinfo=timezone.utc)
    return start, now


@router.get("/stores/{store_id}/metrics", response_model=MetricsResponse)
async def get_metrics(
    store_id: str,
    camera_id: Optional[str] = None,
    window: str = Query("today", pattern="^(today|7d|30d)$"),
    db: AsyncSession = Depends(get_db),
) -> MetricsResponse:
    """
    Real-time store metrics. Staff excluded. Zero-safe (never crashes on empty data).
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

    # ── Unique visitors ───────────────────────────────────────────────────
    uv_result = await db.execute(
        select(func.count(func.distinct(EventORM.visitor_id))).where(
            and_(
                base_filter,
                EventORM.event_type.in_([EventType.ENTRY.value, EventType.REENTRY.value]),
            )
        )
    )
    unique_visitors: int = uv_result.scalar_one() or 0

    # ── Conversion: visitors who had a BILLING_QUEUE_JOIN ─────────────────
    # Proxy for "purchased" — POS correlation happens in pipeline, which marks
    # converted sessions. Here we use: visitors who joined billing queue AND
    # did NOT abandon = converted.
    join_visitors_result = await db.execute(
        select(func.count(func.distinct(EventORM.visitor_id))).where(
            and_(
                base_filter,
                EventORM.event_type == EventType.BILLING_QUEUE_JOIN.value,
            )
        )
    )
    join_visitors: int = join_visitors_result.scalar_one() or 0

    abandon_visitors_result = await db.execute(
        select(func.count(func.distinct(EventORM.visitor_id))).where(
            and_(
                base_filter,
                EventORM.event_type == EventType.BILLING_QUEUE_ABANDON.value,
            )
        )
    )
    abandon_visitors: int = abandon_visitors_result.scalar_one() or 0

    converted_visitors = max(0, join_visitors - abandon_visitors)
    conversion_rate = (
        round(converted_visitors / unique_visitors, 4) if unique_visitors > 0 else 0.0
    )

    # ── Avg dwell per zone (from ZONE_DWELL events) ───────────────────────
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
    avg_dwell_by_zone: dict[str, float] = {
        row[0]: round(float(row[1]), 2) for row in dwell_result.fetchall() if row[0]
    }

    # ── Current queue depth: max queue_depth from recent BILLING_QUEUE_JOIN ─
    queue_result = await db.execute(
        select(func.max(EventORM.meta_queue_depth)).where(
            and_(
                base_filter,
                EventORM.event_type == EventType.BILLING_QUEUE_JOIN.value,
                EventORM.meta_queue_depth.isnot(None),
            )
        )
    )
    queue_depth_raw = queue_result.scalar_one()
    queue_depth: int = int(queue_depth_raw) if queue_depth_raw is not None else 0

    # ── Abandonment rate ──────────────────────────────────────────────────
    total_billing = join_visitors
    abandonment_rate = (
        round(abandon_visitors / total_billing, 4) if total_billing > 0 else 0.0
    )

    logger.info(
        "metrics_computed",
        store_id=store_id,
        window=window,
        unique_visitors=unique_visitors,
        conversion_rate=conversion_rate,
    )

    return MetricsResponse(
        store_id=store_id,
        window=window,
        unique_visitors=unique_visitors,
        conversion_rate=conversion_rate,
        avg_dwell_by_zone=avg_dwell_by_zone,
        queue_depth=queue_depth,
        abandonment_rate=abandonment_rate,
    )

from app.models import CameraMetric, CameraMetricsResponse

@router.get("/stores/{store_id}/cameras", response_model=CameraMetricsResponse)
async def get_camera_metrics(
    store_id: str,
    window: str = Query("today", pattern="^(today|7d|30d)$"),
    db: AsyncSession = Depends(get_db),
) -> CameraMetricsResponse:
    """
    Get unique visitor counts per camera.
    """
    start, end = _window_bounds(window)

    base_filter = and_(
        EventORM.store_id == store_id,
        EventORM.timestamp >= start,
        EventORM.timestamp <= end,
        EventORM.is_staff.is_(False),
    )

    # Count distinct visitors per camera
    cam_result = await db.execute(
        select(EventORM.camera_id, func.count(func.distinct(EventORM.visitor_id)))
        .where(base_filter)
        .group_by(EventORM.camera_id)
    )
    
    cameras = [
        CameraMetric(camera_id=row[0], unique_visitors=row[1])
        for row in cam_result.fetchall()
    ]

    # Fill missing cameras from a static list so dashboard always shows them
    seen_cams = {c.camera_id for c in cameras}
    for default_cam in ["CAM_ENTRY_01", "CAM_FLOOR_01", "CAM_FLOOR_02", "CAM_STOREROOM_01", "CAM_BILLING_01"]:
        if default_cam not in seen_cams:
            cameras.append(CameraMetric(camera_id=default_cam, unique_visitors=0))

    # Sort cameras by name for consistent UI display
    cameras.sort(key=lambda x: x.camera_id)

    return CameraMetricsResponse(store_id=store_id, window=window, cameras=cameras)

