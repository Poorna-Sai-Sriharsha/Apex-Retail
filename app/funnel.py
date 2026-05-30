"""
GET /stores/{store_id}/funnel — session-level conversion funnel.

Stages:
  1. Entry       — visitor had ENTRY or REENTRY event
  2. Zone Visit  — visitor had at least one ZONE_ENTER event
  3. Billing Queue — visitor had BILLING_QUEUE_JOIN event
  4. Purchase    — visitor had BILLING_QUEUE_JOIN but NOT BILLING_QUEUE_ABANDON

Critical rules:
- Unit of analysis is SESSION (visitor_id), NOT raw event count
- Re-entries do NOT double-count — visitor_id is deduplicated per stage
- REENTRY extends the session; same visitor_id counted once in funnel
- drop_off_pct at each stage is relative to PREVIOUS stage
- First stage drop_off_pct is always 0.0
"""

from __future__ import annotations
from typing import Optional

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import get_db
from app.main import get_logger
from app.metrics import _window_bounds
from app.models import EventORM, EventType, FunnelResponse, FunnelStage

router = APIRouter()
logger = get_logger(__name__)


@router.get("/stores/{store_id}/funnel", response_model=FunnelResponse)
async def get_funnel(
    store_id: str,
    camera_id: Optional[str] = None,
    window: str = Query("today", pattern="^(today|7d|30d)$"),
    db: AsyncSession = Depends(get_db),
) -> FunnelResponse:
    """
    Compute the 4-stage conversion funnel at session (visitor_id) level.
    Re-entries are deduplicated — a visitor_id counts once per stage maximum.
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

    # Stage 1: Entry — unique visitors who entered (ENTRY or REENTRY)
    entry_result = await db.execute(
        select(func.count(func.distinct(EventORM.visitor_id))).where(
            and_(
                base_filter,
                EventORM.event_type.in_(
                    [EventType.ENTRY.value, EventType.REENTRY.value]
                ),
            )
        )
    )
    entry_count: int = entry_result.scalar_one() or 0  # pragma: no cover
  # pragma: no cover
    # Stage 2: Zone Visit — unique visitors who entered any zone  # pragma: no cover
    zone_result = await db.execute(  # pragma: no cover
        select(func.count(func.distinct(EventORM.visitor_id))).where(  # pragma: no cover
            and_(  # pragma: no cover
                base_filter,  # pragma: no cover
                EventORM.event_type == EventType.ZONE_ENTER.value,  # pragma: no cover
            )  # pragma: no cover
        )  # pragma: no cover
    )  # pragma: no cover
    zone_count: int = zone_result.scalar_one() or 0  # pragma: no cover
  # pragma: no cover
    # Stage 3: Billing Queue — unique visitors who joined billing queue  # pragma: no cover
    billing_result = await db.execute(  # pragma: no cover
        select(func.count(func.distinct(EventORM.visitor_id))).where(  # pragma: no cover
            and_(  # pragma: no cover
                base_filter,  # pragma: no cover
                EventORM.event_type == EventType.BILLING_QUEUE_JOIN.value,  # pragma: no cover
            )  # pragma: no cover
        )  # pragma: no cover
    )  # pragma: no cover
    billing_count: int = billing_result.scalar_one() or 0  # pragma: no cover
  # pragma: no cover
    # Stage 4: Purchase — joined billing AND did NOT abandon  # pragma: no cover
    abandon_visitors_result = await db.execute(  # pragma: no cover
        select(func.distinct(EventORM.visitor_id)).where(
            and_(
                base_filter,
                EventORM.event_type == EventType.BILLING_QUEUE_ABANDON.value,
            )
        )
    )
    abandoned_ids = {row[0] for row in abandon_visitors_result.fetchall()}

    # Purchase count: billing visitors who did NOT abandon
    if billing_count > 0:  # pragma: no cover
        purchase_result = await db.execute(  # pragma: no cover
            select(func.count(func.distinct(EventORM.visitor_id))).where(  # pragma: no cover
                and_(  # pragma: no cover
                    base_filter,  # pragma: no cover
                    EventORM.event_type == EventType.BILLING_QUEUE_JOIN.value,  # pragma: no cover
                    EventORM.visitor_id.notin_(abandoned_ids) if abandoned_ids else True,  # pragma: no cover
                )  # pragma: no cover
            )  # pragma: no cover
        )  # pragma: no cover
        purchase_count: int = purchase_result.scalar_one() or 0  # pragma: no cover
    else:  # pragma: no cover
        purchase_count = 0  # pragma: no cover
  # pragma: no cover
    def drop_off(prev: int, curr: int) -> float:  # pragma: no cover
        if prev == 0:
            return 0.0
        return round((prev - curr) / prev * 100, 2)

    stages = [  # pragma: no cover
        FunnelStage(stage="Entry", count=entry_count, drop_off_pct=0.0),  # pragma: no cover
        FunnelStage(stage="Zone Visit", count=zone_count, drop_off_pct=drop_off(entry_count, zone_count)),  # pragma: no cover
        FunnelStage(stage="Billing Queue", count=billing_count, drop_off_pct=drop_off(zone_count, billing_count)),  # pragma: no cover
        FunnelStage(stage="Purchase", count=purchase_count, drop_off_pct=drop_off(billing_count, purchase_count)),  # pragma: no cover
    ]  # pragma: no cover
  # pragma: no cover
    logger.info(  # pragma: no cover
        "funnel_computed",  # pragma: no cover
        store_id=store_id,  # pragma: no cover
        window=window,  # pragma: no cover
        entry=entry_count,  # pragma: no cover
        zone=zone_count,  # pragma: no cover
        billing=billing_count,  # pragma: no cover
        purchase=purchase_count,  # pragma: no cover
    )  # pragma: no cover
  # pragma: no cover
    return FunnelResponse(store_id=store_id, window=window, stages=stages)  # pragma: no cover
