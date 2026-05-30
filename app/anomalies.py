"""
GET /stores/{store_id}/anomalies — detect operational anomalies.

Anomaly Types:
  BILLING_QUEUE_SPIKE   — current queue depth > (QUEUE_SPIKE_MULTIPLIER × 7-day avg)
  CONVERSION_DROP       — today's rate < (CONVERSION_DROP_THRESHOLD × 7-day avg)
  DEAD_ZONE             — any zone with 0 ZONE_ENTER events in past DEAD_ZONE_MINUTES

Severity mapping:
  INFO     — informational, no immediate action
  WARN     — worth investigating
  CRITICAL — immediate action required

Each anomaly includes a suggested_action string and a details dict.
"""

from __future__ import annotations
from typing import Optional

import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import get_db
from app.main import get_logger
from app.metrics import _window_bounds
from app.models import (
    AnomalyDetail,
    AnomaliesResponse,
    AnomalySeverity,
    EventORM,
    EventType,
)

router = APIRouter()
logger = get_logger(__name__)

QUEUE_SPIKE_MULTIPLIER = float(os.getenv("QUEUE_SPIKE_MULTIPLIER", "2.0"))
CONVERSION_DROP_THRESHOLD = float(os.getenv("CONVERSION_DROP_THRESHOLD", "0.70"))
DEAD_ZONE_MINUTES = int(os.getenv("DEAD_ZONE_MINUTES", "30"))


async def _get_conversion_rate(
    db: AsyncSession, store_id: str, start: datetime, end: datetime
) -> float:
    """Compute conversion rate for a given time window."""
    base = and_(
        EventORM.store_id == store_id,
        EventORM.timestamp >= start,
        EventORM.timestamp <= end,
        EventORM.is_staff.is_(False),
    )
    uv = await db.execute(
        select(func.count(func.distinct(EventORM.visitor_id))).where(
            and_(base, EventORM.event_type.in_([EventType.ENTRY.value, EventType.REENTRY.value]))
        )
    )
    unique_visitors = uv.scalar_one() or 0
    if unique_visitors == 0:
        return 0.0

    join_q = await db.execute(
        select(func.count(func.distinct(EventORM.visitor_id))).where(
            and_(base, EventORM.event_type == EventType.BILLING_QUEUE_JOIN.value)
        )
    )
    abandon_q = await db.execute(
        select(func.count(func.distinct(EventORM.visitor_id))).where(
            and_(base, EventORM.event_type == EventType.BILLING_QUEUE_ABANDON.value)
        )
    )
    converted = max(0, (join_q.scalar_one() or 0) - (abandon_q.scalar_one() or 0))
    return round(converted / unique_visitors, 4)


async def _get_avg_queue_depth(
    db: AsyncSession, store_id: str, start: datetime, end: datetime
) -> float:
    """Compute average max queue depth over a window."""
    result = await db.execute(
        select(func.avg(EventORM.meta_queue_depth)).where(
            and_(
                EventORM.store_id == store_id,
                EventORM.timestamp >= start,
                EventORM.timestamp <= end,
                EventORM.event_type == EventType.BILLING_QUEUE_JOIN.value,
                EventORM.meta_queue_depth.isnot(None),
                EventORM.is_staff.is_(False),
            )
        )
    )
    val = result.scalar_one()
    return float(val) if val is not None else 0.0


@router.get("/stores/{store_id}/anomalies", response_model=AnomaliesResponse)
async def get_anomalies(
    store_id: str,
    camera_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
) -> AnomaliesResponse:
    """
    Detect store anomalies in real time. Returns list (empty if no anomalies).
    """
    now = datetime.now(timezone.utc)
    today_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    seven_days_ago = now - timedelta(days=7)

    anomalies: list[AnomalyDetail] = []

    # ── 1. BILLING_QUEUE_SPIKE ─────────────────────────────────────────────
    current_queue_result = await db.execute(
        select(func.max(EventORM.meta_queue_depth)).where(
            and_(
                EventORM.store_id == store_id,
                EventORM.timestamp >= today_start,
                EventORM.event_type == EventType.BILLING_QUEUE_JOIN.value,
                EventORM.meta_queue_depth.isnot(None),
                EventORM.is_staff.is_(False),
            )
        )
    )
    current_queue = current_queue_result.scalar_one() or 0

    avg_7d_queue = await _get_avg_queue_depth(db, store_id, seven_days_ago, today_start)

    if avg_7d_queue > 0 and current_queue > QUEUE_SPIKE_MULTIPLIER * avg_7d_queue:
        severity = AnomalySeverity.CRITICAL if current_queue > 3 * avg_7d_queue else AnomalySeverity.WARN
        anomalies.append(
            AnomalyDetail(
                anomaly_type="BILLING_QUEUE_SPIKE",
                severity=severity,
                suggested_action=(
                    f"Open additional billing counters immediately. "
                    f"Current queue depth ({current_queue}) is "
                    f"{current_queue / max(avg_7d_queue, 1):.1f}× the 7-day average."
                ),
                timestamp=now,
                details={
                    "current_queue_depth": current_queue,
                    "avg_7d_queue_depth": round(avg_7d_queue, 2),
                    "spike_multiplier": round(current_queue / max(avg_7d_queue, 1), 2),
                },
            )
        )
    elif avg_7d_queue == 0 and current_queue > 5:
        anomalies.append(
            AnomalyDetail(
                anomaly_type="BILLING_QUEUE_SPIKE",
                severity=AnomalySeverity.WARN,
                suggested_action="Queue depth elevated with no historical baseline. Monitor closely.",
                timestamp=now,
                details={"current_queue_depth": current_queue, "avg_7d_queue_depth": 0},
            )
        )

    # ── 2. CONVERSION_DROP ────────────────────────────────────────────────
    today_rate = await _get_conversion_rate(db, store_id, today_start, now)
    avg_7d_rate = await _get_conversion_rate(db, store_id, seven_days_ago, today_start)

    if avg_7d_rate > 0 and today_rate < CONVERSION_DROP_THRESHOLD * avg_7d_rate:
        drop_pct = round((1 - today_rate / avg_7d_rate) * 100, 1)
        severity = AnomalySeverity.CRITICAL if drop_pct > 50 else AnomalySeverity.WARN
        anomalies.append(
            AnomalyDetail(
                anomaly_type="CONVERSION_DROP",
                severity=severity,
                suggested_action=(
                    f"Conversion rate dropped {drop_pct}% vs 7-day avg. "
                    "Review staffing, product availability, and pricing. "
                    "Check billing queue for friction points."
                ),
                timestamp=now,
                details={
                    "today_conversion_rate": today_rate,
                    "avg_7d_conversion_rate": avg_7d_rate,
                    "drop_pct": drop_pct,
                },
            )
        )

    # ── 3. DEAD_ZONE ──────────────────────────────────────────────────────
    dead_zone_cutoff = now - timedelta(minutes=DEAD_ZONE_MINUTES)

    # Find all zones that have ever been active in this store today
    all_zones_result = await db.execute(
        select(func.distinct(EventORM.zone_id)).where(
            and_(
                EventORM.store_id == store_id,
                EventORM.timestamp >= today_start,
                EventORM.zone_id.isnot(None),
                EventORM.is_staff.is_(False),
            )
        )
    )
    all_zones = {row[0] for row in all_zones_result.fetchall() if row[0]}

    # Find zones with activity in the dead_zone_cutoff window
    active_zones_result = await db.execute(
        select(func.distinct(EventORM.zone_id)).where(
            and_(
                EventORM.store_id == store_id,
                EventORM.timestamp >= dead_zone_cutoff,
                EventORM.event_type == EventType.ZONE_ENTER.value,
                EventORM.zone_id.isnot(None),
                EventORM.is_staff.is_(False),
            )
        )
    )
    active_zones = {row[0] for row in active_zones_result.fetchall() if row[0]}

    dead_zones = all_zones - active_zones
    if dead_zones:
        for dead_zone in sorted(dead_zones):
            anomalies.append(
                AnomalyDetail(
                    anomaly_type="DEAD_ZONE",
                    severity=AnomalySeverity.INFO,
                    suggested_action=(
                        f"Zone '{dead_zone}' has had no visitors for >{DEAD_ZONE_MINUTES} min. "
                        "Consider repositioning staff, adding promotional signage, or reviewing product placement."
                    ),
                    timestamp=now,
                    details={
                        "zone_id": dead_zone,
                        "inactive_minutes": DEAD_ZONE_MINUTES,
                    },
                )
            )

    logger.info(
        "anomalies_computed",
        store_id=store_id,
        anomaly_count=len(anomalies),
    )

    return AnomaliesResponse(store_id=store_id, anomalies=anomalies)
