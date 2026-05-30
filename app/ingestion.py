"""
POST /events/ingest — batch event ingestion with idempotency and partial success.

Design:
- Accepts up to 500 events per batch
- Validates each event against EventSchema
- Deduplicates by event_id using INSERT OR IGNORE (SQLite) / ON CONFLICT DO NOTHING (PG)
- Returns partial success: valid events are persisted, errors are reported per-index
- Safe to call twice with same payload (idempotent)
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import insert, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import get_db
from app.main import get_logger
from app.models import (
    EventORM,
    EventSchema,
    IngestError,
    IngestRequest,
    IngestResponse,
)

router = APIRouter()
logger = get_logger(__name__)

DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./store_intelligence.db")
_IS_SQLITE = DATABASE_URL.startswith("sqlite")


def _event_to_orm(event: EventSchema) -> dict[str, Any]:
    """Convert validated EventSchema to ORM dict for bulk insert."""
    return {
        "event_id": event.event_id,
        "store_id": event.store_id,
        "camera_id": event.camera_id,
        "visitor_id": event.visitor_id,
        "event_type": event.event_type,
        "timestamp": event.timestamp,
        "zone_id": event.zone_id,
        "dwell_ms": event.dwell_ms,
        "is_staff": event.is_staff,
        "confidence": event.confidence,
        "meta_queue_depth": event.metadata.queue_depth,
        "meta_sku_zone": event.metadata.sku_zone,
        "meta_session_seq": event.metadata.session_seq,
    }


async def _upsert_events(
    db: AsyncSession, records: list[dict[str, Any]]
) -> int:
    """
    Insert events ignoring duplicates on event_id.
    Returns number of actually inserted rows.
    """
    if not records:
        return 0

    if _IS_SQLITE:
        stmt = sqlite_insert(EventORM).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["event_id"])
    else:
        stmt = pg_insert(EventORM).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["event_id"])

    result = await db.execute(stmt)
    await db.commit()
    return result.rowcount if result.rowcount is not None else len(records)


@router.post("/events/ingest", response_model=IngestResponse, status_code=200)
async def ingest_events(
    payload: IngestRequest,
    db: AsyncSession = Depends(get_db),
) -> IngestResponse:
    """
    Ingest a batch of up to 500 events.

    - Validates schema for each event
    - Deduplicates by event_id (idempotent)
    - Partial success: valid events persisted, errors reported per index
    - Never returns 5xx for malformed events — only 4xx fields in error list
    """
    raw_events: list[Any] = payload.events
    errors: list[IngestError] = []
    valid_records: list[dict[str, Any]] = []
    valid_count = 0

    for idx, event_raw in enumerate(raw_events):
        try:
            event = EventSchema.model_validate(event_raw)
            valid_records.append(_event_to_orm(event))
            valid_count += 1
        except Exception as exc:
            event_id = None
            if isinstance(event_raw, dict):
                event_id = event_raw.get("event_id")
            elif hasattr(event_raw, "event_id"):
                event_id = getattr(event_raw, "event_id")

            errors.append(
                IngestError(
                    index=idx,
                    event_id=event_id,
                    reason=str(exc),
                )
            )

    inserted = await _upsert_events(db, valid_records)

    logger.info(
        "ingest_complete",
        total_received=len(raw_events),
        accepted=valid_count,
        rejected=len(errors),
        actually_inserted=inserted,
    )

    return IngestResponse(
        accepted=valid_count,
        rejected=len(errors),
        errors=errors,
    )


async def ingest_raw_list(
    db: AsyncSession, events: list[dict[str, Any]]
) -> IngestResponse:
    """
    Internal helper: ingest a list of raw dicts without HTTP layer.
    Used by replay.py and tests.
    """
    errors: list[IngestError] = []
    valid_records: list[dict[str, Any]] = []

    for idx, raw in enumerate(events):
        try:
            schema = EventSchema.model_validate(raw)
            valid_records.append(_event_to_orm(schema))
        except Exception as exc:
            errors.append(
                IngestError(
                    index=idx,
                    event_id=raw.get("event_id"),
                    reason=str(exc),
                )
            )

    await _upsert_events(db, valid_records)

    return IngestResponse(
        accepted=len(valid_records),
        rejected=len(errors),
        errors=errors,
    )
