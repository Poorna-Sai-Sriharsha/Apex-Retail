"""
Event schema definition and JSONL / HTTP emitter for the detection pipeline.

Mirrors app/models.py EventSchema but is standalone (no FastAPI dependency)
so the pipeline can run without the full API stack installed.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx


# ---------------------------------------------------------------------------
# Event Schema
# ---------------------------------------------------------------------------

VALID_EVENT_TYPES = {
    "ENTRY", "EXIT", "ZONE_ENTER", "ZONE_EXIT", "ZONE_DWELL",
    "BILLING_QUEUE_JOIN", "BILLING_QUEUE_ABANDON", "REENTRY",
}


def make_visitor_id() -> str:
    """Generate a visitor_id in format VIS_xxxxxx (6 lowercase hex chars)."""
    hex_part = uuid.uuid4().hex[:6]
    return f"VIS_{hex_part}"


def make_event_id() -> str:
    """
    Generate a globally unique UUIDv4 string.
    WHY UUIDv4? It allows distributed edge nodes (individual stores/cameras) 
    to generate globally unique event IDs statelessly, without needing to 
    coordinate with a central database sequence or worry about collisions.
    """
    return str(uuid.uuid4())


def iso_timestamp(clip_start: datetime, frame_number: int, fps: float = 15.0) -> str:
    """
    Compute ISO-8601 UTC timestamp from clip start time + frame offset.
    This gives accurate timestamps derived from actual footage position.
    """
    offset_seconds = frame_number / fps
    ts = clip_start + timedelta(seconds=offset_seconds)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_event(
    *,
    store_id: str,
    camera_id: str,
    visitor_id: str,
    event_type: str,
    clip_start: datetime,
    frame_number: int,
    fps: float = 15.0,
    zone_id: Optional[str] = None,
    dwell_ms: int = 0,
    is_staff: bool = False,
    confidence: float = 1.0,
    queue_depth: Optional[int] = None,
    sku_zone: Optional[str] = None,
    session_seq: int = 0,
) -> dict[str, Any]:
    """
    Build a fully-compliant event dict matching the Apex Retail schema.
    Confidence is NEVER suppressed — low values pass through as-is.
    """
    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(f"Invalid event_type: {event_type}")

    # Zone validation
    if event_type in ("ENTRY", "EXIT", "REENTRY") and zone_id is not None:
        zone_id = None  # Spec: null for ENTRY/EXIT/REENTRY

    if event_type in ("ZONE_ENTER", "ZONE_EXIT", "ZONE_DWELL",
                       "BILLING_QUEUE_JOIN", "BILLING_QUEUE_ABANDON"):
        if zone_id is None:
            raise ValueError(f"zone_id required for {event_type}")

    if event_type == "BILLING_QUEUE_JOIN" and queue_depth is None:
        queue_depth = 1  # Default minimum queue depth

    return {
        "event_id": make_event_id(),
        "store_id": store_id,
        "camera_id": camera_id,
        "visitor_id": visitor_id,
        "event_type": event_type,
        "timestamp": iso_timestamp(clip_start, frame_number, fps),
        "zone_id": zone_id,
        "dwell_ms": max(0, dwell_ms),
        "is_staff": is_staff,
        "confidence": round(min(1.0, max(0.0, confidence)), 4),
        "metadata": {
            "queue_depth": queue_depth,
            "sku_zone": sku_zone,
            "session_seq": session_seq,
        },
    }


# ---------------------------------------------------------------------------
# JSONL Writer
# ---------------------------------------------------------------------------

class EventEmitter:
    """
    Writes events to a JSONL file and/or POSTs to the API.
    Thread-safe for sequential use; not designed for concurrent writes.
    """

    def __init__(
        self,
        output_path: Optional[str] = None,
        api_url: Optional[str] = None,
        batch_size: int = 100,
    ) -> None:
        self.output_path = output_path
        self.api_url = api_url
        self.batch_size = batch_size
        self._batch: list[dict[str, Any]] = []
        self._file = None

        if output_path:
            self._file = open(output_path, "a", encoding="utf-8")  # noqa: WPS515

    def emit(self, event: dict[str, Any]) -> None:
        """Write event to JSONL file immediately."""
        if self._file:
            self._file.write(json.dumps(event, ensure_ascii=False) + "\n")
            self._file.flush()
        self._batch.append(event)

        if len(self._batch) >= self.batch_size and self.api_url:
            self._flush_to_api()

    def _flush_to_api(self) -> None:
        """POST accumulated batch to the API ingest endpoint."""
        if not self._batch or not self.api_url:
            return
        try:
            payload = {"events": self._batch}
            response = httpx.post(
                f"{self.api_url}/events/ingest",
                json=payload,
                timeout=30.0,
            )
            response.raise_for_status()
        except Exception as exc:
            print(f"[WARN] API ingest failed: {exc}")
        finally:
            self._batch = []

    def close(self) -> None:
        """Flush remaining batch and close file."""
        if self._batch and self.api_url:
            self._flush_to_api()
        if self._file:
            self._file.close()
            self._file = None

    def __enter__(self) -> "EventEmitter":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
