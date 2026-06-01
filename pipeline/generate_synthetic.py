"""
Generate synthetic events for the Store Intelligence system.

Creates realistic visitor sessions across multiple stores with all 8 event types.
Useful for testing and demo when no real CCTV clips are available.

Usage:
    python -m pipeline.generate_synthetic --api-url http://localhost:8000
    python -m pipeline.generate_synthetic --output pipeline/output/events.jsonl --stores 5
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional


# ── Constants ──────────────────────────────────────────────────────────────

STORE_IDS = [
    "STORE_BLR_001",
    "STORE_BLR_002",
    "STORE_BLR_003",
    "STORE_BLR_004",
    "STORE_BLR_005",
]

CAMERA_IDS = [
    "CAM_ENTRY_01",
    "CAM_FLOOR_01",
    "CAM_FLOOR_02",
    "CAM_BILLING_01",
    "CAM_STOREROOM_01",
]

ZONES = ["SKINCARE", "MAKEUP", "HAIRCARE", "FRAGRANCE", "ACCESSORIES", "BILLING"]

EVENT_TYPES = [
    "ENTRY", "EXIT", "ZONE_ENTER", "ZONE_EXIT", "ZONE_DWELL",
    "BILLING_QUEUE_JOIN", "BILLING_QUEUE_ABANDON", "REENTRY",
]


def make_visitor_id() -> str:
    return f"VIS_{uuid.uuid4().hex[:6]}"


def make_event(
    store_id: str,
    camera_id: str,
    visitor_id: str,
    event_type: str,
    timestamp: datetime,
    zone_id: Optional[str] = None,
    dwell_ms: int = 0,
    is_staff: bool = False,
    confidence: float = 0.92,
    queue_depth: Optional[int] = None,
    session_seq: int = 0,
) -> dict[str, Any]:
    """Build a single event dict matching the Apex Retail schema."""
    if event_type in ("ENTRY", "EXIT", "REENTRY"):
        zone_id = None
    if event_type == "BILLING_QUEUE_JOIN" and queue_depth is None:
        queue_depth = random.randint(1, 5)

    return {
        "event_id": str(uuid.uuid4()),
        "store_id": store_id,
        "camera_id": camera_id,
        "visitor_id": visitor_id,
        "event_type": event_type,
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": is_staff,
        "confidence": round(confidence, 4),
        "metadata": {
            "queue_depth": queue_depth,
            "sku_zone": None,
            "session_seq": session_seq,
        },
    }


def generate_visitor_session(
    store_id: str,
    base_time: datetime,
    is_staff: bool = False,
    go_to_billing: bool = True,
    abandon: bool = False,
) -> list[dict[str, Any]]:
    """
    Generate a realistic visitor session with multiple events.
    WHY use probabilities for billing & abandonment? 
    To create realistic conversion funnels where customers naturally drop off. 
    This allows us to test our Anomaly Detection system's CONVERSION_DROP logic 
    under simulated stress conditions.
    """
    visitor_id = make_visitor_id()
    seq = 0
    events: list[dict[str, Any]] = []

    # Confidence varies naturally
    conf = round(random.uniform(0.75, 0.98), 4)

    # 1. ENTRY
    seq += 1
    events.append(make_event(
        store_id=store_id,
        camera_id="CAM_ENTRY_01",
        visitor_id=visitor_id,
        event_type="ENTRY",
        timestamp=base_time,
        is_staff=is_staff,
        confidence=conf,
        session_seq=seq,
    ))

    # 2. Visit 1–3 zones
    num_zones = random.randint(1, 3) if not is_staff else random.randint(3, 5)
    zones_visited = random.sample(
        [z for z in ZONES if z != "BILLING"], min(num_zones, len(ZONES) - 1)
    )

    t = base_time + timedelta(seconds=random.randint(15, 60))
    for zone in zones_visited:
        seq += 1
        cam = random.choice(["CAM_FLOOR_01", "CAM_FLOOR_02"])
        events.append(make_event(
            store_id=store_id, camera_id=cam, visitor_id=visitor_id,
            event_type="ZONE_ENTER", timestamp=t, zone_id=zone,
            is_staff=is_staff, confidence=conf, session_seq=seq,
        ))

        # Dwell in zone
        dwell_seconds = random.randint(20, 180)
        dwell_ms = dwell_seconds * 1000
        t += timedelta(seconds=dwell_seconds)
        seq += 1
        events.append(make_event(
            store_id=store_id, camera_id=cam, visitor_id=visitor_id,
            event_type="ZONE_DWELL", timestamp=t, zone_id=zone,
            dwell_ms=dwell_ms, is_staff=is_staff, confidence=conf, session_seq=seq,
        ))

        # Exit zone
        t += timedelta(seconds=random.randint(5, 30))
        seq += 1
        events.append(make_event(
            store_id=store_id, camera_id=cam, visitor_id=visitor_id,
            event_type="ZONE_EXIT", timestamp=t, zone_id=zone,
            dwell_ms=dwell_ms, is_staff=is_staff, confidence=conf, session_seq=seq,
        ))

        t += timedelta(seconds=random.randint(10, 45))

    # 3. Billing queue (optional)
    if go_to_billing and not is_staff:
        seq += 1
        queue_depth = random.randint(1, 6)
        events.append(make_event(
            store_id=store_id, camera_id="CAM_BILLING_01", visitor_id=visitor_id,
            event_type="BILLING_QUEUE_JOIN", timestamp=t, zone_id="BILLING",
            queue_depth=queue_depth, is_staff=False, confidence=conf, session_seq=seq,
        ))
        t += timedelta(seconds=random.randint(30, 180))

        if abandon:
            seq += 1
            events.append(make_event(
                store_id=store_id, camera_id="CAM_BILLING_01", visitor_id=visitor_id,
                event_type="BILLING_QUEUE_ABANDON", timestamp=t, zone_id="BILLING",
                is_staff=False, confidence=conf, session_seq=seq,
            ))
            t += timedelta(seconds=random.randint(10, 30))

    # 4. EXIT
    t += timedelta(seconds=random.randint(15, 120))
    seq += 1
    events.append(make_event(
        store_id=store_id, camera_id="CAM_ENTRY_01", visitor_id=visitor_id,
        event_type="EXIT", timestamp=t,
        is_staff=is_staff, confidence=conf, session_seq=seq,
    ))

    # 5. Optional re-entry (10% chance)
    if random.random() < 0.10 and not is_staff:
        t += timedelta(minutes=random.randint(5, 25))
        seq += 1
        events.append(make_event(
            store_id=store_id, camera_id="CAM_ENTRY_01", visitor_id=visitor_id,
            event_type="REENTRY", timestamp=t,
            is_staff=False, confidence=round(conf * 0.95, 4), session_seq=seq,
        ))

    return events


def generate_store_events(
    store_id: str,
    num_visitors: int = 40,
    num_staff: int = 3,
    base_time: Optional[datetime] = None,
) -> list[dict[str, Any]]:
    """Generate a full day of events for one store."""
    if base_time is None:
        base_time = datetime.now(timezone.utc) - timedelta(hours=2)

    all_events: list[dict[str, Any]] = []

    # Generate staff sessions
    for _ in range(num_staff):
        t = base_time + timedelta(minutes=random.randint(0, 30))
        session = generate_visitor_session(store_id, t, is_staff=True)
        all_events.extend(session)

    # Generate customer sessions
    for i in range(num_visitors):
        t = base_time + timedelta(minutes=random.randint(0, 90))
        go_billing = random.random() < 0.45  # 45% go to billing
        abandon = random.random() < 0.20 if go_billing else False  # 20% abandon
        session = generate_visitor_session(
            store_id, t,
            is_staff=False,
            go_to_billing=go_billing,
            abandon=abandon,
        )
        all_events.extend(session)

    # Sort by timestamp
    all_events.sort(key=lambda e: e["timestamp"])
    return all_events


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic store events")
    parser.add_argument("--api-url", type=str, default=None,
                        help="API URL to POST events to (e.g. http://localhost:8000)")
    parser.add_argument("--output", type=str, default="pipeline/output/events.jsonl",
                        help="Output JSONL file path")
    parser.add_argument("--stores", type=int, default=5,
                        help="Number of stores to generate (1-5)")
    parser.add_argument("--visitors-per-store", type=int, default=40,
                        help="Number of visitor sessions per store")
    args = parser.parse_args()

    stores = STORE_IDS[: min(args.stores, len(STORE_IDS))]
    all_events: list[dict[str, Any]] = []

    print(f"=== Generating synthetic events for {len(stores)} stores ===\n")

    # WHY generate new UUIDs on every run?
    # By creating fresh visitor_ids each time the script is executed, 
    # we can continuously push load into the system and test database 
    # scalability. The persistent DB naturally accumulates these visitors 
    # (e.g., 40 -> 80 -> 120 unique visitors) over multiple runs.
    for store_id in stores:
        events = generate_store_events(store_id, num_visitors=args.visitors_per_store)
        all_events.extend(events)
        print(f"  {store_id}: {len(events)} events ({args.visitors_per_store} visitors + 3 staff)")

    # Sort all events chronologically
    all_events.sort(key=lambda e: e["timestamp"])

    # Write to JSONL
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for event in all_events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    print(f"\n  Total: {len(all_events)} events -> {output_path}")

    # POST to API if URL provided
    if args.api_url:
        try:
            import httpx
        except ImportError:
            print("\n[ERROR] httpx not installed. Run: pip install httpx")
            sys.exit(1)

        print(f"\n  Ingesting to API at {args.api_url} ...")
        batch_size = 200
        total_accepted = 0
        total_rejected = 0

        for i in range(0, len(all_events), batch_size):
            batch = all_events[i : i + batch_size]
            try:
                resp = httpx.post(
                    f"{args.api_url}/events/ingest",
                    json={"events": batch},
                    timeout=30.0,
                )
                resp.raise_for_status()
                data = resp.json()
                total_accepted += data.get("accepted", 0)
                total_rejected += data.get("rejected", 0)
            except Exception as exc:
                print(f"  [WARN] Batch {i // batch_size + 1} failed: {exc}")

        print(f"  API ingest complete: {total_accepted} accepted, {total_rejected} rejected")

    print("\n=== Done ===")
    print(f"\nDashboard: http://localhost:8000/dashboard/{stores[0]}")
    print(f"Metrics:   curl http://localhost:8000/stores/{stores[0]}/metrics")


if __name__ == "__main__":
    main()
