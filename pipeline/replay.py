"""
Replay stored events from a JSONL file to the live API at configurable speed.

Reads events from the JSONL file, respects original timing between events,
and replays them at the specified speed multiplier.

Usage:
    python pipeline/replay.py pipeline/output/events.jsonl --speed 10 --api http://localhost:8000
    python pipeline/replay.py pipeline/output/events.jsonl --speed 3 --api http://localhost:8000
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


def parse_timestamp(ts_str: str) -> float:
    """Parse ISO-8601 timestamp to epoch seconds."""
    # Handle both 'Z' suffix and '+00:00'
    ts_str = ts_str.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(ts_str)
    except ValueError:
        # Fallback for bare timestamps
        dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
    return dt.timestamp()


def load_events(path: Path) -> list[dict[str, Any]]:
    """Load events from a JSONL file, sorted by timestamp."""
    events: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                events.append(event)
            except json.JSONDecodeError as exc:
                print(f"  [WARN] Skipping line {line_num}: {exc}")

    # Sort by timestamp
    events.sort(key=lambda e: e.get("timestamp", ""))
    return events


def replay(events: list[dict[str, Any]], api_url: str, speed: float, batch_size: int) -> None:
    """Replay events with time-proportional delays."""
    try:
        import httpx
    except ImportError:
        print("[ERROR] httpx not installed. Run: pip install httpx")
        sys.exit(1)

    client = httpx.Client(timeout=30.0)
    total = len(events)
    total_accepted = 0
    total_rejected = 0
    batch: list[dict[str, Any]] = []
    last_ts: float | None = None

    print(f"  Replaying {total} events at {speed}× speed to {api_url}")
    print(f"  Batch size: {batch_size}")
    print()

    for idx, event in enumerate(events):
        ts = parse_timestamp(event.get("timestamp", ""))

        # Calculate proportional delay
        if last_ts is not None and speed > 0:
            gap = max(0, ts - last_ts) / speed
            if gap > 0.001:
                time.sleep(gap)
        last_ts = ts

        batch.append(event)

        # Flush batch when full or on last event
        if len(batch) >= batch_size or idx == total - 1:
            try:
                resp = client.post(
                    f"{api_url}/events/ingest",
                    json={"events": batch},
                )
                resp.raise_for_status()
                data = resp.json()
                accepted = data.get("accepted", 0)
                rejected = data.get("rejected", 0)
                total_accepted += accepted
                total_rejected += rejected

                pct = round((idx + 1) / total * 100, 1)
                store = batch[0].get("store_id", "?")
                print(
                    f"  [{pct:5.1f}%] Batch {idx // batch_size + 1}: "
                    f"{accepted} accepted, {rejected} rejected | "
                    f"Store: {store} | Event #{idx + 1}/{total}"
                )
            except Exception as exc:
                print(f"  [ERROR] Batch failed: {exc}")

            batch = []

    client.close()
    print(f"\n  Replay complete: {total_accepted} accepted, {total_rejected} rejected out of {total} events")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay stored events to the live API at configurable speed"
    )
    parser.add_argument(
        "events_file",
        type=str,
        help="Path to JSONL events file (e.g. pipeline/output/events.jsonl)",
    )
    parser.add_argument(
        "--speed", type=float, default=10.0,
        help="Replay speed multiplier (default: 10x)",
    )
    parser.add_argument(
        "--api", type=str, default="http://localhost:8000",
        help="API base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=100,
        help="Events per API batch (default: 100)",
    )
    args = parser.parse_args()

    events_path = Path(args.events_file)
    if not events_path.exists():
        print(f"[ERROR] Events file not found: {events_path}")
        print("  Run first: python -m pipeline.generate_synthetic --api-url http://localhost:8000")
        sys.exit(1)

    print(f"=== Event Replay ===")
    print(f"  File:  {events_path}")
    print(f"  Speed: {args.speed}×")
    print(f"  API:   {args.api}")
    print()

    events = load_events(events_path)
    if not events:
        print("[ERROR] No events found in file")
        sys.exit(1)

    print(f"  Loaded {len(events)} events")
    print(f"  Time range: {events[0].get('timestamp', '?')} → {events[-1].get('timestamp', '?')}")
    print()

    replay(events, args.api, args.speed, args.batch_size)
    print("\n=== Done ===")


if __name__ == "__main__":
    main()
