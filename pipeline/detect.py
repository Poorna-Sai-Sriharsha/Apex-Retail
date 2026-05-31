from __future__ import annotations
"""
POS transaction correlator.

Loads pos_transactions.csv and correlates each transaction with visitor sessions.
A visitor is "converted" if they were in the BILLING zone in the
POS_CORRELATION_WINDOW_MINUTES window before a transaction timestamp.

POS CSV schema:
  store_id, transaction_id, timestamp, basket_value_inr

Emits:
  - BILLING_QUEUE_ABANDON for visitors who were in billing zone but had no
    matching transaction within the window
"""


import csv
import os
from datetime import datetime, timedelta, timezone
from typing import Optional


POS_WINDOW_MINUTES: int = int(os.getenv("POS_CORRELATION_WINDOW_MINUTES", "5"))
BILLING_ZONE_NAMES: frozenset[str] = frozenset(
    {"BILLING", "CHECKOUT", "POS", "CASH_COUNTER", "BILLING_AREA"}
)


class Transaction:
    """Parsed POS transaction record."""

    def __init__(self, store_id: str, transaction_id: str, timestamp: datetime, basket_value: float) -> None:
        self.store_id = store_id
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.basket_value = basket_value
        self.matched_visitor_id: Optional[str] = None

    def __repr__(self) -> str:
        return f"Transaction({self.transaction_id}, {self.timestamp.isoformat()}, {self.basket_value})"


class POSCorrelator:
    """
    Correlates visitor sessions with POS transactions by time window.

    Usage:
    1. Load transactions: correlator.load_transactions(path)
    2. Record billing zone arrivals: correlator.record_billing_arrival(visitor_id, store_id, ts)
    3. At end of clip: call correlator.resolve() to get converted + abandoned sets
    """

    def __init__(self) -> None:
        self._transactions: list[Transaction] = []
        # visitor_id → (store_id, billing_entry_time)
        self._billing_arrivals: dict[str, tuple[str, datetime]] = {}
        # visitor_id → billing_exit_time (or None if still in queue)
        self._billing_exits: dict[str, Optional[datetime]] = {}

    def load_transactions(self, path: str) -> int:
        """
        Load pos_transactions.csv. Returns number of transactions loaded.
        Expected columns: store_id, transaction_id, timestamp, basket_value_inr
        """
        self._transactions = []
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ts_str = row["timestamp"].strip()
                    # Handle both with and without timezone
                    if ts_str.endswith("Z"):
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    else:
                        ts = datetime.fromisoformat(ts_str)
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)

                    self._transactions.append(
                        Transaction(
                            store_id=row["store_id"].strip(),
                            transaction_id=row["transaction_id"].strip(),
                            timestamp=ts,
                            basket_value=float(row["basket_value_inr"].strip()),
                        )
                    )
                except (KeyError, ValueError) as exc:
                    print(f"[WARN] POS parse error in row {row}: {exc}")

        return len(self._transactions)

    def record_billing_arrival(
        self, visitor_id: str, store_id: str, arrival_time: datetime
    ) -> None:
        """Record when a visitor entered the billing zone."""
        self._billing_arrivals[visitor_id] = (store_id, arrival_time)
        self._billing_exits[visitor_id] = None  # still in queue

    def record_billing_exit(self, visitor_id: str, exit_time: datetime) -> None:
        """Record when a visitor left the billing zone."""
        self._billing_exits[visitor_id] = exit_time

    def resolve(self) -> tuple[set[str], set[str]]:
        """
        Match visitors to transactions.
        Returns (converted_visitor_ids, abandoned_visitor_ids).

        A visitor is converted if there's a transaction in their store
        within POS_WINDOW_MINUTES after their billing zone arrival.
        Otherwise they abandoned.
        """
        converted: set[str] = set()
        abandoned: set[str] = set()

        for visitor_id, (store_id, arrival_time) in self._billing_arrivals.items():
            window_end = arrival_time + timedelta(minutes=POS_WINDOW_MINUTES)
            matched = False

            for txn in self._transactions:
                if txn.store_id != store_id:
                    continue
                if txn.matched_visitor_id is not None:
                    continue  # Already claimed by another visitor
                if arrival_time <= txn.timestamp <= window_end:
                    txn.matched_visitor_id = visitor_id
                    converted.add(visitor_id)
                    matched = True
                    break

            if not matched:
                abandoned.add(visitor_id)

        return converted, abandoned

    def get_queue_depth_at(
        self, store_id: str, timestamp: datetime
    ) -> int:
        """
        Estimate billing queue depth at a given timestamp.
        = visitors who arrived at billing but haven't exited yet.
        """
        count = 0
        for visitor_id, (sid, arrival) in self._billing_arrivals.items():
            if sid != store_id:
                continue
            exit_ts = self._billing_exits.get(visitor_id)
            if exit_ts is None or exit_ts > timestamp:
                if arrival <= timestamp:
                    count += 1
        return count

    def get_transactions_for_store(self, store_id: str) -> list[Transaction]:
        return [t for t in self._transactions if t.store_id == store_id]

"""
Zone mapping: load store_layout.json, map bounding-box centroids to zone polygons.

Each store has a layout JSON with zone definitions:
  {
    "store_id": "ST1008",
    "cameras": {
      "CAM_ENTRY_01": {"type": "entry", "fov_polygon": [[...], ...]},
      ...
    },
    "zones": [
      {
        "zone_id": "SKINCARE",
        "polygon": [[x1,y1],[x2,y2],[x3,y3],[x4,y4]],
        "cameras": ["CAM_FLOOR_01"]
      }
    ],
    "entry_line": {"camera": "CAM_ENTRY_01", "y": 540, "axis": "horizontal"},
    "staff_uniform_hsv": {"hue_low": 100, "hue_high": 130, "sat_low": 50, "val_low": 50}
  }

Zone classification uses point-in-polygon (ray casting algorithm).
"""


import json
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Point-in-polygon (ray casting)
# ---------------------------------------------------------------------------

def point_in_polygon(x: float, y: float, polygon: list[list[float]]) -> bool:
    """
    Ray casting algorithm for point-in-polygon test.
    polygon: list of [x, y] vertices (open polygon — first != last).
    Returns True if point (x, y) is inside the polygon.
    """
    n = len(polygon)
    inside = False
    px, py = x, y
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if ((yi > py) != (yj > py)) and (px < (xj - xi) * (py - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


# ---------------------------------------------------------------------------
# Zone Mapper
# ---------------------------------------------------------------------------

class ZoneMapper:
    """
    Maps pixel (centroid_x, centroid_y) coordinates to zone_id strings
    based on the store's layout configuration.
    """

    def __init__(self, layout_path: str | Path) -> None:
        with open(layout_path, "r", encoding="utf-8") as f:
            self.layout: dict = json.load(f)

        self.store_id: str = self.layout["store_id"]
        self.zones: list[dict] = self.layout.get("zones", [])
        self.entry_line: dict = self.layout.get("entry_line", {})
        self.cameras: dict = self.layout.get("cameras", {})
        self.staff_hsv: dict = self.layout.get("staff_uniform_hsv", {})

    def get_zone(self, cx: float, cy: float, camera_id: str) -> Optional[str]:
        """
        Given centroid coordinates in camera frame, return zone_id or None.
        Only checks zones that are covered by this camera.
        """
        for zone in self.zones:
            if camera_id not in zone.get("cameras", []):
                continue
            polygon = zone["polygon"]
            if point_in_polygon(cx, cy, polygon):
                return zone["zone_id"]
        return None

    def get_all_zones(self) -> list[str]:
        """Return list of all zone IDs in this store."""
        return [z["zone_id"] for z in self.zones]

    def is_entry_crossing(
        self,
        prev_cy: float,
        curr_cy: float,
        camera_id: str,
    ) -> Optional[str]:
        """
        Detect if a track crossed the entry line between frames.
        Returns "ENTRY" (outside→inside), "EXIT" (inside→outside), or None.

        For horizontal lines: centroid moves across y threshold.
        For vertical lines: centroid moves across x threshold.
        """
        if self.entry_line.get("camera") != camera_id:
            return None

        axis = self.entry_line.get("axis", "horizontal")
        threshold = self.entry_line.get("y" if axis == "horizontal" else "x", 540)
        direction = self.entry_line.get("direction", "top_to_bottom")
        # direction: "top_to_bottom" means moving from low y to high y = ENTRY

        if axis == "horizontal":
            if direction == "top_to_bottom":
                if prev_cy < threshold <= curr_cy:
                    return "ENTRY"
                if prev_cy >= threshold > curr_cy:
                    return "EXIT"
            else:  # bottom_to_top
                if prev_cy > threshold >= curr_cy:
                    return "ENTRY"
                if prev_cy <= threshold < curr_cy:
                    return "EXIT"
        else:  # vertical
            threshold = self.entry_line.get("x", 960)
            if direction == "left_to_right":
                if prev_cy < threshold <= curr_cy:
                    return "ENTRY"
                if prev_cy >= threshold > curr_cy:
                    return "EXIT"

        return None


# ---------------------------------------------------------------------------
# Zone State Tracker (per-track)
# ---------------------------------------------------------------------------

class ZoneStateTracker:
    """
    Tracks per-visitor zone occupancy and dwell time.
    Emits ZONE_ENTER, ZONE_EXIT, and ZONE_DWELL events at the right moments.
    """

    DWELL_INTERVAL_FRAMES: int = 30 * 15  # 30s × 15fps

    def __init__(self, zone_mapper: ZoneMapper, fps: float = 15.0) -> None:
        self.mapper = zone_mapper
        self.fps = fps
        # visitor_id → {zone_id, entry_frame, last_dwell_frame}
        self._state: dict[str, dict] = {}

    def update(
        self,
        visitor_id: str,
        cx: float,
        cy: float,
        camera_id: str,
        current_frame: int,
    ) -> list[dict]:
        """
        Update zone state for a visitor. Returns list of zone events to emit.
        Events returned are raw dicts to be passed to build_event().
        """
        events: list[dict] = []
        new_zone = self.mapper.get_zone(cx, cy, camera_id)
        state = self._state.get(visitor_id)

        if state is None:
            # New visitor entering zone tracking
            if new_zone:
                self._state[visitor_id] = {
                    "zone_id": new_zone,
                    "entry_frame": current_frame,
                    "last_dwell_frame": current_frame,
                }
                events.append({"action": "ZONE_ENTER", "zone_id": new_zone})
        else:
            current_zone = state["zone_id"]
            if new_zone != current_zone:
                # Zone changed — emit EXIT for old zone, ENTER for new zone
                dwell_ms = int((current_frame - state["entry_frame"]) / self.fps * 1000)
                if current_zone:
                    events.append({
                        "action": "ZONE_EXIT",
                        "zone_id": current_zone,
                        "dwell_ms": dwell_ms,
                    })
                if new_zone:
                    events.append({"action": "ZONE_ENTER", "zone_id": new_zone})
                    self._state[visitor_id] = {
                        "zone_id": new_zone,
                        "entry_frame": current_frame,
                        "last_dwell_frame": current_frame,
                    }
                else:
                    self._state[visitor_id]["zone_id"] = None
            else:
                # Same zone — check for ZONE_DWELL (every 30s)
                frames_since_dwell = current_frame - state["last_dwell_frame"]
                if frames_since_dwell >= self.DWELL_INTERVAL_FRAMES and current_zone:
                    dwell_ms = int(frames_since_dwell / self.fps * 1000)
                    events.append({
                        "action": "ZONE_DWELL",
                        "zone_id": current_zone,
                        "dwell_ms": dwell_ms,
                    })
                    self._state[visitor_id]["last_dwell_frame"] = current_frame

        return events

    def exit_all(self, visitor_id: str, current_frame: int) -> list[dict]:
        """
        Called when a visitor exits the store. Emit final ZONE_EXIT if in a zone.
        """
        events: list[dict] = []
        state = self._state.pop(visitor_id, None)
        if state and state.get("zone_id"):
            dwell_ms = int((current_frame - state["entry_frame"]) / self.fps * 1000)
            events.append({
                "action": "ZONE_EXIT",
                "zone_id": state["zone_id"],
                "dwell_ms": dwell_ms,
            })
        return events

    def clear(self, visitor_id: str) -> None:
        self._state.pop(visitor_id, None)

"""
Process real CCTV footage from arbitrary .mp4 files.

This script handles the case where the dataset is provided as flat .mp4 files
(e.g., CAM 1.mp4, CAM 2.mp4, ...) instead of the structured
STORE_XXX/ENTRY_camera.mp4 layout.

Usage:
  python -m pipeline.run_real --footage-dir "path/to/CCTV Footage" --api-url http://localhost:8000

Strategy:
  - Each .mp4 file is treated as a single camera for a unique store.
  - CAM 1 -> STORE_BLR_001 (CAM_ENTRY_01)
  - CAM 2 -> ST1008 (CAM_ENTRY_01)
  - ...etc.
  - Since each store has only one camera, we use CAM_ENTRY_01 as the camera ID
    so that entry/exit crossing logic works via the entry_line config.
  - Zone detection works by mapping centroids to polygon regions defined in
    the store layout JSON files.
"""


import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np

# Graceful imports
try:
    import cv2
except ImportError:
    print("[ERROR] OpenCV not found. Install: pip install opencv-python-headless")
    sys.exit(1)

try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
except ImportError:
    YOLO_AVAILABLE = False
    print("[WARN] ultralytics not found. Will use fallback person detection.")

from pipeline.emit import EventEmitter, build_event, make_visitor_id
from pipeline.tracker import ReIDTracker

from pipeline.staff_classifier import StaffClassifier



# ── Constants ──────────────────────────────────────────────────────────────
PERSON_CLASS_ID = 0
YOLO_CONF_THRESHOLD = 0.3  # Lower threshold to catch more people in retail
YOLO_MODEL = os.getenv("YOLO_MODEL", "yolov8n.pt")
FPS = 15.0
FRAME_SKIP = 2  # Process every Nth frame for speed (1 = every frame)

# Camera-to-CameraID mapping for the single store
CAM_CAMERA_MAP = {
    "CAM 1": "CAM_FLOOR_01",     # Skincare
    "CAM 2": "CAM_FLOOR_02",     # Cosmetics
    "CAM 3": "CAM_ENTRY_01",     # Entry/Exit
    "CAM 4": "CAM_STOREROOM_01", # Store Room
    "CAM 5": "CAM_BILLING_01",   # Billing
}


def get_video_info(path: str) -> dict:
    """Get basic video metadata via OpenCV."""
    cap = cv2.VideoCapture(path)
    info = {
        "width": int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
        "height": int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
        "fps": cap.get(cv2.CAP_PROP_FPS),
        "frame_count": int(cap.get(cv2.CAP_PROP_FRAME_COUNT)),
        "duration_sec": int(cap.get(cv2.CAP_PROP_FRAME_COUNT) / max(cap.get(cv2.CAP_PROP_FPS), 1)),
    }
    cap.release()
    return info


def extract_crop(frame: np.ndarray, x1: int, y1: int, x2: int, y2: int) -> Optional[np.ndarray]:
    """Safely extract bounding box crop from frame."""
    h, w = frame.shape[:2]
    x1_, y1_ = max(0, x1), max(0, y1)
    x2_, y2_ = min(w, x2), min(h, y2)
    if x2_ <= x1_ or y2_ <= y1_:
        return None
    return frame[y1_:y2_, x1_:x2_].copy()


def process_video(
    video_path: str,
    store_id: str,
    camera_id: str,
    layout_path: str,
    clip_start: datetime,
    emitter: EventEmitter,
    model: Optional[object] = None,
    max_frames: int = 0,
) -> dict:
    """
    Process a single video file through the YOLOv8 detection pipeline.
    
    Returns stats dict with frame/person/event counts.
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"[ERROR] Cannot open video: {video_path}")
        return {"frames": 0, "persons": 0, "events": 0}

    # Get actual video FPS
    actual_fps = cap.get(cv2.CAP_PROP_FPS) or FPS
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # Initialise pipeline components
    zone_mapper = ZoneMapper(layout_path)
    reid_tracker = ReIDTracker()
    staff_clf = StaffClassifier(hsv_config=zone_mapper.staff_hsv)
    zone_state = ZoneStateTracker(zone_mapper, fps=actual_fps)

    track_state: dict[int, dict] = {}
    stats = {"frames": 0, "persons": 0, "events": 0}
    frame_number = 0

    print(f"\n{'='*70}")
    print(f"[PIPELINE] Processing: {Path(video_path).name}")
    print(f"[PIPELINE] Store: {store_id} | Camera: {camera_id}")
    print(f"[PIPELINE] Resolution: {int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))}x{int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))}")
    print(f"[PIPELINE] FPS: {actual_fps:.1f} | Total frames: {total_frames}")
    print(f"[PIPELINE] Processing every {FRAME_SKIP} frame(s)")
    print(f"{'='*70}")

    t0 = time.time()
    last_progress = 0

    while True:
        if max_frames > 0 and frame_number >= max_frames:
            print(f"  [INFO] Reached max_frames ({max_frames}), stopping video.")
            break

        ret, frame = cap.read()
        if not ret:
            break

        frame_number += 1

        # Skip frames for performance
        if frame_number % FRAME_SKIP != 0:
            continue

        stats["frames"] += 1

        # ── Progress reporting ───────────────────────────────────────────
        pct = int(frame_number / max(total_frames, 1) * 100)
        if pct >= last_progress + 10:
            elapsed = time.time() - t0
            fps_actual = stats["frames"] / max(elapsed, 0.1)
            print(f"  [{pct:3d}%] frame {frame_number}/{total_frames} | "
                  f"{stats['persons']} persons | {stats['events']} events | "
                  f"{fps_actual:.1f} processed fps")
            last_progress = pct

        # ── YOLOv8 Detection ─────────────────────────────────────────────
        current_track_ids: set[int] = set()

        if model is not None:
            try:
                results = model.track(
                    frame,
                    classes=[PERSON_CLASS_ID],
                    conf=YOLO_CONF_THRESHOLD,
                    persist=True,
                    tracker="bytetrack.yaml",
                    verbose=False,
                )
            except Exception as exc:
                print(f"  [WARN] Detection error at frame {frame_number}: {exc}")
                continue
        else:
            results = []

        # ── Extract tracks and emit events ───────────────────────────────
        if model is not None and results and results[0].boxes is not None:
            boxes = results[0].boxes
            ids = boxes.id

            if ids is not None:
                ids_np = ids.cpu().numpy().astype(int)
                xyxy = boxes.xyxy.cpu().numpy()
                confs = boxes.conf.cpu().numpy()

                for i, track_id in enumerate(ids_np):
                    if i >= len(xyxy):
                        continue

                    stats["persons"] += 1
                    current_track_ids.add(int(track_id))

                    x1, y1, x2, y2 = xyxy[i].astype(int)
                    conf = float(confs[i])
                    cx = (x1 + x2) / 2
                    cy = (y1 + y2) / 2

                    # Extract crop for Re-ID
                    crop = extract_crop(frame, x1, y1, x2, y2)

                    # Cross-camera dedup (same store only)
                    existing_vid = reid_tracker.is_cross_camera_duplicate(
                        camera_id, int(track_id), crop
                    )

                    if existing_vid:
                        visitor_id = existing_vid
                        is_reentry = False
                        reid_conf = conf
                    else:
                        visitor_id, is_reentry, reid_conf = reid_tracker.get_or_create_visitor(
                            camera_id, int(track_id), crop, clip_start
                        )

                    # Staff classification
                    staff_clf.update_colour(visitor_id, crop)
                    is_staff = staff_clf.is_staff(visitor_id)

                    # ── Entry / Exit detection ───────────────────────────
                    prev = track_state.get(int(track_id))
                    event_type = None

                    if prev is None:
                        # New track — check entry crossing
                        crossing = zone_mapper.is_entry_crossing(0.0, cy, camera_id)
                        if crossing == "ENTRY" or camera_id.upper().startswith("CAM_ENTRY"):
                            event_type = "REENTRY" if is_reentry else "ENTRY"
                    else:
                        prev_cy = prev["prev_cy"]
                        crossing = zone_mapper.is_entry_crossing(prev_cy, cy, camera_id)
                        if crossing == "EXIT" and prev.get("inside", True):
                            event_type = "EXIT"
                            reid_tracker.record_exit(camera_id, int(track_id), clip_start)
                            # Final zone exit
                            for ze in zone_state.exit_all(visitor_id, frame_number):
                                seq = reid_tracker.next_seq(visitor_id)
                                evt = build_event(
                                    store_id=store_id, camera_id=camera_id,
                                    visitor_id=visitor_id, event_type=ze["action"],
                                    clip_start=clip_start, frame_number=frame_number,
                                    fps=actual_fps, zone_id=ze.get("zone_id"),
                                    dwell_ms=ze.get("dwell_ms", 0),
                                    is_staff=is_staff, confidence=conf,
                                    session_seq=seq,
                                )
                                emitter.emit(evt)
                                stats["events"] += 1
                        elif crossing == "ENTRY" and not prev.get("inside", True):
                            event_type = "REENTRY" if is_reentry else "ENTRY"

                    # Emit ENTRY / EXIT / REENTRY
                    if event_type:
                        seq = reid_tracker.next_seq(visitor_id)
                        evt = build_event(
                            store_id=store_id, camera_id=camera_id,
                            visitor_id=visitor_id, event_type=event_type,
                            clip_start=clip_start, frame_number=frame_number,
                            fps=actual_fps, zone_id=None,
                            is_staff=is_staff,
                            confidence=min(conf, reid_conf),
                            session_seq=seq,
                        )
                        emitter.emit(evt)
                        stats["events"] += 1

                    # ── Zone tracking ────────────────────────────────────
                    zone_events = zone_state.update(visitor_id, cx, cy, camera_id, frame_number)
                    for ze in zone_events:
                        action = ze["action"]
                        zone_id = ze.get("zone_id")

                        if zone_id:
                            staff_clf.update_movement(visitor_id, zone_id)
                            is_staff = staff_clf.is_staff(visitor_id)

                        # Billing zone handling
                        if zone_id and zone_id.upper() in BILLING_ZONE_NAMES:
                            if action == "ZONE_ENTER":
                                seq = reid_tracker.next_seq(visitor_id)
                                bq_evt = build_event(
                                    store_id=store_id, camera_id=camera_id,
                                    visitor_id=visitor_id, event_type="BILLING_QUEUE_JOIN",
                                    clip_start=clip_start, frame_number=frame_number,
                                    fps=actual_fps, zone_id=zone_id,
                                    is_staff=is_staff, confidence=conf,
                                    queue_depth=1, session_seq=seq,
                                )
                                emitter.emit(bq_evt)
                                stats["events"] += 1
                                continue

                        seq = reid_tracker.next_seq(visitor_id)
                        evt = build_event(
                            store_id=store_id, camera_id=camera_id,
                            visitor_id=visitor_id, event_type=action,
                            clip_start=clip_start, frame_number=frame_number,
                            fps=actual_fps, zone_id=zone_id,
                            dwell_ms=ze.get("dwell_ms", 0),
                            is_staff=is_staff, confidence=conf,
                            session_seq=seq,
                        )
                        emitter.emit(evt)
                        stats["events"] += 1

                    # Update track state
                    track_state[int(track_id)] = {
                        "prev_cy": cy,
                        "visitor_id": visitor_id,
                        "inside": True,
                    }

        # Remove stale tracks
        stale = set(track_state.keys()) - current_track_ids
        for tid in stale:
            track_state.pop(tid, None)

    cap.release()

    elapsed = time.time() - t0
    print(f"\n[DONE] {Path(video_path).name}: "
          f"{stats['frames']} frames, {stats['persons']} detections, "
          f"{stats['events']} events in {elapsed:.1f}s")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process real CCTV footage through YOLOv8 detection pipeline"
    )
    parser.add_argument(
        "--footage-dir", required=True,
        help="Directory containing .mp4 CCTV files"
    )
    parser.add_argument(
        "--layouts-dir", default="pipeline/store_layouts",
        help="Store layout JSON directory"
    )
    parser.add_argument(
        "--output-dir", default="pipeline/output",
        help="JSONL output directory"
    )
    parser.add_argument(
        "--api-url", default=None,
        help="API URL for real-time ingest (e.g. http://localhost:8000)"
    )
    parser.add_argument(
        "--frame-skip", type=int, default=2,
        help="Process every Nth frame (default: 2 for speed)"
    )
    parser.add_argument(
        "--max-frames", type=int, default=0,
        help="Max frames per video (0 = all). Useful for quick testing."
    )
    args = parser.parse_args()

    global FRAME_SKIP
    FRAME_SKIP = max(1, args.frame_skip)

    footage_dir = Path(args.footage_dir)
    if not footage_dir.exists():
        print(f"[ERROR] Footage directory not found: {footage_dir}")
        sys.exit(1)

    # Find all .mp4 files
    video_files = sorted(footage_dir.glob("*.mp4"))
    if not video_files:
        print(f"[ERROR] No .mp4 files found in {footage_dir}")
        sys.exit(1)

    print(f"\n{'#'*70}")
    print(f"#  APEX RETAIL — CCTV Detection Pipeline")
    print(f"#  Found {len(video_files)} video file(s)")
    print(f"{'#'*70}")

    # Print video info
    for vf in video_files:
        info = get_video_info(str(vf))
        print(f"  {vf.name}: {info['width']}x{info['height']} @ {info['fps']:.1f}fps, "
              f"{info['frame_count']} frames ({info['duration_sec']}s)")

    # Load YOLOv8 model once (shared across all videos)
    model = None
    if YOLO_AVAILABLE:
        print(f"\n[INFO] Loading YOLOv8 model: {YOLO_MODEL}")
        model = YOLO(YOLO_MODEL)
        print("[INFO] Model loaded successfully")
    else:
        print("[WARN] YOLOv8 not available — no detections will be produced")

    os.makedirs(args.output_dir, exist_ok=True)

    # Use current time as clip_start so events appear in "today" window
    clip_start = datetime.now(timezone.utc) - timedelta(hours=1)

    total_stats = {"frames": 0, "persons": 0, "events": 0}

    for idx, video_path in enumerate(video_files):
        stem = video_path.stem  # e.g. "CAM 1"

        # Read store_id from layout JSON (falls back to STORE_BLR_001)
        import json as _json
        _layout_for_store = Path(args.layouts_dir) / f"{CAM_CAMERA_MAP.get(stem, f'CAM_UNKNOWN_0{idx + 1}')}.json"
        store_id = "STORE_BLR_001"
        if _layout_for_store.exists():
            try:
                with open(_layout_for_store) as _lf:
                    store_id = _json.load(_lf).get("store_id", "STORE_BLR_001")
            except Exception:
                pass
        # Map video file to specific camera ID
        camera_id = CAM_CAMERA_MAP.get(stem)
        if not camera_id:
            camera_id = f"CAM_UNKNOWN_0{idx + 1}"
            print(f"[INFO] No mapping for '{stem}', using {camera_id}")

        # Find layout
        layout_path = Path(args.layouts_dir) / f"{camera_id}.json"
        if not layout_path.exists():
            print(f"[ERROR] No layout for {camera_id} at {layout_path}")
            continue

        output_path = Path(args.output_dir) / f"{camera_id}_events.jsonl"

        with EventEmitter(
            output_path=str(output_path),
            api_url=args.api_url,
            batch_size=100,
        ) as emitter:
            stats = process_video(
                video_path=str(video_path),
                store_id=store_id,
                camera_id=camera_id,
                layout_path=str(layout_path),
                clip_start=clip_start,
                emitter=emitter,
                model=model,
                max_frames=args.max_frames,
            )

        for k in total_stats:
            total_stats[k] += stats[k]

    print(f"\n{'#'*70}")
    print(f"#  PIPELINE COMPLETE")
    print(f"#  Total: {total_stats['frames']} frames, "
          f"{total_stats['persons']} detections, "
          f"{total_stats['events']} events")
    print(f"{'#'*70}\n")


if __name__ == "__main__":
    main()
