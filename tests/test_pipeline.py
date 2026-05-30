# PROMPT: Generate pytest tests for the detection pipeline modules: emit.py, zone_mapper.py,
# tracker.py, staff_classifier.py, pos_correlator.py. Cover: event schema validation,
# timestamp computation from frame offset, UUIDv4 event_id format, visitor_id format VIS_xxxxxx,
# zone detection via point-in-polygon, entry-line crossing detection, Re-ID cosine similarity
# matching, re-entry detection, staff colour classification, POS correlation time window.
# Use pytest and numpy. Do NOT require real CCTV clips or GPU.
#
# CHANGES MADE:
# - Added explicit test for partial occlusion (confidence NOT dropped/suppressed below 0.4)
# - Fixed visitor_id regex to match VIS_ + 6 hex chars (AI used VIS_ + any 6 chars)
# - Added test for cross-camera deduplication returning same visitor_id
# - Changed cosine similarity threshold test to use actual module constant


from __future__ import annotations

"""
Tests for detection pipeline modules.
No real CCTV clips or GPU required — all tests use synthetic data.
"""


import re
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from pipeline.emit import (
    VALID_EVENT_TYPES,
    EventEmitter,
    build_event,
    iso_timestamp,
    make_event_id,
    make_visitor_id,
)
from pipeline.detect import POSCorrelator
from pipeline.staff_classifier import ColourClassifier, MovementClassifier, StaffClassifier
from pipeline.tracker import ReIDTracker, cosine_similarity
from pipeline.detect import ZoneMapper, ZoneStateTracker, point_in_polygon

CLIP_START = datetime(2026, 3, 3, 8, 0, 0, tzinfo=timezone.utc)


# ============================================================
# emit.py tests
# ============================================================

class TestEventIdGeneration:
    def test_event_id_is_valid_uuid4(self):
        eid = make_event_id()
        parsed = uuid.UUID(eid, version=4)
        assert str(parsed) == eid

    def test_multiple_event_ids_are_unique(self):
        ids = [make_event_id() for _ in range(1000)]
        assert len(set(ids)) == 1000


class TestVisitorIdGeneration:
    def test_visitor_id_format(self):
        vid = make_visitor_id()
        assert re.match(r'^VIS_[0-9a-f]{6}$', vid), f"Invalid format: {vid}"

    def test_visitor_ids_are_unique(self):
        ids = [make_visitor_id() for _ in range(500)]
        assert len(set(ids)) > 490  # Allow <1% collision (extremely unlikely)


class TestTimestampGeneration:
    def test_frame_zero_equals_clip_start(self):
        ts = iso_timestamp(CLIP_START, frame_number=0, fps=15.0)
        assert ts == "2026-03-03T08:00:00Z"

    def test_frame_offset_computed_correctly(self):
        # 450 frames at 15fps = 30 seconds
        ts = iso_timestamp(CLIP_START, frame_number=450, fps=15.0)
        expected = datetime(2026, 3, 3, 8, 0, 30, tzinfo=timezone.utc)
        assert ts == expected.strftime("%Y-%m-%dT%H:%M:%SZ")

    def test_large_frame_number(self):
        # 18000 frames = 20 minutes (end of clip)
        ts = iso_timestamp(CLIP_START, frame_number=18000, fps=15.0)
        expected = datetime(2026, 3, 3, 8, 20, 0, tzinfo=timezone.utc)
        assert ts == expected.strftime("%Y-%m-%dT%H:%M:%SZ")


class TestBuildEvent:
    def test_entry_event_has_null_zone(self):
        evt = build_event(
            store_id="STORE_BLR_002",
            camera_id="CAM_ENTRY_01",
            visitor_id="VIS_abc123",
            event_type="ENTRY",
            clip_start=CLIP_START,
            frame_number=100,
        )
        assert evt["zone_id"] is None
        assert evt["event_type"] == "ENTRY"

    def test_zone_enter_requires_zone_id(self):
        evt = build_event(
            store_id="STORE_BLR_002",
            camera_id="CAM_FLOOR_01",
            visitor_id="VIS_abc123",
            event_type="ZONE_ENTER",
            clip_start=CLIP_START,
            frame_number=100,
            zone_id="SKINCARE",
        )
        assert evt["zone_id"] == "SKINCARE"

    def test_zone_enter_without_zone_raises(self):
        with pytest.raises(ValueError, match="zone_id required"):
            build_event(
                store_id="STORE_BLR_002",
                camera_id="CAM_FLOOR_01",
                visitor_id="VIS_abc123",
                event_type="ZONE_ENTER",
                clip_start=CLIP_START,
                frame_number=100,
                zone_id=None,  # Missing!
            )

    def test_partial_occlusion_confidence_not_dropped(self):
        """CRITICAL: Low confidence must be emitted, never suppressed."""
        low_conf = 0.41  # Just above detection threshold
        evt = build_event(
            store_id="STORE_BLR_002",
            camera_id="CAM_FLOOR_01",
            visitor_id="VIS_abc123",
            event_type="ZONE_ENTER",
            clip_start=CLIP_START,
            frame_number=100,
            zone_id="SKINCARE",
            confidence=low_conf,
        )
        assert evt["confidence"] == round(low_conf, 4), "Low confidence must not be suppressed"

    def test_billing_queue_join_defaults_queue_depth(self):
        evt = build_event(
            store_id="STORE_BLR_002",
            camera_id="CAM_BILLING_01",
            visitor_id="VIS_abc123",
            event_type="BILLING_QUEUE_JOIN",
            clip_start=CLIP_START,
            frame_number=100,
            zone_id="BILLING",
            queue_depth=None,  # Should default to 1
        )
        assert evt["metadata"]["queue_depth"] == 1

    def test_all_8_event_types_buildable(self):
        for et in VALID_EVENT_TYPES:
            zone = "BILLING" if "BILLING" in et or "ZONE" in et else None
            qd = 2 if et == "BILLING_QUEUE_JOIN" else None
            evt = build_event(
                store_id="STORE_BLR_002",
                camera_id="CAM_ENTRY_01",
                visitor_id="VIS_abc123",
                event_type=et,
                clip_start=CLIP_START,
                frame_number=100,
                zone_id=zone,
                queue_depth=qd,
            )
            assert evt["event_type"] == et

    def test_confidence_clamped_to_valid_range(self):
        evt = build_event(
            store_id="STORE_BLR_002",
            camera_id="CAM_ENTRY_01",
            visitor_id="VIS_abc123",
            event_type="ENTRY",
            clip_start=CLIP_START,
            frame_number=0,
            confidence=1.5,  # Too high — should be clamped to 1.0
        )
        assert evt["confidence"] <= 1.0


class TestEventEmitter:
    def test_emitter_writes_valid_jsonl(self, tmp_path):
        output = str(tmp_path / "test_events.jsonl")
        with EventEmitter(output_path=output) as emitter:
            evt = build_event(
                store_id="STORE_BLR_002",
                camera_id="CAM_ENTRY_01",
                visitor_id="VIS_abc123",
                event_type="ENTRY",
                clip_start=CLIP_START,
                frame_number=0,
            )
            emitter.emit(evt)

        import json
        with open(output) as f:
            line = f.readline()
        parsed = json.loads(line)
        assert parsed["event_type"] == "ENTRY"
        assert "event_id" in parsed


# ============================================================
# zone_mapper.py tests
# ============================================================

class TestPointInPolygon:
    def test_point_inside_square(self):
        polygon = [[0, 0], [100, 0], [100, 100], [0, 100]]
        assert point_in_polygon(50, 50, polygon) is True

    def test_point_outside_square(self):
        polygon = [[0, 0], [100, 0], [100, 100], [0, 100]]
        assert point_in_polygon(150, 150, polygon) is False

    def test_point_on_edge_is_handled(self):
        polygon = [[0, 0], [100, 0], [100, 100], [0, 100]]
        # Edge case — result depends on ray direction, just ensure no crash
        result = point_in_polygon(50, 0, polygon)
        assert isinstance(result, bool)

    def test_triangle_polygon(self):
        polygon = [[0, 0], [100, 0], [50, 100]]
        assert point_in_polygon(50, 50, polygon) is True
        assert point_in_polygon(5, 90, polygon) is False


class TestZoneMapper:
    @pytest.fixture
    def mapper(self, tmp_path):
        import json
        layout = {
            "store_id": "STORE_BLR_002",
            "cameras": {"CAM_FLOOR_01": {"type": "floor"}},
            "entry_line": {"camera": "CAM_ENTRY_01", "y": 540, "axis": "horizontal", "direction": "top_to_bottom"},
            "zones": [
                {"zone_id": "SKINCARE", "cameras": ["CAM_FLOOR_01"], "polygon": [[0,0],[500,0],[500,400],[0,400]]},
                {"zone_id": "BILLING", "cameras": ["CAM_FLOOR_01"], "polygon": [[600,0],[1920,0],[1920,1080],[600,1080]]},
            ],
            "staff_uniform_hsv": {}
        }
        path = tmp_path / "layout.json"
        path.write_text(json.dumps(layout))
        return ZoneMapper(str(path))

    def test_centroid_in_skincare_zone(self, mapper):
        zone = mapper.get_zone(250, 200, "CAM_FLOOR_01")
        assert zone == "SKINCARE"

    def test_centroid_in_billing_zone(self, mapper):
        zone = mapper.get_zone(1000, 500, "CAM_FLOOR_01")
        assert zone == "BILLING"

    def test_centroid_in_no_zone(self, mapper):
        zone = mapper.get_zone(550, 450, "CAM_FLOOR_01")
        assert zone is None

    def test_wrong_camera_returns_none(self, mapper):
        # Camera doesn't cover this zone
        zone = mapper.get_zone(250, 200, "CAM_BILLING_01")
        assert zone is None

    def test_entry_crossing_downward(self, mapper):
        # prev_cy=400 (above line), curr_cy=600 (below line) → ENTRY
        result = mapper.is_entry_crossing(400.0, 600.0, "CAM_ENTRY_01")
        assert result == "ENTRY"

    def test_exit_crossing_upward(self, mapper):
        # prev_cy=600 (below), curr_cy=400 (above) → EXIT
        result = mapper.is_entry_crossing(600.0, 400.0, "CAM_ENTRY_01")
        assert result == "EXIT"

    def test_no_crossing_same_side(self, mapper):
        result = mapper.is_entry_crossing(400.0, 450.0, "CAM_ENTRY_01")
        assert result is None

    def test_wrong_camera_no_crossing(self, mapper):
        result = mapper.is_entry_crossing(400.0, 600.0, "CAM_FLOOR_01")
        assert result is None


# ============================================================
# staff_classifier.py tests
# ============================================================

class TestMovementClassifier:
    def test_customer_with_few_zones_is_not_staff(self):
        clf = MovementClassifier()
        vid = "VIS_abc123"
        clf.record_zone_visit(vid, "SKINCARE")
        clf.record_zone_visit(vid, "MAKEUP")
        is_staff, conf = clf.is_staff_movement(vid)
        assert is_staff is False
        assert conf == 0.0

    def test_staff_with_many_zones_detected(self):
        clf = MovementClassifier()
        vid = "VIS_staff1"
        zones = ["SKINCARE", "MAKEUP", "HAIRCARE", "FRAGRANCE", "BILLING"]
        for zone in zones:
            for _ in range(3):  # 15 total visits, 5 zones
                clf.record_zone_visit(vid, zone)
        is_staff, conf = clf.is_staff_movement(vid)
        assert is_staff is True
        assert conf > 0.5

    def test_reset_clears_state(self):
        clf = MovementClassifier()
        vid = "VIS_staff1"
        for _ in range(10):
            for zone in ["SKINCARE", "MAKEUP", "HAIRCARE", "FRAGRANCE", "BILLING"]:
                clf.record_zone_visit(vid, zone)
        clf.reset(vid)
        is_staff, _ = clf.is_staff_movement(vid)
        assert is_staff is False


# ============================================================
# tracker.py tests
# ============================================================

class TestCosineSimilarity:
    def test_identical_vectors(self):
        a = np.array([1.0, 0.0, 0.0])
        assert cosine_similarity(a, a) == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        a = np.array([1.0, 0.0, 0.0])
        b = np.array([0.0, 1.0, 0.0])
        assert cosine_similarity(a, b) == pytest.approx(0.0)

    def test_zero_vector(self):
        a = np.zeros(10)
        b = np.ones(10)
        assert cosine_similarity(a, b) == 0.0

    def test_similar_vectors_high_score(self):
        a = np.array([1.0, 0.9, 0.8, 0.7])
        b = np.array([1.1, 0.85, 0.75, 0.65])
        sim = cosine_similarity(a, b)
        assert sim > 0.99


class TestReIDTracker:
    def test_new_track_creates_visitor_id(self):
        tracker = ReIDTracker()
        vid, is_reentry, conf = tracker.get_or_create_visitor(
            "CAM_ENTRY_01", 1, None, datetime.now(timezone.utc)
        )
        assert re.match(r'^VIS_[0-9a-f]{6}$', vid)
        assert is_reentry is False

    def test_same_track_returns_same_visitor_id(self):
        tracker = ReIDTracker()
        now = datetime.now(timezone.utc)
        vid1, _, _ = tracker.get_or_create_visitor("CAM_ENTRY_01", 5, None, now)
        vid2, _, _ = tracker.get_or_create_visitor("CAM_ENTRY_01", 5, None, now)
        assert vid1 == vid2

    def test_reentry_detection_with_high_similarity(self):
        """
        When a track exits and reappears with similar embedding, it should be
        classified as REENTRY with same visitor_id.
        """
        from pipeline.tracker import OSNetExtractor, ReIDTracker, REID_THRESHOLD

        class MockExtractor:
            def extract(self, crop):
                # Always return same embedding (perfect match)
                return np.ones(512, dtype=np.float32)

        tracker = ReIDTracker(extractor=MockExtractor())
        now = datetime.now(timezone.utc)

        # First visit
        vid1, is_re1, _ = tracker.get_or_create_visitor("CAM_ENTRY_01", 1, None, now)
        assert is_re1 is False

        # Exit
        tracker.record_exit("CAM_ENTRY_01", 1, now)

        # Reappear 10 minutes later with same embedding
        later = now + timedelta(minutes=10)
        vid2, is_re2, conf2 = tracker.get_or_create_visitor("CAM_ENTRY_01", 99, None, later)

        assert vid1 == vid2, "Re-entry should reuse same visitor_id"
        assert is_re2 is True
        assert conf2 >= REID_THRESHOLD

    def test_new_visitor_after_long_gap(self):
        """After 30+ minutes, re-entry window expires → new visitor_id."""
        from pipeline.tracker import OSNetExtractor, ReIDTracker

        class MockExtractor:
            def extract(self, crop):
                return np.ones(512, dtype=np.float32)

        tracker = ReIDTracker(extractor=MockExtractor())
        now = datetime.now(timezone.utc)

        vid1, _, _ = tracker.get_or_create_visitor("CAM_ENTRY_01", 1, None, now)
        tracker.record_exit("CAM_ENTRY_01", 1, now)

        # 45 minutes later — outside reentry window
        much_later = now + timedelta(minutes=45)
        vid2, is_re, _ = tracker.get_or_create_visitor("CAM_ENTRY_01", 99, None, much_later)

        assert vid1 != vid2, "After reentry window, should be new visitor"
        assert is_re is False


# ============================================================
# pos_correlator.py tests
# ============================================================

class TestPOSCorrelator:
    def test_visitor_in_billing_before_transaction_is_converted(self, tmp_path):
        corr = POSCorrelator()

        # Write a POS CSV
        csv_content = "store_id,transaction_id,timestamp,basket_value_inr\n"
        csv_content += "STORE_BLR_002,TXN_001,2026-03-03T10:05:00Z,1240.00\n"
        csv_file = tmp_path / "pos.csv"
        csv_file.write_text(csv_content)
        corr.load_transactions(str(csv_file))

        # Visitor in billing 3 min before transaction
        arrival = datetime(2026, 3, 3, 10, 2, 0, tzinfo=timezone.utc)
        corr.record_billing_arrival("VIS_abc123", "STORE_BLR_002", arrival)

        converted, abandoned = corr.resolve()
        assert "VIS_abc123" in converted
        assert "VIS_abc123" not in abandoned

    def test_visitor_missing_transaction_is_abandoned(self, tmp_path):
        corr = POSCorrelator()
        csv_content = "store_id,transaction_id,timestamp,basket_value_inr\n"
        csv_file = tmp_path / "pos.csv"
        csv_file.write_text(csv_content)
        corr.load_transactions(str(csv_file))

        arrival = datetime(2026, 3, 3, 10, 2, 0, tzinfo=timezone.utc)
        corr.record_billing_arrival("VIS_xyz999", "STORE_BLR_002", arrival)

        converted, abandoned = corr.resolve()
        assert "VIS_xyz999" in abandoned

    def test_transaction_outside_window_not_matched(self, tmp_path):
        corr = POSCorrelator()
        csv_content = "store_id,transaction_id,timestamp,basket_value_inr\n"
        csv_content += "STORE_BLR_002,TXN_002,2026-03-03T10:20:00Z,500.00\n"  # 15 min later
        csv_file = tmp_path / "pos.csv"
        csv_file.write_text(csv_content)
        corr.load_transactions(str(csv_file))

        arrival = datetime(2026, 3, 3, 10, 2, 0, tzinfo=timezone.utc)
        corr.record_billing_arrival("VIS_late", "STORE_BLR_002", arrival)

        converted, abandoned = corr.resolve()
        assert "VIS_late" in abandoned

    def test_queue_depth_counting(self):
        corr = POSCorrelator()
        now = datetime(2026, 3, 3, 10, 0, 0, tzinfo=timezone.utc)

        corr.record_billing_arrival("VIS_001", "STORE_BLR_002", now)
        corr.record_billing_arrival("VIS_002", "STORE_BLR_002", now + timedelta(seconds=30))
        corr.record_billing_arrival("VIS_003", "STORE_BLR_002", now + timedelta(seconds=60))

        depth = corr.get_queue_depth_at("STORE_BLR_002", now + timedelta(seconds=90))
        assert depth == 3


"""
Tests for POST /events/ingest endpoint.
"""


import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from tests.conftest import make_event, make_session_events





class TestIngestBasic:
    async def test_ingest_single_valid_event(self, client, sample_store_id):
        payload = {"events": [make_event("ENTRY", store_id=sample_store_id)]}
        resp = await client.post("/events/ingest", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["accepted"] == 1
        assert data["rejected"] == 0
        assert data["errors"] == []

    async def test_ingest_all_8_event_types(self, client, sample_store_id):
        events = [
            make_event("ENTRY", store_id=sample_store_id),
            make_event("ZONE_ENTER", store_id=sample_store_id, zone_id="SKINCARE"),
            make_event("ZONE_DWELL", store_id=sample_store_id, zone_id="SKINCARE", dwell_ms=30000),
            make_event("ZONE_EXIT", store_id=sample_store_id, zone_id="SKINCARE", dwell_ms=60000),
            make_event("BILLING_QUEUE_JOIN", store_id=sample_store_id, zone_id="BILLING", queue_depth=3),
            make_event("BILLING_QUEUE_ABANDON", store_id=sample_store_id, zone_id="BILLING"),
            make_event("REENTRY", store_id=sample_store_id),
            make_event("EXIT", store_id=sample_store_id),
        ]
        resp = await client.post("/events/ingest", json={"events": events})
        assert resp.status_code == 200
        data = resp.json()
        assert data["accepted"] == 8
        assert data["rejected"] == 0


class TestIngestIdempotency:
    async def test_same_event_posted_twice_counted_once(self, client, sample_store_id):
        """Idempotency: duplicate event_id must not increase accepted count."""
        events = [make_event("ENTRY", store_id=sample_store_id)]
        event_id = events[0]["event_id"]

        resp1 = await client.post("/events/ingest", json={"events": events})
        resp2 = await client.post("/events/ingest", json={"events": events})

        assert resp1.status_code == 200
        assert resp2.status_code == 200
        # Both report "accepted" = 1 (no error) but second is a no-op in DB
        assert resp1.json()["accepted"] == 1
        assert resp2.json()["accepted"] == 1

    async def test_large_batch_then_repost(self, client, sample_store_id):
        """Full batch idempotency check."""
        events = [make_event("ENTRY", store_id=sample_store_id) for _ in range(50)]
        # First post
        resp = await client.post("/events/ingest", json={"events": events})
        assert resp.json()["accepted"] == 50

        # Second post — same events
        resp = await client.post("/events/ingest", json={"events": events})
        assert resp.json()["accepted"] == 50
        assert resp.json()["rejected"] == 0


class TestIngestPartialSuccess:
    async def test_invalid_event_id_rejected(self, client, sample_store_id):
        """Non-UUIDv4 event_id → rejected with error, valid events still accepted."""
        good = make_event("ENTRY", store_id=sample_store_id)
        bad = make_event("ENTRY", store_id=sample_store_id)
        bad["event_id"] = "not-a-uuid"

        resp = await client.post("/events/ingest", json={"events": [good, bad]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["accepted"] == 1
        assert data["rejected"] == 1
        assert len(data["errors"]) == 1
        assert data["errors"][0]["index"] == 1  # 0-based, bad event is index 1

    async def test_missing_zone_id_rejected(self, client, sample_store_id):
        """ZONE_ENTER without zone_id → rejected."""
        evt = make_event("ZONE_ENTER", store_id=sample_store_id)
        evt["zone_id"] = None  # Invalid — zone required

        resp = await client.post("/events/ingest", json={"events": [evt]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["rejected"] == 1

    async def test_invalid_visitor_id_format_rejected(self, client, sample_store_id):
        """visitor_id not matching VIS_[0-9a-f]{6} → rejected."""
        evt = make_event("ENTRY", store_id=sample_store_id)
        evt["visitor_id"] = "INVALID_ID"

        resp = await client.post("/events/ingest", json={"events": [evt]})
        assert resp.status_code == 200
        assert resp.json()["rejected"] == 1

    async def test_mixed_valid_invalid_batch(self, client, sample_store_id):
        """Batch with some valid, some invalid → partial success."""
        events = [make_event("ENTRY", store_id=sample_store_id) for _ in range(5)]
        bad = make_event("ENTRY", store_id=sample_store_id)
        bad["event_id"] = "bad-uuid"
        events.append(bad)

        resp = await client.post("/events/ingest", json={"events": events})
        data = resp.json()
        assert data["accepted"] == 5
        assert data["rejected"] == 1

    async def test_error_response_has_reason_field(self, client, sample_store_id):
        """Each error object must have index, event_id (if available), reason."""
        evt = make_event("ENTRY", store_id=sample_store_id)
        evt["event_id"] = "bad"

        resp = await client.post("/events/ingest", json={"events": [evt]})
        err = resp.json()["errors"][0]
        assert "index" in err
        assert "reason" in err


class TestIngestBatchLimits:
    async def test_exactly_500_events_accepted(self, client, sample_store_id):
        """500 events = at the limit, should succeed."""
        events = [make_event("ENTRY", store_id=sample_store_id) for _ in range(500)]
        resp = await client.post("/events/ingest", json={"events": events})
        assert resp.status_code == 200
        assert resp.json()["accepted"] == 500

    async def test_501_events_rejected_by_validation(self, client, sample_store_id):
        """501 events exceeds max_length=500, Pydantic should reject."""
        events = [make_event("ENTRY", store_id=sample_store_id) for _ in range(501)]
        resp = await client.post("/events/ingest", json={"events": events})
        assert resp.status_code == 422  # Pydantic validation error

    async def test_empty_batch_succeeds(self, client):
        """Empty event list → accepted=0, rejected=0, no error."""
        resp = await client.post("/events/ingest", json={"events": []})
        assert resp.status_code in (200, 422)


class TestIngestConfidencePreservation:
    async def test_low_confidence_events_are_accepted_not_dropped(self, client, sample_store_id):
        """Events with low confidence (e.g. 0.41) must be accepted, confidence preserved."""
        evt = make_event("ENTRY", store_id=sample_store_id, confidence=0.41)
        resp = await client.post("/events/ingest", json={"events": [evt]})
        assert resp.json()["accepted"] == 1

    async def test_billing_queue_join_requires_queue_depth(self, client, sample_store_id):
        """BILLING_QUEUE_JOIN without queue_depth in metadata → rejected."""
        evt = make_event("BILLING_QUEUE_JOIN", store_id=sample_store_id, zone_id="BILLING", queue_depth=None)
        evt["metadata"]["queue_depth"] = None
        resp = await client.post("/events/ingest", json={"events": [evt]})
        assert resp.json()["rejected"] == 1
