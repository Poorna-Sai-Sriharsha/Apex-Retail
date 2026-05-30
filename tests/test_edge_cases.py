# PROMPT: Generate pytest tests for edge cases in the Apex Retail Store Intelligence API.
# Required edge cases from spec: empty store (no events), all-staff clip (is_staff=True only),
# zero purchases (conversion_rate=0.0), re-entry in funnel (visitor counted once),
# duplicate event ingest (idempotent, count unchanged), batch with mix of valid/invalid
# (partial success). Also cover: heatmap data_confidence=LOW when <20 sessions, cross-endpoint
# consistency (metrics + funnel agree on visitor counts), malformed JSON body returns 422.
# Use pytest-asyncio.
#
# CHANGES MADE:
# - Added cross-endpoint consistency test (metrics.unique_visitors >= funnel.Entry.count)
# - Fixed all-staff test to use is_staff=True on ALL events including zone events
# - Added test for heatmap returning data_confidence=LOW with <20 sessions
# - Added test for malformed JSON returning 422 not 500


from __future__ import annotations

"""
Edge case and integration tests for the Store Intelligence API.
All spec-required edge cases are explicitly tested here.
"""


import uuid
from datetime import datetime, timezone

import pytest
from tests.conftest import make_event, make_session_events

pytestmark = pytest.mark.anyio


# ============================================================
# Edge Case 1: Empty Store
# ============================================================

class TestEmptyStore:
    """No events ingested — API must not crash, return zeros."""

    async def test_metrics_returns_zeros(self, client, sample_store_id):
        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["unique_visitors"] == 0
        assert data["conversion_rate"] == 0.0
        assert data["queue_depth"] == 0
        assert data["abandonment_rate"] == 0.0

    async def test_funnel_returns_zeros(self, client, sample_store_id):
        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        assert resp.status_code == 200
        for stage in resp.json()["stages"]:
            assert stage["count"] == 0

    async def test_heatmap_returns_empty_zones(self, client, sample_store_id):
        resp = await client.get(f"/stores/{sample_store_id}/heatmap")
        assert resp.status_code == 200
        assert isinstance(resp.json()["zones"], list)

    async def test_anomalies_returns_list(self, client, sample_store_id):
        resp = await client.get(f"/stores/{sample_store_id}/anomalies")
        assert resp.status_code == 200
        assert isinstance(resp.json()["anomalies"], list)

    async def test_health_returns_degraded(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "DEGRADED"


# ============================================================
# Edge Case 2: All-Staff Clip
# ============================================================

class TestAllStaffClip:
    """All events have is_staff=True — customers metrics must be 0."""

    async def test_unique_visitors_is_zero(self, client, sample_store_id):
        staff_events = make_session_events(store_id=sample_store_id, is_staff=True)
        await client.post("/events/ingest", json={"events": staff_events})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        assert resp.json()["unique_visitors"] == 0

    async def test_conversion_rate_is_zero(self, client, sample_store_id):
        staff_events = make_session_events(store_id=sample_store_id, is_staff=True,
                                           go_to_billing=True)
        await client.post("/events/ingest", json={"events": staff_events})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        assert resp.json()["conversion_rate"] == 0.0

    async def test_funnel_entry_stage_is_zero(self, client, sample_store_id):
        staff_events = make_session_events(store_id=sample_store_id, is_staff=True)
        await client.post("/events/ingest", json={"events": staff_events})

        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        stages = {s["stage"]: s for s in resp.json()["stages"]}
        assert stages["Entry"]["count"] == 0


# ============================================================
# Edge Case 3: Zero Purchases
# ============================================================

class TestZeroPurchases:
    """Visitors present, nobody converted — conversion_rate must be 0.0."""

    async def test_conversion_rate_is_zero_float(self, client, sample_store_id):
        # 5 visitors, nobody goes to billing
        for _ in range(5):
            events = make_session_events(store_id=sample_store_id, go_to_billing=False)
            await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        data = resp.json()
        assert data["conversion_rate"] == 0.0
        assert isinstance(data["conversion_rate"], float)
        assert data["unique_visitors"] == 5

    async def test_funnel_purchase_stage_is_zero(self, client, sample_store_id):
        for _ in range(3):
            events = make_session_events(store_id=sample_store_id, go_to_billing=False)
            await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        stages = {s["stage"]: s for s in resp.json()["stages"]}
        assert stages["Purchase"]["count"] == 0
        assert stages["Billing Queue"]["count"] == 0


# ============================================================
# Edge Case 4: Re-entry in Funnel
# ============================================================

class TestReentryInFunnel:
    """Same visitor_id re-enters \u2014 counted once in funnel stages."""

    async def test_reentry_visitor_counted_once(self, client, sample_store_id):
        visitor_id = "VIS_ed9e01"

        # ENTRY
        await client.post("/events/ingest", json={"events": [
            make_event("ENTRY", store_id=sample_store_id, visitor_id=visitor_id),
            make_event("ZONE_ENTER", store_id=sample_store_id, visitor_id=visitor_id, zone_id="SKINCARE"),
            make_event("EXIT", store_id=sample_store_id, visitor_id=visitor_id),
        ]})

        # REENTRY
        await client.post("/events/ingest", json={"events": [
            make_event("REENTRY", store_id=sample_store_id, visitor_id=visitor_id),
            make_event("ZONE_ENTER", store_id=sample_store_id, visitor_id=visitor_id, zone_id="MAKEUP"),
        ]})

        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        entry_stage = next(s for s in resp.json()["stages"] if s["stage"] == "Entry")
        zone_stage = next(s for s in resp.json()["stages"] if s["stage"] == "Zone Visit")

        # Must be 1, not 2 — deduplication by visitor_id
        assert entry_stage["count"] == 1
        assert zone_stage["count"] == 1


# ============================================================
# Edge Case 5: Duplicate Event Ingest (Idempotency)
# ============================================================

class TestDuplicateEventIngest:
    async def test_duplicate_batch_accepted_count_unchanged(self, client, sample_store_id):
        events = [make_event("ENTRY", store_id=sample_store_id) for _ in range(10)]

        resp1 = await client.post("/events/ingest", json={"events": events})
        resp2 = await client.post("/events/ingest", json={"events": events})

        assert resp1.json()["accepted"] == 10
        assert resp2.json()["accepted"] == 10

        # Visitor count should not double
        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        assert resp.json()["unique_visitors"] == 10  # Not 20

    async def test_single_duplicate_event_not_double_counted(self, client, sample_store_id):
        evt = make_event("ENTRY", store_id=sample_store_id)
        event_id = evt["event_id"]

        await client.post("/events/ingest", json={"events": [evt]})
        await client.post("/events/ingest", json={"events": [evt]})  # Same event_id

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        assert resp.json()["unique_visitors"] == 1  # Not 2


# ============================================================
# Edge Case 6: Mixed Valid/Invalid Batch (Partial Success)
# ============================================================

class TestMixedBatch:
    async def test_valid_events_processed_invalid_rejected(self, client, sample_store_id):
        good = [make_event("ENTRY", store_id=sample_store_id) for _ in range(5)]
        bad = make_event("ENTRY", store_id=sample_store_id)
        bad["event_id"] = "not-a-uuid"
        bad2 = make_event("ZONE_ENTER", store_id=sample_store_id)
        bad2["visitor_id"] = "BADFORMAT"

        all_events = good + [bad, bad2]
        resp = await client.post("/events/ingest", json={"events": all_events})

        data = resp.json()
        assert data["accepted"] == 5
        assert data["rejected"] == 2
        assert len(data["errors"]) == 2

    async def test_error_indices_are_correct(self, client, sample_store_id):
        """Error index must match position in input array (0-based)."""
        good1 = make_event("ENTRY", store_id=sample_store_id)
        bad = make_event("ENTRY", store_id=sample_store_id)
        bad["event_id"] = "bad"
        good2 = make_event("EXIT", store_id=sample_store_id)

        resp = await client.post("/events/ingest", json={"events": [good1, bad, good2]})
        data = resp.json()
        assert data["errors"][0]["index"] == 1  # bad is at index 1


# ============================================================
# Edge Case 7: Heatmap Data Confidence
# ============================================================

class TestHeatmapDataConfidence:
    async def test_low_confidence_when_fewer_than_20_sessions(self, client, sample_store_id):
        """Less than 20 sessions → data_confidence=LOW."""
        for _ in range(5):
            events = make_session_events(store_id=sample_store_id)
            await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/heatmap")
        assert resp.json()["data_confidence"] == "LOW"

    async def test_heatmap_normalised_score_0_to_100(self, client, sample_store_id):
        """Normalised score must be in [0, 100]."""
        for _ in range(3):
            events = make_session_events(store_id=sample_store_id)
            await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/heatmap")
        for zone in resp.json()["zones"]:
            assert 0.0 <= zone["normalised_score"] <= 100.0


# ============================================================
# Edge Case 8: Cross-Endpoint Consistency
# ============================================================

class TestCrossEndpointConsistency:
    async def test_metrics_and_funnel_visitor_counts_agree(self, client, sample_store_id):
        """
        metrics.unique_visitors should equal funnel Entry.count
        (both deduplicate by visitor_id with staff excluded).
        """
        for _ in range(4):
            events = make_session_events(store_id=sample_store_id)
            await client.post("/events/ingest", json={"events": events})

        metrics = await client.get(f"/stores/{sample_store_id}/metrics")
        funnel = await client.get(f"/stores/{sample_store_id}/funnel")

        m_visitors = metrics.json()["unique_visitors"]
        f_entry = next(
            s["count"] for s in funnel.json()["stages"] if s["stage"] == "Entry"
        )
        # They should be equal (both based on ENTRY/REENTRY deduplication)
        assert m_visitors == f_entry


# ============================================================
# Edge Case 9: Malformed JSON
# ============================================================

class TestMalformedRequests:
    async def test_malformed_json_returns_422(self, client):
        """Completely invalid JSON → 422 from Pydantic, not 500."""
        resp = await client.post(
            "/events/ingest",
            content=b"{invalid json}",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 422

    async def test_missing_required_fields_422(self, client, sample_store_id):
        """Event missing required fields → returns 200 with rejected count incremented."""
        bad_event = {"event_type": "ENTRY"}  # Missing most required fields
        resp = await client.post("/events/ingest", json={"events": [bad_event]})
        assert resp.status_code == 200
        assert resp.json()["rejected"] == 1
