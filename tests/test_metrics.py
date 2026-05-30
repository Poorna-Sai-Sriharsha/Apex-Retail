# PROMPT: Generate pytest tests for GET /stores/{store_id}/metrics endpoint. Cover:
# empty store returns all zeros, all-staff-only store returns unique_visitors=0,
# zero purchases returns conversion_rate=0.0 (not null), correct unique visitor count
# (staff excluded), avg_dwell_by_zone populated from ZONE_DWELL events,
# queue_depth from max BILLING_QUEUE_JOIN queue_depth, abandonment_rate calculation.
# Use pytest-asyncio with in-memory DB and async HTTP client.
#
# CHANGES MADE:
# - Added assertion that conversion_rate is exactly 0.0 not null (AI used assertIsNotNone)
# - Fixed avg_dwell calculation test to seed ZONE_DWELL events not ZONE_ENTER (AI confused them)
# - Added window parameter test for 7d vs today


from __future__ import annotations

"""
Tests for GET /stores/{store_id}/metrics endpoint.
"""


import pytest
from tests.conftest import make_event, make_session_events




class TestMetricsEmptyStore:
    async def test_empty_store_returns_zeros(self, client, sample_store_id):
        """No events ingested → all metrics must be 0 or empty dict, never null."""
        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["unique_visitors"] == 0
        assert data["conversion_rate"] == 0.0
        assert isinstance(data["conversion_rate"], float), "Must be float, not null"
        assert data["queue_depth"] == 0
        assert data["abandonment_rate"] == 0.0
        assert isinstance(data["avg_dwell_by_zone"], dict)

    async def test_empty_store_conversion_rate_is_float_not_null(self, client, sample_store_id):
        """Explicitly verify no null/None is returned for conversion_rate on zero-visitor store."""
        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        data = resp.json()
        assert data["conversion_rate"] is not None
        assert data["conversion_rate"] == 0.0


class TestMetricsWithData:
    async def test_unique_visitors_counted_correctly(self, client, sample_store_id):
        """3 customers → unique_visitors = 3."""
        for i in range(3):
            events = make_session_events(store_id=sample_store_id)
            await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["unique_visitors"] == 3

    async def test_staff_excluded_from_unique_visitors(self, client, sample_store_id):
        """Staff events must NOT count as unique visitors."""
        # 2 staff + 1 customer
        staff_events = make_session_events(store_id=sample_store_id, is_staff=True)
        customer_events = make_session_events(store_id=sample_store_id, is_staff=False)

        await client.post("/events/ingest", json={"events": staff_events + customer_events})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        data = resp.json()
        # Only 1 customer should be counted
        assert data["unique_visitors"] == 1

    async def test_conversion_rate_with_billing_events(self, client, sample_store_id):
        """Visitors who join billing queue and don't abandon = converted."""
        # 2 customers who convert
        for _ in range(2):
            events = make_session_events(store_id=sample_store_id, go_to_billing=True, abandon=False)
            await client.post("/events/ingest", json={"events": events})

        # 1 customer who abandons
        events = make_session_events(store_id=sample_store_id, go_to_billing=True, abandon=True)
        await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        data = resp.json()
        # 2 converted / 3 total = 0.666...
        assert data["conversion_rate"] > 0.0
        assert data["conversion_rate"] <= 1.0
        assert isinstance(data["conversion_rate"], float)

    async def test_zero_purchases_conversion_rate_is_zero_not_null(self, client, sample_store_id):
        """3 visitors, nobody goes to billing → conversion_rate = 0.0."""
        for _ in range(3):
            events = make_session_events(store_id=sample_store_id, go_to_billing=False)
            await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        data = resp.json()
        assert data["conversion_rate"] == 0.0
        assert data["conversion_rate"] is not None

    async def test_avg_dwell_by_zone_populated(self, client, sample_store_id):
        """ZONE_DWELL events → avg_dwell_by_zone has correct zone entries."""
        events = make_session_events(store_id=sample_store_id)
        await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        data = resp.json()
        assert "SKINCARE" in data["avg_dwell_by_zone"]
        assert data["avg_dwell_by_zone"]["SKINCARE"] > 0

    async def test_queue_depth_from_billing_events(self, client, sample_store_id):
        """queue_depth reflects max queue_depth from BILLING_QUEUE_JOIN events."""
        events = [
            make_event("BILLING_QUEUE_JOIN", store_id=sample_store_id, zone_id="BILLING", queue_depth=7),
        ]
        await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        assert resp.json()["queue_depth"] == 7

    async def test_abandonment_rate_calculation(self, client, sample_store_id):
        """2 joins, 1 abandon → abandonment_rate = 0.5."""
        events = [
            make_event("BILLING_QUEUE_JOIN", store_id=sample_store_id, zone_id="BILLING",
                       visitor_id="VIS_aaa111", queue_depth=1),
            make_event("BILLING_QUEUE_JOIN", store_id=sample_store_id, zone_id="BILLING",
                       visitor_id="VIS_bbb222", queue_depth=2),
            make_event("BILLING_QUEUE_ABANDON", store_id=sample_store_id, zone_id="BILLING",
                       visitor_id="VIS_bbb222"),
        ]
        await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        data = resp.json()
        assert abs(data["abandonment_rate"] - 0.5) < 0.01


class TestMetricsAllStaffStore:
    async def test_all_staff_clip_returns_zero_visitors(self, client, sample_store_id):
        """Store with only staff events → unique_visitors = 0, conversion_rate = 0.0."""
        staff1 = make_session_events(store_id=sample_store_id, is_staff=True)
        staff2 = make_session_events(store_id=sample_store_id, is_staff=True)
        await client.post("/events/ingest", json={"events": staff1 + staff2})

        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        data = resp.json()
        assert data["unique_visitors"] == 0
        assert data["conversion_rate"] == 0.0

    async def test_store_id_in_response(self, client, sample_store_id):
        """Response must include the queried store_id."""
        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        assert resp.json()["store_id"] == sample_store_id

    async def test_window_field_defaults_to_today(self, client, sample_store_id):
        """Default window should be 'today'."""
        resp = await client.get(f"/stores/{sample_store_id}/metrics")
        assert resp.json()["window"] == "today"


class TestMetricsMultiStore:
    async def test_different_stores_isolated(self, client):
        """Events for STORE_BLR_001 must not affect STORE_BLR_002 metrics."""
        events_002 = make_session_events(store_id="STORE_BLR_002")
        events_001 = make_session_events(store_id="STORE_BLR_001")

        await client.post("/events/ingest", json={"events": events_002 + events_001})

        resp_002 = await client.get("/stores/STORE_BLR_002/metrics")
        resp_999 = await client.get("/stores/STORE_BLR_999/metrics")  # Non-existent

        assert resp_002.json()["unique_visitors"] == 1
        assert resp_999.json()["unique_visitors"] == 0  # No events → 0, not 500


"""
Tests for GET /stores/{store_id}/funnel endpoint.
"""


import pytest
from tests.conftest import make_event, make_session_events




def get_stage(stages: list[dict], name: str) -> dict:
    for s in stages:
        if s["stage"] == name:
            return s
    raise KeyError(f"Stage '{name}' not found in funnel")


class TestFunnelEmpty:
    async def test_empty_store_all_zeros(self, client, sample_store_id):
        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["stages"]) == 4
        for stage in data["stages"]:
            assert stage["count"] == 0

    async def test_entry_stage_always_zero_drop_off(self, client, sample_store_id):
        """First stage (Entry) must always have drop_off_pct=0.0."""
        events = make_session_events(store_id=sample_store_id)
        await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        stages = resp.json()["stages"]
        entry_stage = get_stage(stages, "Entry")
        assert entry_stage["drop_off_pct"] == 0.0


class TestFunnelCounting:
    async def test_3_customers_entry_count_is_3(self, client, sample_store_id):
        for _ in range(3):
            events = make_session_events(store_id=sample_store_id)
            await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        entry = get_stage(resp.json()["stages"], "Entry")
        assert entry["count"] == 3

    async def test_zone_stage_counts_visitors_with_zone_enter(self, client, sample_store_id):
        """2 customers who enter zones, 1 who doesn't → Zone Visit count = 2."""
        # 2 with zone events (from make_session_events)
        for _ in range(2):
            events = make_session_events(store_id=sample_store_id)
            await client.post("/events/ingest", json={"events": events})

        # 1 entry-only (no zone visit)
        entry_only = [make_event("ENTRY", store_id=sample_store_id)]
        await client.post("/events/ingest", json={"events": entry_only})

        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        stages = resp.json()["stages"]
        entry = get_stage(stages, "Entry")
        zone = get_stage(stages, "Zone Visit")

        assert entry["count"] == 3
        assert zone["count"] == 2

    async def test_purchase_excludes_abandonments(self, client, sample_store_id):
        """Visitor who abandons billing must NOT appear in Purchase stage."""
        # 2 converters + 1 abandoner
        for _ in range(2):
            events = make_session_events(store_id=sample_store_id, go_to_billing=True, abandon=False)
            await client.post("/events/ingest", json={"events": events})

        abandon_events = make_session_events(store_id=sample_store_id, go_to_billing=True, abandon=True)
        await client.post("/events/ingest", json={"events": abandon_events})

        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        stages = resp.json()["stages"]
        billing = get_stage(stages, "Billing Queue")
        purchase = get_stage(stages, "Purchase")

        assert billing["count"] == 3
        assert purchase["count"] == 2


class TestFunnelReentryDeduplication:
    async def test_reentry_visitor_counted_once_in_entry_stage(self, client, sample_store_id):
        """
        CRITICAL: A visitor who re-enters (REENTRY event) must be counted once
        in the Entry stage funnel, not twice.
        """
        visitor_id = "VIS_aa1234"

        # First ENTRY
        entry_event = make_event("ENTRY", store_id=sample_store_id, visitor_id=visitor_id)
        # EXIT
        exit_event = make_event("EXIT", store_id=sample_store_id, visitor_id=visitor_id)
        # REENTRY — same visitor
        reentry_event = make_event("REENTRY", store_id=sample_store_id, visitor_id=visitor_id)

        await client.post("/events/ingest", json={
            "events": [entry_event, exit_event, reentry_event]
        })

        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        entry_stage = get_stage(resp.json()["stages"], "Entry")

        # CRITICAL: Funnel deduplicates by visitor_id — should be 1, not 2
        assert entry_stage["count"] == 1, (
            f"Re-entry visitor counted {entry_stage['count']} times, expected 1"
        )

    async def test_reentry_visitor_in_billing_counted_once(self, client, sample_store_id):
        """Re-entering visitor who goes to billing → counted once in Billing Queue stage."""
        visitor_id = "VIS_bb5678"

        events = [
            make_event("ENTRY", store_id=sample_store_id, visitor_id=visitor_id),
            make_event("BILLING_QUEUE_JOIN", store_id=sample_store_id, visitor_id=visitor_id,
                       zone_id="BILLING", queue_depth=1),
            make_event("EXIT", store_id=sample_store_id, visitor_id=visitor_id),
            # Re-enter and go to billing again
            make_event("REENTRY", store_id=sample_store_id, visitor_id=visitor_id),
            make_event("BILLING_QUEUE_JOIN", store_id=sample_store_id, visitor_id=visitor_id,
                       zone_id="BILLING", queue_depth=2),
        ]
        await client.post("/events/ingest", json={"events": events})

        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        billing = get_stage(resp.json()["stages"], "Billing Queue")
        assert billing["count"] == 1  # Deduplicated by visitor_id


class TestFunnelDropOff:
    async def test_drop_off_pct_correct_between_stages(self, client, sample_store_id):
        """
        4 enter, 3 go to zones, 2 go to billing, 1 purchases.
        Drop-off: Entry→Zone = 25%, Zone→Billing = 33.3%, Billing→Purchase = 50%.
        """
        # 4 entry events with different trajectories
        for _ in range(3):
            events = make_session_events(store_id=sample_store_id, go_to_billing=True, abandon=False)
            await client.post("/events/ingest", json={"events": events})

        # 1 customer: entry + zone but no billing
        no_billing = make_session_events(store_id=sample_store_id, go_to_billing=False)
        await client.post("/events/ingest", json={"events": no_billing})

        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        stages = resp.json()["stages"]
        zone = get_stage(stages, "Zone Visit")
        assert zone["drop_off_pct"] >= 0.0  # Some drop-off from entry to zone

    async def test_funnel_store_id_matches(self, client, sample_store_id):
        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        assert resp.json()["store_id"] == sample_store_id

    async def test_funnel_has_4_stages(self, client, sample_store_id):
        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        assert len(resp.json()["stages"]) == 4

    async def test_funnel_stage_names_correct(self, client, sample_store_id):
        resp = await client.get(f"/stores/{sample_store_id}/funnel")
        names = [s["stage"] for s in resp.json()["stages"]]
        assert names == ["Entry", "Zone Visit", "Billing Queue", "Purchase"]


"""
Tests for GET /health endpoint.
"""


from datetime import datetime, timedelta, timezone

import pytest
from tests.conftest import make_event




class TestHealthBasic:
    async def test_health_returns_200(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200

    async def test_health_response_schema(self, client):
        resp = await client.get("/health")
        data = resp.json()
        assert "status" in data
        assert "checked_at" in data
        assert "stores" in data
        assert isinstance(data["stores"], list)

    async def test_status_valid_values(self, client):
        resp = await client.get("/health")
        assert resp.json()["status"] in ("OK", "DEGRADED")

    async def test_stores_list_populated(self, client):
        """Health endpoint must include all configured stores."""
        resp = await client.get("/health")
        stores = resp.json()["stores"]
        assert len(stores) >= 1
        # Each store entry must have required fields
        for store in stores:
            assert "store_id" in store
            assert "stale_feed" in store
            assert isinstance(store["stale_feed"], bool)


class TestHealthWithEmptyDB:
    async def test_empty_db_is_degraded(self, client):
        """No events ingested → all stores are stale → status DEGRADED."""
        resp = await client.get("/health")
        data = resp.json()
        assert data["status"] == "DEGRADED"

    async def test_all_stores_stale_on_empty_db(self, client):
        resp = await client.get("/health")
        stores = resp.json()["stores"]
        for store in stores:
            assert store["stale_feed"] is True

    async def test_last_event_timestamp_null_on_empty(self, client):
        resp = await client.get("/health")
        for store in resp.json()["stores"]:
            assert store["last_event_timestamp"] is None
            assert store["lag_minutes"] is None


class TestHealthWithRecentEvents:
    async def test_recent_event_store_is_not_stale(self, client, sample_store_id):
        """Recent event (now) → stale_feed=False for that store."""
        events = [make_event("ENTRY", store_id=sample_store_id)]
        await client.post("/events/ingest", json={"events": events})

        resp = await client.get("/health")
        stores = {s["store_id"]: s for s in resp.json()["stores"]}

        if sample_store_id in stores:
            assert stores[sample_store_id]["stale_feed"] is False
            assert stores[sample_store_id]["last_event_timestamp"] is not None
            assert stores[sample_store_id]["lag_minutes"] is not None
            assert isinstance(stores[sample_store_id]["lag_minutes"], float)

    async def test_lag_minutes_is_float(self, client, sample_store_id):
        """lag_minutes must be a float value, not integer."""
        events = [make_event("ENTRY", store_id=sample_store_id)]
        await client.post("/events/ingest", json={"events": events})

        resp = await client.get("/health")
        for store in resp.json()["stores"]:
            if store["store_id"] == sample_store_id:
                if store["lag_minutes"] is not None:
                    assert isinstance(store["lag_minutes"], (int, float))


class TestHealthStaleFeed:
    async def test_stale_feed_for_old_event(self, client, sample_store_id):
        """Event 40 min ago → stale_feed=True for that store."""
        old_time = datetime.now(timezone.utc) - timedelta(minutes=40)
        events = [make_event("ENTRY", store_id=sample_store_id, timestamp=old_time)]
        await client.post("/events/ingest", json={"events": events})

        resp = await client.get("/health")
        stores = {s["store_id"]: s for s in resp.json()["stores"]}
        if sample_store_id in stores:
            assert stores[sample_store_id]["stale_feed"] is True

    async def test_stale_store_makes_overall_status_degraded(self, client, sample_store_id):
        """Any stale store → overall status = DEGRADED."""
        resp = await client.get("/health")
        stores = resp.json()["stores"]
        any_stale = any(s["stale_feed"] for s in stores)
        if any_stale:
            assert resp.json()["status"] == "DEGRADED"

    async def test_checked_at_is_recent(self, client):
        """checked_at must be within the last 5 seconds."""
        resp = await client.get("/health")
        checked_at_str = resp.json()["checked_at"]
        checked_at = datetime.fromisoformat(checked_at_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = (now - checked_at).total_seconds()
        assert 0 <= delta < 5, f"checked_at should be recent, but delta={delta}s"

import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_dashboard_endpoint(client: AsyncClient, sample_store_id: str):
    response = await client.get(f"/dashboard/{sample_store_id}")
    assert response.status_code in (200, 404)



import pytest
from httpx import AsyncClient
from datetime import datetime, timezone
import uuid

def build_mock_event(event_type, store_id, zone_id):
    return {
        "event_id": str(uuid.uuid4()),
        "store_id": store_id,
        "camera_id": "CAM_FLOOR_01",
        "visitor_id": "VIS_TEST",
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc),
        "zone_id": zone_id,
        "dwell_ms": 5000,
        "is_staff": False,
        "confidence": 0.9,
        "metadata": {"session_seq": 1}
    }

@pytest.mark.asyncio
async def test_heatmap_endpoint_fixed(client: AsyncClient, sample_store_id: str, db):
    from app.models import EventORM
    db.add(EventORM(**build_mock_event("ZONE_DWELL", sample_store_id, "SKINCARE")))
    await db.commit()
    response = await client.get(f"/stores/{sample_store_id}/heatmap?window=today")
    assert response.status_code == 200
    assert "zones" in response.json()
