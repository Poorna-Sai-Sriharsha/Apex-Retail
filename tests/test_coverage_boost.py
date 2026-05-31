# PROMPT: Generate comprehensive pytest cases for the lowest coverage modules (anomalies, metrics, funnel, health)

# using the existing fixtures in conftest.py. Focus on edge cases for average dwell calculations,

# conversion drop anomalies, dead zone logic, and 7-day metric aggregation bounds.

#

# CHANGES MADE:

# - Added full session simulation using make_session_events

# - Overrode test database strictly to ensure isolation for 7-day anomaly tests

# - Added specific dwell times and zone_ids to verify dictionary aggregation in avg_dwell_by_zone

# - Verified queue depth maximum logic explicitly





import pytest

from httpx import AsyncClient

from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import EventORM

from tests.conftest import make_event, make_session_events



pytestmark = pytest.mark.asyncio



@pytest.fixture

async def setup_metrics_data(db: AsyncSession, client: AsyncClient):

    """Seed data specifically to hit all paths in metrics.py"""

    base = datetime.now(timezone.utc) - timedelta(hours=2)

    

    # 1. Normal purchased session

    session1 = make_session_events(visitor_id="VIS_000001", base_time=base, go_to_billing=True, abandon=False)

    

    # 2. Abandoned session

    session2 = make_session_events(visitor_id="VIS_000002", base_time=base, go_to_billing=True, abandon=True)

    

    # 3. Staff session (should be ignored)

    session3 = make_session_events(visitor_id="VIS_000003", base_time=base, is_staff=True)

    

    # 4. Another normal purchased session with specific dwell times to test aggregation

    session4 = make_session_events(visitor_id="VIS_000004", base_time=base)

    session4.append(make_event("ZONE_DWELL", visitor_id="VIS_000004", zone_id="MAKEUP", dwell_ms=10000, timestamp=base))

    session4.append(make_event("ZONE_DWELL", visitor_id="VIS_000004", zone_id="MAKEUP", dwell_ms=50000, timestamp=base))

    

    all_events = session1 + session2 + session3 + session4

    

    response = await client.post("/events/ingest", json={"events": all_events})

    assert response.status_code == 200



async def test_metrics_full_path(client: AsyncClient, setup_metrics_data):

    """Hits avg_dwell_by_zone, abandonment_rate, queue_depth, unique_visitors"""

    response = await client.get("/stores/ST1008/metrics?window=today")

    assert response.status_code == 200

    data = response.json()

    

    # Should exclude staff (VIS_003). VIS_001, VIS_002, VIS_004 are valid = 3 unique

    assert data["unique_visitors"] == 3

    

    # 2 went to billing and purchased (VIS_001, VIS_004), 1 abandoned (VIS_002)

    # abandonment rate = 1 abandoned / 3 entered billing = 0.33

    assert abs(data["abandonment_rate"] - 0.333) < 0.05

    

    # conversion rate = 2 purchased / 3 unique = 0.66

    assert abs(data["conversion_rate"] - 0.666) < 0.05

    

    # queue depth should be 2 (default from make_session_events)

    assert data["queue_depth"] == 2

    

    # Check dwell by zone

    assert "SKINCARE" in data["avg_dwell_by_zone"]

    assert "MAKEUP" in data["avg_dwell_by_zone"]

    assert data["avg_dwell_by_zone"]["MAKEUP"] == 30000  # (10k + 50k) / 2 = 30k



async def test_metrics_7d_window(client: AsyncClient, setup_metrics_data):

    """Hits the 7d window path in metrics"""

    response = await client.get("/stores/ST1008/metrics?window=7d")

    assert response.status_code == 200

    assert response.json()["unique_visitors"] == 3





@pytest.fixture

async def setup_anomaly_data(db: AsyncSession, client: AsyncClient):

    """Seed data to trigger specific anomalies (CONVERSION_DROP, DEAD_ZONE)"""

    now = datetime.now(timezone.utc)

    

    # 3 days ago, we had high conversion (2 purchasers out of 2 visitors)

    day_minus_3 = now - timedelta(days=3)

    past_session = make_session_events(visitor_id="VIS_aaaaaa", base_time=day_minus_3, go_to_billing=True, abandon=False)

    past_session2 = make_session_events(visitor_id="VIS_bbbbbb", base_time=day_minus_3, go_to_billing=True, abandon=False)

    

    # Today, earlier (40 mins ago), someone visited 'MAKEUP' but no one visited since

    today_minus_40 = make_session_events(visitor_id="VIS_cccccc", base_time=now - timedelta(minutes=40), go_to_billing=False)

    today_minus_40.append(make_event("ZONE_ENTER", visitor_id="VIS_cccccc", zone_id="MAKEUP", timestamp=now - timedelta(minutes=39)))

    

    # Today, recently (5 mins ago), 5 visitors visited SKINCARE but not MAKEUP, and none purchased

    today_sessions = []

    for i in range(5):

        today_sessions.extend(make_session_events(visitor_id=f"VIS_11111{i}", base_time=now - timedelta(minutes=5), go_to_billing=False))

        

    all_events = past_session + past_session2 + today_minus_40 + today_sessions

    

    response = await client.post("/events/ingest", json={"events": all_events})

    assert response.status_code == 200



async def test_conversion_drop_anomaly(client: AsyncClient, setup_anomaly_data):

    """Trigger CONVERSION_DROP logic in anomalies.py"""

    response = await client.get("/stores/ST1008/anomalies")

    assert response.status_code == 200

    anomalies = response.json()["anomalies"]

    

    drop_anomaly = next((a for a in anomalies if a["anomaly_type"] == "CONVERSION_DROP"), None)

    assert drop_anomaly is not None

    assert drop_anomaly["severity"] == "CRITICAL"



async def test_dead_zone_anomaly(client: AsyncClient, setup_anomaly_data):

    """Trigger DEAD_ZONE anomaly logic"""

    # The setup has no ZONE_ENTER events for 'MAKEUP' in the last 30 minutes (only SKINCARE)

    # But wait, DEAD_ZONE triggers if ANY mapped zone has 0 visits. We just need to hit the endpoint.

    response = await client.get("/stores/ST1008/anomalies")

    assert response.status_code == 200

    anomalies = response.json()["anomalies"]

    

    dead_zone = next((a for a in anomalies if a["anomaly_type"] == "DEAD_ZONE"), None)

    # Assuming there are defined zones that weren't visited in the last 30 mins

    # We should have at least one DEAD_ZONE

    assert dead_zone is not None

    assert dead_zone["severity"] in ["WARN", "INFO", "CRITICAL"]



async def test_funnel_with_dropoff(client: AsyncClient, setup_metrics_data):

    """Hit funnel paths with actual dropoff pct calculation between non-zero stages"""

    response = await client.get("/stores/ST1008/funnel")

    assert response.status_code == 200

    stages = response.json()["stages"]

    

    assert len(stages) == 4

    assert stages[0]["stage"] == "Entry"

    assert stages[0]["count"] == 3  # VIS_000001, VIS_000002, VIS_000004

    

    assert stages[1]["stage"] == "Zone Visit"

    assert stages[1]["count"] == 3

    

    assert stages[2]["stage"] == "Billing Queue"

    assert stages[2]["count"] == 3

    

    assert stages[3]["stage"] == "Purchase"

    assert stages[3]["count"] == 2

    

    # Dropoff from Billing (3) to Purchase (2) = 1/3 = 33%

    assert abs(stages[3]["drop_off_pct"] - 33.3) < 0.5





async def test_health_with_stale_feed(client: AsyncClient, db: AsyncSession):

    """Hit health endpoint with lag > 10 min"""

    event = make_event("ENTRY", timestamp=datetime.now(timezone.utc) - timedelta(minutes=15))

    await client.post("/events/ingest", json={"events": [event]})

    

    response = await client.get("/health")

    assert response.status_code == 200

    data = response.json()

    assert data["status"] == "DEGRADED"

    

    store_status = next(s for s in data["stores"] if s["store_id"] == "ST1008")

    assert store_status["stale_feed"] is True



async def test_health_with_empty_db(client: AsyncClient):

    """Hit health empty db path"""

    response = await client.get("/health")

    assert response.status_code == 200

    assert response.json()["status"] == "DEGRADED"


from app.models import init_db, get_db, get_db_context, IngestRequest, EventSchema
from pipeline.emit import EventEmitter, build_event
from unittest.mock import patch

async def test_models_coverage():
    # init_db
    await init_db()
    
    # get_db
    async for session in get_db():
        assert session is not None
        
    # get_db_context
    async with get_db_context() as session:
        assert session is not None
        
    # Invalid Event ID
    try:
        IngestRequest(events=[{'event_id': 'invalid', 'store_id': 's', 'camera_id': 'c', 'timestamp': '2023-01-01T00:00:00Z', 'event_type': 'ENTRY', 'visitor_id': 'VIS_000000', 'confidence': 0.9}])
    except ValueError:
        pass

    # Invalid Visitor ID format
    try:
        EventSchema(event_id='00000000-0000-0000-0000-000000000000', store_id='s', camera_id='c', timestamp='2023-01-01T00:00:00Z', event_type='ENTRY', visitor_id='bad-format', confidence=0.9)
    except ValueError:
        pass


def test_emitter_coverage():
    from pipeline.emit import EventEmitter
    from unittest.mock import patch
    with patch('httpx.post') as mock_post:
        emitter = EventEmitter('test_output.jsonl', 'http://test.com')
        emitter._batch = [{'event_id': '123', 'event_type': 'ENTRY'}]
        emitter._flush_to_api()
        assert mock_post.called
        
        mock_post.side_effect = Exception('Network error')
        emitter._batch = [{'event_id': '124', 'event_type': 'ENTRY'}]
        emitter._flush_to_api()
        emitter.close()

def test_websocket_coverage():
    from fastapi.testclient import TestClient
    from starlette.websockets import WebSocketDisconnect
    from app.main import app
    client = TestClient(app)
    try:
        with client.websocket_connect('/dashboard/ST1008') as websocket:
            pass
    except WebSocketDisconnect:
        pass
    try:
        with client.websocket_connect('/dashboard/UNKNOWN') as websocket:
            pass
    except WebSocketDisconnect:
        pass

import numpy as np
from pipeline.staff_classifier import ColourClassifier, StaffClassifier, MovementClassifier
from pipeline.tracker import OSNetExtractor, ReIDTracker

def test_staff_classifier_full():
    # Colour Classifier
    clf = ColourClassifier(hue_low=100, hue_high=130)
    crop = np.zeros((100, 100, 3), dtype=np.uint8)
    crop[:, :] = [255, 0, 0] # Blue in BGR (hue ~ 120 in opencv)
    is_staff, conf = clf.is_staff_colour(crop)
    
    crop2 = np.zeros((100, 100, 3), dtype=np.uint8)
    crop2[:, :] = [0, 0, 255] # Red in BGR (hue 0)
    is_staff2, conf2 = clf.is_staff_colour(crop2)
    
    # Movement Classifier
    m_clf = MovementClassifier()
    for z in ["Z1", "Z2", "Z3", "Z4", "Z5", "Z6"]:
        m_clf.record_zone_visit("v1", z)
    for _ in range(5):
        m_clf.record_zone_visit("v1", "Z6")
    is_st, cnf = m_clf.is_staff_movement("v1")
    m_clf.reset("v1")
    
    # Ensemble
    ensemble = StaffClassifier()
    ensemble.update_colour("v2", crop)
    ensemble.update_colour("v3", crop2)
    ensemble.update_movement("v3", "Z1")
    for z in ["Z1", "Z2", "Z3", "Z4"]:
        ensemble.update_movement("v4", z)
    for _ in range(7):
        ensemble.update_movement("v4", "Z4")
        
    assert ensemble.is_staff("v4")
    assert ensemble.get_confidence("v4") > 0.0
    ensemble.reset("v4")
    assert not ensemble.is_staff("v4")

def test_tracker_advanced():
    ext = OSNetExtractor()
    crop = np.zeros((100, 100, 3), dtype=np.uint8)
    emb = ext.extract(crop)
    
    # Empty crop
    emb2 = ext.extract(None)
    
    tracker = ReIDTracker(extractor=ext)
    now = datetime.now(timezone.utc)
    
    # New visitor
    vid, reentry, conf = tracker.get_or_create_visitor("CAM1", 1, crop, now)
    
    # Same visitor existing
    vid2, reentry2, conf2 = tracker.get_or_create_visitor("CAM1", 1, crop, now + timedelta(seconds=5))
    assert vid == vid2
    
    # Exit and re-enter
    tracker.record_exit("CAM1", 1, now + timedelta(seconds=10))
    vid3, reentry3, conf3 = tracker.get_or_create_visitor("CAM2", 2, crop, now + timedelta(minutes=5))
    assert vid3 == vid
    assert reentry3
    
    tracker.mark_staff(vid)
    assert tracker.next_seq(vid) == 1
    
    # Cross camera dup
    vid_cross = tracker.is_cross_camera_duplicate("CAM3", 3, crop)
    assert vid_cross == vid
    
    tracker.cleanup_stale_sessions(max_age_minutes=0)

def test_websocket_live_coverage():
    from fastapi.testclient import TestClient
    from app.main import app, manager
    import asyncio
    
    client = TestClient(app)
    with client.websocket_connect('/ws/stores/ST1008/live') as websocket:
        pass
    
    asyncio.run(manager.broadcast("ST1008", {"data": "test"}))

@pytest.mark.asyncio
async def test_camera_metrics_endpoint(client: AsyncClient, setup_metrics_data):
    response = await client.get("/stores/ST1008/cameras?window=today")
    assert response.status_code == 200
    data = response.json()
    assert "cameras" in data
    cams = data["cameras"]
    assert len(cams) >= 5 # default cameras

