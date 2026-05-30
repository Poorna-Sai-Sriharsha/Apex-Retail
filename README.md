# Apex Retail Store Intelligence

> End-to-end retail analytics: CCTV detection → structured events → live REST API + dashboard.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-green.svg)](https://fastapi.tiangolo.com)
[![YOLOv8](https://img.shields.io/badge/YOLOv8-ultralytics-red.svg)](https://ultralytics.com)
[![Docker](https://img.shields.io/badge/docker-compose-blue.svg)](https://docker.com)

---

## 5-Command Setup

```bash
# 1. Clone the repository
git clone <your-repo-url> && cd store-intelligence

# 2. Copy environment config
cp .env.example .env

# 3. Start the API stack (PostgreSQL + FastAPI)
docker compose up -d

# 4. Generate synthetic events (or run real pipeline — see below)
python -m pipeline.generate_synthetic --api-url http://localhost:8000

# 5. Query the API
curl http://localhost:8000/stores/APEX_RETAIL/metrics
```

The API is now live at **http://localhost:8000**.

---

## Quick Verification

```bash
# Health check
curl http://localhost:8000/health

# Store metrics
curl http://localhost:8000/stores/APEX_RETAIL/metrics

# Conversion funnel
curl http://localhost:8000/stores/APEX_RETAIL/funnel

# Zone heatmap
curl http://localhost:8000/stores/APEX_RETAIL/heatmap

# Anomaly detection
curl http://localhost:8000/stores/APEX_RETAIL/anomalies

# API docs (Swagger)
open http://localhost:8000/docs
```

---

## Running the Detection Pipeline

### With real CCTV clips (when dataset arrives)

Place clips in this structure:
```
/path/to/clips/
├── STORE_BLR_001/
│   ├── ENTRY_camera.mp4      # Entry/exit threshold camera
│   ├── FLOOR_camera.mp4      # Main floor zone coverage
│   └── BILLING_camera.mp4   # Billing counter area
├── APEX_RETAIL/
│   └── ...
```

Then run:
```bash
# Set POS data path (optional, for conversion rate)
export POS_PATH=/path/to/pos_transactions.csv

# Process all stores
bash pipeline/run.sh /path/to/clips/ http://localhost:8000
```

Events are:
1. Written to `pipeline/output/<store_id>_events.jsonl`
2. Posted to the API in real-time batches of 100

### Without clips (synthetic data)

```bash
# Generate realistic events for all 5 stores
python -m pipeline.generate_synthetic \
  --output pipeline/output/events.jsonl \
  --stores 5 \
  --api-url http://localhost:8000
```

### Simulated real-time replay

```bash
# Replay stored events at 10× speed to the live API
python pipeline/replay.py pipeline/output/events.jsonl \
  --speed 10 \
  --api http://localhost:8000

# Or slower for dashboard demo
python pipeline/replay.py pipeline/output/events.jsonl \
  --speed 3 \
  --api http://localhost:8000
```

Watch the **live dashboard** update as events stream in.

---

## Live Dashboard (Part E \u2014 Bonus)

**URL**: http://localhost:8000/

The dashboard provides a premium, professional "Dark Mode" interface with:
- **Real-time KPI cards**: Visitors, Conversion Rate, Queue Depth, Abandonment Rate
- **Live Conversion Funnel**: 4-stage bar chart with drop-off percentages
- **Zone Heatmap**: Visit frequency normalised 0\u2013100 per zone
- **Anomaly Panel**: Live anomaly feed with severity (INFO/WARN/CRITICAL)

The dashboard auto-refreshes every 5 seconds. It is built with Vanilla HTML/CSS/JS and served directly by FastAPI.

---

## Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-cov httpx aiosqlite

# Run all tests with coverage
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=app --cov=pipeline --cov-report=term-missing

# Run specific test file
pytest tests/test_edge_cases.py -v
```

**Coverage target**: >70% statement coverage (enforced via `--cov-fail-under=70`)

---

## Architecture

```
CCTV Clips (5 stores × 3 cameras)
    ↓ OpenCV + YOLOv8 + ByteTrack
Detection Layer (detect.py)
    ↓ OSNet Re-ID | Staff Classifier | Zone Mapper | POS Correlator
Event Emission (emit.py → events.jsonl)
    ↓ HTTP POST batches
Intelligence API (FastAPI + PostgreSQL)
    ↓ WebSocket push
Live Dashboard (dashboard.html)
```

See [docs/DESIGN.md](docs/DESIGN.md) for full architecture documentation.
See [docs/CHOICES.md](docs/CHOICES.md) for engineering decisions.

---

## Event Schema

All events follow this exact schema:

```json
{
  "event_id":   "uuid-v4",
  "store_id":   "APEX_RETAIL",
  "camera_id":  "CAM_ENTRY_01",
  "visitor_id": "VIS_c8a2f1",
  "event_type": "ZONE_DWELL",
  "timestamp":  "2026-03-03T14:22:10Z",
  "zone_id":    "SKINCARE",
  "dwell_ms":   8400,
  "is_staff":   false,
  "confidence": 0.91,
  "metadata": {
    "queue_depth": null,
    "sku_zone":    "MOISTURISER",
    "session_seq": 5
  }
}
```

Supported event types: `ENTRY`, `EXIT`, `ZONE_ENTER`, `ZONE_EXIT`, `ZONE_DWELL`,
`BILLING_QUEUE_JOIN`, `BILLING_QUEUE_ABANDON`, `REENTRY`

---

## API Reference

### POST /events/ingest
Accepts up to 500 events per batch. Idempotent by `event_id`. Returns partial success.

```bash
curl -X POST http://localhost:8000/events/ingest \
  -H "Content-Type: application/json" \
  -d '{"events": [...]}'
```

### GET /stores/{store_id}/metrics
```json
{
  "store_id": "APEX_RETAIL",
  "window": "today",
  "unique_visitors": 142,
  "conversion_rate": 0.31,
  "avg_dwell_by_zone": {"SKINCARE": 87400},
  "queue_depth": 3,
  "abandonment_rate": 0.18
}
```

### GET /health
Returns `STALE_FEED` warning if any store's last event is >10 minutes old.

---

## Project Structure

```
store-intelligence/
├── pipeline/              # Detection pipeline
│   ├── detect.py          # YOLOv8 + ByteTrack + Re-ID + Zones + POS main loop
│   ├── tracker.py         # OSNet Re-ID + session management
│   ├── emit.py            # Event schema + JSONL writer
│   ├── staff_classifier.py # Ensemble staff detection
│   ├── run.sh             # One-command pipeline runner
│   └── store_layouts/     # Store zone configurations (5 stores)
├── app/                   # FastAPI application
│   ├── main.py            # App + middleware + WebSocket + Heatmap
│   ├── models.py          # Pydantic v2 schemas + ORM + Database
│   ├── ingestion.py       # POST /events/ingest
│   ├── metrics.py         # GET /stores/{id}/metrics
│   ├── anomalies.py       # GET /stores/{id}/anomalies
│   ├── dashboard.html     # Live WebSocket dashboard
├── tests/                 # Test suite (>70% coverage)
│   ├── test_pipeline.py   # Pipeline module tests
│   ├── test_metrics.py    # Metrics endpoint tests
│   ├── test_anomalies.py  # Anomaly detection tests
│   └── test_edge_cases.py # All spec edge cases
├── DESIGN.md              # Architecture + AI decisions
├── CHOICES.md             # 3 key engineering choices
├── docker-compose.yml     # Full stack (API + PostgreSQL)
├── Dockerfile             # Python 3.11 slim
├── requirements.txt       # All dependencies
├── pyproject.toml         # Pytest + coverage config
└── .env.example           # Environment variable template
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite+aiosqlite:///./store_intelligence.db` | Database connection string |
| `LOG_LEVEL` | `INFO` | Logging level |
| `STORE_IDS` | `STORE_BLR_001,...,005` | Comma-separated store IDs |
| `STALE_FEED_MINUTES` | `10` | Minutes before a store is flagged stale |
| `REID_SIMILARITY_THRESHOLD` | `0.75` | Cosine similarity for Re-ID matching |
| `REID_REENTRY_WINDOW_MINUTES` | `30` | Max gap for re-entry detection |
| `QUEUE_SPIKE_MULTIPLIER` | `2.0` | Queue spike = current > N × 7d avg |
| `CONVERSION_DROP_THRESHOLD` | `0.70` | Drop alert if today < 70% of 7d avg |
| `DEAD_ZONE_MINUTES` | `30` | Minutes of inactivity → DEAD_ZONE anomaly |

---

## Adding Real Dataset

When the CCTV ZIP arrives:

1. Extract to `/path/to/dataset/`
2. Copy `store_layout.json` to `pipeline/store_layouts/STORE_BLR_XXX.json` (one per store)
3. Run: `bash pipeline/run.sh /path/to/dataset/clips/ http://localhost:8000`
4. Set `POS_PATH=/path/to/pos_transactions.csv` for conversion rate accuracy
5. Validate against `sample_events.jsonl`: `python pipeline/validate.py`

---

## License

Challenge use only. Not for redistribution.


**Dashboard URL:** [http://localhost:8000/dashboard/APEX_RETAIL](http://localhost:8000/dashboard/APEX_RETAIL)
