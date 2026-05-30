#!/usr/bin/env bash
# ============================================================
# run.sh — Process all CCTV clips → events.jsonl → API ingest
#
# Usage:
#   bash pipeline/run.sh /path/to/clips/
#   bash pipeline/run.sh /path/to/clips/ --api http://localhost:8000
#
# The clips directory must contain one subdirectory per store:
#   /path/to/clips/
#     STORE_BLR_001/
#       ENTRY_camera.mp4
#       FLOOR_camera.mp4
#       BILLING_camera.mp4
#     STORE_BLR_002/
#       ...
#
# Output:
#   pipeline/output/<store_id>_events.jsonl
#
# After processing, events are ingested into the API.
# ============================================================

set -euo pipefail

CLIPS_DIR="${1:-}"
API_URL="${2:-http://localhost:8000}"
LAYOUTS_DIR="pipeline/store_layouts"
OUTPUT_DIR="pipeline/output"
POS_PATH="${POS_PATH:-}"
CLIP_START="${CLIP_START:-2026-03-03T08:00:00Z}"

if [ -z "$CLIPS_DIR" ]; then
    echo "Usage: bash pipeline/run.sh /path/to/clips/ [http://api-url]"
    echo ""
    echo "If no clips are available, generate synthetic events instead:"
    echo "  python -m pipeline.generate_synthetic --api-url $API_URL"
    exit 1
fi

if [ ! -d "$CLIPS_DIR" ]; then
    echo "ERROR: Clips directory not found: $CLIPS_DIR"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "=== Apex Retail Detection Pipeline ==="
echo "Clips dir:   $CLIPS_DIR"
echo "Layouts dir: $LAYOUTS_DIR"
echo "Output dir:  $OUTPUT_DIR"
echo "API URL:     $API_URL"
echo ""

# Check API health before processing
echo "[1/3] Checking API health..."
if curl -sf "$API_URL/health" > /dev/null 2>&1; then
    echo "  API is healthy"
else
    echo "  WARNING: API not responding at $API_URL. Events will be written to JSONL only."
    API_URL=""
fi

# Run detection pipeline
echo ""
echo "[2/3] Running detection pipeline..."

POS_ARG=""
if [ -n "$POS_PATH" ] && [ -f "$POS_PATH" ]; then
    POS_ARG="--pos-path $POS_PATH"
fi

API_ARG=""
if [ -n "$API_URL" ]; then
    API_ARG="--api-url $API_URL"
fi

python -m pipeline.detect \
    --clips-dir "$CLIPS_DIR" \
    --layouts-dir "$LAYOUTS_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --clip-start "$CLIP_START" \
    $POS_ARG \
    $API_ARG

echo ""
echo "[3/3] Pipeline complete."
echo ""

# Count events
TOTAL_EVENTS=0
for f in "$OUTPUT_DIR"/*.jsonl; do
    if [ -f "$f" ]; then
        COUNT=$(wc -l < "$f")
        TOTAL_EVENTS=$((TOTAL_EVENTS + COUNT))
        echo "  $(basename $f): $COUNT events"
    fi
done
echo "  Total: $TOTAL_EVENTS events"
echo ""

# If API URL provided, ingest events
if [ -n "$API_URL" ]; then
    echo "Events were ingested to API in real-time during processing."
    echo "To replay from JSONL: python pipeline/replay.py $OUTPUT_DIR/events.jsonl --speed 10x --api $API_URL"
fi

echo ""
echo "Dashboard: $API_URL/dashboard/STORE_BLR_002"
echo "Metrics:   curl $API_URL/stores/STORE_BLR_002/metrics"
echo ""
echo "=== Done ==="
