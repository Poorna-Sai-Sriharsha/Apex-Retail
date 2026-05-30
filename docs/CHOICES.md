# CHOICES.md — Key Engineering Decisions

Three engineering decisions shaped the architecture of this system significantly. Each is documented with options considered, the AI's suggestion, the final choice, and the rationale.

---

## Decision 1 — Detection Model Selection

### Options Considered

| Model | Speed (GPU) | Accuracy (COCO) | CCTV-Specific Notes |
|---|---|---|---|
| **YOLOv8n** | ~160 FPS | 37.3 mAP | Fast, low memory, group handling via NMS |
| **YOLOv8m** | ~80 FPS | 50.2 mAP | Better occlusion handling, larger model |
| **RT-DETR** (Baidu) | ~114 FPS | 53.1 mAP | Transformer-based, no NMS ambiguity |
| **MediaPipe BlazePose** | Very fast | Person only (no box) | No bounding box → no Re-ID crop |
| **Grounding DINO** | Slow | Flexible | Overkill for single-class detection |

### AI Suggestion

Claude Sonnet suggested **YOLOv8m** as the best balance between accuracy and speed for retail CCTV footage, citing its superior occlusion handling (higher mAP) vs YOLOv8n. It noted that RT-DETR's lack of NMS could be advantageous for group-entry scenarios where NMS can accidentally suppress overlapping bounding boxes for people walking close together.

### Final Choice: YOLOv8n (with configurable upgrade path to YOLOv8m)

I chose **YOLOv8n** as the default, with `YOLO_MODEL` configurable via environment variable. The rationale:

1. **CCTV is not real-time on CPU**: At 1080p 15fps with 15 clips to process, even YOLOv8n is computationally heavy without a GPU. The speed advantage of `n` over `m` is 2× on CPU.
2. **Occlusion is handled by confidence calibration, not model choice**: The spec requires emitting low-confidence detections, not suppressing them. Both models detect partial occlusions; the difference is marginal.
3. **Group entry works via ByteTrack**: NMS separating same-direction people is a solved problem in ByteTrack's BYTE algorithm, which processes low-confidence detections separately.
4. **Upgrade path preserved**: `YOLO_MODEL=yolov8m.pt` in `.env` switches to the larger model with zero code changes.

I partially agreed with the AI — `yolov8m` is better for production with a GPU. For the default containerised setup, `yolov8n` is the pragmatic choice.

---

## Decision 2 — Event Schema Design

### Options Considered

Three schema approaches were evaluated:

1. **Flat schema**: All fields at top level, no `metadata` sub-object. Simple but verbose.
2. **Spec-compliant schema** (chosen): `metadata` sub-object containing `queue_depth`, `sku_zone`, `session_seq`. Matches spec exactly.
3. **Minimal schema**: Only mandatory fields; extensions via `extra_data: dict`. Flexible but non-deterministic.

### Why UUIDv4 for event_id?

UUIDv4 provides global uniqueness without coordination between pipeline processes. If five parallel pipeline processes (one per store) generate events simultaneously, UUIDv4 guarantees no collision. Sequential integers would require a global sequence, creating a coordination bottleneck. The 36-character string overhead is negligible compared to the 1080p frame data being processed.

### Why session_seq in metadata?

The `session_seq` field tracks the ordinal position of each event within a visitor's session (1 = ENTRY, 2 = first ZONE_ENTER, etc.). It lives in `metadata` because:
- The spec explicitly shows it there
- It's not used for primary indexing (visitor_id + timestamp serves that purpose)
- It can be null for events generated outside the pipeline (manual test events)

The AI suggested promoting `session_seq` to the top level for easier querying. I overrode this because the spec schema is the contract, and deviating from it would break the automated scoring test suite.

### Why not visitor_id-only (no event_id)?

Early in design, I considered making `visitor_id + timestamp` the deduplication key. The AI correctly pointed out that two events for the same visitor at the same frame (e.g., ZONE_EXIT + ZONE_ENTER in the same frame) would collide. UUIDv4 event_id solves this without compromising idempotency (the ingest endpoint uses `ON CONFLICT DO NOTHING` on `event_id`).

---

## Decision 3 — API Architecture: SQLite vs PostgreSQL + Session State

### Options Considered

| Approach | Pros | Cons |
|---|---|---|
| **SQLite (aiosqlite)** | Zero setup, works in Docker out of box | No concurrent writes at scale, no replication |
| **PostgreSQL** | Production-grade, connection pooling, JSONB | Requires separate container, more setup |
| **In-memory session state** | Sub-millisecond read for current queue depth | Lost on restart, no persistence |
| **Redis cache** | Fast reads, pub/sub for WebSocket | Extra service, cache invalidation complexity |

### AI Suggestion

Claude Sonnet recommended **PostgreSQL with async SQLAlchemy** as the primary store, with **Redis** as an optional cache for the metrics endpoints. It argued that at 40 stores with real-time ingest, SQLite's write serialisation would become a bottleneck.

### Final Choice: Dual-mode (SQLite dev / PostgreSQL prod) + No Redis

I implemented **dual-mode storage** via `DATABASE_URL` environment variable:
- `sqlite+aiosqlite:///:memory:` for tests
- `sqlite+aiosqlite:///./store_intelligence.db` for local dev
- `postgresql+asyncpg://...` for production (used in docker-compose.yml)

**Why no Redis**: The metrics endpoints are real-time by spec ("not cached"). Adding Redis caching would require cache invalidation on every ingest, defeating the purpose. At 40 stores × 6 endpoints, the PostgreSQL query volume is trivially low — each endpoint executes 2–4 aggregation queries on a well-indexed table. The overhead of a Redis round-trip (network + serialisation) exceeds the DB query time for small datasets.

**Scale consideration**: If scaling to 400+ stores with 100+ events/second, Redis would be warranted for the metrics and heatmap endpoints. The architecture supports this addition without code changes — just add a Redis dependency and wrap the endpoint functions with a cache decorator.

I agreed with the AI's PostgreSQL recommendation for production and implemented it as the docker-compose default. I disagreed on Redis being necessary at this scale, prioritising operational simplicity over premature optimisation.

---

*Total word count: ~700 words*
