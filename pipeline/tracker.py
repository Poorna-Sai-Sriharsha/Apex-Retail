"""
Re-ID based tracker: cross-camera deduplication and re-entry detection.

Uses OSNet (via torchreid) to extract appearance embeddings per track.
Maintains a session store keyed by visitor_id.

Re-entry detection:
  - On new ENTRY event, compute cosine similarity against recent EXITs
  - If similarity > REID_THRESHOLD (0.75) AND time_gap < REENTRY_WINDOW (30 min)
    → emit REENTRY, reuse existing visitor_id
  - Otherwise → new visitor_id

Cross-camera deduplication:
  - Same logic: if a track in camera B has high similarity to active track in A
    → merge into same visitor_id (the person is already counted)

Graceful degradation:
  - If torch/torchreid not available, falls back to track-ID-only mode
  - Confidence is lowered for unverified Re-IDs
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np

# Attempt to import torch / torchreid — degrade gracefully if unavailable
try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    import torchreid
    TORCHREID_AVAILABLE = True
except ImportError:
    TORCHREID_AVAILABLE = False

from pipeline.emit import make_visitor_id

REID_THRESHOLD: float = float(os.getenv("REID_SIMILARITY_THRESHOLD", "0.75"))
REENTRY_WINDOW_MINUTES: int = int(os.getenv("REID_REENTRY_WINDOW_MINUTES", "30"))
EMBEDDING_DIM: int = 512  # OSNet output dim


# ---------------------------------------------------------------------------
# Embedding extractor
# ---------------------------------------------------------------------------

class OSNetExtractor:
    """
    Extracts appearance embeddings using OSNet (torchreid).
    Falls back to random embeddings if torchreid is unavailable.
    """

    def __init__(self) -> None:
        self.model = None
        self.available = False

        if TORCHREID_AVAILABLE and TORCH_AVAILABLE:
            try:
                self.model = torchreid.models.build_model(
                    name="osnet_x1_0",
                    num_classes=1000,
                    pretrained=True,
                )
                self.model.eval()
                if torch.cuda.is_available():
                    self.model = self.model.cuda()
                self.available = True
                print("[INFO] OSNet Re-ID model loaded successfully")
            except Exception as exc:
                print(f"[WARN] OSNet load failed: {exc}. Using random embeddings.")

    def extract(self, crop: Optional[np.ndarray]) -> np.ndarray:
        """
        Extract a 512-dim embedding from a BGR image crop.
        Returns zero vector if crop is None or model unavailable.
        Low confidence is NOT suppressed — it's passed through.
        """
        if crop is None or crop.size == 0:
            return np.zeros(EMBEDDING_DIM, dtype=np.float32)

        if not self.available or self.model is None:
            # Fallback: use colour histogram as pseudo-embedding
            return self._colour_histogram(crop)

        try:
            import cv2

            resized = cv2.resize(crop, (128, 256))
            tensor = torch.from_numpy(resized.transpose(2, 0, 1)).float() / 255.0
            tensor = tensor.unsqueeze(0)  # batch dim
            if torch.cuda.is_available():
                tensor = tensor.cuda()

            with torch.no_grad():
                emb = self.model(tensor)

            return emb.cpu().numpy().flatten().astype(np.float32)
        except Exception:
            return self._colour_histogram(crop)

    def _colour_histogram(self, crop: np.ndarray) -> np.ndarray:
        """Compute normalised colour histogram as fallback embedding."""
        try:
            import cv2
            hist = cv2.calcHist([crop], [0, 1, 2], None, [8, 8, 8], [0, 256] * 3)
            hist = hist.flatten().astype(np.float32)
            norm = np.linalg.norm(hist)
            if norm > 0:
                hist /= norm
            # Pad/truncate to EMBEDDING_DIM
            if len(hist) < EMBEDDING_DIM:
                hist = np.concatenate([hist, np.zeros(EMBEDDING_DIM - len(hist))])
            return hist[:EMBEDDING_DIM]
        except Exception:
            return np.zeros(EMBEDDING_DIM, dtype=np.float32)


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity in range [-1, 1], clamped to [0, 1]."""
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.clip(np.dot(a, b) / (na * nb), 0.0, 1.0))


# ---------------------------------------------------------------------------
# Session store
# ---------------------------------------------------------------------------

class VisitorSession:
    """Tracks state for a single visitor across cameras and re-entries."""

    def __init__(self, visitor_id: str) -> None:
        self.visitor_id = visitor_id
        self.embeddings: list[np.ndarray] = []
        self.last_seen: datetime = datetime.now(timezone.utc)
        self.last_exit: Optional[datetime] = None
        self.zones_visited: list[str] = []
        self.session_seq: int = 0
        self.is_staff: bool = False
        self.active_camera: Optional[str] = None
        self.entry_count: int = 0

    def add_embedding(self, emb: np.ndarray) -> None:
        """Keep last 10 embeddings per visitor (FIFO)."""
        self.embeddings.append(emb)
        if len(self.embeddings) > 10:
            self.embeddings.pop(0)

    def mean_embedding(self) -> Optional[np.ndarray]:
        """Return mean of stored embeddings."""
        if not self.embeddings:
            return None
        return np.mean(self.embeddings, axis=0)

    def next_seq(self) -> int:
        self.session_seq += 1
        return self.session_seq


class ReIDTracker:
    """
    Manages visitor sessions across cameras and re-entries.
    Maps (camera_id, local_track_id) → visitor_id.
    """

    def __init__(self, extractor: Optional[OSNetExtractor] = None) -> None:
        self.extractor = extractor or OSNetExtractor()
        # local key (camera_id, track_id) → visitor_id
        self._track_map: dict[tuple[str, int], str] = {}
        # visitor_id → VisitorSession
        self._sessions: dict[str, VisitorSession] = {}
        # visitor_ids that have exited (for re-entry matching)
        self._exited: list[str] = []

    def get_or_create_visitor(
        self,
        camera_id: str,
        track_id: int,
        crop: Optional[np.ndarray],
        current_time: datetime,
    ) -> tuple[str, bool, float]:
        """
        Returns (visitor_id, is_reentry, confidence).

        For a new track:
        1. Extract embedding from crop
        2. Search exited sessions for Re-ID match
        3. If match → REENTRY with existing visitor_id
        4. Else → new visitor_id
        """
        key = (camera_id, track_id)

        # Already known track
        if key in self._track_map:
            vid = self._track_map[key]
            session = self._sessions.get(vid)
            if session:
                session.last_seen = current_time
                new_emb = self.extractor.extract(crop)
                session.add_embedding(new_emb)
                return vid, False, 1.0
            else:
                # Session was cleaned up — create new
                return self._new_visitor(key, crop, current_time)

        # New track — check for Re-ID match
        new_emb = self.extractor.extract(crop)

        best_vid: Optional[str] = None
        best_sim: float = 0.0

        reentry_cutoff = current_time - timedelta(minutes=REENTRY_WINDOW_MINUTES)

        for vid in self._exited:
            session = self._sessions.get(vid)
            if session is None or session.last_exit is None:
                continue
            if session.last_exit < reentry_cutoff:
                continue  # Too long ago

            mean_emb = session.mean_embedding()
            if mean_emb is None:
                continue

            sim = cosine_similarity(new_emb, mean_emb)
            if sim > best_sim:
                best_sim = sim
                best_vid = vid

        if best_vid and best_sim >= REID_THRESHOLD:
            # Re-entry detected
            session = self._sessions[best_vid]
            session.add_embedding(new_emb)
            session.last_seen = current_time
            session.last_exit = None
            session.active_camera = camera_id
            session.entry_count += 1
            self._track_map[key] = best_vid
            self._exited = [v for v in self._exited if v != best_vid]
            return best_vid, True, round(best_sim, 4)

        # New visitor
        return self._new_visitor(key, crop, current_time, initial_emb=new_emb)

    def _new_visitor(
        self,
        key: tuple[str, int],
        crop: Optional[np.ndarray],
        current_time: datetime,
        initial_emb: Optional[np.ndarray] = None,
    ) -> tuple[str, bool, float]:
        vid = make_visitor_id()
        session = VisitorSession(vid)
        if initial_emb is not None:
            session.add_embedding(initial_emb)
        elif crop is not None:
            session.add_embedding(self.extractor.extract(crop))
        session.last_seen = current_time
        session.active_camera = key[0]
        session.entry_count = 1
        self._sessions[vid] = session
        self._track_map[key] = vid
        return vid, False, 1.0

    def record_exit(self, camera_id: str, track_id: int, exit_time: datetime) -> None:
        """Mark a track as exited — eligible for re-entry matching."""
        key = (camera_id, track_id)
        vid = self._track_map.pop(key, None)
        if vid and vid in self._sessions:
            self._sessions[vid].last_exit = exit_time
            if vid not in self._exited:
                self._exited.append(vid)

    def get_session(self, visitor_id: str) -> Optional[VisitorSession]:
        return self._sessions.get(visitor_id)

    def next_seq(self, visitor_id: str) -> int:
        session = self._sessions.get(visitor_id)
        return session.next_seq() if session else 0

    def mark_staff(self, visitor_id: str) -> None:
        session = self._sessions.get(visitor_id)
        if session:
            session.is_staff = True

    def is_cross_camera_duplicate(
        self,
        camera_id: str,
        track_id: int,
        crop: Optional[np.ndarray],
    ) -> Optional[str]:
        """
        Check if a track in camera B is already tracked in camera A.
        Returns existing visitor_id if match found, else None.
        """
        if crop is None:
            return None

        new_emb = self.extractor.extract(crop)

        for (cam, tid), vid in self._track_map.items():
            if cam == camera_id and tid == track_id:
                continue
            session = self._sessions.get(vid)
            if session is None:
                continue
            mean_emb = session.mean_embedding()
            if mean_emb is None:
                continue
            sim = cosine_similarity(new_emb, mean_emb)
            if sim >= REID_THRESHOLD:
                # Found existing visitor — link new track to same session
                self._track_map[(camera_id, track_id)] = vid
                return vid

        return None

    def cleanup_stale_sessions(self, max_age_minutes: int = 60) -> None:
        """Remove sessions not seen in max_age_minutes (memory management)."""
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
        stale = [
            vid for vid, s in self._sessions.items()
            if s.last_seen < cutoff and s.last_exit is not None
        ]
        for vid in stale:
            del self._sessions[vid]
            self._exited = [v for v in self._exited if v != vid]
