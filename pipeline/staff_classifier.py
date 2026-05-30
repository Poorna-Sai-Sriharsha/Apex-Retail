"""
Staff classifier using two ensemble approaches:

1. Colour-based: HSV range matching for staff uniform colours.
   Extracts dominant colour from the bounding box crop; checks if it falls
   within the configured HSV range from store_layout.json.

2. Movement-based: Tracks zone visit frequency. Staff traverse all zones
   regularly → flag visitors who appear in N distinct zones above threshold.

Ensemble: either signal alone flags the track as staff.
"""

from __future__ import annotations

from typing import Optional

import numpy as np

# Optional OpenCV import — fallback if not installed
try:
    import cv2
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False


# ---------------------------------------------------------------------------
# Colour-based Staff Classifier
# ---------------------------------------------------------------------------

class ColourClassifier:
    """
    Checks if the dominant HSV colour of a bounding box crop matches the
    configured staff uniform colour range.
    """

    def __init__(
        self,
        hue_low: int = 100,
        hue_high: int = 130,
        sat_low: int = 50,
        val_low: int = 50,
    ) -> None:
        self.hue_low = hue_low
        self.hue_high = hue_high
        self.sat_low = sat_low
        self.val_low = val_low

    def _get_dominant_hsv(self, crop: np.ndarray) -> Optional[tuple[float, float, float]]:
        """Return median HSV of the upper 60% of the crop (torso area)."""
        if not CV2_AVAILABLE or crop is None or crop.size == 0:
            return None
        h_crop = int(crop.shape[0] * 0.6)
        torso = crop[:h_crop]
        if torso.size == 0:
            return None
        hsv = cv2.cvtColor(torso, cv2.COLOR_BGR2HSV)
        # Use median to avoid outliers
        h_med = float(np.median(hsv[:, :, 0]))
        s_med = float(np.median(hsv[:, :, 1]))
        v_med = float(np.median(hsv[:, :, 2]))
        return h_med, s_med, v_med

    def is_staff_colour(self, crop: np.ndarray) -> tuple[bool, float]:
        """
        Returns (is_staff, confidence).
        confidence is 0.0 if CV2 unavailable (cannot classify).
        """
        if not CV2_AVAILABLE or crop is None or crop.size == 0:
            return False, 0.0

        hsv = self._get_dominant_hsv(crop)
        if hsv is None:
            return False, 0.0

        h, s, v = hsv
        in_range = (
            self.hue_low <= h <= self.hue_high
            and s >= self.sat_low
            and v >= self.val_low
        )

        # Confidence: how deep inside the range
        if in_range:
            hue_range = max(1, self.hue_high - self.hue_low)
            hue_center = (self.hue_low + self.hue_high) / 2
            hue_dist = abs(h - hue_center) / (hue_range / 2)
            confidence = float(1.0 - hue_dist * 0.3)
        else:
            confidence = 0.0

        return in_range, round(confidence, 4)


# ---------------------------------------------------------------------------
# Movement-based Staff Classifier
# ---------------------------------------------------------------------------

class MovementClassifier:
    """
    Tracks which zones each visitor_id has visited. Staff typically visit
    ALL zones (restocking, assistance) at high frequency.
    Flags visitors who exceed ZONE_COUNT_THRESHOLD distinct zones.
    """

    ZONE_COUNT_THRESHOLD = 4      # Staff visit ≥4 distinct zones
    VISIT_COUNT_THRESHOLD = 10    # Staff make ≥10 total zone visits

    def __init__(self) -> None:
        # visitor_id → {zone_id → count}
        self._zone_visits: dict[str, dict[str, int]] = {}

    def record_zone_visit(self, visitor_id: str, zone_id: str) -> None:
        """Record a zone visit for a visitor."""
        if visitor_id not in self._zone_visits:
            self._zone_visits[visitor_id] = {}
        self._zone_visits[visitor_id][zone_id] = (
            self._zone_visits[visitor_id].get(zone_id, 0) + 1
        )

    def is_staff_movement(self, visitor_id: str) -> tuple[bool, float]:
        """
        Returns (is_staff, confidence) based on zone diversity.
        """
        visits = self._zone_visits.get(visitor_id, {})
        distinct_zones = len(visits)
        total_visits = sum(visits.values())

        is_staff = (
            distinct_zones >= self.ZONE_COUNT_THRESHOLD
            and total_visits >= self.VISIT_COUNT_THRESHOLD
        )

        if is_staff:
            confidence = min(
                1.0,
                0.5 + (distinct_zones - self.ZONE_COUNT_THRESHOLD) * 0.1
            )
        else:
            confidence = 0.0

        return is_staff, round(confidence, 4)

    def reset(self, visitor_id: str) -> None:
        """Clear state for a visitor (e.g., after exit)."""
        self._zone_visits.pop(visitor_id, None)


# ---------------------------------------------------------------------------
# Ensemble Classifier
# ---------------------------------------------------------------------------

class StaffClassifier:
    """
    Ensemble of colour + movement classifiers.
    Either signal flags the visitor as staff (OR logic, conservative).
    Provides per-visitor confidence score.
    """

    def __init__(self, hsv_config: Optional[dict] = None) -> None:
        cfg = hsv_config or {}
        self.colour_clf = ColourClassifier(
            hue_low=cfg.get("hue_low", 100),
            hue_high=cfg.get("hue_high", 130),
            sat_low=cfg.get("sat_low", 50),
            val_low=cfg.get("val_low", 50),
        )
        self.movement_clf = MovementClassifier()

        # visitor_id → (is_staff, confidence, reason)
        self._decisions: dict[str, tuple[bool, float, str]] = {}

    def update_colour(
        self, visitor_id: str, crop: Optional[np.ndarray]
    ) -> None:
        """
        Check colour classification for a frame crop.
        If staff detected, store the decision (sticky — won't unflag).
        """
        if crop is None:
            return
        is_staff, conf = self.colour_clf.is_staff_colour(crop)
        if is_staff:
            self._decisions[visitor_id] = (True, conf, "colour")

    def update_movement(self, visitor_id: str, zone_id: str) -> None:
        """Record zone visit and re-evaluate movement classification."""
        self.movement_clf.record_zone_visit(visitor_id, zone_id)
        is_staff, conf = self.movement_clf.is_staff_movement(visitor_id)
        if is_staff:
            current = self._decisions.get(visitor_id, (False, 0.0, ""))
            if not current[0]:
                self._decisions[visitor_id] = (True, conf, "movement")

    def is_staff(self, visitor_id: str) -> bool:
        """Returns True if visitor has been classified as staff."""
        return self._decisions.get(visitor_id, (False,))[0]

    def get_confidence(self, visitor_id: str) -> float:
        """Returns staff classification confidence (0.0 if customer)."""
        return self._decisions.get(visitor_id, (False, 0.0))[1]

    def reset(self, visitor_id: str) -> None:
        """Clear staff state for visitor (call on EXIT)."""
        self._decisions.pop(visitor_id, None)
        self.movement_clf.reset(visitor_id)

    def get_all_staff_ids(self) -> set[str]:
        """Return all visitor_ids currently classified as staff."""
        return {vid for vid, (is_staff, _, __) in self._decisions.items() if is_staff}
