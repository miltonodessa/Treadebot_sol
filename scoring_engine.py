"""
scoring_engine.py — Probability Scoring Model
==============================================
Combines all normalized features from FeatureVector into a single
0–1 entry probability score.

Weight rationale (derived from D5dtjf wallet analysis):
  • slot_compression  (0.35) — strongest signal; 90% coordinated entry
  • funding_cluster   (0.20) — pre-launch Helius cluster
  • sell_pressure_inv (0.15) — clean buy-side = healthier token
  • buy_volume        (0.15) — pool ≥ 2 SOL → +3% WR
  • creator_rep       (0.10) — bad creator → very high rug rate
  • wallet_coord      (0.05) — adds marginal signal when graph is populated

Decision threshold: score ≥ 0.65 → entry recommended.

Can be used in two ways:
  1. Hard gate: replace all_signals_ok() boolean with score >= threshold
  2. Soft advisory: log score alongside existing signal check (default)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict

from feature_extractor import FeatureVector

log = logging.getLogger("scoring")


# Default weights — must sum to 1.0
DEFAULT_WEIGHTS: Dict[str, float] = {
    "slot_compression":   0.35,
    "funding_cluster":    0.20,
    "sell_pressure_inv":  0.15,
    "buy_volume_norm":    0.15,
    "creator_reputation": 0.10,
    "wallet_coordination": 0.05,
}

DEFAULT_THRESHOLD = 0.65


@dataclass
class ScoreResult:
    score:          float
    threshold:      float
    recommend_entry: bool
    contributions: Dict[str, float]  # feature → weighted contribution

    def summary(self) -> str:
        top = sorted(self.contributions.items(), key=lambda x: -x[1])
        parts = "  ".join(f"{k}={v:.2f}" for k, v in top[:4])
        return (
            f"score={self.score:.3f}  "
            f"{'✅ ENTRY' if self.recommend_entry else '⛔ skip'}  "
            f"[{parts}]"
        )


class ScoringEngine:
    """
    Weighted linear scoring model.

    Usage:
        engine = ScoringEngine()
        result = engine.score(feature_vector)
        if result.recommend_entry:
            ...
    """

    def __init__(
        self,
        weights: Dict[str, float] = None,
        threshold: float = DEFAULT_THRESHOLD,
    ):
        self.weights   = weights or dict(DEFAULT_WEIGHTS)
        self.threshold = threshold
        self._validate_weights()

    def score(self, fv: FeatureVector) -> ScoreResult:
        """
        Compute weighted score for a FeatureVector.
        Updates fv.composite_score in-place.
        """
        feature_values = {
            "slot_compression":    fv.slot_compression,
            "funding_cluster":     fv.funding_cluster,
            "sell_pressure_inv":   fv.sell_pressure_inv,
            "buy_volume_norm":     fv.buy_volume_norm,
            "creator_reputation":  fv.creator_reputation,
            "wallet_coordination": fv.wallet_coordination,
        }

        contributions = {}
        total = 0.0
        for feat, weight in self.weights.items():
            val = feature_values.get(feat, 0.0)
            contrib = weight * val
            contributions[feat] = contrib
            total += contrib

        total = round(min(total, 1.0), 4)
        fv.composite_score = total

        result = ScoreResult(
            score=total,
            threshold=self.threshold,
            recommend_entry=(total >= self.threshold),
            contributions=contributions,
        )

        log.debug("SCORE %-10s  %s", fv.symbol, result.summary())
        return result

    def score_from_signals(
        self,
        slot_compression: float  = 0.0,
        funding_cluster: float   = 0.0,
        sell_pressure: float     = 0.0,   # raw sell pressure (0–1); inverted internally
        buy_vol_sol: float       = 0.0,
        creator_ok: bool         = True,
        wallet_coord: float      = 0.0,
        buy_vol_cap: float       = 5.0,
    ) -> ScoreResult:
        """
        Convenience: build a minimal FeatureVector from raw signal values
        and score it. Useful for backtesting without full infrastructure.
        """
        from feature_extractor import FeatureVector
        fv = FeatureVector(
            mint="",
            symbol="",
            age_sec=0.0,
            slot_compression=slot_compression,
            funding_cluster=funding_cluster,
            sell_pressure_inv=max(1.0 - sell_pressure / 0.50, 0.0),
            buy_volume_norm=min(buy_vol_sol / buy_vol_cap, 1.0),
            creator_reputation=1.0 if creator_ok else 0.0,
            wallet_coordination=wallet_coord,
        )
        return self.score(fv)

    # ── Private ───────────────────────────────────────────────────────────

    def _validate_weights(self):
        total = sum(self.weights.values())
        if abs(total - 1.0) > 0.01:
            log.warning(
                "ScoringEngine weights sum to %.3f (expected 1.0) — normalizing",
                total
            )
            self.weights = {k: v / total for k, v in self.weights.items()}
