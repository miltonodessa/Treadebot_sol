"""
feature_extractor.py — Signal Aggregator / Feature Vector Builder
==================================================================
Aggregates all per-token signals into a normalized FeatureVector
that can be consumed by the ScoringEngine or logged for analysis.

Feature sources:
  • VelocityAnalyzer   — volume, buyers, sell pressure, price
  • SlotAnalyzer       — slot compression score
  • WalletClusterDetector / TokenSignals.cluster_ok
  • CreatorReputation  / TokenSignals.reputation_ok
  • WalletGraph        — wallet coordination score (optional)

All features are normalized to [0, 1] unless explicitly noted.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("features")

LAMPORTS_PER_SOL = 1_000_000_000


@dataclass
class FeatureVector:
    """
    Normalized feature vector for a single token at evaluation time.
    All float fields are in [0, 1] unless stated otherwise.
    """
    mint:    str
    symbol:  str
    age_sec: float   # seconds since token creation (NOT normalized)

    # ── Market features ───────────────────────────────────────────────────
    # Normalized buy volume: 0 SOL → 0.0,  ≥ 5 SOL → 1.0
    buy_volume_norm:      float = 0.0

    # Pool SOL at observation time: 0 → 0.0, ≥ 10 SOL → 1.0
    pool_sol_norm:        float = 0.0

    # ── Buyer quality features ────────────────────────────────────────────
    # Unique buyer count: 0 → 0.0, ≥ 20 → 1.0
    unique_buyers_norm:   float = 0.0

    # Buyer arrival rate: buyers-per-second in the first 30s: 0 → 0, ≥ 1/s → 1.0
    buyer_arrival_rate:   float = 0.0

    # ── Bundle / coordination features ───────────────────────────────────
    # From SlotAnalyzer (0.0–1.0 slot compression score)
    slot_compression:     float = 0.0

    # From WalletClusterDetector (1.0 if cluster detected, else 0.0)
    funding_cluster:      float = 0.0

    # From WalletGraph (0.0–1.0 coordination score)
    wallet_coordination:  float = 0.0

    # ── Risk features ─────────────────────────────────────────────────────
    # Inverted sell pressure: 0% sell → 1.0,  ≥ 50% sell → 0.0
    sell_pressure_inv:    float = 1.0

    # Creator reputation: 1.0 = trusted / new, 0.0 = bad history
    creator_reputation:   float = 1.0

    # ── Derived composite ─────────────────────────────────────────────────
    # Raw composite score (filled by ScoringEngine)
    composite_score:      float = 0.0

    # ── Raw values (for logging/debugging) ───────────────────────────────
    raw_buy_vol_sol:      float = 0.0
    raw_pool_sol:         float = 0.0
    raw_unique_buyers:    int   = 0
    raw_sell_pressure:    float = 0.0
    raw_slot_score:       float = 0.0
    captured_at:          float = field(default_factory=time.time)

    def as_dict(self) -> dict:
        return {
            "mint":               self.mint,
            "symbol":             self.symbol,
            "age_sec":            round(self.age_sec, 1),
            "buy_volume_norm":    round(self.buy_volume_norm,    3),
            "pool_sol_norm":      round(self.pool_sol_norm,      3),
            "unique_buyers_norm": round(self.unique_buyers_norm, 3),
            "buyer_arrival_rate": round(self.buyer_arrival_rate, 3),
            "slot_compression":   round(self.slot_compression,   3),
            "funding_cluster":    round(self.funding_cluster,    3),
            "wallet_coordination":round(self.wallet_coordination,3),
            "sell_pressure_inv":  round(self.sell_pressure_inv,  3),
            "creator_reputation": round(self.creator_reputation, 3),
            "composite_score":    round(self.composite_score,    3),
            # raw
            "raw_buy_vol_sol":    round(self.raw_buy_vol_sol,    3),
            "raw_pool_sol":       round(self.raw_pool_sol,       3),
            "raw_unique_buyers":  self.raw_unique_buyers,
            "raw_sell_pressure":  round(self.raw_sell_pressure,  3),
            "raw_slot_score":     round(self.raw_slot_score,     3),
        }


class FeatureExtractor:
    """
    Builds FeatureVector instances by pulling data from all signal
    sources registered with this extractor.

    Usage:
        fe = FeatureExtractor(
            velocity_analyzer=va,
            slot_analyzer=sa,           # optional
            wallet_graph=wg,            # optional
        )
        fv = fe.extract(mint)
    """

    def __init__(
        self,
        velocity_analyzer,           # VelocityAnalyzer
        slot_analyzer=None,          # SlotAnalyzer | None
        wallet_graph=None,           # WalletGraph  | None
        # Normalization caps
        buy_vol_cap: float   = 5.0,  # SOL: ≥ cap → 1.0
        pool_sol_cap: float  = 10.0, # SOL: ≥ cap → 1.0
        buyers_cap:   int    = 20,   # wallets: ≥ cap → 1.0
        arrival_cap:  float  = 1.0,  # buyers/sec: ≥ cap → 1.0
    ):
        self.va           = velocity_analyzer
        self.sa           = slot_analyzer
        self.wg           = wallet_graph
        self.buy_vol_cap  = buy_vol_cap
        self.pool_sol_cap = pool_sol_cap
        self.buyers_cap   = buyers_cap
        self.arrival_cap  = arrival_cap

    def extract(self, mint: str) -> Optional[FeatureVector]:
        """
        Extract and return a FeatureVector for the given mint.
        Returns None if the token is not tracked.
        """
        sig = self.va.get(mint)
        if sig is None:
            return None

        age = sig.age_sec

        # ── Market features ───────────────────────────────────────────────
        buy_vol_norm  = min(sig.buy_vol_sol  / self.buy_vol_cap,  1.0)
        pool_sol_norm = min(sig.current_pool_sol / self.pool_sol_cap, 1.0) \
                        if sig.current_pool_sol > 0 else 0.0

        # ── Buyer features ────────────────────────────────────────────────
        n_buyers          = sig.unique_buyers
        buyers_norm       = min(n_buyers / self.buyers_cap, 1.0)
        obs_window        = max(min(age, 30.0), 1.0)
        arrival_rate      = min((n_buyers / obs_window) / self.arrival_cap, 1.0)

        # ── Sell pressure (inverted) ──────────────────────────────────────
        sp     = sig.sell_pressure          # 0..1 fraction
        sp_inv = max(1.0 - sp / 0.50, 0.0) # 0% sell→1.0; ≥50% sell→0.0

        # ── Funding cluster ───────────────────────────────────────────────
        funding = 1.0 if sig.cluster_ok else 0.0

        # ── Creator reputation ────────────────────────────────────────────
        creator_rep = 1.0 if sig.reputation_ok else 0.0

        # ── Slot compression ──────────────────────────────────────────────
        slot_score = 0.0
        if self.sa is not None:
            slot_score = self.sa.slot_compression_score_of(mint)
        elif sig.slot_cluster_ok:
            # Fallback: slot_cluster_ok from velocity_analyzer (3 wallets in 2s)
            slot_score = 0.6

        # ── Wallet coordination ───────────────────────────────────────────
        wallet_coord = 0.0
        if self.wg is not None:
            wallet_coord = self.wg.coordination_score(sig.buyer_wallets)

        fv = FeatureVector(
            mint=mint,
            symbol=sig.symbol,
            age_sec=age,
            buy_volume_norm=buy_vol_norm,
            pool_sol_norm=pool_sol_norm,
            unique_buyers_norm=buyers_norm,
            buyer_arrival_rate=arrival_rate,
            slot_compression=slot_score,
            funding_cluster=funding,
            wallet_coordination=wallet_coord,
            sell_pressure_inv=sp_inv,
            creator_reputation=creator_rep,
            # raw
            raw_buy_vol_sol=sig.buy_vol_sol,
            raw_pool_sol=sig.current_pool_sol,
            raw_unique_buyers=n_buyers,
            raw_sell_pressure=sp,
            raw_slot_score=slot_score,
            captured_at=time.time(),
        )

        log.debug(
            "FV %-10s  slot=%.2f  cluster=%.0f  sp_inv=%.2f  "
            "vol=%.2f  buyers=%d  coord=%.2f",
            sig.symbol,
            slot_score, funding, sp_inv,
            buy_vol_norm, n_buyers, wallet_coord,
        )

        return fv
