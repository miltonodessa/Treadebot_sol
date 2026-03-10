"""
velocity_analyzer.py — Real-Time Per-Token Signal Engine
=========================================================
Calibrated against 24,261 round-trip trades of wallet D5dtjf (97 days).

Key findings from historical data:
  • Pool SOL at entry ≥ 2.0 → baseline win rate +3%; ≥ 3.5 → 38%+ WR
  • Buy volume threshold raised to 2.0 SOL (was 1.0)
  • Pool growth >50% → 75% WR → hold; >100% → 93% WR → ride hard

Signals tracked:
  1. Slot Clustering    — 3+ buys in first 2 seconds (bundle detection)
  2. Liquidity Velocity — ≥ 2.0 SOL buy-volume in observation window
  3. Buy/Sell Pressure  — sell_vol / total_vol < 35%
  4. Unique Buyers      — ≥ 5 unique buyer wallets
  5. Sell Absorption    — price not dropping > 20% on early sells
  + pool SOL tracking for risk_manager momentum logic
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("velocity")

LAMPORTS_PER_SOL = 1_000_000_000


@dataclass
class TokenSignals:
    """Live signal state for a single token."""
    mint:       str
    symbol:     str
    creator:    str
    created_at: float   # unix timestamp

    # ── Signal 0: Pre-launch cluster (set by WalletClusterDetector) ───────
    cluster_ok: bool = False

    # ── Signal 4b: Creator reputation (set by CreatorReputation) ──────────
    reputation_ok:     bool = True   # True until checked (benefit of doubt)
    reputation_reason: str  = "pending"

    # ── Signal 1: Slot clustering (first 2 seconds) ───────────────────────
    early_buys:      list = field(default_factory=list)  # [(ts, wallet)]
    slot_cluster_ok: bool = False

    # ── Signal 2: Liquidity velocity ─────────────────────────────────────
    buy_vol_sol:  float = 0.0
    sell_vol_sol: float = 0.0

    # ── Signal 3: Unique buyers / sellers ────────────────────────────────
    buyer_wallets:  set = field(default_factory=set)
    seller_wallets: set = field(default_factory=set)

    # ── Signal 5: Price samples for sell absorption ───────────────────────
    first_price: float = 0.0
    last_price:  float = 0.0
    min_price:   float = float("inf")

    # ── Pool SOL tracking (vSolInBondingCurve from WS) ────────────────────
    # Used by risk_manager for momentum exit and pool-ride logic.
    # Data: pool ≥ 2.0 SOL at entry → improved WR; pool growth > 50% → WR 75%
    entry_pool_sol:   float = 0.0   # pool SOL at first observed trade
    current_pool_sol: float = 0.0   # latest pool SOL

    # ── State ─────────────────────────────────────────────────────────────
    entered:       bool = False
    rejected:      bool = False
    reject_reason: str  = ""

    # ── Runtime price (read by risk_manager) ──────────────────────────────
    current_price: float = 0.0

    # ── Computed properties ───────────────────────────────────────────────

    @property
    def age_sec(self) -> float:
        return time.time() - self.created_at

    @property
    def total_vol_sol(self) -> float:
        return self.buy_vol_sol + self.sell_vol_sol

    @property
    def sell_pressure(self) -> float:
        if self.total_vol_sol <= 0:
            return 0.0
        return self.sell_vol_sol / self.total_vol_sol

    @property
    def unique_buyers(self) -> int:
        return len(self.buyer_wallets)

    @property
    def pool_growth_pct(self) -> float:
        """
        Fractional growth of bonding curve SOL since our entry.
        Data: >0.50 → WR 75%; >1.00 → WR 93%.
        Returns 0.0 if no pool data available.
        """
        if self.entry_pool_sol <= 0 or self.current_pool_sol <= 0:
            return 0.0
        return (self.current_pool_sol - self.entry_pool_sol) / self.entry_pool_sol

    # ── Signal checks ─────────────────────────────────────────────────────

    @property
    def velocity_ok(self) -> bool:
        """≥ 2.0 SOL buy volume during observation window.
        Raised from 1.0 → 2.0 based on wallet data:
        pool < 2 SOL at entry → WR 26-28%; pool 2-3.5 SOL → 28-30%; ≥ 3.5 → 38%+
        """
        return self.buy_vol_sol >= 2.0

    @property
    def velocity_strong(self) -> bool:
        """≥ 3.5 SOL — high-conviction signal (38%+ WR zone)."""
        return self.buy_vol_sol >= 3.5

    @property
    def sell_pressure_ok(self) -> bool:
        return self.sell_pressure < 0.35

    @property
    def unique_buyers_ok(self) -> bool:
        return self.unique_buyers >= 5

    @property
    def sell_absorption_ok(self) -> bool:
        """Price not dropped > 20% from first trade price despite early sells."""
        if self.first_price <= 0 or self.min_price == float("inf"):
            return True
        if self.sell_vol_sol == 0:
            return True
        drop = (self.first_price - self.min_price) / self.first_price
        return drop < 0.20

    @property
    def entry_window_ok(self) -> bool:
        """12–35 seconds after token creation."""
        age = self.age_sec
        return 12.0 <= age <= 35.0

    def all_signals_ok(self) -> tuple[bool, str]:
        """
        Returns (can_enter, reason).
        ALL conditions must be True simultaneously.
        """
        checks = [
            (self.cluster_ok,          "no_funding_cluster"),
            (self.reputation_ok,       f"bad_creator_{self.reputation_reason}"),
            (self.slot_cluster_ok,     "no_slot_bundle"),
            (self.velocity_ok,         f"low_velocity_{self.buy_vol_sol:.2f}SOL"),
            (self.sell_pressure_ok,    f"sell_pressure_{self.sell_pressure:.0%}"),
            (self.unique_buyers_ok,    f"only_{self.unique_buyers}_buyers_need_5"),
            (self.sell_absorption_ok,  "price_drop_rejected"),
            (self.entry_window_ok,     f"outside_window_{self.age_sec:.0f}s"),
        ]
        for ok, reason in checks:
            if not ok:
                return False, reason
        return True, "ok"

    def summary(self) -> str:
        pool_str = f"pool={self.entry_pool_sol:.1f}SOL" if self.entry_pool_sol > 0 else ""
        return (
            f"{self.symbol} age={self.age_sec:.0f}s "
            f"vel={self.buy_vol_sol:.2f}SOL "
            f"sell_p={self.sell_pressure:.0%} "
            f"buyers={self.unique_buyers} "
            f"cluster={self.cluster_ok} "
            f"slot={self.slot_cluster_ok} "
            f"{pool_str}"
        )


# ─────────────────────────────────────────────────────────────────────────────


class VelocityAnalyzer:
    """
    Registry of TokenSignals, updated by the WS event stream.
    Also read by RiskManager for sell pressure and pool momentum on open positions.
    """

    def __init__(self, max_tracked: int = 2000):
        self._tokens: dict[str, TokenSignals] = {}
        self._max = max_tracked

    # ── Public API ────────────────────────────────────────────────────────

    def on_create(
        self,
        mint: str,
        symbol: str,
        creator: str,
        created_at: float,
    ) -> TokenSignals:
        sig = TokenSignals(
            mint=mint, symbol=symbol,
            creator=creator, created_at=created_at,
        )
        self._tokens[mint] = sig
        self._maybe_evict()
        return sig

    def on_trade(self, mint: str, msg: dict) -> Optional[TokenSignals]:
        """
        Process a pump.fun trade WS event.
        Extracts: txType, solAmount, tokenAmount, traderPublicKey,
                  vSolInBondingCurve (pool SOL tracking).
        """
        sig = self._tokens.get(mint)
        if sig is None or sig.rejected:
            return None

        is_buy  = msg.get("txType") == "buy"
        wallet  = msg.get("traderPublicKey", "")
        sol_amt = msg.get("solAmount", 0) / LAMPORTS_PER_SOL
        tok_amt = msg.get("tokenAmount", 1) or 1

        # Pool SOL tracking (vSolInBondingCurve — pumportal WS field)
        pool_sol = msg.get("vSolInBondingCurve", 0) / LAMPORTS_PER_SOL
        if pool_sol > 0:
            if sig.entry_pool_sol == 0:
                sig.entry_pool_sol = pool_sol
            sig.current_pool_sol = pool_sol

        # Price update
        if sol_amt > 0 and tok_amt > 0:
            price = sol_amt / tok_amt
            if sig.first_price == 0:
                sig.first_price = price
            sig.last_price    = price
            sig.current_price = price
            if price < sig.min_price:
                sig.min_price = price

        if is_buy:
            sig.buy_vol_sol += sol_amt
            if wallet:
                sig.buyer_wallets.add(wallet)

            # Signal 1: slot clustering — 3+ different wallets within first 2s
            age = sig.age_sec
            if age <= 2.0 and wallet:
                sig.early_buys.append((time.time(), wallet))
                unique_early = {w for _, w in sig.early_buys}
                if len(unique_early) >= 3 and not sig.slot_cluster_ok:
                    sig.slot_cluster_ok = True
                    log.debug("📦 slot cluster: %s — %d wallets in %.1fs",
                              sig.symbol, len(unique_early), age)

        else:  # sell
            sig.sell_vol_sol += sol_amt
            if wallet:
                sig.seller_wallets.add(wallet)

            # Signal 5: reject immediately if price drops > 20% on early sells
            if not sig.sell_absorption_ok:
                sig.rejected = True
                sig.reject_reason = "sell_absorption_failed"
                drop = (sig.first_price - sig.min_price) / sig.first_price * 100
                log.debug("❌ %s: rejected — price drop %.1f%% on early sell",
                          sig.symbol, drop)

        return sig

    def get(self, mint: str) -> Optional[TokenSignals]:
        return self._tokens.get(mint)

    def sell_pressure_of(self, mint: str) -> float:
        sig = self._tokens.get(mint)
        return sig.sell_pressure if sig else 0.0

    def current_price_of(self, mint: str) -> float:
        sig = self._tokens.get(mint)
        return sig.current_price if sig else 0.0

    def pool_growth_of(self, mint: str) -> float:
        """Pool SOL growth fraction since position entry. 0 if no data."""
        sig = self._tokens.get(mint)
        return sig.pool_growth_pct if sig else 0.0

    def pool_sol_of(self, mint: str) -> tuple[float, float]:
        """Returns (entry_pool_sol, current_pool_sol)."""
        sig = self._tokens.get(mint)
        if sig:
            return sig.entry_pool_sol, sig.current_pool_sol
        return 0.0, 0.0

    def cleanup(self, max_age_sec: float = 60.0):
        """Evict tokens past their entry window that were never entered."""
        now = time.time()
        dead = [
            m for m, s in list(self._tokens.items())
            if not s.entered and now - s.created_at > max_age_sec
        ]
        for m in dead:
            del self._tokens[m]

    # ── Internal ──────────────────────────────────────────────────────────

    def _maybe_evict(self):
        if len(self._tokens) > self._max:
            cutoff = sorted(
                self._tokens, key=lambda m: self._tokens[m].created_at
            )[: self._max // 10]
            for m in cutoff:
                del self._tokens[m]
