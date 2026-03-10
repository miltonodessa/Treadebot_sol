"""
velocity_analyzer.py — Real-Time Per-Token Signal Engine
=========================================================
Отслеживает 5 сигналов для каждого токена:

  1. Slot Clustering    — 3+ buy-транзакций за первые 2 секунды (bundle)
  2. Liquidity Velocity — суммарный SOL buy-объём > 1 SOL за 20–40 секунд
  3. Buy/Sell Pressure  — доля sell volume < 35%
  4. Unique Buyers      — ≥ 10 уникальных кошельков за 40 секунд
  5. Sell Absorption    — ранние продажи не роняют цену > 20%

Дополнительно маркируется результат WalletClusterDetector (signal 0).
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
    """Живое состояние сигналов по конкретному токену."""
    mint:       str
    symbol:     str
    creator:    str
    created_at: float   # unix timestamp

    # ── Signal 0: Pre-launch cluster (заполняется WalletClusterDetector) ──
    cluster_ok: bool = False

    # ── Signal 1: Slot clustering (первые 2 секунды) ──────────────────────
    early_buys: list = field(default_factory=list)   # [(ts, wallet)]
    slot_cluster_ok: bool = False

    # ── Signal 2: Liquidity velocity ──────────────────────────────────────
    buy_vol_sol:  float = 0.0
    sell_vol_sol: float = 0.0

    # ── Signal 3: Unique buyers / sellers ─────────────────────────────────
    buyer_wallets:  set = field(default_factory=set)
    seller_wallets: set = field(default_factory=set)

    # ── Signal 5: Price samples for sell absorption ───────────────────────
    first_price: float = 0.0
    last_price:  float = 0.0
    min_price:   float = float("inf")

    # ── State ─────────────────────────────────────────────────────────────
    entered:       bool = False
    rejected:      bool = False
    reject_reason: str  = ""

    # ── Runtime price (for risk_manager sell absorption on positions) ──────
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

    # Signal checks ────────────────────────────────────────────────────────

    @property
    def velocity_ok(self) -> bool:
        return self.buy_vol_sol >= 1.0

    @property
    def velocity_strong(self) -> bool:
        return self.buy_vol_sol >= 3.0

    @property
    def sell_pressure_ok(self) -> bool:
        return self.sell_pressure < 0.35

    @property
    def unique_buyers_ok(self) -> bool:
        return self.unique_buyers >= 5

    @property
    def sell_absorption_ok(self) -> bool:
        """Price hasn't dropped > 20% from first trade price despite sells."""
        if self.first_price <= 0 or self.min_price == float("inf"):
            return True  # no price data — assume ok
        if self.sell_vol_sol == 0:
            return True  # no sells — absorption N/A
        drop = (self.first_price - self.min_price) / self.first_price
        return drop < 0.20

    @property
    def entry_window_ok(self) -> bool:
        """Must be 12–35 seconds after token creation."""
        age = self.age_sec
        return 12.0 <= age <= 35.0

    def all_signals_ok(self) -> tuple[bool, str]:
        """
        Returns (can_enter, reason).
        ALL six conditions must be True simultaneously.
        """
        checks = [
            (self.cluster_ok,          "no_funding_cluster"),
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
        return (
            f"{self.symbol} age={self.age_sec:.0f}s "
            f"vel={self.buy_vol_sol:.2f}SOL "
            f"sell_p={self.sell_pressure:.0%} "
            f"buyers={self.unique_buyers} "
            f"cluster={self.cluster_ok} "
            f"slot={self.slot_cluster_ok}"
        )


# ─────────────────────────────────────────────────────────────────────────────


class VelocityAnalyzer:
    """
    Реестр TokenSignals.
    Обновляется потоком WS-событий из TokenScanner.
    Читается RiskManager для отслеживания sell pressure на открытых позициях.
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
        """Register new token. Returns its TokenSignals object."""
        sig = TokenSignals(
            mint=mint,
            symbol=symbol,
            creator=creator,
            created_at=created_at,
        )
        self._tokens[mint] = sig
        self._maybe_evict()
        return sig

    def on_trade(self, mint: str, msg: dict) -> Optional[TokenSignals]:
        """
        Process a pump.fun trade WS event.
        Updates velocity, pressure, unique buyers, price.
        Returns the TokenSignals object (or None if not tracked).
        """
        sig = self._tokens.get(mint)
        if sig is None or sig.rejected:
            return None

        is_buy  = msg.get("txType") == "buy"
        wallet  = msg.get("traderPublicKey", "")
        sol_amt = msg.get("solAmount", 0) / LAMPORTS_PER_SOL
        tok_amt = msg.get("tokenAmount", 1) or 1

        # Price update
        if sol_amt > 0 and tok_amt > 0:
            price = sol_amt / tok_amt
            if sig.first_price == 0:
                sig.first_price = price
            sig.last_price = price
            sig.current_price = price
            if price < sig.min_price:
                sig.min_price = price

        if is_buy:
            sig.buy_vol_sol += sol_amt
            if wallet:
                sig.buyer_wallets.add(wallet)

            # Signal 1: slot clustering — 3+ different wallets in first 2 seconds
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

            # Signal 5: sell absorption — immediate rejection if price crashes
            if not sig.sell_absorption_ok:
                sig.rejected = True
                sig.reject_reason = "sell_absorption_failed"
                drop = (sig.first_price - sig.min_price) / sig.first_price * 100
                log.debug("❌ %s: rejected — price drop %.1f%% on sell",
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
            # Remove oldest 10 %
            cutoff = sorted(
                self._tokens, key=lambda m: self._tokens[m].created_at
            )[: self._max // 10]
            for m in cutoff:
                del self._tokens[m]
