"""
slot_analyzer.py — Slot Compression Signal Engine
===================================================
CRITICAL SIGNAL: Slot Compression

Evidence from high-performance wallet analysis:
  • 4–6 buys in 1 slot           → ~90% coordinated entry signal
  • 2 slots within ≤ 500ms       → ~90% coordinated entry signal
  These tokens pump far more frequently than average.

Why this works:
  Regular retail buyers arrive spread across many slots (1 slot ≈ 400ms).
  Coordinated/bundled launches hit the chain in the same slot or consecutive
  slots because they were submitted as Jito bundles or co-signed transactions.
  Normal users cannot accidentally land in the same slot — only coordination
  or bundle infrastructure achieves this.

Detection modes:
  Mode A — Single-slot concentration:
    ≥ SLOT_COMPRESS_MIN_WALLETS (default 4) unique wallets in ONE slot.

  Mode B — Rapid slot sequence:
    Buys across ≥ 2 different slots where the wall-clock delta ≤ 500ms.
    (Handles the case where the bundle spans 2 consecutive slots.)

Output:
  SlotData per token:
    .slot_compression     bool   — True if Mode A or Mode B triggered
    .max_slot_wallets     int    — max unique wallets in any single slot
    .rapid_sequence       bool   — Mode B flag
    .slot_compression_score float — 0.0–1.0 (used by scoring_engine)
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("slot")

# Defaults (overridden by SlotAnalyzer constructor)
_COMPRESS_MIN_WALLETS = 4      # wallets in 1 slot for Mode A
_RAPID_WINDOW_MS      = 500    # ms for Mode B
_MAX_TRACK_SEC        = 60.0   # stop tracking after entry window closes


@dataclass
class SlotData:
    """Per-token slot compression state."""
    mint:       str
    symbol:     str
    created_at: float

    # slot → [(wallet, wall_clock_ts)]
    slot_buys: dict = field(default_factory=lambda: defaultdict(list))

    # Derived — updated lazily on every new trade
    max_slot_wallets:        int   = 0
    rapid_sequence:          bool  = False
    slot_compression:        bool  = False
    slot_compression_score:  float = 0.0

    # Track first/last wall-clock times per slot for Mode B
    slot_first_ts: dict = field(default_factory=dict)   # slot → first wall_ts
    slot_order: list    = field(default_factory=list)   # ordered list of seen slots

    def on_buy(self, slot: int, wallet: str, wall_ts: float):
        """Record a buy. Re-evaluate compression signals."""
        if wallet:
            self.slot_buys[slot].append((wallet, wall_ts))

        # Track slot order (for Mode B timing)
        if slot not in self.slot_first_ts:
            self.slot_first_ts[slot] = wall_ts
            self.slot_order.append(slot)

        self._evaluate()

    def _evaluate(self):
        # ── Mode A: single-slot concentration ─────────────────────────────
        max_wallets = 0
        for slot, entries in self.slot_buys.items():
            unique = len({w for w, _ in entries})
            if unique > max_wallets:
                max_wallets = unique
        self.max_slot_wallets = max_wallets

        mode_a = max_wallets >= _COMPRESS_MIN_WALLETS

        # ── Mode B: rapid slot sequence ────────────────────────────────────
        mode_b = False
        if len(self.slot_order) >= 2:
            sorted_slots = sorted(self.slot_order)
            for i in range(1, len(sorted_slots)):
                s_prev = sorted_slots[i - 1]
                s_curr = sorted_slots[i]
                # Only check consecutive slots
                if s_curr - s_prev <= 2:
                    t_prev = self.slot_first_ts[s_prev]
                    t_curr = self.slot_first_ts[s_curr]
                    delta_ms = (t_curr - t_prev) * 1000
                    if 0 <= delta_ms <= _RAPID_WINDOW_MS:
                        mode_b = True
                        break

        self.rapid_sequence   = mode_b
        self.slot_compression = mode_a or mode_b

        # ── Score ──────────────────────────────────────────────────────────
        # Mode A score: scale by wallet concentration (4 wallets → 0.6, 6+ → 1.0)
        score_a = min(max_wallets / 6.0, 1.0) if mode_a else max_wallets / 10.0
        # Mode B adds a bonus if rapid
        score_b = 0.3 if mode_b else 0.0
        self.slot_compression_score = min(score_a + score_b, 1.0)

    @property
    def age_sec(self) -> float:
        return time.time() - self.created_at

    def summary(self) -> str:
        return (
            f"slot_compress={self.slot_compression}  "
            f"max_wallets_1slot={self.max_slot_wallets}  "
            f"rapid={self.rapid_sequence}  "
            f"score={self.slot_compression_score:.2f}"
        )


class SlotAnalyzer:
    """
    Maintains SlotData for all tracked tokens.
    Updated by token_scanner when trade WS events arrive.

    The slot field in pump.fun WS messages corresponds to the Solana slot
    in which the transaction was confirmed. Two buys in the same slot are
    within ~400ms of each other — this is structurally different from normal
    retail trading patterns.
    """

    def __init__(
        self,
        compress_min_wallets: int = 4,
        rapid_window_ms: int      = 500,
    ):
        global _COMPRESS_MIN_WALLETS, _RAPID_WINDOW_MS
        _COMPRESS_MIN_WALLETS = compress_min_wallets
        _RAPID_WINDOW_MS      = rapid_window_ms

        self._tokens: dict[str, SlotData] = {}

    def on_create(self, mint: str, symbol: str, created_at: float) -> SlotData:
        sd = SlotData(mint=mint, symbol=symbol, created_at=created_at)
        self._tokens[mint] = sd
        return sd

    def on_trade(self, mint: str, msg: dict) -> Optional[SlotData]:
        """Process a trade WS event. Returns updated SlotData or None."""
        sd = self._tokens.get(mint)
        if sd is None:
            return None

        is_buy = msg.get("txType") == "buy"
        if not is_buy:
            return sd

        slot   = msg.get("slot", 0)
        wallet = msg.get("traderPublicKey", "")

        if slot > 0:
            sd.on_buy(slot=slot, wallet=wallet, wall_ts=time.time())
            if sd.slot_compression:
                log.info(
                    "⚡ SLOT COMPRESS  %-10s  max_slot=%d  rapid=%s  score=%.2f",
                    sd.symbol, sd.max_slot_wallets,
                    sd.rapid_sequence, sd.slot_compression_score,
                )

        return sd

    def get(self, mint: str) -> Optional[SlotData]:
        return self._tokens.get(mint)

    def slot_compression_score_of(self, mint: str) -> float:
        sd = self._tokens.get(mint)
        return sd.slot_compression_score if sd else 0.0

    def is_compressed(self, mint: str) -> bool:
        sd = self._tokens.get(mint)
        return sd.slot_compression if sd else False

    def cleanup(self, max_age_sec: float = 120.0):
        """Evict old tokens."""
        now = time.time()
        dead = [m for m, s in list(self._tokens.items())
                if now - s.created_at > max_age_sec]
        for m in dead:
            del self._tokens[m]
