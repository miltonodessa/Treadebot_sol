"""
risk_manager.py — Position Tracker & Risk Engine
==================================================
Parameters calibrated from 24,261 trades of wallet D5dtjf (97 days):

  Win rate: 27.65%  |  Profit factor: 1.37  |  Net: +237 SOL

EXIT STRATEGY (data-driven):
  ┌─────────────────────────────────────────────────────────────────┐
  │ DEAD ZONE: hold < 5 min → WR only 13.7–25% → NO TP BEFORE 5min │
  │ SWEET SPOT: 5–30 min → WR 38–40%                               │
  │ MOONBAG:   30–60 min → WR 74.9%                                 │
  └─────────────────────────────────────────────────────────────────┘

  Pool growth signals:
    growth < 20% after 10 min → momentum dead → exit
    growth ≥ 50%              → WR 75% → defer TP1/TP2, let it run
    growth ≥ 100%             → WR 93% → ride hard until TP3

  Stop loss: -10%  (90th pct loss = -12%; median loss = -3.9%)
  Emergency: sell pressure > 60% on open position
  Max hold:  3600s (60 min) — wallet's 30-60min zone = 74.9% WR

TP levels:
  TP1 = +40%  → sell 40%   (only after min_hold_sec)
  TP2 = +80%  → sell 35%   (only after min_hold_sec, suppressed on pool ride)
  TP3 = +120% → sell 100%
"""
from __future__ import annotations

import csv
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("risk_manager")

PUMP_TOTAL_SUPPLY = 1_000_000_000


@dataclass
class Position:
    """Single open position."""
    mint:          str
    symbol:        str
    entry_price:   float      # SOL per token at buy
    entry_sol:     float      # SOL committed
    token_balance: int        # current token balance
    opened_at:     float      # unix timestamp

    # Signal context (logged to CSV for analysis)
    entry_age_sec:     float = 0.0
    entry_buy_vol:     float = 0.0
    entry_pool_sol:    float = 0.0
    entry_sell_p:      float = 0.0
    entry_buyers:      int   = 0
    was_cluster:       bool  = False
    was_slot_cluster:  bool  = False

    # Live tracking
    current_price:  float = 0.0
    peak_price:     float = 0.0
    realized_sol:   float = 0.0

    # TP flags
    tp1_done: bool = False   # +40%
    tp2_done: bool = False   # +80%
    tp3_done: bool = False   # +120%

    exit_reason: str = ""

    @property
    def pnl_pct(self) -> float:
        if self.entry_price <= 0 or self.current_price <= 0:
            return 0.0
        return (self.current_price - self.entry_price) / self.entry_price

    @property
    def hold_sec(self) -> float:
        return time.time() - self.opened_at

    @property
    def net_pnl(self) -> float:
        return self.realized_sol + self.token_balance * self.current_price - self.entry_sol


@dataclass
class ClosedTrade:
    mint:           str
    symbol:         str
    opened_at:      str
    closed_at:      str
    hold_sec:       float
    entry_sol:      float
    exit_sol:       float
    pnl_sol:        float
    pnl_pct:        float
    exit_reason:    str
    was_cluster:    bool
    entry_buyers:   int
    entry_buy_vol:  float
    entry_pool_sol: float
    entry_age_sec:  float
    entry_sell_p:   float


class RiskManager:
    """
    Tracks all open positions and applies data-driven exit logic.
    Called periodically (every ~1.5s) by main loop via check_all().
    """

    def __init__(
        self,
        trade_engine,
        velocity_analyzer,
        # TP levels
        tp1_pct: float  = 0.40,
        tp2_pct: float  = 0.80,
        tp3_pct: float  = 1.20,
        tp1_frac: float = 0.40,
        tp2_frac: float = 0.35,
        tp3_frac: float = 1.00,
        # Stop loss — data: 90th pct loss = -12%, median = -3.9% → use -10%
        sl_pct: float   = 0.10,
        # Emergency exit on sell pressure spike
        sell_pressure_emergency: float = 0.60,
        # Minimum hold before ANY TP fires — dead zone 0–5min: only 13.7% WR
        min_hold_sec: float   = 300.0,   # 5 minutes
        # Momentum exit: if pool barely grew after X seconds, token is dead
        momentum_exit_sec: float        = 600.0,   # 10 minutes
        momentum_min_pool_growth: float = 0.20,    # pool must grow ≥20% in 10 min
        # Pool ride: when pool growing hard, defer TP1/TP2 and hold for more
        pool_ride_threshold: float      = 0.50,    # pool +50% → WR 75% → hold
        # Hard max hold — wallet's 30-60 min zone = 74.9% WR → set limit at 60min
        max_hold_sec: float   = 3600.0,  # 60 minutes
        max_positions: int    = 5,
        trades_csv: str       = "trades_sniper.csv",
        dry_run: bool         = True,
    ):
        self.te                   = trade_engine
        self.va                   = velocity_analyzer
        self.tp1                  = tp1_pct
        self.tp2                  = tp2_pct
        self.tp3                  = tp3_pct
        self.tp1_f                = tp1_frac
        self.tp2_f                = tp2_frac
        self.tp3_f                = tp3_frac
        self.sl                   = sl_pct
        self.emrg_sp              = sell_pressure_emergency
        self.min_hold             = min_hold_sec
        self.momentum_exit_sec    = momentum_exit_sec
        self.momentum_min_growth  = momentum_min_pool_growth
        self.pool_ride            = pool_ride_threshold
        self.max_hold             = max_hold_sec
        self.max_pos              = max_positions
        self.dry_run              = dry_run

        self.positions: dict[str, Position] = {}
        self.closed: list[ClosedTrade] = []

        self.n_entered = 0
        self.n_wins    = 0
        self.total_pnl = 0.0

        self._csv_path = trades_csv
        self._ensure_csv()

    # ── Position lifecycle ────────────────────────────────────────────────

    def can_open(self) -> bool:
        return len(self.positions) < self.max_pos

    def add_position(self, mint: str, trade_result, sig_ctx: dict) -> Position:
        pos = Position(
            mint=mint,
            symbol=trade_result.symbol,
            entry_price=trade_result.price,
            entry_sol=trade_result.sol_spent,
            token_balance=trade_result.tokens,
            opened_at=trade_result.opened_at,
            current_price=trade_result.price,
            peak_price=trade_result.price,
            entry_age_sec=sig_ctx.get("age_sec", 0),
            entry_buy_vol=sig_ctx.get("buy_vol_sol", 0),
            entry_pool_sol=sig_ctx.get("entry_pool_sol", 0),
            entry_sell_p=sig_ctx.get("sell_pressure", 0),
            entry_buyers=sig_ctx.get("unique_buyers", 0),
            was_cluster=sig_ctx.get("cluster_ok", False),
            was_slot_cluster=sig_ctx.get("slot_cluster_ok", False),
        )
        self.positions[mint] = pos
        self.n_entered += 1
        log.info(
            "📂 Opened  %-10s  entry=%.2e  %.3f SOL  "
            "pool=%.1fSOL  vel=%.2fSOL  buyers=%d",
            pos.symbol, pos.entry_price, pos.entry_sol,
            pos.entry_pool_sol, pos.entry_buy_vol, pos.entry_buyers,
        )
        return pos

    # ── Main risk loop ─────────────────────────────────────────────────────

    async def check_all(self):
        """
        Apply all exit logic to open positions.
        Called every ~1.5s from main loop.

        Exit hierarchy (checked in order):
          1. Emergency sell pressure spike  → immediate full exit
          2. Hard max hold (60 min)         → full exit
          3. Stop loss -10%                 → full exit
          4. Momentum dead (10min, <20% pool growth) → full exit
          5. TP1 +40% (only after 5 min, not on pool-ride) → partial
          6. TP2 +80% (only after 5 min, not on pool-ride) → partial
          7. TP3 +120%                       → full exit
        """
        to_close: list[tuple[str, str]] = []

        for mint, pos in list(self.positions.items()):
            # Price + pool data update from VelocityAnalyzer
            price = self.va.current_price_of(mint)
            if price > 0:
                pos.current_price = price
                if price > pos.peak_price:
                    pos.peak_price = price

            pool_growth = self.va.pool_growth_of(mint)
            hold = pos.hold_sec

            # ── 1. Emergency: sell pressure spike ─────────────────────────
            sp = self.va.sell_pressure_of(mint)
            if sp >= self.emrg_sp:
                log.warning("🚨 %s: sell pressure %.0f%% — EMERGENCY EXIT",
                            pos.symbol, sp * 100)
                to_close.append((mint, f"emergency_sp_{sp:.0%}"))
                continue

            # ── 2. Hard max hold (60 min) ──────────────────────────────────
            if hold >= self.max_hold:
                to_close.append((mint, f"max_hold_{self.max_hold/60:.0f}min"))
                continue

            if pos.current_price <= 0:
                continue

            pnl = pos.pnl_pct

            # ── 3. Stop loss -10% ──────────────────────────────────────────
            if pnl <= -self.sl:
                to_close.append((mint, f"stop_loss_{pnl:.1%}"))
                continue

            # ── 4. Momentum exit ───────────────────────────────────────────
            # Data: if pool barely moved after 10 min → token is dead
            # Only trigger if we actually have pool data (pool_growth != 0 check)
            _, current_pool = self.va.pool_sol_of(mint)
            has_pool_data = current_pool > 0
            if (has_pool_data
                    and hold >= self.momentum_exit_sec
                    and pool_growth < self.momentum_min_growth
                    and not pos.tp1_done):   # if TP1 hit, token showed life
                to_close.append((mint, f"no_momentum_pool_growth_{pool_growth:.0%}"))
                continue

            # ── 5–7. TP levels (only after min_hold_sec = 5 min) ──────────
            if hold < self.min_hold:
                # In dead zone (< 5 min) — only emergency/SL can exit
                continue

            # Pool-ride mode: pool growing hard → defer TP1/TP2, let it run
            # Data: pool growth > 50% → WR 75%; > 100% → WR 93%
            on_pool_ride = pool_growth >= self.pool_ride

            if not pos.tp1_done and pnl >= self.tp1:
                if on_pool_ride:
                    log.debug("🏄 %s: pool +%.0f%% — deferring TP1 (riding momentum)",
                              pos.symbol, pool_growth * 100)
                else:
                    await self._partial_sell(pos, self.tp1_f, f"TP1_{pnl:.0%}")
                    pos.tp1_done = True

            if pos.tp1_done and not pos.tp2_done and pnl >= self.tp2:
                if on_pool_ride:
                    log.debug("🏄 %s: pool +%.0f%% — deferring TP2 (riding momentum)",
                              pos.symbol, pool_growth * 100)
                else:
                    await self._partial_sell(pos, self.tp2_f, f"TP2_{pnl:.0%}")
                    pos.tp2_done = True

            if pnl >= self.tp3:
                to_close.append((mint, f"TP3_{pnl:.0%}"))

        for mint, reason in to_close:
            await self._close_position(mint, reason)

    # ── Trade actions ─────────────────────────────────────────────────────

    async def _partial_sell(self, pos: Position, fraction: float, reason: str):
        to_sell = int(pos.token_balance * fraction)
        if to_sell <= 0:
            return

        sol_recv = await self.te.sell(
            mint=pos.mint, symbol=pos.symbol,
            token_amount=to_sell,
            current_price=pos.current_price,
            reason=reason,
        )
        pos.token_balance -= to_sell
        pos.realized_sol  += sol_recv

        pnl_pct = pos.pnl_pct * 100
        pool_growth = self.va.pool_growth_of(pos.mint)
        icon = "🟢" if sol_recv > 0 else "🔴"
        log.info("%s PARTIAL  %-10s  %d%%  recv=%.4f SOL  pnl=%.1f%%  "
                 "pool_growth=%.0f%%  hold=%.1fmin  [%s]",
                 icon, pos.symbol, int(fraction * 100), sol_recv, pnl_pct,
                 pool_growth * 100, pos.hold_sec / 60, reason)

    async def _close_position(self, mint: str, reason: str):
        pos = self.positions.pop(mint, None)
        if pos is None:
            return

        pos.exit_reason = reason

        if pos.token_balance > 0:
            sol_recv = await self.te.sell(
                mint=pos.mint, symbol=pos.symbol,
                token_amount=pos.token_balance,
                current_price=pos.current_price,
                reason=reason,
            )
            pos.realized_sol  += sol_recv
            pos.token_balance  = 0

        net_pnl = pos.realized_sol - pos.entry_sol
        pnl_pct = net_pnl / pos.entry_sol * 100 if pos.entry_sol else 0
        is_win  = net_pnl > 0

        self.total_pnl += net_pnl
        if is_win:
            self.n_wins += 1

        pool_growth = self.va.pool_growth_of(mint)
        icon = "✅" if is_win else "❌"
        log.info(
            "%s %-10s  PnL %+.4f SOL (%+.1f%%)  hold %.1fmin  "
            "pool_growth=%.0f%%  [%s]  WR %.0f%%",
            icon, pos.symbol,
            net_pnl, pnl_pct,
            pos.hold_sec / 60,
            pool_growth * 100,
            reason,
            self.win_rate,
        )

        self._record_trade(pos)

    # ── Stats ─────────────────────────────────────────────────────────────

    @property
    def win_rate(self) -> float:
        closed = len(self.closed)
        return self.n_wins / closed * 100 if closed > 0 else 0.0

    def status(self) -> dict:
        return {
            "open_positions": len(self.positions),
            "total_trades":   len(self.closed),
            "win_rate_pct":   round(self.win_rate, 1),
            "total_pnl_sol":  round(self.total_pnl, 4),
        }

    def log_summary(self):
        s = self.status()
        log.info(
            "📊 Risk: positions=%d  trades=%d  WR=%.0f%%  PnL=%+.3f SOL",
            s["open_positions"], s["total_trades"],
            s["win_rate_pct"],   s["total_pnl_sol"],
        )

    # ── CSV logging ───────────────────────────────────────────────────────

    def _ensure_csv(self):
        path = Path(self._csv_path)
        if not path.exists():
            with path.open("w", newline="") as f:
                csv.writer(f).writerow([
                    "opened_at", "closed_at", "symbol", "mint",
                    "hold_sec", "entry_sol", "exit_sol", "pnl_sol", "pnl_pct",
                    "exit_reason", "was_cluster", "entry_buyers",
                    "entry_buy_vol", "entry_pool_sol",
                    "entry_age_sec", "entry_sell_p",
                ])

    def _record_trade(self, pos: Position):
        now     = datetime.now(timezone.utc)
        net_pnl = pos.realized_sol - pos.entry_sol
        pnl_pct = net_pnl / pos.entry_sol * 100 if pos.entry_sol else 0

        ct = ClosedTrade(
            mint=pos.mint, symbol=pos.symbol,
            opened_at=datetime.fromtimestamp(pos.opened_at, tz=timezone.utc).isoformat(),
            closed_at=now.isoformat(),
            hold_sec=round(pos.hold_sec, 1),
            entry_sol=round(pos.entry_sol, 6),
            exit_sol=round(pos.realized_sol, 6),
            pnl_sol=round(net_pnl, 6),
            pnl_pct=round(pnl_pct, 2),
            exit_reason=pos.exit_reason,
            was_cluster=pos.was_cluster,
            entry_buyers=pos.entry_buyers,
            entry_buy_vol=pos.entry_buy_vol,
            entry_pool_sol=pos.entry_pool_sol,
            entry_age_sec=pos.entry_age_sec,
            entry_sell_p=pos.entry_sell_p,
        )
        self.closed.append(ct)

        try:
            with open(self._csv_path, "a", newline="") as f:
                csv.writer(f).writerow([
                    ct.opened_at, ct.closed_at, ct.symbol, ct.mint,
                    ct.hold_sec, ct.entry_sol, ct.exit_sol,
                    ct.pnl_sol, ct.pnl_pct, ct.exit_reason,
                    ct.was_cluster, ct.entry_buyers, ct.entry_buy_vol,
                    ct.entry_pool_sol, ct.entry_age_sec, ct.entry_sell_p,
                ])
        except Exception as exc:
            log.debug("CSV write error: %s", exc)
