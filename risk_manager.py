"""
risk_manager.py — Position Tracker & Risk Engine
==================================================
Manages open positions with:
  - TP1 = +40%   → sell 40% of position
  - TP2 = +80%   → sell 35% of remaining
  - TP3 = +120%  → sell 100% of remaining
  - SL  = -25%   → full exit
  - Emergency exit: sell pressure > 60% on open position
  - Max hold: 30 minutes hard stop

All exit actions delegate to TradeEngine.
"""
from __future__ import annotations

import csv
import json
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

    # Signal context (for logging / analysis)
    entry_age_sec:     float = 0.0
    entry_buy_vol:     float = 0.0
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

    exit_reason:   str = ""

    @property
    def pnl_pct(self) -> float:
        if self.entry_price <= 0 or self.current_price <= 0:
            return 0.0
        return (self.current_price - self.entry_price) / self.entry_price

    @property
    def hold_sec(self) -> float:
        return time.time() - self.opened_at

    @property
    def total_sol_out(self) -> float:
        return self.realized_sol + self.token_balance * self.current_price

    @property
    def net_pnl(self) -> float:
        return self.total_sol_out - self.entry_sol


@dataclass
class ClosedTrade:
    mint:        str
    symbol:      str
    opened_at:   str
    closed_at:   str
    hold_sec:    float
    entry_sol:   float
    exit_sol:    float
    pnl_sol:     float
    pnl_pct:     float
    exit_reason: str
    was_cluster: bool
    entry_buyers: int
    entry_buy_vol: float


class RiskManager:
    """
    Tracks all open positions.
    Called by main loop: on each price update + sell_pressure update.
    """

    def __init__(
        self,
        trade_engine,            # TradeEngine instance
        velocity_analyzer,       # VelocityAnalyzer (for sell_pressure)
        tp1_pct: float   = 0.40,
        tp2_pct: float   = 0.80,
        tp3_pct: float   = 1.20,
        tp1_frac: float  = 0.40,
        tp2_frac: float  = 0.35,
        tp3_frac: float  = 1.00,
        sl_pct: float    = 0.25,
        sell_pressure_emergency: float = 0.60,
        max_hold_sec: float = 1800.0,  # 30 min
        max_positions: int  = 5,
        trades_csv: str     = "trades_sniper.csv",
        dry_run: bool       = True,
    ):
        self.te           = trade_engine
        self.va           = velocity_analyzer
        self.tp1          = tp1_pct
        self.tp2          = tp2_pct
        self.tp3          = tp3_pct
        self.tp1_f        = tp1_frac
        self.tp2_f        = tp2_frac
        self.tp3_f        = tp3_frac
        self.sl           = sl_pct
        self.emrg_sp      = sell_pressure_emergency
        self.max_hold     = max_hold_sec
        self.max_pos      = max_positions
        self.dry_run      = dry_run

        self.positions: dict[str, Position] = {}
        self.closed: list[ClosedTrade] = []

        # Stats
        self.n_entered   = 0
        self.n_wins      = 0
        self.total_pnl   = 0.0
        self.total_fees  = 0.0

        # CSV log
        self._csv_path = trades_csv
        self._ensure_csv()

    # ── Position lifecycle ────────────────────────────────────────────────

    def can_open(self) -> bool:
        return len(self.positions) < self.max_pos

    def add_position(self, mint: str, trade_result, sig_ctx: dict) -> Position:
        """Register a new open position after successful buy."""
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
            entry_sell_p=sig_ctx.get("sell_pressure", 0),
            entry_buyers=sig_ctx.get("unique_buyers", 0),
            was_cluster=sig_ctx.get("cluster_ok", False),
            was_slot_cluster=sig_ctx.get("slot_cluster_ok", False),
        )
        self.positions[mint] = pos
        self.n_entered += 1
        log.info("📂 Position opened: %s  entry=%.2e  %.3f SOL",
                 pos.symbol, pos.entry_price, pos.entry_sol)
        return pos

    # ── Main risk loop ─────────────────────────────────────────────────────

    async def check_all(self):
        """
        Called periodically (e.g. every 1–2 seconds) from main loop.
        Updates prices from VelocityAnalyzer and applies TP/SL/emergency logic.
        """
        to_close: list[tuple[str, str]] = []  # (mint, reason)

        for mint, pos in list(self.positions.items()):
            # Price update from velocity analyzer
            price = self.va.current_price_of(mint)
            if price > 0:
                pos.current_price = price
                if price > pos.peak_price:
                    pos.peak_price = price

            # Sell pressure emergency exit
            sp = self.va.sell_pressure_of(mint)
            if sp >= self.emrg_sp:
                log.warning("🚨 %s: sell pressure %.0f%% > %.0f%% — EMERGENCY EXIT",
                            pos.symbol, sp * 100, self.emrg_sp * 100)
                to_close.append((mint, f"emergency_sell_pressure_{sp:.0%}"))
                continue

            # Max hold
            if pos.hold_sec >= self.max_hold:
                to_close.append((mint, "max_hold_30min"))
                continue

            # No price → skip TP/SL
            if pos.current_price <= 0:
                continue

            pnl = pos.pnl_pct

            # Stop loss −25%
            if pnl <= -self.sl:
                to_close.append((mint, f"stop_loss_{pnl:.1%}"))
                continue

            # TP levels (partial sells)
            if not pos.tp1_done and pnl >= self.tp1:
                await self._partial_sell(pos, self.tp1_f, f"TP1_{pnl:.0%}")
                pos.tp1_done = True

            if pos.tp1_done and not pos.tp2_done and pnl >= self.tp2:
                await self._partial_sell(pos, self.tp2_f, f"TP2_{pnl:.0%}")
                pos.tp2_done = True

            if pos.tp2_done and not pos.tp3_done and pnl >= self.tp3:
                to_close.append((mint, f"TP3_{pnl:.0%}"))

        # Execute full closes
        for mint, reason in to_close:
            await self._close_position(mint, reason)

    # ── Trade actions ─────────────────────────────────────────────────────

    async def _partial_sell(self, pos: Position, fraction: float, reason: str):
        """Sell a fraction of the position."""
        to_sell = int(pos.token_balance * fraction)
        if to_sell <= 0:
            return

        sol_recv = await self.te.sell(
            mint=pos.mint,
            symbol=pos.symbol,
            token_amount=to_sell,
            current_price=pos.current_price,
            reason=reason,
        )
        pos.token_balance -= to_sell
        pos.realized_sol  += sol_recv

        pnl_pct = (pos.current_price - pos.entry_price) / pos.entry_price * 100
        icon = "🟢" if sol_recv > 0 else "🔴"
        log.info("%s PARTIAL %-10s  %d%%  recv=%.4f SOL  pnl=%.1f%%  [%s]",
                 icon, pos.symbol, int(fraction * 100), sol_recv, pnl_pct, reason)

    async def _close_position(self, mint: str, reason: str):
        """Sell all remaining tokens and record trade."""
        pos = self.positions.pop(mint, None)
        if pos is None:
            return

        pos.exit_reason = reason

        if pos.token_balance > 0:
            sol_recv = await self.te.sell(
                mint=pos.mint,
                symbol=pos.symbol,
                token_amount=pos.token_balance,
                current_price=pos.current_price,
                reason=reason,
            )
            pos.realized_sol  += sol_recv
            pos.token_balance  = 0

        net_pnl  = pos.realized_sol - pos.entry_sol
        pnl_pct  = net_pnl / pos.entry_sol * 100 if pos.entry_sol else 0
        is_win   = net_pnl > 0

        self.total_pnl += net_pnl
        if is_win:
            self.n_wins += 1

        icon = "✅" if is_win else "❌"
        log.info(
            "%s %-10s  PnL %+.4f SOL (%+.1f%%)  hold %.1f min  [%s]  WR %.0f%%",
            icon, pos.symbol,
            net_pnl, pnl_pct,
            pos.hold_sec / 60,
            reason,
            self.win_rate,
        )

        self._record_trade(pos)

    # ── Stats ─────────────────────────────────────────────────────────────

    @property
    def win_rate(self) -> float:
        if self.n_entered == 0:
            return 0.0
        closed = len(self.closed)
        if closed == 0:
            return 0.0
        return self.n_wins / closed * 100

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
                    "exit_reason", "was_cluster", "entry_buyers", "entry_buy_vol",
                    "entry_age_sec", "entry_sell_p",
                ])

    def _record_trade(self, pos: Position):
        now = datetime.now(timezone.utc)
        net_pnl = pos.realized_sol - pos.entry_sol
        pnl_pct = net_pnl / pos.entry_sol * 100 if pos.entry_sol else 0

        ct = ClosedTrade(
            mint=pos.mint,
            symbol=pos.symbol,
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
        )
        self.closed.append(ct)

        # Append to CSV
        try:
            with open(self._csv_path, "a", newline="") as f:
                csv.writer(f).writerow([
                    ct.opened_at, ct.closed_at, ct.symbol, ct.mint,
                    ct.hold_sec, ct.entry_sol, ct.exit_sol,
                    ct.pnl_sol, ct.pnl_pct, ct.exit_reason,
                    ct.was_cluster, ct.entry_buyers, ct.entry_buy_vol,
                    pos.entry_age_sec, pos.entry_sell_p,
                ])
        except Exception as exc:
            log.debug("CSV write error: %s", exc)
