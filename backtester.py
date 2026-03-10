"""
backtester.py — Historical Replay Engine
=========================================
Replays the D5dtjf Excel trade files to evaluate strategy performance.

Input files: D5d_1.xlsx … D5d_6.xlsx
  Expected columns (flexible — mapped by common aliases):
    mint / token_address / ca
    symbol / name
    tx_type / type / side        ("buy" / "sell")
    sol_amount / sol / amount_sol
    timestamp / time / date
    slot                         (optional — for slot compression)
    wallet / trader / traderPublicKey (optional)

Run:
    python backtester.py

Or from code:
    from backtester import Backtester
    bt = Backtester(glob_pattern="D5d_*.xlsx")
    results = bt.run()
    bt.print_report(results)

Metrics computed:
  • Win rate (%)
  • Profit factor (gross_profit / gross_loss)
  • Average win / average loss
  • Max drawdown
  • Net PnL (SOL)
  • Expected value per trade (EV)
  • Trade count, winning trades, losing trades
  • Breakdown by hold-time bucket and entry hour
"""
from __future__ import annotations

import glob
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("backtester")

# ── Optional dependencies ─────────────────────────────────────────────────────
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    log.warning("pandas not installed — backtester disabled")


# ── Column aliases ────────────────────────────────────────────────────────────

_MINT_ALIASES    = {"mint", "token_address", "ca", "token", "contract"}
_SYMBOL_ALIASES  = {"symbol", "name", "token_name", "token_symbol"}
_TYPE_ALIASES    = {"tx_type", "type", "side", "txtype", "action", "tx type"}
_SOL_ALIASES     = {"sol_amount", "sol", "amount_sol", "sol_amt",
                    "value_sol", "amount", "solAmount"}
_TS_ALIASES      = {"timestamp", "time", "date", "datetime", "ts",
                    "block_time", "blocktime"}
_SLOT_ALIASES    = {"slot", "block_slot", "blockslot"}
_WALLET_ALIASES  = {"wallet", "trader", "traderpublickey", "from", "address"}


def _find_col(df_cols: list[str], aliases: set[str]) -> Optional[str]:
    """Case-insensitive column name lookup."""
    lower_map = {c.lower().replace(" ", "_"): c for c in df_cols}
    for alias in aliases:
        if alias.lower() in lower_map:
            return lower_map[alias.lower()]
    return None


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class Trade:
    mint:      str
    symbol:    str
    tx_type:   str          # "buy" | "sell"
    sol_amt:   float
    ts:        float        # unix timestamp
    slot:      int    = 0
    wallet:    str    = ""


@dataclass
class RoundTrip:
    """A matched buy→sell pair for one token."""
    mint:       str
    symbol:     str
    buy_sol:    float
    sell_sol:   float
    hold_sec:   float
    entry_ts:   float
    pnl_sol:    float = field(init=False)

    def __post_init__(self):
        self.pnl_sol = self.sell_sol - self.buy_sol

    @property
    def pnl_pct(self) -> float:
        if self.buy_sol <= 0:
            return 0.0
        return self.pnl_sol / self.buy_sol

    @property
    def is_win(self) -> bool:
        return self.pnl_sol > 0

    @property
    def hold_bucket(self) -> str:
        m = self.hold_sec / 60
        if m < 2:   return "0-2min"
        if m < 5:   return "2-5min"
        if m < 10:  return "5-10min"
        if m < 20:  return "10-20min"
        if m < 30:  return "20-30min"
        if m < 60:  return "30-60min"
        return "60min+"

    @property
    def entry_hour_utc(self) -> int:
        import datetime
        return datetime.datetime.utcfromtimestamp(self.entry_ts).hour


@dataclass
class BacktestResults:
    round_trips:    list[RoundTrip]
    total_trades:   int   = 0
    wins:           int   = 0
    losses:         int   = 0
    net_pnl:        float = 0.0
    gross_profit:   float = 0.0
    gross_loss:     float = 0.0
    max_drawdown:   float = 0.0
    avg_win_sol:    float = 0.0
    avg_loss_sol:   float = 0.0
    win_rate_pct:   float = 0.0
    profit_factor:  float = 0.0
    ev_per_trade:   float = 0.0

    # Breakdown dicts
    wr_by_bucket:   dict  = field(default_factory=dict)
    pnl_by_bucket:  dict  = field(default_factory=dict)
    wr_by_hour:     dict  = field(default_factory=dict)
    pnl_by_hour:    dict  = field(default_factory=dict)


# ── Main backtester class ─────────────────────────────────────────────────────

class Backtester:
    """
    Loads D5dtjf Excel trade files, matches buy→sell round-trips,
    computes performance metrics.
    """

    def __init__(
        self,
        glob_pattern: str = "D5d_*.xlsx",
        base_dir: str     = None,
        fee_sol: float    = 0.0001,   # priority fee per trade
    ):
        self.pattern  = glob_pattern
        self.base_dir = base_dir or os.path.dirname(os.path.abspath(__file__))
        self.fee_sol  = fee_sol

    # ── Public ────────────────────────────────────────────────────────────

    def run(self) -> BacktestResults:
        if not HAS_PANDAS:
            raise RuntimeError("pandas required: pip install pandas openpyxl")

        trades = self._load_all()
        log.info("Loaded %d raw trades from Excel files", len(trades))

        round_trips = self._match_round_trips(trades)
        log.info("Matched %d round-trip trades", len(round_trips))

        return self._compute_metrics(round_trips)

    def print_report(self, r: BacktestResults):
        print("\n" + "=" * 60)
        print("  BACKTEST REPORT — D5dtjf wallet")
        print("=" * 60)
        print(f"  Round-trip trades : {r.total_trades}")
        print(f"  Win rate          : {r.win_rate_pct:.1f}%")
        print(f"  Net PnL           : {r.net_pnl:+.3f} SOL")
        print(f"  Profit factor     : {r.profit_factor:.2f}")
        print(f"  EV per trade      : {r.ev_per_trade:+.4f} SOL")
        print(f"  Avg win           : +{r.avg_win_sol:.4f} SOL")
        print(f"  Avg loss          :  {r.avg_loss_sol:.4f} SOL")
        print(f"  Max drawdown      : {r.max_drawdown:.3f} SOL")
        print()

        if r.wr_by_bucket:
            print("  Win rate by hold time:")
            for bucket in sorted(r.wr_by_bucket.keys()):
                wr  = r.wr_by_bucket.get(bucket, 0)
                pnl = r.pnl_by_bucket.get(bucket, 0)
                print(f"    {bucket:12s}  WR={wr:.1f}%  PnL={pnl:+.3f}SOL")

        if r.wr_by_hour:
            print("\n  Win rate by UTC entry hour:")
            for hour in sorted(r.wr_by_hour.keys()):
                wr  = r.wr_by_hour.get(hour, 0)
                pnl = r.pnl_by_hour.get(hour, 0)
                print(f"    UTC {hour:02d}h  WR={wr:.1f}%  PnL={pnl:+.3f}SOL")

        print("=" * 60 + "\n")

    # ── Data loading ──────────────────────────────────────────────────────

    def _load_all(self) -> list[Trade]:
        pattern = os.path.join(self.base_dir, self.pattern)
        files   = sorted(glob.glob(pattern))
        if not files:
            raise FileNotFoundError(
                f"No files matched: {pattern}\n"
                "Place D5d_1.xlsx … D5d_6.xlsx in the same directory."
            )
        all_trades = []
        for fp in files:
            log.info("Loading %s", os.path.basename(fp))
            all_trades.extend(self._load_file(fp))
        return all_trades

    def _load_file(self, path: str) -> list[Trade]:
        df = pd.read_excel(path, engine="openpyxl")
        # Normalize column names for lookup
        cols = list(df.columns)

        mint_col   = _find_col(cols, _MINT_ALIASES)
        symbol_col = _find_col(cols, _SYMBOL_ALIASES)
        type_col   = _find_col(cols, _TYPE_ALIASES)
        sol_col    = _find_col(cols, _SOL_ALIASES)
        ts_col     = _find_col(cols, _TS_ALIASES)
        slot_col   = _find_col(cols, _SLOT_ALIASES)
        wallet_col = _find_col(cols, _WALLET_ALIASES)

        if not all([mint_col, type_col, sol_col, ts_col]):
            log.warning(
                "File %s: missing required columns. Found: %s",
                path, cols
            )
            return []

        trades = []
        for _, row in df.iterrows():
            mint    = str(row[mint_col]).strip() if mint_col else ""
            symbol  = str(row[symbol_col]).strip() if symbol_col else mint[:8]
            tx_type = str(row[type_col]).strip().lower() if type_col else ""
            sol_raw = row[sol_col]
            ts_raw  = row[ts_col]
            slot    = int(row[slot_col]) if slot_col and pd.notna(row[slot_col]) else 0
            wallet  = str(row[wallet_col]).strip() if wallet_col else ""

            # Normalize tx type
            if tx_type in ("b", "buy", "bought", "purchase"):
                tx_type = "buy"
            elif tx_type in ("s", "sell", "sold", "sale"):
                tx_type = "sell"
            else:
                continue  # skip unknown

            try:
                sol_amt = float(sol_raw)
            except (ValueError, TypeError):
                continue

            try:
                if hasattr(ts_raw, "timestamp"):
                    ts = ts_raw.timestamp()
                else:
                    ts = float(ts_raw)
            except (ValueError, TypeError):
                ts = time.time()

            if not mint or sol_amt <= 0:
                continue

            trades.append(Trade(
                mint=mint, symbol=symbol,
                tx_type=tx_type, sol_amt=sol_amt,
                ts=ts, slot=slot, wallet=wallet,
            ))

        log.info("  → %d trades from %s", len(trades), os.path.basename(path))
        return trades

    # ── Round-trip matching ───────────────────────────────────────────────

    def _match_round_trips(self, trades: list[Trade]) -> list[RoundTrip]:
        """
        Match buy→sell pairs per mint.
        Uses FIFO matching: earliest buy is matched with earliest sell.
        """
        from collections import defaultdict, deque

        buys_by_mint: dict[str, deque[Trade]] = defaultdict(deque)
        round_trips: list[RoundTrip] = []

        # Sort all trades by timestamp
        sorted_trades = sorted(trades, key=lambda t: t.ts)

        for trade in sorted_trades:
            if trade.tx_type == "buy":
                buys_by_mint[trade.mint].append(trade)
            elif trade.tx_type == "sell":
                q = buys_by_mint.get(trade.mint)
                if q:
                    buy = q.popleft()
                    hold = trade.ts - buy.ts
                    if hold < 0:
                        continue
                    # Subtract fees from sell proceeds
                    sell_net = trade.sol_amt - self.fee_sol
                    buy_cost = buy.sol_amt + self.fee_sol
                    rt = RoundTrip(
                        mint=trade.mint,
                        symbol=trade.symbol or buy.symbol,
                        buy_sol=buy_cost,
                        sell_sol=sell_net,
                        hold_sec=hold,
                        entry_ts=buy.ts,
                    )
                    round_trips.append(rt)

        return round_trips

    # ── Metrics computation ───────────────────────────────────────────────

    def _compute_metrics(self, rts: list[RoundTrip]) -> BacktestResults:
        if not rts:
            return BacktestResults(round_trips=[])

        wins   = [r for r in rts if r.is_win]
        losses = [r for r in rts if not r.is_win]

        gross_profit = sum(r.pnl_sol for r in wins)
        gross_loss   = abs(sum(r.pnl_sol for r in losses))
        net_pnl      = gross_profit - gross_loss

        avg_win  = gross_profit / len(wins)   if wins   else 0.0
        avg_loss = gross_loss   / len(losses) if losses else 0.0
        pf       = gross_profit / gross_loss  if gross_loss > 0 else float("inf")
        wr       = len(wins) / len(rts) * 100
        ev       = net_pnl / len(rts)

        # Max drawdown
        equity   = 0.0
        peak     = 0.0
        max_dd   = 0.0
        for r in sorted(rts, key=lambda x: x.entry_ts):
            equity += r.pnl_sol
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd

        # Breakdown by hold-time bucket
        from collections import defaultdict
        bucket_wins  = defaultdict(int)
        bucket_total = defaultdict(int)
        bucket_pnl   = defaultdict(float)
        for r in rts:
            b = r.hold_bucket
            bucket_total[b] += 1
            bucket_pnl[b]   += r.pnl_sol
            if r.is_win:
                bucket_wins[b] += 1

        wr_by_bucket  = {b: bucket_wins[b] / bucket_total[b] * 100
                         for b in bucket_total}
        pnl_by_bucket = dict(bucket_pnl)

        # Breakdown by UTC entry hour
        hour_wins  = defaultdict(int)
        hour_total = defaultdict(int)
        hour_pnl   = defaultdict(float)
        for r in rts:
            h = r.entry_hour_utc
            hour_total[h] += 1
            hour_pnl[h]   += r.pnl_sol
            if r.is_win:
                hour_wins[h] += 1

        wr_by_hour  = {h: hour_wins[h] / hour_total[h] * 100
                       for h in hour_total}
        pnl_by_hour = dict(hour_pnl)

        return BacktestResults(
            round_trips=rts,
            total_trades=len(rts),
            wins=len(wins),
            losses=len(losses),
            net_pnl=net_pnl,
            gross_profit=gross_profit,
            gross_loss=gross_loss,
            max_drawdown=max_dd,
            avg_win_sol=avg_win,
            avg_loss_sol=avg_loss,
            win_rate_pct=wr,
            profit_factor=pf,
            ev_per_trade=ev,
            wr_by_bucket=wr_by_bucket,
            pnl_by_bucket=pnl_by_bucket,
            wr_by_hour=wr_by_hour,
            pnl_by_hour=pnl_by_hour,
        )


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-5s  %(message)s")
    bt = Backtester()
    results = bt.run()
    bt.print_report(results)
