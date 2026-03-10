"""
main.py — Coordinated Launch Sniper Bot
========================================
Strategy: enter tokens 12–35s after creation ONLY when ALL 6 signals fire:

  0. Pre-launch funding cluster   (Helius API — funding_wallet → 3+ wallets)
  1. First-slot bundle buys       (3+ wallets buying in first 2 seconds)
  2. Liquidity velocity           (≥ 1 SOL buy volume in 40s window)
  3. Low sell pressure            (< 35%)
  4. Unique buyer count           (≥ 10 wallets)
  5. Sell absorption              (price not dropping > 20% on early sells)

Exit:
  TP1 = +40%  → sell 40%
  TP2 = +80%  → sell 35% of remainder
  TP3 = +120% → sell rest
  SL  = −25%
  Emergency: sell pressure > 60%

Run:
  python main.py                    # DRY RUN (default)
  DRY_RUN=0 python main.py         # LIVE
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv()

# ── Logging setup ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d  %(levelname)-5s  %(name)-12s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")

# ── Config (env vars) ─────────────────────────────────────────────────────────

HELIUS_API_KEY   = os.getenv("HELIUS_API_KEY", "")
RPC_URL          = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
PRIVATE_KEY      = os.getenv("OUR_PRIVATE_KEY", "")
DRY_RUN          = bool(int(os.getenv("DRY_RUN", "1")))

# Signal thresholds
ENTRY_MIN_SEC    = float(os.getenv("ENTRY_MIN_SEC",    "12"))
ENTRY_MAX_SEC    = float(os.getenv("ENTRY_MAX_SEC",    "35"))
CLUSTER_WIN_SEC  = int(os.getenv("CLUSTER_WINDOW_SEC", "120"))
CLUSTER_MIN_W    = int(os.getenv("CLUSTER_MIN_WALLETS","3"))
CLUSTER_MIN_SOL  = float(os.getenv("CLUSTER_MIN_SOL",  "0.10"))
CLUSTER_MAX_SOL  = float(os.getenv("CLUSTER_MAX_SOL",  "1.50"))
VELOCITY_MIN     = float(os.getenv("VELOCITY_MIN_SOL", "1.0"))
SELL_P_MAX       = float(os.getenv("SELL_PRESSURE_MAX","0.35"))
BUYERS_MIN       = int(os.getenv("BUYERS_MIN",         "5"))
SLOT_CLUSTER_MIN = int(os.getenv("SLOT_CLUSTER_MIN",   "3"))

# Trade
BUY_SIZE_SOL     = float(os.getenv("BUY_SIZE_SOL",     "0.10"))
SLIPPAGE_PCT     = float(os.getenv("SLIPPAGE_PCT",     "25.0"))
PRIORITY_FEE     = float(os.getenv("PRIORITY_FEE_SOL", "0.0001"))
MAX_POSITIONS    = int(os.getenv("MAX_POSITIONS",       "5"))

# Risk
TP1_PCT          = float(os.getenv("TP1_PCT",           "0.40"))
TP2_PCT          = float(os.getenv("TP2_PCT",           "0.80"))
TP3_PCT          = float(os.getenv("TP3_PCT",           "1.20"))
TP1_FRAC         = float(os.getenv("TP1_FRAC",          "0.40"))
TP2_FRAC         = float(os.getenv("TP2_FRAC",          "0.35"))
TP3_FRAC         = float(os.getenv("TP3_FRAC",          "1.00"))
SL_PCT           = float(os.getenv("SL_PCT",            "0.25"))
EMRG_SELL_P      = float(os.getenv("EMRG_SELL_PRESSURE","0.60"))
MAX_HOLD_SEC     = int(os.getenv("MAX_HOLD_SEC",        "1800"))

STATUS_INTERVAL  = int(os.getenv("STATUS_INTERVAL",    "60"))    # seconds

# ── Imports (after env loaded) ────────────────────────────────────────────────

from velocity_analyzer      import VelocityAnalyzer, TokenSignals
from wallet_cluster_detector import WalletClusterDetector
from token_scanner          import TokenScanner
from trade_engine           import TradeEngine
from risk_manager           import RiskManager


# ─────────────────────────────────────────────────────────────────────────────
# Bot
# ─────────────────────────────────────────────────────────────────────────────

class SniperBot:
    """
    Orchestrates all modules:
      TokenScanner → fires on_entry_signal()
      → RiskManager.can_open() check
      → TradeEngine.buy()
      → RiskManager.add_position()

    check_risk_loop() runs every 1.5s:
      → RiskManager.check_all() applies TP/SL/emergency
    """

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.va:      Optional[VelocityAnalyzer]      = None
        self.cd:      Optional[WalletClusterDetector] = None
        self.te:      Optional[TradeEngine]            = None
        self.rm:      Optional[RiskManager]            = None
        self.scanner: Optional[TokenScanner]           = None
        self._start_time = time.time()

    async def setup(self):
        connector = aiohttp.TCPConnector(
            ssl=False, limit=64, ttl_dns_cache=300
        )
        self.session = aiohttp.ClientSession(connector=connector)

        # Velocity analyzer (signal state)
        self.va = VelocityAnalyzer(max_tracked=2000)

        # Wallet cluster detector (optional — requires HELIUS_API_KEY)
        if HELIUS_API_KEY:
            self.cd = WalletClusterDetector(
                api_key=HELIUS_API_KEY,
                session=self.session,
                window_sec=CLUSTER_WIN_SEC,
                min_wallets=CLUSTER_MIN_W,
                min_sol=CLUSTER_MIN_SOL,
                max_sol=CLUSTER_MAX_SOL,
            )
            log.info("✅ WalletClusterDetector enabled")
        else:
            log.warning(
                "⚠️  HELIUS_API_KEY not set — cluster detection disabled. "
                "Signal 0 will be auto-approved."
            )

        # Trade engine
        self.te = TradeEngine(
            private_key_b58=PRIVATE_KEY,
            rpc_url=RPC_URL,
            session=self.session,
            buy_size_sol=BUY_SIZE_SOL,
            slippage_pct=SLIPPAGE_PCT,
            priority_fee_sol=PRIORITY_FEE,
            dry_run=DRY_RUN,
        )

        # Risk manager
        _ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.rm = RiskManager(
            trade_engine=self.te,
            velocity_analyzer=self.va,
            tp1_pct=TP1_PCT, tp2_pct=TP2_PCT, tp3_pct=TP3_PCT,
            tp1_frac=TP1_FRAC, tp2_frac=TP2_FRAC, tp3_frac=TP3_FRAC,
            sl_pct=SL_PCT,
            sell_pressure_emergency=EMRG_SELL_P,
            max_hold_sec=MAX_HOLD_SEC,
            max_positions=MAX_POSITIONS,
            trades_csv=f"trades_sniper_{_ts}.csv",
            dry_run=DRY_RUN,
        )

        # Token scanner
        self.scanner = TokenScanner(
            velocity_analyzer=self.va,
            cluster_detector=self.cd,
            on_entry=self.on_entry_signal,
            session=self.session,
            entry_min_sec=ENTRY_MIN_SEC,
            entry_max_sec=ENTRY_MAX_SEC,
        )

    async def run(self):
        await self.setup()
        self._print_config()

        tasks = [
            asyncio.ensure_future(self.scanner.run()),
            asyncio.ensure_future(self._risk_loop()),
            asyncio.ensure_future(self._status_loop()),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            await self.teardown()

    async def teardown(self):
        log.info("Shutting down…")
        if self.scanner:
            await self.scanner.stop()
        if self.session:
            await self.session.close()
        if self.rm:
            self.rm.log_summary()

    # ── Entry signal callback ─────────────────────────────────────────────

    async def on_entry_signal(self, mint: str, sig: TokenSignals):
        """
        Called by TokenScanner when all 6 signals are satisfied.
        Attempts to open a position if capacity allows.
        """
        if not self.rm.can_open():
            log.info("⛔ %s: max positions reached (%d/%d)",
                     sig.symbol, len(self.rm.positions), MAX_POSITIONS)
            return

        # Symbol dedup: skip if we already hold a token with the same symbol
        sym_upper = sig.symbol.upper()
        if any(p.symbol.upper() == sym_upper for p in self.rm.positions.values()):
            log.debug("⛔ %s: duplicate symbol in portfolio", sig.symbol)
            return

        log.info(
            "💰 Buying  %-10s  %.3f SOL  [age=%.0fs  vel=%.2f  sp=%.0f%%  "
            "buyers=%d  cluster=%s]",
            sig.symbol, BUY_SIZE_SOL,
            sig.age_sec, sig.buy_vol_sol,
            sig.sell_pressure * 100,
            sig.unique_buyers, sig.cluster_ok,
        )

        result = await self.te.buy(mint=mint, symbol=sig.symbol)
        if result is None:
            log.warning("❌ Buy failed: %s", sig.symbol)
            return

        ctx = {
            "age_sec":        sig.age_sec,
            "buy_vol_sol":    sig.buy_vol_sol,
            "sell_pressure":  sig.sell_pressure,
            "unique_buyers":  sig.unique_buyers,
            "cluster_ok":     sig.cluster_ok,
            "slot_cluster_ok": sig.slot_cluster_ok,
        }
        self.rm.add_position(mint, result, ctx)

    # ── Background loops ──────────────────────────────────────────────────

    async def _risk_loop(self):
        """Apply TP/SL/emergency to all open positions every 1.5 seconds."""
        while True:
            try:
                await self.rm.check_all()
            except Exception as exc:
                log.debug("risk_loop error: %s", exc)
            await asyncio.sleep(1.5)

    async def _status_loop(self):
        """Print status every STATUS_INTERVAL seconds."""
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            uptime = (time.time() - self._start_time) / 60
            sc = self.scanner.status() if self.scanner else {}
            rm = self.rm.status() if self.rm else {}
            log.info(
                "📈 STATUS  uptime=%.0fmin  "
                "seen=%d  entered=%d  expired=%d  "
                "positions=%d  trades=%d  WR=%.0f%%  PnL=%+.3f SOL",
                uptime,
                sc.get("created", 0),
                sc.get("entered", 0),
                sc.get("expired", 0),
                rm.get("open_positions", 0),
                rm.get("total_trades", 0),
                rm.get("win_rate_pct", 0),
                rm.get("total_pnl_sol", 0),
            )

    # ── Config banner ─────────────────────────────────────────────────────

    def _print_config(self):
        mode = "🔴 LIVE" if not DRY_RUN else "🟡 DRY RUN"
        log.info("=" * 65)
        log.info("  Coordinated Launch Sniper  %s", mode)
        log.info("  Entry window:  %gs – %gs after creation", ENTRY_MIN_SEC, ENTRY_MAX_SEC)
        log.info("  Signals: cluster=%s  slot_buys≥%d  vel≥%.1fSOL"
                 "  sp<%.0f%%  buyers≥%d",
                 "Helius" if HELIUS_API_KEY else "DISABLED",
                 SLOT_CLUSTER_MIN, VELOCITY_MIN,
                 SELL_P_MAX * 100, BUYERS_MIN)
        log.info("  TP: +%.0f%% / +%.0f%% / +%.0f%%   SL: -%.0f%%   Emrg: sp>%.0f%%",
                 TP1_PCT * 100, TP2_PCT * 100, TP3_PCT * 100,
                 SL_PCT * 100, EMRG_SELL_P * 100)
        log.info("  Position: %.3f SOL  max=%d  hold≤%dmin",
                 BUY_SIZE_SOL, MAX_POSITIONS, MAX_HOLD_SEC // 60)
        log.info("=" * 65)


# ── Entry point ───────────────────────────────────────────────────────────────

async def _main():
    bot = SniperBot()
    loop = asyncio.get_event_loop()

    # Graceful shutdown on SIGINT / SIGTERM
    def _shutdown(sig_num, frame):
        log.info("Signal %d received — shutting down", sig_num)
        for task in asyncio.all_tasks(loop):
            task.cancel()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    await bot.run()


if __name__ == "__main__":
    asyncio.run(_main())
