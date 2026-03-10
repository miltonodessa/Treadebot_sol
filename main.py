"""
main.py — Coordinated Launch Sniper Bot
========================================
Parameters optimized from 24,261 round-trip trades of wallet D5dtjf (97 days):
  Win rate 27.65%  |  Profit factor 1.37  |  Net +237 SOL

Entry: 12–35s after token creation. ALL signals must fire:
  0. Pre-launch funding cluster   (Helius — source → 3+ wallets in 120s)
  1. First-slot bundle buys       (3+ wallets in first 2 seconds)
  2. Liquidity velocity           (≥ 2.0 SOL buy volume — raised from 1.0)
  3. Low sell pressure            (< 35%)
  4. Unique buyer count           (≥ 5 wallets)
  5. Sell absorption              (price not dropping > 20% on early sells)
  4b. Creator reputation          (< 10% survival rate from past launches → reject)

Exit (data-driven from D5dtjf analysis):
  NEVER sell before 5 min — dead zone 0–5min has only 13.7% WR
  TP1 = +40%  → sell 40%   (only after 5 min; skipped on pool-ride)
  TP2 = +80%  → sell 35%   (only after 5 min; skipped on pool-ride)
  TP3 = +120% → sell rest
  SL  = −10%  (was -25%; data shows 90th pct loss = -12%)
  Momentum exit: if pool grew < 20% after 10 min → token dead → exit
  Pool ride: pool growth ≥ 50% → WR 75% → defer TP1/TP2
  Emergency: sell pressure > 60%
  Max hold: 60 min (wallet 30–60 min zone = 74.9% WR)

Best trading hours UTC: 17–19, 22  (avoid 07–09)

Run:
  python main.py              # DRY RUN (default)
  DRY_RUN=0 python main.py   # LIVE
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

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s.%(msecs)03d  %(levelname)-5s  %(name)-12s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")

# ── Config ────────────────────────────────────────────────────────────────────

HELIUS_API_KEY   = os.getenv("HELIUS_API_KEY", "")
RPC_URL          = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
PRIVATE_KEY      = os.getenv("OUR_PRIVATE_KEY", "")
DRY_RUN          = bool(int(os.getenv("DRY_RUN", "1")))

# Entry signals
ENTRY_MIN_SEC    = float(os.getenv("ENTRY_MIN_SEC",    "12"))
ENTRY_MAX_SEC    = float(os.getenv("ENTRY_MAX_SEC",    "35"))
CLUSTER_WIN_SEC  = int(os.getenv("CLUSTER_WINDOW_SEC", "120"))
CLUSTER_MIN_W    = int(os.getenv("CLUSTER_MIN_WALLETS","3"))
CLUSTER_MIN_SOL  = float(os.getenv("CLUSTER_MIN_SOL",  "0.10"))
CLUSTER_MAX_SOL  = float(os.getenv("CLUSTER_MAX_SOL",  "1.50"))
# Velocity raised 1.0 → 2.0: pool ≥ 2 SOL at entry → +3% WR improvement
VELOCITY_MIN     = float(os.getenv("VELOCITY_MIN_SOL", "2.0"))
SELL_P_MAX       = float(os.getenv("SELL_PRESSURE_MAX","0.35"))
BUYERS_MIN       = int(os.getenv("BUYERS_MIN",         "5"))
SLOT_CLUSTER_MIN = int(os.getenv("SLOT_CLUSTER_MIN",   "3"))

# Time filter: best hours 17-19, 22 UTC; worst 07-09 UTC (negative total PnL)
# Comma-separated UTC hours to avoid. Empty = no filter.
AVOID_HOURS_RAW  = os.getenv("AVOID_HOURS_UTC", "7,8,9")
AVOID_HOURS_UTC: set[int] = {
    int(h) for h in AVOID_HOURS_RAW.split(",") if h.strip().isdigit()
}

# Trade
BUY_SIZE_SOL     = float(os.getenv("BUY_SIZE_SOL",     "0.10"))
SLIPPAGE_PCT     = float(os.getenv("SLIPPAGE_PCT",     "25.0"))
PRIORITY_FEE     = float(os.getenv("PRIORITY_FEE_SOL", "0.0001"))
MAX_POSITIONS    = int(os.getenv("MAX_POSITIONS",       "5"))

# TP levels
TP1_PCT          = float(os.getenv("TP1_PCT",           "0.40"))
TP2_PCT          = float(os.getenv("TP2_PCT",           "0.80"))
TP3_PCT          = float(os.getenv("TP3_PCT",           "1.20"))
TP1_FRAC         = float(os.getenv("TP1_FRAC",          "0.40"))
TP2_FRAC         = float(os.getenv("TP2_FRAC",          "0.35"))
TP3_FRAC         = float(os.getenv("TP3_FRAC",          "1.00"))

# SL: -10% (was -25%; data: 90th pct loss = -12%, median = -3.9%)
SL_PCT           = float(os.getenv("SL_PCT",            "0.10"))

# Emergency exit
EMRG_SELL_P      = float(os.getenv("EMRG_SELL_PRESSURE","0.60"))

# Minimum hold before ANY TP: dead zone 0–5min → WR only 13.7%
MIN_HOLD_SEC     = int(os.getenv("MIN_HOLD_SEC",        "300"))   # 5 minutes

# Momentum exit: exit if pool barely moved after 10 min
MOMENTUM_EXIT_SEC       = int(os.getenv("MOMENTUM_EXIT_SEC",        "600"))   # 10 min
MOMENTUM_MIN_POOL_GR    = float(os.getenv("MOMENTUM_MIN_POOL_GROWTH","0.20")) # 20%

# Pool ride: pool growth ≥ 50% → WR 75% → defer TP1/TP2
POOL_RIDE_THRESHOLD     = float(os.getenv("POOL_RIDE_THRESHOLD",    "0.50"))

# Max hold: 60 min (30–60 min zone = 74.9% WR in wallet data)
MAX_HOLD_SEC     = int(os.getenv("MAX_HOLD_SEC",        "3600"))  # 60 min

STATUS_INTERVAL  = int(os.getenv("STATUS_INTERVAL",    "60"))

# ── Imports ───────────────────────────────────────────────────────────────────

from velocity_analyzer       import VelocityAnalyzer, TokenSignals
from wallet_cluster_detector import WalletClusterDetector
from creator_reputation      import CreatorReputation
from token_scanner           import TokenScanner
from trade_engine            import TradeEngine
from risk_manager            import RiskManager


# ─────────────────────────────────────────────────────────────────────────────

class SniperBot:

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.va:      Optional[VelocityAnalyzer]      = None
        self.cd:      Optional[WalletClusterDetector] = None
        self.cr:      Optional[CreatorReputation]     = None
        self.te:      Optional[TradeEngine]            = None
        self.rm:      Optional[RiskManager]            = None
        self.scanner: Optional[TokenScanner]           = None
        self._start_time = time.time()

    async def setup(self):
        connector = aiohttp.TCPConnector(ssl=False, limit=64, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(connector=connector)

        self.va = VelocityAnalyzer(max_tracked=2000)

        if HELIUS_API_KEY:
            self.cd = WalletClusterDetector(
                api_key=HELIUS_API_KEY, session=self.session,
                window_sec=CLUSTER_WIN_SEC, min_wallets=CLUSTER_MIN_W,
                min_sol=CLUSTER_MIN_SOL, max_sol=CLUSTER_MAX_SOL,
            )
            self.cr = CreatorReputation(
                api_key=HELIUS_API_KEY, session=self.session,
                min_launches=int(os.getenv("REPUTATION_MIN_LAUNCHES", "5")),
                min_survival_rate=float(os.getenv("REPUTATION_MIN_SURVIVAL", "0.10")),
                survival_mcap_usd=float(os.getenv("REPUTATION_SURVIVAL_MCAP", "5000")),
            )
            log.info("✅ Helius: WalletClusterDetector + CreatorReputation enabled")
        else:
            log.warning("⚠️  HELIUS_API_KEY not set — signals 0 & 4b auto-approved")

        self.te = TradeEngine(
            private_key_b58=PRIVATE_KEY, rpc_url=RPC_URL,
            session=self.session, buy_size_sol=BUY_SIZE_SOL,
            slippage_pct=SLIPPAGE_PCT, priority_fee_sol=PRIORITY_FEE,
            dry_run=DRY_RUN,
        )

        _ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.rm = RiskManager(
            trade_engine=self.te,
            velocity_analyzer=self.va,
            tp1_pct=TP1_PCT, tp2_pct=TP2_PCT, tp3_pct=TP3_PCT,
            tp1_frac=TP1_FRAC, tp2_frac=TP2_FRAC, tp3_frac=TP3_FRAC,
            sl_pct=SL_PCT,
            sell_pressure_emergency=EMRG_SELL_P,
            min_hold_sec=MIN_HOLD_SEC,
            momentum_exit_sec=MOMENTUM_EXIT_SEC,
            momentum_min_pool_growth=MOMENTUM_MIN_POOL_GR,
            pool_ride_threshold=POOL_RIDE_THRESHOLD,
            max_hold_sec=MAX_HOLD_SEC,
            max_positions=MAX_POSITIONS,
            trades_csv=f"trades_sniper_{_ts}.csv",
            dry_run=DRY_RUN,
        )

        self.scanner = TokenScanner(
            velocity_analyzer=self.va,
            cluster_detector=self.cd,
            on_entry=self.on_entry_signal,
            session=self.session,
            entry_min_sec=ENTRY_MIN_SEC,
            entry_max_sec=ENTRY_MAX_SEC,
            reputation_checker=self.cr,
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
        # Time-of-day filter (worst hours UTC 07-09)
        if AVOID_HOURS_UTC:
            from datetime import datetime as _dt, timezone as _tz
            hour = _dt.now(_tz.utc).hour
            if hour in AVOID_HOURS_UTC:
                log.debug("⏰ %s: skipped — avoid hour %02d UTC", sig.symbol, hour)
                return

        if not self.rm.can_open():
            log.info("⛔ %s: max positions (%d/%d)",
                     sig.symbol, len(self.rm.positions), MAX_POSITIONS)
            return

        # Dedup by symbol
        sym = sig.symbol.upper()
        if any(p.symbol.upper() == sym for p in self.rm.positions.values()):
            log.debug("⛔ %s: duplicate symbol in portfolio", sig.symbol)
            return

        log.info(
            "💰 Buying  %-10s  %.3f SOL  "
            "[age=%.0fs  vel=%.2f  pool=%.1f  sp=%.0f%%  buyers=%d  cluster=%s]",
            sig.symbol, BUY_SIZE_SOL,
            sig.age_sec, sig.buy_vol_sol, sig.entry_pool_sol,
            sig.sell_pressure * 100, sig.unique_buyers, sig.cluster_ok,
        )

        result = await self.te.buy(mint=mint, symbol=sig.symbol)
        if result is None:
            log.warning("❌ Buy failed: %s", sig.symbol)
            return

        self.rm.add_position(mint, result, {
            "age_sec":         sig.age_sec,
            "buy_vol_sol":     sig.buy_vol_sol,
            "entry_pool_sol":  sig.entry_pool_sol,
            "sell_pressure":   sig.sell_pressure,
            "unique_buyers":   sig.unique_buyers,
            "cluster_ok":      sig.cluster_ok,
            "slot_cluster_ok": sig.slot_cluster_ok,
        })

    # ── Background loops ──────────────────────────────────────────────────

    async def _risk_loop(self):
        while True:
            try:
                await self.rm.check_all()
            except Exception as exc:
                log.debug("risk_loop error: %s", exc)
            await asyncio.sleep(1.5)

    async def _status_loop(self):
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            uptime = (time.time() - self._start_time) / 60
            sc = self.scanner.status() if self.scanner else {}
            rm = self.rm.status() if self.rm else {}
            log.info(
                "📈 STATUS  uptime=%.0fmin  seen=%d  entered=%d  expired=%d  "
                "positions=%d  trades=%d  WR=%.0f%%  PnL=%+.3f SOL",
                uptime,
                sc.get("created", 0), sc.get("entered", 0), sc.get("expired", 0),
                rm.get("open_positions", 0), rm.get("total_trades", 0),
                rm.get("win_rate_pct", 0), rm.get("total_pnl_sol", 0),
            )

    # ── Config banner ─────────────────────────────────────────────────────

    def _print_config(self):
        mode = "🔴 LIVE" if not DRY_RUN else "🟡 DRY RUN"
        avoid = ",".join(str(h) for h in sorted(AVOID_HOURS_UTC)) or "none"
        log.info("=" * 68)
        log.info("  Coordinated Launch Sniper  %s  (calibrated: D5dtjf 24k trades)", mode)
        log.info("  Entry:    %gs–%gs  vel≥%.1fSOL  sp<%.0f%%  buyers≥%d  slot≥%d",
                 ENTRY_MIN_SEC, ENTRY_MAX_SEC, VELOCITY_MIN,
                 SELL_P_MAX * 100, BUYERS_MIN, SLOT_CLUSTER_MIN)
        log.info("  Cluster:  %s  Reputation: %s  Avoid UTC hours: %s",
                 "Helius" if HELIUS_API_KEY else "OFF",
                 "Helius" if HELIUS_API_KEY else "OFF",
                 avoid)
        log.info("  TP:  +%.0f%%→sell%.0f%%  +%.0f%%→sell%.0f%%  +%.0f%%→sell ALL",
                 TP1_PCT*100, TP1_FRAC*100,
                 TP2_PCT*100, TP2_FRAC*100,
                 TP3_PCT*100)
        log.info("  SL: -%.0f%%  Emrg-sp: %.0f%%  min_hold: %ds  max_hold: %dmin",
                 SL_PCT*100, EMRG_SELL_P*100, MIN_HOLD_SEC, MAX_HOLD_SEC//60)
        log.info("  Momentum: exit if pool<+%.0f%% after %ds  | ride if pool>+%.0f%%",
                 MOMENTUM_MIN_POOL_GR*100, MOMENTUM_EXIT_SEC, POOL_RIDE_THRESHOLD*100)
        log.info("  Position: %.3f SOL  max=%d",
                 BUY_SIZE_SOL, MAX_POSITIONS)
        log.info("=" * 68)


# ── Entry point ───────────────────────────────────────────────────────────────

async def _main():
    bot = SniperBot()
    loop = asyncio.get_event_loop()

    def _shutdown(sig_num, _frame):
        log.info("Signal %d — shutting down", sig_num)
        for task in asyncio.all_tasks(loop):
            task.cancel()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    await bot.run()


if __name__ == "__main__":
    asyncio.run(_main())
