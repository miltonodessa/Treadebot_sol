"""
PUMPSCALP v1.0 — Pump.fun Early Entry Scalping Bot
Стратегия из trading_strategy.docx

Суть: покупать Pump.fun токены в первые 5-30 мин при MC < 500 SOL,
если Score ≥ 70/195. Выход по TP1/TP2/TP3/SL/trailing/time-stop.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import websockets
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction
from base64 import b64decode

import config

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pumpscalp")

# ─── Runtime config ───────────────────────────────────────────────────────────
DRY_RUN             = os.getenv("DRY_RUN", "true").lower() == "true"
VIRTUAL_BALANCE_SOL = float(os.getenv("VIRTUAL_BALANCE_SOL", "50.0"))
BIRDEYE_API_KEY     = os.getenv("BIRDEYE_API_KEY", "")

# Score / entry
SCORE_THRESHOLD    = int(os.getenv("SCORE_THRESHOLD", "70"))
MAX_TOKEN_AGE_MIN  = int(os.getenv("MAX_TOKEN_AGE_MIN", "30"))   # enter only < 30 min
MAX_WATCH_AGE_MIN  = int(os.getenv("MAX_WATCH_AGE_MIN", "120"))  # stop watching after 2h
SCAN_INTERVAL_SEC  = int(os.getenv("SCAN_INTERVAL_SEC", "30"))   # rescan every 30s

# Position sizing (Kelly 1/4 ÷ 5 positions = 2.3% per trade)
MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS", "5"))
POSITION_SIZE_PCT  = float(os.getenv("POSITION_SIZE_PCT", "0.023"))

# Exit parameters
SL_PCT             = float(os.getenv("SL_PCT", "0.08"))          # -8% stop-loss
TP1_PCT            = float(os.getenv("TP1_PCT", "0.15"))         # +15% → sell 50%
TP2_PCT            = float(os.getenv("TP2_PCT", "0.30"))         # +30% → sell 35%
TP3_PCT            = float(os.getenv("TP3_PCT", "1.00"))         # +100% → sell rest
TP1_FRACTION       = float(os.getenv("TP1_FRACTION", "0.50"))
TP2_FRACTION       = float(os.getenv("TP2_FRACTION", "0.35"))
TIME_STOP_MIN      = int(os.getenv("TIME_STOP_MIN", "45"))       # force exit after 45 min
TRAILING_TRIGGER   = float(os.getenv("TRAILING_TRIGGER", "0.20")) # after +20% activate trailing
TRAILING_SL_PCT    = float(os.getenv("TRAILING_SL_PCT", "0.10")) # trailing SL at +10%

# Risk
DAILY_LOSS_LIMIT_PCT = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.05"))  # -5% bank → stop

# Execution
SLIPPAGE_BPS            = int(os.getenv("SLIPPAGE_BPS", "150"))
PRIORITY_FEE_MICROLAMPORTS = int(os.getenv("PRIORITY_FEE_MICROLAMPORTS", "500000"))

# Anti-rugpull thresholds
MIN_LIQUIDITY_SOL  = float(os.getenv("MIN_LIQUIDITY_SOL", "10.0"))
MAX_TOP10_HOLDER_PCT = float(os.getenv("MAX_TOP10_HOLDER_PCT", "0.50"))
MIN_BUY_PRESSURE   = float(os.getenv("MIN_BUY_PRESSURE", "0.70"))
MIN_UNIQUE_WALLETS = int(os.getenv("MIN_UNIQUE_WALLETS", "5"))
VOL_SURGE_MULTIPLIER = float(os.getenv("VOL_SURGE_MULTIPLIER", "3.0"))

# API
PUMPPORTAL_WSS = "wss://pumpportal.fun/api/data"
JUPITER_QUOTE  = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP   = "https://quote-api.jup.ag/v6/swap"
JUPITER_PRICE  = "https://price.jup.ag/v4/price"
BIRDEYE_TOKEN  = "https://public-api.birdeye.so/defi/token_overview"
WSOL_MINT      = "So11111111111111111111111111111111111111112"


# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class TokenWatch:
    """Token discovered via PumpPortal, being monitored for entry signal."""
    mint:               str
    name:               str
    symbol:             str
    creator:            str          # dev wallet
    created_at:         float        # unix timestamp
    initial_sol:        float        # virtual_sol_reserves at creation
    total_supply:       int          # total token supply


@dataclass
class Position:
    mint:           str
    name:           str
    symbol:         str
    entry_price:    float            # SOL per token
    size_sol:       float            # SOL invested
    token_balance:  int              # tokens held
    opened_at:      float

    # Exit levels
    sl_price:       float
    tp1_price:      float
    tp2_price:      float
    tp3_price:      float
    time_stop_at:   float

    # State
    tp1_done:       bool = False
    tp2_done:       bool = False
    trailing_sl:    Optional[float] = None   # price, once activated
    pnl_sol:        float = 0.0


@dataclass
class Stats:
    signals_seen:   int = 0
    signals_entered: int = 0
    trades_total:   int = 0
    trades_win:     int = 0
    session_pnl:    float = 0.0
    daily_pnl:      float = 0.0
    day_stopped:    bool = False


# ─── Global state ─────────────────────────────────────────────────────────────

class BotState:
    def __init__(self):
        self.bank: float = VIRTUAL_BALANCE_SOL if DRY_RUN else 0.0
        self.positions: dict[str, Position] = {}
        self.watchlist: dict[str, TokenWatch] = {}
        self.stats = Stats()
        self._lock = asyncio.Lock()

    def in_daily_loss(self) -> bool:
        return self.stats.daily_pnl <= -(self.bank * DAILY_LOSS_LIMIT_PCT)


state = BotState()

# ─── SSL context ──────────────────────────────────────────────────────────────
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

# ─── Keypair (live mode only) ─────────────────────────────────────────────────
_keypair: Optional[Keypair] = None

def _load_keypair() -> Optional[Keypair]:
    pk = config.OUR_PRIVATE_KEY
    if not pk:
        return None
    try:
        import base58
        return Keypair.from_bytes(base58.b58decode(pk))
    except Exception as e:
        log.error("Keypair load error: %s", e)
        return None


# ─── Jupiter helpers ──────────────────────────────────────────────────────────

async def jup_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    session: aiohttp.ClientSession,
) -> Optional[dict]:
    try:
        async with session.get(
            JUPITER_QUOTE,
            params={
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": amount,
                "slippageBps": SLIPPAGE_BPS,
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                log.debug("jup_quote %s: %s", r.status, await r.text())
                return None
            return await r.json()
    except Exception as e:
        log.debug("jup_quote error: %s", e)
        return None


async def jup_swap_tx(quote: dict, session: aiohttp.ClientSession) -> Optional[str]:
    """Returns base64 swap transaction or None."""
    if _keypair is None:
        return None
    payload = {
        "quoteResponse": quote,
        "userPublicKey": str(_keypair.pubkey()),
        "wrapAndUnwrapSol": True,
        "prioritizationFeeLamports": PRIORITY_FEE_MICROLAMPORTS,
    }
    try:
        async with session.post(
            JUPITER_SWAP,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                log.warning("jup_swap %s: %s", r.status, await r.text())
                return None
            d = await r.json()
            return d.get("swapTransaction")
    except Exception as e:
        log.warning("jup_swap_tx error: %s", e)
        return None


async def send_tx(tx64: str, session: aiohttp.ClientSession) -> Optional[str]:
    """Sign and send a base64 versioned transaction. Returns signature."""
    if _keypair is None:
        return None
    try:
        raw = b64decode(tx64)
        tx = VersionedTransaction.from_bytes(raw)
        tx.sign([_keypair])
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "sendTransaction",
            "params": [
                tx.to_json(),
                {"encoding": "base64", "skipPreflight": False,
                 "maxRetries": 3, "preflightCommitment": "confirmed"},
            ],
        }
        async with session.post(
            config.RPC_URL,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            d = await r.json()
            if "error" in d:
                log.warning("sendTransaction error: %s", d["error"])
                return None
            return d.get("result")
    except Exception as e:
        log.warning("send_tx error: %s", e)
        return None


# ─── Price / market data ──────────────────────────────────────────────────────

async def get_sol_balance(session: aiohttp.ClientSession) -> float:
    if DRY_RUN:
        return state.bank
    try:
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getBalance",
            "params": [config.OUR_WALLET, {"commitment": "confirmed"}],
        }
        async with session.post(config.RPC_URL, json=payload,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            d = await r.json()
            return d["result"]["value"] / 1e9
    except Exception:
        return state.bank


async def get_token_price_sol(mint: str, session: aiohttp.ClientSession) -> Optional[float]:
    """Returns token price in SOL via Jupiter quote (1 token → SOL)."""
    try:
        async with session.get(
            JUPITER_PRICE,
            params={"ids": f"{mint},{WSOL_MINT}"},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status != 200:
                return None
            d = await r.json()
            data = d.get("data", {})
            token_usd = data.get(mint, {}).get("price", 0)
            sol_usd   = data.get(WSOL_MINT, {}).get("price", 1)
            if sol_usd == 0 or token_usd == 0:
                return None
            return token_usd / sol_usd
    except Exception:
        return None


async def get_birdeye_data(mint: str, session: aiohttp.ClientSession) -> dict:
    """Fetch token_overview from Birdeye. Returns {} on failure."""
    if not BIRDEYE_API_KEY:
        return {}
    try:
        async with session.get(
            BIRDEYE_TOKEN,
            params={"address": mint},
            headers={"X-API-KEY": BIRDEYE_API_KEY},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status != 200:
                return {}
            d = await r.json()
            return d.get("data", {})
    except Exception:
        return {}


async def get_top10_holder_pct(mint: str, session: aiohttp.ClientSession) -> float:
    """Returns fraction held by top-10 accounts (0.0–1.0). 1.0 on failure (conservative)."""
    try:
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenLargestAccounts",
            "params": [mint, {"commitment": "confirmed"}],
        }
        async with session.post(config.RPC_URL, json=payload,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            d = await r.json()
            accounts = d.get("result", {}).get("value", [])
            if not accounts:
                return 1.0
            total = sum(int(a.get("amount", 0)) for a in accounts)
            top10 = sum(int(a.get("amount", 0)) for a in accounts[:10])
            return top10 / total if total > 0 else 1.0
    except Exception:
        return 1.0


async def get_dev_sells(creator: str, mint: str, session: aiohttp.ClientSession) -> int:
    """
    Check if dev wallet has sold tokens in the last hour.
    Returns number of sell transactions found.
    """
    try:
        url = f"https://api.helius.xyz/v0/addresses/{creator}/transactions"
        async with session.get(
            url,
            params={"api-key": config.HELIUS_API_KEY, "limit": 50, "type": "SWAP"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return 0
            txs = await r.json()
        now = time.time()
        sells = 0
        for tx in txs:
            if now - tx.get("timestamp", 0) > 3600:
                break
            # Look for token transfers sending mint away from creator
            for transfer in tx.get("tokenTransfers", []):
                if (transfer.get("mint") == mint
                        and transfer.get("fromUserAccount") == creator):
                    sells += 1
        return sells
    except Exception:
        return 0


# ─── Score engine ─────────────────────────────────────────────────────────────

async def score_token(
    watch: TokenWatch,
    session: aiohttp.ClientSession,
) -> tuple[int, dict]:
    """
    Calculate entry score (0–195). Returns (score, details).
    Scoring from trading_strategy.docx section 3.1:
      +30  MC < 200 SOL
      +20  MC 200–500 SOL
      +25  Age < 30 min
      +15  Pump.fun platform
      +20  Volume 5m > 3× avg
      +15  Buy pressure > 70%
      +25  ≥5 unique wallets / 5min
      +20  Dev wallet no sells
      +10  Liquidity > 10 SOL
      +15  No rugpull (top-10 < 50%)
    """
    score = 0
    details: dict[str, object] = {}

    # ── Fetch live data ──────────────────────────────────────────────────────
    bird, top10_pct = await asyncio.gather(
        get_birdeye_data(watch.mint, session),
        get_top10_holder_pct(watch.mint, session),
    )
    dev_sells = await get_dev_sells(watch.creator, watch.mint, session)

    # ── MC (SOL) ─────────────────────────────────────────────────────────────
    mc_usd = bird.get("mc", 0) or bird.get("realMc", 0)
    sol_price_usd = 200.0  # fallback; will try to get live below
    try:
        price_field = bird.get("price", 0)
        if price_field and watch.total_supply:
            mc_usd = price_field * watch.total_supply
    except Exception:
        pass

    # Estimate SOL price from Birdeye (use WSOL data or fallback)
    mc_sol = mc_usd / sol_price_usd if sol_price_usd > 0 else 0

    if mc_sol > 0:
        if mc_sol < 200:
            score += 30
            details["mc"] = f"ultra-early {mc_sol:.0f} SOL (+30)"
        elif mc_sol < 500:
            score += 20
            details["mc"] = f"early {mc_sol:.0f} SOL (+20)"
        else:
            details["mc"] = f"{mc_sol:.0f} SOL (0)"
    else:
        details["mc"] = "unknown"

    # ── Age ──────────────────────────────────────────────────────────────────
    age_sec = time.time() - watch.created_at
    age_min = age_sec / 60
    if age_min < 30:
        score += 25
        details["age"] = f"{age_min:.1f} min (+25)"
    else:
        details["age"] = f"{age_min:.1f} min (0)"

    # ── Pump.fun platform (always true since we watch PumpPortal) ────────────
    score += 15
    details["platform"] = "pump.fun (+15)"

    # ── Volume surge (5min) ──────────────────────────────────────────────────
    vol_5m   = bird.get("v5m", 0) or 0
    vol_1h   = bird.get("v1h", 0) or 0
    avg_vol_5m = vol_1h / 12 if vol_1h > 0 else 0   # 1h / 12 = per-5min average
    if avg_vol_5m > 0 and vol_5m >= avg_vol_5m * VOL_SURGE_MULTIPLIER:
        score += 20
        details["volume"] = f"surge {vol_5m:.0f}$ vs avg {avg_vol_5m:.0f}$ (+20)"
    else:
        details["volume"] = f"5m={vol_5m:.0f}$ avg={avg_vol_5m:.0f}$ (0)"

    # ── Buy pressure ─────────────────────────────────────────────────────────
    buy_5m  = bird.get("buy5m", 0) or 0
    sell_5m = bird.get("sell5m", 0) or 0
    total_5m = buy_5m + sell_5m
    buy_pct = buy_5m / total_5m if total_5m > 0 else 0
    if buy_pct >= MIN_BUY_PRESSURE:
        score += 15
        details["buy_pressure"] = f"{buy_pct:.0%} (+15)"
    else:
        details["buy_pressure"] = f"{buy_pct:.0%} (0)"

    # ── Unique wallets 5min ──────────────────────────────────────────────────
    unique_5m = bird.get("uniqueWallet5m", 0) or 0
    if unique_5m >= MIN_UNIQUE_WALLETS:
        score += 25
        details["unique_wallets"] = f"{unique_5m} (+25)"
    else:
        details["unique_wallets"] = f"{unique_5m} (0)"

    # ── Dev wallet sells ─────────────────────────────────────────────────────
    if dev_sells == 0:
        score += 20
        details["dev_sells"] = "clean (+20)"
    else:
        details["dev_sells"] = f"{dev_sells} sells (0)"

    # ── Liquidity ────────────────────────────────────────────────────────────
    liquidity_sol = watch.initial_sol  # from PumpPortal virtual_sol_reserves
    liq_usd = bird.get("liquidity", 0) or 0
    if liq_usd > 0 and sol_price_usd > 0:
        liquidity_sol = liq_usd / sol_price_usd
    if liquidity_sol >= MIN_LIQUIDITY_SOL:
        score += 10
        details["liquidity"] = f"{liquidity_sol:.1f} SOL (+10)"
    else:
        details["liquidity"] = f"{liquidity_sol:.1f} SOL (0)"

    # ── Anti-rugpull (top-10 holders) ────────────────────────────────────────
    if top10_pct < MAX_TOP10_HOLDER_PCT:
        score += 15
        details["top10"] = f"{top10_pct:.0%} (+15)"
    else:
        details["top10"] = f"{top10_pct:.0%} concentrated (0)"

    details["total_score"] = score
    return score, details


# ─── Trade execution ──────────────────────────────────────────────────────────

async def execute_buy(
    watch: TokenWatch,
    size_sol: float,
    session: aiohttp.ClientSession,
) -> Optional[Position]:
    """Buy token. Returns Position on success."""
    lamports = int(size_sol * 1e9)

    if DRY_RUN:
        # Estimate price via Jupiter quote
        quote = await jup_quote(WSOL_MINT, watch.mint, lamports, session)
        if not quote:
            log.warning("[DRY] No quote for %s, skip", watch.symbol)
            return None
        tokens_out = int(quote.get("outAmount", 0))
        if tokens_out == 0:
            return None
        entry_price = size_sol / tokens_out  # SOL per token
        log.info("🟡 [DRY] BUY  %-10s  %.3f SOL → %d tokens  (~%.8f SOL/tok)",
                 watch.symbol, size_sol, tokens_out, entry_price)
    else:
        quote = await jup_quote(WSOL_MINT, watch.mint, lamports, session)
        if not quote:
            log.warning("No quote for %s", watch.symbol)
            return None
        tokens_out = int(quote.get("outAmount", 0))
        tx64 = await jup_swap_tx(quote, session)
        if not tx64:
            return None
        sig = await send_tx(tx64, session)
        if not sig:
            log.warning("Buy TX failed for %s", watch.symbol)
            return None
        entry_price = size_sol / max(tokens_out, 1)
        log.info("🟢 BUY  %-10s  %.3f SOL → %d tokens  sig=%.8s",
                 watch.symbol, size_sol, tokens_out, sig)

    now = time.time()
    pos = Position(
        mint          = watch.mint,
        name          = watch.name,
        symbol        = watch.symbol,
        entry_price   = entry_price,
        size_sol      = size_sol,
        token_balance = tokens_out,
        opened_at     = now,
        sl_price      = entry_price * (1 - SL_PCT),
        tp1_price     = entry_price * (1 + TP1_PCT),
        tp2_price     = entry_price * (1 + TP2_PCT),
        tp3_price     = entry_price * (1 + TP3_PCT),
        time_stop_at  = now + TIME_STOP_MIN * 60,
    )

    if DRY_RUN:
        state.bank -= size_sol

    return pos


async def execute_sell(
    pos: Position,
    fraction: float,
    reason: str,
    current_price: float,
    session: aiohttp.ClientSession,
) -> float:
    """Sell `fraction` of position. Returns SOL received."""
    to_sell = int(pos.token_balance * fraction)
    if to_sell <= 0:
        return 0.0

    if DRY_RUN:
        sol_back = to_sell * current_price
        log.info("🟡 [DRY] SELL %-10s  %d%% @ %.8f SOL/tok  → +%.4f SOL  [%s]",
                 pos.symbol, int(fraction * 100), current_price, sol_back, reason)
    else:
        quote = await jup_quote(pos.mint, WSOL_MINT, to_sell, session)
        if not quote:
            log.warning("No sell quote for %s", pos.symbol)
            return 0.0
        sol_back = int(quote.get("outAmount", 0)) / 1e9
        tx64 = await jup_swap_tx(quote, session)
        if not tx64:
            return 0.0
        sig = await send_tx(tx64, session)
        if not sig:
            log.warning("Sell TX failed for %s", pos.symbol)
            return 0.0
        log.info("🔴 SELL %-10s  %d%% → +%.4f SOL  [%s]  sig=%.8s",
                 pos.symbol, int(fraction * 100), sol_back, reason, sig)

    pos.token_balance -= to_sell
    pos.pnl_sol += sol_back
    if DRY_RUN:
        state.bank += sol_back

    return sol_back


# ─── Position manager ─────────────────────────────────────────────────────────

async def manage_positions(session: aiohttp.ClientSession):
    """Check TP/SL/trailing/time-stop for all open positions."""
    if not state.positions:
        return

    to_close: list[str] = []

    for mint, pos in list(state.positions.items()):
        price = await get_token_price_sol(mint, session)
        if price is None:
            continue

        now = time.time()
        pnl_pct = (price - pos.entry_price) / pos.entry_price

        # ── Trailing stop ────────────────────────────────────────────────────
        if pnl_pct >= TRAILING_TRIGGER and pos.trailing_sl is None:
            pos.trailing_sl = pos.entry_price * (1 + TRAILING_SL_PCT)
            log.info("⚡ Trailing SL activated for %s @ %.8f SOL (+%.0f%%)",
                     pos.symbol, pos.trailing_sl, TRAILING_SL_PCT * 100)

        if pos.trailing_sl and price > pos.trailing_sl:
            # Update trailing level upward
            new_trail = price * (1 - (TRAILING_TRIGGER - TRAILING_SL_PCT))
            if new_trail > pos.trailing_sl:
                pos.trailing_sl = new_trail

        # ── Stop-loss ────────────────────────────────────────────────────────
        sl = pos.trailing_sl if pos.trailing_sl else pos.sl_price
        if price <= sl:
            await execute_sell(pos, 1.0, f"SL {pnl_pct:+.0%}", price, session)
            to_close.append(mint)
            _record_trade(pos)
            continue

        # ── Time-stop ────────────────────────────────────────────────────────
        if now >= pos.time_stop_at:
            await execute_sell(pos, 1.0, f"TIME-STOP {pnl_pct:+.0%}", price, session)
            to_close.append(mint)
            _record_trade(pos)
            continue

        # ── TP3 ──────────────────────────────────────────────────────────────
        if pos.tp1_done and pos.tp2_done and price >= pos.tp3_price:
            await execute_sell(pos, 1.0, f"TP3 +{TP3_PCT:.0%}", price, session)
            to_close.append(mint)
            _record_trade(pos)
            continue

        # ── TP2 ──────────────────────────────────────────────────────────────
        if pos.tp1_done and not pos.tp2_done and price >= pos.tp2_price:
            await execute_sell(pos, TP2_FRACTION, f"TP2 +{TP2_PCT:.0%}", price, session)
            pos.tp2_done = True
            continue

        # ── TP1 ──────────────────────────────────────────────────────────────
        if not pos.tp1_done and price >= pos.tp1_price:
            await execute_sell(pos, TP1_FRACTION, f"TP1 +{TP1_PCT:.0%}", price, session)
            pos.tp1_done = True
            continue

    for mint in to_close:
        state.positions.pop(mint, None)


def _record_trade(pos: Position):
    """Record closed trade in stats."""
    net = pos.pnl_sol - pos.size_sol
    state.stats.trades_total += 1
    state.stats.session_pnl += net
    state.stats.daily_pnl   += net
    if net > 0:
        state.stats.trades_win += 1
    log.info("📋 Closed %-10s  PnL: %+.4f SOL", pos.symbol, net)


# ─── Signal scan loop ─────────────────────────────────────────────────────────

async def scan_loop(session: aiohttp.ClientSession):
    """Every SCAN_INTERVAL_SEC: score watchlist tokens and enter if threshold met."""
    while True:
        await asyncio.sleep(SCAN_INTERVAL_SEC)

        if state.stats.day_stopped:
            continue

        if state.in_daily_loss():
            log.warning("🚫 Daily loss limit hit (%.1f%%), stopping for today.",
                        DAILY_LOSS_LIMIT_PCT * 100)
            state.stats.day_stopped = True
            continue

        now = time.time()
        expired = [m for m, w in state.watchlist.items()
                   if now - w.created_at > MAX_WATCH_AGE_MIN * 60]
        for m in expired:
            state.watchlist.pop(m, None)

        candidates = [
            w for w in state.watchlist.values()
            if w.mint not in state.positions
            and (now - w.created_at) / 60 <= MAX_TOKEN_AGE_MIN
        ]

        for watch in candidates:
            if len(state.positions) >= MAX_POSITIONS:
                break

            score, details = await score_token(watch, session)
            state.stats.signals_seen += 1

            age_min = (now - watch.created_at) / 60
            log.info(
                "🔍 %-10s  score=%d/195  age=%.1fmin  MC=%s  liq=%s  "
                "vol=%s  buy=%s  wallets=%s  dev=%s  top10=%s",
                watch.symbol, score, age_min,
                details.get("mc", "?"), details.get("liquidity", "?"),
                details.get("volume", "?"), details.get("buy_pressure", "?"),
                details.get("unique_wallets", "?"), details.get("dev_sells", "?"),
                details.get("top10", "?"),
            )

            if score < SCORE_THRESHOLD:
                continue

            # ── Enter position ───────────────────────────────────────────────
            balance = await get_sol_balance(session)
            trade_sol = min(balance * POSITION_SIZE_PCT, balance * 0.10)
            if trade_sol < 0.05:
                log.warning("Insufficient balance for %s (%.3f SOL)", watch.symbol, balance)
                continue

            log.info("✅ SIGNAL  %-10s  score=%d  size=%.3f SOL",
                     watch.symbol, score, trade_sol)

            pos = await execute_buy(watch, trade_sol, session)
            if pos:
                state.positions[watch.mint] = pos
                state.stats.signals_entered += 1
                state.watchlist.pop(watch.mint, None)

            await asyncio.sleep(0.5)   # brief pause between entries


# ─── PumpPortal WebSocket ─────────────────────────────────────────────────────

async def pumpportal_listener():
    """Subscribe to new Pump.fun token events via PumpPortal WebSocket."""
    while True:
        try:
            async with websockets.connect(
                PUMPPORTAL_WSS, ping_interval=None, ssl=_ssl_ctx
            ) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("✅ PumpPortal WS connected — watching for new tokens")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    mint = msg.get("mint")
                    if not mint:
                        continue

                    # Skip if already watching or already in a position
                    if mint in state.watchlist or mint in state.positions:
                        continue

                    name    = msg.get("name", "?")
                    symbol  = msg.get("symbol", "?")
                    creator = msg.get("traderPublicKey") or msg.get("creator", "")
                    ts      = msg.get("timestamp") or msg.get("created_timestamp")
                    if ts and ts > 1e12:
                        ts = ts / 1000   # ms → s
                    created_at = float(ts) if ts else time.time()

                    vsol    = msg.get("virtualSolReserves", 0)
                    if vsol and vsol > 1e9:
                        vsol = vsol / 1e9  # lamports → SOL
                    supply  = int(msg.get("totalSupply") or msg.get("vTokensInBondingCurve") or 1_000_000_000)

                    watch = TokenWatch(
                        mint=mint, name=name, symbol=symbol,
                        creator=creator, created_at=created_at,
                        initial_sol=float(vsol), total_supply=supply,
                    )
                    state.watchlist[mint] = watch
                    log.debug("👀 New token: %-10s  %s  liq=%.1f SOL",
                              symbol, mint[:8], vsol)

        except websockets.ConnectionClosed:
            log.warning("PumpPortal WS closed, reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            log.error("PumpPortal WS error: %s", e)
            await asyncio.sleep(10)


# ─── Position monitor loop ────────────────────────────────────────────────────

async def position_monitor(session: aiohttp.ClientSession):
    """Check open positions every 5 seconds."""
    while True:
        await asyncio.sleep(5)
        try:
            await manage_positions(session)
        except Exception as e:
            log.error("position_monitor error: %s", e)


# ─── Status logger ────────────────────────────────────────────────────────────

async def status_logger(session: aiohttp.ClientSession):
    while True:
        await asyncio.sleep(60)
        now     = datetime.now(timezone.utc)
        balance = await get_sol_balance(session)
        s       = state.stats
        wr      = s.trades_win / s.trades_total * 100 if s.trades_total else 0
        mode    = "DRY RUN 🟡" if DRY_RUN else "LIVE 🟢"

        log.info("─" * 65)
        log.info("📊 %s UTC  |  Режим: %s", now.strftime("%H:%M"), mode)
        log.info("   Баланс:       %.4f SOL%s", balance,
                 " (виртуальный)" if DRY_RUN else "")
        log.info("   Позиций:      %d / %d открыто", len(state.positions), MAX_POSITIONS)
        log.info("   Токенов в наблюдении: %d", len(state.watchlist))
        log.info("   Сделок:       %d всего | %d побед | WR %.0f%%",
                 s.trades_total, s.trades_win, wr)
        log.info("   PnL сессии:   %+.4f SOL  |  дневной: %+.4f SOL",
                 s.session_pnl, s.daily_pnl)
        log.info("   Сигналов:     %d оценено | %d вошли",
                 s.signals_seen, s.signals_entered)
        log.info("   Параметры:    score≥%d | MC<500SOL | age<%dmin | SL=%d%% | TP1=+%d%% TP2=+%d%% TP3=+%d%%",
                 SCORE_THRESHOLD, MAX_TOKEN_AGE_MIN,
                 int(SL_PCT * 100), int(TP1_PCT * 100),
                 int(TP2_PCT * 100), int(TP3_PCT * 100))
        log.info("─" * 65)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    global _keypair

    log.info("═" * 65)
    log.info("  PUMPSCALP v1.0  —  Pump.fun Early Entry Scalping Bot")
    log.info("  Режим: %s  |  Банк: %.2f SOL  |  Score threshold: %d",
             "DRY RUN 🟡" if DRY_RUN else "LIVE 🟢", VIRTUAL_BALANCE_SOL, SCORE_THRESHOLD)
    log.info("  TP1=+%d%%→%d%%  TP2=+%d%%→%d%%  TP3=+%d%%→rest  SL=-%d%%  TIME=%dmin",
             int(TP1_PCT * 100), int(TP1_FRACTION * 100),
             int(TP2_PCT * 100), int(TP2_FRACTION * 100),
             int(TP3_PCT * 100), int(SL_PCT * 100), TIME_STOP_MIN)
    log.info("═" * 65)

    if not DRY_RUN:
        _keypair = _load_keypair()
        if _keypair is None:
            log.error("OUR_PRIVATE_KEY не задан. Переключаю в DRY RUN.")
    else:
        state.bank = VIRTUAL_BALANCE_SOL

    conn = aiohttp.TCPConnector(ssl=_ssl_ctx, limit=20)
    async with aiohttp.ClientSession(connector=conn) as session:
        await asyncio.gather(
            pumpportal_listener(),
            scan_loop(session),
            position_monitor(session),
            status_logger(session),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Бот остановлен пользователем.")
