"""
PUMPSCALP v2.0 — Pump.fun Momentum Sniper
Стратегия выведена из анализа реального кошелька nya666pQkP3PzWxi7JngU3rRMHuc7zbLK8c8wxQ4qpT
(125,075 транзакций, февраль 2026, net PnL +770.87 SOL за 28 дней)

─── МАТЕМАТИЧЕСКАЯ ОСНОВА ────────────────────────────────────────────────────
Ключевые паттерны из реальных данных:

  Hold < 1 min : WR 30.9%, avg PnL -0.0045 SOL  ← убыток, выходить быстро
  Hold 1-5 min : WR 53.1%, avg PnL +0.0953 SOL  ← прибыльная зона
  Hold 5-15 min: WR 78.2%, avg PnL +0.3831 SOL  ← ЗОЛОТАЯ ЗОНА
  Hold > 15 min: WR 33.3%, avg PnL → падает     ← выходить

  Размер 0.5-1.0 SOL: WR 41.7%, avg PnL +0.046 ← оптимальный
  Размер < 0.1 SOL  : WR 27.3%, avg PnL -0.002  ← убыточный

  EV per trade: +0.0158 SOL × 1800 trades/day = +28.4 SOL/day

─── АЛГОРИТМ ──────────────────────────────────────────────────────────────────
  1. Поймать новый pump.fun токен через PumpPortal WebSocket
  2. Быстрый вход (< 500ms) если базовые критерии OK
  3. Т+30с: проверка моментума → если цена не выросла на 2%+, выход (мелкий стоп)
  4. Т+60с: если рост < 4%, выход
  5. Моментум подтверждён → держим с trailing stop
  6. Золотая зона 5-15 мин: WR 78.2%, держим с широким trailing
  7. Жёсткий выход на Т+15 мин (WR резко падает после)
  8. Fat Right Tail: если рост > 50%, ослабить trailing — это может быть x10+
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

# ─── Конфигурация ─────────────────────────────────────────────────────────────
DRY_RUN              = os.getenv("DRY_RUN", "true").lower() == "true"
VIRTUAL_BALANCE_SOL  = float(os.getenv("VIRTUAL_BALANCE_SOL", "50.0"))
BIRDEYE_API_KEY      = os.getenv("BIRDEYE_API_KEY", "")

# Entry
SCORE_THRESHOLD      = int(os.getenv("SCORE_THRESHOLD", "60"))    # снижен: быстрый вход важнее идеального сигнала
MAX_TOKEN_AGE_MIN    = int(os.getenv("MAX_TOKEN_AGE_MIN", "20"))  # только свежие < 20 мин
MAX_WATCH_AGE_MIN    = int(os.getenv("MAX_WATCH_AGE_MIN", "60"))  # наблюдение до 1ч
SCAN_INTERVAL_SEC    = int(os.getenv("SCAN_INTERVAL_SEC", "15"))  # сканировать каждые 15с (быстрее)

# Sizing — из данных: 0.5-1.0 SOL = оптимальная зона (WR 41.7%)
MAX_POSITIONS        = int(os.getenv("MAX_POSITIONS", "5"))
POSITION_SIZE_SOL    = float(os.getenv("POSITION_SIZE_SOL", "0.5"))    # фиксированный размер
POSITION_MAX_SOL     = float(os.getenv("POSITION_MAX_SOL", "1.0"))     # максимум при высоком скоре

# Exit — данные показывают: 5-15 мин = WR 78.2%, после 15 мин WR падает
TIME_STOP_MIN        = int(os.getenv("TIME_STOP_MIN", "15"))     # жёсткий выход через 15 мин!
MOMENTUM_CHECK_1_SEC = int(os.getenv("MOMENTUM_CHECK_1_SEC", "30"))  # первая проверка T+30с
MOMENTUM_CHECK_2_SEC = int(os.getenv("MOMENTUM_CHECK_2_SEC", "60"))  # вторая проверка T+60с
MOMENTUM_MIN_1_PCT   = float(os.getenv("MOMENTUM_MIN_1_PCT", "0.02"))  # минимум +2% к T+30с
MOMENTUM_MIN_2_PCT   = float(os.getenv("MOMENTUM_MIN_2_PCT", "0.04"))  # минимум +4% к T+60с

# TP/SL — адаптированы под fat right tail стратегию
SL_PCT               = float(os.getenv("SL_PCT", "0.08"))          # -8% стоп
TP1_PCT              = float(os.getenv("TP1_PCT", "0.15"))          # +15% → продать 40%
TP1_FRACTION         = float(os.getenv("TP1_FRACTION", "0.40"))
TP2_PCT              = float(os.getenv("TP2_PCT", "0.50"))          # +50% → продать 35%
TP2_FRACTION         = float(os.getenv("TP2_FRACTION", "0.35"))
TP3_PCT              = float(os.getenv("TP3_PCT", "2.00"))          # +200% → продать 20%
TP3_FRACTION         = float(os.getenv("TP3_FRACTION", "0.20"))     # держим 5% до луны
TRAILING_TRIGGER     = float(os.getenv("TRAILING_TRIGGER", "0.20")) # trailing после +20%
TRAILING_SL_PCT      = float(os.getenv("TRAILING_SL_PCT", "0.08"))  # trailing SL: -8% от пика
TAIL_TRIGGER         = float(os.getenv("TAIL_TRIGGER", "0.50"))     # при +50% — расширить trailing
TAIL_SL_PCT          = float(os.getenv("TAIL_SL_PCT", "0.15"))      # fat right tail: -15% от пика

# Risk
DAILY_LOSS_LIMIT_PCT = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.05"))

# Время торговли — из данных: 18-23 UTC лучшие часы, 16 UTC убыточный
BEST_HOURS_UTC       = set(map(int, os.getenv("BEST_HOURS_UTC", "18,19,20,21,22,23,0,1,2,3").split(",")))
WORST_HOURS_UTC      = set(map(int, os.getenv("WORST_HOURS_UTC", "16").split(",")))
SKIP_WORST_HOURS     = os.getenv("SKIP_WORST_HOURS", "true").lower() == "true"

# Copy-trading — отслеживать кошелёк-источник для extra сигналов
WATCH_WALLET         = os.getenv("WATCH_WALLET", "nya666pQkP3PzWxi7JngU3rRMHuc7zbLK8c8wxQ4qpT")
COPY_TRADE_BONUS     = int(os.getenv("COPY_TRADE_BONUS", "30"))  # +30 к score если мастер купил

# Execution
SLIPPAGE_BPS         = int(os.getenv("SLIPPAGE_BPS", "200"))  # 2% — быстро важнее экономии на слиппаже
PRIORITY_FEE_MICROLAMPORTS = int(os.getenv("PRIORITY_FEE_MICROLAMPORTS", "1000000"))  # высокий приоритет

# Anti-rugpull
MIN_LIQUIDITY_SOL    = float(os.getenv("MIN_LIQUIDITY_SOL", "5.0"))   # снижен: входим раньше
MAX_TOP10_HOLDER_PCT = float(os.getenv("MAX_TOP10_HOLDER_PCT", "0.60"))
MIN_BUY_PRESSURE     = float(os.getenv("MIN_BUY_PRESSURE", "0.60"))
MIN_UNIQUE_WALLETS   = int(os.getenv("MIN_UNIQUE_WALLETS", "3"))
VOL_SURGE_MULT       = float(os.getenv("VOL_SURGE_MULT", "2.0"))

# API endpoints
PUMPPORTAL_WSS = "wss://pumpportal.fun/api/data"
JUPITER_QUOTE  = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP   = "https://quote-api.jup.ag/v6/swap"
JUPITER_PRICE  = "https://price.jup.ag/v4/price"
BIRDEYE_TOKEN  = "https://public-api.birdeye.so/defi/token_overview"
WSOL_MINT      = "So11111111111111111111111111111111111111112"


# ─── Фазы позиции ─────────────────────────────────────────────────────────────
class Phase:
    MOMENTUM_CHECK_1 = "momentum_check_1"  # ждём T+30с
    MOMENTUM_CHECK_2 = "momentum_check_2"  # ждём T+60с (если прошёл 1й)
    ACTIVE           = "active"            # моментум подтверждён, держим
    CLOSING          = "closing"           # идёт закрытие


# ─── Структуры данных ─────────────────────────────────────────────────────────

@dataclass
class TokenWatch:
    mint:         str
    name:         str
    symbol:       str
    creator:      str
    created_at:   float
    initial_sol:  float
    total_supply: int
    copy_signal:  bool = False    # кошелёк-мастер купил этот токен


@dataclass
class Position:
    mint:          str
    name:          str
    symbol:        str
    entry_price:   float    # SOL per token
    entry_sol:     float    # SOL потрачено
    token_balance: int      # токенов на руках
    opened_at:     float
    phase:         str = Phase.MOMENTUM_CHECK_1

    # Exit levels
    sl_price:      float = 0.0
    tp1_price:     float = 0.0
    tp2_price:     float = 0.0
    tp3_price:     float = 0.0
    time_stop_at:  float = 0.0

    # Trailing
    peak_price:    float = 0.0
    trailing_sl:   Optional[float] = None

    # State
    tp1_done:      bool = False
    tp2_done:      bool = False
    tp3_done:      bool = False
    pnl_sol:       float = 0.0
    copy_trade:    bool = False


@dataclass
class DayStats:
    date:          str = ""
    trades:        int = 0
    wins:          int = 0
    pnl:           float = 0.0
    fees:          float = 0.0
    stopped:       bool = False    # дневной лимит убытка достигнут


class BotState:
    def __init__(self):
        self.bank:       float = VIRTUAL_BALANCE_SOL if DRY_RUN else 0.0
        self.positions:  dict[str, Position] = {}
        self.watchlist:  dict[str, TokenWatch] = {}
        self.day:        DayStats = DayStats()
        self.session_pnl: float = 0.0
        self.session_trades: int = 0
        self.session_wins:   int = 0
        self.signals_seen:   int = 0
        self.signals_entered: int = 0
        self.copy_signals:   int = 0
        # Токены купленные кошельком-мастером в последний час
        self.master_buys: set[str] = set()

    @property
    def win_rate(self) -> float:
        return self.session_wins / self.session_trades * 100 if self.session_trades else 0.0

    def is_good_hour(self) -> bool:
        h = datetime.now(timezone.utc).hour
        if SKIP_WORST_HOURS and h in WORST_HOURS_UTC:
            return False
        return True

    def daily_loss_hit(self) -> bool:
        return self.day.pnl <= -(self.bank * DAILY_LOSS_LIMIT_PCT)


state = BotState()

# ─── SSL ──────────────────────────────────────────────────────────────────────
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

_keypair: Optional[Keypair] = None


def _load_keypair() -> Optional[Keypair]:
    pk = config.OUR_PRIVATE_KEY
    if not pk:
        return None
    try:
        import base58
        return Keypair.from_bytes(base58.b58decode(pk))
    except Exception as e:
        log.error("Keypair error: %s", e)
        return None


# ─── Jupiter helpers ──────────────────────────────────────────────────────────

async def jup_quote(
    input_mint: str, output_mint: str, amount: int, session: aiohttp.ClientSession
) -> Optional[dict]:
    try:
        async with session.get(
            JUPITER_QUOTE,
            params={"inputMint": input_mint, "outputMint": output_mint,
                    "amount": amount, "slippageBps": SLIPPAGE_BPS},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None


async def jup_swap_tx(quote: dict, session: aiohttp.ClientSession) -> Optional[str]:
    if _keypair is None:
        return None
    try:
        async with session.post(
            JUPITER_SWAP,
            json={"quoteResponse": quote, "userPublicKey": str(_keypair.pubkey()),
                  "wrapAndUnwrapSol": True,
                  "prioritizationFeeLamports": PRIORITY_FEE_MICROLAMPORTS},
            timeout=aiohttp.ClientTimeout(total=12),
        ) as r:
            if r.status != 200:
                return None
            d = await r.json()
            return d.get("swapTransaction")
    except Exception:
        return None


async def send_tx(tx64: str, session: aiohttp.ClientSession) -> Optional[str]:
    if _keypair is None:
        return None
    try:
        raw = b64decode(tx64)
        tx = VersionedTransaction.from_bytes(raw)
        tx.sign([_keypair])
        payload = {"jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
                   "params": [tx.to_json(),
                               {"encoding": "base64", "skipPreflight": False,
                                "maxRetries": 3, "preflightCommitment": "confirmed"}]}
        async with session.post(config.RPC_URL, json=payload,
                                timeout=aiohttp.ClientTimeout(total=30)) as r:
            d = await r.json()
            if "error" in d:
                return None
            return d.get("result")
    except Exception as e:
        log.debug("send_tx error: %s", e)
        return None


# ─── Данные токена ────────────────────────────────────────────────────────────

async def get_token_price_sol(mint: str, session: aiohttp.ClientSession) -> Optional[float]:
    """Цена токена в SOL через Jupiter Price API."""
    try:
        async with session.get(
            JUPITER_PRICE, params={"ids": f"{mint},{WSOL_MINT}"},
            timeout=aiohttp.ClientTimeout(total=6),
        ) as r:
            if r.status != 200:
                return None
            d = await r.json()
            data = d.get("data", {})
            t_usd   = data.get(mint, {}).get("price", 0)
            sol_usd = data.get(WSOL_MINT, {}).get("price", 1)
            if not t_usd or not sol_usd:
                return None
            return t_usd / sol_usd
    except Exception:
        return None


async def get_birdeye_data(mint: str, session: aiohttp.ClientSession) -> dict:
    if not BIRDEYE_API_KEY:
        return {}
    try:
        async with session.get(
            BIRDEYE_TOKEN, params={"address": mint},
            headers={"X-API-KEY": BIRDEYE_API_KEY},
            timeout=aiohttp.ClientTimeout(total=6),
        ) as r:
            return (await r.json()).get("data", {}) if r.status == 200 else {}
    except Exception:
        return {}


async def get_top10_pct(mint: str, session: aiohttp.ClientSession) -> float:
    """Доля топ-10 холдеров. 1.0 при ошибке (консервативно)."""
    try:
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts",
                   "params": [mint, {"commitment": "confirmed"}]}
        async with session.post(config.RPC_URL, json=payload,
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            accs = (await r.json()).get("result", {}).get("value", [])
            if not accs:
                return 1.0
            total = sum(int(a.get("amount", 0)) for a in accs)
            top10 = sum(int(a.get("amount", 0)) for a in accs[:10])
            return top10 / total if total > 0 else 1.0
    except Exception:
        return 1.0


async def get_dev_sells(creator: str, mint: str, session: aiohttp.ClientSession) -> int:
    try:
        url = f"https://api.helius.xyz/v0/addresses/{creator}/transactions"
        async with session.get(url, params={"api-key": config.HELIUS_API_KEY,
                                            "limit": 30, "type": "SWAP"},
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return 0
            txs = await r.json()
        now = time.time()
        sells = 0
        for tx in txs:
            if now - tx.get("timestamp", 0) > 3600:
                break
            for t in tx.get("tokenTransfers", []):
                if t.get("mint") == mint and t.get("fromUserAccount") == creator:
                    sells += 1
        return sells
    except Exception:
        return 0


# ─── Скоринг (данные-обоснованный) ───────────────────────────────────────────

async def score_token(watch: TokenWatch, session: aiohttp.ClientSession) -> tuple[int, dict]:
    """
    Score 0–210. Порог входа: 60.

    Отличие от v1.0: снижен порог, добавлен copy-trade бонус (+30 если мастер купил),
    быстрота входа важнее идеального скора.
    """
    score = 0
    d: dict = {}

    bird, top10_pct = await asyncio.gather(
        get_birdeye_data(watch.mint, session),
        get_top10_pct(watch.mint, session),
    )
    dev_sells = await get_dev_sells(watch.creator, watch.mint, session)

    # ── Copy-trade бонус ─────────────────────────────────────────────────────
    if watch.copy_signal or watch.mint in state.master_buys:
        score += COPY_TRADE_BONUS
        d["copy"] = f"мастер купил (+{COPY_TRADE_BONUS})"
    else:
        d["copy"] = "—"

    # ── MC в SOL ──────────────────────────────────────────────────────────────
    mc_usd    = bird.get("mc") or bird.get("realMc") or 0
    sol_usd   = 150.0
    mc_sol    = mc_usd / sol_usd if sol_usd > 0 else 0
    if mc_sol > 0:
        if mc_sol < 100:
            score += 35; d["mc"] = f"ultra {mc_sol:.0f} SOL (+35)"
        elif mc_sol < 300:
            score += 25; d["mc"] = f"early {mc_sol:.0f} SOL (+25)"
        elif mc_sol < 500:
            score += 15; d["mc"] = f"{mc_sol:.0f} SOL (+15)"
        else:
            d["mc"] = f"{mc_sol:.0f} SOL (пропуск)"
            if mc_sol > 1000 and not watch.copy_signal:
                return 0, {"skip": "MC > 1000 SOL"}
    else:
        d["mc"] = "неизвестно"

    # ── Возраст ──────────────────────────────────────────────────────────────
    age_sec = time.time() - watch.created_at
    age_min = age_sec / 60
    if age_min < 5:
        score += 30; d["age"] = f"{age_min:.1f} мин (+30)"
    elif age_min < 15:
        score += 20; d["age"] = f"{age_min:.1f} мин (+20)"
    elif age_min < 30:
        score += 10; d["age"] = f"{age_min:.1f} мин (+10)"
    else:
        d["age"] = f"{age_min:.1f} мин (0)"

    # ── Pump.fun ──────────────────────────────────────────────────────────────
    score += 15; d["platform"] = "pump.fun (+15)"

    # ── Объём 5мин ───────────────────────────────────────────────────────────
    v5m = bird.get("v5m", 0) or 0
    v1h = bird.get("v1h", 0) or 0
    avg5 = v1h / 12 if v1h > 0 else 0
    if avg5 > 0 and v5m >= avg5 * VOL_SURGE_MULT:
        score += 20; d["vol"] = f"всплеск {v5m:.0f}$ (+20)"
    else:
        d["vol"] = f"v5m={v5m:.0f}$ (0)"

    # ── Buy pressure ─────────────────────────────────────────────────────────
    buy5  = bird.get("buy5m", 0) or 0
    sell5 = bird.get("sell5m", 0) or 0
    tot5  = buy5 + sell5
    bp    = buy5 / tot5 if tot5 > 0 else 0
    if bp >= MIN_BUY_PRESSURE:
        score += 15; d["bp"] = f"{bp:.0%} (+15)"
    else:
        d["bp"] = f"{bp:.0%} (0)"

    # ── Уникальные кошельки ──────────────────────────────────────────────────
    uw5 = bird.get("uniqueWallet5m", 0) or 0
    if uw5 >= MIN_UNIQUE_WALLETS:
        score += 25; d["wallets"] = f"{uw5} (+25)"
    else:
        d["wallets"] = f"{uw5} (0)"

    # ── Dev wallet ───────────────────────────────────────────────────────────
    if dev_sells == 0:
        score += 20; d["dev"] = "чист (+20)"
    else:
        d["dev"] = f"{dev_sells} продаж (0)"

    # ── Ликвидность ──────────────────────────────────────────────────────────
    liq_sol = watch.initial_sol
    liq_usd = bird.get("liquidity", 0) or 0
    if liq_usd > 0 and sol_usd > 0:
        liq_sol = liq_usd / sol_usd
    if liq_sol >= MIN_LIQUIDITY_SOL:
        score += 10; d["liq"] = f"{liq_sol:.1f} SOL (+10)"
    else:
        d["liq"] = f"{liq_sol:.1f} SOL (0)"

    # ── Anti-rug ─────────────────────────────────────────────────────────────
    if top10_pct < MAX_TOP10_HOLDER_PCT:
        score += 15; d["top10"] = f"{top10_pct:.0%} (+15)"
    else:
        d["top10"] = f"{top10_pct:.0%} концентрация (0)"

    d["total"] = score
    return score, d


# ─── Вход в позицию ──────────────────────────────────────────────────────────

async def open_position(
    watch: TokenWatch, size_sol: float, score: int, session: aiohttp.ClientSession
) -> Optional[Position]:
    lamports = int(size_sol * 1e9)

    if DRY_RUN:
        quote = await jup_quote(WSOL_MINT, watch.mint, lamports, session)
        if not quote:
            return None
        tokens_out = int(quote.get("outAmount", 0))
        if tokens_out == 0:
            return None
        entry_price = size_sol / tokens_out
        log.info("🟡 [DRY] BUY  %-10s  %.3f SOL  score=%d  %d токенов  @%.2e",
                 watch.symbol, size_sol, score, tokens_out, entry_price)
    else:
        quote = await jup_quote(WSOL_MINT, watch.mint, lamports, session)
        if not quote:
            return None
        tokens_out = int(quote.get("outAmount", 0))
        tx64 = await jup_swap_tx(quote, session)
        if not tx64:
            return None
        sig = await send_tx(tx64, session)
        if not sig:
            log.warning("BUY TX failed: %s", watch.symbol)
            return None
        entry_price = size_sol / max(tokens_out, 1)
        log.info("🟢 BUY  %-10s  %.3f SOL  score=%d  sig=%.8s",
                 watch.symbol, size_sol, score, sig)

    now = time.time()
    pos = Position(
        mint=watch.mint, name=watch.name, symbol=watch.symbol,
        entry_price=entry_price, entry_sol=size_sol,
        token_balance=tokens_out, opened_at=now,
        phase=Phase.MOMENTUM_CHECK_1,
        sl_price=entry_price * (1 - SL_PCT),
        tp1_price=entry_price * (1 + TP1_PCT),
        tp2_price=entry_price * (1 + TP2_PCT),
        tp3_price=entry_price * (1 + TP3_PCT),
        time_stop_at=now + TIME_STOP_MIN * 60,
        peak_price=entry_price,
        copy_trade=watch.copy_signal,
    )
    if DRY_RUN:
        state.bank -= size_sol
    return pos


# ─── Выход из позиции ─────────────────────────────────────────────────────────

async def close_position(
    pos: Position, fraction: float, reason: str,
    price: float, session: aiohttp.ClientSession
) -> float:
    to_sell = int(pos.token_balance * fraction)
    if to_sell <= 0:
        return 0.0

    if DRY_RUN:
        sol_back = to_sell * price
        pnl = sol_back - pos.entry_sol * fraction
        icon = "🟢" if pnl >= 0 else "🔴"
        log.info("%s [DRY] SELL %-10s  %d%%  %+.4f SOL  [%s]",
                 icon, pos.symbol, int(fraction * 100), pnl, reason)
    else:
        quote = await jup_quote(pos.mint, WSOL_MINT, to_sell, session)
        if not quote:
            log.warning("No sell quote: %s", pos.symbol)
            return 0.0
        sol_back = int(quote.get("outAmount", 0)) / 1e9
        pnl = sol_back - pos.entry_sol * fraction
        tx64 = await jup_swap_tx(quote, session)
        if not tx64:
            return 0.0
        sig = await send_tx(tx64, session)
        if not sig:
            return 0.0
        icon = "🟢" if pnl >= 0 else "🔴"
        log.info("%s SELL %-10s  %d%%  %+.4f SOL  [%s]  %.8s",
                 icon, pos.symbol, int(fraction * 100), pnl, reason, sig)

    pos.token_balance -= to_sell
    pos.pnl_sol += sol_back
    if DRY_RUN:
        state.bank += sol_back
    return sol_back


def _record_close(pos: Position, sold_fraction: float = 1.0):
    net = pos.pnl_sol - pos.entry_sol * sold_fraction
    state.session_trades += 1
    state.session_pnl += net
    state.day.trades += 1
    state.day.pnl += net
    if net > 0:
        state.session_wins += 1
        state.day.wins += 1
    hold_min = (time.time() - pos.opened_at) / 60
    ret_pct = net / pos.entry_sol * 100 if pos.entry_sol > 0 else 0
    log.info("📋 %-10s  PnL %+.4f SOL (%+.1f%%)  hold %.1f мин  WR %.0f%%",
             pos.symbol, net, ret_pct, hold_min, state.win_rate)


# ─── Управление позициями ─────────────────────────────────────────────────────

async def manage_positions(session: aiohttp.ClientSession):
    """
    Главная логика выхода. Вызывается каждые 2 секунды.

    Фазы:
      MOMENTUM_CHECK_1: T+30с → если цена < entry+2%, выходим
      MOMENTUM_CHECK_2: T+60с → если цена < entry+4%, выходим
      ACTIVE: моментум подтверждён → TP/SL/trailing/time-stop
    """
    to_remove: list[str] = []

    for mint, pos in list(state.positions.items()):
        price = await get_token_price_sol(mint, session)
        if price is None:
            continue

        now     = time.time()
        age_sec = now - pos.opened_at
        pnl_pct = (price - pos.entry_price) / pos.entry_price

        # Обновляем пиковую цену
        if price > pos.peak_price:
            pos.peak_price = price

        # ── ФАЗА 1: T+30с momentum check ────────────────────────────────────
        if pos.phase == Phase.MOMENTUM_CHECK_1:
            if age_sec < MOMENTUM_CHECK_1_SEC:
                # Жёсткий стоп если уже -SL_PCT
                if pnl_pct <= -SL_PCT:
                    await close_position(pos, 1.0, f"EARLY_SL {pnl_pct:+.0%}", price, session)
                    to_remove.append(mint)
                    _record_close(pos)
                continue

            if pnl_pct >= MOMENTUM_MIN_1_PCT:
                pos.phase = Phase.MOMENTUM_CHECK_2
                log.info("⚡ %-10s T+30s: %+.1f%% — моментум ✓ (ждём T+60s)",
                         pos.symbol, pnl_pct * 100)
            else:
                # Нет моментума → выходим (данные: <1мин WR 30.9%, убыточно)
                await close_position(pos, 1.0, f"NO_MOMENTUM_30s {pnl_pct:+.1%}", price, session)
                to_remove.append(mint)
                _record_close(pos)
            continue

        # ── ФАЗА 2: T+60с momentum check ────────────────────────────────────
        if pos.phase == Phase.MOMENTUM_CHECK_2:
            if age_sec < MOMENTUM_CHECK_2_SEC:
                if pnl_pct <= -SL_PCT:
                    await close_position(pos, 1.0, f"EARLY_SL {pnl_pct:+.0%}", price, session)
                    to_remove.append(mint)
                    _record_close(pos)
                continue

            if pnl_pct >= MOMENTUM_MIN_2_PCT:
                pos.phase = Phase.ACTIVE
                log.info("✅ %-10s T+60s: %+.1f%% — моментум подтверждён! ДЕРЖИМ 5-15мин",
                         pos.symbol, pnl_pct * 100)
            else:
                await close_position(pos, 1.0, f"NO_MOMENTUM_60s {pnl_pct:+.1%}", price, session)
                to_remove.append(mint)
                _record_close(pos)
            continue

        # ── ФАЗА ACTIVE ──────────────────────────────────────────────────────
        # Обновляем trailing stop
        if pnl_pct >= TAIL_TRIGGER:
            # Fat right tail: ослабляем trailing (данные: лучшая сделка x392)
            new_trail = pos.peak_price * (1 - TAIL_SL_PCT)
            if pos.trailing_sl is None or new_trail > pos.trailing_sl:
                pos.trailing_sl = new_trail
        elif pnl_pct >= TRAILING_TRIGGER and pos.trailing_sl is None:
            pos.trailing_sl = pos.peak_price * (1 - TRAILING_SL_PCT)
            log.info("⚡ Trailing SL активирован: %-10s @ %.6f SOL", pos.symbol, pos.trailing_sl)
        elif pos.trailing_sl and price > pos.peak_price * (1 - TRAILING_SL_PCT + 0.01):
            # Двигаем trailing вверх
            new_trail = pos.peak_price * (1 - (TAIL_SL_PCT if pnl_pct >= TAIL_TRIGGER else TRAILING_SL_PCT))
            if new_trail > pos.trailing_sl:
                pos.trailing_sl = new_trail

        # Stop-loss
        sl = pos.trailing_sl if pos.trailing_sl else pos.sl_price
        if price <= sl:
            await close_position(pos, 1.0, f"SL {pnl_pct:+.0%}", price, session)
            to_remove.append(mint)
            _record_close(pos)
            continue

        # Time-stop — данные: WR после 15 мин падает с 78.2% до 33.3%
        if now >= pos.time_stop_at:
            await close_position(pos, 1.0, f"TIME15min {pnl_pct:+.0%}", price, session)
            to_remove.append(mint)
            _record_close(pos)
            continue

        # TP3 → продать 20% (оставляем лотерейный билет)
        if not pos.tp3_done and pos.tp2_done and price >= pos.tp3_price:
            await close_position(pos, TP3_FRACTION, f"TP3 +{TP3_PCT:.0%}", price, session)
            pos.tp3_done = True
            continue

        # TP2 → продать 35%
        if not pos.tp2_done and pos.tp1_done and price >= pos.tp2_price:
            await close_position(pos, TP2_FRACTION, f"TP2 +{TP2_PCT:.0%}", price, session)
            pos.tp2_done = True
            continue

        # TP1 → продать 40%
        if not pos.tp1_done and price >= pos.tp1_price:
            await close_position(pos, TP1_FRACTION, f"TP1 +{TP1_PCT:.0%}", price, session)
            pos.tp1_done = True
            continue

    for m in to_remove:
        state.positions.pop(m, None)


# ─── Сканирование и вход ──────────────────────────────────────────────────────

async def scan_loop(session: aiohttp.ClientSession):
    """Каждые SCAN_INTERVAL_SEC: оценить watchlist и войти в лучшие сигналы."""
    while True:
        await asyncio.sleep(SCAN_INTERVAL_SEC)

        if state.daily_loss_hit():
            if not state.day.stopped:
                state.day.stopped = True
                log.warning("🚫 Дневной лимит убытка %.0f%% достигнут. Пауза до UTC 00:00",
                            DAILY_LOSS_LIMIT_PCT * 100)
            continue

        if SKIP_WORST_HOURS and not state.is_good_hour():
            h = datetime.now(timezone.utc).hour
            log.debug("🕐 Час %d UTC — паузируем (в данных убыточный час)", h)
            continue

        now = time.time()

        # Очистка устаревших токенов
        for m in [m for m, w in state.watchlist.items()
                  if now - w.created_at > MAX_WATCH_AGE_MIN * 60]:
            state.watchlist.pop(m, None)

        # Кандидаты: свежие токены не в позициях, в зоне возраста
        candidates = [
            w for w in state.watchlist.values()
            if w.mint not in state.positions
            and (now - w.created_at) / 60 <= MAX_TOKEN_AGE_MIN
        ]

        # Сортируем: copy-trade токены первые, потом по свежести
        candidates.sort(key=lambda w: (not w.copy_signal, w.created_at))

        for watch in candidates:
            if len(state.positions) >= MAX_POSITIONS:
                break

            score, details = await score_token(watch, session)
            state.signals_seen += 1

            age_min = (now - watch.created_at) / 60

            # Формируем строку деталей для лога
            detail_str = " | ".join(f"{k}={v}" for k, v in details.items()
                                    if k not in ("total",))
            log.info("🔍 %-10s  score=%d  age=%.1f мин  %s",
                     watch.symbol, score, age_min, detail_str)

            if score < SCORE_THRESHOLD:
                continue

            # Размер позиции: увеличиваем при высоком скоре или copy-trade
            if watch.copy_signal or score >= 120:
                size_sol = POSITION_MAX_SOL       # 1.0 SOL при высоком скоре
            elif score >= 90:
                size_sol = (POSITION_SIZE_SOL + POSITION_MAX_SOL) / 2  # 0.75 SOL
            else:
                size_sol = POSITION_SIZE_SOL       # 0.5 SOL базовый

            # Проверка баланса
            available = state.bank if DRY_RUN else await get_sol_balance(session)
            size_sol = min(size_sol, available * 0.20)   # не более 20% баланса
            if size_sol < 0.05:
                log.warning("Недостаточно баланса для %s (%.3f SOL)", watch.symbol, available)
                continue

            log.info("✅ СИГНАЛ  %-10s  score=%d  size=%.2f SOL%s",
                     watch.symbol, score,
                     size_sol, "  ⭐ COPY" if watch.copy_signal else "")

            pos = await open_position(watch, size_sol, score, session)
            if pos:
                state.positions[watch.mint] = pos
                state.signals_entered += 1
                if watch.copy_signal:
                    state.copy_signals += 1
                state.watchlist.pop(watch.mint, None)

            await asyncio.sleep(0.3)


async def get_sol_balance(session: aiohttp.ClientSession) -> float:
    if DRY_RUN:
        return state.bank
    try:
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getBalance",
                   "params": [config.OUR_WALLET, {"commitment": "confirmed"}]}
        async with session.post(config.RPC_URL, json=payload,
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            return (await r.json())["result"]["value"] / 1e9
    except Exception:
        return state.bank


# ─── PumpPortal WebSocket ─────────────────────────────────────────────────────

async def pumpportal_listener():
    """
    Слушаем новые pump.fun токены через PumpPortal WebSocket.
    При каждом новом токене добавляем в watchlist.
    """
    while True:
        try:
            async with websockets.connect(
                PUMPPORTAL_WSS, ping_interval=None, ssl=_ssl_ctx
            ) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("✅ PumpPortal WS: подключён, слушаем новые токены")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    mint = msg.get("mint")
                    if not mint or mint in state.watchlist or mint in state.positions:
                        continue

                    ts = msg.get("timestamp") or msg.get("created_timestamp")
                    if ts and ts > 1e12:
                        ts = ts / 1000
                    created_at = float(ts) if ts else time.time()

                    vsol = msg.get("virtualSolReserves", 0)
                    if vsol and vsol > 1e9:
                        vsol = vsol / 1e9

                    watch = TokenWatch(
                        mint=mint,
                        name=msg.get("name", "?"),
                        symbol=msg.get("symbol", "?"),
                        creator=msg.get("traderPublicKey") or msg.get("creator", ""),
                        created_at=created_at,
                        initial_sol=float(vsol),
                        total_supply=int(msg.get("totalSupply") or 1_000_000_000),
                        copy_signal=mint in state.master_buys,
                    )
                    state.watchlist[mint] = watch
                    log.debug("👀 Новый токен: %-10s  %s  liq=%.1f SOL",
                              watch.symbol, mint[:8], vsol)

        except websockets.ConnectionClosed:
            log.warning("PumpPortal WS закрыт, переподключаемся через 5с...")
            await asyncio.sleep(5)
        except Exception as e:
            log.error("PumpPortal WS ошибка: %s", e)
            await asyncio.sleep(10)


# ─── Copy-trading: мониторинг кошелька-мастера ───────────────────────────────

async def master_wallet_monitor(session: aiohttp.ClientSession):
    """
    Отслеживаем покупки кошелька nya666... (770 SOL за 28 дней).
    Если мастер купил токен → добавляем +30 к score этого токена.
    Источник сигнала: Helius Enhanced Transactions API.
    """
    if not WATCH_WALLET or not config.HELIUS_API_KEY:
        return

    log.info("👁️  Мониторинг мастер-кошелька: %s", WATCH_WALLET[:12])
    last_sig: Optional[str] = None

    while True:
        await asyncio.sleep(10)   # проверяем каждые 10 секунд
        try:
            url = f"https://api.helius.xyz/v0/addresses/{WATCH_WALLET}/transactions"
            params = {"api-key": config.HELIUS_API_KEY, "limit": 20, "type": "SWAP"}
            if last_sig:
                params["before"] = last_sig

            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    continue
                txs = await r.json()

            if not txs:
                continue

            now = time.time()
            new_buys: set[str] = set()

            for tx in txs:
                if now - tx.get("timestamp", 0) > 300:  # только последние 5 мин
                    break
                # Ищем входящие токен-трансферы (покупка)
                for t in tx.get("tokenTransfers", []):
                    mint = t.get("mint", "")
                    if (t.get("toUserAccount") == WATCH_WALLET
                            and mint
                            and mint != WSOL_MINT):
                        new_buys.add(mint)
                        if mint in state.watchlist and not state.watchlist[mint].copy_signal:
                            state.watchlist[mint].copy_signal = True
                            log.info("⭐ COPY SIGNAL: мастер купил %-10s  %s",
                                     state.watchlist[mint].symbol, mint[:8])

            if txs:
                last_sig = txs[0].get("signature")

            state.master_buys.update(new_buys)
            # Чистим старые (держим только за последний час)
            # (упрощённо: set растёт, но за день это ~1229 уникальных токенов — OK)

        except Exception as e:
            log.debug("master_wallet_monitor error: %s", e)


# ─── Position monitor (быстрый цикл) ─────────────────────────────────────────

async def position_monitor(session: aiohttp.ClientSession):
    """Проверяем открытые позиции каждые 2 секунды."""
    while True:
        await asyncio.sleep(2)
        try:
            await manage_positions(session)
        except Exception as e:
            log.error("position_monitor error: %s", e)


# ─── Статус-логгер ────────────────────────────────────────────────────────────

async def status_logger(session: aiohttp.ClientSession):
    while True:
        await asyncio.sleep(60)
        bal  = await get_sol_balance(session)
        h    = datetime.now(timezone.utc).hour
        day  = datetime.now(timezone.utc).strftime("%a")

        log.info("═" * 65)
        log.info("  %s  %02d:xx UTC  |  %s  |  банк: %.4f SOL%s",
                 day, h,
                 "DRY RUN 🟡" if DRY_RUN else "LIVE 🟢",
                 bal, " (виртуальный)" if DRY_RUN else "")
        log.info("  Позиций: %d/%d  |  Watchlist: %d  |  Master buys: %d",
                 len(state.positions), MAX_POSITIONS,
                 len(state.watchlist), len(state.master_buys))
        log.info("  Сделок: %d  |  Победы: %d  |  WR: %.1f%%",
                 state.session_trades, state.session_wins, state.win_rate)
        log.info("  PnL сессии: %+.4f SOL  |  Дневной: %+.4f SOL",
                 state.session_pnl, state.day.pnl)
        log.info("  Сигналов: %d оценено / %d вошли / %d copy-trade",
                 state.signals_seen, state.signals_entered, state.copy_signals)

        # Открытые позиции
        for mint, pos in list(state.positions.items()):
            price = await get_token_price_sol(mint, session)
            if price:
                pnl_pct = (price - pos.entry_price) / pos.entry_price * 100
                age_min = (time.time() - pos.opened_at) / 60
                phase_icon = {"momentum_check_1": "⏳", "momentum_check_2": "⏳⏳",
                              "active": "🏃"}.get(pos.phase, "?")
                log.info("  %s %-10s  %+.1f%%  %.1f мин  %s%s",
                         phase_icon, pos.symbol, pnl_pct, age_min, pos.phase,
                         " ⭐" if pos.copy_trade else "")
        log.info("═" * 65)


# ─── Сброс дневной статистики ─────────────────────────────────────────────────

async def daily_reset():
    """Сбрасывает дневную статистику в полночь UTC."""
    while True:
        now = datetime.now(timezone.utc)
        seconds_to_midnight = (24 * 3600) - (now.hour * 3600 + now.minute * 60 + now.second)
        await asyncio.sleep(seconds_to_midnight)
        state.day = DayStats(date=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        log.info("🌅 Новый торговый день. Счётчики сброшены.")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    global _keypair

    log.info("═" * 65)
    log.info("  PUMPSCALP v2.0  —  Momentum Sniper")
    log.info("  Основан на анализе: nya666...Q4qpT  (+770 SOL за 28 дней)")
    log.info("  Режим: %s  |  Банк: %.1f SOL  |  Score ≥ %d",
             "DRY RUN 🟡" if DRY_RUN else "LIVE 🟢", VIRTUAL_BALANCE_SOL, SCORE_THRESHOLD)
    log.info("  Размер позиции: %.2f–%.2f SOL  |  Max позиций: %d",
             POSITION_SIZE_SOL, POSITION_MAX_SOL, MAX_POSITIONS)
    log.info("  TP1 +%d%%→%d%%  TP2 +%d%%→%d%%  TP3 +%d%%→%d%%  SL -%d%%  TIME %d мин",
             int(TP1_PCT*100), int(TP1_FRACTION*100),
             int(TP2_PCT*100), int(TP2_FRACTION*100),
             int(TP3_PCT*100), int(TP3_FRACTION*100),
             int(SL_PCT*100), TIME_STOP_MIN)
    log.info("  Momentum gate: T+30s≥+%.0f%%  T+60s≥+%.0f%%",
             MOMENTUM_MIN_1_PCT*100, MOMENTUM_MIN_2_PCT*100)
    log.info("  Мастер-кошелёк: %s", WATCH_WALLET[:12] if WATCH_WALLET else "не задан")
    log.info("═" * 65)

    if not DRY_RUN:
        _keypair = _load_keypair()
        if _keypair is None:
            log.error("OUR_PRIVATE_KEY не задан. Переключаю в DRY_RUN.")

    state.day.date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    conn = aiohttp.TCPConnector(ssl=_ssl_ctx, limit=30)
    async with aiohttp.ClientSession(connector=conn) as session:
        await asyncio.gather(
            pumpportal_listener(),
            scan_loop(session),
            position_monitor(session),
            master_wallet_monitor(session),
            status_logger(session),
            daily_reset(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Бот остановлен.")
        log.info("Итого сделок: %d  |  PnL: %+.4f SOL  |  WR: %.1f%%",
                 state.session_trades, state.session_pnl, state.win_rate)
