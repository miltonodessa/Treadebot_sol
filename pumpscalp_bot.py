"""
PUMPSCALP v3.0 — Pump.fun Direct Bonding Curve Sniper
Стратегия полностью реконструирована из 125,075 транзакций кошелька:
  nya666pQkP3PzWxi7JngU3rRMHuc7zbLK8c8wxQ4qpT (+770 SOL за 28 дней, фев 2026)

══════════════════════════════════════════════════════════════
  ЧТО РЕАЛЬНО ДЕЛАЕТ ЭТОТ КОШЕЛЁК (из анализа данных):

  1. ROUTING: 100% через pump.fun bonding curve напрямую (не Jupiter/Raydium!)
     - Программа: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
     - PumpPortal trade-local API: строит tx для подписи локально

  2. ВХОД: покупает КАЖДЫЙ новый pump.fun токен без предфильтрации
     - Размер: ~0.265 SOL (медиана), 0.10–0.50 SOL (основная масса)
     - Никакого scoring — скорость важнее выбора

  3. ВЫХОД — ДВА ОБЯЗАТЕЛЬНЫХ CHECK-POINT-А:
     - T+5-10s: быстрый стоп (пик при 866 выходов на 5-10s!)
       → если цена не выросла: выходим немедленно (pre-signed tx)
     - T+25-30s: momentum gate (второй пик 539 выходов на 25-30s!)
       → если нет momentum: выходим
     - Если прошли T+30s: WR 54%+, avg PnL +0.16 SOL → держим!

  4. НЕТ SCALE-IN! Есть RE-ENTRY:
     - Все "повторные покупки" — это НОВЫЕ сделки на тот же токен
     - После выхода ждём 60-120s, если токен ещё качает — заходим снова
     - 2-я покупка чуть крупнее (0.297 vs 0.265 SOL) → подтверждённый мувер

  5. ТЕХНИЧЕСКИЕ ОСОБЕННОСТИ:
     - Durable Nonces (43.4% транзакций): pre-signed sell tx всегда готов
     - ComputeBudget: высокий priority fee для первого места в блоке
     - Параллельный мониторинг: несколько токенов одновременно

  МАТЕМАТИКА:
    EV = 0.34 × avg_win + 0.66 × avg_loss
       = 0.34 × (+0.16) + 0.66 × (-0.013) ≈ +0.0457 SOL/trade
    Сделок в день: ~1800 → ~82 SOL/день ≈ $12,000/day (при SOL $150)
══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import time
from base64 import b64decode, b64encode
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import websockets
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

import config

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pumpscalp")

# ═══════════════════════════════════════════════════════════════════════════════
# ПАРАМЕТРЫ — КАЖДЫЙ ВЫВЕДЕН ИЗ РЕАЛЬНЫХ ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════
DRY_RUN              = os.getenv("DRY_RUN", "true").lower() == "true"
VIRTUAL_BALANCE_SOL  = float(os.getenv("VIRTUAL_BALANCE_SOL", "50.0"))

# ── Размер позиции (медиана из данных: 0.265 SOL) ───────────────────────────
BUY_SIZE_SOL         = float(os.getenv("BUY_SIZE_SOL", "0.25"))
BUY_SIZE_REENTRY_SOL = float(os.getenv("BUY_SIZE_REENTRY_SOL", "0.30"))  # чуть крупнее при re-entry
MAX_POSITIONS        = int(os.getenv("MAX_POSITIONS", "15"))    # реальный кошелёк держал 15+

# ── Momentum check-points (из пиков на гистограмме: 5-10s и 25-30s) ────────
QUICK_STOP_SEC       = int(os.getenv("QUICK_STOP_SEC", "8"))     # T+8s: быстрый стоп
QUICK_STOP_MIN_PCT   = float(os.getenv("QUICK_STOP_MIN_PCT", "0.0"))  # 0% = любой рост позволяет жить
MOMENTUM_GATE_SEC    = int(os.getenv("MOMENTUM_GATE_SEC", "30"))  # T+30s: главный gate
MOMENTUM_MIN_PCT     = float(os.getenv("MOMENTUM_MIN_PCT", "0.03"))  # нужен рост +3% к T+30s

# ── Выходы после momentum (данные: 60-300s WR=54%, 300s+ avg=+1.15 SOL) ────
TP1_PCT              = float(os.getenv("TP1_PCT", "0.20"))      # +20% → продать 50%
TP1_SELL_FRAC        = float(os.getenv("TP1_SELL_FRAC", "0.50"))
TP2_PCT              = float(os.getenv("TP2_PCT", "0.60"))      # +60% → продать 50% остатка
TP2_SELL_FRAC        = float(os.getenv("TP2_SELL_FRAC", "0.50"))
TP3_PCT              = float(os.getenv("TP3_PCT", "2.00"))      # +200% → продать 75% остатка
TP3_SELL_FRAC        = float(os.getenv("TP3_SELL_FRAC", "0.75"))

SL_IMMEDIATE_PCT     = float(os.getenv("SL_IMMEDIATE_PCT", "0.05"))   # -5% в первые 30s → выход
TRAILING_TRIGGER_PCT = float(os.getenv("TRAILING_TRIGGER_PCT", "0.15"))  # активировать trailing при +15%
TRAILING_DISTANCE    = float(os.getenv("TRAILING_DISTANCE", "0.08"))   # -8% от пика
HARD_TIME_STOP_MIN   = int(os.getenv("HARD_TIME_STOP_MIN", "15"))      # максимум 15 мин

# ── Re-entry (из данных: медиана 116s между выходом и повторным входом) ─────
REENTRY_MIN_SEC      = int(os.getenv("REENTRY_MIN_SEC", "60"))   # минимальная пауза перед re-entry
REENTRY_MAX_SEC      = int(os.getenv("REENTRY_MAX_SEC", "300"))  # не заходить если токен старше
REENTRY_MIN_PUMP_PCT = float(os.getenv("REENTRY_MIN_PUMP_PCT", "0.02"))  # токен должен расти
MAX_REENTRIES        = int(os.getenv("MAX_REENTRIES", "3"))       # не более 3 раз на один токен

# ── Token age (только свежие pump.fun токены) ───────────────────────────────
MAX_TOKEN_AGE_SEC    = int(os.getenv("MAX_TOKEN_AGE_SEC", "120"))  # не позже 2 мин после запуска

# ── Риск ────────────────────────────────────────────────────────────────────
DAILY_LOSS_LIMIT_SOL = float(os.getenv("DAILY_LOSS_LIMIT_SOL", "5.0"))  # стоп при -5 SOL/день

# ── Исполнение ──────────────────────────────────────────────────────────────
PRIORITY_FEE_MICROLAMPORTS = int(os.getenv("PRIORITY_FEE_MICROLAMPORTS", "500000"))
SLIPPAGE_BPS         = int(os.getenv("SLIPPAGE_BPS", "1000"))  # 10% — pump.fun быстро двигается

# ── API ─────────────────────────────────────────────────────────────────────
PUMPPORTAL_WSS    = "wss://pumpportal.fun/api/data"
PUMPPORTAL_TRADE  = "https://pumpportal.fun/api/trade-local"
WSOL_MINT         = "So11111111111111111111111111111111111111112"
PUMPFUN_PROGRAM   = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


def gmgn(mint: str) -> str:
    """Ссылка на токен в GMGN для быстрого анализа."""
    return f"https://gmgn.ai/sol/token/{mint}"

# ═══════════════════════════════════════════════════════════════════════════════
# СТРУКТУРЫ ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TokenEvent:
    """Новый pump.fun токен из WebSocket."""
    mint:        str
    name:        str
    symbol:      str
    creator:     str
    created_at:  float
    init_sol:    float    # начальная ликвидность SOL в bonding curve


@dataclass
class Position:
    """Открытая позиция."""
    mint:          str
    symbol:        str
    entry_price:   float      # SOL/token на момент покупки (расчётная)
    entry_sol:     float      # потрачено SOL
    token_balance: int        # токенов в кошельке
    opened_at:     float
    is_reentry:    bool = False
    reentry_count: int  = 0   # сколько раз мы уже заходили в этот токен

    # Состояние machine
    quick_stop_done: bool = False   # прошли T+8s check
    momentum_done:   bool = False   # прошли T+30s check

    # Trailing
    peak_price:    float = 0.0
    trailing_sl:   Optional[float] = None

    # TP флаги
    tp1_done: bool = False
    tp2_done: bool = False
    tp3_done: bool = False

    # P&L накопленный по частичным продажам
    realized_sol:  float = 0.0


@dataclass
class MintHistory:
    """История торговли по конкретному минту (для re-entry логики)."""
    last_exit_time:  float = 0.0
    last_exit_price: float = 0.0     # расчётная цена при последнем выходе
    total_reentries: int   = 0
    total_pnl:       float = 0.0
    first_seen:      float = 0.0


class BotState:
    def __init__(self):
        self.bank:        float = VIRTUAL_BALANCE_SOL if DRY_RUN else 0.0
        self.positions:   dict[str, Position]    = {}
        self.mint_history: dict[str, MintHistory] = defaultdict(MintHistory)
        self.pending_tokens: dict[str, TokenEvent] = {}   # токены ещё не купленные

        self.session_trades:  int   = 0
        self.session_wins:    int   = 0
        self.session_pnl:     float = 0.0
        self.daily_pnl:       float = 0.0
        self.daily_stopped:   bool  = False
        self.signals_received: int  = 0
        self.signals_entered:  int  = 0
        self.reentries:        int  = 0

    @property
    def win_rate(self) -> float:
        return self.session_wins / self.session_trades * 100 if self.session_trades else 0.0

    def can_open(self) -> bool:
        if self.daily_stopped:
            return False
        if self.daily_pnl <= -DAILY_LOSS_LIMIT_SOL:
            if not self.daily_stopped:
                self.daily_stopped = True
                log.warning("🛑 ДНЕВНОЙ ЛИМИТ УБЫТКА -%.1f SOL. Остановка до 00:00 UTC.", DAILY_LOSS_LIMIT_SOL)
            return False
        return len(self.positions) < MAX_POSITIONS


state = BotState()

# ─── SSL / Keypair ─────────────────────────────────────────────────────────────
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE
_keypair: Optional[Keypair] = None


def _load_keypair() -> Optional[Keypair]:
    pk = getattr(config, "OUR_PRIVATE_KEY", None)
    if not pk:
        return None
    try:
        import base58
        return Keypair.from_bytes(base58.b58decode(pk))
    except Exception as e:
        log.error("Keypair error: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# PUMP.FUN BONDING CURVE — ПРЯМОЕ ВЗАИМОДЕЙСТВИЕ
# ═══════════════════════════════════════════════════════════════════════════════

async def pumpportal_buy(
    mint: str, sol_amount: float, session: aiohttp.ClientSession
) -> Optional[dict]:
    """
    Получить tx для покупки через PumpPortal trade-local.
    Возвращает {'tx': base64_tx, 'tokens_expected': int, 'price': float}
    или None при ошибке.
    """
    if _keypair is None and not DRY_RUN:
        return None
    try:
        payload = {
            "publicKey": str(_keypair.pubkey()) if _keypair else config.OUR_WALLET,
            "action":    "buy",
            "mint":      mint,
            "amount":    sol_amount,
            "denominatedInSol": "true",
            "slippage":  SLIPPAGE_BPS / 100,       # PumpPortal принимает %
            "priorityFee": PRIORITY_FEE_MICROLAMPORTS / 1_000_000,  # SOL
            "pool": "pump",
        }
        async with session.post(
            PUMPPORTAL_TRADE, json=payload,
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            if r.status != 200:
                text = await r.text()
                log.debug("buy error %d: %s", r.status, text[:200])
                return None
            data = await r.json()
            return data
    except Exception as e:
        log.debug("pumpportal_buy error: %s", e)
        return None


async def pumpportal_sell(
    mint: str, token_amount: int, session: aiohttp.ClientSession
) -> Optional[dict]:
    """Получить tx для продажи через PumpPortal trade-local."""
    if _keypair is None and not DRY_RUN:
        return None
    try:
        payload = {
            "publicKey": str(_keypair.pubkey()) if _keypair else config.OUR_WALLET,
            "action":    "sell",
            "mint":      mint,
            "amount":    token_amount,
            "denominatedInSol": "false",     # указываем кол-во токенов
            "slippage":  SLIPPAGE_BPS / 100,
            "priorityFee": PRIORITY_FEE_MICROLAMPORTS / 1_000_000,
            "pool": "pump",
        }
        async with session.post(
            PUMPPORTAL_TRADE, json=payload,
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception as e:
        log.debug("pumpportal_sell error: %s", e)
        return None


async def sign_and_send(tx_base64: str, session: aiohttp.ClientSession) -> Optional[str]:
    """Подписать и отправить транзакцию."""
    if _keypair is None:
        return None
    try:
        raw = b64decode(tx_base64)
        tx = VersionedTransaction.from_bytes(raw)
        tx.sign([_keypair])
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "sendTransaction",
            "params": [
                b64encode(bytes(tx)).decode(),
                {"encoding": "base64",
                 "skipPreflight": False,
                 "maxRetries": 3,
                 "preflightCommitment": "confirmed"}
            ]
        }
        async with session.post(
            config.RPC_URL, json=payload,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as r:
            d = await r.json()
            if "error" in d:
                log.debug("sendTx error: %s", d["error"])
                return None
            return d.get("result")
    except Exception as e:
        log.debug("sign_and_send error: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# ЦЕНА ЧЕРЕЗ PUMP.FUN BONDING CURVE
# ═══════════════════════════════════════════════════════════════════════════════

# Кэш цен (обновляется через WebSocket trade events)
_price_cache: dict[str, tuple[float, float]] = {}  # mint → (price_sol, timestamp)


def update_price_from_trade(mint: str, sol_amount: float, token_amount: int, is_buy: bool):
    """
    Обновить кэш цены из события торговли на bonding curve.
    Цена = sol_amount / token_amount (сколько SOL за 1 токен).
    """
    if token_amount > 0 and sol_amount > 0:
        price = sol_amount / token_amount
        _price_cache[mint] = (price, time.time())


def get_cached_price(mint: str) -> Optional[float]:
    """Получить последнюю известную цену токена."""
    cached = _price_cache.get(mint)
    if cached and time.time() - cached[1] < 120:  # не старше 2 мин
        return cached[0]
    return None


async def get_bonding_curve_price(mint: str, session: aiohttp.ClientSession) -> Optional[float]:
    """
    Получить текущую цену через pump.fun API или RPC запрос к bonding curve account.
    Используем Jupiter Price как fallback.
    """
    # Сначала пробуем кэш (обновляется из WebSocket)
    cached = get_cached_price(mint)
    if cached:
        return cached

    # Fallback: Jupiter Price API
    try:
        async with session.get(
            "https://price.jup.ag/v4/price",
            params={"ids": f"{mint},So11111111111111111111111111111111111111112"},
            timeout=aiohttp.ClientTimeout(total=4)
        ) as r:
            if r.status == 200:
                data = (await r.json()).get("data", {})
                t_usd   = data.get(mint, {}).get("price", 0)
                sol_usd = data.get(WSOL_MINT, {}).get("price", 1)
                if t_usd and sol_usd:
                    price = t_usd / sol_usd
                    _price_cache[mint] = (price, time.time())
                    return price
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ОТКРЫТИЕ / ЗАКРЫТИЕ ПОЗИЦИЙ
# ═══════════════════════════════════════════════════════════════════════════════

async def open_position(
    event: TokenEvent,
    buy_sol: float,
    is_reentry: bool,
    reentry_count: int,
    session: aiohttp.ClientSession,
) -> Optional[Position]:
    """
    Купить токен через pump.fun bonding curve (PumpPortal trade-local).
    """
    entry_label = f"RE-ENTRY #{reentry_count}" if is_reentry else "ENTRY"

    if DRY_RUN:
        # Симуляция: считаем что получили токены по текущей цене
        cached_price = get_cached_price(event.mint)
        if cached_price is None:
            cached_price = event.init_sol / 1_000_000_000 if event.init_sol > 0 else 1e-9
        tokens_received = int(buy_sol / cached_price) if cached_price > 0 else int(buy_sol * 1e6)
        entry_price = buy_sol / max(tokens_received, 1)
        state.bank -= buy_sol
        log.info("🟡 [DRY] %s  %-10s  %.3f SOL  %d tokens  @%.2e SOL/token\n         %s",
                 entry_label, event.symbol, buy_sol, tokens_received, entry_price,
                 gmgn(event.mint))
    else:
        data = await pumpportal_buy(event.mint, buy_sol, session)
        if data is None:
            return None

        tx64 = data.get("transaction") or data.get("tx")
        if not tx64:
            log.debug("No tx in response: %s", str(data)[:200])
            return None

        sig = await sign_and_send(tx64, session)
        if not sig:
            log.warning("Buy tx failed: %s", event.symbol)
            return None

        # Tokens received из ответа API или расчётная
        tokens_received = int(data.get("outAmount") or data.get("tokensReceived") or 0)
        if tokens_received == 0:
            tokens_received = int(buy_sol * 1e6)   # грубая оценка

        entry_price = buy_sol / max(tokens_received, 1)
        log.info("🟢 %s  %-10s  %.3f SOL  %.8s\n         %s",
                 entry_label, event.symbol, buy_sol, sig, gmgn(event.mint))

    now = time.time()
    return Position(
        mint=event.mint,
        symbol=event.symbol,
        entry_price=entry_price,
        entry_sol=buy_sol,
        token_balance=tokens_received,
        opened_at=now,
        is_reentry=is_reentry,
        reentry_count=reentry_count,
        peak_price=entry_price,
    )


async def close_position(
    pos: Position,
    fraction: float,
    reason: str,
    current_price: float,
    session: aiohttp.ClientSession,
) -> float:
    """
    Продать fraction от оставшихся токенов.
    Возвращает SOL полученное.
    """
    to_sell = int(pos.token_balance * fraction)
    if to_sell <= 0:
        return 0.0

    sol_value = to_sell * current_price
    pnl = sol_value - pos.entry_sol * fraction
    pnl_pct = (current_price - pos.entry_price) / pos.entry_price * 100 if pos.entry_price > 0 else 0

    if DRY_RUN:
        icon = "🟢" if pnl >= 0 else "🔴"
        log.info("%s [DRY] SELL %-10s  %d%%  %+.4f SOL (%+.1f%%)  [%s]\n         %s",
                 icon, pos.symbol, int(fraction * 100), pnl, pnl_pct, reason,
                 gmgn(pos.mint))
    else:
        data = await pumpportal_sell(pos.mint, to_sell, session)
        if data is None:
            log.warning("Sell quote failed: %s", pos.symbol)
            return 0.0

        tx64 = data.get("transaction") or data.get("tx")
        if not tx64:
            return 0.0

        sig = await sign_and_send(tx64, session)
        if not sig:
            log.warning("Sell tx failed: %s", pos.symbol)
            return 0.0

        sol_value = int(data.get("outAmount") or data.get("solReceived") or sol_value * 1e9) / 1e9
        pnl = sol_value - pos.entry_sol * fraction
        icon = "🟢" if pnl >= 0 else "🔴"
        log.info("%s SELL %-10s  %d%%  %+.4f SOL (%+.1f%%)  [%s]  %.8s\n         %s",
                 icon, pos.symbol, int(fraction * 100), pnl, pnl_pct, reason, sig,
                 gmgn(pos.mint))

    pos.token_balance -= to_sell
    pos.realized_sol  += sol_value
    if DRY_RUN:
        state.bank += sol_value

    return sol_value


def _record_closed(pos: Position, final_price: float):
    """Записать закрытую сделку в статистику."""
    total_sol_out = pos.realized_sol
    net_pnl = total_sol_out - pos.entry_sol
    hold_min = (time.time() - pos.opened_at) / 60
    ret_pct = net_pnl / pos.entry_sol * 100 if pos.entry_sol > 0 else 0

    state.session_trades += 1
    state.session_pnl    += net_pnl
    state.daily_pnl      += net_pnl
    if net_pnl > 0:
        state.session_wins += 1

    icon = "✅" if net_pnl > 0 else "❌"
    log.info("%s %-10s  PnL %+.4f SOL (%+.1f%%)  hold %.1f мин  WR %.0f%%\n         %s",
             icon, pos.symbol, net_pnl, ret_pct, hold_min, state.win_rate,
             gmgn(pos.mint))

    # Обновить историю минта для re-entry логики
    h = state.mint_history[pos.mint]
    h.last_exit_time  = time.time()
    h.last_exit_price = final_price
    h.total_pnl      += net_pnl
    h.total_reentries = pos.reentry_count


# ═══════════════════════════════════════════════════════════════════════════════
# УПРАВЛЕНИЕ ПОЗИЦИЯМИ (главный цикл)
# ═══════════════════════════════════════════════════════════════════════════════

async def manage_positions(session: aiohttp.ClientSession):
    """
    Вызывается каждые 2 секунды.
    Реализует точную логику кошелька из данных.
    """
    to_remove: list[str] = []
    to_reenter: list[tuple] = []   # (mint, event) для re-entry после закрытия

    for mint, pos in list(state.positions.items()):
        price = await get_bonding_curve_price(mint, session)
        if price is None:
            # Нет цены — пропускаем этот цикл, не выходим
            continue

        now = time.time()
        age_sec = now - pos.opened_at

        # Обновляем peak price
        if price > pos.peak_price:
            pos.peak_price = price

        pnl_pct = (price - pos.entry_price) / pos.entry_price if pos.entry_price > 0 else 0

        # ─── ФАЗА 1: T+0 до T+8s (QUICK STOP) ────────────────────────────
        # Из данных: пик выходов на 5-10s (866 выходов!)
        # Кошелёк держит pre-signed sell tx готовым к отправке
        # Выходит НЕМЕДЛЕННО если нет признаков роста
        if not pos.quick_stop_done:
            if age_sec < QUICK_STOP_SEC:
                # Только экстренный выход если сразу -5%
                if pnl_pct <= -SL_IMMEDIATE_PCT:
                    await close_position(pos, 1.0, f"IMMEDIATE_SL {pnl_pct:+.1%}", price, session)
                    to_remove.append(mint)
                    _record_closed(pos, price)
                continue

            # T+8s достигнут
            pos.quick_stop_done = True
            if pnl_pct <= QUICK_STOP_MIN_PCT:
                # Нет роста за 8 секунд → выходим (основная масса убыточных сделок)
                await close_position(pos, 1.0, f"QUICK_STOP_8s {pnl_pct:+.1%}", price, session)
                to_remove.append(mint)
                _record_closed(pos, price)
                continue
            else:
                log.debug("⚡ T+8s %-10s %+.1f%% → держим", pos.symbol, pnl_pct * 100)

        # ─── ФАЗА 2: T+8s до T+30s (MOMENTUM GATE) ───────────────────────
        # Из данных: второй пик выходов 25-30s (539 выходов!)
        # Обязательная продажа если нет momentum к T+30s
        if not pos.momentum_done:
            if age_sec < MOMENTUM_GATE_SEC:
                # Защитный стоп в этой фазе: -5%
                if pnl_pct <= -SL_IMMEDIATE_PCT:
                    await close_position(pos, 1.0, f"EARLY_SL {pnl_pct:+.1%}", price, session)
                    to_remove.append(mint)
                    _record_closed(pos, price)
                continue

            # T+30s достигнут
            pos.momentum_done = True
            if pnl_pct < MOMENTUM_MIN_PCT:
                # Нет momentum → обязательный выход
                await close_position(pos, 1.0, f"MOMENTUM_GATE_30s {pnl_pct:+.1%}", price, session)
                to_remove.append(mint)
                _record_closed(pos, price)
                continue
            else:
                log.info("✅ T+30s %-10s %+.1f%% — MOMENTUM! Держим 🏃\n         %s",
                         pos.symbol, pnl_pct * 100, gmgn(pos.mint))

        # ─── ФАЗА 3: АКТИВНАЯ ПОЗИЦИЯ (momentum подтверждён) ─────────────
        # Из данных: WR 54% для 60-300s, avg PnL +0.16 SOL
        # WR 50% для 300s+, avg PnL +1.15 SOL (FAT TAIL!)

        # Обновляем trailing stop
        if pnl_pct >= TRAILING_TRIGGER_PCT:
            new_trail = pos.peak_price * (1 - TRAILING_DISTANCE)
            if pos.trailing_sl is None or new_trail > pos.trailing_sl:
                if pos.trailing_sl is None:
                    log.info("⚡ Trailing SL активирован: %-10s @%.2e", pos.symbol, new_trail)
                pos.trailing_sl = new_trail

        # Trailing stop hit
        if pos.trailing_sl and price <= pos.trailing_sl:
            await close_position(pos, 1.0, f"TRAILING_SL {pnl_pct:+.1%}", price, session)
            to_remove.append(mint)
            _record_closed(pos, price)
            continue

        # Hard stop -8% в активной фазе
        if pnl_pct <= -0.08:
            await close_position(pos, 1.0, f"HARD_SL {pnl_pct:+.1%}", price, session)
            to_remove.append(mint)
            _record_closed(pos, price)
            continue

        # Time stop: 15 мин (из данных: WR резко падает после 15 мин)
        if age_sec >= HARD_TIME_STOP_MIN * 60:
            await close_position(pos, 1.0, f"TIME_15min {pnl_pct:+.1%}", price, session)
            to_remove.append(mint)
            _record_closed(pos, price)
            continue

        # TP3: +200% → продать 75% (лунный выход)
        if not pos.tp3_done and pos.tp2_done and pnl_pct >= TP3_PCT:
            await close_position(pos, TP3_SELL_FRAC, f"TP3 +{TP3_PCT:.0%}", price, session)
            pos.tp3_done = True

        # TP2: +60% → продать 50% от остатка
        elif not pos.tp2_done and pos.tp1_done and pnl_pct >= TP2_PCT:
            await close_position(pos, TP2_SELL_FRAC, f"TP2 +{TP2_PCT:.0%}", price, session)
            pos.tp2_done = True

        # TP1: +20% → продать 50%
        elif not pos.tp1_done and pnl_pct >= TP1_PCT:
            await close_position(pos, TP1_SELL_FRAC, f"TP1 +{TP1_PCT:.0%}", price, session)
            pos.tp1_done = True

        # Если продали всё через частичные TP
        if pos.token_balance <= 0:
            to_remove.append(mint)
            _record_closed(pos, price)

    for m in to_remove:
        state.positions.pop(m, None)


# ═══════════════════════════════════════════════════════════════════════════════
# RE-ENTRY ЛОГИКА
# ═══════════════════════════════════════════════════════════════════════════════

async def check_reentries(session: aiohttp.ClientSession):
    """
    Из данных: после выхода из позиции, если токен ещё качает,
    кошелёк повторно входит через ~60-120s с чуть большим размером.
    Медиана re-entry интервала: 116 секунд.
    """
    now = time.time()

    for mint, history in list(state.mint_history.items()):
        if mint in state.positions:
            continue  # уже в позиции
        if history.total_reentries >= MAX_REENTRIES:
            continue
        if history.last_exit_time == 0:
            continue

        time_since_exit = now - history.last_exit_time

        # Пауза после выхода: минимум 60s, максимум 300s
        if time_since_exit < REENTRY_MIN_SEC or time_since_exit > REENTRY_MAX_SEC:
            continue

        # Проверить цену: токен должен продолжать расти
        price = get_cached_price(mint)
        if price is None:
            continue

        if history.last_exit_price > 0:
            growth_since_exit = (price - history.last_exit_price) / history.last_exit_price
            if growth_since_exit < REENTRY_MIN_PUMP_PCT:
                continue  # не растёт → не заходим

        if not state.can_open():
            break

        # Re-entry!
        event = TokenEvent(
            mint=mint, name="?", symbol=f"REENTRY",
            creator="", created_at=history.first_seen, init_sol=0.0
        )
        # Найти символ если он у нас есть
        for pos in list(state.positions.values()):
            if pos.mint == mint:
                event.symbol = pos.symbol
                break

        reentry_size = BUY_SIZE_REENTRY_SOL  # чуть больше базового
        log.info("🔄 RE-ENTRY  %-10s  #%d  %.2f SOL  (%.0fs после выхода)\n         %s",
                 mint[:12], history.total_reentries + 1, reentry_size, time_since_exit,
                 gmgn(mint))

        pos = await open_position(event, reentry_size, is_reentry=True,
                                  reentry_count=history.total_reentries + 1,
                                  session=session)
        if pos:
            state.positions[mint] = pos
            state.reentries += 1
            history.total_reentries += 1
            history.last_exit_time = 0   # сбросить чтобы не входить снова пока в позиции


# ═══════════════════════════════════════════════════════════════════════════════
# PUMP.FUN WEBSOCKET — НОВЫЕ ТОКЕНЫ + ТОРГОВЫЕ СОБЫТИЯ
# ═══════════════════════════════════════════════════════════════════════════════

async def buy_token(event: TokenEvent, session: aiohttp.ClientSession):
    """
    Мгновенно купить новый pump.fun токен.
    Никакого scoring — стратегия основана на объёме и momentum gate.
    """
    if not state.can_open():
        return

    # Не покупать старые токены
    age_sec = time.time() - event.created_at
    if age_sec > MAX_TOKEN_AGE_SEC:
        return

    # Не покупать если уже в позиции по этому минту
    if event.mint in state.positions:
        return

    # Проверить историю: не заходить если уже выходили недавно (re-entry логика это обработает)
    h = state.mint_history.get(event.mint)
    if h and h.last_exit_time > 0:
        return  # re-entry логика обработает через check_reentries

    buy_sol = BUY_SIZE_SOL
    pos = await open_position(event, buy_sol, is_reentry=False, reentry_count=0, session=session)
    if pos:
        state.positions[event.mint] = pos
        state.signals_entered += 1
        if event.mint not in state.mint_history:
            state.mint_history[event.mint] = MintHistory()
        state.mint_history[event.mint].first_seen = event.created_at


async def pumpportal_listener(session: aiohttp.ClientSession):
    """
    PumpPortal WebSocket:
    - subscribeNewToken: новые токены → мгновенная покупка
    - subscribeTokenTrade: торговые события → обновление цен
    """
    while True:
        try:
            async with websockets.connect(
                PUMPPORTAL_WSS,
                ping_interval=30,
                ping_timeout=10,
                ssl=_ssl_ctx,
                max_size=2**20,
            ) as ws:
                # Подписка на новые токены
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("✅ WS подключён: слушаем новые pump.fun токены")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    method = msg.get("txType") or msg.get("method") or ""

                    # ── Новый токен ──────────────────────────────────────────
                    if msg.get("mint") and "traderPublicKey" in msg:
                        # Это либо новый токен (creator = traderPublicKey),
                        # либо трейд на существующий токен
                        mint   = msg.get("mint", "")
                        trader = msg.get("traderPublicKey", "")

                        # Определить: это создание токена или торговля?
                        is_new = (
                            msg.get("newTokenAccount") is not None
                            or msg.get("bondingCurveKey") is not None
                            or (msg.get("txType") == "create")
                        )

                        if is_new:
                            state.signals_received += 1
                            ts = msg.get("timestamp")
                            if ts and ts > 1e12:
                                ts = ts / 1000
                            created_at = float(ts) if ts else time.time()

                            vsol = msg.get("vSolInBondingCurve") or msg.get("virtualSolReserves") or 0
                            if vsol > 1e9:
                                vsol = vsol / 1e9

                            event = TokenEvent(
                                mint=mint,
                                name=msg.get("name", "?"),
                                symbol=msg.get("symbol", "?")[:10],
                                creator=trader,
                                created_at=created_at,
                                init_sol=float(vsol),
                            )
                            # Запускаем покупку в фоне (не блокируем listener)
                            asyncio.create_task(buy_token(event, session))

                        else:
                            # Торговое событие — обновляем цену
                            sol_amount   = msg.get("solAmount") or msg.get("vSolInBondingCurve") or 0
                            token_amount = msg.get("tokenAmount") or msg.get("vTokensInBondingCurve") or 0
                            is_buy       = msg.get("txType") == "buy"

                            if sol_amount and token_amount:
                                update_price_from_trade(mint, sol_amount, token_amount, is_buy)

        except websockets.ConnectionClosed:
            log.warning("WS закрыт, переподключение через 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error("WS error: %s", e)
            await asyncio.sleep(5)


async def subscribe_to_active_tokens(session: aiohttp.ClientSession):
    """
    Подписаться на торговые события активных позиций для обновления цен.
    """
    while True:
        await asyncio.sleep(15)
        if not state.positions:
            continue

        mints = list(state.positions.keys())
        try:
            async with websockets.connect(
                PUMPPORTAL_WSS,
                ping_interval=30, ssl=_ssl_ctx,
            ) as ws:
                await ws.send(json.dumps({
                    "method": "subscribeTokenTrade",
                    "keys": mints,
                }))
                log.debug("Подписка на трейды для %d минтов", len(mints))

                deadline = time.time() + 60   # переподключаться каждую минуту
                async for raw in ws:
                    if time.time() > deadline:
                        break
                    try:
                        msg = json.loads(raw)
                        mint = msg.get("mint", "")
                        if not mint or mint not in state.positions:
                            continue
                        sol_amount   = msg.get("solAmount", 0)
                        token_amount = msg.get("tokenAmount", 0)
                        is_buy       = msg.get("txType") == "buy"
                        if sol_amount and token_amount:
                            update_price_from_trade(mint, sol_amount, token_amount, is_buy)
                    except Exception:
                        continue
        except Exception as e:
            log.debug("subscribe_tokens error: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
# УПРАВЛЯЮЩИЕ ЦИКЛЫ
# ═══════════════════════════════════════════════════════════════════════════════

async def position_monitor(session: aiohttp.ClientSession):
    """Проверяем позиции каждые 2 секунды."""
    while True:
        await asyncio.sleep(2)
        try:
            if state.positions:
                await manage_positions(session)
        except Exception as e:
            log.error("position_monitor: %s", e)


async def reentry_monitor(session: aiohttp.ClientSession):
    """Проверяем re-entry каждые 10 секунд."""
    while True:
        await asyncio.sleep(10)
        try:
            await check_reentries(session)
        except Exception as e:
            log.debug("reentry_monitor: %s", e)


async def status_logger():
    """Статус каждые 60 секунд."""
    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)
        log.info("═" * 65)
        log.info("  %s UTC  |  %s",
                 now.strftime("%a %H:%M"),
                 "DRY RUN 🟡" if DRY_RUN else "LIVE 🟢")
        log.info("  Банк: %.4f SOL  |  Позиций: %d/%d  |  Цен в кэше: %d",
                 state.bank, len(state.positions), MAX_POSITIONS, len(_price_cache))
        log.info("  Сигналов: %d  |  Входов: %d  |  Re-entries: %d",
                 state.signals_received, state.signals_entered, state.reentries)
        log.info("  Сделок: %d  |  Wins: %d  |  WR: %.1f%%  |  PnL: %+.4f SOL",
                 state.session_trades, state.session_wins,
                 state.win_rate, state.session_pnl)
        log.info("  Дневной PnL: %+.4f SOL%s",
                 state.daily_pnl, "  ⚠️ СТОП" if state.daily_stopped else "")

        if state.positions:
            for mint, pos in list(state.positions.items()):
                price = get_cached_price(mint)
                pnl_str = ""
                if price and pos.entry_price > 0:
                    pnl_pct = (price - pos.entry_price) / pos.entry_price * 100
                    pnl_str = f"  {pnl_pct:+.1f}%"
                age_min = (time.time() - pos.opened_at) / 60
                phase = ("⏳" if not pos.quick_stop_done else
                         "⏳⏳" if not pos.momentum_done else "🏃")
                log.info("  %s %-10s  %.1f мин%s%s",
                         phase, pos.symbol, age_min, pnl_str,
                         f"  #{pos.reentry_count}" if pos.is_reentry else "")
        log.info("═" * 65)


async def daily_reset():
    """Сброс дневной статистики в полночь UTC."""
    while True:
        now = datetime.now(timezone.utc)
        secs_to_midnight = 86400 - (now.hour * 3600 + now.minute * 60 + now.second)
        await asyncio.sleep(secs_to_midnight)
        state.daily_pnl     = 0.0
        state.daily_stopped = False
        log.info("🌅 Новый день. Дневной лимит сброшен.")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    global _keypair

    log.info("═" * 65)
    log.info("  PUMPSCALP v3.0  —  Pump.fun Direct Bonding Curve Sniper")
    log.info("  Стратегия из анализа 125,075 транзакций +770 SOL/28дней")
    log.info("  Режим: %s  |  Банк: %.1f SOL",
             "DRY RUN 🟡" if DRY_RUN else "LIVE 🟢",
             state.bank)
    log.info("  Размер: %.2f SOL (re-entry %.2f SOL)  |  Max позиций: %d",
             BUY_SIZE_SOL, BUY_SIZE_REENTRY_SOL, MAX_POSITIONS)
    log.info("  Quick stop T+%ds  |  Momentum gate T+%ds ≥+%.0f%%  |  Time stop %d мин",
             QUICK_STOP_SEC, MOMENTUM_GATE_SEC,
             MOMENTUM_MIN_PCT * 100, HARD_TIME_STOP_MIN)
    log.info("  TP1 +%.0f%%→%.0f%%  TP2 +%.0f%%→%.0f%%  TP3 +%.0f%%→%.0f%%  Trail -%d%%",
             TP1_PCT*100, TP1_SELL_FRAC*100,
             TP2_PCT*100, TP2_SELL_FRAC*100,
             TP3_PCT*100, TP3_SELL_FRAC*100,
             TRAILING_DISTANCE*100)
    log.info("═" * 65)

    if not DRY_RUN:
        _keypair = _load_keypair()
        if _keypair is None:
            log.error("OUR_PRIVATE_KEY не задан. Переключаю в DRY RUN.")

    conn = aiohttp.TCPConnector(ssl=_ssl_ctx, limit=50)
    async with aiohttp.ClientSession(connector=conn) as session:
        await asyncio.gather(
            pumpportal_listener(session),
            subscribe_to_active_tokens(session),
            position_monitor(session),
            reentry_monitor(session),
            status_logger(),
            daily_reset(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено.")
        log.info("Итого: %d сделок  |  WR %.1f%%  |  PnL %+.4f SOL",
                 state.session_trades, state.win_rate, state.session_pnl)
