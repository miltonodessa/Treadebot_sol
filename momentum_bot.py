"""
Momentum Scalping Bot
=====================
Стратегия выявлена из анализа 2TE2F за 53 дня (582 свапа):

ПРОФИЛЬ ВЫИГРЫШЕЙ (34% win rate, avg +28.5%, hold median 15s):
  - Большие победы: +50–120% за 2–75 секунд
  - Маленькие потери: –5–25%
  - Соотношение: win_avg/loss_avg = 28.5/9.8 ≈ 2.9x → положительное EV

ЛОГИКА ВХОДА:
  Детектируем MOMENTUM SIGNAL — когда на токен идёт крупная скоординированная покупка:
  - 3+ крупные покупки (>0.5 SOL) в течение 20 секунд от разных кошельков
  - Или 1 очень крупная покупка (>5 SOL) за последние 10 секунд
  Это значит: кто-то большой зашёл → импульс вверх → мы ловим хвост

ЛОГИКА ВЫХОДА (chunk strategy из данных 2TE2F):
  Чанк 1 (40% позиции): +15% от цены входа
  Чанк 2 (35% позиции): +35% от цены входа
  Чанк 3 (25% позиции): +80% или time-stop 90 секунд
  Stop-loss: –25% от цены входа
  Emergency exit: 5 минут (300 секунд)

ВРЕМЯ: только 15:00–23:59 UTC (как у 2TE2F — европейский вечер)
"""

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import websockets
from solders.keypair import Keypair  # type: ignore

import config
from token_filter import validate_token

log = logging.getLogger("momentum_bot")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

# ─── Константы ────────────────────────────────────────────────────────────────

SOL_MINT  = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WSOL_MINT = "So11111111111111111111111111111111111111112"

# Активные часы UTC (из анализа 2TE2F)
ACTIVE_HOURS = set(range(15, 24))

# ─── Exit levels (выявлены из профиля сделок 2TE2F) ──────────────────────────
EXIT_LEVELS = [
    {"pct": 0.15, "sell_fraction": 0.40},   # +15% → продаём 40%
    {"pct": 0.35, "sell_fraction": 0.35},   # +35% → продаём 35%
    {"pct": 0.80, "sell_fraction": 1.00},   # +80% → продаём остаток
]
STOP_LOSS_PCT    = float(getattr(config, "STOP_LOSS_PCT", 0.25))
TIME_STOP_SECS   = 90     # чанк 3 — time stop
EMERGENCY_SECS   = int(getattr(config, "MAX_HOLD_SECS", 300))

# ─── Signal detection thresholds ─────────────────────────────────────────────
SIGNAL_WINDOW_SECS   = 20    # окно для подсчёта покупок
SIGNAL_MIN_BUYS      = 3     # минимум крупных покупок в окне
SIGNAL_MIN_SOL_EACH  = 0.5   # минимальный размер каждой (SOL)
SIGNAL_BIG_BUY_SOL   = 5.0   # одна большая покупка тоже триггер
SIGNAL_COOLDOWN_SECS = 60    # не заходим в один токен дважды за 60с

# ─── Position sizing (из данных 2TE2F: 2 / 3 / 5 SOL) ───────────────────────
BUY_SIZE_SOL     = float(getattr(config, "MAX_SOL_PER_TRADE", 0.5))
MAX_POSITIONS    = int(getattr(config, "MAX_OPEN_POSITIONS", 5))
DAILY_LOSS_LIMIT = float(getattr(config, "DAILY_LOSS_LIMIT_SOL", 1.0))
PROFIT_PARK_SOL  = float(getattr(config, "PROFIT_PARK_SOL", 5.0))


# ─── Состояние ────────────────────────────────────────────────────────────────

@dataclass
class Position:
    mint: str
    buy_sol: float
    buy_price: float      # SOL за 1 token (из Jupiter quote)
    token_balance: int    # raw units
    decimals: int
    open_ts: float
    exit_levels_done: list[bool] = field(default_factory=lambda: [False]*len(EXIT_LEVELS))
    last_exit_attempt: float = 0.0


@dataclass
class BotState:
    positions: dict[str, Position] = field(default_factory=dict)
    # Очередь покупок по каждому токену для детекции сигнала
    buy_events: dict[str, deque] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=20)))
    # Когда последний раз мы входили в этот токен
    last_entry: dict[str, float] = field(default_factory=dict)
    daily_pnl: float = 0.0
    daily_date: str = ""
    session_profit: float = 0.0
    paused: bool = False
    processed_sigs: set[str] = field(default_factory=set)

    def reset_daily_if_needed(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.daily_date != today:
            self.daily_pnl = 0.0
            self.daily_date = today
            log.info("📅 Новый день — сброс дневного PnL")

    def record_sig(self, sig: str):
        self.processed_sigs.add(sig)
        if len(self.processed_sigs) > 20_000:
            self.processed_sigs = set(list(self.processed_sigs)[-10_000:])

    def add_buy_event(self, mint: str, sol_amount: float, buyer: str):
        self.buy_events[mint].append({
            "ts": time.time(),
            "sol": sol_amount,
            "buyer": buyer,
        })

    def get_momentum_signal(self, mint: str) -> Optional[str]:
        """
        Возвращает тип сигнала если детектирован моментум, иначе None.
        """
        events = self.buy_events.get(mint, deque())
        now = time.time()
        recent = [e for e in events if now - e["ts"] <= SIGNAL_WINDOW_SECS]

        if not recent:
            return None

        # Большая покупка
        if any(e["sol"] >= SIGNAL_BIG_BUY_SOL for e in recent):
            return f"big_buy ({max(e['sol'] for e in recent):.1f} SOL)"

        # Несколько крупных покупок от разных кошельков
        big = [e for e in recent if e["sol"] >= SIGNAL_MIN_SOL_EACH]
        unique_buyers = len({e["buyer"] for e in big})
        if len(big) >= SIGNAL_MIN_BUYS and unique_buyers >= 2:
            return f"cluster ({len(big)} buys, {unique_buyers} wallets)"

        return None


state = BotState()


# ─── Helius helpers ───────────────────────────────────────────────────────────

async def helius_get_tx(sig: str, session: aiohttp.ClientSession) -> Optional[dict]:
    url = f"https://api.helius.xyz/v0/transactions/?api-key={config.HELIUS_API_KEY}"
    try:
        async with session.post(
            url, json={"transactions": [sig]},
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
            return data[0] if data else None
    except Exception:
        return None


async def rpc_call(method: str, params: list, session: aiohttp.ClientSession) -> Optional[dict]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        async with session.post(
            config.RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            return await r.json()
    except Exception:
        return None


async def get_sol_balance(session: aiohttp.ClientSession) -> float:
    r = await rpc_call("getBalance", [config.OUR_WALLET, {"commitment": "confirmed"}], session)
    if r and "result" in r:
        return r["result"]["value"] / 1e9
    return 0.0


async def get_token_balance(mint: str, session: aiohttp.ClientSession) -> int:
    r = await rpc_call(
        "getTokenAccountsByOwner",
        [config.OUR_WALLET, {"mint": mint}, {"encoding": "jsonParsed", "commitment": "confirmed"}],
        session
    )
    try:
        accounts = r["result"]["value"]
        if not accounts:
            return 0
        return int(accounts[0]["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
    except Exception:
        return 0


# ─── Jupiter v6 ───────────────────────────────────────────────────────────────

async def jupiter_quote(
    input_mint: str, output_mint: str, amount: int,
    session: aiohttp.ClientSession
) -> Optional[dict]:
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount,
        "slippageBps": config.SLIPPAGE_BPS,
        "maxAccounts": "64",
    }
    try:
        async with session.get(
            "https://quote-api.jup.ag/v6/quote", params=params,
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None


async def jupiter_swap_and_send(
    input_mint: str, output_mint: str, amount: int,
    session: aiohttp.ClientSession
) -> Optional[str]:
    """Полный цикл: quote → swap tx → sign → send."""
    import base64
    from solders.transaction import VersionedTransaction

    quote = await jupiter_quote(input_mint, output_mint, amount, session)
    if not quote:
        log.warning("Нет котировки для %s→%s", input_mint[:8], output_mint[:8])
        return None

    swap_payload = {
        "quoteResponse": quote,
        "userPublicKey": config.OUR_WALLET,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": {
            "priorityLevelWithMaxLamports": {
                "maxLamports": config.PRIORITY_FEE_MICROLAMPORTS,
                "priorityLevel": "high",
            }
        },
    }
    try:
        async with session.post(
            "https://quote-api.jup.ag/v6/swap", json=swap_payload,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            if r.status != 200:
                return None
            d = await r.json()
            tx_b64 = d.get("swapTransaction")
    except Exception:
        return None

    if not tx_b64 or not config.OUR_PRIVATE_KEY:
        return None

    keypair = Keypair.from_base58_string(config.OUR_PRIVATE_KEY)
    raw = base64.b64decode(tx_b64)
    tx = VersionedTransaction.from_bytes(raw)
    tx.sign([keypair])

    send_payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "sendTransaction",
        "params": [
            base64.b64encode(bytes(tx)).decode(),
            {"encoding": "base64", "skipPreflight": False,
             "maxRetries": 3, "preflightCommitment": "confirmed"},
        ],
    }
    try:
        async with session.post(
            config.RPC_URL, json=send_payload, timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            d = await r.json()
            if "error" in d:
                log.error("sendTransaction error: %s", d["error"])
                return None
            return d.get("result")
    except Exception as e:
        log.error("send error: %s", e)
        return None


# ─── Торговая логика ──────────────────────────────────────────────────────────

def is_active() -> bool:
    return datetime.now(timezone.utc).hour in ACTIVE_HOURS


def should_skip_token(mint: str) -> Optional[str]:
    """Возвращает причину пропуска или None если токен подходит."""
    if mint in config.TOKEN_BLACKLIST:
        return "blacklist"
    if mint == USDC_MINT:
        return "stablecoin"
    # Cooldown — не заходим в один токен дважды за 60 секунд
    last = state.last_entry.get(mint, 0)
    if time.time() - last < SIGNAL_COOLDOWN_SECS:
        return f"cooldown ({int(time.time()-last)}s ago)"
    return None


async def open_position(mint: str, signal_type: str, session: aiohttp.ClientSession):
    """Открываем позицию по моментум-сигналу."""
    state.reset_daily_if_needed()

    if state.paused:
        return
    if not is_active():
        return

    skip_reason = should_skip_token(mint)
    if skip_reason:
        log.debug("Пропуск %s: %s", mint[:8], skip_reason)
        return

    if len(state.positions) >= MAX_POSITIONS:
        log.debug("Достигнут лимит позиций (%d)", MAX_POSITIONS)
        return

    # Проверка токена через фильтр
    ok, reason = await validate_token(mint, BUY_SIZE_SOL, session, config.HELIUS_API_KEY)
    if not ok:
        log.info("🚫 Токен %s не прошёл фильтр: %s", mint[:8], reason)
        return

    # Проверка баланса
    balance = await get_sol_balance(session)
    if balance < BUY_SIZE_SOL + 0.05:
        log.warning("Недостаточно SOL: %.3f < %.3f", balance, BUY_SIZE_SOL + 0.05)
        return

    lamports = int(BUY_SIZE_SOL * 1e9)

    # Получаем котировку для определения цены входа
    quote = await jupiter_quote(SOL_MINT, mint, lamports, session)
    if not quote:
        return

    out_amount = int(quote.get("outAmount", 0))
    if out_amount == 0:
        return

    # Цена входа: SOL / tokens
    entry_price = BUY_SIZE_SOL / (out_amount / 1e6)  # assuming 6 decimals

    log.info(
        "⚡ СИГНАЛ [%s] %s | Заходим на %.3f SOL | entry ~%.8f SOL/token",
        signal_type, mint[:12], BUY_SIZE_SOL, entry_price
    )

    sig = await jupiter_swap_and_send(SOL_MINT, mint, lamports, session)
    if not sig:
        log.warning("Не удалось открыть позицию %s", mint[:8])
        return

    state.last_entry[mint] = time.time()

    # Ждём подтверждения и получаем баланс
    await asyncio.sleep(3)
    token_balance = await get_token_balance(mint, session)
    if token_balance == 0:
        log.warning("Баланс токена = 0 после покупки %s", mint[:8])
        return

    state.positions[mint] = Position(
        mint=mint,
        buy_sol=BUY_SIZE_SOL,
        buy_price=entry_price,
        token_balance=token_balance,
        decimals=6,
        open_ts=time.time(),
    )
    log.info("✅ Позиция открыта: %s | %d tokens | sig %.12s...", mint[:8], token_balance, sig)


async def close_chunk(
    pos: Position, fraction: float, reason: str, session: aiohttp.ClientSession
) -> bool:
    """
    Продаём fraction от текущего баланса позиции.
    Возвращает True если продажа прошла.
    """
    to_sell = int(pos.token_balance * fraction)
    if to_sell <= 0:
        return False

    sig = await jupiter_swap_and_send(pos.mint, SOL_MINT, to_sell, session)
    if not sig:
        return False

    pos.token_balance -= to_sell
    log.info(
        "📤 ПРОДАЖА [%s] %s | %.0f%% позиции | sig %.12s...",
        reason, pos.mint[:8], fraction * 100, sig
    )
    return True


async def close_full(pos: Position, reason: str, session: aiohttp.ClientSession):
    """Закрываем всю оставшуюся позицию."""
    balance = await get_token_balance(pos.mint, session)
    if balance <= 0:
        state.positions.pop(pos.mint, None)
        return
    sig = await jupiter_swap_and_send(pos.mint, SOL_MINT, balance, session)
    state.positions.pop(pos.mint, None)
    held = time.time() - pos.open_ts
    log.info(
        "🔒 ЗАКРЫТА [%s] %s | удержание %.0fs | sig %.12s...",
        reason, pos.mint[:8], held, sig[:12] if sig else "None"
    )


async def get_current_price(pos: Position, session: aiohttp.ClientSession) -> Optional[float]:
    """
    Текущая цена токена в SOL через Jupiter.
    Спрашиваем котировку на продажу 1/10 позиции (чтобы не двигать рынок).
    """
    sample = max(int(pos.token_balance * 0.1), 1000)
    quote = await jupiter_quote(pos.mint, SOL_MINT, sample, session)
    if not quote:
        return None
    out = int(quote.get("outAmount", 0))
    if out == 0:
        return None
    # Цена: SOL / tokens
    return (out / 1e9) / (sample / 10 ** pos.decimals)


# ─── Position Manager (watchdog) ─────────────────────────────────────────────

async def position_manager(session: aiohttp.ClientSession):
    """
    Главный цикл управления позициями.
    Проверяет каждые 2 секунды: достигнуты ли цели, нужен ли стоп.
    """
    while True:
        await asyncio.sleep(2)

        for mint, pos in list(state.positions.items()):
            now = time.time()
            held = now - pos.open_ts

            # Emergency exit
            if held > EMERGENCY_SECS:
                log.warning("⚠️  Emergency exit %s (%.0fs)", mint[:8], held)
                await close_full(pos, "emergency", session)
                continue

            if pos.token_balance <= 0:
                state.positions.pop(mint, None)
                continue

            # Получаем текущую цену (не чаще раза в 3 секунды)
            if now - pos.last_exit_attempt < 3:
                continue
            pos.last_exit_attempt = now

            current_price = await get_current_price(pos, session)
            if current_price is None:
                continue

            price_change = (current_price - pos.buy_price) / pos.buy_price

            # Stop-loss
            if price_change <= -STOP_LOSS_PCT:
                log.warning("🛑 Stop-loss %s | %.1f%%", mint[:8], price_change * 100)
                await close_full(pos, f"stop-loss {price_change*100:.0f}%", session)
                state.daily_pnl -= pos.buy_sol * STOP_LOSS_PCT
                continue

            # Time-stop для чанка 3 (90 секунд)
            if not pos.exit_levels_done[2] and held > TIME_STOP_SECS:
                if not pos.exit_levels_done[1]:  # ещё не продали чанк 2
                    # Продаём всё остальное
                    log.info("⏰ Time-stop %s (%.0fs) | price_change: %+.1f%%",
                             mint[:8], held, price_change * 100)
                    await close_full(pos, "time-stop", session)
                    continue

            # Проверяем уровни выхода
            for i, level in enumerate(EXIT_LEVELS):
                if pos.exit_levels_done[i]:
                    continue
                if price_change >= level["pct"]:
                    # Продаём нужную долю от ТЕКУЩЕГО баланса
                    remaining_fraction = level["sell_fraction"]
                    sold = await close_chunk(
                        pos, remaining_fraction,
                        f"target+{level['pct']*100:.0f}%", session
                    )
                    if sold:
                        pos.exit_levels_done[i] = True
                        log.info(
                            "  %s: уровень %d сработал | price %+.1f%% | остаток %d",
                            mint[:8], i + 1, price_change * 100, pos.token_balance
                        )
                        if i == len(EXIT_LEVELS) - 1 or pos.token_balance <= 100:
                            state.positions.pop(mint, None)
                    break


# ─── On-chain transaction parser ─────────────────────────────────────────────

def parse_buy_from_tx(tx: dict) -> Optional[tuple[str, float, str]]:
    """
    Извлекаем BUY сделку из Helius enhanced transaction.
    Возвращает (mint, sol_spent, buyer_wallet) или None.
    """
    if not tx:
        return None

    # Ищем SOL → token swap
    for acc_data in tx.get("accountData", []):
        sol_change = acc_data.get("nativeBalanceChange", 0) / 1e9
        if sol_change >= -SIGNAL_MIN_SOL_EACH:  # не крупная трата
            continue
        buyer = acc_data.get("account", "")
        if buyer == config.OUR_WALLET:  # наши собственные транзакции
            continue

        # Ищем полученный токен
        for transfer in tx.get("tokenTransfers", []):
            if transfer.get("toUserAccount") == buyer:
                mint = transfer.get("mint")
                if mint and mint != USDC_MINT:
                    return (mint, abs(sol_change), buyer)

    return None


# ─── WebSocket listener ───────────────────────────────────────────────────────

async def market_listener(session: aiohttp.ClientSession):
    """
    Слушаем logs для Pump.fun программы и Jupiter v6.
    Детектируем крупные покупки → проверяем моментум → входим.
    """
    PROGRAMS_TO_WATCH = [
        "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",   # Pump.fun
        "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",   # Pump.fun AMM
        "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",   # Jupiter v6
    ]

    log.info("👁  Market listener запущен | программы: %d", len(PROGRAMS_TO_WATCH))

    # Подписываемся на каждую программу
    async def watch_program(program_id: str):
        while True:
            try:
                async with websockets.connect(
                    config.WSS_URL, ping_interval=20, ping_timeout=10
                ) as ws:
                    sub = {
                        "jsonrpc": "2.0", "id": 1,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [program_id]},
                            {"commitment": "confirmed"},
                        ],
                    }
                    await ws.send(json.dumps(sub))

                    async for raw in ws:
                        msg = json.loads(raw)
                        if "result" in msg:
                            continue

                        value = msg.get("params", {}).get("result", {}).get("value", {})
                        sig = value.get("signature")
                        err = value.get("err")

                        if not sig or err or sig in state.processed_sigs:
                            continue
                        state.record_sig(sig)

                        # Получаем полную транзакцию
                        tx = await helius_get_tx(sig, session)
                        parsed = parse_buy_from_tx(tx)
                        if not parsed:
                            continue

                        mint, sol_spent, buyer = parsed
                        state.add_buy_event(mint, sol_spent, buyer)

                        # Проверяем сигнал
                        signal = state.get_momentum_signal(mint)
                        if signal and mint not in state.positions:
                            asyncio.create_task(
                                open_position(mint, signal, session)
                            )

            except websockets.ConnectionClosed:
                log.warning("🔌 WS закрыт [%s], переподключение...", program_id[:12])
                await asyncio.sleep(5)
            except Exception as e:
                log.error("WS ошибка [%s]: %s", program_id[:12], e)
                await asyncio.sleep(10)

    await asyncio.gather(*[watch_program(p) for p in PROGRAMS_TO_WATCH])


# ─── Profit parking ───────────────────────────────────────────────────────────

async def profit_parker(session: aiohttp.ClientSession):
    """Периодически паркуем прибыль в USDC (как делает 2TE2F)."""
    while True:
        await asyncio.sleep(600)  # каждые 10 минут
        if state.positions:  # не паркуем если есть открытые позиции
            continue
        balance = await get_sol_balance(session)
        # Храним минимальный рабочий баланс + паркуем остальное
        min_working = BUY_SIZE_SOL * MAX_POSITIONS * 1.5 + 0.1
        to_park = balance - min_working
        if to_park >= PROFIT_PARK_SOL:
            log.info("💰 Паркуем %.3f SOL в USDC", to_park)
            lamports = int(to_park * 1e9)
            sig = await jupiter_swap_and_send(SOL_MINT, USDC_MINT, lamports, session)
            if sig:
                log.info("💎 Запарковано в USDC: %.3f SOL | sig %.12s...", to_park, sig)


# ─── Status logger ────────────────────────────────────────────────────────────

async def status_logger(session: aiohttp.ClientSession):
    while True:
        await asyncio.sleep(300)
        now = datetime.now(timezone.utc)
        balance = await get_sol_balance(session)
        active = is_active()

        # Статистика сигналов
        total_events = sum(len(q) for q in state.buy_events.values())
        tokens_with_activity = sum(1 for q in state.buy_events.values() if len(q) > 0)

        log.info(
            "📊 %s UTC | Баланс: %.4f SOL | Позиций: %d/%d | "
            "Дневной PnL: %+.4f | Токенов в радаре: %d | Активен: %s",
            now.strftime("%H:%M"),
            balance, len(state.positions), MAX_POSITIONS,
            state.daily_pnl, tokens_with_activity,
            "✅" if active else f"💤 (след. вкл в 15:00 UTC)",
        )


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    if not config.HELIUS_API_KEY:
        log.error("❌ HELIUS_API_KEY не задан. Скопируй .env.example → .env")
        return
    if not config.OUR_PRIVATE_KEY:
        log.error("❌ OUR_PRIVATE_KEY не задан")
        return
    if not config.OUR_WALLET:
        log.error("❌ OUR_WALLET_ADDRESS не задан")
        return

    log.info("🚀 Momentum Scalping Bot запущен")
    log.info("📐 Стратегия: моментум-сниппинг (по паттерну 2TE2F)")
    log.info("⚙️  Размер позиции: %.3f SOL | SL: %.0f%% | Выход: +15%%/+35%%/+80%%",
             BUY_SIZE_SOL, STOP_LOSS_PCT * 100)
    log.info("🕐 Активен: 15:00–23:59 UTC")
    log.info("📡 Сигнал: %d+ покупок по %.1f+ SOL за %ds ИЛИ 1 покупка >%.0f SOL",
             SIGNAL_MIN_BUYS, SIGNAL_MIN_SOL_EACH, SIGNAL_WINDOW_SECS, SIGNAL_BIG_BUY_SOL)

    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(
            market_listener(session),
            position_manager(session),
            profit_parker(session),
            status_logger(session),
        )


if __name__ == "__main__":
    asyncio.run(main())
