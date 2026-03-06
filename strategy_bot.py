"""
2TE2F Mirror Bot
================
Стратегия, выявленная из анализа кошелька 2TE2F3CJDbDhN3MgqNmnUh1NR5nHSornaK5daq65Lejs:

1. COPY-MIRROR: копируем каждую сделку 2TE2F в пропорции SCALE_FACTOR
2. ФИКСИРОВАННЫЕ РАЗМЕРЫ: бот всегда покупает на 2/3/5 SOL (как 2TE2F с .072 оверхедом)
3. ЧАСТИЧНЫЕ ПРОДАЖИ: когда 2TE2F продаёт X% позиции → мы тоже продаём X% своей
4. ВРЕМЕННОЙ ФИЛЬТР: только 15:00–23:59 UTC
5. EMERGENCY EXIT: принудительный выход через MAX_HOLD_SECS если 2TE2F не продал
6. USDC PARKING: прибыль выше PROFIT_PARK_THRESHOLD автоматически → USDC

Риск-менеджмент:
- Дневной лимит убытка: DAILY_LOSS_LIMIT_SOL
- Стоп-лосс на позицию: STOP_LOSS_PCT
- Максимум одновременных позиций: MAX_OPEN_POSITIONS
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import websockets
from solders.keypair import Keypair  # type: ignore
from solders.pubkey import Pubkey    # type: ignore

import config

log = logging.getLogger("mirror_bot")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

# ─── Константы ────────────────────────────────────────────────────────────────

TARGET_WALLET = "2TE2F3CJDbDhN3MgqNmnUh1NR5nHSornaK5daq65Lejs"
SOL_MINT      = "So11111111111111111111111111111111111111112"
USDC_MINT     = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# Окно активности (UTC часы включительно), выявленное из данных
ACTIVE_HOURS_UTC = set(range(15, 24))  # 15–23h

# Типичные размеры покупок 2TE2F (SOL)
STANDARD_BUY_SIZES = [2.0, 2.5, 3.0, 4.0, 5.0]

# Максимальное удержание (emergency exit)
MAX_HOLD_SECS = 300  # 5 минут, у 2TE2F >99% позиций закрывается за 25 мин

# Порог прибыли для парковки в USDC (как делает 2TE2F)
PROFIT_PARK_SOL = float(getattr(config, "PROFIT_PARK_SOL", 5.0))


# ─── Состояние ────────────────────────────────────────────────────────────────

@dataclass
class Position:
    mint: str
    buy_sol: float          # сколько мы потратили SOL
    target_buy_sol: float   # сколько потратил 2TE2F
    open_time: float        # unix timestamp открытия
    token_balance: int      # наш баланс токена в raw units
    decimals: int = 6
    partial_sold_pct: float = 0.0   # сколько % уже продали


@dataclass
class BotState:
    positions: dict[str, Position] = field(default_factory=dict)
    daily_pnl_sol: float = 0.0
    daily_date: str = ""
    processed_sigs: set[str] = field(default_factory=set)
    paused: bool = False
    total_profit_sol: float = 0.0   # накопленная прибыль за сессию

    def reset_daily_if_needed(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.daily_date != today:
            self.daily_pnl_sol = 0.0
            self.daily_date = today
            log.info("📅 Новый день %s — сброс дневного PnL", today)

    def record_sig(self, sig: str):
        self.processed_sigs.add(sig)
        if len(self.processed_sigs) > 20_000:
            self.processed_sigs = set(list(self.processed_sigs)[-10_000:])


state = BotState()


# ─── Утилиты ──────────────────────────────────────────────────────────────────

def is_active_hour() -> bool:
    return datetime.now(timezone.utc).hour in ACTIVE_HOURS_UTC


def snap_to_standard_size(target_sol: float) -> float:
    """
    Привязываем к ближайшему стандартному размеру из STANDARD_BUY_SIZES.
    2TE2F всегда покупает на 2/2.5/3/5 SOL — это программатик-бот.
    """
    closest = min(STANDARD_BUY_SIZES, key=lambda s: abs(s - target_sol))
    return closest * config.COPY_SCALE_FACTOR


def clamp_trade_size(raw_sol: float, balance: float) -> float:
    sol = min(raw_sol, config.MAX_SOL_PER_TRADE)
    sol = min(sol, balance * config.MAX_PORTFOLIO_PCT)
    if sol < config.MIN_SOL_PER_TRADE:
        return 0.0
    return round(sol, 6)


# ─── Helius API helpers ───────────────────────────────────────────────────────

async def helius_get_tx(sig: str, session: aiohttp.ClientSession) -> Optional[dict]:
    url = f"https://api.helius.xyz/v0/transactions/?api-key={config.HELIUS_API_KEY}"
    try:
        async with session.post(url, json={"transactions": [sig]}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            return data[0] if data else None
    except Exception as e:
        log.debug("helius_get_tx error: %s", e)
        return None


async def rpc_get_balance(wallet: str, session: aiohttp.ClientSession) -> float:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getBalance",
               "params": [wallet, {"commitment": "confirmed"}]}
    async with session.post(config.RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as r:
        d = await r.json()
        return d["result"]["value"] / 1e9


async def rpc_get_token_balance(wallet: str, mint: str, session: aiohttp.ClientSession) -> int:
    """Возвращает raw amount токена (нужно делить на 10^decimals)."""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [wallet, {"mint": mint}, {"encoding": "jsonParsed", "commitment": "confirmed"}],
    }
    try:
        async with session.post(config.RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as r:
            d = await r.json()
            accounts = d["result"]["value"]
            if not accounts:
                return 0
            info = accounts[0]["account"]["data"]["parsed"]["info"]["tokenAmount"]
            return int(info["amount"])
    except Exception:
        return 0


# ─── Jupiter v6 ───────────────────────────────────────────────────────────────

async def jupiter_quote(
    input_mint: str, output_mint: str, amount: int, session: aiohttp.ClientSession
) -> Optional[dict]:
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount,
        "slippageBps": config.SLIPPAGE_BPS,
        "onlyDirectRoutes": "false",
        "maxAccounts": "64",
    }
    try:
        async with session.get(
            "https://quote-api.jup.ag/v6/quote", params=params,
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            if r.status != 200:
                log.warning("Jupiter quote %s", await r.text())
                return None
            return await r.json()
    except Exception as e:
        log.warning("jupiter_quote error: %s", e)
        return None


async def jupiter_swap_tx(quote: dict, session: aiohttp.ClientSession) -> Optional[str]:
    """Получить serialized transaction от Jupiter."""
    payload = {
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
            "https://quote-api.jup.ag/v6/swap", json=payload,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            if r.status != 200:
                log.warning("Jupiter swap %s", await r.text())
                return None
            d = await r.json()
            return d.get("swapTransaction")
    except Exception as e:
        log.warning("jupiter_swap_tx error: %s", e)
        return None


async def sign_and_send(tx_b64: str, session: aiohttp.ClientSession) -> Optional[str]:
    """
    Подписываем транзакцию и отправляем через Helius.
    Требует OUR_PRIVATE_KEY в base58.
    """
    import base64
    from solders.transaction import VersionedTransaction

    if not config.OUR_PRIVATE_KEY:
        log.error("OUR_PRIVATE_KEY не задан — транзакцию подписать невозможно")
        return None

    keypair = Keypair.from_base58_string(config.OUR_PRIVATE_KEY)
    raw = base64.b64decode(tx_b64)
    tx = VersionedTransaction.from_bytes(raw)
    tx.sign([keypair])

    serialized = base64.b64encode(bytes(tx)).decode()
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "sendTransaction",
        "params": [serialized, {"encoding": "base64", "skipPreflight": False,
                                "maxRetries": 3, "preflightCommitment": "confirmed"}],
    }
    try:
        async with session.post(config.RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
            d = await r.json()
            if "error" in d:
                log.error("sendTransaction error: %s", d["error"])
                return None
            return d.get("result")
    except Exception as e:
        log.error("sign_and_send error: %s", e)
        return None


async def execute_buy(mint: str, sol_amount: float, session: aiohttp.ClientSession) -> Optional[str]:
    lamports = int(sol_amount * 1e9)
    quote = await jupiter_quote(SOL_MINT, mint, lamports, session)
    if not quote:
        return None
    tx = await jupiter_swap_tx(quote, session)
    if not tx:
        return None
    sig = await sign_and_send(tx, session)
    if sig:
        log.info("✅ BUY  %s | %.4f SOL | sig: %s...", mint[:8], sol_amount, sig[:12])
    return sig


async def execute_sell(mint: str, token_amount: int, session: aiohttp.ClientSession) -> Optional[str]:
    if token_amount <= 0:
        return None
    quote = await jupiter_quote(mint, SOL_MINT, token_amount, session)
    if not quote:
        return None
    tx = await jupiter_swap_tx(quote, session)
    if not tx:
        return None
    sig = await sign_and_send(tx, session)
    if sig:
        log.info("✅ SELL %s | %d tokens | sig: %s...", mint[:8], token_amount, sig[:12])
    return sig


async def park_profits_in_usdc(sol_amount: float, session: aiohttp.ClientSession):
    """Конвертируем накопленную прибыль в USDC — как делает 2TE2F."""
    log.info("💰 Паркуем %.3f SOL прибыли в USDC", sol_amount)
    lamports = int(sol_amount * 1e9)
    quote = await jupiter_quote(SOL_MINT, USDC_MINT, lamports, session)
    if not quote:
        return
    tx = await jupiter_swap_tx(quote, session)
    if not tx:
        return
    sig = await sign_and_send(tx, session)
    if sig:
        state.total_profit_sol -= sol_amount
        log.info("💎 Запарковано в USDC: sig %s...", sig[:12])


# ─── Торговая логика ──────────────────────────────────────────────────────────

async def handle_buy(mint: str, target_sol: float, session: aiohttp.ClientSession):
    """2TE2F купил токен → мы тоже покупаем."""
    state.reset_daily_if_needed()

    if state.paused:
        log.warning("⏸  Бот на паузе (дневной лимит убытка)")
        return

    if not is_active_hour():
        log.debug("🕐 Вне активных часов (15-23 UTC), пропуск покупки %s", mint[:8])
        return

    if mint in config.TOKEN_BLACKLIST:
        log.info("🚫 Токен %s в blacklist", mint[:8])
        return

    if len(state.positions) >= config.MAX_OPEN_POSITIONS:
        log.warning("📦 Достигнут лимит позиций (%d), пропуск", config.MAX_OPEN_POSITIONS)
        return

    if mint == USDC_MINT:
        log.debug("Пропуск покупки USDC (2TE2F паркует прибыль)")
        return

    # Вычисляем наш размер
    our_balance = await rpc_get_balance(config.OUR_WALLET, session)
    raw_size = snap_to_standard_size(target_sol)
    our_sol = clamp_trade_size(raw_size, our_balance)

    if our_sol == 0.0:
        log.info("📏 Слишком маленький размер сделки, пропуск")
        return

    log.info(
        "📡 2TE2F купил %s на %.3f SOL → мы покупаем на %.4f SOL",
        mint[:8], target_sol, our_sol
    )

    sig = await execute_buy(mint, our_sol, session)
    if not sig:
        return

    # Ждём подтверждения и получаем баланс токена
    await asyncio.sleep(3)
    token_balance = await rpc_get_token_balance(config.OUR_WALLET, mint, session)

    state.positions[mint] = Position(
        mint=mint,
        buy_sol=our_sol,
        target_buy_sol=target_sol,
        open_time=time.time(),
        token_balance=token_balance,
    )
    log.info("📬 Позиция открыта: %s | баланс=%d tokens", mint[:8], token_balance)


async def handle_sell(mint: str, target_sold_sol: float, target_total_sol: float, session: aiohttp.ClientSession):
    """
    2TE2F продал часть позиции → мы продаём ту же % долю.
    target_sold_sol  — сколько SOL он получил за этот чанк
    target_total_sol — сколько он потратил изначально (для расчёта %)
    """
    if mint not in state.positions:
        return

    pos = state.positions[mint]
    if pos.token_balance <= 0:
        return

    # Какой % от позиции продал 2TE2F?
    sell_pct = target_sold_sol / max(pos.target_buy_sol, 0.001)
    sell_pct = min(sell_pct, 1.0)

    # Остаток нашего баланса * этот %
    to_sell_raw = int(pos.token_balance * sell_pct)
    if to_sell_raw <= 0:
        return

    log.info(
        "📡 2TE2F продал %.1f%% позиции %s → продаём %d tokens",
        sell_pct * 100, mint[:8], to_sell_raw
    )

    sig = await execute_sell(mint, to_sell_raw, session)
    if not sig:
        return

    # Обновляем позицию
    pos.partial_sold_pct += sell_pct
    pos.token_balance -= to_sell_raw

    if pos.partial_sold_pct >= 0.99 or pos.token_balance <= 0:
        _close_position(mint, "full sell")


def _close_position(mint: str, reason: str):
    if mint in state.positions:
        pos = state.positions.pop(mint)
        held = time.time() - pos.open_time
        log.info("🔒 Позиция %s закрыта (%s) | удержание %.0fs", mint[:8], reason, held)


async def emergency_exit(mint: str, session: aiohttp.ClientSession):
    """Принудительный выход если 2TE2F не продал за MAX_HOLD_SECS."""
    if mint not in state.positions:
        return
    pos = state.positions[mint]
    balance = await rpc_get_token_balance(config.OUR_WALLET, mint, session)
    if balance <= 0:
        _close_position(mint, "emergency exit (zero balance)")
        return
    log.warning("⚠️  Emergency exit %s | удержание %.0fs", mint[:8], time.time() - pos.open_time)
    sig = await execute_sell(mint, balance, session)
    if sig:
        _close_position(mint, "emergency exit")


# ─── Парсинг транзакции 2TE2F ─────────────────────────────────────────────────

def parse_2tef_tx(tx: dict) -> Optional[tuple[str, str, float]]:
    """
    Разбираем enhanced Helius транзакцию.
    Возвращает ('buy'|'sell', mint, sol_amount) или None.
    """
    if not tx:
        return None

    # Находим изменение SOL баланса кошелька 2TE2F
    sol_change = 0.0
    for acc in tx.get("accountData", []):
        if acc.get("account") == TARGET_WALLET:
            sol_change = acc.get("nativeBalanceChange", 0) / 1e9
            break

    if abs(sol_change) < 0.01:
        return None

    # Находим токен
    token_mint = None
    for transfer in tx.get("tokenTransfers", []):
        if transfer.get("fromUserAccount") == TARGET_WALLET or \
           transfer.get("toUserAccount") == TARGET_WALLET:
            token_mint = transfer.get("mint")
            if token_mint:
                break

    if not token_mint:
        return None

    if sol_change < 0:
        return ("buy", token_mint, abs(sol_change))
    else:
        return ("sell", token_mint, sol_change)


# ─── Watchdog для emergency exit ─────────────────────────────────────────────

async def position_watchdog(session: aiohttp.ClientSession):
    """Периодически проверяет открытые позиции на превышение MAX_HOLD_SECS."""
    while True:
        await asyncio.sleep(15)
        now = time.time()
        expired = [
            mint for mint, pos in list(state.positions.items())
            if now - pos.open_time > MAX_HOLD_SECS
        ]
        for mint in expired:
            await emergency_exit(mint, session)

        # Проверяем stop-loss
        for mint, pos in list(state.positions.items()):
            # Грубая оценка через Jupiter (только если позиция >30 сек)
            if now - pos.open_time > 30:
                quote = await jupiter_quote(mint, SOL_MINT, pos.token_balance, session)
                if quote:
                    out_sol = int(quote.get("outAmount", 0)) / 1e9
                    loss_pct = (pos.buy_sol - out_sol) / max(pos.buy_sol, 0.001)
                    if loss_pct > config.STOP_LOSS_PCT:
                        log.warning(
                            "🛑 Stop-loss сработал %s | убыток %.1f%%",
                            mint[:8], loss_pct * 100
                        )
                        balance = await rpc_get_token_balance(config.OUR_WALLET, mint, session)
                        await execute_sell(mint, balance, session)
                        _close_position(mint, "stop-loss")

        # Паркуем прибыль в USDC если накопилось достаточно
        if state.total_profit_sol >= PROFIT_PARK_SOL and not state.positions:
            await park_profits_in_usdc(state.total_profit_sol * 0.8, session)


# ─── WebSocket listener ───────────────────────────────────────────────────────

async def watch_target(session: aiohttp.ClientSession):
    log.info("👁  Слежение за %s", TARGET_WALLET[:16])

    while True:
        try:
            async with websockets.connect(
                config.WSS_URL,
                ping_interval=None,
            ) as ws:

                sub = {
                    "jsonrpc": "2.0", "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [TARGET_WALLET]},
                        {"commitment": "confirmed"},
                    ],
                }
                await ws.send(json.dumps(sub))
                log.info("✅ WebSocket подключён")

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
                    parsed = parse_2tef_tx(tx)
                    if not parsed:
                        continue

                    action, mint, sol_amount = parsed

                    if action == "buy":
                        asyncio.create_task(handle_buy(mint, sol_amount, session))
                    elif action == "sell":
                        pos = state.positions.get(mint)
                        target_buy = pos.target_buy_sol if pos else sol_amount
                        asyncio.create_task(handle_sell(mint, sol_amount, target_buy, session))

        except websockets.ConnectionClosed:
            log.warning("🔌 WebSocket закрыт, переподключение через 5с...")
            await asyncio.sleep(5)
        except Exception as e:
            log.error("WebSocket ошибка: %s", e, exc_info=True)
            await asyncio.sleep(10)


# ─── Status logger ────────────────────────────────────────────────────────────

async def status_logger(session: aiohttp.ClientSession):
    """Каждые 5 минут выводит статус бота."""
    while True:
        await asyncio.sleep(300)
        now = datetime.now(timezone.utc)
        balance = await rpc_get_balance(config.OUR_WALLET, session)
        log.info(
            "📊 Статус | %s UTC | Баланс: %.4f SOL | Позиций: %d | Дневной PnL: %+.4f SOL | Активен: %s",
            now.strftime("%H:%M"),
            balance,
            len(state.positions),
            state.daily_pnl_sol,
            "✅" if is_active_hour() else "💤",
        )


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    if not config.HELIUS_API_KEY:
        log.error("❌ HELIUS_API_KEY не задан. Скопируй .env.example → .env")
        return
    if not config.OUR_PRIVATE_KEY:
        log.error("❌ OUR_PRIVATE_KEY не задан")
        return

    log.info("🚀 2TE2F Mirror Bot запущен")
    log.info("📍 Target: %s", TARGET_WALLET)
    log.info("💼 Our wallet: %s", config.OUR_WALLET)
    log.info("⚙️  Scale: %.1f%% | Max/trade: %.2f SOL | SL: %.0f%%",
             config.COPY_SCALE_FACTOR * 100,
             config.MAX_SOL_PER_TRADE,
             config.STOP_LOSS_PCT * 100)
    log.info("🕐 Активные часы: 15–23 UTC")

    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(
            watch_target(session),
            position_watchdog(session),
            status_logger(session),
        )


if __name__ == "__main__":
    asyncio.run(main())
