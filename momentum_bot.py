"""
Momentum Scalping Bot  ·  Стратегия по паттерну 2TE2F
======================================================

Запуск:
  DRY_RUN=true  python momentum_bot.py   # симуляция, реальных сделок нет
  DRY_RUN=false python momentum_bot.py   # боевой режим

Все параметры — в .env (см. .env.example).
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import websockets

import config
from token_filter import validate_token

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bot")

# ─── Режим работы ─────────────────────────────────────────────────────────────

DRY_RUN            = os.getenv("DRY_RUN", "true").lower() in ("true", "1", "yes")
TIME_FILTER        = os.getenv("TIME_FILTER", "false").lower() in ("true", "1", "yes")
ACTIVE_HOURS       = set(range(15, 24))   # 15–23 UTC

# ─── Параметры входа ──────────────────────────────────────────────────────────

BUY_SIZE_SOL       = float(os.getenv("BUY_SIZE_SOL",       "0.5"))
MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS",         "5"))
DAILY_LOSS_LIMIT   = float(os.getenv("DAILY_LOSS_LIMIT_SOL","1.0"))

# ─── Параметры сигнала ────────────────────────────────────────────────────────

SIG_WINDOW_SECS    = int(os.getenv("SIG_WINDOW_SECS",       "20"))   # окно (сек)
SIG_MIN_BUYS       = int(os.getenv("SIG_MIN_BUYS",          "3"))    # мин. покупок
SIG_MIN_SOL_EACH   = float(os.getenv("SIG_MIN_SOL_EACH",    "0.5"))  # мин. размер
SIG_BIG_BUY_SOL    = float(os.getenv("SIG_BIG_BUY_SOL",     "5.0")) # одна большая
SIG_COOLDOWN_SECS  = int(os.getenv("SIG_COOLDOWN_SECS",     "60"))   # не входить повторно

# ─── Параметры выхода (чанки) ─────────────────────────────────────────────────
# Формат: "процент_прибыли:доля_продажи" через запятую
# По умолчанию — выявленный паттерн 2TE2F: +15%→40%, +35%→35%, +80%→100%
_exit_raw = os.getenv("EXIT_LEVELS", "0.15:0.40,0.35:0.35,0.80:1.00")
EXIT_LEVELS: list[dict] = []
for part in _exit_raw.split(","):
    pct_str, frac_str = part.strip().split(":")
    EXIT_LEVELS.append({"pct": float(pct_str), "sell_fraction": float(frac_str)})

STOP_LOSS_PCT      = float(os.getenv("STOP_LOSS_PCT",       "0.25"))  # –25%
TIME_STOP_SECS     = int(os.getenv("TIME_STOP_SECS",        "90"))    # 90с таймер 3-го чанка
EMERGENCY_SECS     = int(os.getenv("EMERGENCY_SECS",        "300"))   # 5 мин принуд. выход

# ─── Константы ────────────────────────────────────────────────────────────────

SOL_MINT  = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

PROGRAMS_TO_WATCH = [
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",   # Pump.fun
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",   # Pump.fun AMM
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",   # Jupiter v6
]


# ─── Состояние ────────────────────────────────────────────────────────────────

@dataclass
class Position:
    mint: str
    buy_sol: float
    buy_price: float        # SOL / token
    token_balance: int      # raw units (6 decimals)
    open_ts: float
    exit_done: list[bool]   = field(default_factory=lambda: [False] * len(EXIT_LEVELS))
    last_price_check: float = 0.0

    # dry-run статистика
    simulated_return_sol: float = 0.0


@dataclass
class Stats:
    trades_total: int   = 0
    trades_win: int     = 0
    trades_loss: int    = 0
    pnl_total_sol: float = 0.0
    daily_pnl: float    = 0.0
    daily_date: str     = ""
    signals_seen: int   = 0
    signals_fired: int  = 0

    def reset_daily_if_needed(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.daily_date != today:
            self.daily_pnl = 0.0
            self.daily_date = today

    def record_close(self, pnl: float):
        self.trades_total += 1
        self.pnl_total_sol += pnl
        self.daily_pnl     += pnl
        if pnl > 0:
            self.trades_win  += 1
        else:
            self.trades_loss += 1


@dataclass
class BotState:
    positions: dict[str, Position]      = field(default_factory=dict)
    buy_events: dict[str, deque]        = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=30)))
    last_entry: dict[str, float]        = field(default_factory=dict)
    processed_sigs: set[str]            = field(default_factory=set)
    paused: bool                        = False
    virtual_sol_balance: float          = BUY_SIZE_SOL * MAX_POSITIONS * 3   # стартовый dry-run баланс
    stats: Stats                        = field(default_factory=Stats)

    def record_sig(self, sig: str):
        self.processed_sigs.add(sig)
        if len(self.processed_sigs) > 20_000:
            self.processed_sigs = set(list(self.processed_sigs)[-10_000:])

    def add_buy_event(self, mint: str, sol: float, buyer: str):
        self.buy_events[mint].append({"ts": time.time(), "sol": sol, "buyer": buyer})

    def get_signal(self, mint: str) -> Optional[str]:
        now    = time.time()
        events = self.buy_events.get(mint, deque())
        recent = [e for e in events if now - e["ts"] <= SIG_WINDOW_SECS]
        if not recent:
            return None
        # Одна очень крупная покупка
        biggest = max(e["sol"] for e in recent)
        if biggest >= SIG_BIG_BUY_SOL:
            return f"big_buy {biggest:.1f} SOL"
        # Кластер нескольких крупных от разных кошельков
        big = [e for e in recent if e["sol"] >= SIG_MIN_SOL_EACH]
        if len(big) >= SIG_MIN_BUYS and len({e["buyer"] for e in big}) >= 2:
            return f"cluster {len(big)}×{SIG_MIN_SOL_EACH}+ SOL"
        return None


state = BotState()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def is_active() -> bool:
    if not TIME_FILTER:
        return True
    return datetime.now(timezone.utc).hour in ACTIVE_HOURS


def _dry(label: str) -> str:
    return f"[DRY] {label}" if DRY_RUN else label


# ─── RPC / API ────────────────────────────────────────────────────────────────

async def rpc(method: str, params: list, session: aiohttp.ClientSession) -> Optional[dict]:
    try:
        async with session.post(
            config.RPC_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            return await r.json()
    except Exception:
        return None


async def get_sol_balance(session: aiohttp.ClientSession) -> float:
    if DRY_RUN:
        return state.virtual_sol_balance
    r = await rpc("getBalance", [config.OUR_WALLET, {"commitment": "confirmed"}], session)
    return r["result"]["value"] / 1e9 if r and "result" in r else 0.0


async def get_token_balance(mint: str, session: aiohttp.ClientSession) -> int:
    if DRY_RUN:
        pos = state.positions.get(mint)
        return pos.token_balance if pos else 0
    r = await rpc(
        "getTokenAccountsByOwner",
        [config.OUR_WALLET, {"mint": mint},
         {"encoding": "jsonParsed", "commitment": "confirmed"}],
        session,
    )
    try:
        return int(r["result"]["value"][0]["account"]["data"]["parsed"]["info"]
                   ["tokenAmount"]["amount"])
    except Exception:
        return 0


async def helius_get_tx(sig: str, session: aiohttp.ClientSession) -> Optional[dict]:
    url = f"https://api.helius.xyz/v0/transactions/?api-key={config.HELIUS_API_KEY}"
    try:
        async with session.post(
            url, json={"transactions": [sig]},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            data = await r.json()
            return data[0] if data else None
    except Exception:
        return None


# ─── Jupiter ──────────────────────────────────────────────────────────────────

async def jup_quote(
    input_mint: str, output_mint: str, amount: int,
    session: aiohttp.ClientSession,
) -> Optional[dict]:
    try:
        async with session.get(
            "https://quote-api.jup.ag/v6/quote",
            params={
                "inputMint":  input_mint,
                "outputMint": output_mint,
                "amount":     amount,
                "slippageBps": config.SLIPPAGE_BPS,
                "maxAccounts": "64",
            },
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            return await r.json() if r.status == 200 else None
    except Exception:
        return None


async def jup_swap(
    input_mint: str, output_mint: str, amount: int,
    session: aiohttp.ClientSession,
) -> Optional[str]:
    """В dry-run возвращает фиктивный sig, в боевом — реальный."""
    if DRY_RUN:
        return f"DRY_{int(time.time()*1000)}"

    import base64
    from solders.keypair import Keypair
    from solders.transaction import VersionedTransaction

    quote = await jup_quote(input_mint, output_mint, amount, session)
    if not quote:
        return None

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
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            d    = await r.json()
            tx64 = d.get("swapTransaction")
    except Exception:
        return None

    if not tx64 or not config.OUR_PRIVATE_KEY:
        return None

    kp  = Keypair.from_base58_string(config.OUR_PRIVATE_KEY)
    tx  = VersionedTransaction.from_bytes(base64.b64decode(tx64))
    tx.sign([kp])

    try:
        async with session.post(
            config.RPC_URL,
            json={
                "jsonrpc": "2.0", "id": 1,
                "method": "sendTransaction",
                "params": [
                    base64.b64encode(bytes(tx)).decode(),
                    {"encoding": "base64", "skipPreflight": False,
                     "maxRetries": 3, "preflightCommitment": "confirmed"},
                ],
            },
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            d = await r.json()
            return d.get("result") if "error" not in d else None
    except Exception:
        return None


# ─── Цена позиции ─────────────────────────────────────────────────────────────

async def get_price_change(pos: Position, session: aiohttp.ClientSession) -> Optional[float]:
    """Текущее изменение цены относительно цены покупки (0.15 = +15%)."""
    sample = max(int(pos.token_balance * 0.1), 1_000)
    quote  = await jup_quote(pos.mint, SOL_MINT, sample, session)
    if not quote:
        return None
    out = int(quote.get("outAmount", 0))
    if out == 0:
        return None
    current_price = (out / 1e9) / (sample / 1e6)
    return (current_price - pos.buy_price) / pos.buy_price


# ─── Торговые операции ────────────────────────────────────────────────────────

async def open_position(mint: str, signal: str, session: aiohttp.ClientSession):
    state.stats.reset_daily_if_needed()

    if state.paused:
        return
    if not is_active():
        log.debug("Вне активных часов, пропуск")
        return
    if mint in config.TOKEN_BLACKLIST or mint == USDC_MINT:
        return
    if time.time() - state.last_entry.get(mint, 0) < SIG_COOLDOWN_SECS:
        return
    if len(state.positions) >= MAX_POSITIONS:
        return
    if state.stats.daily_pnl < -DAILY_LOSS_LIMIT:
        if not state.paused:
            log.warning("⛔ Дневной лимит убытка (%.3f SOL). Пауза до UTC 00:00.", state.stats.daily_pnl)
            state.paused = True
        return

    # Фильтр ликвидности
    ok, reason = await validate_token(mint, BUY_SIZE_SOL, session, config.HELIUS_API_KEY)
    if not ok:
        log.info("🚫 %s не прошёл фильтр: %s", mint[:8], reason)
        return

    # Баланс
    balance = await get_sol_balance(session)
    if balance < BUY_SIZE_SOL + 0.05:
        log.warning("💸 Недостаточно SOL: %.3f", balance)
        return

    # Quote для цены входа
    lamports = int(BUY_SIZE_SOL * 1e9)
    quote    = await jup_quote(SOL_MINT, mint, lamports, session)
    if not quote:
        return
    out_amount = int(quote.get("outAmount", 0))
    if out_amount == 0:
        return

    entry_price = BUY_SIZE_SOL / (out_amount / 1e6)

    tag = "🟡 [DRY]" if DRY_RUN else "🟢 [LIVE]"
    log.info(
        "%s ВХОД  %s | сигнал: %s | %.3f SOL | цена: %.8f",
        tag, mint[:16], signal, BUY_SIZE_SOL, entry_price,
    )

    sig = await jup_swap(SOL_MINT, mint, lamports, session)
    if not sig:
        log.warning("Не удалось войти в %s", mint[:8])
        return

    state.last_entry[mint] = time.time()
    state.stats.signals_fired += 1

    if DRY_RUN:
        state.virtual_sol_balance -= BUY_SIZE_SOL

    state.positions[mint] = Position(
        mint=mint,
        buy_sol=BUY_SIZE_SOL,
        buy_price=entry_price,
        token_balance=out_amount,
        open_ts=time.time(),
        exit_done=[False] * len(EXIT_LEVELS),
    )


async def sell_chunk(
    pos: Position, fraction: float, reason: str,
    price_change: float, session: aiohttp.ClientSession,
) -> bool:
    """Продаём fraction от текущего токен-баланса. Возвращает True если прошло."""
    to_sell = int(pos.token_balance * fraction)
    if to_sell <= 0:
        return False

    if DRY_RUN:
        # Симулируем возврат SOL через Jupiter quote
        quote = await jup_quote(pos.mint, SOL_MINT, to_sell, session)
        if quote:
            sol_back = int(quote.get("outAmount", 0)) / 1e9
        else:
            # Fallback: рассчитываем через price_change
            sol_back = pos.buy_sol * fraction * (1 + price_change)
        state.virtual_sol_balance += sol_back
        pos.simulated_return_sol  += sol_back
        sig = f"DRY_{int(time.time()*1000)}"
    else:
        sig = await jup_swap(pos.mint, SOL_MINT, to_sell, session)
        if not sig:
            return False

    pos.token_balance -= to_sell
    tag = "🟡 [DRY]" if DRY_RUN else "🟢 [LIVE]"
    log.info(
        "%s ПРОДАЖА  %s | %s | %.0f%% позиции | цена %+.1f%% | sig: %s",
        tag, pos.mint[:16], reason, fraction * 100, price_change * 100, sig[:16],
    )
    return True


async def close_all(
    pos: Position, reason: str,
    price_change: float, session: aiohttp.ClientSession,
):
    """Закрываем всю оставшуюся позицию."""
    if pos.token_balance <= 0:
        state.positions.pop(pos.mint, None)
        return

    if DRY_RUN:
        quote = await jup_quote(pos.mint, SOL_MINT, pos.token_balance, session)
        if quote:
            sol_back = int(quote.get("outAmount", 0)) / 1e9
        else:
            remaining_frac = pos.token_balance / (pos.token_balance + 1)   # approx
            sol_back = pos.buy_sol * (1 + price_change)
        state.virtual_sol_balance += sol_back
        pos.simulated_return_sol  += sol_back
        sig = f"DRY_{int(time.time()*1000)}"
    else:
        real_balance = await get_token_balance(pos.mint, session)
        sig = await jup_swap(pos.mint, SOL_MINT, real_balance, session)

    held = time.time() - pos.open_ts
    pnl  = pos.simulated_return_sol - pos.buy_sol if DRY_RUN else 0.0

    tag = "🟡 [DRY]" if DRY_RUN else "🟢 [LIVE]"
    log.info(
        "%s ЗАКРЫТА %s | %s | удержание: %.0fs | PnL: %+.4f SOL | sig: %s",
        tag, pos.mint[:16], reason, held, pnl, sig[:16] if sig else "ERR",
    )

    if DRY_RUN:
        state.stats.record_close(pnl)

    state.positions.pop(pos.mint, None)


# ─── Position Manager ─────────────────────────────────────────────────────────

async def position_manager(session: aiohttp.ClientSession):
    while True:
        await asyncio.sleep(2)

        for mint, pos in list(state.positions.items()):
            now  = time.time()
            held = now - pos.open_ts

            # Emergency exit
            if held > EMERGENCY_SECS:
                log.warning("⚠️  Emergency exit %s (%.0fs)", mint[:8], held)
                pc = await get_price_change(pos, session) or 0.0
                await close_all(pos, "emergency", pc, session)
                continue

            if pos.token_balance <= 0:
                state.positions.pop(mint, None)
                continue

            # Не проверяем цену чаще раза в 3с
            if now - pos.last_price_check < 3:
                continue
            pos.last_price_check = now

            pc = await get_price_change(pos, session)
            if pc is None:
                continue

            # Stop-loss
            if pc <= -STOP_LOSS_PCT:
                log.warning("🛑 Stop-loss %s | %+.1f%%", mint[:8], pc * 100)
                await close_all(pos, f"stop-loss {pc*100:.0f}%", pc, session)
                continue

            # Time-stop (чанк 3 не дождался цели)
            if held > TIME_STOP_SECS and not pos.exit_done[-1]:
                log.info("⏰ Time-stop %s | %+.1f%%", mint[:8], pc * 100)
                await close_all(pos, "time-stop", pc, session)
                continue

            # Уровни выхода
            for i, lvl in enumerate(EXIT_LEVELS):
                if pos.exit_done[i]:
                    continue
                if pc >= lvl["pct"]:
                    sold = await sell_chunk(pos, lvl["sell_fraction"],
                                            f"+{lvl['pct']*100:.0f}%", pc, session)
                    if sold:
                        pos.exit_done[i] = True
                        if i == len(EXIT_LEVELS) - 1 or pos.token_balance <= 500:
                            pnl = pos.simulated_return_sol - pos.buy_sol
                            if DRY_RUN:
                                state.stats.record_close(pnl)
                            state.positions.pop(mint, None)
                    break


# ─── TX Parser ────────────────────────────────────────────────────────────────

def parse_buy(tx: dict) -> Optional[tuple[str, float, str]]:
    """Возвращает (mint, sol_spent, buyer) или None."""
    if not tx:
        return None
    for acc in tx.get("accountData", []):
        change = acc.get("nativeBalanceChange", 0) / 1e9
        if change >= -SIG_MIN_SOL_EACH:
            continue
        buyer = acc.get("account", "")
        if buyer == config.OUR_WALLET:
            continue
        for tr in tx.get("tokenTransfers", []):
            if tr.get("toUserAccount") == buyer:
                mint = tr.get("mint")
                if mint and mint != USDC_MINT:
                    return mint, abs(change), buyer
    return None


# ─── WebSocket listener ───────────────────────────────────────────────────────

async def watch_program(program_id: str, session: aiohttp.ClientSession):
    label = program_id[:16]
    while True:
        try:
            async with websockets.connect(
                config.WSS_URL, ping_interval=20, ping_timeout=10
            ) as ws:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1,
                    "method": "logsSubscribe",
                    "params": [{"mentions": [program_id]}, {"commitment": "confirmed"}],
                }))
                log.debug("WS подключён: %s", label)

                async for raw in ws:
                    msg   = json.loads(raw)
                    if "result" in msg:
                        continue
                    value = msg.get("params", {}).get("result", {}).get("value", {})
                    sig   = value.get("signature")
                    if not sig or value.get("err") or sig in state.processed_sigs:
                        continue
                    state.record_sig(sig)

                    tx     = await helius_get_tx(sig, session)
                    parsed = parse_buy(tx)
                    if not parsed:
                        continue

                    mint, sol_spent, buyer = parsed
                    state.add_buy_event(mint, sol_spent, buyer)
                    state.stats.signals_seen += 1

                    signal = state.get_signal(mint)
                    if signal and mint not in state.positions:
                        asyncio.create_task(open_position(mint, signal, session))

        except websockets.ConnectionClosed:
            log.warning("WS закрыт [%s], реконнект...", label)
            await asyncio.sleep(5)
        except Exception as e:
            log.error("WS ошибка [%s]: %s", label, e)
            await asyncio.sleep(10)


async def market_listener(session: aiohttp.ClientSession):
    await asyncio.gather(*[watch_program(p, session) for p in PROGRAMS_TO_WATCH])


# ─── Status Logger ────────────────────────────────────────────────────────────

async def status_logger(session: aiohttp.ClientSession):
    while True:
        await asyncio.sleep(60)
        now     = datetime.now(timezone.utc)
        balance = await get_sol_balance(session)
        s       = state.stats
        wr      = s.trades_win / s.trades_total * 100 if s.trades_total else 0

        mode = "DRY RUN 🟡" if DRY_RUN else "LIVE 🟢"
        tf   = f"⏰ {ACTIVE_HOURS}" if TIME_FILTER else "🕐 без ограничений"

        log.info("─" * 65)
        log.info("📊 %s UTC  |  Режим: %s  |  Время: %s", now.strftime("%H:%M"), mode, tf)
        log.info(
            "   Баланс:      %.4f SOL%s",
            balance, " (виртуальный)" if DRY_RUN else "",
        )
        log.info(
            "   Позиций:     %d / %d открыто",
            len(state.positions), MAX_POSITIONS,
        )
        log.info(
            "   Сделок:      %d всего | %d побед | %d потерь | WR %.0f%%",
            s.trades_total, s.trades_win, s.trades_loss, wr,
        )
        log.info(
            "   PnL сессии:  %+.4f SOL  |  дневной: %+.4f SOL",
            s.pnl_total_sol, s.daily_pnl,
        )
        log.info(
            "   Сигналов:    %d замечено | %d вошли",
            s.signals_seen, s.signals_fired,
        )
        log.info(
            "   Параметры:   BUY=%.2f SOL | SL=%.0f%% | EXIT=%s",
            BUY_SIZE_SOL, STOP_LOSS_PCT * 100,
            " / ".join(f"+{l['pct']*100:.0f}%→{l['sell_fraction']*100:.0f}%%" for l in EXIT_LEVELS),
        )
        if state.positions:
            log.info("   Открытые позиции:")
            for m, p in state.positions.items():
                held = time.time() - p.open_ts
                log.info(
                    "     %s | удержание %.0fs | баланс %d tok",
                    m[:24], held, p.token_balance,
                )
        log.info("─" * 65)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    if not config.HELIUS_API_KEY:
        log.error("HELIUS_API_KEY не задан — скопируй .env.example → .env")
        return

    if not DRY_RUN and (not config.OUR_PRIVATE_KEY or not config.OUR_WALLET):
        log.error("В боевом режиме нужны OUR_PRIVATE_KEY и OUR_WALLET_ADDRESS")
        return

    mode = "🟡 DRY RUN (сделки не исполняются)" if DRY_RUN else "🟢 LIVE (реальные деньги!)"
    log.info("━" * 65)
    log.info("  Momentum Scalping Bot")
    log.info("  Режим:       %s", mode)
    log.info("  Вход:        %.3f SOL / сделку  |  макс позиций: %d", BUY_SIZE_SOL, MAX_POSITIONS)
    log.info("  Стоп:        –%.0f%%  |  Emergency: %ds", STOP_LOSS_PCT * 100, EMERGENCY_SECS)
    log.info("  Уровни:      %s",
             "  →  ".join(f"+{l['pct']*100:.0f}% sell {l['sell_fraction']*100:.0f}%%" for l in EXIT_LEVELS))
    log.info("  Сигнал:      %d покупок ≥%.1f SOL за %ds  ИЛИ  одна ≥%.0f SOL",
             SIG_MIN_BUYS, SIG_MIN_SOL_EACH, SIG_WINDOW_SECS, SIG_BIG_BUY_SOL)
    log.info("  Время:       %s", f"15–23 UTC" if TIME_FILTER else "круглосуточно (TIME_FILTER=false)")
    log.info("━" * 65)

    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(
            market_listener(session),
            position_manager(session),
            status_logger(session),
        )


if __name__ == "__main__":
    asyncio.run(main())
