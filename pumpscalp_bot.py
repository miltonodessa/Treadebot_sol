"""
PUMPSCALP v4.0 — Pump.fun Direct Bonding Curve Sniper
Стратегия реконструирована из ПОЛНОГО анализа 101,190 сделок кошелька:
  nya666pQkP3PzWxi7JngU3rRMHuc7zbLK8c8wxQ4qpT (9 файлов part_1–9.xlsx)

══════════════════════════════════════════════════════════════
  РЕАЛЬНАЯ СТРАТЕГИЯ (из 49,867 завершённых пар buy→sell):

  СТАТИСТИКА:
    Win rate:     34.6%
    Avg win:      +41.3%
    Avg loss:     -17.8%
    Медиан hold:  23s    (сред. 44s)
    Best trade:   +39,176%  (держал 38 мин!)
    EV/сделку:    +2.7%
    Сделок/день:  ~1,700

  1. ВХОД: НЕМЕДЛЕННО на каждый новый токен, без watch-window.
     - Avg размер: 0.338 SOL. Размер >1 SOL → WR 54% (лучшие конвикции).
     - Никакого scoring / filtering — скорость = всё.

  2. ВЫХОДЫ — ДВА CHECKPOINT-А:
     - T+8s  QUICK_STOP: если цена не выросла → токен мёртвый → выходим.
       (6,520 выходов на 5-10s из 49,867 — самый большой кластер потерь)
     - T+25s MOMENTUM_GATE: если рост < 5% → выходим.
       (ещё ~4,000 выходов на 25-45s)
     - Прошёл оба gate: WR прыгает до 40-55% → ДЕРЖИМ.

  3. HOLD TIME vs WIN RATE (из данных):
     - <30s:    WR=25%  avg_win=+25%  (режем лузеров)
     - 30-60s:  WR=29%  avg_win=+49%
     - 1-2 мин: WR=40%  avg_win=+59%
     - 2-5 мин: WR=48%  avg_win=+58%
     - >5 мин:  WR=55%  avg_win=+52%  ← лучшая зона, держим!

  4. RE-ENTRY: после выхода из позиции, если токен ещё растёт →
     повторный вход через 60-120s с тем же размером.

  5. TP УРОВНИ (оптимизированы под +41.3% avg win):
     - TP1 +50%: продаём 35% (фиксируем)
     - TP2 +100%: продаём 50% оставшегося (защищаем прибыль)
     - TP3 +500%: продаём остаток (moonbag → mega-winner)
     - Trailing SL: активируется после +50%, тянется в -15% от пика.

  МАТЕМАТИКА:
    EV = 0.346 × 41.3% + 0.654 × (−17.8%) ≈ +2.7% на сделку
    При 1,700 сделок/день × 0.3 SOL × 2.7% ≈ 13.8 SOL/день
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

import csv
import aiohttp
import websockets
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

import config

# ─── Файл для сохранения истории сделок ───────────────────────────────────────
_SESSION_START  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
TRADES_CSV_PATH = os.getenv("TRADES_CSV_PATH", f"trades_{_SESSION_START}.csv")
TRADES_JSON_PATH = TRADES_CSV_PATH.replace(".csv", "_summary.json")

# ─── Накопительный мастер-лог (пополняется каждую сделку, не перезаписывается) ─
MASTER_LOG     = os.getenv("MASTER_LOG", "trade_log_master.csv")
# ─── ML-параметры от ночного тренера ──────────────────────────────────────────
ML_PARAMS_FILE = os.getenv("ML_PARAMS_FILE", "ml_params.json")

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

# ── Pump.fun: total supply ───────────────────────────────────────────────────
PUMP_TOTAL_SUPPLY      = 1_000_000_000   # 1B токенов — стандартный pump.fun
PUMP_MAYHEM_SUPPLY     = 2_000_000_000   # 2B токенов — Mayhem mode (AI-агент)

# ── Mayhem mode фильтр ───────────────────────────────────────────────────────
# Mayhem mode: AI-агент (Agent Pumpy) торгует токеном 24h после создания.
# vTokensInBondingCurve при создании: стандарт ~793M, Mayhem ~1.79B.
# Причины пропускать:
#   - Цену двигает AI, а не реальный спрос → паттерны стратегии не работают
#   - Случайный walk (равные buy/sell) → невозможно предсказать momentum
#   - После 24h AI сжигает токены → резкий дамп
SKIP_MAYHEM_MODE       = os.getenv("SKIP_MAYHEM_MODE", "true").lower() == "true"
MAYHEM_VTOKENS_THRESH  = float(os.getenv("MAYHEM_VTOKENS_THRESH", "1_500_000_000"))  # 1.5B
# Кошелёк AI-агента (если он в traderPublicKey при создании — Mayhem)
MAYHEM_AI_AGENT_WALLET = "BwWK17cbHxwWBKZkUYvzxLcNQ1YVyaFezduWbtm2de6s"

# ── Размер позиции (медиана из реальных данных nya666: 0.338 SOL) ───────────
BUY_SIZE_SOL         = float(os.getenv("BUY_SIZE_SOL", "0.3"))
BUY_SIZE_REENTRY_SOL = float(os.getenv("BUY_SIZE_REENTRY_SOL", str(BUY_SIZE_SOL)))
MAX_POSITIONS        = int(os.getenv("MAX_POSITIONS", "10"))

# ── Выходы (из анализа 49,867 завершённых пар) ──────────────────────────────
# >5 мин WR=55%, time stop не должен рубить раньше
HARD_TIME_STOP_MIN   = int(os.getenv("HARD_TIME_STOP_MIN", "8"))
SL_PCT               = float(os.getenv("SL_PCT", "0.15"))         # avg loss = -17.8% → SL 15%

# TP1 +50%: продать 35% (avg win +41.3% → первый фикс чуть выше среднего)
TP1_PCT              = float(os.getenv("TP1_PCT", "0.50"))
TP1_SELL_FRAC        = float(os.getenv("TP1_SELL_FRAC", "0.35"))
# TP2 +100%: продать 50% остатка (защита прибыли, оставляем ~15% moonbag)
TP2_PCT              = float(os.getenv("TP2_PCT", "1.00"))
TP2_SELL_FRAC        = float(os.getenv("TP2_SELL_FRAC", "0.50"))
# TP3 +500%: продать весь moonbag (лучший трейд was +39,176%)
TP3_PCT              = float(os.getenv("TP3_PCT", "5.00"))
TP3_SELL_FRAC        = float(os.getenv("TP3_SELL_FRAC", "1.00"))

# ── Momentum checkpoints ─────────────────────────────────────────────────────
# T+8s QUICK_STOP: 6,520 выходов на 5-10s — самый большой кластер потерь.
# Если цена не выросла (≤ entry) → токен мёртвый → выходим немедленно.
QUICK_STOP_SEC       = int(os.getenv("QUICK_STOP_SEC", "8"))
# T+25s MOMENTUM_GATE: второй кластер выходов. Рост <5% за 25s → dead.
MOMENTUM_GATE_SEC    = int(os.getenv("MOMENTUM_GATE_SEC", "25"))
# Минимальный рост за MOMENTUM_GATE_SEC чтобы НЕ выходить
MOMENTUM_MIN_PCT     = float(os.getenv("MOMENTUM_MIN_PCT", "0.05"))  # 5% рост

# Trailing: после +50% → SL подтягивается в -15% от пика
TRAILING_TRIGGER_PCT = float(os.getenv("TRAILING_TRIGGER_PCT", "0.50"))
TRAILING_DISTANCE    = float(os.getenv("TRAILING_DISTANCE", "0.15"))  # -15% от пика

# ── Token age (только свежие pump.fun токены) ───────────────────────────────
MAX_TOKEN_AGE_SEC    = int(os.getenv("MAX_TOKEN_AGE_SEC", "120"))  # не позже 2 мин после запуска

# ── Re-entry ─────────────────────────────────────────────────────────────────
# Из данных: scale-in на победителях — если токен ещё качает через 60-300s,
# повторно заходим с тем же размером.
REENTRY_MIN_SEC      = int(os.getenv("REENTRY_MIN_SEC", "60"))
REENTRY_MAX_SEC      = int(os.getenv("REENTRY_MAX_SEC", "300"))
REENTRY_MIN_PUMP_PCT = float(os.getenv("REENTRY_MIN_PUMP_PCT", "0.02"))
MAX_REENTRIES        = int(os.getenv("MAX_REENTRIES", "2"))   # до 2 re-entry на токен

# ── Риск ────────────────────────────────────────────────────────────────────
DAILY_LOSS_LIMIT_SOL = float(os.getenv("DAILY_LOSS_LIMIT_SOL", "5.0"))  # стоп при -5 SOL/день

# ── Фильтры на основе данных token_signals (25,987 сделок) ──────────────────
# Dev buy 0.5-2 SOL: WR=16.3%, avg_pnl=-0.0093 → пропускать
# Dev buy >2 SOL: WR=24.1% → пропускать (слишком агрессивный дев)
# Dev buy 0-0.5 SOL: WR=29%, dev buy = нет: WR=32.2% → нормально
DEV_BUY_SKIP_MIN_SOL = float(os.getenv("DEV_BUY_SKIP_MIN_SOL", "0.5"))
DEV_BUY_SKIP_MAX_SOL = float(os.getenv("DEV_BUY_SKIP_MAX_SOL", "9999.0"))  # skip 0.5+ SOL dev buys

# Зона смерти по числу покупателей в первые 30s:
# 3-50 buyers_in_60s: WR=1-20% (средняя ажиотажная зона — ловушка)
# <3 buyers: WR=31.3% или >50 buyers: WR=39-40% → держим
# На практике к T+25s видим ~половину от 60s → зона [3..50] → проверяем на MOMENTUM_GATE
BUYER_DEAD_ZONE_MIN  = int(os.getenv("BUYER_DEAD_ZONE_MIN", "3"))
BUYER_DEAD_ZONE_MAX  = int(os.getenv("BUYER_DEAD_ZONE_MAX", "50"))

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
# ML: накопительный лог + загрузка параметров от ночного тренера
# ═══════════════════════════════════════════════════════════════════════════════

_MASTER_FIELDS = [
    "opened_at", "closed_at", "hold_sec", "symbol", "mint",
    "entry_sol", "exit_sol", "pnl_sol", "pnl_pct",
    "entry_mcap_sol", "exit_mcap_sol", "exit_reason",
    "is_reentry", "reentry_num", "passed_quick_stop", "passed_momentum",
]


def _append_master(t) -> None:
    """Дозапись одной закрытой сделки в накопительный мастер-лог (append)."""
    exists = os.path.exists(MASTER_LOG)
    try:
        with open(MASTER_LOG, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=_MASTER_FIELDS)
            if not exists:
                w.writeheader()
            w.writerow({
                "opened_at":         t.opened_at,
                "closed_at":         t.closed_at,
                "hold_sec":          t.hold_sec,
                "symbol":            t.symbol,
                "mint":              t.mint,
                "entry_sol":         t.entry_sol,
                "exit_sol":          t.exit_sol,
                "pnl_sol":           t.pnl_sol,
                "pnl_pct":           t.pnl_pct,
                "entry_mcap_sol":    t.entry_mcap_sol,
                "exit_mcap_sol":     t.exit_mcap_sol,
                "exit_reason":       t.exit_reason,
                "is_reentry":        t.is_reentry,
                "reentry_num":       t.reentry_num,
                "passed_quick_stop": t.passed_quick_stop,
                "passed_momentum":   t.passed_momentum,
            })
    except Exception as e:
        log.debug("master log append error: %s", e)


def _load_ml_params() -> None:
    """Загрузить параметры от ML-тренера (ml_params.json) и применить глобально."""
    global QUICK_STOP_SEC, MOMENTUM_GATE_SEC, MOMENTUM_MIN_PCT, SL_PCT
    global TP1_PCT, TP1_SELL_FRAC, TP2_PCT, TP2_SELL_FRAC, TRAILING_DISTANCE
    global MIN_ENTRY_MCAP_SOL, MAX_ENTRY_MCAP_SOL, ML_ENTRY_FILTER_ENABLED

    if not os.path.exists(ML_PARAMS_FILE):
        return
    try:
        with open(ML_PARAMS_FILE, encoding="utf-8") as f:
            p = json.load(f)
        params = p.get("best_params", {})

        if "quick_stop_sec"     in params: QUICK_STOP_SEC     = int(params["quick_stop_sec"])
        if "momentum_gate_sec"  in params: MOMENTUM_GATE_SEC  = int(params["momentum_gate_sec"])
        if "momentum_min_pct"   in params: MOMENTUM_MIN_PCT   = float(params["momentum_min_pct"])
        if "sl_pct"             in params: SL_PCT             = float(params["sl_pct"])
        if "tp1_pct"            in params: TP1_PCT            = float(params["tp1_pct"])
        if "tp1_sell_frac"      in params: TP1_SELL_FRAC      = float(params["tp1_sell_frac"])
        if "tp2_pct"            in params: TP2_PCT            = float(params["tp2_pct"])
        if "tp2_sell_frac"      in params: TP2_SELL_FRAC      = float(params["tp2_sell_frac"])
        if "trailing_distance"  in params: TRAILING_DISTANCE  = float(params["trailing_distance"])
        if "min_entry_mcap_sol" in params: MIN_ENTRY_MCAP_SOL = float(params["min_entry_mcap_sol"])
        if "max_entry_mcap_sol" in params: MAX_ENTRY_MCAP_SOL = float(params["max_entry_mcap_sol"])
        if "entry_filter"       in params: ML_ENTRY_FILTER_ENABLED = bool(params["entry_filter"])

        score  = p.get("score", 0)
        ntrade = p.get("trades_used", 0)
        log.info("✅ ML-параметры загружены из %s  score=%.4f  trades=%d",
                 ML_PARAMS_FILE, score, ntrade)
        log.info("   quick_stop=%ds  momentum_gate=%ds  sl=%.0f%%  tp1=%.0f%%  tp2=%.0f%%  "
                 "mcap=[%.1f–%.1f]",
                 QUICK_STOP_SEC, MOMENTUM_GATE_SEC, SL_PCT*100,
                 TP1_PCT*100, TP2_PCT*100,
                 MIN_ENTRY_MCAP_SOL, MAX_ENTRY_MCAP_SOL)
    except Exception as e:
        log.warning("⚠️  Не удалось загрузить ML-параметры: %s", e)


# Фильтр по mcap (обновляется из ml_params.json)
MIN_ENTRY_MCAP_SOL      = float(os.getenv("MIN_ENTRY_MCAP_SOL", "0.0"))
MAX_ENTRY_MCAP_SOL      = float(os.getenv("MAX_ENTRY_MCAP_SOL", "9999.0"))
ML_ENTRY_FILTER_ENABLED = False

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
    init_sol:    float    # начальная ликвидность SOL в bonding curve (vSolInBondingCurve)
    dev_buy_sol: float = 0.0   # сколько SOL вложил создатель при запуске (solAmount в create tx)
    is_mayhem:   bool  = False  # Mayhem mode: AI-агент торгует 24h, 2B supply


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
    exit_reason:   str   = ""    # причина закрытия для лога

    # Капитализация при входе (SOL = price × 1B)
    entry_mcap_sol: float = 0.0


@dataclass
class MintHistory:
    """История торговли по конкретному минту (для re-entry логики)."""
    last_exit_time:  float = 0.0
    last_exit_price: float = 0.0     # расчётная цена при последнем выходе
    total_reentries: int   = 0
    total_pnl:       float = 0.0
    first_seen:      float = 0.0


@dataclass
class TradeRecord:
    """Запись о закрытой сделке — сохраняется в CSV/JSON для анализа."""
    # Идентификация
    mint:         str
    symbol:       str
    gmgn_url:     str

    # Время
    opened_at:    str    # ISO UTC
    closed_at:    str    # ISO UTC
    hold_sec:     float

    # Деньги
    entry_sol:      float
    exit_sol:       float
    pnl_sol:        float
    pnl_pct:        float

    # Капитализация (SOL = price × 1B tokens)
    entry_mcap_sol: float
    exit_mcap_sol:  float

    # Контекст
    exit_reason:  str
    is_reentry:   bool
    reentry_num:  int    # 0 = первичный вход

    # Фазы (прошёл ли T+8s и T+30s check)
    passed_quick_stop: bool
    passed_momentum:   bool


class BotState:
    def __init__(self):
        self.bank:        float = VIRTUAL_BALANCE_SOL if DRY_RUN else 0.0
        self.positions:   dict[str, Position]    = {}
        self.mint_history: dict[str, MintHistory] = defaultdict(MintHistory)

        self.session_trades:  int   = 0
        self.session_wins:    int   = 0
        self.session_pnl:     float = 0.0
        self.daily_pnl:       float = 0.0
        self.daily_stopped:   bool  = False
        self.signals_received: int  = 0
        self.signals_entered:  int  = 0
        self.signals_skipped:  int  = 0   # пропущено (max_positions)
        self.reentries:        int  = 0

        # Статистика фильтров (из token_signals анализа)
        self.filtered_mayhem:    int = 0  # пропущено Mayhem mode токенов
        self.filtered_dev_buy:   int = 0  # пропущено из-за dev buy 0.5+ SOL
        self.filtered_dead_zone: int = 0  # выходы из-за buyer dead zone на MOMENTUM_GATE

        # Счётчик buy-событий по минту (для dead zone detection)
        # ключ: mint, значение: кол-во buy-ивентов с WebSocket с момента открытия
        self.mint_buy_count: dict[str, int] = defaultdict(int)

        # История сделок для экспорта
        self.trade_log:   list[TradeRecord] = []

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
        entry_mcap_sol=round(entry_price * PUMP_TOTAL_SUPPLY, 4),
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
    """Записать закрытую сделку в статистику и trade_log."""
    now = time.time()
    total_sol_out = pos.realized_sol
    net_pnl  = total_sol_out - pos.entry_sol
    hold_sec = now - pos.opened_at
    ret_pct  = net_pnl / pos.entry_sol * 100 if pos.entry_sol > 0 else 0

    state.session_trades += 1
    state.session_pnl    += net_pnl
    state.daily_pnl      += net_pnl
    if net_pnl > 0:
        state.session_wins += 1

    exit_mcap_sol = round(final_price * PUMP_TOTAL_SUPPLY, 4) if final_price else 0.0

    icon = "✅" if net_pnl > 0 else "❌"
    log.info("%s %-10s  PnL %+.4f SOL (%+.1f%%)  hold %.1f мин  WR %.0f%%\n"
             "         mcap: вход %.1f SOL → выход %.1f SOL\n"
             "         %s",
             icon, pos.symbol, net_pnl, ret_pct, hold_sec / 60, state.win_rate,
             pos.entry_mcap_sol, exit_mcap_sol, gmgn(pos.mint))

    # Запись в trade_log
    state.trade_log.append(TradeRecord(
        mint=pos.mint,
        symbol=pos.symbol,
        gmgn_url=gmgn(pos.mint),
        opened_at=datetime.fromtimestamp(pos.opened_at, tz=timezone.utc).isoformat(),
        closed_at=datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        hold_sec=round(hold_sec, 1),
        entry_sol=round(pos.entry_sol, 6),
        exit_sol=round(total_sol_out, 6),
        pnl_sol=round(net_pnl, 6),
        pnl_pct=round(ret_pct, 2),
        entry_mcap_sol=pos.entry_mcap_sol,
        exit_mcap_sol=exit_mcap_sol,
        exit_reason=pos.exit_reason,
        is_reentry=pos.is_reentry,
        reentry_num=pos.reentry_count,
        passed_quick_stop=pos.quick_stop_done,
        passed_momentum=pos.momentum_done,
    ))
    # Сразу дозаписываем в накопительный мастер-лог (append, не перезапись)
    _append_master(state.trade_log[-1])

    # Обновить историю минта для re-entry логики
    h = state.mint_history[pos.mint]
    h.last_exit_time  = now
    h.last_exit_price = final_price
    h.total_pnl      += net_pnl
    h.total_reentries = pos.reentry_count


# ═══════════════════════════════════════════════════════════════════════════════
# УПРАВЛЕНИЕ ПОЗИЦИЯМИ (главный цикл)
# ═══════════════════════════════════════════════════════════════════════════════

async def manage_positions(session: aiohttp.ClientSession):
    """
    Вызывается каждые 2 секунды.
    Реализует реальную стратегию nya666: два checkpoint-а → держим победителей.
    """
    to_remove: list[str] = []

    for mint, pos in list(state.positions.items()):
        price = await get_bonding_curve_price(mint, session)
        now = time.time()
        age_sec = now - pos.opened_at

        # Считаем pnl_pct если есть цена
        pnl_pct: Optional[float] = None
        if price and pos.entry_price > 0:
            pnl_pct = (price - pos.entry_price) / pos.entry_price
            if price > pos.peak_price:
                pos.peak_price = price

        # ── HARD TIME STOP — работает всегда ────────────────────────────────
        if age_sec >= HARD_TIME_STOP_MIN * 60:
            close_price = price if price else pos.entry_price
            reason = f"TIME_{HARD_TIME_STOP_MIN}min" + (" [no_price]" if not price else "")
            await close_position(pos, 1.0, reason, close_price, session)
            pos.exit_reason = reason
            to_remove.append(mint)
            _record_closed(pos, close_price)
            continue

        # ── T+8s QUICK_STOP: цена не выросла = токен мёртвый ───────────────
        # Данные: 6,520 выходов на 5-10s (крупнейший кластер потерь)
        if not pos.quick_stop_done and age_sec >= QUICK_STOP_SEC:
            is_dead = price is None or pnl_pct is None or pnl_pct <= 0.0
            if is_dead:
                close_price = price if price else pos.entry_price
                reason = "QUICK_STOP_8s"
                await close_position(pos, 1.0, reason, close_price, session)
                pos.exit_reason = reason
                to_remove.append(mint)
                _record_closed(pos, close_price)
                continue
            else:
                pos.quick_stop_done = True   # прошли T+8s с позитивным momentum

        # ── T+25s MOMENTUM_GATE: рост < 5% = нет momentum ──────────────────
        # Данные: второй кластер выходов ~4,000 на 25-45s
        # + buyer dead zone: 3-50 покупателей в первые 30s → WR=1-20% (ловушка!)
        if pos.quick_stop_done and not pos.momentum_done and age_sec >= MOMENTUM_GATE_SEC:
            insufficient = price is None or pnl_pct is None or pnl_pct < MOMENTUM_MIN_PCT

            # Dead zone check: счётчик buy-событий по WebSocket
            # <3 buyers: WR=31% ✅  |  3-50: WR=1-20% ❌  |  >50: WR=39% ✅
            buy_count = state.mint_buy_count.get(mint, 0)
            in_dead_zone = BUYER_DEAD_ZONE_MIN <= buy_count <= BUYER_DEAD_ZONE_MAX

            if insufficient or in_dead_zone:
                close_price = price if price else pos.entry_price
                reason_parts = []
                if insufficient:
                    reason_parts.append(f"<{MOMENTUM_MIN_PCT:.0%}")
                if in_dead_zone:
                    reason_parts.append(f"dead_zone_{buy_count}buyers")
                    state.filtered_dead_zone += 1
                reason = f"MOMENTUM_GATE_{MOMENTUM_GATE_SEC}s [{'+'.join(reason_parts)}]"
                await close_position(pos, 1.0, reason, close_price, session)
                pos.exit_reason = reason
                to_remove.append(mint)
                _record_closed(pos, close_price)
                continue
            else:
                pos.momentum_done = True    # прошли оба gate → держим!

        # Без цены дальше ничего не делаем (ждём данных)
        if price is None or pnl_pct is None:
            continue

        # ── АКТИВНАЯ ФАЗА: SL → Trailing → TP ──────────────────────────────

        # Hard SL -15% (данные: avg loss = -17.8%)
        if pnl_pct <= -SL_PCT:
            reason = f"SL_{SL_PCT:.0%} {pnl_pct:+.1%}"
            await close_position(pos, 1.0, reason, price, session)
            pos.exit_reason = reason
            to_remove.append(mint)
            _record_closed(pos, price)
            continue

        # Trailing stop: активируется после +50% → тянется в -15% от пика
        if pnl_pct >= TRAILING_TRIGGER_PCT:
            new_trail = pos.peak_price * (1 - TRAILING_DISTANCE)
            if pos.trailing_sl is None or new_trail > pos.trailing_sl:
                if pos.trailing_sl is None:
                    log.info("⚡ Trailing SL: %-10s @%.2e (peak +%.0f%%)",
                             pos.symbol, new_trail, pnl_pct * 100)
                pos.trailing_sl = new_trail

        if pos.trailing_sl and price <= pos.trailing_sl:
            reason = f"TRAILING_SL {pnl_pct:+.1%}"
            await close_position(pos, 1.0, reason, price, session)
            pos.exit_reason = reason
            to_remove.append(mint)
            _record_closed(pos, price)
            continue

        # TP1 +50% → продаём 35%
        if not pos.tp1_done and pnl_pct >= TP1_PCT:
            await close_position(pos, TP1_SELL_FRAC, f"TP1 +{TP1_PCT:.0%}", price, session)
            pos.tp1_done = True

        # TP2 +100% → продаём 50% оставшегося (moonbag ~15%)
        if not pos.tp2_done and pos.tp1_done and pnl_pct >= TP2_PCT:
            await close_position(pos, TP2_SELL_FRAC, f"TP2 +{TP2_PCT:.0%}", price, session)
            pos.tp2_done = True

        # TP3 +500% → закрываем moonbag
        if not pos.tp3_done and pos.tp2_done and pnl_pct >= TP3_PCT:
            await close_position(pos, TP3_SELL_FRAC, f"TP3 +{TP3_PCT:.0%}", price, session)
            pos.tp3_done = True

        if pos.token_balance <= 0:
            pos.exit_reason = "ALL_TP"
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
# НЕМЕДЛЕННЫЙ ВХОД (реальная стратегия nya666)
# ═══════════════════════════════════════════════════════════════════════════════

async def _buy_token(event: TokenEvent, session: aiohttp.ClientSession):
    """
    Немедленная покупка токена при получении события создания.
    Применяем фильтры на основе анализа 25,987 реальных сделок:
      - Dev buy 0.5+ SOL: WR=16.3% → пропускаем
      - Co-buy (оба кошелька): WR=63.7% → берём с увеличенным размером (TODO)
    Checkpoints (T+8s, T+25s + dead zone) режут плохие позиции после входа.
    """
    mint = event.mint

    if mint in state.positions:
        return
    if mint in state.mint_history and state.mint_history[mint].last_exit_time > 0:
        return  # уже торговали этим токеном

    age = time.time() - event.created_at
    if age > MAX_TOKEN_AGE_SEC:
        return

    # ── Фильтр Mayhem mode ───────────────────────────────────────────────────
    # Mayhem: AI-агент (Agent Pumpy) делает случайный buy/sell 24h.
    # Паттерны стратегии не работают, после 24h AI сжигает токены → дамп.
    if SKIP_MAYHEM_MODE and event.is_mayhem:
        state.signals_skipped += 1
        state.filtered_mayhem = getattr(state, "filtered_mayhem", 0) + 1
        log.debug("⛔ %s: Mayhem mode → пропуск (AI-агент торгует 24h)", event.symbol)
        return

    # ── Фильтр по dev buy (из анализа token_signals) ────────────────────────
    # Dev buy >= DEV_BUY_SKIP_MIN_SOL: WR=16.3%, avg_pnl=-0.009 → пропускаем
    # Dev buy = нет / <0.5 SOL: WR=32.2% → нормально
    dev_sol = event.dev_buy_sol or 0.0
    if dev_sol >= DEV_BUY_SKIP_MIN_SOL:
        state.signals_skipped += 1
        state.filtered_dev_buy += 1
        log.debug("⛔ %s: dev_buy=%.2f SOL (≥%.1f) → пропуск [WR=16%%]",
                  event.symbol, dev_sol, DEV_BUY_SKIP_MIN_SOL)
        return

    if not state.can_open():
        state.signals_skipped += 1
        return

    pos = await open_position(event, BUY_SIZE_SOL, is_reentry=False,
                              reentry_count=0, session=session)
    if pos:
        state.positions[mint] = pos
        state.signals_entered += 1
        if mint not in state.mint_history:
            state.mint_history[mint] = MintHistory()
        state.mint_history[mint].first_seen = event.created_at


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

                            # solAmount в create-tx = сколько SOL дев вложил при запуске
                            dev_sol = msg.get("solAmount") or 0
                            if dev_sol > 1e9:
                                dev_sol = dev_sol / 1e9

                            # ── Mayhem mode detection ─────────────────────────
                            # Метод 1: явный флаг от pumpportal (если добавят)
                            is_mayhem = bool(
                                msg.get("mayhem")
                                or msg.get("isMayhem")
                                or msg.get("is_mayhem")
                            )
                            # Метод 2: vTokensInBondingCurve > 1.5B (Mayhem = 2B supply)
                            vtokens = msg.get("vTokensInBondingCurve") or msg.get("tokenAmount") or 0
                            if vtokens and float(vtokens) >= MAYHEM_VTOKENS_THRESH:
                                is_mayhem = True
                            # Метод 3: создатель = AI-агент кошелёк
                            if trader == MAYHEM_AI_AGENT_WALLET:
                                is_mayhem = True

                            if is_mayhem:
                                log.debug("🤖 Mayhem token: %s (%s)  vtokens=%.0f",
                                          msg.get("symbol", "?"), mint[:8], float(vtokens or 0))

                            event = TokenEvent(
                                mint=mint,
                                name=msg.get("name", "?"),
                                symbol=msg.get("symbol", "?")[:10],
                                creator=trader,
                                created_at=created_at,
                                init_sol=float(vsol),
                                dev_buy_sol=float(dev_sol),
                                is_mayhem=is_mayhem,
                            )
                            # Немедленная покупка — nya666 не ждёт, скорость = всё
                            asyncio.ensure_future(_buy_token(event, session))

                        else:
                            # Торговое событие — обновляем цену + считаем покупателей
                            sol_amount   = msg.get("solAmount") or msg.get("vSolInBondingCurve") or 0
                            token_amount = msg.get("tokenAmount") or msg.get("vTokensInBondingCurve") or 0
                            is_buy       = msg.get("txType") == "buy"
                            trader       = msg.get("traderPublicKey", "")

                            if sol_amount and token_amount:
                                update_price_from_trade(mint, sol_amount, token_amount, is_buy)

                            # Считаем buy-события для dead zone detection на MOMENTUM_GATE
                            # Данные: 3-50 buyers в первые 60s → WR=1-20% (зона смерти)
                            if is_buy and mint in state.positions:
                                state.mint_buy_count[mint] += 1

        except websockets.ConnectionClosed:
            log.warning("WS закрыт, переподключение через 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error("WS error: %s", e)
            await asyncio.sleep(5)


async def subscribe_to_active_tokens(session: aiohttp.ClientSession):
    """
    Подписаться на торговые события активных позиций для обновления цен.
    Переподключается каждые 20s чтобы подхватывать новые позиции.
    БАГ-ФИКС: убран sleep(15) в начале — цены должны идти с первой секунды.
    """
    while True:
        if not state.positions:
            await asyncio.sleep(1)
            continue

        mints = list(state.positions.keys())
        try:
            async with websockets.connect(
                PUMPPORTAL_WSS,
                ping_interval=20, ssl=_ssl_ctx,
            ) as ws:
                await ws.send(json.dumps({
                    "method": "subscribeTokenTrade",
                    "keys": mints,
                }))
                log.debug("Подписка на трейды для %d минтов", len(mints))

                deadline = time.time() + 20   # переподключаться каждые 20s (подхватываем новые позиции быстро)
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
            await asyncio.sleep(1)


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
        log.info("  Сигналов: %d  |  Входов: %d  |  Пропущено: %d  "
                 "(mayhem: %d  dev_buy: %d  dead_zone: %d)",
                 state.signals_received, state.signals_entered, state.signals_skipped,
                 state.filtered_mayhem, state.filtered_dev_buy, state.filtered_dead_zone)
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


def save_trade_log():
    """
    Сохранить историю сделок в CSV + JSON-summary.
    CSV — строка на сделку, удобно скормить в анализатор.
    JSON — агрегаты по exit_reason, hold, WR.
    """
    trades = state.trade_log
    if not trades:
        return

    # ── CSV ───────────────────────────────────────────────────────────────────
    fields = [
        "opened_at", "closed_at", "hold_sec",
        "symbol", "mint", "gmgn_url",
        "entry_sol", "exit_sol", "pnl_sol", "pnl_pct",
        "entry_mcap_sol", "exit_mcap_sol",
        "exit_reason", "is_reentry", "reentry_num",
        "passed_quick_stop", "passed_momentum",
    ]
    with open(TRADES_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for t in trades:
            w.writerow({
                "opened_at":         t.opened_at,
                "closed_at":         t.closed_at,
                "hold_sec":          t.hold_sec,
                "symbol":            t.symbol,
                "mint":              t.mint,
                "gmgn_url":          t.gmgn_url,
                "entry_sol":         t.entry_sol,
                "exit_sol":          t.exit_sol,
                "pnl_sol":           t.pnl_sol,
                "pnl_pct":           t.pnl_pct,
                "entry_mcap_sol":    t.entry_mcap_sol,
                "exit_mcap_sol":     t.exit_mcap_sol,
                "exit_reason":       t.exit_reason,
                "is_reentry":        t.is_reentry,
                "reentry_num":       t.reentry_num,
                "passed_quick_stop": t.passed_quick_stop,
                "passed_momentum":   t.passed_momentum,
            })

    # ── JSON summary ──────────────────────────────────────────────────────────
    from collections import defaultdict as _dd
    import dataclasses

    wins  = [t for t in trades if t.pnl_sol > 0]
    loses = [t for t in trades if t.pnl_sol <= 0]

    by_reason: dict = _dd(lambda: {"count": 0, "pnl": 0.0, "wins": 0})
    by_phase:  dict = {
        "quick_stop_phase":  {"count": 0, "pnl": 0.0, "wins": 0},
        "momentum_phase":    {"count": 0, "pnl": 0.0, "wins": 0},
        "active_phase":      {"count": 0, "pnl": 0.0, "wins": 0},
    }
    for t in trades:
        r = t.exit_reason.split()[0] if t.exit_reason else "UNKNOWN"
        by_reason[r]["count"] += 1
        by_reason[r]["pnl"]   += t.pnl_sol
        by_reason[r]["wins"]  += int(t.pnl_sol > 0)

        if not t.passed_quick_stop:
            phase = "quick_stop_phase"
        elif not t.passed_momentum:
            phase = "momentum_phase"
        else:
            phase = "active_phase"
        by_phase[phase]["count"] += 1
        by_phase[phase]["pnl"]   += t.pnl_sol
        by_phase[phase]["wins"]  += int(t.pnl_sol > 0)

    def _wr(d):
        return round(d["wins"] / d["count"] * 100, 1) if d["count"] else 0

    # mcap buckets: <5 SOL / 5-20 / 20-100 / >100
    def _mcap_bucket(sol: float) -> str:
        if sol < 5:    return "<5_SOL"
        if sol < 20:   return "5-20_SOL"
        if sol < 100:  return "20-100_SOL"
        return ">100_SOL"

    by_mcap: dict = _dd(lambda: {"count": 0, "pnl": 0.0, "wins": 0})
    for t in trades:
        b = _mcap_bucket(t.entry_mcap_sol)
        by_mcap[b]["count"] += 1
        by_mcap[b]["pnl"]   += t.pnl_sol
        by_mcap[b]["wins"]  += int(t.pnl_sol > 0)

    summary = {
        "session_start":   _SESSION_START,
        "trades_total":    len(trades),
        "win_rate_pct":    round(len(wins) / len(trades) * 100, 1) if trades else 0,
        "pnl_total_sol":   round(sum(t.pnl_sol for t in trades), 4),
        "avg_pnl_sol":     round(sum(t.pnl_sol for t in trades) / len(trades), 4) if trades else 0,
        "avg_win_sol":     round(sum(t.pnl_sol for t in wins) / len(wins), 4) if wins else 0,
        "avg_loss_sol":    round(sum(t.pnl_sol for t in loses) / len(loses), 4) if loses else 0,
        "avg_hold_sec":    round(sum(t.hold_sec for t in trades) / len(trades), 1) if trades else 0,
        "avg_entry_mcap_sol": round(sum(t.entry_mcap_sol for t in trades) / len(trades), 2) if trades else 0,
        "avg_exit_mcap_sol":  round(sum(t.exit_mcap_sol for t in trades) / len(trades), 2) if trades else 0,
        "reentries":       sum(1 for t in trades if t.is_reentry),
        "by_exit_reason":  {k: {**v, "wr_pct": _wr(v)} for k, v in sorted(by_reason.items())},
        "by_phase":        {k: {**v, "wr_pct": _wr(v)} for k, v in by_phase.items()},
        "by_entry_mcap":   {k: {**v, "wr_pct": _wr(v)}
                            for k, v in sorted(by_mcap.items())},
        "top10_wins":      [
            {"symbol": t.symbol, "pnl_sol": t.pnl_sol, "pnl_pct": t.pnl_pct,
             "hold_sec": t.hold_sec, "reason": t.exit_reason,
             "entry_mcap_sol": t.entry_mcap_sol, "exit_mcap_sol": t.exit_mcap_sol,
             "url": t.gmgn_url}
            for t in sorted(trades, key=lambda x: x.pnl_sol, reverse=True)[:10]
        ],
        "top10_losses":    [
            {"symbol": t.symbol, "pnl_sol": t.pnl_sol, "pnl_pct": t.pnl_pct,
             "hold_sec": t.hold_sec, "reason": t.exit_reason,
             "entry_mcap_sol": t.entry_mcap_sol, "exit_mcap_sol": t.exit_mcap_sol,
             "url": t.gmgn_url}
            for t in sorted(trades, key=lambda x: x.pnl_sol)[:10]
        ],
    }
    with open(TRADES_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    log.info("💾 Сохранено %d сделок → %s  |  summary → %s",
             len(trades), TRADES_CSV_PATH, TRADES_JSON_PATH)


async def autosave_loop():
    """Автосохранение CSV каждые 5 минут."""
    while True:
        await asyncio.sleep(300)
        try:
            save_trade_log()
        except Exception as e:
            log.debug("autosave error: %s", e)


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

    # Загружаем ML-параметры от ночного тренера (если есть)
    _load_ml_params()

    log.info("═" * 65)
    log.info("  PUMPSCALP v4.0  —  Pump.fun Direct Bonding Curve Sniper")
    log.info("  Стратегия: 101,190 сделок nya666 | WR=34.6%% EV=+2.7%%/trade")
    log.info("  Режим: %s  |  Банк: %.1f SOL",
             "DRY RUN 🟡" if DRY_RUN else "LIVE 🟢",
             state.bank)
    log.info("  Размер: %.2f SOL  |  Re-entry: %.2f SOL  |  Max позиций: %d",
             BUY_SIZE_SOL, BUY_SIZE_REENTRY_SOL, MAX_POSITIONS)
    log.info("  Quick stop T+%ds  |  Momentum gate T+%ds ≥+%.0f%%  |  Time stop %d мин",
             QUICK_STOP_SEC, MOMENTUM_GATE_SEC,
             MOMENTUM_MIN_PCT * 100, HARD_TIME_STOP_MIN)
    log.info("  TP1 +%.0f%%→%.0f%%  TP2 +%.0f%%→%.0f%%  TP3 +%.0f%%→%.0f%%  Trail after+%.0f%% (-%.0f%%)",
             TP1_PCT*100, TP1_SELL_FRAC*100,
             TP2_PCT*100, TP2_SELL_FRAC*100,
             TP3_PCT*100, TP3_SELL_FRAC*100,
             TRAILING_TRIGGER_PCT*100, TRAILING_DISTANCE*100)
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
            autosave_loop(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено.")
        log.info("Итого: %d сделок  |  WR %.1f%%  |  PnL %+.4f SOL",
                 state.session_trades, state.win_rate, state.session_pnl)
        save_trade_log()
