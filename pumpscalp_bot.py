"""
PUMPSCALP v5.0 — Pump.fun Direct Bonding Curve Sniper
Стратегия реконструирована из анализа 22,254 сделок кошелька:
  D5dtjfGj9PhWsqKjepxFSAnRMrADrYfGPWjVwZSoapKe (97 дней, дек 2025 – март 2026)

══════════════════════════════════════════════════════════════
  РЕАЛЬНАЯ СТРАТЕГИЯ (из анализа D5dtjf кошелька):

  СТАТИСТИКА:
    Win rate:       30.4%
    Avg win:        +30.1% от позиции (+0.146 SOL)
    Avg loss:       -6.0%  от позиции (-0.029 SOL)  ← режем быстро!
    EV/сделку:      +5.0%  (+0.024 SOL)
    Сделок/день:    ~229   (очень избирательный!)
    Дневная прибыль: ~5.55 SOL
    Best trade:     +38.09 SOL (TOTO, держал 8,031s)

  ЗОЛОТОЙ ПАТТЕРН (26.9% сделок → 100% прибыли):
    n_buyers_before_wallet = 0  (ПЕРВЫЙ покупатель!)
    n_buyers_in_60s ≥ 91        (максимальный momentum)
    BC SOL при входе: 1.0–3.0 SOL
    → WR 62.6%, avg PnL +0.164 SOL (+34.9%), 5.68 SOL/день

  ПОЧЕМУ ОСТАЛЬНЫЕ 73.1% СДЕЛОК = МИНУС:
    Любое n_buyers_before_wallet > 0: WR падает с 57.9% до ~20%
    n_buyers_in_60s < 91: WR 0.5–36%, avg PnL отрицательный
    → те 16,625 "других" сделок: -245 SOL совокупно!

  ВЫХОДЫ:
    - T+10s QUICK_STOP: если цена не выросла → выходим
    - T+60s MOMENTUM_GATE: если buyers_in_60s < 91 → выходим
      (нет высокого momentum → нет сделки)
    - Hold 15–30 мин: WR=70.6%, avg PnL +0.09 SOL  ← лучшая зона!
    - Hold >30 мин:  WR=61.8%, avg PnL +0.46 SOL   ← мегавинеры!
    - Hard stop 25 мин (не 8 мин — это убивает лучшие сделки)

  TP УРОВНИ (оптимизированы под D5dtjf данные):
    - TP1 +50%:  продаём 35% (фиксируем прибыль)
    - TP2 +150%: продаём 50% оставшегося
    - TP3 +500%: продаём весь остаток (moonbag)
    - Trailing SL: активируется после +50%, тянется в -12% от пика

  ФИЛЬТРЫ ВХОДА (базируются на 56,654 сделках D5dtjf+nya666):
    - dev_buy < 0.1 SOL — данные D5dtjf: >0.1 SOL WR=14.6%
    - BC SOL 0.3–20.0 SOL: nya666 медиана=11.31 SOL, golden BC<10 WR=53.8%
      Не используем старый MAX=2.5 (блокировал 97% nya666 golden сделок!)
    - n_transfers=1 → ПРОПУСК (nya666: WR=27.8%, golden WR=42.9% — худший!)
      ntr=4+: nya666 golden WR=55-64%, AvgPnL=0.15-0.28 SOL
    - Часы: НЕЙТРАЛЬНО (nya666 h7-9 UTC WR=43-47% = такой же как остальные)
    - Mcap в USD ≤ MAX_ENTRY_MCAP_USD (настраивается)
    - НЕТ DCA: nya666 n_buys=1→WR=64%, n_buys=2→WR=46%, 3+→WR<34%
    - НЕТ re-entry (убытки при multi-buy подтверждены обоими кошельками)

  MOMENTUM GATE (ГЛАВНЫЙ ФИЛЬТР):
    - nb60=96 = SENTINEL "96+" в системе (14,612 сделок, WR=49%, AvgPnL=+0.086)
    - nb60=91-95: РЕАЛЬНЫЕ значения WR=0%!! → мёртвые токены, выходим НЕМЕДЛЕННО
    - nb60=97-100: WR=33-37%, AvgPnL часто ОТРИЦАТЕЛЬНЫЙ → тоже хуже sentinel
    - Вывод: держим ТОЛЬКО при nb60=96 (≥96 buyers = cap value)

  КОМИССИИ (DRY RUN учитывает реальные расходы):
    - Priority fee: PRIORITY_FEE_MICROLAMPORTS / 1e6 SOL
    - Pump.fun protocol fee: 1% от объёма
    - Network base fee: ~0.000005 SOL
    - Итого на сделку 0.47 SOL: ~0.005+ SOL расходов

  МАТЕМАТИКА:
    EV = 0.626 × 34.9% + 0.374 × (−6.0%) ≈ +19.6% на золотую сделку
    ~57 золотых сделок/день × 0.47 SOL × 19.6% ≈ 5.25 SOL/день
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
# ПАРАМЕТРЫ — КАЖДЫЙ ВЫВЕДЕН ИЗ АНАЛИЗА 22,254 СДЕЛОК D5dtjf КОШЕЛЬКА
# ═══════════════════════════════════════════════════════════════════════════════
DRY_RUN              = os.getenv("DRY_RUN", "true").lower() == "true"
VIRTUAL_BALANCE_SOL  = float(os.getenv("VIRTUAL_BALANCE_SOL", "50.0"))

# ── Pump.fun: total supply ───────────────────────────────────────────────────
PUMP_TOTAL_SUPPLY      = 1_000_000_000   # 1B токенов — стандартный pump.fun
PUMP_MAYHEM_SUPPLY     = 2_000_000_000   # 2B токенов — Mayhem mode (AI-агент)

# ── Mayhem mode фильтр ───────────────────────────────────────────────────────
SKIP_MAYHEM_MODE       = os.getenv("SKIP_MAYHEM_MODE", "true").lower() == "true"
MAYHEM_VTOKENS_THRESH  = float(os.getenv("MAYHEM_VTOKENS_THRESH", "1_500_000_000"))
MAYHEM_AI_AGENT_WALLET = "BwWK17cbHxwWBKZkUYvzxLcNQ1YVyaFezduWbtm2de6s"

# ── Размер позиции (медиана D5dtjf: 0.47 SOL, диапазон 0.395–0.919) ─────────
BUY_SIZE_SOL         = float(os.getenv("BUY_SIZE_SOL", "0.47"))
MAX_POSITIONS        = int(os.getenv("MAX_POSITIONS", "5"))   # D5dtjf: ~229/день, не 1700

# ── Выходы ───────────────────────────────────────────────────────────────────
# D5dtjf: 15–30 мин → WR=70.6%, avg +0.09 SOL ← лучшая зона!
# >30 мин → WR=61.8%, avg +0.46 SOL (мегавинеры)
# 8-мин стоп УБИВАЕТ лучшие сделки → ставим 25 мин
HARD_TIME_STOP_MIN   = int(os.getenv("HARD_TIME_STOP_MIN", "25"))
# D5dtjf avg loss = -6% от позиции → SL 8%
SL_PCT               = float(os.getenv("SL_PCT", "0.08"))

# TP уровни (оптимизированы под D5dtjf профиль):
# TP1 +50%: продать 35%
TP1_PCT              = float(os.getenv("TP1_PCT", "0.50"))
TP1_SELL_FRAC        = float(os.getenv("TP1_SELL_FRAC", "0.35"))
# TP2 +150%: продать 50% остатка (D5dtjf средний победитель +30%, держим до 150%)
TP2_PCT              = float(os.getenv("TP2_PCT", "1.50"))
TP2_SELL_FRAC        = float(os.getenv("TP2_SELL_FRAC", "0.50"))
# TP3 +500%: закрыть moonbag (TOTO: +7,814%)
TP3_PCT              = float(os.getenv("TP3_PCT", "5.00"))
TP3_SELL_FRAC        = float(os.getenv("TP3_SELL_FRAC", "1.00"))

# ── Pre-entry фильтр (ждём покупателей перед входом) ──────────────────────────
# Проблема: при покупке на newToken-событии WR=1% (мы уже опоздали, SAC≥0)
# Решение: НЕ покупаем сразу — наблюдаем за токеном, входим только если
#          за PREENTRY_WAIT_SEC секунд появится ≥PREENTRY_BUYERS_MIN покупателей
# 0 = старое поведение (мгновенная покупка, не рекомендуется)
# 5 = входим только в "горячие" токены (≥5 покупателей за 15 секунд)
PREENTRY_BUYERS_MIN  = int(os.getenv("PREENTRY_BUYERS_MIN", "5"))
PREENTRY_WAIT_SEC    = int(os.getenv("PREENTRY_WAIT_SEC", "15"))

# ── Momentum checkpoints ───────────────────────────────────────────────────────
# T+10s QUICK_STOP: если цена не выросла → токен мёртвый
QUICK_STOP_SEC       = int(os.getenv("QUICK_STOP_SEC", "10"))
# T+60s MOMENTUM_GATE: POST-BUY проверка температуры токена
# ВАЖНО: это НЕ "подождать пока 96 человек купят до нас"!
# Бот покупает ПЕРВЫМ (при создании). Затем 60 секунд считаем покупателей.
# Это мера популярности токена ПОСЛЕ нашей покупки:
#   nb60=96: SENTINEL "96+" — горячий токен
#     nya666: nbw=0 + nb60=96 → WR=49%, AvgPnL=+0.086 SOL ← ДЕРЖИМ
#     nya666: nbw>0 + nb60=96 → WR=19.6%, AvgPnL=-0.034   ← убыток (мы не копируем это)
#   nb60<96: холодный токен → WR=0-26%, уходим с минимальным убытком
#   nb60=91-95: реальные маленькие значения (не sentinel!) WR=14.7% → ВЫХОД
#   nb60=97-100: WR=32%, часто AvgPnL<0 (парадоксально — "перегрев" = поздно)
# nya666 покупает с nbw=5-12 в 32% сделок → WR=10-15% → это его плохие сделки!
# Бот берёт только лучшее: первым + горячий токен (nb60=96)
MOMENTUM_GATE_SEC    = int(os.getenv("MOMENTUM_GATE_SEC", "60"))
MOMENTUM_MIN_PCT     = float(os.getenv("MOMENTUM_MIN_PCT", "0.0"))
BUYERS_60S_MIN       = int(os.getenv("BUYERS_60S_MIN", "96"))        # sentinel "96+"

# Trailing: после +50% → SL подтягивается в -12% от пика (D5dtjf тighter exits)
TRAILING_TRIGGER_PCT = float(os.getenv("TRAILING_TRIGGER_PCT", "0.50"))
TRAILING_DISTANCE    = float(os.getenv("TRAILING_DISTANCE", "0.12"))

# ── Token age ────────────────────────────────────────────────────────────────
MAX_TOKEN_AGE_SEC    = int(os.getenv("MAX_TOKEN_AGE_SEC", "120"))

# ── Re-entry — ОТКЛЮЧЁН (D5dtjf данные: DCA = 48.4% крупных убытков) ────────
# Кошелёк D5dtjf: multi-buy avg +0.007 vs single-buy +0.026 → DCA вредит
MAX_REENTRIES        = int(os.getenv("MAX_REENTRIES", "0"))   # 0 = отключено

# ── Риск ─────────────────────────────────────────────────────────────────────
DAILY_LOSS_LIMIT_SOL = float(os.getenv("DAILY_LOSS_LIMIT_SOL", "5.0"))

# ── Фильтры входа — ВАЖНО: совместимость с PumpPortal полями ────────────────
#
# ПРОБЛЕМА: поля vSolInBondingCurve и solAmount из PumpPortal НЕ равны
# полям first_buy_bc_sol и dev_buy_sol из signal-данных!
#
# vSolInBondingCurve при создании = виртуальный резерв pump.fun ≈ 30 SOL (всегда!)
#   first_buy_bc_sol в данных = реальный SOL в пуле = vSol - ~30 (offset)
#   → При создании реальный SOL = 0, фильтр по BC не применим на entry
#
# solAmount в create-событии = SOL создателя при запуске (если он купил)
#   dev_buy_sol в данных = тот же смысл, но могут быть расхождения
#   → Может работать, но нужна валидация
#
# РЕКОМЕНДАЦИЯ: BC фильтр на entry НЕ РАБОТАЕТ (vSol=30 SOL всегда > MAX)
# Выставляем MAX_ENTRY_BC_SOL=0 чтобы отключить
# Основной фильтр качества = MOMENTUM_GATE (96 buyers в 60s)
DEV_BUY_SKIP_MIN_SOL = float(os.getenv("DEV_BUY_SKIP_MIN_SOL", "0.0"))  # 0 = выключен

# BC SOL фильтр (на основе vSolInBondingCurve):
# 0.0 = отключён (рекомендуется: vSolInBondingCurve ≠ first_buy_bc_sol из данных)
# >0  = включить: актуально только если PumpPortal передаёт реальный BC SOL
MIN_ENTRY_BC_SOL     = float(os.getenv("MIN_ENTRY_BC_SOL", "0.0"))
MAX_ENTRY_BC_SOL     = float(os.getenv("MAX_ENTRY_BC_SOL", "0.0"))  # 0 = выключен

# ── Фильтр по n_transfers при создании токена ─────────────────────────────────
# ══ ПОДТВЕРЖДЕНО на nya666 (34,399 сделок) ══
# nya666 ВСЕ:    ntr=1 WR=27.8%, AvgPnL=-0.016 (ХУДШИЙ!)
#                ntr=2 WR=31.2%, ntr=3 WR=39.4%, ntr=4 WR=42.8%, ntr=11+ WR=58.7%
# nya666 GOLDEN: ntr=1 WR=42.9%, AvgPnL=+0.028 (ХУДШИЙ в golden!)
#                ntr=4 WR=55.9%, ntr=5 WR=61.1%! ntr=6-10 WR=53.4%, ntr=11+ WR=64.5%
# Паттерн: чем больше transfers при создании — тем лучше (более сложные, оригинальные токены)
# ntr=1: однотипный шаблон, вероятно массовый скам/шаблонный запуск → ПРОПУСКАЕМ
SKIP_N_TRANSFERS_1   = os.getenv("SKIP_N_TRANSFERS_1", "true").lower() == "true"

# ── Фильтр по времени суток (UTC) ────────────────────────────────────────────
# ══ ОБНОВЛЕНО по 34,399 сделкам nya666 ══
# D5dtjf (22,255 сделок): 7-9 UTC слабее (golden WR=49-51% vs 62-69%)
# nya666 (34,399 сделок): НЕЙТРАЛЬНО — h7=45.1%, h8=47.1%, h9=43.5% (такой же WR!)
# → Hour filter для nya666 НЕ РАБОТАЕТ, отключаем по умолчанию
# "" = торгуем круглосуточно (default); "7,8,9" = для D5dtjf-режима
AVOID_HOURS_UTC      = [int(h) for h in os.getenv("AVOID_HOURS_UTC", "").split(",") if h.strip()]

# ── Фильтр по Market Cap в USD ────────────────────────────────────────────────
# Позволяет торговать только токены ниже указанного mcap (в долларах).
# 0.0 = отключено. Пример: 5000.0 → входим только если mcap ≤ $5,000
# mcap_sol × SOL_PRICE_USD → mcap_usd
MAX_ENTRY_MCAP_USD   = float(os.getenv("MAX_ENTRY_MCAP_USD", "0.0"))
SOL_PRICE_USD        = float(os.getenv("SOL_PRICE_USD", "150.0"))   # обновлять вручную

# ── Комиссии для DRY_RUN симуляции ───────────────────────────────────────────
# Pump.fun снимает 1% с каждой операции (buy + sell)
# Priority fee задаётся отдельно
# Network base fee: ~0.000005 SOL
PUMPFUN_FEE_PCT      = float(os.getenv("PUMPFUN_FEE_PCT", "0.01"))      # 1% protocol fee
NETWORK_FEE_SOL      = float(os.getenv("NETWORK_FEE_SOL", "0.000005"))  # ~5000 lamports

# ── Исполнение ────────────────────────────────────────────────────────────────
PRIORITY_FEE_MICROLAMPORTS = int(os.getenv("PRIORITY_FEE_MICROLAMPORTS", "1000"))  # делится на 1e6 → SOL; 1000=0.001 SOL
SLIPPAGE_BPS         = int(os.getenv("SLIPPAGE_BPS", "1000"))

# ── API ───────────────────────────────────────────────────────────────────────
PUMPPORTAL_WSS    = "wss://pumpportal.fun/api/data"
PUMPPORTAL_TRADE  = "https://pumpportal.fun/api/trade-local"
WSOL_MINT         = "So11111111111111111111111111111111111111112"
PUMPFUN_PROGRAM   = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


def gmgn(mint: str) -> str:
    """Ссылка на токен в GMGN для быстрого анализа."""
    return f"https://gmgn.ai/sol/token/{mint}"


# ═══════════════════════════════════════════════════════════════════════════════
# КОМИССИИ — реалистичная симуляция для DRY_RUN
# ═══════════════════════════════════════════════════════════════════════════════

def _tx_fee_sol() -> float:
    """Priority fee + base network fee за одну транзакцию."""
    return PRIORITY_FEE_MICROLAMPORTS / 1_000_000 + NETWORK_FEE_SOL


def _apply_buy_fees_dry(sol_amount: float) -> tuple[float, float]:
    """
    Симуляция расходов на покупку.
    Возвращает (effective_sol_for_tokens, total_fee_sol).
    - Protocol fee 1% списывается из суммы покупки
    - Tx fee списывается сверху (с баланса)
    """
    protocol_fee = sol_amount * PUMPFUN_FEE_PCT
    tx_fee       = _tx_fee_sol()
    effective    = sol_amount - protocol_fee
    return effective, protocol_fee + tx_fee


def _apply_sell_fees_dry(gross_sol: float) -> tuple[float, float]:
    """
    Симуляция расходов на продажу.
    Возвращает (net_sol_received, total_fee_sol).
    - Protocol fee 1% списывается из полученных SOL
    - Tx fee списывается из полученных SOL
    """
    protocol_fee = gross_sol * PUMPFUN_FEE_PCT
    tx_fee       = _tx_fee_sol()
    net_sol      = gross_sol - protocol_fee - tx_fee
    return max(net_sol, 0.0), protocol_fee + tx_fee


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
    global BUYERS_60S_MIN, MIN_ENTRY_BC_SOL, MAX_ENTRY_BC_SOL
    global MAX_ENTRY_MCAP_USD, SOL_PRICE_USD

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
        if "buyers_60s_min"     in params: BUYERS_60S_MIN     = int(params["buyers_60s_min"])
        if "min_entry_bc_sol"   in params: MIN_ENTRY_BC_SOL   = float(params["min_entry_bc_sol"])
        if "max_entry_bc_sol"   in params: MAX_ENTRY_BC_SOL   = float(params["max_entry_bc_sol"])
        if "max_entry_mcap_usd" in params: MAX_ENTRY_MCAP_USD = float(params["max_entry_mcap_usd"])
        if "sol_price_usd"      in params: SOL_PRICE_USD      = float(params["sol_price_usd"])

        score  = p.get("score", 0)
        ntrade = p.get("trades_used", 0)
        log.info("✅ ML-параметры загружены из %s  score=%.4f  trades=%d",
                 ML_PARAMS_FILE, score, ntrade)
        log.info("   quick_stop=%ds  momentum_gate=%ds  buyers_min=%d  sl=%.0f%%  "
                 "bc=[%.1f–%.1f] SOL  mcap_max=$%.0f",
                 QUICK_STOP_SEC, MOMENTUM_GATE_SEC, BUYERS_60S_MIN, SL_PCT*100,
                 MIN_ENTRY_BC_SOL, MAX_ENTRY_BC_SOL, MAX_ENTRY_MCAP_USD)
    except Exception as e:
        log.warning("⚠️  Не удалось загрузить ML-параметры: %s", e)

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
    init_sol:    float    # BC SOL при создании (vSolInBondingCurve)
    dev_buy_sol: float = 0.0   # SOL вложенный девом при запуске
    is_mayhem:   bool  = False
    n_transfers: int   = -1   # кол-во transfer-ов при создании (-1 = неизвестно)


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
        self.session_fees:    float = 0.0  # накопленные комиссии (DRY_RUN)
        self.daily_pnl:       float = 0.0
        self.daily_stopped:   bool  = False
        self.signals_received: int  = 0
        self.signals_entered:  int  = 0
        self.signals_skipped:  int  = 0

        # Статистика фильтров
        self.filtered_mayhem:      int = 0  # пропущено Mayhem mode
        self.filtered_dev_buy:     int = 0  # пропущено из-за dev buy ≥0.1 SOL
        self.filtered_bc_sol:      int = 0  # пропущено из-за BC SOL вне диапазона
        self.filtered_mcap_usd:    int = 0  # пропущено из-за mcap > лимита USD
        self.filtered_hours:       int = 0  # пропущено из-за нерабочего часа UTC
        self.filtered_n_transfers: int = 0  # пропущено из-за n_transfers=1
        self.filtered_momentum:    int = 0  # выходы на MOMENTUM_GATE (<91 buyers)

        # Счётчик buy-событий по минту (для MOMENTUM_GATE)
        self.mint_buy_count: dict[str, int] = defaultdict(int)
        # Время открытия позиции для каждого минта (чтобы считать только в окне)
        self.mint_open_time: dict[str, float] = {}

        # Pre-entry: токены на наблюдении перед входом
        # mint → {'event': TokenEvent, 'buyers': int, 'started': float}
        self.watching_tokens: dict[str, dict] = {}
        self.signals_expired: int = 0   # токены, не набравшие покупателей за PREENTRY_WAIT_SEC

        # История сделок для экспорта
        self.trade_log: list[TradeRecord] = []

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
        # Симуляция комиссий: protocol fee 1% + priority + network
        effective_sol, fee_sol = _apply_buy_fees_dry(buy_sol)
        tokens_received = int(effective_sol / cached_price) if cached_price > 0 else int(effective_sol * 1e6)
        entry_price = buy_sol / max(tokens_received, 1)
        state.bank -= (buy_sol + _tx_fee_sol())   # списываем сумму + tx fee сверху
        state.session_fees += fee_sol
        log.info("🟡 [DRY] %s  %-10s  %.3f SOL  fee=%.5f SOL  %d tokens  @%.2e\n         %s",
                 entry_label, event.symbol, buy_sol, fee_sol, tokens_received, entry_price,
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

    gross_sol = to_sell * current_price
    pnl_pct = (current_price - pos.entry_price) / pos.entry_price * 100 if pos.entry_price > 0 else 0

    if DRY_RUN:
        # Симуляция комиссий при продаже: protocol fee 1% + tx fee
        sol_value, fee_sol = _apply_sell_fees_dry(gross_sol)
        pnl = sol_value - pos.entry_sol * fraction
        state.session_fees += fee_sol
        icon = "🟢" if pnl >= 0 else "🔴"
        log.info("%s [DRY] SELL %-10s  %d%%  gross=%.4f  net=%+.4f SOL (%+.1f%%)  fee=%.5f  [%s]\n         %s",
                 icon, pos.symbol, int(fraction * 100), gross_sol, pnl, pnl_pct, fee_sol, reason,
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

        sol_value = int(data.get("outAmount") or data.get("solReceived") or gross_sol * 1e9) / 1e9
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
        # ── T+10s QUICK_STOP: цена не выросла = токен мёртвый ───────────────
        if not pos.quick_stop_done and age_sec >= QUICK_STOP_SEC:
            is_dead = price is None or pnl_pct is None or pnl_pct <= 0.0
            if is_dead:
                close_price = price if price else pos.entry_price
                reason = f"QUICK_STOP_{QUICK_STOP_SEC}s"
                await close_position(pos, 1.0, reason, close_price, session)
                pos.exit_reason = reason
                to_remove.append(mint)
                _record_closed(pos, close_price)
                continue
            else:
                pos.quick_stop_done = True

        # ── T+60s MOMENTUM_GATE (D5dtjf стратегия) ──────────────────────────
        # КЛЮЧЕВОЙ ФИЛЬТР: нужно ≥91 покупателей в 60-секундном окне.
        # D5dtjf данные: nb60≥91 → WR=41%, avg +0.065 SOL. nb60<91 → WR<37%, убыток.
        # Считаем buy-события через WebSocket с момента открытия позиции.
        if pos.quick_stop_done and not pos.momentum_done and age_sec >= MOMENTUM_GATE_SEC:
            buy_count = state.mint_buy_count.get(mint, 0)
            has_momentum = buy_count >= BUYERS_60S_MIN

            # Дополнительная проверка на рост цены (если MOMENTUM_MIN_PCT > 0)
            price_ok = (MOMENTUM_MIN_PCT == 0.0 or
                        (price is not None and pnl_pct is not None and
                         pnl_pct >= MOMENTUM_MIN_PCT))

            if not has_momentum or not price_ok:
                close_price = price if price else pos.entry_price
                reason_parts = []
                if not has_momentum:
                    reason_parts.append(f"buyers_{buy_count}<{BUYERS_60S_MIN}")
                    state.filtered_momentum += 1
                if not price_ok:
                    reason_parts.append(f"price<{MOMENTUM_MIN_PCT:.0%}")
                reason = f"MOMENTUM_GATE_{MOMENTUM_GATE_SEC}s [{'+'.join(reason_parts)}]"
                await close_position(pos, 1.0, reason, close_price, session)
                pos.exit_reason = reason
                to_remove.append(mint)
                _record_closed(pos, close_price)
                continue
            else:
                log.info("✅ MOMENTUM OK %-10s  buyers=%d≥%d  pnl=%+.1f%%  → держим!",
                         pos.symbol, buy_count, BUYERS_60S_MIN,
                         pnl_pct * 100 if pnl_pct else 0)
                pos.momentum_done = True

        # Без цены дальше ничего не делаем (ждём данных)
        if price is None or pnl_pct is None:
            continue

        # ── АКТИВНАЯ ФАЗА: SL → Trailing → TP ──────────────────────────────

        # Hard SL (D5dtjf avg loss -6%, ставим SL 8%)
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

        reentry_size = BUY_SIZE_SOL
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

    # ── Фильтр по времени суток UTC ──────────────────────────────────────────
    # Golden trades 7-9 UTC: WR=49-51% vs 62-69% в хорошие часы → пропускаем
    if AVOID_HOURS_UTC:
        current_hour = datetime.now(timezone.utc).hour
        if current_hour in AVOID_HOURS_UTC:
            state.signals_skipped += 1
            state.filtered_hours += 1
            return

    # ── Фильтр Mayhem mode ───────────────────────────────────────────────────
    if SKIP_MAYHEM_MODE and event.is_mayhem:
        state.signals_skipped += 1
        state.filtered_mayhem += 1
        log.debug("⛔ %s: Mayhem mode → пропуск", event.symbol)
        return

    # ── Фильтр по n_transfers при создании ───────────────────────────────────
    # n_transfers=1: WR=27.5% (худший бакет!), golden WR=56.9% vs 63%+ у других
    # n_transfers=0,2,6-10+: нормальные результаты
    if SKIP_N_TRANSFERS_1 and event.n_transfers == 1:
        state.signals_skipped += 1
        state.filtered_n_transfers += 1
        log.debug("⛔ %s: n_transfers=1 → пропуск [WR=27.5%%]", event.symbol)
        return

    # ── Диагностический лог (debug-уровень) для калибровки фильтров ─────────
    log.debug("🔍 %s: vSol=%.2f dev_buy=%.3f n_tr=%d",
              event.symbol, event.init_sol, event.dev_buy_sol, event.n_transfers)

    # ── Фильтр по dev buy (solAmount из PumpPortal create-события) ───────────
    # 0.0 = фильтр отключён (по умолчанию: solAmount ≠ dev_buy_sol из данных)
    # Включить: DEV_BUY_SKIP_MIN_SOL=0.5 в env если убедились что поле корректно
    dev_sol = event.dev_buy_sol or 0.0
    if DEV_BUY_SKIP_MIN_SOL > 0 and dev_sol >= DEV_BUY_SKIP_MIN_SOL:
        state.signals_skipped += 1
        state.filtered_dev_buy += 1
        log.debug("⛔ %s: dev_buy=%.3f SOL (≥%.2f) → пропуск",
                  event.symbol, dev_sol, DEV_BUY_SKIP_MIN_SOL)
        return

    # ── Фильтр по BC SOL (vSolInBondingCurve) ───────────────────────────────
    # MAX=0 = отключён (по умолчанию: vSolInBondingCurve ≠ first_buy_bc_sol)
    # При создании pump.fun vSolInBondingCurve ≈ 30 SOL (виртуальный резерв)
    # Включить: MAX_ENTRY_BC_SOL=35 в env чтобы отсечь аномальные токены (>35 SOL)
    if MAX_ENTRY_BC_SOL > 0:
        bc_sol = event.init_sol or 0.0
        if not (MIN_ENTRY_BC_SOL <= bc_sol <= MAX_ENTRY_BC_SOL):
            state.signals_skipped += 1
            state.filtered_bc_sol += 1
            log.debug("⛔ %s: BC=%.2f SOL вне [%.1f–%.1f] → пропуск",
                      event.symbol, bc_sol, MIN_ENTRY_BC_SOL, MAX_ENTRY_BC_SOL)
            return

    # ── Фильтр по Market Cap в USD ────────────────────────────────────────────
    # MAX_ENTRY_MCAP_USD = 0 → отключено
    # mcap_sol = BC_sol * (PUMP_TOTAL_SUPPLY / vTokensInBondingCurve), упрощённо:
    # mcap_usd ≈ init_sol * SOL_PRICE_USD * коэффициент (~37x для pump.fun)
    # Упрощение: используем init_sol как прокси (настраивай SOL_PRICE_USD точно)
    if MAX_ENTRY_MCAP_USD > 0:
        # Приближение: pump.fun initial mcap ≈ vSol × (1B / 793M) × SOL_PRICE_USD
        mcap_usd = event.init_sol * 1.26 * SOL_PRICE_USD if event.init_sol > 0 else 0
        if mcap_usd > MAX_ENTRY_MCAP_USD:
            state.signals_skipped += 1
            state.filtered_mcap_usd += 1
            log.debug("⛔ %s: mcap=$%.0f > $%.0f → пропуск",
                      event.symbol, mcap_usd, MAX_ENTRY_MCAP_USD)
            return

    if not state.can_open():
        state.signals_skipped += 1
        return

    pos = await open_position(event, BUY_SIZE_SOL, is_reentry=False,
                              reentry_count=0, session=session)
    if pos:
        state.positions[mint] = pos
        state.mint_open_time[mint] = pos.opened_at   # для 60s окна MOMENTUM_GATE
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

                            # n_transfers: если PumpPortal передаёт это поле
                            # n_transfers=1 → WR=27.5% (худший), фильтруем
                            n_transfers_raw = msg.get("tokenTransfers") or msg.get("n_transfers")
                            if n_transfers_raw is None:
                                n_transfers_raw = msg.get("transfersAtCreate")
                            n_transfers = int(n_transfers_raw) if n_transfers_raw is not None else -1

                            event = TokenEvent(
                                mint=mint,
                                name=msg.get("name", "?"),
                                symbol=msg.get("symbol", "?")[:10],
                                creator=trader,
                                created_at=created_at,
                                init_sol=float(vsol),
                                dev_buy_sol=float(dev_sol),
                                is_mayhem=is_mayhem,
                                n_transfers=n_transfers,
                            )

                            if PREENTRY_BUYERS_MIN > 0:
                                # Pre-entry: быстрые фильтры ДО подписки
                                # (не тратим subscribeTokenTrade на заведомо плохие токены)
                                _skip = False
                                if AVOID_HOURS_UTC:
                                    if datetime.now(timezone.utc).hour in AVOID_HOURS_UTC:
                                        _skip = True
                                if not _skip and SKIP_MAYHEM_MODE and is_mayhem:
                                    _skip = True
                                if not _skip and SKIP_N_TRANSFERS_1 and n_transfers == 1:
                                    _skip = True
                                if _skip:
                                    state.signals_skipped += 1
                                    log.debug("⛔ pre-filter %s → пропуск до watching",
                                              event.symbol)
                                else:
                                    # Подписываемся на трейды токена чтобы считать покупателей
                                    state.watching_tokens[mint] = {
                                        'event':   event,
                                        'buyers':  0,
                                        'started': time.time(),
                                    }
                                    await ws.send(json.dumps({
                                        "method": "subscribeTokenTrade",
                                        "keys":   [mint],
                                    }))
                                    log.debug("👁 watching %s (ждём ≥%d покупателей за %ds)",
                                              event.symbol, PREENTRY_BUYERS_MIN, PREENTRY_WAIT_SEC)
                            else:
                                # Старое поведение: мгновенная покупка (не рекомендуется)
                                asyncio.ensure_future(_buy_token(event, session))

                        else:
                            # Торговое событие — обновляем цену + счётчики
                            sol_amount   = msg.get("solAmount") or msg.get("vSolInBondingCurve") or 0
                            token_amount = msg.get("tokenAmount") or msg.get("vTokensInBondingCurve") or 0
                            is_buy       = msg.get("txType") == "buy"
                            trader       = msg.get("traderPublicKey", "")

                            if sol_amount and token_amount:
                                update_price_from_trade(mint, sol_amount, token_amount, is_buy)

                            if is_buy:
                                # ── Pre-entry счётчик ────────────────────────────
                                if mint in state.watching_tokens:
                                    w = state.watching_tokens[mint]
                                    w['buyers'] += 1
                                    log.debug("👁 %s: %d покупателей (порог %d)",
                                              w['event'].symbol, w['buyers'], PREENTRY_BUYERS_MIN)
                                    if w['buyers'] >= PREENTRY_BUYERS_MIN:
                                        log.info("🔥 %s: %d покупателей за %.0fs → ВХОДИМ!",
                                                 w['event'].symbol, w['buyers'],
                                                 time.time() - w['started'])
                                        asyncio.ensure_future(_buy_token(w['event'], session))
                                        del state.watching_tokens[mint]

                                # ── MOMENTUM_GATE счётчик (post-buy) ─────────────
                                if mint in state.positions:
                                    open_t = state.mint_open_time.get(mint, 0)
                                    if open_t and (time.time() - open_t) <= MOMENTUM_GATE_SEC:
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


async def watching_cleanup():
    """Удаляем токены, не набравшие покупателей за PREENTRY_WAIT_SEC секунд."""
    while True:
        await asyncio.sleep(5)
        if not state.watching_tokens:
            continue
        now = time.time()
        expired = [m for m, w in list(state.watching_tokens.items())
                   if now - w['started'] > PREENTRY_WAIT_SEC]
        for m in expired:
            w = state.watching_tokens.pop(m, None)
            if w:
                state.signals_expired += 1
                log.debug("⏰ %s: истёк таймаут %ds (набрал %d/%d покупателей) → пропуск",
                          w['event'].symbol, PREENTRY_WAIT_SEC,
                          w['buyers'], PREENTRY_BUYERS_MIN)


async def status_logger():
    """Статус каждые 60 секунд."""
    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)
        log.info("═" * 70)
        log.info("  %s UTC  |  %s",
                 now.strftime("%a %H:%M"),
                 "DRY RUN 🟡" if DRY_RUN else "LIVE 🟢")
        log.info("  Банк: %.4f SOL  |  Позиций: %d/%d  |  Цен в кэше: %d",
                 state.bank, len(state.positions), MAX_POSITIONS, len(_price_cache))
        log.info("  Сигналов: %d  |  Входов: %d  |  Пропущено: %d  |  Наблюдаем: %d  |  Истекло: %d",
                 state.signals_received, state.signals_entered, state.signals_skipped,
                 len(state.watching_tokens), state.signals_expired)
        log.info("  Фильтры: mayhem=%d  dev_buy=%d  BC_sol=%d  mcap_usd=%d  hours=%d  n_trans1=%d  momentum=%d",
                 state.filtered_mayhem, state.filtered_dev_buy,
                 state.filtered_bc_sol, state.filtered_mcap_usd,
                 state.filtered_hours, state.filtered_n_transfers, state.filtered_momentum)
        pnl_str = f"{state.session_pnl:+.4f}"
        fee_str = f"  (fees: -{state.session_fees:.4f} SOL)" if DRY_RUN else ""
        log.info("  Сделок: %d  |  Wins: %d  |  WR: %.1f%%  |  PnL: %s SOL%s",
                 state.session_trades, state.session_wins,
                 state.win_rate, pnl_str, fee_str)
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
                buyers  = state.mint_buy_count.get(mint, 0)
                phase   = ("⏳" if not pos.quick_stop_done else
                           f"👀{buyers}b" if not pos.momentum_done else "🏃")
                log.info("  %s %-10s  %.1f мин%s",
                         phase, pos.symbol, age_min, pnl_str)
        log.info("═" * 70)


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

    log.info("═" * 70)
    log.info("  PUMPSCALP v6.0  —  nya666+D5dtjf Strategy (56,654 trades analyzed)")
    log.info("  Стратегия: первый покупатель + nb60=%d (sentinel) | nb60<96=WR:0%% → немедленный выход",
             BUYERS_60S_MIN)
    log.info("  Режим: %s  |  Банк: %.1f SOL",
             "DRY RUN 🟡 (с комиссиями)" if DRY_RUN else "LIVE 🟢",
             state.bank)
    log.info("  Размер: %.2f SOL  |  Max позиций: %d  |  DCA/Re-entry: ОТКЛЮЧЕНО",
             BUY_SIZE_SOL, MAX_POSITIONS)
    log.info("  Quick stop T+%ds  |  MOMENTUM_GATE T+%ds (need ≥%d buyers)  |  Time stop %d мин",
             QUICK_STOP_SEC, MOMENTUM_GATE_SEC, BUYERS_60S_MIN, HARD_TIME_STOP_MIN)
    log.info("  Фильтры входа: dev_buy=%s  BC=%s  mcap≤$%.0f  avoid_hours=%s  skip_ntrans1=%s",
             f"<{DEV_BUY_SKIP_MIN_SOL}" if DEV_BUY_SKIP_MIN_SOL > 0 else "OFF",
             f"[{MIN_ENTRY_BC_SOL}–{MAX_ENTRY_BC_SOL}]" if MAX_ENTRY_BC_SOL > 0 else "OFF",
             MAX_ENTRY_MCAP_USD, AVOID_HOURS_UTC or "none", SKIP_N_TRANSFERS_1)
    log.info("  Pre-entry: ждём ≥%d покупателей за %ds → ТОГДА входим (0=мгновенно)",
             PREENTRY_BUYERS_MIN, PREENTRY_WAIT_SEC)
    log.info("  Post-entry: QUICK_STOP T+%ds → MOMENTUM_GATE T+%ds ≥%d buyers",
             QUICK_STOP_SEC, MOMENTUM_GATE_SEC, BUYERS_60S_MIN)
    log.info("  SL -%.0f%%  TP1 +%.0f%%→%.0f%%  TP2 +%.0f%%→%.0f%%  TP3 +%.0f%%  Trail -%.0f%% от пика",
             SL_PCT*100, TP1_PCT*100, TP1_SELL_FRAC*100,
             TP2_PCT*100, TP2_SELL_FRAC*100,
             TP3_PCT*100, TRAILING_DISTANCE*100)
    if DRY_RUN:
        log.info("  Комиссии: protocol=%.0f%%  priority=%.5f SOL  network=%.6f SOL",
                 PUMPFUN_FEE_PCT*100,
                 PRIORITY_FEE_MICROLAMPORTS / 1_000_000,
                 NETWORK_FEE_SOL)
    log.info("═" * 70)

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
            status_logger(),
            daily_reset(),
            autosave_loop(),
            watching_cleanup(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено.")
        log.info("Итого: %d сделок  |  WR %.1f%%  |  PnL %+.4f SOL",
                 state.session_trades, state.win_rate, state.session_pnl)
        save_trade_log()
