"""
Bot configuration. Copy .env.example to .env and fill in your values.

Поддерживаются два бота:
- pumpscalp_bot.py  — PUMPSCALP v1.0 (рекомендуется)
- strategy_bot.py   — copy-trading 2TE2F
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── RPC ────────────────────────────────────────────────────────────────────────
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
WSS_URL = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# ── Our wallet ─────────────────────────────────────────────────────────────────
OUR_PRIVATE_KEY = os.getenv("OUR_PRIVATE_KEY", "")  # base58
OUR_WALLET      = os.getenv("OUR_WALLET_ADDRESS", "")

# ── Trading parameters ─────────────────────────────────────────────────────────
# Масштаб относительно размера 2TE2F (0.1 = 10% от его сделки)
# 2TE2F покупает на 2-5 SOL → при 0.1 мы будем на 0.2-0.5 SOL
COPY_SCALE_FACTOR = float(os.getenv("COPY_SCALE_FACTOR", "0.1"))

# Максимум SOL в одной сделке (защита от крупных аномальных сделок)
MAX_SOL_PER_TRADE = float(os.getenv("MAX_SOL_PER_TRADE", "0.5"))

# Минимум SOL в сделке (пропускаем пыль)
MIN_SOL_PER_TRADE = float(os.getenv("MIN_SOL_PER_TRADE", "0.05"))

# Максимум % от баланса на одну позицию
MAX_PORTFOLIO_PCT = float(os.getenv("MAX_PORTFOLIO_PCT", "0.10"))  # 10%

# Максимум одновременно открытых позиций
# 2TE2F редко держит >3 позиций одновременно
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "5"))

# Slippage в basis points (100 = 1%)
# 2TE2F работает быстро — используем 1% чтобы не терять исполнение
SLIPPAGE_BPS = int(os.getenv("SLIPPAGE_BPS", "100"))

# Priority fee (microlamports) — повышаем для быстрого исполнения
# 2TE2F платит ~0.0055 SOL за сделку
PRIORITY_FEE_MICROLAMPORTS = int(os.getenv("PRIORITY_FEE_MICROLAMPORTS", "500_000"))

# ── Risk management ────────────────────────────────────────────────────────────
# Дневной лимит убытка — бот останавливается на день
DAILY_LOSS_LIMIT_SOL = float(os.getenv("DAILY_LOSS_LIMIT_SOL", "1.0"))

# Стоп-лосс на позицию (% убытка от покупки)
# 2TE2F редко получает убыток >30%, ставим чуть жёстче
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.25"))  # 25%

# Emergency exit если 2TE2F не продал (сек) — из данных: max hold 25 мин
MAX_HOLD_SECS = int(os.getenv("MAX_HOLD_SECS", "300"))  # 5 минут

# Порог прибыли для парковки в USDC (как делает 2TE2F сам)
PROFIT_PARK_SOL = float(os.getenv("PROFIT_PARK_SOL", "5.0"))

# ── Filters ────────────────────────────────────────────────────────────────────
TOKEN_BLACKLIST: set[str] = set(os.getenv("TOKEN_BLACKLIST", "").split(",")) - {""}

# ── Coordinated Launch Sniper (main.py) ────────────────────────────────────────
# Signal 0: Pre-launch funding cluster (Helius API)
CLUSTER_WINDOW_SEC   = int(os.getenv("CLUSTER_WINDOW_SEC",   "120"))   # look-back window
CLUSTER_MIN_WALLETS  = int(os.getenv("CLUSTER_MIN_WALLETS",  "3"))     # min funded wallets
CLUSTER_MIN_SOL      = float(os.getenv("CLUSTER_MIN_SOL",    "0.10"))  # ignore < 0.1 SOL transfers
CLUSTER_MAX_SOL      = float(os.getenv("CLUSTER_MAX_SOL",    "1.50"))  # ignore > 1.5 SOL transfers

# Entry window
ENTRY_MIN_SEC        = float(os.getenv("ENTRY_MIN_SEC",       "12"))
ENTRY_MAX_SEC        = float(os.getenv("ENTRY_MAX_SEC",       "35"))

# Signal 1: Slot clustering
SLOT_CLUSTER_MIN     = int(os.getenv("SLOT_CLUSTER_MIN",      "3"))    # wallets in first 2 seconds

# Signal 2: Liquidity velocity
VELOCITY_MIN_SOL     = float(os.getenv("VELOCITY_MIN_SOL",    "1.0"))  # minimum buy vol
VELOCITY_STRONG_SOL  = float(os.getenv("VELOCITY_STRONG_SOL", "3.0"))  # strong signal

# Signal 3: Sell pressure
SELL_PRESSURE_MAX    = float(os.getenv("SELL_PRESSURE_MAX",   "0.35"))  # < 35%
SELL_PRESSURE_EMRG   = float(os.getenv("SELL_PRESSURE_EMRG",  "0.60"))  # emergency exit > 60%

# Signal 4: Unique buyers
BUYERS_MIN           = int(os.getenv("BUYERS_MIN",             "10"))   # ≥ 10

# Signal 5: Sell absorption — price drop threshold (20% → reject)
PRICE_DROP_REJECT    = float(os.getenv("PRICE_DROP_REJECT",   "0.20"))

# Trade
BUY_SIZE_SOL_SNIPER  = float(os.getenv("BUY_SIZE_SOL",        "0.10"))
SLIPPAGE_PCT_SNIPER  = float(os.getenv("SLIPPAGE_PCT",         "25.0"))  # %
PRIORITY_FEE_SNIPER  = float(os.getenv("PRIORITY_FEE_SOL",    "0.0001"))

# Risk
TP1_PCT_SNIPER       = float(os.getenv("TP1_PCT",              "0.40"))   # +40%
TP2_PCT_SNIPER       = float(os.getenv("TP2_PCT",              "0.80"))   # +80%
TP3_PCT_SNIPER       = float(os.getenv("TP3_PCT",              "1.20"))   # +120%
TP1_FRAC_SNIPER      = float(os.getenv("TP1_FRAC",             "0.40"))   # sell 40% at TP1
TP2_FRAC_SNIPER      = float(os.getenv("TP2_FRAC",             "0.35"))   # sell 35% at TP2
TP3_FRAC_SNIPER      = float(os.getenv("TP3_FRAC",             "1.00"))   # sell all at TP3
SL_PCT_SNIPER        = float(os.getenv("SL_PCT",               "0.25"))   # -25%
MAX_POSITIONS_SNIPER = int(os.getenv("MAX_POSITIONS",           "5"))
MAX_HOLD_SNIPER      = int(os.getenv("MAX_HOLD_SEC",            "1800"))   # 30 min

# Старые wallets dict для обратной совместимости с bot.py
TARGET_WALLETS = {
    "2TE2F3CJDbDhN3MgqNmnUh1NR5nHSornaK5daq65Lejs": {
        "label": "Evening Copy-Bot",
        "active_hours_utc": list(range(15, 24)),
        "avg_trade_sol": 3.0,
        "enabled": True,
    },
}
