"""
Bot configuration. Copy .env.example to .env and fill in your values.

Параметры настроены под стратегию 2TE2F:
- Торгует только 15-23h UTC
- Размеры: 2/3/5 SOL (scale фактором от этих размеров)
- Медианное удержание: 20 секунд (emergency exit через 5 мин)
- Частичные продажи вслед за мастером
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── RPC ────────────────────────────────────────────────────────────────────────
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
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

# Старые wallets dict для обратной совместимости с bot.py
TARGET_WALLETS = {
    "2TE2F3CJDbDhN3MgqNmnUh1NR5nHSornaK5daq65Lejs": {
        "label": "Evening Copy-Bot",
        "active_hours_utc": list(range(15, 24)),
        "avg_trade_sol": 3.0,
        "enabled": True,
    },
}
