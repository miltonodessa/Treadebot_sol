"""
Bot configuration. Copy .env.example to .env and fill in your values.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── RPC ────────────────────────────────────────────────────────────────────────
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
WSS_URL = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# ── Wallets to copy-trade ──────────────────────────────────────────────────────
# 2TE2F: Evening bot (15-23h UTC), ~11 swaps/day, avg 3 SOL per trade
# AF6sy: 24/7 bot, ~44 swaps/day, avg 2 SOL per trade
TARGET_WALLETS = {
    "2TE2F3CJDbDhN3MgqNmnUh1NR5nHSornaK5daq65Lejs": {
        "label": "Evening Copy-Bot",
        "active_hours_utc": list(range(14, 24)),  # 14-23h UTC
        "avg_trade_sol": 3.0,
        "enabled": True,
    },
    "AF6syABApBp7d1NfjUkmKG7BBBEWVMvXadyvqQgjLnRN": {
        "label": "24/7 Bot",
        "active_hours_utc": list(range(0, 24)),
        "avg_trade_sol": 2.0,
        "enabled": True,
    },
}

# ── Our wallet ─────────────────────────────────────────────────────────────────
OUR_PRIVATE_KEY = os.getenv("OUR_PRIVATE_KEY", "")  # base58 or JSON array
OUR_WALLET = os.getenv("OUR_WALLET_ADDRESS", "")

# ── Trading parameters ─────────────────────────────────────────────────────────
# Scale factor relative to target wallet's trade size (0.1 = 10% of their size)
COPY_SCALE_FACTOR = float(os.getenv("COPY_SCALE_FACTOR", "0.1"))

# Max SOL per single trade
MAX_SOL_PER_TRADE = float(os.getenv("MAX_SOL_PER_TRADE", "0.5"))

# Min SOL per single trade (skip dust trades)
MIN_SOL_PER_TRADE = float(os.getenv("MIN_SOL_PER_TRADE", "0.05"))

# Max % of our balance to use per trade
MAX_PORTFOLIO_PCT = float(os.getenv("MAX_PORTFOLIO_PCT", "0.05"))  # 5%

# Slippage tolerance in basis points (50 = 0.5%)
SLIPPAGE_BPS = int(os.getenv("SLIPPAGE_BPS", "100"))

# Priority fee in microlamports
PRIORITY_FEE_MICROLAMPORTS = int(os.getenv("PRIORITY_FEE_MICROLAMPORTS", "100000"))

# ── Risk management ────────────────────────────────────────────────────────────
# Stop copying a token if we've lost more than X SOL on it
MAX_LOSS_PER_TOKEN_SOL = float(os.getenv("MAX_LOSS_PER_TOKEN_SOL", "0.3"))

# Daily loss limit - stop bot for the day if we lose this much
DAILY_LOSS_LIMIT_SOL = float(os.getenv("DAILY_LOSS_LIMIT_SOL", "2.0"))

# Auto-sell if token drops X% from our buy price
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.30"))  # 30%

# Take profit at X% gain
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "1.00"))  # 100% = 2x

# ── Filters ────────────────────────────────────────────────────────────────────
# Skip tokens that are on this blacklist
TOKEN_BLACKLIST: set[str] = set(os.getenv("TOKEN_BLACKLIST", "").split(",")) - {""}

# Only copy trades where target uses these DEXes (empty = all)
ALLOWED_DEXES: list[str] = ["Jupiter v6"]

# Min liquidity in USD to trade a token
MIN_TOKEN_LIQUIDITY_USD = float(os.getenv("MIN_TOKEN_LIQUIDITY_USD", "10000"))
