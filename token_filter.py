"""
Token filter — проверяем токен перед входом.

Выявлено из анализа 2TE2F:
- Торгует pump.fun токены (24%) и мигрировавшие на Raydium (76%)
- Избегает токены со слишком малой ликвидностью (не попадаются в данных)
- НЕ торгует USDC/USDT/стейблы
- Токены с одной крупной покупкой девелопера — признак rug pull

Фильтр НЕ пытается предсказать, вырастет ли токен.
Он только отсеивает явных кандидатов на rug pull и слишком рискованные позиции.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp

log = logging.getLogger("token_filter")

# Известные стейблы и ликвидные токены — не торгуем ими (не мемкоины)
SKIP_MINTS = {
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "So11111111111111111111111111111111111111112",      # wSOL
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",   # mSOL
    "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",   # ETH (Wormhole)
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",   # BONK
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",   # RAY
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",    # JUP
    "WENWENvqqNya429ubCdR81ZmD69brwQaaBYY6p3LCpk",    # WEN
}


@dataclass
class TokenInfo:
    mint: str
    liquidity_sol: float      # ликвидность в SOL
    holder_count: int
    top_holder_pct: float     # % у крупнейшего держателя
    is_pump_fun: bool
    age_seconds: float        # возраст токена в секундах
    recent_volume_sol: float  # объём за последние 5 минут


async def get_token_info_helius(
    mint: str, session: aiohttp.ClientSession, api_key: str
) -> Optional[TokenInfo]:
    """
    Получаем метаданные токена через Helius DAS API.
    """
    url = f"https://api.helius.xyz/v0/token-metadata?api-key={api_key}"
    try:
        async with session.post(
            url, json={"mintAccounts": [mint]},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
            if not data:
                return None
            meta = data[0]

            # Pump.fun токены определяем по программе создания
            is_pump = "pump" in mint.lower() or meta.get("onChainData", {}).get(
                "updateAuthority", ""
            ) == "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM"

            return TokenInfo(
                mint=mint,
                liquidity_sol=0.0,  # заполняется отдельно
                holder_count=0,
                top_holder_pct=0.0,
                is_pump_fun=is_pump,
                age_seconds=0.0,
                recent_volume_sol=0.0,
            )
    except Exception:
        return None


def passes_basic_filter(mint: str) -> tuple[bool, str]:
    """
    Быстрая проверка без API-запросов.
    Возвращает (ok, reason).
    """
    if mint in SKIP_MINTS:
        return False, "known non-meme token"

    # Pump.fun токены заканчиваются на 'pump' — это хороший признак ликвидности
    # на ранней стадии (можно быстро продать обратно по bonding curve)
    if mint.endswith("pump"):
        return True, "pump.fun token"

    # Раймиум токены — нужна ликвидность
    return True, "ok"


async def check_liquidity_jupiter(
    mint: str, test_sol: float, session: aiohttp.ClientSession
) -> tuple[bool, float]:
    """
    Проверяем ликвидность через Jupiter: можем ли мы купить test_sol SOL
    с приемлемым slippage?
    Возвращает (ok, price_impact_pct).
    """
    lamports = int(test_sol * 1e9)
    try:
        async with session.get(
            "https://quote-api.jup.ag/v6/quote",
            params={
                "inputMint": "So11111111111111111111111111111111111111112",
                "outputMint": mint,
                "amount": lamports,
                "slippageBps": 1000,  # широкий slippage для теста
            },
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            if r.status != 200:
                return False, 999.0
            data = await r.json()

            price_impact = float(data.get("priceImpactPct", 99.0))
            # Если price impact при нашем размере < 5% — ликвидность ок
            return price_impact < 5.0, price_impact
    except Exception:
        return False, 999.0


async def validate_token(
    mint: str,
    our_buy_sol: float,
    session: aiohttp.ClientSession,
    api_key: str = "",
) -> tuple[bool, str]:
    """
    Полная проверка токена перед входом.
    Возвращает (ok, reason).

    Вызывается перед каждой покупкой — быстро (< 2 секунды).
    """
    # 1. Базовый фильтр (без API)
    ok, reason = passes_basic_filter(mint)
    if not ok:
        return False, reason

    # 2. Проверка ликвидности через Jupiter
    liquid, impact = await check_liquidity_jupiter(mint, our_buy_sol, session)
    if not liquid:
        return False, f"low liquidity (price impact {impact:.1f}%)"

    log.debug("✅ %s прошёл фильтр | price impact: %.2f%%", mint[:8], impact)
    return True, f"ok (impact {impact:.2f}%)"
