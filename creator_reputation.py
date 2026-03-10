"""
creator_reputation.py — Creator Wallet Reputation Filter
==========================================================
Проверяет историю создателя токена:

  50 launches, 0 survivors → мусор → отклоняем
   5 launches, 2 survivors → нарратив жив → пропускаем

Логика:
  1. Через Helius API получаем прошлые pump.fun-токены создателя
  2. Для каждого токена проверяем через pump.fun API, выжил ли
     (graduated to Raydium OR market_cap > $5k)
  3. Если creators запустил ≥ REPUTATION_MIN_LAUNCHES токенов
     и доля выживших < REPUTATION_MIN_SURVIVAL_RATE → reject

Новые кошельки (< REPUTATION_MIN_LAUNCHES) — пропускаем (нет данных).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

log = logging.getLogger("reputation")

LAMPORTS_PER_SOL  = 1_000_000_000
PUMP_PROGRAM      = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_COINS_API    = "https://frontend-api.pump.fun/coins/{mint}"
HELIUS_TX_URL     = "https://api.helius.xyz/v0/addresses/{addr}/transactions"


@dataclass
class ReputationResult:
    creator:        str
    total_launches: int
    survivors:      int
    survival_rate:  float   # 0.0 – 1.0
    is_reputable:   bool
    reason:         str     # human-readable verdict


class CreatorReputation:
    """
    Thread-safe async reputation checker with two-level cache:
      creator → ReputationResult  (cached CACHE_TTL seconds)
      mint    → bool survived      (cached indefinitely per session)
    """

    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        min_launches: int        = 5,      # ignore creators with fewer launches
        min_survival_rate: float = 0.10,   # < 10% survivors → reject
        survival_mcap_usd: float = 5_000,  # token "survived" if mcap > $5k
        max_tokens_to_check: int = 30,     # check last N tokens per creator
        cache_ttl: float         = 600.0,  # 10-minute cache
    ):
        if not api_key:
            log.warning("HELIUS_API_KEY not set — creator reputation disabled")
        self.api_key          = api_key
        self.session          = session
        self.min_launches     = min_launches
        self.min_survival     = min_survival_rate
        self.survival_mcap    = survival_mcap_usd
        self.max_check        = max_tokens_to_check
        self.cache_ttl        = cache_ttl

        # creator → (ReputationResult, timestamp)
        self._cache: dict[str, tuple[ReputationResult, float]] = {}
        # mint → survived (bool)
        self._mint_cache: dict[str, bool] = {}
        self._in_flight: set[str] = set()

    # ── Public ────────────────────────────────────────────────────────────

    async def check(self, creator: str) -> ReputationResult:
        """
        Returns ReputationResult.is_reputable:
          True  → allow entry
          False → skip this token (bad creator history)
        """
        if not self.api_key or not creator:
            return self._unknown(creator, "no_api_key")

        # Cache hit
        cached = self._cache.get(creator)
        if cached and time.time() - cached[1] < self.cache_ttl:
            return cached[0]

        # Deduplicate concurrent checks for same creator
        if creator in self._in_flight:
            await asyncio.sleep(0.3)
            cached = self._cache.get(creator)
            if cached:
                return cached[0]
            return self._unknown(creator, "in_flight")

        self._in_flight.add(creator)
        try:
            result = await self._evaluate(creator)
            self._cache[creator] = (result, time.time())
            return result
        except Exception as exc:
            log.debug("reputation check error [%s]: %s", creator[:8], exc)
            return self._unknown(creator, f"error: {exc}")
        finally:
            self._in_flight.discard(creator)

    # ── Core logic ────────────────────────────────────────────────────────

    async def _evaluate(self, creator: str) -> ReputationResult:
        # Step 1: get creator's pump.fun token launch history
        mints = await self._get_created_tokens(creator)

        if len(mints) < self.min_launches:
            return self._unknown(
                creator,
                f"new_creator_{len(mints)}_launches",
                total_launches=len(mints),
            )

        # Step 2: check survival for each mint (parallel)
        mints_to_check = mints[: self.max_check]
        tasks = [self._is_survived(m) for m in mints_to_check]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        survivors = sum(
            1 for r in results if r is True
        )
        total     = len(mints_to_check)
        rate      = survivors / total if total else 0.0

        is_ok    = rate >= self.min_survival
        verdict  = (
            f"ok_{survivors}/{total}_survived_{rate:.0%}"
            if is_ok
            else f"rejected_{survivors}/{total}_survived_{rate:.0%}"
        )

        result = ReputationResult(
            creator=creator,
            total_launches=len(mints),
            survivors=survivors,
            survival_rate=rate,
            is_reputable=is_ok,
            reason=verdict,
        )

        icon = "✅" if is_ok else "🚫"
        log.info(
            "%s Creator %s: %d launches, %d survived (%.0f%%) — %s",
            icon, creator[:8],
            len(mints), survivors, rate * 100,
            "PASS" if is_ok else "BLOCKED",
        )
        return result

    # ── Helius: past token creations ──────────────────────────────────────

    async def _get_created_tokens(self, creator: str) -> list[str]:
        """
        Return mints of tokens created by `creator` via pump.fun program.
        Uses Helius enhanced transactions (type=COMPRESSED_NFT_MINT or raw).
        """
        url    = HELIUS_TX_URL.format(addr=creator)
        params = {
            "api-key": self.api_key,
            "limit":   "100",
        }
        try:
            async with self.session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=7),
            ) as r:
                if r.status != 200:
                    return []
                txs = await r.json()
        except Exception:
            return []

        if not isinstance(txs, list):
            return []

        mints: list[str] = []
        for tx in txs:
            # Look for pump.fun "create" instructions
            for ix in tx.get("instructions", []):
                if ix.get("programId") == PUMP_PROGRAM:
                    # Mint is typically the first account after program
                    accounts = ix.get("accounts", [])
                    if accounts:
                        candidate = accounts[0]
                        if candidate not in mints and candidate != creator:
                            mints.append(candidate)
                            break
            # Also parse tokenTransfers for newly created mints
            for tt in tx.get("tokenTransfers", []):
                mint = tt.get("mint", "")
                if mint and mint not in mints:
                    # Only add if this tx is a creation (fromUserAccount empty)
                    if not tt.get("fromUserAccount"):
                        mints.append(mint)

        return mints

    # ── Pump.fun: survival check ──────────────────────────────────────────

    async def _is_survived(self, mint: str) -> bool:
        """
        A token "survived" if:
          - Graduated to Raydium (complete=true), OR
          - Market cap > survival_mcap_usd
        """
        if mint in self._mint_cache:
            return self._mint_cache[mint]

        try:
            async with self.session.get(
                PUMP_COINS_API.format(mint=mint),
                timeout=aiohttp.ClientTimeout(total=4),
            ) as r:
                if r.status != 200:
                    self._mint_cache[mint] = False
                    return False
                data = await r.json()
        except Exception:
            return False

        graduated = bool(data.get("complete", False))
        mcap_usd  = float(data.get("usd_market_cap", 0))
        survived  = graduated or mcap_usd >= self.survival_mcap

        self._mint_cache[mint] = survived
        return survived

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _unknown(
        creator: str,
        reason: str,
        total_launches: int = 0,
    ) -> ReputationResult:
        return ReputationResult(
            creator=creator,
            total_launches=total_launches,
            survivors=0,
            survival_rate=0.0,
            is_reputable=True,   # unknown → allow (benefit of the doubt)
            reason=reason,
        )
