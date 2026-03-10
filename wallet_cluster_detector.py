"""
wallet_cluster_detector.py — Pre-Launch Wallet Funding Detection
================================================================
Обнаруживает координированный запуск токена по паттерну:

    funding_wallet → wallet_A  (0.1–1.5 SOL)
    funding_wallet → wallet_B  (0.1–1.5 SOL)
    funding_wallet → wallet_C  (0.1–1.5 SOL)

Все переводы — в пределах CLUSTER_WINDOW_SEC до создания токена.
Использует Helius Enhanced Transactions API.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Optional

import aiohttp

log = logging.getLogger("cluster")

LAMPORTS_PER_SOL = 1_000_000_000
_HELIUS_TX_URL   = "https://api.helius.xyz/v0/addresses/{addr}/transactions"


class WalletClusterDetector:
    """
    Для каждого нового токена:
      1. Получаем входящие SOL-переводы на адрес creator за последние window_sec
      2. Находим источники (funding wallets)
      3. Для каждого источника считаем, сколько разных кошельков он профинансировал
      4. Если >= min_wallets — флаг «coordinated launch»
    """

    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        window_sec: int   = 120,
        min_wallets: int  = 3,
        min_sol: float    = 0.10,
        max_sol: float    = 1.50,
        cache_ttl: float  = 300.0,
    ):
        if not api_key:
            log.warning("HELIUS_API_KEY not set — cluster detection disabled")
        self.api_key     = api_key
        self.session     = session
        self.window_sec  = window_sec
        self.min_wallets = min_wallets
        self.min_sol     = min_sol
        self.max_sol     = max_sol
        self.cache_ttl   = cache_ttl
        # creator → (result, ts)
        self._cache: dict[str, tuple[bool, float]] = {}
        self._in_flight: set[str] = set()

    # ──────────────────────────────────────────────────────────────────────────

    async def is_coordinated(
        self,
        creator: str,
        created_at: float,
    ) -> bool:
        """
        Async entry point. Returns True if a funding cluster is detected.
        Returns False if API key absent or any error occurs (safe default).
        """
        if not self.api_key or not creator:
            return False

        # Cache hit
        cached = self._cache.get(creator)
        if cached and time.time() - cached[1] < self.cache_ttl:
            return cached[0]

        # Deduplicate concurrent requests for same creator
        if creator in self._in_flight:
            await asyncio.sleep(0.2)
            return self._cache.get(creator, (False, 0))[0]

        self._in_flight.add(creator)
        try:
            result = await self._detect(creator, created_at)
            self._cache[creator] = (result, time.time())
            return result
        except Exception as exc:
            log.debug("cluster detect error [%s]: %s", creator[:8], exc)
            return False
        finally:
            self._in_flight.discard(creator)

    # ──────────────────────────────────────────────────────────────────────────

    async def _detect(self, creator: str, created_at: float) -> bool:
        """Core detection logic."""
        cutoff = created_at - self.window_sec

        # Step 1: incoming SOL transfers to creator
        sources = await self._incoming_sources(creator, cutoff)
        if not sources:
            return False

        log.debug("cluster: creator=%s got SOL from %d unique sources",
                  creator[:8], len(sources))

        # Step 2: for each source check how many wallets it funded in the window
        for source in sources:
            funded = await self._wallets_funded_by(source, cutoff)
            if len(funded) >= self.min_wallets:
                log.info(
                    "🔗 Cluster: %s → funded %d wallets (creator=%s incl.)",
                    source[:8], len(funded), creator[:8],
                )
                return True

        return False

    async def _incoming_sources(
        self, address: str, cutoff: float
    ) -> set[str]:
        """
        Return all wallets that sent 0.1–1.5 SOL to `address` after `cutoff`.
        Uses Helius Enhanced Transactions API (type=TRANSFER).
        """
        url = _HELIUS_TX_URL.format(addr=address)
        params = {"api-key": self.api_key, "type": "TRANSFER", "limit": "100"}

        try:
            async with self.session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=6),
            ) as r:
                if r.status != 200:
                    return set()
                txs = await r.json()
        except asyncio.TimeoutError:
            return set()

        if not isinstance(txs, list):
            return set()

        sources: set[str] = set()
        for tx in txs:
            if tx.get("timestamp", 0) < cutoff:
                break  # sorted descending by timestamp
            for t in tx.get("nativeTransfers", []):
                frm = t.get("fromUserAccount", "")
                to  = t.get("toUserAccount", "")
                amt = t.get("amount", 0) / LAMPORTS_PER_SOL
                if to == address and frm != address:
                    if self.min_sol <= amt <= self.max_sol:
                        sources.add(frm)
        return sources

    async def _wallets_funded_by(
        self, source: str, cutoff: float
    ) -> set[str]:
        """
        Return all wallets that `source` sent 0.1–1.5 SOL to after `cutoff`.
        """
        url = _HELIUS_TX_URL.format(addr=source)
        params = {"api-key": self.api_key, "type": "TRANSFER", "limit": "100"}

        try:
            async with self.session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=6),
            ) as r:
                if r.status != 200:
                    return set()
                txs = await r.json()
        except (asyncio.TimeoutError, Exception):
            return set()

        if not isinstance(txs, list):
            return set()

        funded: set[str] = set()
        for tx in txs:
            if tx.get("timestamp", 0) < cutoff:
                break
            for t in tx.get("nativeTransfers", []):
                frm = t.get("fromUserAccount", "")
                to  = t.get("toUserAccount", "")
                amt = t.get("amount", 0) / LAMPORTS_PER_SOL
                if frm == source and to != source:
                    if self.min_sol <= amt <= self.max_sol:
                        funded.add(to)
        return funded
