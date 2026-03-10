"""
trade_engine.py — Solana Trade Executor
========================================
Handles buy/sell execution via PumpPortal trade-local API.
Signs and sends VersionedTransactions via Helius RPC.

Supports DRY_RUN mode: simulates all trades without sending tx.
"""
from __future__ import annotations

import logging
import ssl
import time
from base64 import b64decode, b64encode
from typing import Optional

import aiohttp
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

log = logging.getLogger("trade_engine")

PUMPPORTAL_TRADE  = "https://pumpportal.fun/api/trade-local"
PUMP_TOTAL_SUPPLY = 1_000_000_000  # 1B tokens (pump.fun standard)
LAMPORTS_PER_SOL  = 1_000_000_000

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode    = ssl.CERT_NONE


class TradeEngine:
    """
    Stateless trade executor.
    Instantiate once, reuse for all trades.
    """

    def __init__(
        self,
        private_key_b58: str,
        rpc_url: str,
        session: aiohttp.ClientSession,
        buy_size_sol: float   = 0.10,
        slippage_pct: float   = 25.0,     # %  (25% for micro-cap)
        priority_fee_sol: float = 0.0001, # SOL
        dry_run: bool         = True,
    ):
        self.session          = session
        self.rpc_url          = rpc_url
        self.buy_size_sol     = buy_size_sol
        self.slippage_pct     = slippage_pct
        self.priority_fee_sol = priority_fee_sol
        self.dry_run          = dry_run

        self._keypair: Optional[Keypair] = _load_keypair(private_key_b58)
        if not self._keypair and not dry_run:
            log.error("No valid keypair — all trades will fail in LIVE mode")

        # Simple price cache: mint → (price_sol_per_tok, ts)
        self._price_cache: dict[str, tuple[float, float]] = {}

    # ── Price ─────────────────────────────────────────────────────────────

    def update_price(self, mint: str, sol_amt: float, tok_amt: int):
        """Called by the WS event loop when a trade is observed."""
        if sol_amt > 0 and tok_amt > 0:
            self._price_cache[mint] = (sol_amt / tok_amt, time.time())

    def get_price(self, mint: str) -> Optional[float]:
        entry = self._price_cache.get(mint)
        if entry and time.time() - entry[1] < 120:
            return entry[0]
        return None

    @property
    def pubkey_str(self) -> str:
        if self._keypair:
            return str(self._keypair.pubkey())
        return ""

    # ── Buy ───────────────────────────────────────────────────────────────

    async def buy(
        self,
        mint: str,
        symbol: str,
        sol_amount: Optional[float] = None,
    ) -> Optional["TradeResult"]:
        """
        Buy `sol_amount` SOL worth of `mint`.
        Returns TradeResult or None on failure.
        """
        amount = sol_amount or self.buy_size_sol

        if self.dry_run:
            return self._dry_buy(mint, symbol, amount)

        data = await self._pumpportal_request("buy", mint, amount)
        if not data:
            return None

        tx64 = data.get("transaction") or data.get("tx")
        if not tx64:
            log.debug("buy: no tx in response: %s", str(data)[:200])
            return None

        sig = await self._sign_and_send(tx64)
        if not sig:
            log.warning("buy tx failed: %s", symbol)
            return None

        tokens = int(data.get("outAmount") or data.get("tokensReceived") or 0)
        if tokens == 0:
            tokens = int(amount * 1e6)  # rough estimate
        price  = amount / max(tokens, 1)

        self.update_price(mint, amount, tokens)

        log.info("🟢 BUY  %-10s  %.3f SOL  %d tok  @%.2e  sig=%.8s",
                 symbol, amount, tokens, price, sig)

        return TradeResult(
            mint=mint, symbol=symbol,
            sol_spent=amount, tokens=tokens,
            price=price, tx_sig=sig,
        )

    def _dry_buy(
        self, mint: str, symbol: str, amount: float
    ) -> "TradeResult":
        cached = self.get_price(mint)
        price  = cached if cached else 1e-9
        fee    = amount * 0.01 + self.priority_fee_sol
        net    = amount - fee
        tokens = int(net / price) if price > 0 else int(net * 1e6)
        price  = amount / max(tokens, 1)

        log.info("🟡 [DRY] BUY  %-10s  %.3f SOL  %d tok  fee=%.5f SOL",
                 symbol, amount, tokens, fee)

        return TradeResult(
            mint=mint, symbol=symbol,
            sol_spent=amount, tokens=tokens,
            price=price, tx_sig="DRY_RUN",
            fee_sol=fee,
        )

    # ── Sell ──────────────────────────────────────────────────────────────

    async def sell(
        self,
        mint: str,
        symbol: str,
        token_amount: int,
        current_price: float,
        reason: str,
    ) -> float:
        """
        Sell `token_amount` tokens of `mint`.
        Returns SOL received (0.0 on failure).
        """
        if token_amount <= 0:
            return 0.0

        gross_sol = token_amount * current_price

        if self.dry_run:
            return self._dry_sell(mint, symbol, token_amount, current_price, reason)

        data = await self._pumpportal_request("sell", mint, token_amount, denominated_sol=False)
        if not data:
            log.warning("sell quote failed: %s", symbol)
            return 0.0

        tx64 = data.get("transaction") or data.get("tx")
        if not tx64:
            return 0.0

        sig = await self._sign_and_send(tx64)
        if not sig:
            log.warning("sell tx failed: %s", symbol)
            return 0.0

        sol_received = int(data.get("outAmount") or data.get("solReceived") or
                           gross_sol * LAMPORTS_PER_SOL) / LAMPORTS_PER_SOL

        log.info("🔴 SELL  %-10s  %.4f SOL  [%s]  sig=%.8s",
                 symbol, sol_received, reason, sig)
        return sol_received

    def _dry_sell(
        self,
        mint: str,
        symbol: str,
        token_amount: int,
        current_price: float,
        reason: str,
    ) -> float:
        gross = token_amount * current_price
        fee   = gross * 0.01 + self.priority_fee_sol
        net   = gross - fee
        log.info("🟡 [DRY] SELL  %-10s  gross=%.4f  net=%.4f  fee=%.5f  [%s]",
                 symbol, gross, net, fee, reason)
        return net

    # ── PumpPortal API ────────────────────────────────────────────────────

    async def _pumpportal_request(
        self,
        action: str,
        mint: str,
        amount,
        denominated_sol: bool = True,
    ) -> Optional[dict]:
        pub = self.pubkey_str
        if not pub:
            return None

        payload = {
            "publicKey":         pub,
            "action":            action,
            "mint":              mint,
            "amount":            amount,
            "denominatedInSol":  "true" if denominated_sol else "false",
            "slippage":          self.slippage_pct,
            "priorityFee":       self.priority_fee_sol,
            "pool":              "pump",
        }
        try:
            async with self.session.post(
                PUMPPORTAL_TRADE,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
                ssl=_ssl_ctx,
            ) as r:
                if r.status != 200:
                    text = await r.text()
                    log.debug("pumpportal %s %d: %s", action, r.status, text[:200])
                    return None
                return await r.json()
        except Exception as exc:
            log.debug("pumpportal_%s error: %s", action, exc)
            return None

    # ── Sign & Send ───────────────────────────────────────────────────────

    async def _sign_and_send(self, tx_base64: str) -> Optional[str]:
        if not self._keypair:
            return None
        try:
            raw = b64decode(tx_base64)
            tx  = VersionedTransaction.from_bytes(raw)
            tx.sign([self._keypair])
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method":  "sendTransaction",
                "params":  [
                    b64encode(bytes(tx)).decode(),
                    {
                        "encoding":             "base64",
                        "skipPreflight":        False,
                        "maxRetries":           3,
                        "preflightCommitment":  "confirmed",
                    },
                ],
            }
            async with self.session.post(
                self.rpc_url, json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                d = await r.json()
                if "error" in d:
                    log.debug("sendTx error: %s", d["error"])
                    return None
                return d.get("result")
        except Exception as exc:
            log.debug("sign_and_send error: %s", exc)
            return None


# ── Helpers ───────────────────────────────────────────────────────────────────


def _load_keypair(private_key_b58: str) -> Optional[Keypair]:
    if not private_key_b58:
        return None
    try:
        import base58
        return Keypair.from_bytes(base58.b58decode(private_key_b58))
    except Exception as exc:
        log.error("Failed to load keypair: %s", exc)
        return None


# ── Data class ────────────────────────────────────────────────────────────────


class TradeResult:
    __slots__ = ("mint", "symbol", "sol_spent", "tokens",
                 "price", "tx_sig", "fee_sol", "opened_at")

    def __init__(
        self,
        mint: str,
        symbol: str,
        sol_spent: float,
        tokens: int,
        price: float,
        tx_sig: str,
        fee_sol: float = 0.0,
    ):
        self.mint      = mint
        self.symbol    = symbol
        self.sol_spent = sol_spent
        self.tokens    = tokens
        self.price     = price
        self.tx_sig    = tx_sig
        self.fee_sol   = fee_sol
        self.opened_at = time.time()
