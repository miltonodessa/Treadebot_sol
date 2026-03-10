"""
token_scanner.py — Real-Time Token Scanner & Signal Coordinator
================================================================
Connects to the pump.fun WebSocket, routes events to:
  • VelocityAnalyzer  (trade events)
  • WalletClusterDetector (on token create — async background)

Signal evaluation loop runs every 500ms and fires on_entry_signal()
when ALL 6 conditions are met within the 12–35s entry window.
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from typing import Awaitable, Callable, Optional

import aiohttp
import websockets

from velocity_analyzer import VelocityAnalyzer, TokenSignals

log = logging.getLogger("scanner")

PUMPFUN_WS = "wss://pumpportal.fun/api/data"

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode    = ssl.CERT_NONE

# Callback type: async (mint, TokenSignals) → None
EntryCallback = Callable[[str, TokenSignals], Awaitable[None]]


class TokenScanner:
    """
    Main WebSocket driver.

    Usage:
        scanner = TokenScanner(va, cd, on_entry=my_callback, session=session)
        await scanner.run()
    """

    def __init__(
        self,
        velocity_analyzer: VelocityAnalyzer,
        cluster_detector,           # WalletClusterDetector | None
        on_entry: EntryCallback,
        session: aiohttp.ClientSession,
        entry_min_sec: float = 12.0,
        entry_max_sec: float = 35.0,
        signal_check_interval: float = 0.5,
    ):
        self.va            = velocity_analyzer
        self.cd            = cluster_detector
        self.on_entry      = on_entry
        self.session       = session
        self.entry_min     = entry_min_sec
        self.entry_max     = entry_max_sec
        self.check_iv      = signal_check_interval

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._subscribed: set[str] = set()
        self._running     = False

        # Stats
        self.n_created    = 0
        self.n_entered    = 0
        self.n_rejected   = 0
        self.n_expired    = 0

    # ── Public ────────────────────────────────────────────────────────────

    async def run(self):
        """Start scanner. Reconnects automatically on WS failure."""
        self._running = True
        asyncio.ensure_future(self._signal_loop())
        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("Scanner WS error: %s — reconnecting in 3s", exc)
                await asyncio.sleep(3)

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

    # ── WebSocket ─────────────────────────────────────────────────────────

    async def _connect_and_listen(self):
        async with websockets.connect(
            PUMPFUN_WS,
            ssl=_ssl_ctx,
            ping_interval=20,
            ping_timeout=10,
            max_size=2 ** 22,
            open_timeout=15,
        ) as ws:
            self._ws = ws
            log.info("📡 Connected to pump.fun WS")
            await ws.send(json.dumps({"method": "subscribeNewToken"}))

            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                await self._route(ws, msg)

    async def _route(self, ws, msg: dict):
        tx_type = msg.get("txType", "")
        mint    = msg.get("mint", "")

        if tx_type == "create" or (mint and "name" in msg and "symbol" in msg):
            await self._on_create(ws, msg)
        elif tx_type in ("buy", "sell") and mint:
            self.va.on_trade(mint, msg)

    async def _on_create(self, ws, msg: dict):
        mint       = msg.get("mint", "")
        symbol     = msg.get("symbol", "?")
        creator    = msg.get("traderPublicKey", "")
        created_at = msg.get("createdAt") or time.time()

        if not mint:
            return

        sig = self.va.on_create(mint, symbol, creator, float(created_at))
        self.n_created += 1

        log.debug("🆕 %s  %s  creator=%s", symbol, mint[:8], creator[:8])

        # Subscribe to trade feed for this token
        if mint not in self._subscribed:
            self._subscribed.add(mint)
            try:
                await ws.send(json.dumps({
                    "method": "subscribeTokenTrade",
                    "keys":   [mint],
                }))
            except Exception:
                pass

        # Background: cluster detection via Helius API
        if self.cd:
            asyncio.ensure_future(self._cluster_task(sig))
        else:
            # No Helius key — skip cluster signal requirement
            sig.cluster_ok = True

    async def _cluster_task(self, sig: TokenSignals):
        """Run WalletClusterDetector asynchronously for one token."""
        try:
            result = await self.cd.is_coordinated(
                creator=sig.creator,
                created_at=sig.created_at,
            )
            sig.cluster_ok = result
            if result:
                log.info("🔗 Cluster detected: %s", sig.symbol)
        except Exception as exc:
            log.debug("cluster_task error [%s]: %s", sig.symbol, exc)
            sig.cluster_ok = False  # safe default

    # ── Signal evaluation loop ────────────────────────────────────────────

    async def _signal_loop(self):
        """
        Runs every check_iv seconds.
        For each tracked token:
          - If in 12–35s window and all signals met → fire on_entry callback
          - If past 35s and not entered → mark expired
        """
        while self._running:
            try:
                await self._evaluate_all()
                self.va.cleanup(max_age_sec=self.entry_max + 5)
            except Exception as exc:
                log.debug("signal_loop error: %s", exc)
            await asyncio.sleep(self.check_iv)

    async def _evaluate_all(self):
        for mint, sig in list(self.va._tokens.items()):
            if sig.entered or sig.rejected:
                continue

            age = sig.age_sec

            if age < self.entry_min:
                continue  # too early

            if age > self.entry_max:
                sig.rejected = True
                sig.reject_reason = f"expired_{age:.0f}s"
                self.n_expired += 1
                log.debug("⏰ %s: expired (age=%.0fs) — %s",
                          sig.symbol, age, sig.summary())
                continue

            ok, reason = sig.all_signals_ok()
            if ok:
                sig.entered = True
                self.n_entered += 1
                log.info(
                    "🚀 ENTRY SIGNAL  %-10s  age=%.1fs  vel=%.2fSOL  "
                    "buyers=%d  sell_p=%.0f%%  cluster=%s  slot=%s",
                    sig.symbol, age,
                    sig.buy_vol_sol,
                    sig.unique_buyers,
                    sig.sell_pressure * 100,
                    sig.cluster_ok,
                    sig.slot_cluster_ok,
                )
                asyncio.ensure_future(self._fire_entry(mint, sig))
            else:
                log.debug("⏳ %s: waiting — blocked by [%s]  %s",
                          sig.symbol, reason, sig.summary())

    async def _fire_entry(self, mint: str, sig: TokenSignals):
        try:
            await self.on_entry(mint, sig)
        except Exception as exc:
            log.error("on_entry callback error [%s]: %s", sig.symbol, exc)

    # ── Status ────────────────────────────────────────────────────────────

    def status(self) -> dict:
        tracked = sum(
            1 for s in self.va._tokens.values()
            if not s.entered and not s.rejected
        )
        return {
            "created":  self.n_created,
            "entered":  self.n_entered,
            "rejected": self.n_rejected,
            "expired":  self.n_expired,
            "tracking": tracked,
            "wr_pct":   round(
                self.n_entered / max(self.n_created, 1) * 100, 1
            ),
        }
