"""
Solana Copy-Trading Bot
Monitors target wallets via Helius WebSocket and copies their trades via Jupiter.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import websockets

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── State ──────────────────────────────────────────────────────────────────────

class BotState:
    def __init__(self):
        self.daily_pnl: float = 0.0
        self.daily_reset_date: str = ""
        self.positions: dict[str, dict] = {}   # mint -> {buy_price, amount, buy_sol}
        self.processed_sigs: set[str] = set()  # avoid double-processing

state = BotState()


# ── Helius helpers ─────────────────────────────────────────────────────────────

async def get_transaction(sig: str, session: aiohttp.ClientSession) -> Optional[dict]:
    """Fetch parsed transaction from Helius enhanced API."""
    url = f"https://api.helius.xyz/v0/transactions/?api-key={config.HELIUS_API_KEY}"
    payload = {"transactions": [sig]}
    async with session.post(url, json=payload) as resp:
        if resp.status != 200:
            log.warning("Helius tx fetch failed: %s", await resp.text())
            return None
        data = await resp.json()
        return data[0] if data else None


async def get_sol_balance(wallet: str, session: aiohttp.ClientSession) -> float:
    """Get SOL balance via RPC."""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getBalance",
        "params": [wallet, {"commitment": "confirmed"}],
    }
    async with session.post(config.RPC_URL, json=payload) as resp:
        data = await resp.json()
        return data["result"]["value"] / 1e9


# ── Jupiter swap ───────────────────────────────────────────────────────────────

async def get_jupiter_quote(
    input_mint: str,
    output_mint: str,
    amount_lamports: int,
    session: aiohttp.ClientSession,
) -> Optional[dict]:
    """Get best route quote from Jupiter v6."""
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount_lamports,
        "slippageBps": config.SLIPPAGE_BPS,
    }
    async with session.get("https://quote-api.jup.ag/v6/quote", params=params) as resp:
        if resp.status != 200:
            log.warning("Jupiter quote failed: %s", await resp.text())
            return None
        return await resp.json()


async def execute_swap(quote: dict, session: aiohttp.ClientSession) -> Optional[str]:
    """
    Execute swap via Jupiter v6.
    Returns transaction signature or None on failure.
    NOTE: Requires our wallet's private key to sign.
    """
    if not config.OUR_PRIVATE_KEY:
        log.error("OUR_PRIVATE_KEY not set — cannot sign transaction")
        return None

    payload = {
        "quoteResponse": quote,
        "userPublicKey": config.OUR_WALLET,
        "wrapAndUnwrapSol": True,
        "prioritizationFeeLamports": config.PRIORITY_FEE_MICROLAMPORTS,
    }
    async with session.post("https://quote-api.jup.ag/v6/swap", json=payload) as resp:
        if resp.status != 200:
            log.warning("Jupiter swap failed: %s", await resp.text())
            return None
        data = await resp.json()

    # Sign and send transaction (requires solders/solana-py)
    # TODO: implement signing with config.OUR_PRIVATE_KEY
    swap_tx_b64 = data.get("swapTransaction")
    log.info("Got swap transaction (signing not yet implemented): %s...", swap_tx_b64[:40] if swap_tx_b64 else "None")
    return None  # placeholder


# ── Trade logic ────────────────────────────────────────────────────────────────

SOL_MINT = "So11111111111111111111111111111111111111112"


def is_active_hour(wallet_cfg: dict) -> bool:
    """Check if current UTC hour is within wallet's active hours."""
    current_hour = datetime.now(timezone.utc).hour
    return current_hour in wallet_cfg.get("active_hours_utc", list(range(24)))


def check_daily_reset():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state.daily_reset_date != today:
        state.daily_pnl = 0.0
        state.daily_reset_date = today
        log.info("Daily PnL reset for %s", today)


def calculate_our_trade_size(target_sol: float, our_balance: float, wallet_cfg: dict) -> float:
    """Calculate how much SOL we should trade."""
    # Scale relative to target's trade
    scaled = abs(target_sol) * config.COPY_SCALE_FACTOR

    # Cap at max per trade
    scaled = min(scaled, config.MAX_SOL_PER_TRADE)

    # Cap at max portfolio %
    scaled = min(scaled, our_balance * config.MAX_PORTFOLIO_PCT)

    # Ensure minimum
    if scaled < config.MIN_SOL_PER_TRADE:
        return 0.0

    return round(scaled, 6)


async def on_buy_detected(
    wallet: str,
    token_mint: str,
    target_sol_spent: float,
    session: aiohttp.ClientSession,
):
    """Called when target wallet buys a token."""
    wallet_cfg = config.TARGET_WALLETS[wallet]

    if not wallet_cfg.get("enabled"):
        return

    if not is_active_hour(wallet_cfg):
        log.debug("Outside active hours for %s, skipping buy", wallet_cfg["label"])
        return

    if token_mint in config.TOKEN_BLACKLIST:
        log.info("Token %s is blacklisted, skipping", token_mint[:8])
        return

    check_daily_reset()
    if state.daily_pnl < -config.DAILY_LOSS_LIMIT_SOL:
        log.warning("Daily loss limit reached (%.3f SOL), bot paused", state.daily_pnl)
        return

    our_balance = await get_sol_balance(config.OUR_WALLET, session)
    our_sol = calculate_our_trade_size(target_sol_spent, our_balance, wallet_cfg)

    if our_sol == 0:
        log.info("Trade size too small, skipping buy of %s", token_mint[:8])
        return

    log.info(
        "[BUY] %s bought %s | Target spent: %.3f SOL | We spend: %.3f SOL",
        wallet_cfg["label"], token_mint[:8], target_sol_spent, our_sol,
    )

    amount_lamports = int(our_sol * 1e9)
    quote = await get_jupiter_quote(SOL_MINT, token_mint, amount_lamports, session)
    if not quote:
        return

    sig = await execute_swap(quote, session)
    if sig:
        state.positions[token_mint] = {
            "buy_sol": our_sol,
            "buy_time": time.time(),
            "sig": sig,
        }
        log.info("Buy executed: %s", sig)


async def on_sell_detected(
    wallet: str,
    token_mint: str,
    session: aiohttp.ClientSession,
):
    """Called when target wallet sells a token."""
    wallet_cfg = config.TARGET_WALLETS[wallet]

    if not wallet_cfg.get("enabled"):
        return

    if token_mint not in state.positions:
        log.debug("No position in %s, skipping sell", token_mint[:8])
        return

    pos = state.positions[token_mint]
    log.info(
        "[SELL] %s sold %s | We sell our %.3f SOL position",
        wallet_cfg["label"], token_mint[:8], pos["buy_sol"],
    )

    # Get our token balance for this mint
    # TODO: fetch actual token balance via getTokenAccountsByOwner
    amount_tokens = 0  # placeholder

    if amount_tokens == 0:
        log.warning("No token balance found for %s, skipping sell", token_mint[:8])
        return

    quote = await get_jupiter_quote(token_mint, SOL_MINT, amount_tokens, session)
    if not quote:
        return

    sig = await execute_swap(quote, session)
    if sig:
        del state.positions[token_mint]
        log.info("Sell executed: %s", sig)


# ── Transaction parser ─────────────────────────────────────────────────────────

def parse_swap_from_helius_tx(tx: dict, watched_wallet: str) -> Optional[tuple[str, str, float]]:
    """
    Parse a Helius enhanced transaction to detect swaps by the watched wallet.
    Returns (action, token_mint, sol_amount) or None.
    action = "buy" or "sell"
    """
    if not tx:
        return None

    account_data = tx.get("accountData", [])
    token_transfers = tx.get("tokenTransfers", [])

    # Find SOL change for watched wallet
    sol_change = 0.0
    for acc in account_data:
        if acc.get("account") == watched_wallet:
            sol_change = acc.get("nativeBalanceChange", 0) / 1e9
            break

    # Find token involved
    token_mint = None
    for transfer in token_transfers:
        if transfer.get("fromUserAccount") == watched_wallet or \
           transfer.get("toUserAccount") == watched_wallet:
            token_mint = transfer.get("mint")
            break

    if not token_mint or sol_change == 0:
        return None

    if sol_change < -0.01:  # SOL went out -> bought token
        return ("buy", token_mint, abs(sol_change))
    elif sol_change > 0.01:  # SOL came in -> sold token
        return ("sell", token_mint, sol_change)

    return None


# ── WebSocket listener ─────────────────────────────────────────────────────────

async def watch_wallet(wallet: str, session: aiohttp.ClientSession):
    """Subscribe to wallet transactions via Helius WebSocket."""
    wallet_cfg = config.TARGET_WALLETS[wallet]
    log.info("Starting watcher for %s (%s)", wallet[:8], wallet_cfg["label"])

    while True:
        try:
            async with websockets.connect(config.WSS_URL) as ws:
                # Subscribe to account notifications
                sub_msg = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [wallet]},
                        {"commitment": "confirmed"},
                    ],
                }
                await ws.send(json.dumps(sub_msg))
                log.info("Subscribed to logs for %s", wallet[:8])

                async for raw in ws:
                    msg = json.loads(raw)

                    # Skip subscription confirmation
                    if "result" in msg:
                        continue

                    params = msg.get("params", {})
                    result = params.get("result", {})
                    value = result.get("value", {})
                    sig = value.get("signature")

                    if not sig or sig in state.processed_sigs:
                        continue

                    state.processed_sigs.add(sig)
                    # Keep processed set bounded
                    if len(state.processed_sigs) > 10000:
                        state.processed_sigs = set(list(state.processed_sigs)[-5000:])

                    # Fetch and parse the transaction
                    tx = await get_transaction(sig, session)
                    parsed = parse_swap_from_helius_tx(tx, wallet)

                    if parsed:
                        action, token_mint, sol_amount = parsed
                        if action == "buy":
                            await on_buy_detected(wallet, token_mint, sol_amount, session)
                        elif action == "sell":
                            await on_sell_detected(wallet, token_mint, session)

        except websockets.ConnectionClosed:
            log.warning("WebSocket closed for %s, reconnecting in 5s...", wallet[:8])
            await asyncio.sleep(5)
        except Exception as e:
            log.error("Error watching %s: %s", wallet[:8], e)
            await asyncio.sleep(10)


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    if not config.HELIUS_API_KEY:
        log.error("HELIUS_API_KEY not set. Copy .env.example to .env and fill it in.")
        return

    log.info("Starting Solana Copy-Trading Bot")
    log.info("Watching %d wallets", len(config.TARGET_WALLETS))

    async with aiohttp.ClientSession() as session:
        # Watch all target wallets concurrently
        tasks = [
            watch_wallet(wallet, session)
            for wallet, cfg in config.TARGET_WALLETS.items()
            if cfg.get("enabled")
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
