"""
Parser: collect token creation data + early buyers for each mint nya666 traded.
Uses Helius API to fetch:
 1. Token CREATE transaction (dev buy, metadata, timestamp)
 2. All transactions in first 60s (early buyers, buy pressure)
 3. Price action during nya666's holding period (BC state over time)

Run: python token_data_parser.py
Output: token_signals.json (one record per unique mint)
"""
import asyncio
import aiohttp
import json
import openpyxl
import time
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
import os

# ---- CONFIG ----
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")  # Set via env var
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_API = f"https://api.helius.xyz/v0"

WALLET = "nya666pQkP3PzWxi7JngU3rRMHuc7zbLK8c8wxQ4qpT"
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
# Путь к nya.xlsx — можно переопределить через env:
#   XLSX_FILE=/path/to/nya.xlsx python token_data_parser.py
XLSX_FILE = Path(os.getenv("XLSX_FILE", Path(__file__).parent / "nya.xlsx"))
OUTPUT_FILE = Path(os.getenv("OUTPUT_FILE", "token_signals.json"))

# Known smart money wallets (add more as discovered)
KNOWN_SMART_WALLETS = {
    "nya666pQkP3PzWxi7JngU3rRMHuc7zbLK8c8wxQ4qpT": "nya666",
    # Add other known profitable wallets here
}

# Rate limiting: Helius free tier = 10 req/s
CONCURRENCY = 5
RATE_LIMIT_DELAY = 0.15  # seconds between requests


# ---- STEP 1: Extract all mints + first buy timestamps from xlsx ----

def extract_nya666_mints() -> dict[str, dict]:
    """Returns {mint: {first_buy_ts, first_buy_sol, first_buy_bc_sol, all_buys}}"""
    if not XLSX_FILE.exists():
        print(f"ERROR: файл не найден: {XLSX_FILE.resolve()}")
        print(f"  Положи nya.xlsx рядом со скриптом или укажи путь:")
        print(f"  XLSX_FILE=/path/to/nya.xlsx python token_data_parser.py")
        return {}

    print(f"Читаю: {XLSX_FILE.resolve()}")
    wb = openpyxl.load_workbook(str(XLSX_FILE), read_only=True)
    ws = wb.active
    rows = list(ws.rows)
    headers = [c.value for c in rows[0]]
    total_rows = len(rows) - 1
    print(f"Строк в файле: {total_rows}")

    mints = defaultdict(lambda: {
        "first_buy_ts": None,
        "first_buy_sol": None,
        "first_buy_bc_sol": None,
        "all_buys": [],
    })

    for idx, row in enumerate(rows[1:], 1):
        if idx % 10000 == 0:
            print(f"  Обработано {idx}/{total_rows}...")

        d = {headers[j]: row[j].value for j in range(len(headers))}
        if not d.get("success"):
            continue
        if "Instruction: Buy" not in str(d.get("log_messages", "")):
            continue

        sc = _parse_json(d.get("sol_changes"))
        tc = _parse_json(d.get("token_changes"))

        nya_sc = next((x for x in sc if x["account"] == WALLET), None)
        nya_tc = next((x for x in tc if x.get("owner") == WALLET), None)
        if not nya_sc or not nya_tc:
            continue

        mint = nya_tc["mint"]
        sol_spent = abs(nya_sc.get("delta_sol", 0))
        ts = d["block_time"]

        # BC pre-sol = аккаунт получивший SOL (не nya666) с балансом > 0.005 SOL
        bc_candidates = [
            x for x in sc
            if x["account"] != WALLET and x.get("delta_sol", 0) > 0
            and x.get("pre_lamports", 0) > 5_000_000
        ]
        bc = max(bc_candidates, key=lambda x: x.get("delta_sol", 0)) if bc_candidates else None
        bc_pre_sol = bc["pre_lamports"] / 1e9 if bc else None

        mints[mint]["all_buys"].append({
            "ts": ts, "sol": sol_spent, "bc_pre_sol": bc_pre_sol,
            "signature": d.get("signature", "")
        })

        if mints[mint]["first_buy_ts"] is None or ts < mints[mint]["first_buy_ts"]:
            mints[mint]["first_buy_ts"] = ts
            mints[mint]["first_buy_sol"] = sol_spent
            mints[mint]["first_buy_bc_sol"] = bc_pre_sol

    wb.close()
    print(f"Найдено уникальных токенов: {len(mints)}")
    return dict(mints)


def _parse_json(val):
    if not val:
        return []
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return []
    return val


# ---- STEP 2: Fetch token creation data via Helius ----

async def fetch_token_creation(session: aiohttp.ClientSession, mint: str) -> dict | None:
    """
    Get the first transaction for a mint = creation transaction.
    Returns: {create_ts, dev_wallet, dev_buy_sol, has_metadata, metadata_uri}
    """
    url = f"{HELIUS_API}/addresses/{mint}/transactions"
    params = {
        "api-key": HELIUS_API_KEY,
        "limit": 5,
        "type": "SWAP",  # pump.fun creates are swaps
        "order": "asc",   # oldest first
    }
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 429:
                await asyncio.sleep(2)
                return None
            if resp.status != 200:
                return None
            data = await resp.json()
            if not data:
                return None

            first_tx = data[0]
            create_ts = first_tx.get("timestamp")
            fee_payer = first_tx.get("feePayer")

            # Extract dev buy amount from native transfers
            native_transfers = first_tx.get("nativeTransfers", [])
            dev_buy_sol = 0
            for t in native_transfers:
                if t.get("toUserAccount") and "pump" in str(t.get("toUserAccount", "")).lower():
                    dev_buy_sol += t.get("amount", 0) / 1e9

            # Token transfers to estimate
            token_transfers = first_tx.get("tokenTransfers", [])

            return {
                "create_ts": create_ts,
                "dev_wallet": fee_payer,
                "dev_buy_sol": dev_buy_sol,
                "first_tx_sig": first_tx.get("signature"),
                "n_token_transfers_at_create": len(token_transfers),
            }
    except Exception as e:
        return None


async def fetch_early_transactions(
    session: aiohttp.ClientSession,
    mint: str,
    create_ts: int,
    nya666_first_buy_ts: int,
    window_seconds: int = 60
) -> dict:
    """
    Get all transactions for mint in first `window_seconds` after creation.
    Returns: {n_buyers_total, n_buyers_before_nya666, unique_buyers, known_wallets_present, buy_pressure_sol}
    """
    url = f"{HELIUS_API}/addresses/{mint}/transactions"
    params = {
        "api-key": HELIUS_API_KEY,
        "limit": 100,
        "type": "SWAP",
        "order": "asc",
    }
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return {}
            txs = await resp.json()

        cutoff = create_ts + window_seconds
        early_txs = [t for t in txs if t.get("timestamp", 0) <= cutoff]

        buyers = set()
        known_wallets = []
        buy_pressure_sol = 0.0
        n_before_nya666 = 0

        for tx in early_txs:
            buyer = tx.get("feePayer")
            if not buyer:
                continue
            buyers.add(buyer)

            if buyer in KNOWN_SMART_WALLETS:
                known_wallets.append(KNOWN_SMART_WALLETS[buyer])

            # SOL spent
            for nt in tx.get("nativeTransfers", []):
                if nt.get("fromUserAccount") == buyer:
                    buy_pressure_sol += nt.get("amount", 0) / 1e9

            if tx.get("timestamp", 0) < nya666_first_buy_ts:
                n_before_nya666 += 1

        return {
            "n_buyers_in_window": len(buyers),
            "n_buyers_before_nya666": n_before_nya666,
            "unique_buyer_wallets": list(buyers)[:20],  # sample
            "known_wallets_present": known_wallets,
            "buy_pressure_sol_in_window": round(buy_pressure_sol, 4),
            "n_early_txs": len(early_txs),
        }
    except Exception:
        return {}


async def fetch_token_metadata(session: aiohttp.ClientSession, mint: str) -> dict:
    """Check if token has valid metadata (website, twitter, telegram)."""
    url = f"{HELIUS_API}/token-metadata"
    params = {"api-key": HELIUS_API_KEY}
    payload = {"mintAccounts": [mint], "includeOffChain": True, "disableCache": False}
    try:
        async with session.post(url, params=params, json=payload,
                                timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            if not data:
                return {}
            item = data[0]
            off_chain = item.get("offChainMetadata", {}) or {}
            meta = off_chain.get("metadata", {}) or {}
            ext = meta.get("extensions", {}) or {}

            return {
                "has_website": bool(ext.get("website")),
                "has_twitter": bool(ext.get("twitter")),
                "has_telegram": bool(ext.get("telegram")),
                "description": meta.get("description", "")[:100],
                "name": meta.get("name", ""),
                "symbol": meta.get("symbol", ""),
            }
    except Exception:
        return {}


# ---- STEP 3: Fetch BC state over time (price action during hold) ----

async def fetch_bc_timeline(
    session: aiohttp.ClientSession,
    mint: str,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    """
    Get BC SOL values at each buy/sell event during nya666's holding period.
    Returns list of {ts, bc_sol, side (buy/sell), buyer}
    """
    url = f"{HELIUS_API}/addresses/{mint}/transactions"
    params = {
        "api-key": HELIUS_API_KEY,
        "limit": 100,
        "type": "SWAP",
        "order": "asc",
    }
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return []
            txs = await resp.json()

        timeline = []
        for tx in txs:
            ts = tx.get("timestamp", 0)
            if ts < start_ts or ts > end_ts:
                continue
            timeline.append({
                "ts": ts,
                "buyer": tx.get("feePayer"),
                "type": tx.get("type"),
            })
        return timeline
    except Exception:
        return []


# ---- MAIN PIPELINE ----

async def process_mint(
    session: aiohttp.ClientSession,
    mint: str,
    mint_data: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    async with semaphore:
        await asyncio.sleep(RATE_LIMIT_DELAY)

        first_buy_ts = mint_data["first_buy_ts"]
        result = {
            "mint": mint,
            "nya666_first_buy_ts": first_buy_ts,
            "nya666_first_buy_sol": mint_data["first_buy_sol"],
            "nya666_first_buy_bc_sol": mint_data["first_buy_bc_sol"],
            "nya666_n_buys": len(mint_data["all_buys"]),
            "nya666_total_invested_sol": sum(b["sol"] for b in mint_data["all_buys"]),
        }

        # 1. Creation data
        creation = await fetch_token_creation(session, mint)
        if creation:
            result.update(creation)
            create_ts = creation.get("create_ts", 0)
            if create_ts and first_buy_ts:
                result["seconds_after_creation"] = first_buy_ts - create_ts
        else:
            create_ts = None

        # 2. Early buyers (if we have creation timestamp)
        if create_ts:
            await asyncio.sleep(RATE_LIMIT_DELAY)
            early = await fetch_early_transactions(
                session, mint, create_ts, first_buy_ts, window_seconds=60
            )
            result.update(early)

        # 3. Metadata
        await asyncio.sleep(RATE_LIMIT_DELAY)
        meta = await fetch_token_metadata(session, mint)
        result.update(meta)

        return result


async def main():
    if not HELIUS_API_KEY:
        print("ERROR: Set HELIUS_API_KEY environment variable")
        print("  export HELIUS_API_KEY=your_key_here")
        return

    # Load already processed mints
    existing = {}
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            for item in json.load(f):
                existing[item["mint"]] = item
        print(f"Loaded {len(existing)} already processed mints")

    # Extract mints from xlsx
    all_mints = extract_nya666_mints()

    # Filter unprocessed
    to_process = {m: d for m, d in all_mints.items() if m not in existing}
    print(f"Mints to process: {len(to_process)} (skipping {len(existing)} already done)")

    if not to_process:
        print("Nothing to process. Analyzing existing data...")
        analyze_signals(list(existing.values()))
        return

    # Process in batches
    semaphore = asyncio.Semaphore(CONCURRENCY)
    results = list(existing.values())

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            process_mint(session, mint, data, semaphore)
            for mint, data in to_process.items()
        ]

        done = 0
        batch_size = 50
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i+batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)

            for r in batch_results:
                if isinstance(r, dict):
                    results.append(r)
                    existing[r["mint"]] = r

            done += len(batch)
            print(f"Progress: {done}/{len(tasks)} ({done/len(tasks)*100:.1f}%)")

            # Save checkpoint
            with open(OUTPUT_FILE, "w") as f:
                json.dump(results, f, indent=2, default=str)

    print(f"\nSaved {len(results)} records to {OUTPUT_FILE}")
    analyze_signals(results)


def analyze_signals(records: list[dict]):
    """Analyze collected signals to find selection patterns."""
    print("\n" + "="*60)
    print("SIGNAL ANALYSIS")
    print("="*60)

    valid = [r for r in records if r.get("seconds_after_creation") is not None]
    print(f"\nRecords with creation data: {len(valid)}")

    if not valid:
        print("No data with creation timestamps yet.")
        return

    import numpy as np

    # Time from creation to nya666's first buy
    deltas = [r["seconds_after_creation"] for r in valid]
    print(f"\n[SECONDS FROM TOKEN CREATION TO nya666 FIRST BUY]:")
    print(f"  Median: {np.median(deltas):.0f}s")
    print(f"  P10: {np.percentile(deltas, 10):.0f}s")
    print(f"  P90: {np.percentile(deltas, 90):.0f}s")
    print(f"  <10s: {sum(1 for d in deltas if d < 10)}")
    print(f"  10-60s: {sum(1 for d in deltas if 10 <= d < 60)}")
    print(f"  >60s: {sum(1 for d in deltas if d >= 60)}")

    # Dev buy amount
    dev_buys = [r.get("dev_buy_sol", 0) for r in valid if r.get("dev_buy_sol")]
    if dev_buys:
        print(f"\n[DEV BUY AT CREATION]:")
        print(f"  Median: {np.median(dev_buys):.4f} SOL")
        print(f"  >0.5 SOL: {sum(1 for d in dev_buys if d > 0.5)}")
        print(f"  >1.0 SOL: {sum(1 for d in dev_buys if d > 1.0)}")

    # Known wallets
    all_known = [r for r in valid if r.get("known_wallets_present")]
    print(f"\n[KNOWN WALLETS PRESENT IN FIRST 60s]: {len(all_known)} tokens")

    # Metadata
    with_meta = [r for r in valid if r.get("has_website") or r.get("has_twitter")]
    print(f"\n[TOKENS WITH METADATA]: {len(with_meta)} / {len(valid)} ({len(with_meta)/len(valid)*100:.1f}%)")

    # Position size vs signals
    print(f"\n[BUY SIZE vs ENTRY SPEED]:")
    small_buys = [r for r in valid if r.get("nya666_first_buy_sol", 0) < 0.15]
    large_buys = [r for r in valid if r.get("nya666_first_buy_sol", 0) >= 0.40]

    if small_buys and large_buys:
        small_deltas = [r["seconds_after_creation"] for r in small_buys]
        large_deltas = [r["seconds_after_creation"] for r in large_buys]
        print(f"  Small buys (<0.15 SOL) median entry: {np.median(small_deltas):.0f}s after creation")
        print(f"  Large buys (>=0.40 SOL) median entry: {np.median(large_deltas):.0f}s after creation")

        small_buyers = [r.get("n_buyers_before_nya666", 0) for r in small_buys if r.get("n_buyers_before_nya666") is not None]
        large_buyers = [r.get("n_buyers_before_nya666", 0) for r in large_buys if r.get("n_buyers_before_nya666") is not None]
        if small_buyers and large_buyers:
            print(f"  Small buys: median {np.median(small_buyers):.0f} buyers before nya666")
            print(f"  Large buys: median {np.median(large_buyers):.0f} buyers before nya666")


if __name__ == "__main__":
    asyncio.run(main())
