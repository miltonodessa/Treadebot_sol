"""
Multi-wallet parser: анализирует паттерны покупок для нескольких кошельков.

Читает все *_full_report.xlsx и nya.xlsx из папки скрипта.
Wallet address определяется автоматически из колонки wallet_address.

Output:
  token_signals.json  — один record на каждый уникальный mint
  wallet_stats.json   — сводная статистика по каждому кошельку

Run:
  python token_data_parser.py              # обычный запуск
  python token_data_parser.py --force      # сброс кэша, с нуля
  python token_data_parser.py --no-api     # только парсить xlsx, без Helius
"""
import asyncio
import aiohttp
import json
import openpyxl
import sys
from pathlib import Path
from collections import defaultdict
import os

# ---- CONFIG ----
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "23df9945-a693-4412-95f5-f47ce65b3e4d")
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_API = "https://api-mainnet.helius-rpc.com/v0"

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Папка с xlsx файлами (рядом со скриптом)
DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).parent))
OUTPUT_FILE = Path(os.getenv("OUTPUT_FILE", "token_signals.json"))
WALLET_STATS_FILE = Path(os.getenv("WALLET_STATS_FILE", "wallet_stats.json"))

CONCURRENCY = 5
RATE_LIMIT_DELAY = 0.15


# ---- STEP 1: Парсинг xlsx ----

def find_xlsx_files() -> list[Path]:
    """Находит все xlsx файлы с транзакциями в DATA_DIR."""
    files = []
    # Приоритет: nya.xlsx
    nya = DATA_DIR / "nya.xlsx"
    if nya.exists():
        files.append(nya)
    # Все *_full_report.xlsx
    for f in sorted(DATA_DIR.glob("*_full_report.xlsx")):
        files.append(f)
    # Любые другие xlsx (part_*.xlsx и т.п.)
    for f in sorted(DATA_DIR.glob("part_*.xlsx")):
        files.append(f)
    return files


def _parse_json(val):
    if not val:
        return []
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return []
    return []


def parse_xlsx_file(path: Path, wallet_hint: str | None = None) -> tuple[str, dict]:
    """
    Парсит один xlsx файл.
    Возвращает (wallet_address, {mint: mint_data})
    mint_data = {first_buy_ts, first_buy_sol, first_buy_bc_sol, all_buys, all_sells}
    """
    wb = openpyxl.load_workbook(str(path), read_only=True)
    ws = wb.active
    rows = list(ws.rows)
    headers = [c.value for c in rows[0]]
    total = len(rows) - 1
    print(f"  {path.name}: {total} строк", end="", flush=True)

    # Определяем кошелёк
    wallet = wallet_hint
    if not wallet and "wallet_address" in headers:
        for row in rows[1:]:
            d = {headers[j]: row[j].value for j in range(len(headers))}
            w = d.get("wallet_address")
            if w:
                wallet = w
                break

    if not wallet:
        print(" — не найден wallet_address, пропускаю")
        wb.close()
        return None, {}

    print(f" | wallet: {wallet[:8]}...")

    mints = defaultdict(lambda: {
        "first_buy_ts": None,
        "first_buy_sol": None,
        "first_buy_bc_sol": None,
        "all_buys": [],
        "all_sells": [],
        "wallet": wallet,
    })

    buy_count = 0
    sell_count = 0

    for idx, row in enumerate(rows[1:], 1):
        if idx % 20000 == 0:
            print(f"    {idx}/{total}...")

        d = {headers[j]: row[j].value for j in range(len(headers))}
        if not d.get("success"):
            continue

        log = str(d.get("log_messages", ""))
        is_buy = "Instruction: Buy" in log
        is_sell = "Instruction: Sell" in log
        if not is_buy and not is_sell:
            continue

        sc = _parse_json(d.get("sol_changes"))
        tc = _parse_json(d.get("token_changes"))
        ts = d.get("block_time")

        wallet_sc = next((x for x in sc if x.get("account") == wallet), None)
        wallet_tc = next((x for x in tc if x.get("owner") == wallet), None)
        if not wallet_sc or not wallet_tc:
            continue

        mint = wallet_tc.get("mint")
        if not mint:
            continue

        sol_delta = wallet_sc.get("delta_sol", 0)

        # BC = bonding curve account (получает SOL при покупке)
        bc_candidates = [
            x for x in sc
            if x.get("account") != wallet and x.get("delta_sol", 0) > 0
            and x.get("pre_lamports", 0) > 5_000_000
        ]
        bc = max(bc_candidates, key=lambda x: x.get("delta_sol", 0)) if bc_candidates else None
        bc_pre_sol = bc["pre_lamports"] / 1e9 if bc else None
        bc_post_sol = (bc["pre_lamports"] + bc.get("delta_lamports", 0)) / 1e9 if bc else None

        entry = {
            "ts": ts,
            "sol": abs(sol_delta),
            "bc_pre_sol": bc_pre_sol,
            "bc_post_sol": bc_post_sol,
            "signature": d.get("signature", ""),
        }

        if is_buy:
            buy_count += 1
            mints[mint]["all_buys"].append(entry)
            if mints[mint]["first_buy_ts"] is None or (ts and ts < mints[mint]["first_buy_ts"]):
                mints[mint]["first_buy_ts"] = ts
                mints[mint]["first_buy_sol"] = abs(sol_delta)
                mints[mint]["first_buy_bc_sol"] = bc_pre_sol
        elif is_sell:
            sell_count += 1
            mints[mint]["all_sells"].append(entry)

    wb.close()
    print(f"    → {len(mints)} токенов, {buy_count} покупок, {sell_count} продаж")
    return wallet, dict(mints)


def extract_all_wallets() -> dict[str, dict]:
    """
    Читает все xlsx файлы.
    Возвращает {wallet_address: {mint: mint_data}}
    """
    files = find_xlsx_files()
    if not files:
        print(f"ERROR: нет xlsx файлов в {DATA_DIR.resolve()}")
        print("  Положи *_full_report.xlsx или nya.xlsx рядом со скриптом")
        return {}

    print(f"\nНайдено файлов: {len(files)}")
    wallets: dict[str, dict] = {}  # wallet -> {mint -> data}

    for path in files:
        wallet, mints = parse_xlsx_file(path)
        if not wallet:
            continue
        if wallet not in wallets:
            wallets[wallet] = {}
        # Мёржим данные (если один кошелёк разбит на несколько файлов)
        for mint, data in mints.items():
            if mint not in wallets[wallet]:
                wallets[wallet][mint] = data
            else:
                existing = wallets[wallet][mint]
                existing["all_buys"].extend(data["all_buys"])
                existing["all_sells"].extend(data["all_sells"])
                if data["first_buy_ts"] and (
                    existing["first_buy_ts"] is None
                    or data["first_buy_ts"] < existing["first_buy_ts"]
                ):
                    existing["first_buy_ts"] = data["first_buy_ts"]
                    existing["first_buy_sol"] = data["first_buy_sol"]
                    existing["first_buy_bc_sol"] = data["first_buy_bc_sol"]

    print(f"\nИтого кошельков: {len(wallets)}")
    for w, mints in wallets.items():
        total_buys = sum(len(m["all_buys"]) for m in mints.values())
        total_sells = sum(len(m["all_sells"]) for m in mints.values())
        print(f"  {w[:8]}...{w[-4:]}: {len(mints)} токенов, {total_buys} покупок, {total_sells} продаж")

    return wallets


def find_co_buys(wallets: dict[str, dict]) -> dict[str, list[str]]:
    """Находит токены купленные несколькими кошельками одновременно."""
    mint_wallets: dict[str, list[str]] = defaultdict(list)
    for wallet, mints in wallets.items():
        for mint in mints:
            mint_wallets[mint].append(wallet)
    return {m: ws for m, ws in mint_wallets.items() if len(ws) > 1}


# ---- STEP 2: Helius API ----

async def fetch_token_creation(session: aiohttp.ClientSession, mint: str) -> dict | None:
    url = f"{HELIUS_API}/addresses/{mint}/transactions"
    params = {"api-key": HELIUS_API_KEY, "limit": 5, "type": "SWAP", "order": "asc"}
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
            native_transfers = first_tx.get("nativeTransfers", [])
            dev_buy_sol = sum(
                t.get("amount", 0) / 1e9 for t in native_transfers
                if t.get("toUserAccount") and "pump" in str(t.get("toUserAccount", "")).lower()
            )
            return {
                "create_ts": first_tx.get("timestamp"),
                "dev_wallet": first_tx.get("feePayer"),
                "dev_buy_sol": dev_buy_sol,
                "first_tx_sig": first_tx.get("signature"),
                "n_token_transfers_at_create": len(first_tx.get("tokenTransfers", [])),
            }
    except Exception:
        return None


async def fetch_early_buyers(
    session: aiohttp.ClientSession,
    mint: str,
    create_ts: int,
    wallet_first_buy_ts: int,
    known_wallets: dict[str, str],
    window_seconds: int = 60,
) -> dict:
    url = f"{HELIUS_API}/addresses/{mint}/transactions"
    params = {"api-key": HELIUS_API_KEY, "limit": 100, "type": "SWAP", "order": "asc"}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return {}
            txs = await resp.json()

        cutoff = create_ts + window_seconds
        early_txs = [t for t in txs if t.get("timestamp", 0) <= cutoff]

        buyers = set()
        known_present = []
        buy_pressure_sol = 0.0
        n_before_wallet = 0

        for tx in early_txs:
            buyer = tx.get("feePayer")
            if not buyer:
                continue
            buyers.add(buyer)
            if buyer in known_wallets:
                known_present.append(known_wallets[buyer])
            for nt in tx.get("nativeTransfers", []):
                if nt.get("fromUserAccount") == buyer:
                    buy_pressure_sol += nt.get("amount", 0) / 1e9
            if tx.get("timestamp", 0) < wallet_first_buy_ts:
                n_before_wallet += 1

        return {
            "n_buyers_in_60s": len(buyers),
            "n_buyers_before_wallet": n_before_wallet,
            "known_wallets_present": known_present,
            "buy_pressure_sol_60s": round(buy_pressure_sol, 4),
        }
    except Exception:
        return {}


async def fetch_token_metadata(session: aiohttp.ClientSession, mint: str) -> dict:
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
                "name": meta.get("name", ""),
                "symbol": meta.get("symbol", ""),
                "description": (meta.get("description") or "")[:100],
            }
    except Exception:
        return {}


# ---- STEP 3: Pipeline ----

async def process_mint(
    session: aiohttp.ClientSession,
    mint: str,
    mint_data: dict,
    wallet: str,
    wallet_label: str,
    known_wallets: dict[str, str],
    co_buy_wallets: list[str],
    semaphore: asyncio.Semaphore,
) -> dict:
    async with semaphore:
        await asyncio.sleep(RATE_LIMIT_DELAY)

        first_buy_ts = mint_data["first_buy_ts"]
        all_buys = mint_data["all_buys"]
        all_sells = mint_data["all_sells"]

        # Hold time
        hold_seconds = None
        if all_buys and all_sells:
            first_sell_ts = min(s["ts"] for s in all_sells if s.get("ts"))
            if first_buy_ts and first_sell_ts:
                hold_seconds = first_sell_ts - first_buy_ts

        # PnL estimate (SOL in vs SOL out)
        sol_in = sum(b["sol"] for b in all_buys)
        sol_out = sum(s["sol"] for s in all_sells)

        result = {
            "mint": mint,
            "wallet": wallet,
            "wallet_label": wallet_label,
            # Entry
            "first_buy_ts": first_buy_ts,
            "first_buy_sol": mint_data["first_buy_sol"],
            "first_buy_bc_sol": mint_data["first_buy_bc_sol"],
            "n_buys": len(all_buys),
            "total_sol_in": round(sol_in, 6),
            # Exit
            "n_sells": len(all_sells),
            "total_sol_out": round(sol_out, 6),
            "hold_seconds": hold_seconds,
            "estimated_pnl_sol": round(sol_out - sol_in, 6),
            # Co-buy signal
            "co_buy_wallets": [w for w in co_buy_wallets if w != wallet],
            "is_co_buy": len(co_buy_wallets) > 1,
        }

        # 1. Token creation
        creation = await fetch_token_creation(session, mint)
        if creation:
            result.update(creation)
            if creation.get("create_ts") and first_buy_ts:
                result["seconds_after_creation"] = first_buy_ts - creation["create_ts"]
        create_ts = (creation or {}).get("create_ts")

        # 2. Early buyers
        if create_ts and first_buy_ts:
            await asyncio.sleep(RATE_LIMIT_DELAY)
            early = await fetch_early_buyers(
                session, mint, create_ts, first_buy_ts, known_wallets
            )
            result.update(early)

        # 3. Metadata
        await asyncio.sleep(RATE_LIMIT_DELAY)
        meta = await fetch_token_metadata(session, mint)
        result.update(meta)

        return result


# ---- STEP 4: Analysis ----

def analyze_wallet(wallet: str, label: str, records: list[dict]):
    """Детальная статистика по одному кошельку."""
    print(f"\n{'='*60}")
    print(f"КОШЕЛЁК: {label}")
    print(f"  {wallet}")
    print(f"{'='*60}")
    print(f"Токенов всего: {len(records)}")

    valid = [r for r in records if r.get("seconds_after_creation") is not None]
    print(f"С данными о создании: {len(valid)}")
    if not valid:
        return

    try:
        import numpy as np
    except ImportError:
        print("Установи numpy: pip install numpy")
        return

    # Hold time
    holds = [r["hold_seconds"] for r in records if r.get("hold_seconds") is not None]
    if holds:
        print(f"\n[ВРЕМЯ УДЕРЖАНИЯ]")
        print(f"  Медиана:  {np.median(holds)/60:.1f} мин")
        print(f"  P10:      {np.percentile(holds, 10)/60:.1f} мин")
        print(f"  P90:      {np.percentile(holds, 90)/60:.1f} мин")
        print(f"  <5 мин:   {sum(1 for h in holds if h < 300)}")
        print(f"  5-30 мин: {sum(1 for h in holds if 300 <= h < 1800)}")
        print(f"  >30 мин:  {sum(1 for h in holds if h >= 1800)}")

    # Entry speed
    deltas = [r["seconds_after_creation"] for r in valid]
    print(f"\n[СКОРОСТЬ ВХОДА (сек после создания токена)]")
    print(f"  Медиана: {np.median(deltas):.0f}s")
    print(f"  P10:     {np.percentile(deltas, 10):.0f}s")
    print(f"  P90:     {np.percentile(deltas, 90):.0f}s")
    print(f"  <10s:    {sum(1 for d in deltas if d < 10)}")
    print(f"  10-60s:  {sum(1 for d in deltas if 10 <= d < 60)}")
    print(f"  >60s:    {sum(1 for d in deltas if d >= 60)}")

    # Position sizes
    buy_sizes = [r["first_buy_sol"] for r in records if r.get("first_buy_sol")]
    if buy_sizes:
        print(f"\n[РАЗМЕР ПЕРВОЙ ПОКУПКИ (SOL)]")
        print(f"  Медиана: {np.median(buy_sizes):.4f}")
        print(f"  P10:     {np.percentile(buy_sizes, 10):.4f}")
        print(f"  P90:     {np.percentile(buy_sizes, 90):.4f}")
        print(f"  <0.1:    {sum(1 for x in buy_sizes if x < 0.1)}")
        print(f"  0.1-0.3: {sum(1 for x in buy_sizes if 0.1 <= x < 0.3)}")
        print(f"  >0.3:    {sum(1 for x in buy_sizes if x >= 0.3)}")

    # BC at entry
    bc_entries = [r["first_buy_bc_sol"] for r in valid if r.get("first_buy_bc_sol")]
    if bc_entries:
        print(f"\n[BC SOL ПРИ ВХОДЕ (размер пула)]")
        print(f"  Медиана: {np.median(bc_entries):.2f} SOL")
        print(f"  <10 SOL: {sum(1 for x in bc_entries if x < 10)}")
        print(f"  10-30:   {sum(1 for x in bc_entries if 10 <= x < 30)}")
        print(f"  >30:     {sum(1 for x in bc_entries if x >= 30)}")

    # PnL
    pnls = [r["estimated_pnl_sol"] for r in records if r.get("n_sells", 0) > 0]
    if pnls:
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        print(f"\n[PnL (только закрытые позиции, {len(pnls)} шт)]")
        print(f"  Win rate:    {len(wins)/len(pnls)*100:.1f}%")
        print(f"  Avg win:     +{np.mean(wins):.4f} SOL" if wins else "  Avg win: -")
        print(f"  Avg loss:    {np.mean(losses):.4f} SOL" if losses else "  Avg loss: -")
        print(f"  Total PnL:   {sum(pnls):+.4f} SOL")

    # Dev buy
    dev_buys = [r["dev_buy_sol"] for r in valid if r.get("dev_buy_sol")]
    if dev_buys:
        print(f"\n[DEV BUY ПРИ СОЗДАНИИ]")
        print(f"  Медиана:  {np.median(dev_buys):.4f} SOL")
        print(f"  >0.5 SOL: {sum(1 for d in dev_buys if d > 0.5)}")

    # Co-buys
    co_buys = [r for r in records if r.get("is_co_buy")]
    if co_buys:
        print(f"\n[CO-BUY с другими отслеживаемыми кошельками]: {len(co_buys)} токенов")


def analyze_all(records: list[dict], wallets: dict[str, str]):
    """Общий анализ всех кошельков + cross-wallet."""
    by_wallet: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        by_wallet[r["wallet"]].append(r)

    for wallet, wallet_records in by_wallet.items():
        label = wallets.get(wallet, wallet[:8])
        analyze_wallet(wallet, label, wallet_records)

    # Co-buy анализ
    co_buy_records = [r for r in records if r.get("is_co_buy")]
    if co_buy_records:
        print(f"\n{'='*60}")
        print(f"CO-BUY СИГНАЛЫ (куплено несколькими кошельками)")
        print(f"{'='*60}")
        mints_seen = set()
        for r in co_buy_records:
            mint = r["mint"]
            if mint not in mints_seen:
                mints_seen.add(mint)
                wallets_str = ", ".join([r["wallet_label"]] + r.get("co_buy_wallets", []))
                sec = r.get("seconds_after_creation", "?")
                print(f"  {mint[:8]}... | {wallets_str} | entry: {sec}s after create")
        print(f"\nВсего co-buy токенов: {len(mints_seen)}")


# ---- MAIN ----

async def main():
    force = "--force" in sys.argv
    no_api = "--no-api" in sys.argv

    # Загружаем кэш
    existing: dict[str, dict] = {}  # key = f"{wallet}:{mint}"
    if OUTPUT_FILE.exists() and not force:
        with open(OUTPUT_FILE) as f:
            for item in json.load(f):
                key = f"{item.get('wallet', '')}:{item['mint']}"
                existing[key] = item
        print(f"Кэш: {len(existing)} записей (--force для сброса)")
    elif force:
        print("--force: начинаю с нуля")

    # Парсим xlsx
    all_wallets = extract_all_wallets()
    if not all_wallets:
        return

    # Ярлыки для кошельков
    wallet_labels: dict[str, str] = {}
    for i, w in enumerate(all_wallets.keys(), 1):
        wallet_labels[w] = f"wallet_{i}_{w[:6]}"

    # Co-buy индекс
    co_buy_index = find_co_buys(all_wallets)
    co_buy_mints = set(co_buy_index.keys())
    if co_buy_mints:
        print(f"\nCo-buy токенов (куплены несколькими кошельками): {len(co_buy_mints)}")

    if no_api:
        print("\n--no-api: пропускаю Helius запросы, только xlsx данные")
        # Быстрый локальный анализ без API
        records = []
        for wallet, mints in all_wallets.items():
            label = wallet_labels[wallet]
            for mint, data in mints.items():
                all_buys = data["all_buys"]
                all_sells = data["all_sells"]
                sol_in = sum(b["sol"] for b in all_buys)
                sol_out = sum(s["sol"] for s in all_sells)
                hold_seconds = None
                if all_buys and all_sells:
                    first_sell = min((s["ts"] for s in all_sells if s.get("ts")), default=None)
                    if data["first_buy_ts"] and first_sell:
                        hold_seconds = first_sell - data["first_buy_ts"]
                co_wallets = co_buy_index.get(mint, [mint not in co_buy_index and wallet])
                records.append({
                    "mint": mint,
                    "wallet": wallet,
                    "wallet_label": label,
                    "first_buy_ts": data["first_buy_ts"],
                    "first_buy_sol": data["first_buy_sol"],
                    "first_buy_bc_sol": data["first_buy_bc_sol"],
                    "n_buys": len(all_buys),
                    "total_sol_in": round(sol_in, 6),
                    "n_sells": len(all_sells),
                    "total_sol_out": round(sol_out, 6),
                    "hold_seconds": hold_seconds,
                    "estimated_pnl_sol": round(sol_out - sol_in, 6),
                    "co_buy_wallets": co_buy_index.get(mint, []),
                    "is_co_buy": mint in co_buy_mints,
                })
        with open(OUTPUT_FILE, "w") as f:
            json.dump(records, f, indent=2, default=str)
        analyze_all(records, wallet_labels)
        return

    # API обработка
    if not HELIUS_API_KEY:
        print("ERROR: задай HELIUS_API_KEY")
        return

    # Собираем задачи (пропускаем уже обработанные)
    tasks_args = []
    for wallet, mints in all_wallets.items():
        label = wallet_labels[wallet]
        for mint, data in mints.items():
            key = f"{wallet}:{mint}"
            if key not in existing:
                co_wallets = co_buy_index.get(mint, [wallet])
                tasks_args.append((mint, data, wallet, label, co_wallets))

    print(f"\nК обработке: {len(tasks_args)} (пропускаю {len(existing)} из кэша)")

    results = list(existing.values())
    semaphore = asyncio.Semaphore(CONCURRENCY)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        # known_wallets для передачи в fetch_early_buyers
        known = {w: wallet_labels[w] for w in all_wallets.keys()}

        tasks = [
            process_mint(session, mint, data, wallet, label, known, co_wallets, semaphore)
            for mint, data, wallet, label, co_wallets in tasks_args
        ]

        batch_size = 50
        done = 0
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, dict):
                    results.append(r)
                    key = f"{r['wallet']}:{r['mint']}"
                    existing[key] = r
            done += len(batch)
            pct = done / len(tasks) * 100
            print(f"  {done}/{len(tasks)} ({pct:.1f}%)")
            with open(OUTPUT_FILE, "w") as f:
                json.dump(results, f, indent=2, default=str)

    print(f"\nСохранено {len(results)} записей → {OUTPUT_FILE}")
    analyze_all(results, wallet_labels)


if __name__ == "__main__":
    asyncio.run(main())
