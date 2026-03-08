"""
Deep analysis of nya666 bot trading patterns.
Extracts: bonding curve state, position sizing tiers, hold logic, selection signals.
"""
import openpyxl
import json
import numpy as np
from collections import defaultdict, Counter
from pathlib import Path

WALLET = 'nya666pQkP3PzWxi7JngU3rRMHuc7zbLK8c8wxQ4qpT'
DATA_DIR = Path('/tmp')


def parse_json(val):
    if not val:
        return []
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return []
    return val


def extract_buys_from_file(path: str, limit=5000) -> list[dict]:
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    rows = list(ws.rows)
    headers = [c.value for c in rows[0]]

    buys = []
    for row in rows[1:limit+1]:
        d = {headers[j]: row[j].value for j in range(len(headers))}
        if not d.get('success'):
            continue
        if 'Instruction: Buy' not in str(d.get('log_messages', '')):
            continue

        sc = parse_json(d.get('sol_changes'))
        tc = parse_json(d.get('token_changes'))
        accs = parse_json(d.get('accounts'))

        # nya666 SOL spent
        nya_sc = next((x for x in sc if x['account'] == WALLET), None)
        if not nya_sc:
            continue
        sol_spent = abs(nya_sc.get('delta_sol', 0))
        if sol_spent < 0.001:
            continue

        # Token mint from nya666's token change
        nya_tc = next((x for x in tc if x.get('owner') == WALLET), None)
        if not nya_tc:
            continue
        mint = nya_tc['mint']
        tokens_received = nya_tc.get('delta_amount', 0) or 0

        # Bonding curve = account that RECEIVED SOL and is NOT nya666
        bc_candidates = [
            x for x in sc
            if x['account'] != WALLET
            and x.get('delta_sol', 0) > 0
            and x.get('pre_lamports', 0) > 5_000_000  # > 0.005 SOL existing balance
        ]

        bc = max(bc_candidates, key=lambda x: x.get('delta_sol', 0)) if bc_candidates else None

        # SOL in bonding curve BEFORE this buy = market cap proxy
        bc_pre_sol = bc['pre_lamports'] / 1e9 if bc else None
        bc_post_sol = bc['post_lamports'] / 1e9 if bc else None

        # Count total accounts - more accounts = more complex tx (more early buyers?)
        n_accounts = len(accs)

        buys.append({
            'ts': d['block_time'],
            'signature': d.get('signature', ''),
            'mint': mint,
            'sol_spent': round(sol_spent, 4),
            'tokens_received': tokens_received,
            'bc_pre_sol': bc_pre_sol,   # SOL in curve before buy = market cap proxy
            'bc_post_sol': bc_post_sol,
            'bc_account': bc['account'] if bc else None,
            'n_accounts': n_accounts,
        })

    wb.close()
    return buys


def analyze_patterns(buys: list[dict]):
    print(f"\n{'='*60}")
    print(f"TOTAL BUYS ANALYZED: {len(buys)}")
    print(f"{'='*60}")

    # 1. Position size tiers
    sizes = [b['sol_spent'] for b in buys]
    print("\n[1] POSITION SIZE TIERS:")
    tier_counts = Counter(round(s, 1) for s in sizes)
    for size, cnt in sorted(tier_counts.items()):
        pct = cnt / len(sizes) * 100
        bar = '#' * int(pct)
        print(f"  ~{size:.1f} SOL: {cnt:4d} ({pct:.1f}%) {bar}")

    # 2. Bonding curve state at buy (market cap at entry)
    bc_data = [b for b in buys if b['bc_pre_sol'] is not None]
    print(f"\n[2] BONDING CURVE SOL AT BUY (market cap proxy) n={len(bc_data)}:")
    if bc_data:
        bc_vals = [b['bc_pre_sol'] for b in bc_data]
        print(f"  Median: {np.median(bc_vals):.4f} SOL")
        print(f"  Mean:   {np.mean(bc_vals):.4f} SOL")
        print(f"  P10:    {np.percentile(bc_vals, 10):.4f} SOL")
        print(f"  P90:    {np.percentile(bc_vals, 90):.4f} SOL")
        print(f"  Min:    {min(bc_vals):.4f} SOL")
        print(f"  Max:    {max(bc_vals):.4f} SOL")

        # Distribution
        buckets = [(0, 0.05), (0.05, 0.1), (0.1, 0.3), (0.3, 0.6), (0.6, 1.0), (1.0, 999)]
        for lo, hi in buckets:
            cnt = sum(1 for v in bc_vals if lo <= v < hi)
            pct = cnt / len(bc_vals) * 100
            print(f"  {lo:.2f}-{hi:.2f} SOL BC: {cnt:4d} ({pct:.1f}%)")

    # 3. Correlation: BC pre-SOL vs position size
    print(f"\n[3] BC SOL vs BUY SIZE (selection signal):")
    if bc_data:
        # Group by size tier
        tier_bc = defaultdict(list)
        for b in bc_data:
            tier = round(b['sol_spent'], 1)
            tier_bc[tier].append(b['bc_pre_sol'])

        for tier in sorted(tier_bc.keys()):
            vals = tier_bc[tier]
            if len(vals) >= 3:
                print(f"  Size ~{tier:.1f} SOL → BC median={np.median(vals):.4f} SOL, n={len(vals)}")

    # 4. Token price at entry (SOL/token ratio)
    priced = [b for b in buys if b['tokens_received'] and b['bc_pre_sol']]
    print(f"\n[4] ENTRY PRICE ANALYSIS n={len(priced)}:")
    if priced:
        # tokens_received per SOL spent
        ratios = [b['tokens_received'] / b['sol_spent'] for b in priced]
        print(f"  Tokens per SOL - Median: {np.median(ratios):,.0f}")
        print(f"  Tokens per SOL - P10: {np.percentile(ratios, 10):,.0f}")
        print(f"  Tokens per SOL - P90: {np.percentile(ratios, 90):,.0f}")

    # 5. Time-of-day analysis (when does bot trade most?)
    from datetime import datetime, timezone
    hours = [datetime.fromtimestamp(b['ts'], tz=timezone.utc).hour for b in buys if b['ts']]
    print(f"\n[5] ACTIVITY BY HOUR UTC:")
    hour_counts = Counter(hours)
    for h in range(24):
        cnt = hour_counts.get(h, 0)
        bar = '#' * int(cnt / max(hour_counts.values()) * 30)
        print(f"  {h:02d}:00  {cnt:4d}  {bar}")

    # 6. Unique mints - repeat buys?
    mints = [b['mint'] for b in buys]
    unique_mints = len(set(mints))
    print(f"\n[6] UNIQUE TOKENS: {unique_mints} / {len(buys)} buys")
    repeat = Counter(mints)
    multi = [(m, c) for m, c in repeat.items() if c > 1]
    print(f"  Tokens bought multiple times: {len(multi)}")
    for m, c in sorted(multi, key=lambda x: -x[1])[:5]:
        print(f"    {m[:20]}... x{c}")

    return buys


if __name__ == '__main__':
    all_buys = []
    for i in range(1, 10):
        path = DATA_DIR / f'part_{i}.xlsx'
        if path.exists():
            print(f"Processing part_{i}.xlsx...", end=' ', flush=True)
            buys = extract_buys_from_file(str(path), limit=3000)
            print(f"{len(buys)} buys")
            all_buys.extend(buys)

    analyze_patterns(all_buys)
