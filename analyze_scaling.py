"""
Analyze nya666 scaling-in behavior on same token.
Key hypothesis: bot makes probe buy, then scales in if price moves.
"""
import openpyxl
import json
import numpy as np
from collections import defaultdict
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


def extract_all_trades(path: str, limit=5000) -> list[dict]:
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    rows = list(ws.rows)
    headers = [c.value for c in rows[0]]

    trades = []
    for row in rows[1:limit+1]:
        d = {headers[j]: row[j].value for j in range(len(headers))}
        if not d.get('success'):
            continue

        log = str(d.get('log_messages', ''))
        is_buy = 'Instruction: Buy' in log
        is_sell = 'Instruction: Sell' in log
        if not (is_buy or is_sell):
            continue

        sc = parse_json(d.get('sol_changes'))
        tc = parse_json(d.get('token_changes'))

        nya_sc = next((x for x in sc if x['account'] == WALLET), None)
        nya_tc = next((x for x in tc if x.get('owner') == WALLET), None)

        if not nya_sc or not nya_tc:
            continue

        sol_delta = nya_sc.get('delta_sol', 0)
        mint = nya_tc['mint']

        # BC account = receives SOL on buy
        bc_candidates = [
            x for x in sc
            if x['account'] != WALLET
            and x.get('pre_lamports', 0) > 5_000_000
        ]
        bc = max(bc_candidates, key=lambda x: abs(x.get('delta_sol', 0))) if bc_candidates else None
        bc_pre_sol = bc['pre_lamports'] / 1e9 if bc else None

        trades.append({
            'ts': d['block_time'],
            'mint': mint,
            'sol_delta': sol_delta,  # negative = buy, positive = sell
            'is_buy': is_buy,
            'is_sell': is_sell,
            'bc_pre_sol': bc_pre_sol,
        })

    wb.close()
    return trades


def analyze_scaling(trades: list[dict]):
    # Group by mint
    by_mint = defaultdict(list)
    for t in trades:
        by_mint[t['mint']].append(t)

    # Sort each token's trades by time
    for mint in by_mint:
        by_mint[mint].sort(key=lambda x: x['ts'])

    print(f"\nTotal unique tokens: {len(by_mint)}")

    # Tokens with multiple buys = scaling in
    multi_buy_tokens = []
    for mint, txs in by_mint.items():
        buys = [t for t in txs if t['is_buy']]
        sells = [t for t in txs if t['is_sell']]
        if len(buys) > 1:
            multi_buy_tokens.append({
                'mint': mint,
                'n_buys': len(buys),
                'n_sells': len(sells),
                'buys': buys,
                'sells': sells,
            })

    multi_buy_tokens.sort(key=lambda x: -x['n_buys'])
    print(f"Tokens with 2+ buys: {len(multi_buy_tokens)}")
    print(f"Tokens with 5+ buys: {sum(1 for t in multi_buy_tokens if t['n_buys'] >= 5)}")

    # Analyze a sample of multi-buy tokens
    print("\n[SCALING PATTERN - top 10 tokens by buy count]:")
    for tok in multi_buy_tokens[:10]:
        buys = tok['buys']
        print(f"\n  Mint: {tok['mint'][:20]}...")
        print(f"  Buys: {tok['n_buys']}  Sells: {tok['n_sells']}")

        # Show buy sequence: time delta and size
        t0 = buys[0]['ts']
        buy_sizes = []
        for i, b in enumerate(buys):
            dt = b['ts'] - t0
            size = abs(b['sol_delta'])
            bc = b['bc_pre_sol']
            buy_sizes.append(size)
            print(f"    Buy {i+1}: +{dt:6.0f}s  {size:.3f} SOL  BC={bc:.2f} SOL" if bc else
                  f"    Buy {i+1}: +{dt:6.0f}s  {size:.3f} SOL")

        # BC growth = price appreciation between buys
        bc_vals = [b['bc_pre_sol'] for b in buys if b['bc_pre_sol']]
        if len(bc_vals) >= 2:
            bc_growth = (bc_vals[-1] - bc_vals[0]) / bc_vals[0] * 100 if bc_vals[0] > 0 else 0
            print(f"    BC growth: {bc_vals[0]:.2f} → {bc_vals[-1]:.2f} SOL (+{bc_growth:.1f}%)")

    # Distribution of time between buys on same token
    print("\n[TIME BETWEEN CONSECUTIVE BUYS ON SAME TOKEN]:")
    deltas = []
    for tok in multi_buy_tokens:
        buys = tok['buys']
        for i in range(1, len(buys)):
            dt = buys[i]['ts'] - buys[i-1]['ts']
            deltas.append(dt)

    if deltas:
        print(f"  Median: {np.median(deltas):.0f}s")
        print(f"  P25: {np.percentile(deltas, 25):.0f}s")
        print(f"  P75: {np.percentile(deltas, 75):.0f}s")
        print(f"  <10s: {sum(1 for d in deltas if d < 10)} ({sum(1 for d in deltas if d < 10)/len(deltas)*100:.0f}%)")
        print(f"  10-60s: {sum(1 for d in deltas if 10 <= d < 60)}")
        print(f"  60-300s: {sum(1 for d in deltas if 60 <= d < 300)}")
        print(f"  >300s: {sum(1 for d in deltas if d >= 300)}")

    # Does second buy happen at higher BC (= confirms price went up)?
    print("\n[BC GROWTH BETWEEN BUY 1 and BUY 2]:")
    growths = []
    for tok in multi_buy_tokens:
        buys = tok['buys']
        if len(buys) >= 2 and buys[0]['bc_pre_sol'] and buys[1]['bc_pre_sol']:
            g = (buys[1]['bc_pre_sol'] - buys[0]['bc_pre_sol']) / buys[0]['bc_pre_sol'] * 100
            growths.append(g)

    if growths:
        print(f"  Median growth: +{np.median(growths):.1f}%")
        print(f"  P25: {np.percentile(growths, 25):.1f}%")
        print(f"  P75: {np.percentile(growths, 75):.1f}%")
        print(f"  Negative (bought at lower price): {sum(1 for g in growths if g < 0)}")
        print(f"  0-20%: {sum(1 for g in growths if 0 <= g < 20)}")
        print(f"  20-50%: {sum(1 for g in growths if 20 <= g < 50)}")
        print(f"  50-100%: {sum(1 for g in growths if 50 <= g < 100)}")
        print(f"  >100%: {sum(1 for g in growths if g >= 100)}")

    # Does second buy SIZE correlate with first buy size or with BC growth?
    print("\n[FIRST BUY SIZE vs SECOND BUY SIZE]:")
    pairs = []
    for tok in multi_buy_tokens:
        buys = tok['buys']
        if len(buys) >= 2:
            s1 = abs(buys[0]['sol_delta'])
            s2 = abs(buys[1]['sol_delta'])
            pairs.append((s1, s2))

    if pairs:
        # Is second buy typically larger, smaller, or same?
        larger = sum(1 for s1, s2 in pairs if s2 > s1 * 1.2)
        smaller = sum(1 for s1, s2 in pairs if s2 < s1 * 0.8)
        similar = len(pairs) - larger - smaller
        print(f"  2nd buy LARGER: {larger} ({larger/len(pairs)*100:.0f}%)")
        print(f"  2nd buy SIMILAR: {similar} ({similar/len(pairs)*100:.0f}%)")
        print(f"  2nd buy SMALLER: {smaller} ({smaller/len(pairs)*100:.0f}%)")

        # Average sizes
        avg_s1 = np.mean([s for s, _ in pairs])
        avg_s2 = np.mean([s for _, s in pairs])
        print(f"  Avg first buy: {avg_s1:.3f} SOL")
        print(f"  Avg second buy: {avg_s2:.3f} SOL")

    # Single buy tokens - what are they?
    single_buy = [mint for mint, txs in by_mint.items()
                  if sum(1 for t in txs if t['is_buy']) == 1]
    print(f"\n[SINGLE-BUY TOKENS]: {len(single_buy)}")
    single_sizes = []
    for mint in single_buy[:100]:  # sample
        buys = [t for t in by_mint[mint] if t['is_buy']]
        if buys:
            single_sizes.append(abs(buys[0]['sol_delta']))
    if single_sizes:
        print(f"  Avg single-buy size: {np.mean(single_sizes):.3f} SOL")
        print(f"  Median: {np.median(single_sizes):.3f} SOL")


if __name__ == '__main__':
    all_trades = []
    for i in range(1, 10):
        path = DATA_DIR / f'part_{i}.xlsx'
        if path.exists():
            print(f"Processing part_{i}.xlsx...", end=' ', flush=True)
            trades = extract_all_trades(str(path), limit=3000)
            print(f"{len(trades)} trades")
            all_trades.extend(trades)

    analyze_scaling(all_trades)
