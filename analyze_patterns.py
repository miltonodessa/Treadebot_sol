#!/usr/bin/env python3
"""
Deep pattern analysis for wallet D5dtjfGj9PhWsqKjepxFSAnRMrADrYfGPWjVwZSoapKe
Finds NEW patterns not previously identified.
"""

import json
import re
import math
from collections import defaultdict, Counter
from statistics import median, stdev, mean

WALLET = "D5dtjfGj9PhWsqKjepxFSAnRMrADrYfGPWjVwZSoapKe"

# Load data
with open("/home/user/Treadebot_sol/token_signals_1.json") as f:
    d1 = json.load(f)
with open("/home/user/Treadebot_sol/token_signals_2.json") as f:
    d2 = json.load(f)

all_data = d1 + d2
data = [r for r in all_data if r["wallet"] == WALLET]

print(f"Total records for wallet: {len(data)}")
winners = [r for r in data if r["estimated_pnl_sol"] > 0]
losers  = [r for r in data if r["estimated_pnl_sol"] <= 0]
print(f"Winners: {len(winners)} ({100*len(winners)/len(data):.1f}%)")
print(f"Losers:  {len(losers)} ({100*len(losers)/len(data):.1f}%)")
print()

def stats(records, label=""):
    n = len(records)
    if n == 0:
        return f"  {label}: N=0"
    pnls = [r["estimated_pnl_sol"] for r in records]
    wins = [p for p in pnls if p > 0]
    wr = 100*len(wins)/n
    avg_pnl = mean(pnls)
    med_pnl = median(pnls)
    avg_win = mean(wins) if wins else 0
    losses = [p for p in pnls if p <= 0]
    avg_loss = mean(losses) if losses else 0
    return f"  {label}: N={n} | WR={wr:.1f}% | AvgPnL={avg_pnl:.4f} | MedPnL={med_pnl:.4f} | AvgWin={avg_win:.4f} | AvgLoss={avg_loss:.4f}"

def pct(vals, p):
    """Return percentile p of vals"""
    if not vals:
        return None
    s = sorted(vals)
    idx = (len(s)-1) * p / 100
    lo, hi = int(idx), min(int(idx)+1, len(s)-1)
    return s[lo] + (s[hi]-s[lo]) * (idx-lo)

# ============================================================
# GOLDEN subset: nbw=0 + nb60>=91
# ============================================================
golden = [r for r in data if r.get("n_buyers_before_wallet") == 0 and (r.get("n_buyers_in_60s") or 0) >= 91]
golden_w = [r for r in golden if r["estimated_pnl_sol"] > 0]
golden_l = [r for r in golden if r["estimated_pnl_sol"] <= 0]
print(f"=== GOLDEN TRADES (nbw=0 + nb60>=91): {len(golden)} total ===")
print(stats(golden, "ALL golden"))
print(stats(golden_w, "Golden WINNERS"))
print(stats(golden_l, "Golden LOSERS"))
print()

# ============================================================
# 1. TOKEN NAME / SYMBOL PATTERNS
# ============================================================
print("=" * 70)
print("ANALYSIS 1: TOKEN NAME / SYMBOL PATTERNS")
print("=" * 70)

def analyze_symbol_patterns(records, label):
    print(f"\n--- {label} (N={len(records)}) ---")

    # Symbol length
    sym_len_bins = defaultdict(list)
    for r in records:
        sym = r.get("symbol", "") or ""
        l = len(sym)
        bucket = f"len_{l}" if l <= 6 else "len_7+"
        sym_len_bins[bucket].append(r)
    print("\nSymbol length distribution:")
    for k in sorted(sym_len_bins.keys()):
        recs = sym_len_bins[k]
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wr = 100*sum(1 for p in pnls if p > 0)/len(pnls)
        avg = mean(pnls)
        print(f"  {k}: N={len(recs)} | WR={wr:.1f}% | AvgPnL={avg:.4f}")

    # ALL CAPS vs mixed
    all_caps = [r for r in records if r.get("symbol","").isupper() and r.get("symbol","")]
    mixed    = [r for r in records if not (r.get("symbol","").isupper()) and r.get("symbol","")]
    pnls_caps = [r["estimated_pnl_sol"] for r in all_caps]
    pnls_mix  = [r["estimated_pnl_sol"] for r in mixed]
    wr_caps = 100*sum(1 for p in pnls_caps if p > 0)/len(pnls_caps) if pnls_caps else 0
    wr_mix  = 100*sum(1 for p in pnls_mix  if p > 0)/len(pnls_mix)  if pnls_mix  else 0
    print(f"\nALL-CAPS symbol: N={len(all_caps)} | WR={wr_caps:.1f}% | AvgPnL={mean(pnls_caps):.4f}" if pnls_caps else "ALL-CAPS: N=0")
    print(f"Mixed-case symbol: N={len(mixed)} | WR={wr_mix:.1f}% | AvgPnL={mean(pnls_mix):.4f}" if pnls_mix else "Mixed: N=0")

    # Numbers in symbol
    has_num = [r for r in records if re.search(r'\d', r.get("symbol","") or "")]
    no_num  = [r for r in records if not re.search(r'\d', r.get("symbol","") or "")]
    pnls_num = [r["estimated_pnl_sol"] for r in has_num]
    pnls_non = [r["estimated_pnl_sol"] for r in no_num]
    wr_num = 100*sum(1 for p in pnls_num if p > 0)/len(pnls_num) if pnls_num else 0
    wr_non = 100*sum(1 for p in pnls_non if p > 0)/len(pnls_non) if pnls_non else 0
    print(f"\nSymbol has numbers: N={len(has_num)} | WR={wr_num:.1f}% | AvgPnL={mean(pnls_num):.4f}" if pnls_num else "Symbol has numbers: N=0")
    print(f"Symbol no numbers:  N={len(no_num)} | WR={wr_non:.1f}% | AvgPnL={mean(pnls_non):.4f}" if pnls_non else "Symbol no numbers: N=0")

    # Special chars in symbol
    special_re = re.compile(r'[\$\!\@\#\%\^\&\*\(\)\-\_\+\=\[\]\{\}\|\;\:\,\.\?\~]')
    has_spec = [r for r in records if special_re.search(r.get("symbol","") or "")]
    no_spec  = [r for r in records if not special_re.search(r.get("symbol","") or "")]
    pnls_spec = [r["estimated_pnl_sol"] for r in has_spec]
    pnls_nsp  = [r["estimated_pnl_sol"] for r in no_spec]
    wr_spec = 100*sum(1 for p in pnls_spec if p > 0)/len(pnls_spec) if pnls_spec else 0
    wr_nsp  = 100*sum(1 for p in pnls_nsp  if p > 0)/len(pnls_nsp)  if pnls_nsp  else 0
    print(f"\nSymbol has special chars: N={len(has_spec)} | WR={wr_spec:.1f}% | AvgPnL={mean(pnls_spec):.4f}" if pnls_spec else "Special chars: N=0")
    print(f"Symbol no special chars:  N={len(no_spec)} | WR={wr_nsp:.1f}% | AvgPnL={mean(pnls_nsp):.4f}" if pnls_nsp else "No special: N=0")

    # Name length bins
    name_len_bins = defaultdict(list)
    for r in records:
        name = r.get("name", "") or ""
        l = len(name)
        if l <= 5: bucket = "1-5"
        elif l <= 10: bucket = "6-10"
        elif l <= 15: bucket = "11-15"
        elif l <= 20: bucket = "16-20"
        else: bucket = "21+"
        name_len_bins[bucket].append(r)
    print("\nName length distribution:")
    for k in ["1-5","6-10","11-15","16-20","21+"]:
        recs = name_len_bins.get(k, [])
        if not recs: continue
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wr = 100*sum(1 for p in pnls if p > 0)/len(pnls)
        avg = mean(pnls)
        print(f"  name_len {k}: N={len(recs)} | WR={wr:.1f}% | AvgPnL={avg:.4f}")

analyze_symbol_patterns(data, "ALL TRADES")
analyze_symbol_patterns(golden, "GOLDEN TRADES (nbw=0 + nb60>=91)")

# Has social links
for label, subset in [("ALL", data), ("GOLDEN", golden)]:
    print(f"\n--- Social links for {label} ---")
    for field in ["has_website", "has_twitter", "has_telegram"]:
        yes = [r for r in subset if r.get(field)]
        no  = [r for r in subset if not r.get(field)]
        pyes = [r["estimated_pnl_sol"] for r in yes]
        pno  = [r["estimated_pnl_sol"] for r in no]
        wr_y = 100*sum(1 for p in pyes if p > 0)/len(pyes) if pyes else 0
        wr_n = 100*sum(1 for p in pno if p > 0)/len(pno) if pno else 0
        avg_y = mean(pyes) if pyes else 0
        avg_n = mean(pno) if pno else 0
        print(f"  {field}=True:  N={len(yes)} | WR={wr_y:.1f}% | AvgPnL={avg_y:.4f}")
        print(f"  {field}=False: N={len(no)}  | WR={wr_n:.1f}% | AvgPnL={avg_n:.4f}")

# ============================================================
# 2. BC VELOCITY / MOMENTUM (vSolInBondingCurve = first_buy_bc_sol)
# ============================================================
print("\n" + "=" * 70)
print("ANALYSIS 2: BC SOL (first_buy_bc_sol) FINE-GRAINED")
print("=" * 70)

bc_buckets = [
    ("0.0-0.5",  0.0, 0.5),
    ("0.5-1.0",  0.5, 1.0),
    ("1.0-1.5",  1.0, 1.5),
    ("1.5-2.0",  1.5, 2.0),
    ("2.0-2.5",  2.0, 2.5),
    ("2.5-3.0",  2.5, 3.0),
    ("3.0-4.0",  3.0, 4.0),
    ("4.0-5.0",  4.0, 5.0),
    ("5.0-7.0",  5.0, 7.0),
    ("7.0-10.0", 7.0, 10.0),
    ("10.0+",   10.0, 999),
]

def bc_analysis(subset, label):
    print(f"\n--- BC Sol analysis for {label} ---")
    print(f"{'BC Range':<12} {'N':>6} {'WR%':>7} {'AvgPnL':>9} {'MedPnL':>9} {'AvgWin':>9} {'AvgLoss':>9}")
    for name, lo, hi in bc_buckets:
        recs = [r for r in subset if lo <= (r.get("first_buy_bc_sol") or 0) < hi]
        if not recs: continue
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        wr = 100*len(wins)/len(pnls)
        avg = mean(pnls)
        med = median(pnls)
        aw = mean(wins) if wins else 0
        al = mean(losses) if losses else 0
        print(f"  {name:<12} {len(recs):>6} {wr:>7.1f} {avg:>9.4f} {med:>9.4f} {aw:>9.4f} {al:>9.4f}")

bc_analysis(data, "ALL TRADES")
bc_analysis(golden, "GOLDEN (nbw=0 + nb60>=91)")

# ============================================================
# 3. N_TRANSFERS AT CREATE (n_token_transfers_at_create)
# ============================================================
print("\n" + "=" * 70)
print("ANALYSIS 3: N_TOKEN_TRANSFERS_AT_CREATE")
print("=" * 70)

def ntransfers_analysis(subset, label):
    print(f"\n--- {label} ---")
    buckets = defaultdict(list)
    for r in subset:
        n = r.get("n_token_transfers_at_create", 0) or 0
        if n == 0: k = "0"
        elif n == 1: k = "1"
        elif n == 2: k = "2"
        elif n <= 5: k = "3-5"
        elif n <= 10: k = "6-10"
        else: k = "11+"
        buckets[k].append(r)
    print(f"{'Transfers':>12} {'N':>7} {'WR%':>7} {'AvgPnL':>9} {'MedPnL':>9}")
    for k in ["0","1","2","3-5","6-10","11+"]:
        recs = buckets.get(k, [])
        if not recs: continue
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wins = [p for p in pnls if p > 0]
        wr = 100*len(wins)/len(pnls)
        avg = mean(pnls)
        med = median(pnls)
        print(f"  {k:>10} {len(recs):>7} {wr:>7.1f} {avg:>9.4f} {med:>9.4f}")

ntransfers_analysis(data, "ALL TRADES")
ntransfers_analysis(golden, "GOLDEN (nbw=0 + nb60>=91)")

# ============================================================
# 4. SECONDS_AFTER_CREATION DEEP DIVE
# ============================================================
print("\n" + "=" * 70)
print("ANALYSIS 4: SECONDS_AFTER_CREATION DEEP DIVE")
print("=" * 70)

sac_vals = [r["seconds_after_creation"] for r in data if r.get("seconds_after_creation") is not None]
neg_sac = [v for v in sac_vals if v < 0]
pos_sac = [v for v in sac_vals if v >= 0]
print(f"\nNegative SAC count: {len(neg_sac)} ({100*len(neg_sac)/len(sac_vals):.1f}%)")
print(f"Positive SAC count: {len(pos_sac)} ({100*len(pos_sac)/len(sac_vals):.1f}%)")
print(f"\nNeg SAC percentiles (P10/P25/P50/P75/P90/P99):")
for p in [10,25,50,75,90,99]:
    print(f"  P{p}: {pct(neg_sac, p):.0f}s")
print(f"\nNeg SAC range: {min(neg_sac):.0f} to {max(neg_sac):.0f}")

# SAC buckets for win rate
sac_buckets = [
    ("neg < -3600",  -99999, -3600),
    ("-3600 to -1800", -3600, -1800),
    ("-1800 to -600", -1800, -600),
    ("-600 to -300",  -600, -300),
    ("-300 to -60",   -300, -60),
    ("-60 to 0",      -60, 0),
    ("0 to 30",        0, 30),
    ("30 to 60",      30, 60),
    ("60 to 120",     60, 120),
    ("120 to 300",   120, 300),
    ("300 to 600",   300, 600),
    ("600+",         600, 999999),
]

def sac_analysis(subset, label):
    print(f"\n--- SAC analysis for {label} ---")
    print(f"{'SAC Range':>22} {'N':>7} {'WR%':>7} {'AvgPnL':>9} {'MedPnL':>9}")
    for name, lo, hi in sac_buckets:
        recs = [r for r in subset if r.get("seconds_after_creation") is not None and lo <= r["seconds_after_creation"] < hi]
        if not recs: continue
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wins = [p for p in pnls if p > 0]
        wr = 100*len(wins)/len(pnls)
        avg = mean(pnls)
        med = median(pnls)
        print(f"  {name:>20} {len(recs):>7} {wr:>7.1f} {avg:>9.4f} {med:>9.4f}")

sac_analysis(data, "ALL TRADES")
sac_analysis(golden, "GOLDEN (nbw=0 + nb60>=91)")

# ============================================================
# 5. EXIT TIMING FOR WINNING GOLDEN TRADES
# ============================================================
print("\n" + "=" * 70)
print("ANALYSIS 5: EXIT TIMING FOR WINNING GOLDEN TRADES")
print("=" * 70)

hold_wins = sorted([r["hold_seconds"] for r in golden_w if r.get("hold_seconds") is not None])
if hold_wins:
    print(f"\nWinning golden trades N={len(hold_wins)}")
    for p in [10,25,50,75,90,95]:
        print(f"  P{p} hold_seconds: {pct(hold_wins, p):.0f}s ({pct(hold_wins, p)/60:.1f} min)")

    # Finer hold buckets for golden winners
    hold_buckets = [(f"{lo}-{hi}s", lo, hi) for lo, hi in [
        (0,60),(60,120),(120,300),(300,600),(600,900),(900,1200),(1200,1800),(1800,2700),(2700,3600),(3600,7200),(7200,99999)
    ]]
    print(f"\n{'Hold Range':>15} {'N':>7} {'AvgPnL':>9} {'MedPnL':>9} {'MaxPnL':>9}")
    for name, lo, hi in hold_buckets:
        recs = [r for r in golden_w if lo <= (r.get("hold_seconds") or 0) < hi]
        if not recs: continue
        pnls = [r["estimated_pnl_sol"] for r in recs]
        avg = mean(pnls)
        med = median(pnls)
        mx = max(pnls)
        print(f"  {name:>13} {len(recs):>7} {avg:>9.4f} {med:>9.4f} {mx:>9.4f}")

# PnL distribution for golden winners
pnl_wins_g = sorted([r["estimated_pnl_sol"] for r in golden_w])
print(f"\nGolden winner PnL distribution (SOL):")
for p in [10,25,50,75,90,95,99]:
    print(f"  P{p}: {pct(pnl_wins_g, p):.4f} SOL")

# PnL% bins
# Assume entry ~0.47 SOL
entry = 0.47
pnl_pct = [100*r["estimated_pnl_sol"]/max(r.get("total_sol_in",entry),0.01) for r in golden_w]
pnl_pct_bins = [(f"{lo}%-{hi}%", lo, hi) for lo, hi in [
    (0,10),(10,20),(20,30),(30,50),(50,75),(75,100),(100,150),(150,200),(200,500),(500,9999)
]]
print(f"\nGolden winner PnL% buckets (vs invested):")
print(f"{'PnL% Range':>15} {'N':>7} {'Cum%':>7}")
cum = 0
for name, lo, hi in pnl_pct_bins:
    cnt = sum(1 for p in pnl_pct if lo <= p < hi)
    if cnt == 0: continue
    cum += cnt
    cum_pct = 100*cum/len(pnl_pct)
    print(f"  {name:>13} {cnt:>7} {cum_pct:>7.1f}")

# ============================================================
# 6. CONSECUTIVE PATTERNS / INTER-TRADE TIMING
# ============================================================
print("\n" + "=" * 70)
print("ANALYSIS 6: CONSECUTIVE PATTERNS / INTER-TRADE TIMING")
print("=" * 70)

# Sort by first_buy_ts
sorted_data = sorted(data, key=lambda r: r.get("first_buy_ts") or 0)

# Compute inter-trade gaps
gaps_after_win = []
gaps_after_loss = []
for i in range(1, len(sorted_data)):
    prev = sorted_data[i-1]
    curr = sorted_data[i]
    gap = (curr.get("first_buy_ts") or 0) - (prev.get("first_buy_ts") or 0)
    if gap < 0 or gap > 86400:  # skip gaps > 1 day (session breaks)
        continue
    if prev["estimated_pnl_sol"] > 0:
        gaps_after_win.append(gap)
    else:
        gaps_after_loss.append(gap)

print(f"\nInter-trade gap AFTER WIN (N={len(gaps_after_win)}):")
for p in [10,25,50,75,90]:
    print(f"  P{p}: {pct(gaps_after_win, p):.0f}s")
print(f"  Mean: {mean(gaps_after_win):.0f}s" if gaps_after_win else "  (no data)")

print(f"\nInter-trade gap AFTER LOSS (N={len(gaps_after_loss)}):")
for p in [10,25,50,75,90]:
    print(f"  P{p}: {pct(gaps_after_loss, p):.0f}s")
print(f"  Mean: {mean(gaps_after_loss):.0f}s" if gaps_after_loss else "  (no data)")

# Win rate by "trade number in session"
# Define session as gap > 1800s
sessions = []
current_session = [sorted_data[0]]
for i in range(1, len(sorted_data)):
    gap = (sorted_data[i].get("first_buy_ts") or 0) - (sorted_data[i-1].get("first_buy_ts") or 0)
    if gap > 1800:
        sessions.append(current_session)
        current_session = []
    current_session.append(sorted_data[i])
sessions.append(current_session)

print(f"\nSessions (gap>30min): {len(sessions)}")
session_lengths = [len(s) for s in sessions]
print(f"Session length P50: {median(session_lengths):.0f} trades | P90: {pct(session_lengths, 90):.0f} | Max: {max(session_lengths)}")

# WR by position in session (1st trade, 2nd trade, ...)
pos_stats = defaultdict(list)
for sess in sessions:
    for idx, r in enumerate(sess):
        pos = min(idx+1, 10)  # cap at 10+
        pos_stats[pos].append(r)

print(f"\nWin rate by trade position in session:")
print(f"{'Position':>10} {'N':>7} {'WR%':>7} {'AvgPnL':>9}")
for pos in sorted(pos_stats.keys()):
    recs = pos_stats[pos]
    pnls = [r["estimated_pnl_sol"] for r in recs]
    wr = 100*sum(1 for p in pnls if p > 0)/len(pnls)
    avg = mean(pnls)
    label = f"#{pos}" if pos < 10 else "#10+"
    print(f"  {label:>8} {len(recs):>7} {wr:>7.1f} {avg:>9.4f}")

# Consecutive losses analysis
max_consec_loss = 0
cur_consec = 0
streak_stats = defaultdict(list)  # streak_length -> next_trade_pnl
last_streak = 0
for r in sorted_data:
    if r["estimated_pnl_sol"] <= 0:
        cur_consec += 1
    else:
        if last_streak > 0:
            streak_stats[min(last_streak, 5)].append(r["estimated_pnl_sol"])
        cur_consec = 0
        last_streak = 0
    max_consec_loss = max(max_consec_loss, cur_consec)
    last_streak = cur_consec

print(f"\nMax consecutive losses: {max_consec_loss}")

# ============================================================
# 7. BC SOL WITHIN GOLDEN SUBSET (winners vs losers)
# ============================================================
print("\n" + "=" * 70)
print("ANALYSIS 7: BC SOL WITHIN GOLDEN TRADES (winners vs losers)")
print("=" * 70)

g_win_bc = [r.get("first_buy_bc_sol", 0) for r in golden_w]
g_los_bc = [r.get("first_buy_bc_sol", 0) for r in golden_l]

if g_win_bc:
    print(f"\nGolden WINNER BC Sol - Mean={mean(g_win_bc):.3f} | Median={median(g_win_bc):.3f} | P10={pct(g_win_bc,10):.3f} | P90={pct(g_win_bc,90):.3f}")
if g_los_bc:
    print(f"Golden LOSER  BC Sol - Mean={mean(g_los_bc):.3f} | Median={median(g_los_bc):.3f} | P10={pct(g_los_bc,10):.3f} | P90={pct(g_los_bc,90):.3f}")

# Fine-grained BC buckets for golden trades
print(f"\nFine-grained BC for GOLDEN trades:")
print(f"{'BC Range':>12} {'N':>7} {'WR%':>7} {'AvgPnL':>9} {'MedPnL':>9}")
fine_bc = [
    ("0.0-0.3",  0.0, 0.3),
    ("0.3-0.5",  0.3, 0.5),
    ("0.5-0.7",  0.5, 0.7),
    ("0.7-1.0",  0.7, 1.0),
    ("1.0-1.3",  1.0, 1.3),
    ("1.3-1.5",  1.3, 1.5),
    ("1.5-1.7",  1.5, 1.7),
    ("1.7-2.0",  1.7, 2.0),
    ("2.0-2.5",  2.0, 2.5),
    ("2.5-3.0",  2.5, 3.0),
    ("3.0+",     3.0, 999),
]
for name, lo, hi in fine_bc:
    recs = [r for r in golden if lo <= r.get("first_buy_bc_sol",0) < hi]
    if not recs: continue
    pnls = [r["estimated_pnl_sol"] for r in recs]
    wins = [p for p in pnls if p > 0]
    wr = 100*len(wins)/len(pnls)
    avg = mean(pnls)
    med = median(pnls)
    print(f"  {name:>10} {len(recs):>7} {wr:>7.1f} {avg:>9.4f} {med:>9.4f}")

# ============================================================
# 8. TOKEN LAUNCH TIMING FOR GOLDEN TRADES
# ============================================================
print("\n" + "=" * 70)
print("ANALYSIS 8: SAC FOR GOLDEN TRADES")
print("=" * 70)

sac_analysis(golden, "GOLDEN (nbw=0 + nb60>=91)")

golden_sac = [r.get("seconds_after_creation") for r in golden if r.get("seconds_after_creation") is not None]
golden_sac_win = [r.get("seconds_after_creation") for r in golden_w if r.get("seconds_after_creation") is not None]
golden_sac_los = [r.get("seconds_after_creation") for r in golden_l if r.get("seconds_after_creation") is not None]

if golden_sac_win:
    print(f"\nGolden WINNER SAC: Mean={mean(golden_sac_win):.1f} | Median={median(golden_sac_win):.1f}")
    for p in [10,25,50,75,90]:
        print(f"  P{p}: {pct(golden_sac_win, p):.1f}s")
if golden_sac_los:
    print(f"\nGolden LOSER SAC: Mean={mean(golden_sac_los):.1f} | Median={median(golden_sac_los):.1f}")
    for p in [10,25,50,75,90]:
        print(f"  P{p}: {pct(golden_sac_los, p):.1f}s")

# ============================================================
# 9. NB60 FINE-GRAINED: 91-100 breakdown
# ============================================================
print("\n" + "=" * 70)
print("ANALYSIS 9: NB60 FINE-GRAINED (91-100 breakdown)")
print("=" * 70)

# First, overall NB60 breakdown
nb60_buckets = [
    ("0-10",   0, 10),
    ("11-20",  11, 20),
    ("21-30",  21, 30),
    ("31-40",  31, 40),
    ("41-50",  41, 50),
    ("51-60",  51, 60),
    ("61-70",  61, 70),
    ("71-80",  71, 80),
    ("81-90",  81, 90),
    ("91-93",  91, 93),
    ("94-96",  94, 96),
    ("97-98",  97, 98),
    ("99",     99, 99),
    ("100",   100, 100),
]

print(f"\nFull NB60 breakdown (nbw=0 only):")
nbw0 = [r for r in data if r.get("n_buyers_before_wallet") == 0]
print(f"{'NB60 Range':>12} {'N':>7} {'WR%':>7} {'AvgPnL':>9} {'MedPnL':>9}")
for name, lo, hi in nb60_buckets:
    recs = [r for r in nbw0 if lo <= r.get("n_buyers_in_60s", 0) <= hi]
    if not recs: continue
    pnls = [r["estimated_pnl_sol"] for r in recs]
    wins = [p for p in pnls if p > 0]
    wr = 100*len(wins)/len(pnls)
    avg = mean(pnls)
    med = median(pnls)
    print(f"  {name:>10} {len(recs):>7} {wr:>7.1f} {avg:>9.4f} {med:>9.4f}")

# Specifically 91-100 breakdown (all data and nbw=0)
print(f"\nFine-grained 91-100 (ALL trades, no nbw filter):")
print(f"{'NB60':>6} {'N':>7} {'WR%':>7} {'AvgPnL':>9}")
for nb in range(91, 101):
    recs = [r for r in data if r.get("n_buyers_in_60s") == nb]
    if not recs: continue
    pnls = [r["estimated_pnl_sol"] for r in recs]
    wr = 100*sum(1 for p in pnls if p > 0)/len(pnls)
    avg = mean(pnls)
    print(f"  {nb:>4} {len(recs):>7} {wr:>7.1f} {avg:>9.4f}")

# Is nb60=100 special? compare pct of trades at exactly 100
nb100 = [r for r in data if r.get("n_buyers_in_60s") == 100]
print(f"\nNB60=100 trades: {len(nb100)} ({100*len(nb100)/len(data):.1f}% of all)")
if nb100:
    pnls = [r["estimated_pnl_sol"] for r in nb100]
    print(f"  WR={100*sum(1 for p in pnls if p > 0)/len(pnls):.1f}% | AvgPnL={mean(pnls):.4f}")
    # BC sol distribution at nb60=100
    bc_vals = [r.get("first_buy_bc_sol",0) for r in nb100]
    print(f"  BC Sol: Mean={mean(bc_vals):.3f} | Median={median(bc_vals):.3f}")

# ============================================================
# 10. DEV BUY vs NBW=0 ANALYSIS
# ============================================================
print("\n" + "=" * 70)
print("ANALYSIS 10: DEV BUY SOL FOR NBW=0 GOLDEN TRADES")
print("=" * 70)

dev_buckets = [
    ("dev=0",      0.0, 0.001),
    ("dev 0-0.01", 0.001, 0.01),
    ("dev 0.01-0.05", 0.01, 0.05),
    ("dev 0.05-0.1",  0.05, 0.1),
    ("dev 0.1-0.2",   0.1, 0.2),
    ("dev 0.2-0.5",   0.2, 0.5),
    ("dev 0.5+",      0.5, 999),
]

def dev_analysis(subset, label):
    print(f"\n--- Dev buy analysis for {label} ---")
    print(f"{'Dev Range':>16} {'N':>7} {'WR%':>7} {'AvgPnL':>9} {'MedPnL':>9}")
    for name, lo, hi in dev_buckets:
        recs = [r for r in subset if lo <= (r.get("dev_buy_sol") or 0) < hi]
        if not recs: continue
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wins = [p for p in pnls if p > 0]
        wr = 100*len(wins)/len(pnls)
        avg = mean(pnls)
        med = median(pnls)
        print(f"  {name:>14} {len(recs):>7} {wr:>7.1f} {avg:>9.4f} {med:>9.4f}")

dev_analysis(data, "ALL TRADES")
dev_analysis(golden, "GOLDEN (nbw=0 + nb60>=91)")

# Dev=0 breakdown for golden
golden_dev0 = [r for r in golden if (r.get("dev_buy_sol") or 0) < 0.001]
golden_devpos = [r for r in golden if (r.get("dev_buy_sol") or 0) >= 0.001]
print(f"\nGolden dev=0: N={len(golden_dev0)}")
print(stats(golden_dev0, "  dev=0"))
print(f"Golden dev>0: N={len(golden_devpos)}")
print(stats(golden_devpos, "  dev>0"))

# ============================================================
# BONUS: BUY PRESSURE (buy_pressure_sol_60s)
# ============================================================
print("\n" + "=" * 70)
print("BONUS ANALYSIS: BUY PRESSURE (buy_pressure_sol_60s)")
print("=" * 70)

bp_buckets = [
    ("0.0",      0.0, 0.001),
    ("0-1 SOL",  0.001, 1.0),
    ("1-2 SOL",  1.0, 2.0),
    ("2-3 SOL",  2.0, 3.0),
    ("3-5 SOL",  3.0, 5.0),
    ("5-10 SOL", 5.0, 10.0),
    ("10+ SOL",  10.0, 999),
]

def bp_analysis(subset, label):
    print(f"\n--- Buy Pressure for {label} ---")
    print(f"{'BP Range':>12} {'N':>7} {'WR%':>7} {'AvgPnL':>9} {'MedPnL':>9}")
    for name, lo, hi in bp_buckets:
        recs = [r for r in subset if lo <= (r.get("buy_pressure_sol_60s") or 0) < hi]
        if not recs: continue
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wins = [p for p in pnls if p > 0]
        wr = 100*len(wins)/len(pnls)
        avg = mean(pnls)
        med = median(pnls)
        print(f"  {name:>10} {len(recs):>7} {wr:>7.1f} {avg:>9.4f} {med:>9.4f}")

bp_analysis(data, "ALL TRADES")
bp_analysis(golden, "GOLDEN (nbw=0 + nb60>=91)")

# ============================================================
# BONUS 2: KNOWN WALLETS PRESENT
# ============================================================
print("\n" + "=" * 70)
print("BONUS ANALYSIS: KNOWN WALLETS PRESENT")
print("=" * 70)

kw_yes = [r for r in data if r.get("known_wallets_present")]
kw_no  = [r for r in data if not r.get("known_wallets_present")]
print(stats(kw_yes, "known_wallets_present=True "))
print(stats(kw_no,  "known_wallets_present=False"))

# For golden
kw_yes_g = [r for r in golden if r.get("known_wallets_present")]
kw_no_g  = [r for r in golden if not r.get("known_wallets_present")]
print(f"\nFor GOLDEN:")
print(stats(kw_yes_g, "known_wallets_present=True "))
print(stats(kw_no_g,  "known_wallets_present=False"))

# ============================================================
# BONUS 3: CO_BUY ANALYSIS
# ============================================================
print("\n" + "=" * 70)
print("BONUS ANALYSIS: IS_CO_BUY")
print("=" * 70)

cobuy_yes = [r for r in data if r.get("is_co_buy")]
cobuy_no  = [r for r in data if not r.get("is_co_buy")]
print(stats(cobuy_yes, "is_co_buy=True "))
print(stats(cobuy_no,  "is_co_buy=False"))

cobuy_yes_g = [r for r in golden if r.get("is_co_buy")]
cobuy_no_g  = [r for r in golden if not r.get("is_co_buy")]
print(f"\nFor GOLDEN:")
print(stats(cobuy_yes_g, "is_co_buy=True "))
print(stats(cobuy_no_g,  "is_co_buy=False"))

# ============================================================
# BONUS 4: N_BUYS (DCA deeper analysis)
# ============================================================
print("\n" + "=" * 70)
print("BONUS ANALYSIS: N_BUYS (number of buy transactions)")
print("=" * 70)

def nbuys_analysis(subset, label):
    print(f"\n--- {label} ---")
    print(f"{'N_Buys':>8} {'N':>7} {'WR%':>7} {'AvgPnL':>9} {'MedPnL':>9}")
    for nb in [1,2,3,4,5]:
        recs = [r for r in subset if r.get("n_buys") == nb]
        if not recs: continue
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wins = [p for p in pnls if p > 0]
        wr = 100*len(wins)/len(pnls)
        avg = mean(pnls)
        med = median(pnls)
        print(f"  {nb:>6} {len(recs):>7} {wr:>7.1f} {avg:>9.4f} {med:>9.4f}")
    # 6+
    recs = [r for r in subset if (r.get("n_buys") or 1) >= 6]
    if recs:
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wr = 100*sum(1 for p in pnls if p > 0)/len(pnls)
        avg = mean(pnls)
        med = median(pnls)
        print(f"  {'6+':>6} {len(recs):>7} {wr:>7.1f} {avg:>9.4f} {med:>9.4f}")

nbuys_analysis(data, "ALL TRADES")
nbuys_analysis(golden, "GOLDEN (nbw=0 + nb60>=91)")

# ============================================================
# BONUS 5: HOUR OF DAY deeper (beyond 17-19 already known)
# ============================================================
print("\n" + "=" * 70)
print("BONUS ANALYSIS: HOUR OF DAY FULL BREAKDOWN")
print("=" * 70)

def hour_analysis(subset, label):
    print(f"\n--- {label} ---")
    print(f"{'Hour UTC':>10} {'N':>7} {'WR%':>7} {'AvgPnL':>9}")
    hour_data = defaultdict(list)
    for r in subset:
        ts = r.get("first_buy_ts")
        if ts:
            h = (ts // 3600) % 24
            hour_data[h].append(r)
    for h in sorted(hour_data.keys()):
        recs = hour_data[h]
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wr = 100*sum(1 for p in pnls if p > 0)/len(pnls)
        avg = mean(pnls)
        print(f"  {h:>8}h {len(recs):>7} {wr:>7.1f} {avg:>9.4f}")

hour_analysis(data, "ALL TRADES")
hour_analysis(golden, "GOLDEN (nbw=0 + nb60>=91)")

# ============================================================
# BONUS 6: DESCRIPTION / SOCIAL COMBO
# ============================================================
print("\n" + "=" * 70)
print("BONUS ANALYSIS: SOCIAL COMBO PATTERNS")
print("=" * 70)

# All social vs no social
for label, subset in [("ALL", data), ("GOLDEN", golden)]:
    print(f"\n--- {label} social combinations ---")
    combos = defaultdict(list)
    for r in subset:
        key = (bool(r.get("has_website")), bool(r.get("has_twitter")), bool(r.get("has_telegram")))
        combos[key].append(r)
    print(f"{'Web':>5} {'Twit':>5} {'Tele':>5} {'N':>7} {'WR%':>7} {'AvgPnL':>9}")
    for key in sorted(combos.keys(), key=lambda k: -len(combos[k])):
        recs = combos[key]
        if len(recs) < 5: continue
        pnls = [r["estimated_pnl_sol"] for r in recs]
        wr = 100*sum(1 for p in pnls if p > 0)/len(pnls)
        avg = mean(pnls)
        w, t, tg = key
        print(f"  {'Y' if w else 'N':>5} {'Y' if t else 'N':>5} {'Y' if tg else 'N':>5} {len(recs):>7} {wr:>7.1f} {avg:>9.4f}")

# ============================================================
# SUMMARY: NEW ACTIONABLE FILTERS
# ============================================================
print("\n" + "=" * 70)
print("SUMMARY: DATA SUMMARY FOR FILTER DEVELOPMENT")
print("=" * 70)
print(f"\nTotal wallet trades: {len(data)}")
print(f"Overall WR: {100*len(winners)/len(data):.1f}%")
print(f"Golden trades (nbw=0+nb60>=91): {len(golden)}")
print(f"Golden WR: {100*len(golden_w)/max(len(golden),1):.1f}%")
print(f"\nGolden dev=0: {len(golden_dev0)} trades | WR={100*sum(1 for r in golden_dev0 if r['estimated_pnl_sol']>0)/max(len(golden_dev0),1):.1f}%")
print(f"Golden dev>0: {len(golden_devpos)} trades | WR={100*sum(1 for r in golden_devpos if r['estimated_pnl_sol']>0)/max(len(golden_devpos),1):.1f}%")

print("\nDone.")
