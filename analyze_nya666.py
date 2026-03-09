#!/usr/bin/env python3
"""
Полный анализ кошелька nya666 по всем 4 файлам (34,399 сделок).
Цель: найти реальные паттерны для улучшения стратегии копи-трейдинга.
"""
import json, collections, math, statistics
from datetime import datetime, timezone

# ── Загрузка данных ───────────────────────────────────────────────────────────
print("Загружаем данные...")
all_records = []
for fn in range(1, 5):
    with open(f'/home/user/Treadebot_sol/token_signals_{fn}.json', 'rb') as f:
        content = f.read().decode('utf-8', errors='replace').replace('}{', '},{')
    data = json.loads(content)
    all_records.extend(data)

# Фильтруем только nya666
NYA = 'nya666pQkP3PzWxi7JngU3rRMHuc7zbLK8c8wxQ4qpT'
D5D = 'D5dtjfGj9PhWsqKjepxFSAnRMrADrYfGPWjVwZSoapKe'

nya = [r for r in all_records if r['wallet'] == NYA]
d5d = [r for r in all_records if r['wallet'] == D5D]
print(f"nya666: {len(nya)} | D5dtjf: {len(d5d)} | всего: {len(all_records)}")

# ── Утилиты ───────────────────────────────────────────────────────────────────
def pnl(r): return float(r.get('estimated_pnl_sol') or 0)
def bc(r):  return float(r.get('first_buy_bc_sol') or 0)
def nbw(r): return int(r.get('n_buyers_before_wallet') or 0)
def nb60(r):return int(r.get('n_buyers_in_60s') or 0)
def dev(r): return float(r.get('dev_buy_sol') or 0)
def sac(r): return int(r.get('seconds_after_creation') or 0)
def ntr(r): return int(r.get('n_token_transfers_at_create') or -1)
def hold(r):return int(r.get('hold_seconds') or 0)
def nbuys(r):return int(r.get('n_buys') or 1)
def bp60(r): return float(r.get('buy_pressure_sol_60s') or 0)
def co_buy(r): return bool(r.get('is_co_buy'))
def known_w(r): return bool(r.get('known_wallets_present'))

def is_win(r): return pnl(r) > 0
def wr(records): return sum(1 for r in records if is_win(r)) / len(records) * 100 if records else 0
def avg_pnl(records): return statistics.mean([pnl(r) for r in records]) if records else 0
def avg_win(records):
    wins = [pnl(r) for r in records if is_win(r)]
    return statistics.mean(wins) if wins else 0
def avg_loss(records):
    losses = [pnl(r) for r in records if not is_win(r)]
    return statistics.mean(losses) if losses else 0

def print_table(title, rows, cols=None):
    if cols is None:
        cols = ['Bucket', 'N', 'WR%', 'AvgPnL', 'AvgWin', 'AvgLoss']
    print(f"\n### {title}")
    header = ' | '.join(f'{c:>12}' for c in cols)
    print('| ' + header + ' |')
    print('|' + '|'.join(['-'*14]*len(cols)) + '|')
    for row in rows:
        line = ' | '.join(f'{str(v):>12}' for v in row)
        print('| ' + line + ' |')

def bucket_analysis(records, key_fn, buckets, title):
    """Анализирует по бакетам."""
    rows = []
    for label, lo, hi in buckets:
        subset = [r for r in records if lo <= key_fn(r) < hi]
        if not subset: continue
        rows.append([
            label, len(subset),
            f'{wr(subset):.1f}%',
            f'{avg_pnl(subset):.4f}',
            f'{avg_win(subset):.4f}',
            f'{avg_loss(subset):.4f}'
        ])
    print_table(title, rows)

# ── BASELINE ──────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("## BASELINE — nya666")
print("="*70)
print(f"Всего: {len(nya)} | WR: {wr(nya):.1f}% | AvgPnL: {avg_pnl(nya):.4f} SOL")
print(f"AvgWin: {avg_win(nya):.4f} SOL | AvgLoss: {avg_loss(nya):.4f} SOL")

# Golden определение (nbw=0 + nb60>=91)
golden = [r for r in nya if nbw(r) == 0 and nb60(r) >= 91]
print(f"\nGolden (nbw=0, nb60≥91): {len(golden)} | WR: {wr(golden):.1f}% | AvgPnL: {avg_pnl(golden):.4f}")
print(f"AvgWin golden: {avg_win(golden):.4f} | AvgLoss golden: {avg_loss(golden):.4f}")

# ── АНАЛИЗ 1: N_BUYERS_BEFORE_WALLET (NBW) ───────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 1: N_BUYERS_BEFORE_WALLET (NBW)")
print("="*70)

bucket_analysis(nya, nbw, [
    ('nbw=0',     0, 1),
    ('nbw=1',     1, 2),
    ('nbw=2',     2, 3),
    ('nbw=3',     3, 4),
    ('nbw=4-5',   4, 6),
    ('nbw=6-10',  6, 11),
    ('nbw=11-20', 11, 21),
    ('nbw=21+',   21, 9999),
], "NBW — ВСЕ сделки")

# ── АНАЛИЗ 2: N_BUYERS_IN_60S (NB60) ─────────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 2: N_BUYERS_IN_60S (NB60)")
print("="*70)

bucket_analysis(nya, nb60, [
    ('nb60=0-10',  0, 11),
    ('nb60=11-30', 11, 31),
    ('nb60=31-60', 31, 61),
    ('nb60=61-80', 61, 81),
    ('nb60=81-90', 81, 91),
    ('nb60=91-96', 91, 97),
    ('nb60=96',    96, 97),
    ('nb60=97',    97, 98),
    ('nb60=98',    98, 99),
    ('nb60=99',    99, 100),
    ('nb60=100',   100, 101),
], "NB60 — ВСЕ сделки")

# NB60 в golden (nbw=0)
nbw0 = [r for r in nya if nbw(r) == 0]
bucket_analysis(nbw0, nb60, [
    ('nb60<91',    0, 91),
    ('nb60=91-95', 91, 96),
    ('nb60=96',    96, 97),
    ('nb60=97-98', 97, 99),
    ('nb60=99',    99, 100),
    ('nb60=100',   100, 101),
], "NB60 при NBW=0")

# ── АНАЛИЗ 3: BC SOL ──────────────────────────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 3: BC SOL (first_buy_bc_sol)")
print("="*70)

bucket_analysis(nya, bc, [
    ('0-0.3',    0,   0.3),
    ('0.3-0.5',  0.3, 0.5),
    ('0.5-0.7',  0.5, 0.7),
    ('0.7-1.0',  0.7, 1.0),
    ('1.0-1.5',  1.0, 1.5),
    ('1.5-2.0',  1.5, 2.0),
    ('2.0-2.5',  2.0, 2.5),
    ('2.5-3.0',  2.5, 3.0),
    ('3.0-4.0',  3.0, 4.0),
    ('4.0-5.0',  4.0, 5.0),
    ('5.0+',     5.0, 999),
], "BC SOL — ВСЕ сделки")

bucket_analysis(golden, bc, [
    ('0-0.3',    0,   0.3),
    ('0.3-0.7',  0.3, 0.7),
    ('0.7-1.0',  0.7, 1.0),
    ('1.0-1.5',  1.0, 1.5),
    ('1.5-2.0',  1.5, 2.0),
    ('2.0-2.5',  2.0, 2.5),
    ('2.5-3.0',  2.5, 3.0),
    ('3.0+',     3.0, 999),
], "BC SOL — GOLDEN сделки")

# ── АНАЛИЗ 4: N_TOKEN_TRANSFERS_AT_CREATE ─────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 4: N_TOKEN_TRANSFERS_AT_CREATE")
print("="*70)

bucket_analysis(nya, ntr, [
    ('0 (fresh)', 0, 1),
    ('1',         1, 2),
    ('2',         2, 3),
    ('3-5',       3, 6),
    ('6-10',      6, 11),
    ('11+',       11, 9999),
], "N_TRANSFERS — ВСЕ сделки")

bucket_analysis(golden, ntr, [
    ('0 (fresh)', 0, 1),
    ('1',         1, 2),
    ('2',         2, 3),
    ('3-5',       3, 6),
    ('6-10',      6, 11),
    ('11+',       11, 9999),
], "N_TRANSFERS — GOLDEN сделки")

# ── АНАЛИЗ 5: DEV BUY SOL ─────────────────────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 5: DEV BUY SOL")
print("="*70)

bucket_analysis(nya, dev, [
    ('dev=0',      0,    0.001),
    ('dev 0-0.05', 0.001, 0.05),
    ('dev 0.05-0.1', 0.05, 0.1),
    ('dev 0.1-0.2', 0.1, 0.2),
    ('dev 0.2-0.5', 0.2, 0.5),
    ('dev 0.5+',   0.5, 999),
], "DEV BUY — ВСЕ сделки")

bucket_analysis(golden, dev, [
    ('dev=0',      0,    0.001),
    ('dev 0-0.05', 0.001, 0.05),
    ('dev 0.05-0.1', 0.05, 0.1),
    ('dev 0.1-0.2', 0.1, 0.2),
    ('dev 0.2+',   0.2, 999),
], "DEV BUY — GOLDEN сделки")

# ── АНАЛИЗ 6: SECONDS_AFTER_CREATION ─────────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 6: SECONDS_AFTER_CREATION (SAC)")
print("="*70)

sac_values = [sac(r) for r in nya]
neg_sac = [s for s in sac_values if s < 0]
pos_sac = [s for s in sac_values if s >= 0]
print(f"Отрицательный SAC: {len(neg_sac)} ({100*len(neg_sac)/len(nya):.1f}%)")
print(f"Положительный SAC: {len(pos_sac)} ({100*len(pos_sac)/len(nya):.1f}%)")

bucket_analysis(nya, sac, [
    ('SAC<-3600',   -99999999, -3600),
    ('SAC-3600/-1800', -3600, -1800),
    ('SAC-1800/-600',  -1800, -600),
    ('SAC-600/-300',   -600,  -300),
    ('SAC-300/-60',    -300,  -60),
    ('SAC-60/0',       -60,   0),
    ('SAC 0-30',        0,    30),
    ('SAC 30+',        30,    999999),
], "SAC — ВСЕ сделки")

bucket_analysis(golden, sac, [
    ('SAC<-3600',   -99999999, -3600),
    ('SAC-3600/-1800', -3600, -1800),
    ('SAC-1800/-600',  -1800, -600),
    ('SAC-600/-300',   -600,  -300),
    ('SAC-300/-60',    -300,  -60),
    ('SAC-60/0',       -60,   0),
], "SAC — GOLDEN сделки")

# ── АНАЛИЗ 7: BUY_PRESSURE_SOL_60S ───────────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 7: BUY_PRESSURE_SOL_60S")
print("="*70)

bucket_analysis(nya, bp60, [
    ('0-1 SOL',   0, 1),
    ('1-2 SOL',   1, 2),
    ('2-5 SOL',   2, 5),
    ('5-10 SOL',  5, 10),
    ('10-20 SOL', 10, 20),
    ('20-50 SOL', 20, 50),
    ('50+ SOL',   50, 999),
], "BUY PRESSURE 60s — ВСЕ сделки")

bucket_analysis(golden, bp60, [
    ('0-1 SOL',   0, 1),
    ('1-2 SOL',   1, 2),
    ('2-5 SOL',   2, 5),
    ('5-10 SOL',  5, 10),
    ('10-20 SOL', 10, 20),
    ('20-50 SOL', 20, 50),
    ('50+ SOL',   50, 999),
], "BUY PRESSURE 60s — GOLDEN сделки")

# ── АНАЛИЗ 8: IS_CO_BUY и KNOWN_WALLETS_PRESENT ──────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 8: CO_BUY и KNOWN_WALLETS")
print("="*70)

for label, subset in [("ВСЕ", nya), ("GOLDEN", golden)]:
    co = [r for r in subset if co_buy(r)]
    noco = [r for r in subset if not co_buy(r)]
    kw = [r for r in subset if known_w(r)]
    nokw = [r for r in subset if not known_w(r)]
    print(f"\n{label} ({len(subset)} сделок):")
    print(f"  is_co_buy=True:    N={len(co):5d}  WR={wr(co):.1f}%  AvgPnL={avg_pnl(co):.4f}")
    print(f"  is_co_buy=False:   N={len(noco):5d}  WR={wr(noco):.1f}%  AvgPnL={avg_pnl(noco):.4f}")
    print(f"  known_wallets=T:   N={len(kw):5d}  WR={wr(kw):.1f}%  AvgPnL={avg_pnl(kw):.4f}")
    print(f"  known_wallets=F:   N={len(nokw):5d}  WR={wr(nokw):.1f}%  AvgPnL={avg_pnl(nokw):.4f}")

# co_buy partner details
co_partners = collections.Counter()
for r in nya:
    for w in (r.get('co_buy_wallets') or []):
        co_partners[w] += 1
print("\nТоп co_buy партнёры nya666:")
for w, cnt in co_partners.most_common(10):
    subset_co = [r for r in nya if w in (r.get('co_buy_wallets') or [])]
    print(f"  {w[:20]}...  N={cnt}  WR={wr(subset_co):.1f}%  AvgPnL={avg_pnl(subset_co):.4f}")

# ── АНАЛИЗ 9: N_BUYS (DCA поведение) ─────────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 9: N_BUYS (DCA клифф)")
print("="*70)

for label, subset in [("ВСЕ", nya), ("GOLDEN", golden)]:
    print(f"\n{label}:")
    for nb in [1, 2, 3, 4, 5]:
        s = [r for r in subset if nbuys(r) == nb]
        if s:
            print(f"  n_buys={nb}: N={len(s):5d}  WR={wr(s):.1f}%  AvgPnL={avg_pnl(s):.4f}")
    s5 = [r for r in subset if nbuys(r) >= 5]
    if s5:
        print(f"  n_buys≥5:  N={len(s5):5d}  WR={wr(s5):.1f}%  AvgPnL={avg_pnl(s5):.4f}")

# ── АНАЛИЗ 10: HOLD TIME (EXIT TIMING) ───────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 10: HOLD TIME — EXIT TIMING")
print("="*70)

# Winners hold time
gold_wins = [r for r in golden if is_win(r)]
gold_losses = [r for r in golden if not is_win(r)]
hold_wins = sorted([hold(r) for r in gold_wins])
hold_losses = sorted([hold(r) for r in gold_losses])

def pct(lst, p): return lst[int(len(lst)*p/100)] if lst else 0
print(f"\nGolden WINNERS ({len(gold_wins)}) hold time percentiles:")
for p in [10,25,50,75,90,95,99]:
    print(f"  P{p}: {pct(hold_wins, p)}s")

print(f"\nGolden LOSERS ({len(gold_losses)}) hold time percentiles:")
for p in [10,25,50,75,90]:
    print(f"  P{p}: {pct(hold_losses, p)}s")

# PnL по hold бакетам для golden winners
bucket_analysis(gold_wins, hold, [
    ('0-30s',    0, 30),
    ('30-60s',   30, 60),
    ('60-120s',  60, 120),
    ('120-300s', 120, 300),
    ('300-600s', 300, 600),
    ('600-900s', 600, 900),
    ('900-1800s',900, 1800),
    ('1800-3600s',1800, 3600),
    ('3600s+',   3600, 9999999),
], "PnL по hold time — Golden WINNERS")

# ── АНАЛИЗ 11: ЧАСЫ UTC ───────────────────────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 11: ЧАСЫ UTC")
print("="*70)

def get_hour(r):
    ts = r.get('first_buy_ts') or r.get('create_ts') or 0
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).hour if ts else -1

print("\nВСЕ сделки по часам UTC:")
hour_rows = []
for h in range(24):
    s = [r for r in nya if get_hour(r) == h]
    if s:
        hour_rows.append([f'h={h:02d}UTC', len(s), f'{wr(s):.1f}%', f'{avg_pnl(s):.4f}'])
print('| ' + ' | '.join(f'{c:>12}' for c in ['Hour', 'N', 'WR%', 'AvgPnL']) + ' |')
for row in hour_rows:
    print('| ' + ' | '.join(f'{str(v):>12}' for v in row) + ' |')

print("\nGOLDEN сделки по часам UTC:")
for h in range(24):
    s = [r for r in golden if get_hour(r) == h]
    if s:
        print(f"  h={h:02d}: N={len(s):4d}  WR={wr(s):.1f}%  AvgPnL={avg_pnl(s):.4f}")

# ── АНАЛИЗ 12: SYMBOL LENGTH ──────────────────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 12: SYMBOL LENGTH")
print("="*70)

def sym_len(r): return len(r.get('symbol') or '')
bucket_analysis(golden, sym_len, [
    ('len=1', 1, 2),
    ('len=2', 2, 3),
    ('len=3', 3, 4),
    ('len=4', 4, 5),
    ('len=5', 5, 6),
    ('len=6', 6, 7),
    ('len=7+', 7, 99),
], "Symbol Length — GOLDEN")

# ── АНАЛИЗ 13: КОМБО-ФИЛЬТРЫ ─────────────────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 13: КОМБО-ФИЛЬТРЫ (поиск лучшего сочетания)")
print("="*70)

# Базовый golden
print(f"\nБазовый golden (nbw=0, nb60≥91): N={len(golden)}, WR={wr(golden):.1f}%, AvgPnL={avg_pnl(golden):.4f}")

# + BC < 2.5
g1 = [r for r in golden if bc(r) < 2.5]
print(f"+ BC<2.5:                 N={len(g1):5d}, WR={wr(g1):.1f}%, AvgPnL={avg_pnl(g1):.4f}")

# + BC 0.3-2.5
g2 = [r for r in golden if 0.3 <= bc(r) < 2.5]
print(f"+ BC 0.3-2.5:             N={len(g2):5d}, WR={wr(g2):.1f}%, AvgPnL={avg_pnl(g2):.4f}")

# + BC 0.3-2.5 + n_transfers≠1
g3 = [r for r in g2 if ntr(r) != 1]
print(f"+ BC 0.3-2.5 + ntr≠1:     N={len(g3):5d}, WR={wr(g3):.1f}%, AvgPnL={avg_pnl(g3):.4f}")

# + dev < 0.1
g4 = [r for r in g3 if dev(r) < 0.1]
print(f"+ dev<0.1:                N={len(g4):5d}, WR={wr(g4):.1f}%, AvgPnL={avg_pnl(g4):.4f}")

# + n_buys=1
g5 = [r for r in g4 if nbuys(r) == 1]
print(f"+ n_buys=1:               N={len(g5):5d}, WR={wr(g5):.1f}%, AvgPnL={avg_pnl(g5):.4f}")

# + исключить часы 7-9
def not_bad_hour(r): return get_hour(r) not in [7, 8, 9]
g6 = [r for r in g5 if not_bad_hour(r)]
print(f"+ avoid 7-9 UTC:          N={len(g6):5d}, WR={wr(g6):.1f}%, AvgPnL={avg_pnl(g6):.4f}")

# + is_co_buy
g7 = [r for r in g5 if co_buy(r)]  # без hour filter — кол-во
print(f"\n+ is_co_buy=True (всё):   N={len(g7):5d}, WR={wr(g7):.1f}%, AvgPnL={avg_pnl(g7):.4f}")
g7b = [r for r in g6 if co_buy(r)]
print(f"+ все фильтры + co_buy:   N={len(g7b):5d}, WR={wr(g7b):.1f}%, AvgPnL={avg_pnl(g7b):.4f}")

# Попробуем более широкий nb60 порог — что если 81?
g_81 = [r for r in nya if nbw(r) == 0 and nb60(r) >= 81]
print(f"\ngolden nb60≥81 (шире):    N={len(g_81):5d}, WR={wr(g_81):.1f}%, AvgPnL={avg_pnl(g_81):.4f}")
g_81f = [r for r in g_81 if 0.3 <= bc(r) < 2.5 and ntr(r) != 1 and dev(r) < 0.1 and nbuys(r) == 1]
print(f"+ все фильтры:            N={len(g_81f):5d}, WR={wr(g_81f):.1f}%, AvgPnL={avg_pnl(g_81f):.4f}")

# ── АНАЛИЗ 14: BUY_PRESSURE + GOLDEN ─────────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 14: BUY PRESSURE как фильтр")
print("="*70)

bucket_analysis(golden, bp60, [
    ('bp<2',   0, 2),
    ('bp 2-5', 2, 5),
    ('bp 5-10', 5, 10),
    ('bp 10-20', 10, 20),
    ('bp 20-50', 20, 50),
    ('bp 50+',  50, 9999),
], "Buy Pressure — GOLDEN")

# Оптимальный порог BP для золотых
for bp_min in [0, 1, 2, 5, 10, 15, 20]:
    s = [r for r in golden if bp60(r) >= bp_min]
    print(f"  bp≥{bp_min:3d}: N={len(s):5d}  WR={wr(s):.1f}%  AvgPnL={avg_pnl(s):.4f}")

# ── АНАЛИЗ 15: СРАВНЕНИЕ nya666 vs D5dtjf ────────────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 15: nya666 vs D5dtjf СРАВНЕНИЕ")
print("="*70)

for label, subset in [("nya666", nya), ("D5dtjf", d5d)]:
    gold_s = [r for r in subset if nbw(r) == 0 and nb60(r) >= 91]
    print(f"\n{label}:")
    print(f"  Всего: {len(subset)} | WR: {wr(subset):.1f}% | AvgPnL: {avg_pnl(subset):.4f}")
    print(f"  Golden: {len(gold_s)} ({100*len(gold_s)/len(subset):.1f}%) | WR: {wr(gold_s):.1f}% | AvgPnL: {avg_pnl(gold_s):.4f}")
    print(f"  BC med: {statistics.median([bc(r) for r in subset]):.2f} SOL | BC mean: {statistics.mean([bc(r) for r in subset]):.2f}")
    print(f"  Dev=0: {100*sum(1 for r in subset if dev(r) < 0.001)/len(subset):.1f}% | Dev≥0.1: {100*sum(1 for r in subset if dev(r) >= 0.1)/len(subset):.1f}%")
    print(f"  n_buys=1: {100*sum(1 for r in subset if nbuys(r) == 1)/len(subset):.1f}%")
    print(f"  is_co_buy: {100*sum(1 for r in subset if co_buy(r))/len(subset):.1f}%")

# ── АНАЛИЗ 16: KNOWN_WALLETS_PRESENT как фильтр ───────────────────────────────
print("\n" + "="*70)
print("## АНАЛИЗ 16: KNOWN_WALLETS_PRESENT")
print("="*70)

kw_vals = [r.get('known_wallets_present') for r in nya[:100]]
print(f"Sample known_wallets_present values: {set(kw_vals)}")

# Если это список или строка — проверим
kw_sample = nya[0].get('known_wallets_present')
print(f"Type: {type(kw_sample)}, Value: {kw_sample}")

for label, subset in [("ВСЕ", nya), ("GOLDEN", golden)]:
    kw_true = [r for r in subset if r.get('known_wallets_present')]
    kw_false = [r for r in subset if not r.get('known_wallets_present')]
    print(f"\n{label}: known_wallets_present=True: N={len(kw_true)}, WR={wr(kw_true):.1f}%, AvgPnL={avg_pnl(kw_true):.4f}")
    print(f"{label}: known_wallets_present=False: N={len(kw_false)}, WR={wr(kw_false):.1f}%, AvgPnL={avg_pnl(kw_false):.4f}")

# ── ИТОГОВЫЕ РЕКОМЕНДАЦИИ ─────────────────────────────────────────────────────
print("\n" + "="*70)
print("## ИТОГОВЫЕ РЕКОМЕНДАЦИИ")
print("="*70)
print("(заполняются после интерпретации результатов выше)")
