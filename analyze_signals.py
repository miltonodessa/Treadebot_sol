"""
Analyze token_signals.json to find selection patterns from existing data.
Then extends records with Helius API data (creation time, dev buy, early buyers).
"""
import json
import numpy as np
from collections import Counter, defaultdict
from pathlib import Path
import os

SIGNALS_FILE = Path("token_signals.json")


def load_data() -> list[dict]:
    with open(SIGNALS_FILE) as f:
        return json.load(f)


def analyze_existing(data: list[dict]):
    print(f"\n{'='*60}")
    print(f"АНАЛИЗ {len(data)} токенов (данные из xlsx)")
    print(f"{'='*60}")

    # --- 1. BC pre-SOL distribution (market cap at entry) ---
    bc_vals = [r["nya666_first_buy_bc_sol"] for r in data if r.get("nya666_first_buy_bc_sol")]
    print(f"\n[1] BC SOL ПРИ ПЕРВОЙ ПОКУПКЕ (рыночная капа на входе):")
    print(f"  Записей с BC данными: {len(bc_vals)}")
    print(f"  Median:  {np.median(bc_vals):.3f} SOL")
    print(f"  P10:     {np.percentile(bc_vals, 10):.3f} SOL")
    print(f"  P25:     {np.percentile(bc_vals, 25):.3f} SOL")
    print(f"  P75:     {np.percentile(bc_vals, 75):.3f} SOL")
    print(f"  P90:     {np.percentile(bc_vals, 90):.3f} SOL")
    print(f"  Max:     {max(bc_vals):.1f} SOL")

    buckets = [
        (0, 1,   "VERY EARLY (0-1 SOL)"),
        (1, 3,   "EARLY (1-3 SOL)"),
        (3, 7,   "FRESH (3-7 SOL)"),
        (7, 15,  "MID (7-15 SOL)"),
        (15, 30, "PUMPED (15-30 SOL)"),
        (30, 99999, "LATE (30+ SOL)"),
    ]
    print()
    for lo, hi, label in buckets:
        cnt = sum(1 for v in bc_vals if lo <= v < hi)
        pct = cnt / len(bc_vals) * 100
        bar = "█" * int(pct / 2)
        print(f"  {label:<25} {cnt:5d} ({pct:5.1f}%) {bar}")

    # --- 2. Position size tiers ---
    sizes = [r["nya666_first_buy_sol"] for r in data if r.get("nya666_first_buy_sol")]
    print(f"\n[2] РАЗМЕР ПЕРВОЙ ПОКУПКИ (тиры):")
    tier_map = [
        (0,    0.12, "Tier 1: ~0.09-0.10 SOL (probe)"),
        (0.12, 0.25, "Tier 2: ~0.20 SOL"),
        (0.25, 0.38, "Tier 3: ~0.30 SOL"),
        (0.38, 0.55, "Tier 4: ~0.40-0.50 SOL"),
        (0.55, 0.90, "Tier 5: ~0.60-0.80 SOL"),
        (0.90, 1.50, "Tier 6: ~1.0 SOL"),
        (1.50, 9999, "Tier 7: 2.0+ SOL (исключительный)"),
    ]
    for lo, hi, label in tier_map:
        cnt = sum(1 for s in sizes if lo <= s < hi)
        pct = cnt / len(sizes) * 100
        bar = "█" * int(pct / 2)
        print(f"  {label:<40} {cnt:5d} ({pct:5.1f}%) {bar}")

    # --- 3. BC при входе vs размер позиции ---
    print(f"\n[3] BC SOL НА ВХОДЕ vs РАЗМЕР ПОЗИЦИИ (ключевая корреляция):")
    print(f"  {'Тир':<12} {'Медиана BC':>12} {'P25 BC':>8} {'P75 BC':>8} {'N':>6}")
    print(f"  {'-'*50}")
    for lo, hi, label in tier_map:
        group = [r for r in data
                 if r.get("nya666_first_buy_sol") and lo <= r["nya666_first_buy_sol"] < hi
                 and r.get("nya666_first_buy_bc_sol")]
        if len(group) < 5:
            continue
        bcs = [r["nya666_first_buy_bc_sol"] for r in group]
        name = label.split(":")[0]
        print(f"  {name:<12} {np.median(bcs):>12.3f} {np.percentile(bcs,25):>8.3f} {np.percentile(bcs,75):>8.3f} {len(group):>6}")

    # --- 4. Число повторных покупок vs первый размер ---
    print(f"\n[4] КОЛ-ВО ПОВТОРНЫХ ПОКУПОК vs ПЕРВЫЙ РАЗМЕР:")
    print(f"  (Если бот добавляет к победителям → больший тир = больше re-buys)")
    for lo, hi, label in tier_map:
        group = [r for r in data
                 if r.get("nya666_first_buy_sol") and lo <= r["nya666_first_buy_sol"] < hi
                 and r.get("nya666_n_buys")]
        if len(group) < 5:
            continue
        n_buys = [r["nya666_n_buys"] for r in group]
        multi = sum(1 for n in n_buys if n > 1)
        name = label.split(":")[0]
        print(f"  {name:<12} avg_buys={np.mean(n_buys):.2f}  multi={multi}/{len(group)} ({multi/len(group)*100:.0f}%)")

    # --- 5. BC при входе у токенов с МНОГИМИ повторными покупками ---
    print(f"\n[5] ТОКЕНЫ С МНОГИМИ ПОКУПКАМИ (самые активные):")
    multi = sorted(data, key=lambda r: r.get("nya666_n_buys", 0), reverse=True)[:20]
    print(f"  {'N buys':>7} {'First SOL':>10} {'BC entry':>10} {'Total SOL':>10}  Mint")
    for r in multi:
        print(f"  {r.get('nya666_n_buys',0):>7} {r.get('nya666_first_buy_sol',0):>10.3f} "
              f"{r.get('nya666_first_buy_bc_sol',0):>10.3f} "
              f"{r.get('nya666_total_invested_sol',0):>10.3f}  {r['mint'][:20]}...")

    # --- 6. Самые ранние входы (BC < 1 SOL) ---
    very_early = [r for r in data if r.get("nya666_first_buy_bc_sol", 99) < 1.0]
    print(f"\n[6] ОЧЕНЬ РАННИЕ ВХОДЫ (BC < 1.0 SOL): {len(very_early)} токенов")
    if very_early:
        sizes_ve = [r["nya666_first_buy_sol"] for r in very_early if r.get("nya666_first_buy_sol")]
        print(f"  Медиана первой покупки: {np.median(sizes_ve):.3f} SOL")
        print(f"  Самые ранние входы:")
        very_early_sorted = sorted(very_early, key=lambda r: r.get("nya666_first_buy_bc_sol", 99))
        for r in very_early_sorted[:10]:
            print(f"    BC={r['nya666_first_buy_bc_sol']:.4f} SOL  buy={r['nya666_first_buy_sol']:.3f} SOL  "
                  f"n_buys={r['nya666_n_buys']}  {r['mint'][:20]}...")

    # --- 7. Анализ временных паттернов ---
    from datetime import datetime, timezone
    hours = [datetime.fromtimestamp(r["nya666_first_buy_ts"], tz=timezone.utc).hour
             for r in data if r.get("nya666_first_buy_ts")]
    hour_counts = Counter(hours)
    peak_hour = max(hour_counts, key=hour_counts.get)
    print(f"\n[7] АКТИВНОСТЬ ПО ЧАСАМ UTC (пик: {peak_hour:02d}:00):")
    max_cnt = max(hour_counts.values())
    for h in range(24):
        cnt = hour_counts.get(h, 0)
        bar = "█" * int(cnt / max_cnt * 30)
        print(f"  {h:02d}:00  {cnt:5d}  {bar}")

    # --- 8. Выводы ---
    print(f"\n{'='*60}")
    print("ВЫВОДЫ ДЛЯ ЛОГИКИ БОТА:")
    print(f"{'='*60}")

    # BC filter
    p95_bc = np.percentile(bc_vals, 95)
    p5_bc = np.percentile(bc_vals, 5)
    print(f"\n1. ФИЛЬТР ВХОДА (BC):")
    print(f"   95% покупок при BC < {p95_bc:.1f} SOL")
    print(f"   → вероятный лимит: НЕ покупать если BC > {round(p95_bc/5)*5:.0f} SOL")

    # Size logic from BC
    early_group = [r for r in data if r.get("nya666_first_buy_bc_sol", 99) < 3]
    late_group  = [r for r in data if r.get("nya666_first_buy_bc_sol", 0) > 15]
    if early_group and late_group:
        early_sizes = [r["nya666_first_buy_sol"] for r in early_group if r.get("nya666_first_buy_sol")]
        late_sizes  = [r["nya666_first_buy_sol"] for r in late_group if r.get("nya666_first_buy_sol")]
        print(f"\n2. РАЗМЕР ПОЗИЦИИ vs МОМЕНТ ВХОДА:")
        print(f"   BC < 3 SOL  → медиана покупки {np.median(early_sizes):.3f} SOL (ранний вход)")
        print(f"   BC > 15 SOL → медиана покупки {np.median(late_sizes):.3f} SOL (поздний вход)")
        if np.median(early_sizes) > np.median(late_sizes) * 1.1:
            print(f"   ✓ ПОДТВЕРЖДЕНО: бот покупает БОЛЬШЕ при раннем входе")
        elif np.median(late_sizes) > np.median(early_sizes) * 1.1:
            print(f"   ✓ ПОДТВЕРЖДЕНО: бот покупает БОЛЬШЕ при позднем входе (momentum)")
        else:
            print(f"   → BC НЕ является основным фактором размера позиции")
            print(f"   → нужны данные о создании токена (Helius) для полного анализа")


def print_helius_instructions():
    print(f"\n{'='*60}")
    print("СЛЕДУЮЩИЙ ШАГ: HELIUS API")
    print(f"{'='*60}")
    print("""
Для полного анализа нужны данные о создании каждого токена.
Получи бесплатный API ключ: https://dev.helius.xyz

Затем запусти:
  export HELIUS_API_KEY=ваш_ключ
  python token_data_parser.py

Это добавит к каждой записи:
  - seconds_after_creation  (насколько рано вошел nya666)
  - dev_buy_sol             (сколько дев вложил при создании)
  - n_buyers_before_nya666  (сколько купили до него)
  - has_website/twitter     (есть ли метадата)

После этого мы сможем точно восстановить логику отбора токенов.
""")


if __name__ == "__main__":
    data = load_data()
    analyze_existing(data)
    print_helius_instructions()
