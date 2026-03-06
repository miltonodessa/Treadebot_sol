"""
Wallet analysis script for Solana copy-trading bot.
Analyzes trading patterns from exported GMGN/Helius Excel reports.
"""

import pandas as pd
import json
from datetime import datetime, timezone
from pathlib import Path

WALLET_FILES = {
    "2TE2F3CJDbDhN3MgqNmnUh1NR5nHSornaK5daq65Lejs": "2TE2F3CJDbDhN3MgqNmn_full_report.xlsx",
    "AF6syABApBp7d1NfjUkmKG7BBBEWVMvXadyvqQgjLnRN": "AF6syABApBp7d1NfjUkm_full_report.xlsx",
}


def parse_token_list(val) -> list[dict]:
    if not val or (isinstance(val, float)):
        return []
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return []
    return val if isinstance(val, list) else []


def analyze_wallet(wallet_address: str, xlsx_path: str) -> dict:
    path = Path(xlsx_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {xlsx_path}")

    # --- Summary ---
    df_summary = pd.read_excel(path, sheet_name="Summary")
    summary = dict(zip(df_summary["Metric"], df_summary["Value"]))

    # --- Swaps ---
    df = pd.read_excel(path, sheet_name="Swaps")
    df["block_time_utc"] = pd.to_datetime(df["block_time_utc"], utc=True)
    df["hour_utc"] = df["block_time_utc"].dt.hour
    df["date"] = df["block_time_utc"].dt.date

    buys = df[df["sol_delta"] < 0]
    sells = df[df["sol_delta"] > 0]

    # Token stats
    mints = set()
    for col in ["tokens_in", "tokens_out"]:
        for val in df[col].dropna():
            for t in parse_token_list(val):
                if "mint" in t:
                    mints.add(t["mint"])

    # Hourly distribution
    hourly = df.groupby("hour_utc").size().reindex(range(24), fill_value=0).to_dict()

    # Peak hours (top 3)
    peak_hours = sorted(hourly.items(), key=lambda x: x[1], reverse=True)[:5]

    # Daily PnL
    daily_pnl = df.groupby("date")["sol_delta"].sum()

    # Is it an evening-only bot? Check if >70% of trades happen between 15-23h UTC
    evening_trades = sum(v for h, v in hourly.items() if 15 <= h <= 23)
    evening_ratio = evening_trades / max(len(df), 1)

    # DEX usage
    dex_usage: dict[str, int] = {}
    for val in df["dex_programs"].dropna():
        for dex in parse_token_list(val):
            if isinstance(dex, str):
                dex_usage[dex] = dex_usage.get(dex, 0) + 1

    gross_pnl = df["sol_delta"].sum()
    total_fees = df["fee_sol"].sum()

    return {
        "wallet": wallet_address,
        "summary": {
            "total_transactions": int(summary.get("total_transactions", 0)),
            "successful_transactions": int(summary.get("successful_transactions", 0)),
            "failed_transactions": int(summary.get("failed_transactions", 0)),
            "net_sol_change": float(summary.get("net_sol_change", 0)),
            "total_fees_sol": float(summary.get("total_fees_sol", 0)),
        },
        "trading": {
            "total_swaps": len(df),
            "buys": len(buys),
            "sells": len(sells),
            "gross_sol_pnl": round(gross_pnl, 6),
            "total_swap_fees_sol": round(total_fees, 6),
            "net_swap_pnl": round(gross_pnl - total_fees, 6),
            "avg_buy_sol": round(buys["sol_delta"].mean(), 4) if len(buys) > 0 else 0,
            "avg_sell_sol": round(sells["sol_delta"].mean(), 4) if len(sells) > 0 else 0,
            "max_sell_sol": round(df["sol_delta"].max(), 4),
            "max_buy_sol": round(df["sol_delta"].min(), 4),
            "active_days": df["date"].nunique(),
            "unique_tokens_traded": len(mints),
            "dex_usage": dex_usage,
        },
        "behavior": {
            "hourly_distribution": hourly,
            "peak_hours_utc": [h for h, _ in peak_hours],
            "evening_only_ratio": round(evening_ratio, 3),
            "is_evening_bot": evening_ratio > 0.60,
            "avg_swaps_per_day": round(len(df) / max(df["date"].nunique(), 1), 1),
        },
        "daily_pnl": {str(d): round(v, 4) for d, v in daily_pnl.items()},
    }


def print_report(data: dict):
    w = data["wallet"]
    s = data["summary"]
    t = data["trading"]
    b = data["behavior"]

    print("=" * 60)
    print(f"WALLET: {w}")
    print("=" * 60)

    print("\n--- SUMMARY ---")
    print(f"  Total txs:       {s['total_transactions']:,}")
    print(f"  Success rate:    {s['successful_transactions'] / max(s['total_transactions'], 1) * 100:.1f}%")
    print(f"  Net SOL change:  {s['net_sol_change']:+.4f} SOL")
    print(f"  Total fees paid: {s['total_fees_sol']:.4f} SOL")

    print("\n--- TRADING ---")
    print(f"  Total swaps:     {t['total_swaps']:,}  (buys: {t['buys']}, sells: {t['sells']})")
    print(f"  Gross swap PnL:  {t['gross_sol_pnl']:+.4f} SOL")
    print(f"  Net swap PnL:    {t['net_swap_pnl']:+.4f} SOL")
    print(f"  Avg buy size:    {t['avg_buy_sol']:.4f} SOL")
    print(f"  Avg sell size:   {t['avg_sell_sol']:.4f} SOL")
    print(f"  Active days:     {t['active_days']}")
    print(f"  Unique tokens:   {t['unique_tokens_traded']}")
    if t["dex_usage"]:
        print(f"  DEX used:        {', '.join(t['dex_usage'].keys())}")
    else:
        print("  DEX used:        Custom router (no standard DEX tag)")

    print("\n--- BEHAVIOR ---")
    print(f"  Avg swaps/day:   {b['avg_swaps_per_day']}")
    print(f"  Evening-only:    {'YES' if b['is_evening_bot'] else 'NO'} ({b['evening_only_ratio']*100:.0f}% trades 15-23h UTC)")
    print(f"  Peak hours UTC:  {b['peak_hours_utc']}")

    print("\n--- HOURLY HEATMAP (UTC) ---")
    dist = b["hourly_distribution"]
    max_val = max(dist.values()) or 1
    for h in range(24):
        cnt = dist.get(h, 0)
        bar = "█" * int(cnt / max_val * 30)
        print(f"  {h:02d}h | {bar:<30} {cnt:4d}")

    print("\n--- LAST 10 DAYS PnL ---")
    daily = list(data["daily_pnl"].items())[-10:]
    for d, v in daily:
        arrow = "▲" if v > 0 else "▼"
        print(f"  {d}  {arrow}  {v:+.4f} SOL")
    print()


def main():
    results = []
    for wallet, fname in WALLET_FILES.items():
        print(f"\nAnalyzing {wallet[:8]}...")
        try:
            data = analyze_wallet(wallet, fname)
            print_report(data)
            results.append(data)
        except FileNotFoundError as e:
            print(f"  ERROR: {e}")

    # Comparison
    if len(results) == 2:
        a, b = results
        print("=" * 60)
        print("COMPARISON")
        print("=" * 60)
        print(f"{'Metric':<30} {'Wallet A':>15} {'Wallet B':>15}")
        print("-" * 60)
        metrics = [
            ("Net swap PnL (SOL)", "trading.net_swap_pnl"),
            ("Total swaps", "trading.total_swaps"),
            ("Avg swaps/day", "behavior.avg_swaps_per_day"),
            ("Unique tokens", "trading.unique_tokens_traded"),
            ("Evening-only", "behavior.is_evening_bot"),
        ]
        for label, key in metrics:
            parts = key.split(".")
            va = results[0]
            vb = results[1]
            for p in parts:
                va = va[p]
                vb = vb[p]
            print(f"  {label:<28} {str(va):>15} {str(vb):>15}")


if __name__ == "__main__":
    main()
