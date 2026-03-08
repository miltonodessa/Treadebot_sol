"""
ml_trainer.py — Ночной скрипт самообучения PUMPSCALP

Запускается каждую ночь (cron), читает накопленные сделки,
обучает модель и сохраняет оптимальные параметры в ml_params.json.

Архитектура обучения:
  1. Загрузка и валидация trade_log_master.csv
  2. Объединение с token_signals.json (растёт по мере парсинга)
  3. Анализ статистики по фазам, mcap, времени суток
  4. LightGBM классификатор (win/loss prediction)
  5. Optuna: оптимизация порогов входа по историческим данным
  6. Сохранение ml_params.json + ml_report.txt

Установка зависимостей (один раз):
  pip install lightgbm optuna scikit-learn pandas numpy
"""
from __future__ import annotations

import json
import os
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ─── Пути к файлам ────────────────────────────────────────────────────────────
MASTER_LOG        = os.getenv("MASTER_LOG",        "trade_log_master.csv")
TOKEN_SIGNALS     = os.getenv("TOKEN_SIGNALS",     "token_signals.json")
ML_PARAMS_FILE    = os.getenv("ML_PARAMS_FILE",    "ml_params.json")
ML_REPORT_FILE    = os.getenv("ML_REPORT_FILE",    "ml_report.txt")
ML_MODEL_FILE     = os.getenv("ML_MODEL_FILE",     "ml_model.pkl")
ML_HISTORY_FILE   = os.getenv("ML_HISTORY_FILE",   "ml_history.jsonl")

# ─── Минимум сделок для обучения ──────────────────────────────────────────────
MIN_TRADES_FOR_ML     = int(os.getenv("MIN_TRADES_FOR_ML",     "200"))
MIN_TRADES_FOR_OPTUNA = int(os.getenv("MIN_TRADES_FOR_OPTUNA", "100"))

# ─── Окно обучения: только последние N дней (рынок меняется) ──────────────────
TRAIN_WINDOW_DAYS = int(os.getenv("TRAIN_WINDOW_DAYS", "30"))

# ─── Optuna ───────────────────────────────────────────────────────────────────
OPTUNA_TRIALS = int(os.getenv("OPTUNA_TRIALS", "300"))


# ══════════════════════════════════════════════════════════════════════════════
# 1. ЗАГРУЗКА ДАННЫХ
# ══════════════════════════════════════════════════════════════════════════════

def load_trades() -> pd.DataFrame:
    """Загрузить накопительный лог сделок."""
    if not Path(MASTER_LOG).exists():
        print(f"[trainer] Файл {MASTER_LOG} не найден — бот ещё не торговал.")
        sys.exit(0)

    df = pd.read_csv(MASTER_LOG, parse_dates=["opened_at", "closed_at"])
    print(f"[trainer] Загружено {len(df)} сделок из {MASTER_LOG}")

    # Обрезаем по окну обучения
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=TRAIN_WINDOW_DAYS)
    cutoff = cutoff.tz_localize(None)
    if df["opened_at"].dtype.tz is not None:
        df["opened_at"] = df["opened_at"].dt.tz_localize(None)
    df = df[df["opened_at"] >= cutoff]
    print(f"[trainer] После фильтра {TRAIN_WINDOW_DAYS} дней: {len(df)} сделок")

    if len(df) < 10:
        print("[trainer] Слишком мало данных. Торгуй ещё!")
        sys.exit(0)

    return df.reset_index(drop=True)


def load_token_signals() -> dict:
    """Загрузить token_signals.json (может быть пустым/sparse)."""
    if not Path(TOKEN_SIGNALS).exists():
        return {}
    with open(TOKEN_SIGNALS, encoding="utf-8") as f:
        data = json.load(f)
    # Поддержка как list, так и dict
    if isinstance(data, list):
        return {item["mint"]: item for item in data if "mint" in item}
    return data


# ══════════════════════════════════════════════════════════════════════════════
# 2. FEATURE ENGINEERING
# ══════════════════════════════════════════════════════════════════════════════

def build_features(df: pd.DataFrame, signals: dict) -> pd.DataFrame:
    """Собрать признаки для ML-модели."""
    df = df.copy()

    # Целевая переменная
    df["won"] = (df["pnl_sol"] > 0).astype(int)

    # Временные признаки
    if df["opened_at"].dtype.tz is None:
        opened = df["opened_at"]
    else:
        opened = df["opened_at"].dt.tz_localize(None)
    df["hour_utc"]    = opened.dt.hour
    df["dow"]         = opened.dt.dayofweek   # 0=Пн, 6=Вс
    df["is_weekend"]  = (df["dow"] >= 5).astype(int)

    # Торговые признаки
    df["is_reentry"]       = df["is_reentry"].astype(int)
    df["passed_quick_stop"] = df["passed_quick_stop"].astype(int)
    df["passed_momentum"]   = df["passed_momentum"].astype(int)

    # Mcap buckets (логарифм лучше работает с деревьями)
    df["log_entry_mcap"] = np.log1p(df["entry_mcap_sol"].clip(0))

    # Обогащение из token_signals (если есть)
    if signals:
        def _get(mint, key, default=0.0):
            entry = signals.get(mint, {})
            return entry.get(key, default)

        df["sig_bc_sol"]          = df["mint"].apply(lambda m: _get(m, "nya666_first_buy_bc_sol"))
        df["sig_first_buy_sol"]   = df["mint"].apply(lambda m: _get(m, "nya666_first_buy_sol"))
        df["sig_n_buys"]          = df["mint"].apply(lambda m: _get(m, "nya666_n_buys"))
        df["has_signals"]         = (df["sig_bc_sol"] > 0).astype(int)

        # Расширенные поля (появятся когда parser обогатит данные)
        df["dev_buy_sol"]         = df["mint"].apply(lambda m: _get(m, "dev_buy_sol"))
        df["early_buyers"]        = df["mint"].apply(lambda m: _get(m, "early_buyers_count"))
        df["buyers_before_me"]    = df["mint"].apply(lambda m: _get(m, "buyers_before_me"))
        df["co_buy"]              = df["mint"].apply(lambda m: int(_get(m, "co_buy_signal", False)))
        df["has_twitter"]         = df["mint"].apply(lambda m: int(_get(m, "has_twitter", False)))
        df["has_website"]         = df["mint"].apply(lambda m: int(_get(m, "has_website", False)))
        df["creation_age_sec"]    = df["mint"].apply(lambda m: _get(m, "entry_speed_sec"))

    return df


# ══════════════════════════════════════════════════════════════════════════════
# 3. СТАТИСТИЧЕСКИЙ АНАЛИЗ
# ══════════════════════════════════════════════════════════════════════════════

def analyze_stats(df: pd.DataFrame) -> dict:
    """Глубокая статистика по сделкам — понять что работает."""
    report = {}

    wins  = df[df["won"] == 1]
    loses = df[df["won"] == 0]

    report["total"]    = len(df)
    report["win_rate"] = round(len(wins) / len(df) * 100, 2)
    report["pnl_total_sol"] = round(df["pnl_sol"].sum(), 4)
    report["avg_win_pct"]   = round(wins["pnl_pct"].mean(), 2) if len(wins) else 0
    report["avg_loss_pct"]  = round(loses["pnl_pct"].mean(), 2) if len(loses) else 0
    report["avg_hold_sec"]  = round(df["hold_sec"].mean(), 1)
    report["ev_per_trade"]  = round(df["pnl_pct"].mean(), 3)

    # По фазам (quick_stop / momentum / active)
    phases = {
        "quick_stop":  df[(df["passed_quick_stop"] == 0)],
        "momentum":    df[(df["passed_quick_stop"] == 1) & (df["passed_momentum"] == 0)],
        "active":      df[(df["passed_quick_stop"] == 1) & (df["passed_momentum"] == 1)],
    }
    report["by_phase"] = {}
    for name, sub in phases.items():
        if len(sub):
            report["by_phase"][name] = {
                "count":    len(sub),
                "wr_pct":   round((sub["won"] == 1).mean() * 100, 1),
                "avg_pnl":  round(sub["pnl_sol"].mean(), 4),
            }

    # По mcap (выровнять на логарифмической шкале)
    df["mcap_bin"] = pd.cut(
        df["entry_mcap_sol"],
        bins=[0, 3, 6, 12, 25, 50, 100, float("inf")],
        labels=["<3", "3-6", "6-12", "12-25", "25-50", "50-100", ">100"],
    )
    report["by_mcap"] = {}
    for label, sub in df.groupby("mcap_bin", observed=True):
        if len(sub) >= 5:
            report["by_mcap"][str(label)] = {
                "count":   len(sub),
                "wr_pct":  round((sub["won"] == 1).mean() * 100, 1),
                "avg_pnl": round(sub["pnl_sol"].mean(), 4),
            }

    # По часу UTC (лучшее время торговли)
    report["by_hour"] = {}
    for hour, sub in df.groupby("hour_utc"):
        if len(sub) >= 10:
            report["by_hour"][int(hour)] = {
                "count":  len(sub),
                "wr_pct": round((sub["won"] == 1).mean() * 100, 1),
            }

    # Re-entry vs первичный вход
    for flag, label in [(0, "primary"), (1, "reentry")]:
        sub = df[df["is_reentry"] == flag]
        if len(sub):
            report[f"{label}_wr"] = round((sub["won"] == 1).mean() * 100, 1)
            report[f"{label}_avg_pnl"] = round(sub["pnl_sol"].mean(), 4)

    return report


# ══════════════════════════════════════════════════════════════════════════════
# 4. ML-МОДЕЛЬ (LightGBM)
# ══════════════════════════════════════════════════════════════════════════════

def train_model(df: pd.DataFrame, signals: dict) -> tuple:
    """
    Обучить LightGBM классификатор для предсказания win/loss.
    Возвращает (model, threshold, feature_importances, auc).
    """
    try:
        import lightgbm as lgb
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.metrics import roc_auc_score
        import pickle
    except ImportError:
        print("[trainer] ⚠️  lightgbm/sklearn не установлены. Пропуск ML-модели.")
        print("         Установи: pip install lightgbm scikit-learn")
        return None, 0.5, {}, 0.5

    if len(df) < MIN_TRADES_FOR_ML:
        print(f"[trainer] Мало данных для ML ({len(df)} < {MIN_TRADES_FOR_ML}). Нужно больше сделок.")
        return None, 0.5, {}, 0.5

    # Набор признаков (берём только реально заполненные)
    base_features = ["log_entry_mcap", "is_reentry", "hour_utc", "dow"]
    signal_features = [
        "sig_bc_sol", "sig_n_buys", "dev_buy_sol",
        "early_buyers", "buyers_before_me", "co_buy",
        "has_twitter", "has_website", "creation_age_sec",
    ]
    available = [c for c in base_features + signal_features if c in df.columns]

    # Убираем признаки где >90% нулей (ещё не заполнены парсером)
    feat_cols = [c for c in available
                 if c in base_features or (df[c] != 0).mean() > 0.1]

    X = df[feat_cols].fillna(0).values
    y = df["won"].values

    model = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        class_weight="balanced",
        verbose=-1,
        random_state=42,
    )

    # Кросс-валидация для оценки качества
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(model, X, y, cv=cv, scoring="roc_auc")
    auc = scores.mean()
    print(f"[trainer] LightGBM CV AUC: {auc:.3f} ± {scores.std():.3f}  (features: {feat_cols})")

    if auc < 0.52:
        print("[trainer] AUC слишком низкий — модель не лучше случайного. Фильтр не применяется.")
        return None, 0.5, {}, auc

    # Финальное обучение
    model.fit(X, y)

    # Оптимальный порог (максимизируем EV)
    proba = model.predict_proba(X)[:, 1]
    best_thresh, best_ev = 0.5, -999
    for thresh in np.arange(0.35, 0.75, 0.01):
        mask = proba >= thresh
        if mask.sum() < 20:
            break
        ev = df.loc[mask, "pnl_pct"].mean()
        if ev > best_ev:
            best_ev, best_thresh = ev, thresh

    importances = dict(zip(feat_cols, model.feature_importances_))

    # Сохраняем модель
    import pickle
    with open(ML_MODEL_FILE, "wb") as f:
        pickle.dump({"model": model, "features": feat_cols, "threshold": best_thresh}, f)
    print(f"[trainer] Модель сохранена → {ML_MODEL_FILE}  threshold={best_thresh:.2f}")

    return model, best_thresh, importances, auc


# ══════════════════════════════════════════════════════════════════════════════
# 5. OPTUNA: ОПТИМИЗАЦИЯ ПАРАМЕТРОВ ПО ИСТОРИЧЕСКИМ СДЕЛКАМ
# ══════════════════════════════════════════════════════════════════════════════

def _simulate_strategy(df: pd.DataFrame, params: dict) -> float:
    """
    Симулятор: сколько бы мы заработали с данными параметрами?

    На основе имеющихся данных:
    - Фильтруем сделки по mcap (min/max)
    - Фильтруем reentry если не хотим
    - Возвращаем суммарный pnl_pct (усредн.) как score

    Ограничение: без price-series не можем симулировать TP/SL —
    используем реальный исход сделки как proxy.
    """
    mask = pd.Series([True] * len(df), index=df.index)

    # Фильтр mcap
    min_mcap = params.get("min_entry_mcap_sol", 0.0)
    max_mcap = params.get("max_entry_mcap_sol", 9999.0)
    mask &= (df["entry_mcap_sol"] >= min_mcap) & (df["entry_mcap_sol"] <= max_mcap)

    # Фильтр reentry
    if params.get("skip_reentries", False):
        mask &= (df["is_reentry"] == 0)

    # Фильтр по часу UTC (активные часы)
    hour_from = params.get("active_hour_from", 0)
    hour_to   = params.get("active_hour_to",   24)
    if hour_from < hour_to:
        mask &= (df["hour_utc"] >= hour_from) & (df["hour_utc"] < hour_to)

    sub = df[mask]
    if len(sub) < 20:
        return -999.0   # слишком мало сделок — штраф

    # Score = EV (средний pnl_pct) × количество сделок (больше хороших = лучше)
    ev     = sub["pnl_pct"].mean()
    n_norm = len(sub) / len(df)
    return ev * (0.5 + 0.5 * n_norm)  # штрафуем за слишком агрессивный фильтр


def run_optuna(df: pd.DataFrame) -> dict:
    """Optuna: найти лучшие параметры фильтрации."""
    try:
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ImportError:
        print("[trainer] ⚠️  optuna не установлена. pip install optuna")
        return {}

    if len(df) < MIN_TRADES_FOR_OPTUNA:
        print(f"[trainer] Мало данных для Optuna ({len(df)} < {MIN_TRADES_FOR_OPTUNA}). Пропуск.")
        return {}

    # Определяем реальный диапазон mcap в данных
    mcap_p5  = df["entry_mcap_sol"].quantile(0.05)
    mcap_p95 = df["entry_mcap_sol"].quantile(0.95)

    def objective(trial):
        params = {
            "min_entry_mcap_sol": trial.suggest_float("min_entry_mcap_sol", 0.0, mcap_p5 * 3),
            "max_entry_mcap_sol": trial.suggest_float("max_entry_mcap_sol", mcap_p95 * 0.3, mcap_p95 * 2),
            "skip_reentries":     trial.suggest_categorical("skip_reentries", [True, False]),
            "active_hour_from":   trial.suggest_int("active_hour_from", 0, 12),
            "active_hour_to":     trial.suggest_int("active_hour_to", 12, 24),
        }
        return _simulate_strategy(df, params)

    study = optuna.create_study(direction="maximize",
                                sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(objective, n_trials=OPTUNA_TRIALS, show_progress_bar=False)

    best = study.best_params
    score = study.best_value
    print(f"[trainer] Optuna best score={score:.4f}  params={best}")

    # Валидация: убедиться что лучше дефолта
    default_score = _simulate_strategy(df, {})
    if score <= default_score * 1.02:
        print(f"[trainer] Optuna не нашла значимого улучшения ({score:.4f} vs default {default_score:.4f}). Оставляем дефолт.")
        return {}

    return best


# ══════════════════════════════════════════════════════════════════════════════
# 6. СОХРАНЕНИЕ РЕЗУЛЬТАТОВ
# ══════════════════════════════════════════════════════════════════════════════

def save_params(stats: dict, optuna_params: dict, model_info: dict, trades_used: int) -> dict:
    """Собрать и сохранить ml_params.json."""
    # Базовые рекомендации из статистики
    best_params: dict = {}

    # Из Optuna (если нашла что-то)
    if optuna_params:
        best_params["min_entry_mcap_sol"] = round(optuna_params.get("min_entry_mcap_sol", 0.0), 2)
        best_params["max_entry_mcap_sol"] = round(optuna_params.get("max_entry_mcap_sol", 9999.0), 2)
        if optuna_params.get("skip_reentries"):
            best_params["skip_reentries"] = True
        ah_from = optuna_params.get("active_hour_from", 0)
        ah_to   = optuna_params.get("active_hour_to", 24)
        if ah_from > 0 or ah_to < 24:
            best_params["active_hour_from"] = ah_from
            best_params["active_hour_to"]   = ah_to

    # ML-фильтр (если модель хорошая)
    if model_info.get("auc", 0) >= 0.54:
        best_params["entry_filter"]        = True
        best_params["entry_filter_thresh"] = round(model_info.get("threshold", 0.5), 3)
    else:
        best_params["entry_filter"] = False

    result = {
        "generated_at":  datetime.now(timezone.utc).isoformat(),
        "trades_used":   trades_used,
        "score":         round(model_info.get("auc", 0.0), 4),
        "stats_summary": {
            "win_rate_pct":  stats.get("win_rate"),
            "ev_per_trade":  stats.get("ev_per_trade"),
            "pnl_total_sol": stats.get("pnl_total_sol"),
        },
        "best_params":   best_params,
        "feature_importances": model_info.get("importances", {}),
    }

    with open(ML_PARAMS_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"[trainer] ✅ Параметры сохранены → {ML_PARAMS_FILE}")
    return result


def save_report(stats: dict, model_info: dict, optuna_params: dict, result: dict) -> None:
    """Сохранить читаемый текстовый отчёт."""
    lines = [
        "=" * 65,
        f"  ML TRAINER REPORT  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 65,
        f"  Сделок в обучении:  {result['trades_used']}",
        f"  Win rate:           {stats['win_rate']:.1f}%",
        f"  EV/сделку:          {stats['ev_per_trade']:+.3f}%",
        f"  Total PnL:          {stats['pnl_total_sol']:+.4f} SOL",
        f"  Avg hold:           {stats['avg_hold_sec']:.1f}s",
        "",
        "  По фазам:",
    ]
    for phase, s in stats.get("by_phase", {}).items():
        lines.append(f"    {phase:<15} count={s['count']:>5}  WR={s['wr_pct']:.1f}%  avg_pnl={s['avg_pnl']:+.4f}")

    lines += ["", "  По mcap (SOL):"]
    for mcap, s in stats.get("by_mcap", {}).items():
        lines.append(f"    {mcap:<10} count={s['count']:>5}  WR={s['wr_pct']:.1f}%  avg_pnl={s['avg_pnl']:+.4f}")

    lines += ["", "  По часу UTC (топ-5 по WR):"]
    by_hour = stats.get("by_hour", {})
    top_hours = sorted(by_hour.items(), key=lambda x: x[1]["wr_pct"], reverse=True)[:5]
    for hour, s in top_hours:
        lines.append(f"    {hour:02d}:00  count={s['count']:>4}  WR={s['wr_pct']:.1f}%")

    lines += ["", "  Re-entry vs первичный:"]
    lines.append(f"    Primary: WR={stats.get('primary_wr', 0):.1f}%  avg={stats.get('primary_avg_pnl', 0):+.4f}")
    lines.append(f"    Re-entry: WR={stats.get('reentry_wr', 0):.1f}%  avg={stats.get('reentry_avg_pnl', 0):+.4f}")

    auc = model_info.get("auc", 0)
    lines += ["", f"  LightGBM AUC: {auc:.3f}  (>0.54 = применяем фильтр)"]
    if model_info.get("importances"):
        top_feats = sorted(model_info["importances"].items(), key=lambda x: x[1], reverse=True)[:5]
        lines.append("  Top features:")
        for fname, imp in top_feats:
            lines.append(f"    {fname:<25} importance={imp:.0f}")

    if optuna_params:
        lines += ["", "  Optuna параметры:"]
        for k, v in optuna_params.items():
            lines.append(f"    {k}: {v}")

    params = result.get("best_params", {})
    lines += ["", "  ✅ Активные параметры бота (ml_params.json):"]
    for k, v in params.items():
        lines.append(f"    {k}: {v}")

    lines += ["", "=" * 65]
    text = "\n".join(lines)
    print(text)
    with open(ML_REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    print(f"[trainer] Отчёт сохранён → {ML_REPORT_FILE}")


def append_history(result: dict) -> None:
    """Дозапись в историю обучений (для отслеживания прогресса)."""
    record = {
        "ts":          result["generated_at"],
        "trades_used": result["trades_used"],
        "win_rate":    result["stats_summary"]["win_rate_pct"],
        "ev":          result["stats_summary"]["ev_per_trade"],
        "auc":         result["score"],
        "params":      result["best_params"],
    }
    with open(ML_HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{'='*65}")
    print(f"  PUMPSCALP ML TRAINER  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*65}\n")

    # 1. Загрузка
    df      = load_trades()
    signals = load_token_signals()
    n_signals = sum(1 for v in signals.values() if len(v) > 6) if signals else 0
    print(f"[trainer] Token signals: {len(signals)} минтов ({n_signals} обогащены парсером)")

    # 2. Признаки
    df = build_features(df, signals)

    # 3. Статистика
    print("\n[trainer] Анализ статистики...")
    stats = analyze_stats(df)

    # 4. ML-модель
    print("\n[trainer] Обучение LightGBM...")
    model, threshold, importances, auc = train_model(df, signals)
    model_info = {"auc": auc, "threshold": threshold, "importances": importances}

    # 5. Optuna
    print(f"\n[trainer] Optuna ({OPTUNA_TRIALS} trials)...")
    optuna_params = run_optuna(df)

    # 6. Сохранение
    print("\n[trainer] Сохранение результатов...")
    result = save_params(stats, optuna_params, model_info, trades_used=len(df))
    save_report(stats, model_info, optuna_params, result)
    append_history(result)

    print(f"\n[trainer] ✅ Готово! Бот при следующем запуске подхватит {ML_PARAMS_FILE}")


if __name__ == "__main__":
    main()
