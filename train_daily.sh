#!/usr/bin/env bash
# train_daily.sh — Ночное переобучение ML-модели
#
# Настройка cron (каждый день в 03:00 UTC):
#   crontab -e
#   0 3 * * * /path/to/Treadebot_sol/train_daily.sh >> /path/to/Treadebot_sol/ml_training.log 2>&1
#
# Или если хочешь уведомление в Telegram — добавь в конец скрипта вызов curl.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "=========================================="
echo "  ML TRAINER  $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "=========================================="

# Активация venv (если есть)
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    echo "[cron] venv активирован: .venv"
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    echo "[cron] venv активирован: venv"
else
    echo "[cron] venv не найден, используем системный python"
fi

# Проверка наличия данных
if [ ! -f "trade_log_master.csv" ]; then
    echo "[cron] trade_log_master.csv не найден — пропускаем обучение"
    exit 0
fi

LINES=$(wc -l < trade_log_master.csv)
echo "[cron] Сделок в логе: $((LINES - 1))"

if [ "$LINES" -lt 50 ]; then
    echo "[cron] Мало данных ($((LINES-1)) сделок). Нужно минимум 50. Пропускаем."
    exit 0
fi

# Установка зависимостей если не установлены
python3 -c "import lightgbm, optuna, sklearn" 2>/dev/null || {
    echo "[cron] Устанавливаю зависимости..."
    pip install lightgbm optuna scikit-learn --quiet
}

# Запуск тренера
echo "[cron] Запуск ml_trainer.py..."
python3 ml_trainer.py

echo "[cron] ✅ Обучение завершено: $(date -u '+%H:%M UTC')"

# Опционально: уведомление в Telegram
# BOT_TOKEN="ваш_токен"
# CHAT_ID="ваш_chat_id"
# MSG=$(tail -20 ml_report.txt | tr '\n' '|')
# curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
#      -d "chat_id=${CHAT_ID}&text=${MSG}" > /dev/null
