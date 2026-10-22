#!/data/data/com.termux/files/usr/bin/bash
# Запуск Ravins Bot с автоперезапуском. Использование: bash start.sh
cd "$(dirname "$0")"
termux-wake-lock 2>/dev/null
while true; do
  git pull --quiet 2>/dev/null
  pip install -q -r requirements.txt 2>/dev/null
  python bot.py
  echo "Бот остановился, перезапуск через 5 секунд..."
  sleep 5
done
