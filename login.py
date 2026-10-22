"""Одноразовый вход в аккаунт Telegram для рассылки в чаты.

Запуск (один раз): python login.py
Спросит номер телефона, код из Telegram и пароль двухэтапной защиты (если он есть).
Результат: файл ravins.session. Это ключ к вашему аккаунту: никому его не отправляйте
и не загружайте на GitHub.
"""
import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()
API_ID = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
SESSION = os.getenv("SESSION", "ravins")


async def main():
    if not API_ID or not API_HASH:
        raise SystemExit("Сначала впишите API_ID и API_HASH в файл .env (см. README)")
    client = TelegramClient(SESSION, int(API_ID), API_HASH)
    await client.start()  # интерактивно: телефон, код, пароль
    me = await client.get_me()
    print(f"Вход выполнен: {me.first_name}. Теперь перезапустите бота.")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
