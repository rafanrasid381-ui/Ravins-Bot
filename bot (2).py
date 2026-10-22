"""Ravins Bot: бот для рассылок в Telegram.

Возможности:
- рассылка подписчикам бота (любой контент, кнопка-ссылка);
- рассылка в выбранные чаты и каналы от имени вашего аккаунта (нужен вход через login.py);
- отправка сразу или по расписанию, остановка на ходу, статистика, экспорт получателей в CSV.
"""
import asyncio
import csv
import functools
import html as htmllib
import io
import logging
import os
import random
import re
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from telegram import InlineKeyboardButton as Btn
from telegram import InlineKeyboardMarkup as Markup
from telegram import Update
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

try:  # режим чатов необязателен: без telethon бот работает как обычно
    from telethon import TelegramClient
    from telethon.errors import FloodWaitError
except ImportError:  # pragma: no cover
    TelegramClient = None

    class FloodWaitError(Exception):
        seconds = 0

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x) for x in re.findall(r"\d+", os.getenv("ADMIN_IDS", ""))}
DB_PATH = os.getenv("DB_PATH", "bot.db")
TZ = timedelta(hours=float(os.getenv("TZ_OFFSET", "3")))  # по умолчанию Москва, UTC+3
API_ID = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
SESSION = os.getenv("SESSION", "ravins")
SEND_DELAY = 0.05  # подписчикам: ~20 сообщений/сек, с запасом от лимита Telegram (30/сек)
CHAT_DELAY = (5.0, 12.0)  # в чаты от аккаунта: случайная пауза в секундах, чтобы не ловить бан
PAGE_SIZE = 8

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("ravins")

S_CONTENT, S_BUTTON, S_CONFIRM, S_WHEN = range(4)
LOCK = asyncio.Lock()  # одновременно идёт только одна рассылка
STOP: set[int] = set()  # id рассылок, которые просили остановить
TASKS: set[asyncio.Task] = set()

CHATS_HELP = (
    "💬 Режим чатов выключен: бот ещё не подключён к вашему аккаунту.\n\n"
    "Как включить (в Termux):\n"
    "1. pip install telethon==1.36.0\n"
    "2. Получите API_ID и API_HASH на my.telegram.org (API development tools) "
    "и допишите их в .env\n"
    "3. Выполните python login.py и войдите по коду\n"
    "4. Перезапустите бота"
)


def spawn(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    TASKS.add(task)
    task.add_done_callback(TASKS.discard)
    return task


# ---------- База данных ----------
def fetch(sql, args=()):
    with closing(sqlite3.connect(DB_PATH)) as c:
        return c.execute(sql, args).fetchall()


def execute(sql, args=()):
    """Возвращает (lastrowid, rowcount)."""
    with closing(sqlite3.connect(DB_PATH)) as c:
        cur = c.execute(sql, args)
        c.commit()
        return cur.lastrowid, cur.rowcount


def init_db():
    execute(
        """CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            active INTEGER DEFAULT 1,
            joined TEXT DEFAULT CURRENT_TIMESTAMP)"""
    )
    execute(
        """CREATE TABLE IF NOT EXISTS broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            chat_id INTEGER,
            message_id INTEGER,
            btn_text TEXT,
            btn_url TEXT,
            run_at INTEGER,
            status TEXT,
            sent INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            created TEXT DEFAULT CURRENT_TIMESTAMP)"""
    )
    execute(
        """CREATE TABLE IF NOT EXISTS chats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE,
            title TEXT,
            enabled INTEGER DEFAULT 0)"""
    )
    # миграция: новые колонки для старых баз
    for col in ("target TEXT DEFAULT 'users'", "kind TEXT", "file_id TEXT", "body TEXT", "fname TEXT"):
        try:
            execute(f"ALTER TABLE broadcasts ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass  # колонка уже есть
    # рассылки, прерванные перезапуском бота, повторно не отправляем
    execute("UPDATE broadcasts SET status='interrupted' WHERE status='running'")


def upsert_user(user):
    execute(
        "INSERT INTO users (id, username, first_name, active) VALUES (?,?,?,1) "
        "ON CONFLICT(id) DO UPDATE SET username=excluded.username, "
        "first_name=excluded.first_name, active=1",
        (user.id, user.username, user.first_name),
    )


def set_active(uid, flag):
    execute("UPDATE users SET active=? WHERE id=?", (flag, uid))


def active_ids():
    return [r[0] for r in fetch("SELECT id FROM users WHERE active=1")]


def enabled_chats_count() -> int:
    return fetch("SELECT COUNT(*) FROM chats WHERE enabled=1")[0][0]


def create_broadcast(admin_id, bc, run_at):
    bid, _ = execute(
        "INSERT INTO broadcasts (admin_id, chat_id, message_id, btn_text, btn_url, run_at, status, "
        "target, kind, file_id, body, fname) VALUES (?,?,?,?,?,?, 'scheduled', ?,?,?,?,?)",
        (
            admin_id,
            bc["chat"],
            bc["mid"],
            bc.get("bt"),
            bc.get("bu"),
            run_at,
            bc.get("target", "users"),
            bc.get("kind"),
            bc.get("file"),
            bc.get("body"),
            bc.get("fname"),
        ),
    )
    return bid


def claim(bid) -> bool:
    """Атомарно переводит рассылку из 'scheduled' в 'running'."""
    _, n = execute("UPDATE broadcasts SET status='running' WHERE id=? AND status='scheduled'", (bid,))
    return n == 1


# ---------- Вспомогательное ----------
def fmt_ts(epoch) -> str:
    return (datetime.fromtimestamp(epoch, timezone.utc) + TZ).strftime("%d.%m.%Y %H:%M")


def parse_when(text: str):
    """'+30' (минут), 'ЧЧ:ММ', 'ДД.ММ ЧЧ:ММ', 'ДД.ММ.ГГГГ ЧЧ:ММ' -> datetime (UTC) или None."""
    text = text.strip()
    now = datetime.now(timezone.utc)
    local_now = (now + TZ).replace(tzinfo=None)
    try:
        m = re.fullmatch(r"\+(\d{1,5})", text)
        if m:
            return now + timedelta(minutes=int(m.group(1)))
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", text)
        if m:
            cand = local_now.replace(hour=int(m[1]), minute=int(m[2]), second=0, microsecond=0)
            if cand <= local_now:
                cand += timedelta(days=1)
            return (cand - TZ).replace(tzinfo=timezone.utc)
        m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2}|\d{4}))? (\d{1,2}):(\d{2})", text)
        if m:
            year = int(m[3]) if m[3] else local_now.year
            if year < 100:
                year += 2000
            cand = datetime(year, int(m[2]), int(m[1]), int(m[4]), int(m[5]))
            return (cand - TZ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return None


def markup_for(text, url):
    return Markup([[Btn(text, url=url)]]) if text and url else None


def extract_content(msg):
    """Что отправлять в чаты от аккаунта: (вид, file_id, текст в HTML, имя файла)."""
    if msg.text:
        return "text", None, msg.text_html, None
    cap = msg.caption_html or ""
    if msg.photo:
        return "photo", msg.photo[-1].file_id, cap, "photo.jpg"
    if msg.video:
        return "video", msg.video.file_id, cap, msg.video.file_name or "video.mp4"
    if msg.document:
        return "document", msg.document.file_id, cap, msg.document.file_name or "file"
    return None, None, None, None


def main_menu():
    return Markup(
        [
            [Btn("📨 Создать рассылку", callback_data="new")],
            [
                Btn("🗓 Запланированные", callback_data="sched"),
                Btn("👥 Получатели", callback_data="recipients"),
            ],
            [
                Btn("💬 Чаты", callback_data="chp:0"),
                Btn("📊 Статистика", callback_data="stats"),
            ],
        ]
    )


def back_menu():
    return Markup([[Btn("⬅️ В меню", callback_data="menu")]])


async def safe_edit(q, text, markup=None):
    try:
        await q.edit_message_text(text, reply_markup=markup)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            raise


def admin_only(fn):
    @functools.wraps(fn)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or user.id not in ADMIN_IDS:
            if update.callback_query:
                await update.callback_query.answer("Нет доступа", show_alert=True)
            return ConversationHandler.END
        return await fn(update, context)

    return wrapper


# ---------- Команды ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return ConversationHandler.END
    user = update.effective_user
    upsert_user(user)
    context.user_data.pop("bc", None)
    if user.id in ADMIN_IDS:
        await update.message.reply_text(
            "👋 Ravins Bot: панель управления рассылкой. Выберите действие:",
            reply_markup=main_menu(),
        )
    elif not ADMIN_IDS:
        await update.message.reply_text(
            f"⚙️ Бот ещё не настроен.\nВаш Telegram ID: {user.id}\n\n"
            f"Впишите его в файл .env: ADMIN_IDS={user.id}, затем перезапустите бота."
        )
    else:
        await update.message.reply_text(
            "✅ Добро пожаловать в Ravins Bot! Вы подписаны на рассылку.\nОтписаться: /stop"
        )
    return ConversationHandler.END


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_active(update.effective_user.id, 0)
    await update.message.reply_text("Вы отписались от рассылки. Вернуться: /start")


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Ваш Telegram ID: {update.effective_user.id}")


@admin_only
async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("bc", None)
    await update.message.reply_text("Выберите действие:", reply_markup=main_menu())
    return ConversationHandler.END


@admin_only
async def menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await safe_edit(q, "Выберите действие:", main_menu())


async def noop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()


# ---------- Получатели, статистика, расписание ----------
@admin_only
async def recipients_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    total = fetch("SELECT COUNT(*) FROM users")[0][0]
    active = fetch("SELECT COUNT(*) FROM users WHERE active=1")[0][0]
    lines = [f"👥 Всего: {total}", f"✅ Активных: {active}", f"🚫 Отписались/заблокировали: {total - active}"]
    last = fetch("SELECT username, first_name FROM users ORDER BY joined DESC, id DESC LIMIT 5")
    if last:
        lines.append("\nПоследние подписчики:")
        lines += [f"• @{u}" if u else f"• {n or 'без имени'}" for u, n in last]
    kb = Markup(
        [[Btn("📄 Экспорт в CSV", callback_data="export")], [Btn("⬅️ В меню", callback_data="menu")]]
    )
    await safe_edit(q, "\n".join(lines), kb)


@admin_only
async def export_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "username", "first_name", "active", "joined"])
    w.writerows(fetch("SELECT id, username, first_name, active, joined FROM users ORDER BY joined"))
    data = io.BytesIO(buf.getvalue().encode("utf-8-sig"))
    await context.bot.send_document(q.message.chat_id, document=data, filename="recipients.csv")


STATUS_ICON = {
    "done": "✅",
    "running": "⏳",
    "scheduled": "🕐",
    "cancelled": "🚫",
    "stopped": "⛔",
    "interrupted": "⚠️",
}


@admin_only
async def stats_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    active = fetch("SELECT COUNT(*) FROM users WHERE active=1")[0][0]
    n, sent, failed = fetch(
        "SELECT COUNT(*), COALESCE(SUM(sent),0), COALESCE(SUM(failed),0) "
        "FROM broadcasts WHERE status IN ('done','stopped')"
    )[0]
    lines = [
        "📊 Статистика",
        f"\nРассылок проведено: {n}",
        f"Доставлено всего: {sent}",
        f"Ошибок всего: {failed}",
        f"Активных получателей: {active}",
        f"Чатов выбрано: {enabled_chats_count()}",
    ]
    last = fetch("SELECT id, run_at, status, sent, failed, target FROM broadcasts ORDER BY id DESC LIMIT 5")
    if last:
        lines.append("\nПоследние рассылки:")
        for bid, run_at, st, s, f, tg in last:
            who = "💬" if tg == "chats" else "👥"
            lines.append(f"{STATUS_ICON.get(st, '•')} {who} #{bid} · {fmt_ts(run_at)} · доставлено {s}, ошибок {f}")
    await safe_edit(q, "\n".join(lines), back_menu())


@admin_only
async def sched_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    rows = fetch("SELECT id, run_at, target FROM broadcasts WHERE status='scheduled' ORDER BY run_at LIMIT 10")
    if not rows:
        await safe_edit(q, "Запланированных рассылок нет.", back_menu())
        return
    kb = [
        [Btn(f"❌ {'💬' if tg == 'chats' else '👥'} #{bid} · {fmt_ts(run_at)}", callback_data=f"sc:{bid}")]
        for bid, run_at, tg in rows
    ]
    kb.append([Btn("⬅️ В меню", callback_data="menu")])
    await safe_edit(
        q,
        f"🗓 Запланированные рассылки (UTC{TZ.total_seconds() / 3600:+g}).\n"
        "Нажмите на рассылку, чтобы отменить её:",
        Markup(kb),
    )


@admin_only
async def sched_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    bid = int(q.data.split(":")[1])
    _, n = execute("UPDATE broadcasts SET status='cancelled' WHERE id=? AND status='scheduled'", (bid,))
    await q.answer("Рассылка отменена" if n else "Уже нельзя отменить")
    await sched_cb(update, context)


# ---------- Чаты (от имени вашего аккаунта) ----------
def can_post(entity) -> bool:
    """Есть ли у аккаунта право писать в эту группу или канал."""
    if getattr(entity, "left", False):
        return False
    is_admin = bool(getattr(entity, "creator", False) or getattr(entity, "admin_rights", None))
    if getattr(entity, "broadcast", False):  # канал
        rights = getattr(entity, "admin_rights", None)
        return bool(getattr(entity, "creator", False) or (rights and getattr(rights, "post_messages", False)))
    rights = getattr(entity, "default_banned_rights", None)  # группа
    if rights is not None and getattr(rights, "send_messages", False):
        return is_admin
    return True


async def refresh_chats(client) -> int:
    seen = set()
    async for d in client.iter_dialogs():
        if not (d.is_group or d.is_channel) or not can_post(d.entity):
            continue
        execute(
            "INSERT INTO chats (tg_id, title) VALUES (?,?) "
            "ON CONFLICT(tg_id) DO UPDATE SET title=excluded.title",
            (d.id, d.name or str(d.id)),
        )
        seen.add(d.id)
    for rid, tg_id in fetch("SELECT id, tg_id FROM chats"):
        if tg_id not in seen:
            execute("DELETE FROM chats WHERE id=?", (rid,))
    return len(seen)


def chats_view(page: int):
    rows = fetch("SELECT id, title, enabled FROM chats ORDER BY title COLLATE NOCASE")
    total = len(rows)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(max(page, 0), pages - 1)
    kb = [
        [Btn(("✅ " if en else "⬜ ") + (title or "без названия")[:40], callback_data=f"cht:{rid}:{page}")]
        for rid, title, en in rows[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]
    ]
    nav = []
    if page > 0:
        nav.append(Btn("◀️", callback_data=f"chp:{page - 1}"))
    nav.append(Btn(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(Btn("▶️", callback_data=f"chp:{page + 1}"))
    kb.append(nav)
    kb.append([Btn("🔄 Обновить список", callback_data="chr"), Btn("⬅️ В меню", callback_data="menu")])
    enabled = sum(1 for r in rows if r[2])
    text = (
        f"💬 Чаты вашего аккаунта, где можно писать. Выбрано: {enabled} из {total}.\n"
        "Нажмите на чат, чтобы включить (✅) или выключить (⬜) его для рассылки."
    )
    if total == 0:
        text = "💬 Подходящих чатов не найдено. Вступите в чаты или нажмите «Обновить список»."
    return text, Markup(kb)


@admin_only
async def chats_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    client = context.application.bot_data.get("tg")
    if client is None:
        await q.answer()
        await safe_edit(q, CHATS_HELP, back_menu())
        return
    page = int(q.data.split(":")[1]) if q.data.startswith("chp:") else 0
    if q.data == "chr" or not fetch("SELECT 1 FROM chats LIMIT 1"):
        await q.answer("Загружаю список чатов...")
        try:
            await refresh_chats(client)
        except Exception:
            log.exception("Не удалось получить список чатов")
            await safe_edit(q, "Не удалось получить список чатов. Попробуйте позже.", back_menu())
            return
    else:
        await q.answer()
    text, kb = chats_view(page)
    await safe_edit(q, text, kb)


@admin_only
async def chat_toggle_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    _, rid, page = q.data.split(":")
    execute("UPDATE chats SET enabled = 1 - enabled WHERE id=?", (int(rid),))
    await q.answer()
    text, kb = chats_view(int(page))
    await safe_edit(q, text, kb)


# ---------- Создание рассылки ----------
@admin_only
async def new_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data.pop("bc", None)
    await safe_edit(
        q,
        "✍️ Отправьте сообщение для рассылки: текст, фото, видео, файл.\n"
        "Оно уйдёт получателям в точности как вы его отправили.\n\n"
        "⚠️ Не удаляйте это сообщение из чата до окончания рассылки.\n"
        "Отмена: /menu",
    )
    return S_CONTENT


@admin_only
async def got_content(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    kind, fid, body, fname = extract_content(msg)
    context.user_data["bc"] = {
        "chat": msg.chat_id,
        "mid": msg.message_id,
        "kind": kind,
        "file": fid,
        "body": body,
        "fname": fname,
    }
    kb = Markup([[Btn("Без кнопки", callback_data="nobtn")], [Btn("❌ Отмена", callback_data="cancel")]])
    await msg.reply_text(
        "🔘 Добавить кнопку-ссылку под сообщением?\n\n"
        "Отправьте текст и ссылку в формате:\nТекст кнопки | https://ссылка\n\n"
        "Или нажмите «Без кнопки».\n"
        "(В чатах от аккаунта кнопка превратится в строку-ссылку под текстом.)",
        reply_markup=kb,
    )
    return S_BUTTON


@admin_only
async def got_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    if "|" not in text:
        await update.message.reply_text("Нужен формат: Текст кнопки | https://ссылка")
        return S_BUTTON
    bt, bu = (s.strip() for s in text.split("|", 1))
    if not bt or len(bt) > 64 or not re.fullmatch(r"(https?|tg)://\S+", bu):
        await update.message.reply_text(
            "Не получилось. Текст кнопки до 64 символов, ссылка должна начинаться с https://"
        )
        return S_BUTTON
    context.user_data["bc"].update(bt=bt, bu=bu)
    return await send_preview(update.effective_chat.id, context)


@admin_only
async def no_button_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    return await send_preview(update.effective_chat.id, context)


async def send_preview(chat_id, context: ContextTypes.DEFAULT_TYPE):
    bc = context.user_data.get("bc")
    if not bc:
        await context.bot.send_message(chat_id, "Сообщение потерялось, начните заново.", reply_markup=main_menu())
        return ConversationHandler.END
    try:
        await context.bot.copy_message(
            chat_id, bc["chat"], bc["mid"], reply_markup=markup_for(bc.get("bt"), bc.get("bu"))
        )
    except TelegramError as e:
        log.warning("Предпросмотр не удался: %s", e)
        await context.bot.send_message(chat_id, "Не удалось скопировать это сообщение. Отправьте другое.")
        return S_CONTENT
    n = len(active_ids())
    m = enabled_chats_count()
    can_chats = context.application.bot_data.get("tg") is not None and bc.get("kind") and m > 0
    rows = [[Btn(f"🚀 Сейчас: подписчикам ({n})", callback_data="now:users")]]
    if can_chats:
        rows.append([Btn(f"🚀 Сейчас: в чаты ({m})", callback_data="now:chats")])
    rows.append([Btn("🕐 Позже: подписчикам", callback_data="later:users")])
    if can_chats:
        rows.append([Btn("🕐 Позже: в чаты", callback_data="later:chats")])
    rows.append([Btn("❌ Отмена", callback_data="cancel")])
    await context.bot.send_message(chat_id, "☝️ Так увидят получатели. Что делаем?", reply_markup=Markup(rows))
    return S_CONFIRM


@admin_only
async def now_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    target = q.data.split(":")[1]
    bc = context.user_data.get("bc")
    if not bc:
        await q.answer()
        await safe_edit(q, "Сообщение потерялось, начните заново.", main_menu())
        return ConversationHandler.END
    if target == "users" and not active_ids():
        await q.answer("Нет активных получателей", show_alert=True)
        return S_CONFIRM
    if target == "chats" and enabled_chats_count() == 0:
        await q.answer("Не выбрано ни одного чата", show_alert=True)
        return S_CONFIRM
    await q.answer()
    context.user_data.pop("bc", None)
    bc["target"] = target
    bid = create_broadcast(update.effective_user.id, bc, int(time.time()))
    claim(bid)
    spawn(run_broadcast(context.application, bid))
    extra = "\nВ чаты сообщения идут с паузами, это займёт время." if target == "chats" else ""
    await safe_edit(
        q,
        f"⏳ Рассылка #{bid} запущена. Пришлю отчёт по завершении.{extra}",
        Markup([[Btn("⛔ Остановить", callback_data=f"stop:{bid}")]]),
    )
    return ConversationHandler.END


@admin_only
async def later_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    bc = context.user_data.get("bc")
    if not bc:
        await safe_edit(q, "Сообщение потерялось, начните заново.", main_menu())
        return ConversationHandler.END
    bc["target"] = q.data.split(":")[1]
    now_local = (datetime.now(timezone.utc) + TZ).strftime("%d.%m.%Y %H:%M")
    await safe_edit(
        q,
        "🕐 Когда отправить? Сейчас " + now_local + ".\n\n"
        "Форматы:\n"
        "• 18:30: сегодня (или завтра, если время прошло)\n"
        "• 25.10 18:30\n"
        "• 25.10.2026 18:30\n"
        "• +30: через 30 минут\n\n"
        "Отмена: /menu",
    )
    return S_WHEN


@admin_only
async def got_when(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dt = parse_when(update.message.text or "")
    if dt is None:
        await update.message.reply_text("Не понял время. Примеры: 18:30, 25.10 18:30, +30")
        return S_WHEN
    if dt <= datetime.now(timezone.utc):
        await update.message.reply_text("Это время уже прошло. Введите время в будущем.")
        return S_WHEN
    bc = context.user_data.pop("bc", None)
    if not bc:
        await update.message.reply_text("Сообщение потерялось, начните заново.", reply_markup=main_menu())
        return ConversationHandler.END
    bid = create_broadcast(update.effective_user.id, bc, int(dt.timestamp()))
    await update.message.reply_text(
        f"✅ Рассылка #{bid} запланирована на {fmt_ts(int(dt.timestamp()))}.\n"
        "Не удаляйте исходное сообщение из чата до отправки. "
        "Бот должен быть запущен в это время.",
        reply_markup=main_menu(),
    )
    return ConversationHandler.END


async def cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("bc", None)
    q = update.callback_query
    await q.answer()
    await safe_edit(q, "Отменено.", main_menu())
    return ConversationHandler.END


@admin_only
async def stop_broadcast_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bid = int(update.callback_query.data.split(":")[1])
    STOP.add(bid)
    await update.callback_query.answer("Останавливаю...")


# ---------- Отправка ----------
async def send_one(bot, uid, chat, mid, markup) -> bool:
    for _ in range(3):
        try:
            await bot.copy_message(uid, chat, mid, reply_markup=markup)
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except Forbidden:
            set_active(uid, 0)  # пользователь заблокировал бота
            return False
        except BadRequest as e:
            if "chat not found" in str(e).lower():
                set_active(uid, 0)
            else:
                log.warning("Не отправлено %s: %s", uid, e)
            return False
        except TelegramError as e:
            log.warning("Ошибка отправки %s: %s", uid, e)
            await asyncio.sleep(1)
    return False


async def send_chat(app: Application, tg_id, kind, file_id, body, fname, bt, bu) -> bool:
    """Отправка в чат или канал от имени аккаунта."""
    client = app.bot_data.get("tg")
    if client is None or not kind:
        return False
    text = body or ""
    if bt and bu:
        text += f'\n\n<a href="{htmllib.escape(bu, quote=True)}">{htmllib.escape(bt)}</a>'
    data = None
    for _ in range(3):
        try:
            if kind == "text":
                await client.send_message(tg_id, text, parse_mode="html")
            else:
                if data is None:
                    f = await app.bot.get_file(file_id)  # у Bot API лимит 20 МБ
                    data = bytes(await f.download_as_bytearray())
                bio = io.BytesIO(data)
                bio.name = fname or "file"
                long_caption = len(text) > 900
                await client.send_file(
                    tg_id,
                    bio,
                    caption=None if long_caption else text,
                    parse_mode="html",
                    force_document=(kind == "document"),
                )
                if long_caption:
                    await client.send_message(tg_id, text, parse_mode="html")
            return True
        except FloodWaitError as e:
            if e.seconds > 300:
                log.warning("Слишком длинный FloodWait (%s с) для чата %s", e.seconds, tg_id)
                return False
            await asyncio.sleep(e.seconds + 1)
        except ValueError:  # аккаунт ещё не «видел» этот чат: подгружаем диалоги
            try:
                await client.get_dialogs()
            except Exception:
                return False
        except Exception as e:
            log.warning("Не отправлено в чат %s: %s", tg_id, e)
            return False
    return False


async def run_broadcast(app: Application, bid: int):
    row = fetch(
        "SELECT admin_id, chat_id, message_id, btn_text, btn_url, target, kind, file_id, body, fname "
        "FROM broadcasts WHERE id=?",
        (bid,),
    )
    if not row:
        return
    admin_id, chat, mid, bt, bu, target, kind, file_id, body, fname = row[0]
    markup = markup_for(bt, bu)
    sent = failed = 0
    stopped = False
    failed_names: list[str] = []
    try:
        async with LOCK:
            if target == "chats":
                targets = fetch("SELECT tg_id, title FROM chats WHERE enabled=1")
                for i, (tg_id, title) in enumerate(targets):
                    if bid in STOP:
                        stopped = True
                        break
                    if await send_chat(app, tg_id, kind, file_id, body, fname, bt, bu):
                        sent += 1
                    else:
                        failed += 1
                        failed_names.append(title or str(tg_id))
                    if i < len(targets) - 1:
                        await asyncio.sleep(random.uniform(*CHAT_DELAY))
            else:
                for uid in active_ids():
                    if bid in STOP:
                        stopped = True
                        break
                    if await send_one(app.bot, uid, chat, mid, markup):
                        sent += 1
                    else:
                        failed += 1
                    await asyncio.sleep(SEND_DELAY)
    except Exception:
        log.exception("Сбой рассылки #%s", bid)
    status = "stopped" if stopped else "done"
    execute("UPDATE broadcasts SET status=?, sent=?, failed=? WHERE id=?", (status, sent, failed, bid))
    STOP.discard(bid)
    title = "⛔ Рассылка остановлена" if stopped else "✅ Рассылка завершена"
    where = "в чаты" if target == "chats" else "подписчикам"
    text = f"{title} (#{bid}, {where})\nДоставлено: {sent}\nОшибок: {failed}"
    if failed_names:
        text += "\n\nНе удалось отправить:\n" + "\n".join(f"• {n}" for n in failed_names[:10])
    try:
        await app.bot.send_message(admin_id, text, reply_markup=main_menu())
    except TelegramError:
        pass


async def scheduler(app: Application):
    while True:
        try:
            due = fetch("SELECT id FROM broadcasts WHERE status='scheduled' AND run_at<=?", (int(time.time()),))
            for (bid,) in due:
                if claim(bid):
                    log.info("Запуск запланированной рассылки #%s", bid)
                    spawn(run_broadcast(app, bid))
        except Exception:
            log.exception("Сбой планировщика")
        await asyncio.sleep(10)


# ---------- Служебное ----------
async def on_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cm = update.my_chat_member
    if not cm or cm.chat.type != "private":
        return
    if cm.new_chat_member.status in ("kicked", "left"):
        set_active(cm.from_user.id, 0)
    elif cm.new_chat_member.status == "member":
        upsert_user(cm.from_user)


async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    log.error("Ошибка: %s", context.error, exc_info=context.error)


async def start_userclient(app: Application):
    """Подключает аккаунт для рассылки в чаты (если настроен через login.py)."""
    if TelegramClient is None or not API_ID or not API_HASH:
        log.info("Режим чатов выключен (нет telethon или API_ID/API_HASH)")
        return
    try:
        client = TelegramClient(SESSION, int(API_ID), API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            log.warning("Аккаунт не авторизован: выполните python login.py")
            await client.disconnect()
            return
        app.bot_data["tg"] = client
        me = await client.get_me()
        log.info("Режим чатов включён, аккаунт: %s", me.first_name)
    except Exception:
        log.exception("Не удалось подключить аккаунт, режим чатов выключен")


async def post_init(app: Application):
    init_db()
    app.bot_data["scheduler"] = asyncio.create_task(scheduler(app))
    await start_userclient(app)
    try:
        await app.bot.set_my_commands(
            [("start", "Меню / подписка"), ("menu", "Панель управления"), ("stop", "Отписаться"), ("id", "Мой ID")]
        )
    except TelegramError:
        pass
    me = await app.bot.get_me()
    log.info("Ravins Bot запущен: @%s, админов: %d", me.username, len(ADMIN_IDS))
    if not ADMIN_IDS:
        log.warning("ADMIN_IDS пуст: напишите боту /start, он покажет ваш ID")


async def post_shutdown(app: Application):
    task = app.bot_data.get("scheduler")
    if task:
        task.cancel()
    client = app.bot_data.get("tg")
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass


def main():
    if not TOKEN:
        raise SystemExit("Не задан BOT_TOKEN. Создайте файл .env по образцу .env.example")
    app = (
        Application.builder()
        .token(TOKEN)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .get_updates_connect_timeout(30)
        .get_updates_read_timeout(40)
        .get_updates_write_timeout(30)
        .get_updates_pool_timeout(30)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    private = filters.ChatType.PRIVATE
    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(new_cb, pattern="^new$")],
        states={
            S_CONTENT: [MessageHandler(private & filters.ALL & ~filters.COMMAND, got_content)],
            S_BUTTON: [
                CallbackQueryHandler(no_button_cb, pattern="^nobtn$"),
                MessageHandler(private & filters.TEXT & ~filters.COMMAND, got_button),
            ],
            S_CONFIRM: [
                CallbackQueryHandler(now_cb, pattern=r"^now:(users|chats)$"),
                CallbackQueryHandler(later_cb, pattern=r"^later:(users|chats)$"),
            ],
            S_WHEN: [MessageHandler(private & filters.TEXT & ~filters.COMMAND, got_when)],
        },
        fallbacks=[
            CallbackQueryHandler(cancel_cb, pattern="^cancel$"),
            CommandHandler("start", start),
            CommandHandler("menu", menu_cmd),
        ],
        allow_reentry=True,
    )
    app.add_handler(conv)
    app.add_handler(CommandHandler("start", start, filters=private))
    app.add_handler(CommandHandler("menu", menu_cmd, filters=private))
    app.add_handler(CommandHandler("stop", stop_cmd, filters=private))
    app.add_handler(CommandHandler("id", id_cmd, filters=private))
    app.add_handler(CallbackQueryHandler(menu_cb, pattern="^menu$"))
    app.add_handler(CallbackQueryHandler(noop_cb, pattern="^noop$"))
    app.add_handler(CallbackQueryHandler(recipients_cb, pattern="^recipients$"))
    app.add_handler(CallbackQueryHandler(export_cb, pattern="^export$"))
    app.add_handler(CallbackQueryHandler(stats_cb, pattern="^stats$"))
    app.add_handler(CallbackQueryHandler(sched_cb, pattern="^sched$"))
    app.add_handler(CallbackQueryHandler(sched_cancel_cb, pattern=r"^sc:\d+$"))
    app.add_handler(CallbackQueryHandler(chats_cb, pattern=r"^(chp:\d+|chr)$"))
    app.add_handler(CallbackQueryHandler(chat_toggle_cb, pattern=r"^cht:\d+:\d+$"))
    app.add_handler(CallbackQueryHandler(stop_broadcast_cb, pattern=r"^stop:\d+$"))
    app.add_handler(ChatMemberHandler(on_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_error_handler(on_error)

    app.run_polling(
        allowed_updates=["message", "callback_query", "my_chat_member"],
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
