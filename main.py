import os
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

# DB imports
import sqlite3

# Optional postgres
DB_URL = os.getenv("DATABASE_URL")
if DB_URL:
    import psycopg2

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

# timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    from backports.zoneinfo import ZoneInfo  # if needed on old Python

# env
TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    raise RuntimeError("Please set TELEGRAM_TOKEN environment variable")
TIMEZONE = os.getenv("TIMEZONE", "UTC")
PORT = int(os.getenv("PORT", 8080))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# load scenarios
with open("scenarios.json", "r", encoding="utf-8") as f:
    scenarios = json.load(f)

# DB init: sqlite by default, postgres if DATABASE_URL provided
if DB_URL:
    # Postgres
    conn = psycopg2.connect(DB_URL, sslmode='require')
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scheduled_messages (
      id SERIAL PRIMARY KEY,
      chat_id BIGINT NOT NULL,
      payload TEXT NOT NULL,
      scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL,
      sent BOOLEAN DEFAULT FALSE
    );
    """)
else:
    DB_PATH = os.getenv("SQLITE_PATH", "bot-db.sqlite3")
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scheduled_messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chat_id INTEGER NOT NULL,
      payload TEXT NOT NULL,
      scheduled_time TEXT NOT NULL,
      sent INTEGER DEFAULT 0
    );
    """)
    conn.commit()

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# helper to send a payload (text, photo, audio)
async def send_payload(chat_id: int, item: dict):
    t = item.get("type")
    try:
        if t == "text":
            await bot.send_message(chat_id, item.get("text", ""), parse_mode="HTML")
        elif t == "photo":
            await bot.send_photo(chat_id, item.get("url"), caption=item.get("caption", ""), parse_mode="HTML")
        elif t == "audio":
            await bot.send_audio(chat_id, item.get("url"), caption=item.get("caption", ""), parse_mode="HTML")
        else:
            logger.warning("Unknown payload type: %s", t)
    except Exception as e:
        logger.exception("Failed to send payload: %s", e)

# background checker for due scheduled messages
async def due_checker():
    logger.info("Scheduler: started")
    while True:
        try:
            now_utc_iso = datetime.now(timezone.utc).isoformat()
            if DB_URL:
                # Postgres: sent is boolean
                cursor.execute(
                    "SELECT id, chat_id, payload FROM scheduled_messages WHERE sent=FALSE AND scheduled_time <= %s",
                    (now_utc_iso,)
                )
                rows = cursor.fetchall()
            else:
                cursor.execute(
                    "SELECT id, chat_id, payload FROM scheduled_messages WHERE sent=0 AND scheduled_time <= ?",
                    (now_utc_iso,)
                )
                rows = cursor.fetchall()

            for row in rows:
                id_, chat_id, payload = row
                try:
                    item = json.loads(payload)
                    await send_payload(chat_id, item)
                except Exception:
                    logger.exception("Error sending scheduled message id=%s", id_)
                # mark sent
                if DB_URL:
                    cursor.execute("UPDATE scheduled_messages SET sent=TRUE WHERE id=%s", (id_,))
                else:
                    cursor.execute("UPDATE scheduled_messages SET sent=1 WHERE id=?", (id_,))
                    conn.commit()
        except Exception:
            logger.exception("Scheduler loop failed")
        await asyncio.sleep(30)

# run the scenario sequence for a single user (chat_id)
async def run_sequence(chat_id: int):
    logger.info("Start sequence for %s", chat_id)
    for step in scenarios.get("sequence", []):
        typ = step.get("type")
        # send immediate
        if typ == "text":
            await bot.send_message(chat_id, step.get("text", ""), parse_mode="HTML")
        elif typ == "photo":
            await bot.send_photo(chat_id, step.get("url"), caption=step.get("caption", ""), parse_mode="HTML")
        elif typ == "audio":
            await bot.send_audio(chat_id, step.get("url"), caption=step.get("caption", ""), parse_mode="HTML")
        else:
            logger.warning("Unknown step type %s", typ)

        # delay between steps in seconds (optional)
        delay = step.get("delay_seconds", 0)
        if delay and delay > 0:
            await asyncio.sleep(delay)

        # if this step schedules a future message at a specific time (e.g. next day 10:00)
        if "schedule_next_day_time" in step:
            time_str = step["schedule_next_day_time"]  # format HH:MM, e.g. "10:00"
            tz_name = os.getenv("TIMEZONE", "UTC")
            try:
                tz = ZoneInfo(tz_name)
            except Exception:
                tz = timezone.utc
            now = datetime.now(tz)
            hh, mm = map(int, time_str.split(":"))
            scheduled_local = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if scheduled_local <= now:
                scheduled_local = scheduled_local + timedelta(days=1)
            # payload for scheduled send (a single message object like in sequence)
            payload = step.get("scheduled_message")
            if payload:
                scheduled_utc = scheduled_local.astimezone(timezone.utc).isoformat()
                if DB_URL:
                    cursor.execute(
                        "INSERT INTO scheduled_messages (chat_id, payload, scheduled_time) VALUES (%s, %s, %s)",
                        (chat_id, json.dumps(payload), scheduled_utc)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO scheduled_messages (chat_id, payload, scheduled_time) VALUES (?, ?, ?)",
                        (chat_id, json.dumps(payload), scheduled_utc)
                    )
                    conn.commit()
            else:
                logger.warning("schedule_next_day_time set but scheduled_message missing")
            # stop further immediate steps for this user — the rest will be handled later
            return

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.reply("Вы подписаны — начинаю отправлять сообщения.")
    asyncio.create_task(run_sequence(message.chat.id))

async def on_startup(dp):
    # start scheduler
    asyncio.create_task(due_checker())

    # minimal webserver for healthchecks
    from aiohttp import web
    async def handle(request):
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health server started on port %s", PORT)

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
