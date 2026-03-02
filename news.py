import time
import sqlite3
import requests
import asyncio
import os
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import TelegramError

# =========================
# إعدادات
# =========================

BOT_TOKEN = os.environ.get("BOT_TOKEN")
TARGET_CHANNEL = "@newssokl"

MAX_AGE_SECONDS = 60
CHECK_EVERY_SECONDS = 5
MAX_POSTS_PER_CYCLE = 20
SLEEP_BETWEEN_SENDS = 0.4
DISABLE_WEB_PREVIEW = False
FETCH_LIMIT_PER_SOURCE = 25

SOURCES = [
    "IraninArabic",
    "Iraq_now3",
    "iraqalhadath_net",
    "mehwar_1",
    "ajanews",
    "Alarabiya",
    "ReutersAr",
    "France24_ar",
    "asharqnewsbreaking",
    "AlarabyTelevision",
]

DB_FILE = "posted.sqlite3"


def db_init():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS posted (
            post_id TEXT PRIMARY KEY,
            ts INTEGER
        )
    """)
    con.commit()
    return con


def already_posted(con, post_id: str) -> bool:
    cur = con.cursor()
    cur.execute("SELECT 1 FROM posted WHERE post_id = ?", (post_id,))
    return cur.fetchone() is not None


def mark_posted(con, post_id: str):
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO posted(post_id, ts) VALUES(?, ?)",
        (post_id, int(time.time())),
    )
    con.commit()


def fetch_channel_posts(username: str, limit: int):
    url = f"https://t.me/s/{username}"
    r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    blocks = soup.select("div.tgme_widget_message")
    posts = []

    for b in blocks[-limit:]:
        data_post = b.get("data-post")
        if not data_post:
            continue

        time_el = b.select_one("a.tgme_widget_message_date time")
        dt_utc = None
        if time_el and time_el.has_attr("datetime"):
            dt = datetime.fromisoformat(time_el["datetime"])
            dt_utc = dt.astimezone(timezone.utc)

        text_el = b.select_one("div.tgme_widget_message_text")
        text = text_el.get_text("\n").strip() if text_el else ""

        if not text:
            continue

        posts.append({"id": data_post, "text": text, "dt_utc": dt_utc})

    return posts


def is_fresh_enough(post_dt_utc):
    if post_dt_utc is None:
        return False
    now_utc = datetime.now(timezone.utc)
    age = (now_utc - post_dt_utc).total_seconds()
    return 0 <= age <= MAX_AGE_SECONDS


async def main():
    bot = Bot(token=BOT_TOKEN)
    con = db_init()

    me = await bot.get_me()
    print("Bot OK:", me.username)

    while True:
        sent_this_cycle = 0

        for src in SOURCES:
            if sent_this_cycle >= MAX_POSTS_PER_CYCLE:
                break

            try:
                posts = fetch_channel_posts(src, FETCH_LIMIT_PER_SOURCE)

                for p in posts:
                    if sent_this_cycle >= MAX_POSTS_PER_CYCLE:
                        break

                    if not is_fresh_enough(p["dt_utc"]):
                        continue

                    if already_posted(con, p["id"]):
                        continue

                    await bot.send_message(
                        chat_id=TARGET_CHANNEL,
                        text=p["text"],
                        disable_web_page_preview=DISABLE_WEB_PREVIEW,
                    )

                    mark_posted(con, p["id"])
                    sent_this_cycle += 1
                    await asyncio.sleep(SLEEP_BETWEEN_SENDS)

            except requests.RequestException:
                pass
            except TelegramError:
                pass

        await asyncio.sleep(CHECK_EVERY_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())