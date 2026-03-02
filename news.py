import os
import re
import time
import json
import html
import io
import hashlib
import logging
import sqlite3
import asyncio
import threading
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple

import requests
from bs4 import BeautifulSoup

from telegram import Bot
from telegram.error import TelegramError
from http.server import BaseHTTPRequestHandler, HTTPServer

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("telegram-news-bot")

# -------------------------
# Health server (Koyeb Web Service needs port/health)
# -------------------------
def start_health_server():
    port = int(os.environ.get("PORT", "8000"))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, format, *args):
            return

    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info(f"Health server running on port {port}")
    server.serve_forever()

# =========================
# Helpers
# =========================
def _json_list_env(name: str) -> List[str]:
    raw = os.environ.get(name)
    if not raw:
        return []
    try:
        v = json.loads(raw)
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
    except Exception:
        pass
    return []

def env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso_datetime_to_utc(s: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def normalize_text(text: str) -> str:
    # تنظيف آمن بدون حذف كلمات/سطور محتوى
    if not text:
        return ""
    t = html.unescape(text).replace("\r", "")
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def is_fresh(dt_utc: Optional[datetime], max_age_seconds: int) -> bool:
    if dt_utc is None:
        return False
    age = (now_utc() - dt_utc).total_seconds()
    return 0 <= age <= max_age_seconds

def content_fingerprint(text: str, media_url: Optional[str]) -> str:
    base = (text or "").strip() + "||" + (media_url or "")
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()

# =========================
# Config
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL", "@newssokl").strip()

# ✅ سريع جداً
CHECK_EVERY_SECONDS = int(os.environ.get("CHECK_EVERY_SECONDS", "1"))          # افتراضي 1 ثانية
SLEEP_BETWEEN_SENDS = float(os.environ.get("SLEEP_BETWEEN_SENDS", "0.30"))     # زيادة بسيطة لتقليل Flood
MAX_POSTS_PER_CYCLE = int(os.environ.get("MAX_POSTS_PER_CYCLE", "30"))

# ✅ فقط أقل من دقيقة
MAX_AGE_SECONDS = int(os.environ.get("MAX_AGE_SECONDS", "60"))

# ✅ نقلل القراءة حتى يصير أسرع
FETCH_LIMIT_PER_SOURCE = int(os.environ.get("FETCH_LIMIT_PER_SOURCE", "12"))

DISABLE_WEB_PREVIEW = env_bool("DISABLE_WEB_PREVIEW", True)

# حدود تحميل الميديا (حتى ما يطيح السيرفر بسبب فيديوهات كبيرة)
MAX_MEDIA_BYTES = int(os.environ.get("MAX_MEDIA_BYTES", str(20 * 1024 * 1024)))  # 20MB افتراضيًا

# مصادر (تقدر تزوّدها من ENV: SOURCES كـ JSON)
SOURCES = _json_list_env("SOURCES") or [
    "IraninArabic",
    "iraninarabic_ir",
    "arabic_farsnews",
    "Tasnim_Ar",
    "alalamarabic",
    "Khamenei_arabi",

    "almayadeen",
    "almanarnews",
    "ajanews",
    "Alarabiya",
    "alhadath_brk",
    "AlarabyTelevision",
    "RTarabic_br",

    "Iraq_now3",
    "iraqalhadath_net",
    "mehwar_1",
    "Iran_bel_Arabi",
    "alentedar",
]

DB_FILE = os.environ.get("DB_FILE", "posted.sqlite3")

# =========================
# DB (anti-duplicate)
# =========================
def db_init() -> sqlite3.Connection:
    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS posted (
            post_id TEXT PRIMARY KEY,
            fp TEXT,
            ts INTEGER
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_posted_ts ON posted(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_posted_fp ON posted(fp)")
    con.commit()
    return con

def already_posted(con: sqlite3.Connection, post_id: str, fp: str) -> bool:
    cur = con.cursor()
    cur.execute("SELECT 1 FROM posted WHERE post_id = ?", (post_id,))
    if cur.fetchone() is not None:
        return True
    cur.execute("SELECT 1 FROM posted WHERE fp = ?", (fp,))
    return cur.fetchone() is not None

def mark_posted(con: sqlite3.Connection, post_id: str, fp: str):
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO posted(post_id, fp, ts) VALUES(?, ?, ?)",
        (post_id, fp, int(time.time()))
    )
    con.commit()

def prune_old(con: sqlite3.Connection, keep_seconds: int = 7 * 24 * 3600):
    cur = con.cursor()
    cur.execute("DELETE FROM posted WHERE ts < ?", (int(time.time()) - keep_seconds,))
    con.commit()

# =========================
# Fetch from t.me/s/<channel>
# =========================
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})
SESSION_TIMEOUT = 20

def extract_media(block) -> Tuple[Optional[str], Optional[str]]:
    # Photo
    photo_wrap = block.select_one("a.tgme_widget_message_photo_wrap")
    if photo_wrap:
        style = photo_wrap.get("style", "")
        m = re.search(r"background-image:\s*url\('(.*?)'\)", style)
        if m:
            return "photo", m.group(1)

    # Video
    video = block.select_one("div.tgme_widget_message_video_wrap video")
    if video:
        src = video.get("src")
        if src:
            return "video", src
        source = video.select_one("source")
        if source and source.get("src"):
            return "video", source.get("src")

    return None, None

def fetch_channel_posts_sync(username: str, limit: int) -> List[Dict]:
    url = f"https://t.me/s/{username}"
    r = SESSION.get(url, timeout=SESSION_TIMEOUT)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    blocks = soup.select("div.tgme_widget_message")
    posts: List[Dict] = []

    for b in blocks[-limit:]:
        data_post = b.get("data-post")  # channel/123
        if not data_post:
            continue

        dt_utc = None
        time_el = b.select_one("a.tgme_widget_message_date time")
        if time_el and time_el.has_attr("datetime"):
            dt_utc = parse_iso_datetime_to_utc(time_el["datetime"])

        text_el = b.select_one("div.tgme_widget_message_text")
        text = text_el.get_text("\n").strip() if text_el else ""
        text = normalize_text(text)

        media_type, media_url = extract_media(b)

        if not text and not media_url:
            continue

        posts.append({
            "id": data_post,
            "src": username,  # داخلي فقط
            "dt_utc": dt_utc,
            "text": text,
            "media_type": media_type,
            "media_url": media_url,
        })

    return posts

async def fetch_channel_posts(username: str, limit: int) -> List[Dict]:
    return await asyncio.to_thread(fetch_channel_posts_sync, username, limit)

# =========================
# Message format (NO SOURCE / NO LINK / NO TIME)
# =========================
def format_pretty_text(post: Dict) -> str:
    """
    ✅ شكل جميل: عنوان + النص فقط (بدون وقت/رابط/مصدر).
    """
    body = post.get("text", "") or ""
    body = html.escape(body)

    header = "🟦 <b>خبر عاجل</b>\n"
    sep = "—" * 18
    text = f"{header}{sep}\n{body}"
    return text.strip()

def build_caption_from_formatted(formatted_html: str) -> Tuple[str, Optional[str]]:
    # كابشن الميديا 1024؛ نخلي هامش
    if len(formatted_html) <= 900:
        return formatted_html, None
    cap = formatted_html[:900].rstrip() + "…"
    extra = formatted_html[900:].lstrip()
    if extra:
        extra = "…" + extra
    return cap, extra

# =========================
# Download media (to avoid Telegram "Wrong type of web page content")
# =========================
def download_media_bytes(url: str) -> Optional[io.BytesIO]:
    """
    يحمل الميديا داخل الذاكرة بحد أقصى MAX_MEDIA_BYTES.
    """
    try:
        with requests.get(url, timeout=20, stream=True) as r:
            r.raise_for_status()
            total = 0
            buf = io.BytesIO()
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > MAX_MEDIA_BYTES:
                    log.warning(f"Media too large ({total} bytes), skipping download.")
                    return None
                buf.write(chunk)
            buf.seek(0)
            return buf
    except Exception as e:
        log.warning(f"Download failed: {e}")
        return None

# =========================
# Telegram send helpers
# =========================
async def send_text_html(bot: Bot, chat_id: str, html_text: str):
    t = (html_text or "").strip()
    if not t:
        return

    chunks: List[str] = []
    while len(t) > 4096:
        cut = t.rfind("\n", 0, 3800)
        if cut == -1:
            cut = 3800
        chunks.append(t[:cut])
        t = t[cut:].lstrip()
    if t:
        chunks.append(t)

    for c in chunks:
        await bot.send_message(
            chat_id=chat_id,
            text=c,
            parse_mode="HTML",
            disable_web_page_preview=DISABLE_WEB_PREVIEW,
        )
        await asyncio.sleep(SLEEP_BETWEEN_SENDS)

async def send_post(bot: Bot, chat_id: str, post: Dict) -> bool:
    formatted = format_pretty_text(post)

    media_type = post.get("media_type")
    media_url = post.get("media_url")

    # Text only
    if not media_type or not media_url:
        if not formatted:
            return False
        await send_text_html(bot, chat_id, formatted)
        return True

    caption, extra = build_caption_from_formatted(formatted)

    # Download first then send as file to avoid Telegram URL issues
    file_obj = await asyncio.to_thread(download_media_bytes, media_url)
    if not file_obj:
        # fallback to text only
        await send_text_html(bot, chat_id, formatted)
        return True

    try:
        if media_type == "photo":
            file_obj.name = "photo.jpg"
            await bot.send_photo(chat_id=chat_id, photo=file_obj, caption=caption, parse_mode="HTML")
        elif media_type == "video":
            file_obj.name = "video.mp4"
            await bot.send_video(
                chat_id=chat_id,
                video=file_obj,
                caption=caption,
                parse_mode="HTML",
                supports_streaming=True
            )
        else:
            await send_text_html(bot, chat_id, formatted)
            return True
    finally:
        try:
            file_obj.close()
        except Exception:
            pass

    await asyncio.sleep(SLEEP_BETWEEN_SENDS)
    if extra:
        await send_text_html(bot, chat_id, extra)

    return True

# =========================
# Main loop (FAST + parallel fetch)
# =========================
async def main():
    threading.Thread(target=start_health_server, daemon=True).start()

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing. Set it in environment variables.")

    bot = Bot(token=BOT_TOKEN)
    con = db_init()
    prune_old(con)

    me = await bot.get_me()
    log.info(f"✅ Bot OK: @{me.username}")
    log.info(f"🎯 Target: {TARGET_CHANNEL}")
    log.info(f"📡 Sources: {len(SOURCES)} channels")
    log.info(f"⚡ Check every: {CHECK_EVERY_SECONDS}s | Fresh window: {MAX_AGE_SECONDS}s")

    backoff = 1
    last_prune = time.time()

    while True:
        sent_this_cycle = 0

        try:
            tasks = [fetch_channel_posts(src, FETCH_LIMIT_PER_SOURCE) for src in SOURCES]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            all_posts: List[Dict] = []
            for r in results:
                if isinstance(r, Exception):
                    continue
                all_posts.extend(r)

            # فقط آخر دقيقة
            fresh_posts = [p for p in all_posts if is_fresh(p.get("dt_utc"), MAX_AGE_SECONDS)]
            fresh_posts.sort(key=lambda x: x.get("dt_utc") or now_utc())

            for p in fresh_posts:
                if sent_this_cycle >= MAX_POSTS_PER_CYCLE:
                    break

                fp = content_fingerprint(p.get("text", ""), p.get("media_url"))
                if already_posted(con, p["id"], fp):
                    continue

                ok = await send_post(bot, TARGET_CHANNEL, p)
                if ok:
                    mark_posted(con, p["id"], fp)
                    sent_this_cycle += 1

            backoff = 1

        except TelegramError as e:
            log.warning(f"TELEGRAM error: {e}")
            backoff = min(backoff * 2, 30)
        except Exception as e:
            log.exception(f"Unexpected error: {e}")
            backoff = min(backoff * 2, 30)

        # تنظيف DB كل 6 ساعات
        if time.time() - last_prune > 6 * 3600:
            try:
                prune_old(con)
                last_prune = time.time()
                log.info("🧹 DB pruned.")
            except Exception:
                log.exception("DB prune failed")

        await asyncio.sleep(max(CHECK_EVERY_SECONDS, backoff))

if __name__ == "__main__":
    asyncio.run(main())