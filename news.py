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
from urllib.parse import urlparse

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
        def _send_ok_headers(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()

        def do_GET(self):
            self._send_ok_headers()
            self.wfile.write(b"OK")

        def do_HEAD(self):
            self._send_ok_headers()

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
    if not text:
        return ""
    t = html.unescape(text).replace("\r", "")
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def remove_hashtags_and_freq(text: str) -> str:
    if not text:
        return ""

    t = text

    # حذف سطور فيها كلمة "تردد" أو "ترددات"
    lines = t.splitlines()
    filtered_lines = []
    for line in lines:
        if re.search(r"\bتردد\b|\bترددات\b", line):
            continue
        filtered_lines.append(line)
    t = "\n".join(filtered_lines)

    # حذف الهاشتاكات
    t = re.sub(r"#([\w\u0600-\u06FF_]+)", "", t)

    # حذف أرقام 5 خانات
    t = re.sub(r"\b\d{5}\b", "", t)

    # تنظيف فراغات
    t = re.sub(r"[ \t]{2,}", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    t = re.sub(r"^\s+|\s+$", "", t, flags=re.MULTILINE)
    return t.strip()

def is_fresh(dt_utc: Optional[datetime], max_age_seconds: int) -> bool:
    if dt_utc is None:
        return False
    age = (now_utc() - dt_utc).total_seconds()
    return 0 <= age <= max_age_seconds

def parse_post_id(data_post: str) -> Tuple[str, int]:
    try:
        ch, mid = data_post.split("/", 1)
        return ch, int(mid)
    except Exception:
        return "", -1

# =========================
# Config (ثابتة حسب طلبك)
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL", "@newssokl").strip()

OUR_USERNAME = "@newssokl"
_OUR_USER_PLAIN = "newssokl"

# ✅ لم نعد نستخدم رابط القناة داخل التوقيع
BRAND_NAME = os.environ.get("BRAND_NAME", "اخبار المرصد").strip()

# ✅ بحث كل 10 ثواني
CHECK_EVERY_SECONDS = 10

# إرسال سريع (انتبه من Flood)
SLEEP_BETWEEN_SENDS = float(os.environ.get("SLEEP_BETWEEN_SENDS", "0.15"))

MAX_POSTS_PER_CYCLE = int(os.environ.get("MAX_POSTS_PER_CYCLE", "50"))

# ✅ أي خبر عمره أكثر من 60 ثانية لا ينشر
MAX_AGE_SECONDS = 60

# ✅ خليها صغيرة للسرعة
FETCH_LIMIT_PER_SOURCE = int(os.environ.get("FETCH_LIMIT_PER_SOURCE", "3"))

DISABLE_WEB_PREVIEW = env_bool("DISABLE_WEB_PREVIEW", True)

MAX_MEDIA_BYTES = int(os.environ.get("MAX_MEDIA_BYTES", str(15 * 1024 * 1024)))  # 15MB

SOURCES = _json_list_env("SOURCES") or [
    "IraninArabic",
    "iraninarabic_ir",
    "arabic_farsnews",
    "Tasnim_Ar",
    "Khamenei_arabi",
    "alalamarabic",
    "almayadeen",
    "Iraq_now3",
    "mehwar_1",
    "iraqalhadath_net",
    "almanarnews",
    "manarbreaking",
    "ReutersAr",
]

DB_FILE = os.environ.get("DB_FILE", "posted.sqlite3")

# -------------------------
# Networking tuning
# -------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})
SESSION_TIMEOUT = float(os.environ.get("SESSION_TIMEOUT", "10"))
MAX_CONCURRENT_FETCHES = int(os.environ.get("MAX_CONCURRENT_FETCHES", "6"))

# =========================
# Text cleaning: remove other channel usernames + keep ours + add ours footer
# =========================
def strip_external_usernames(text: str) -> str:
    if not text:
        return ""

    t = text

    # حذف @username (مع استثناء يوزرنا)
    def _repl_at(m):
        u = (m.group(1) or "").lower()
        if u == _OUR_USER_PLAIN:
            return m.group(0)
        return ""

    t = re.sub(r"@([A-Za-z0-9_]{4,32})", _repl_at, t)

    # حذف روابط t.me/xxxx و t.me/s/xxxx (مع استثناء يوزرنا)
    def _repl_tme(m):
        u = (m.group("user") or "").lower()
        if u == _OUR_USER_PLAIN:
            return m.group(0)
        return ""

    t = re.sub(r"(?:https?://)?t\.me/(?:s/)?(?P<user>[A-Za-z0-9_]{4,32})(?:\S*)?", _repl_tme, t)

    t = re.sub(r"[ \t]{2,}", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def append_our_signature(text: str) -> str:
    base = (text or "").strip()
    # ✅ التوقيع: فقط اسم البراند + اليوزر (بدون رابط)
    sig = f"\n\n—\n{BRAND_NAME}\n{OUR_USERNAME}".strip()

    low = base.lower()
    if _OUR_USER_PLAIN in low:
        return base
    return (base + sig) if base else sig

def canonical_text_for_fp(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""

    t = t.lower()
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"(?:https?://)?t\.me/\S+", " ", t)
    t = re.sub(r"@([a-z0-9_]{4,32})", " ", t)
    t = re.sub(r"[“”\"'`]", " ", t)
    t = re.sub(r"[•·●♦■▶️➡️🔻🔺🔹🔸]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def media_basename(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        p = urlparse(url)
        name = (p.path or "").split("/")[-1]
        return name or ""
    except Exception:
        return ""

def content_fingerprint(text: str, media_type: Optional[str], media_url: Optional[str]) -> str:
    canon = canonical_text_for_fp(text)
    if canon:
        base = f"t||{canon}"
    else:
        base = f"m||{media_type or ''}||{media_basename(media_url)}"
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()

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
    cur.execute("SELECT 1 FROM posted WHERE fp = ?", (fp,))
    if cur.fetchone() is not None:
        return True
    cur.execute("SELECT 1 FROM posted WHERE post_id = ?", (post_id,))
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
def extract_media(block) -> Tuple[Optional[str], Optional[str]]:
    photo_wrap = block.select_one("a.tgme_widget_message_photo_wrap")
    if photo_wrap:
        style = photo_wrap.get("style", "")
        m = re.search(r"background-image:\s*url\('(.*?)'\)", style)
        if m:
            return "photo", m.group(1)

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
        data_post = b.get("data-post")
        if not data_post:
            continue

        dt_utc = None
        time_el = b.select_one("a.tgme_widget_message_date time")
        if time_el and time_el.has_attr("datetime"):
            dt_utc = parse_iso_datetime_to_utc(time_el["datetime"])

        # لا نأخذ القديم
        if dt_utc is None or not is_fresh(dt_utc, MAX_AGE_SECONDS):
            continue

        text_el = b.select_one("div.tgme_widget_message_text")
        text = text_el.get_text("\n").strip() if text_el else ""
        text = normalize_text(text)
        text = remove_hashtags_and_freq(text)

        # حذف يوزرات وروابط قنوات أخرى
        text = strip_external_usernames(text)
        text = normalize_text(text)

        media_type, media_url = extract_media(b)

        if not text and not media_url:
            continue

        _, mid = parse_post_id(data_post)

        posts.append({
            "id": data_post,
            "mid": mid,
            "src": username,
            "dt_utc": dt_utc,
            "text": text,
            "media_type": media_type,
            "media_url": media_url,
        })

    return posts

async def fetch_channel_posts(username: str, limit: int, sem: asyncio.Semaphore) -> List[Dict]:
    async with sem:
        return await asyncio.to_thread(fetch_channel_posts_sync, username, limit)

# =========================
# Message format
# =========================
def format_pretty_text(post: Dict) -> str:
    body = (post.get("text") or "").strip()
    body = append_our_signature(body)

    body_html = html.escape(body)
    header = "🟦 <b>خبر عاجل</b>\n"
    sep = "—" * 18
    return f"{header}{sep}\n{body_html}".strip()

# ✅✅ FIX: لا ترسل extra أبداً، وخلي التوقيع دائماً داخل الكابتشن حتى لو قصّينا
def build_caption_from_formatted(formatted_html: str) -> str:
    MAX_CAPTION = 900
    s = (formatted_html or "").strip()
    if len(s) <= MAX_CAPTION:
        return s

    # ✅ نحاول نثبت "كتلة التوقيع" داخل الكابتشن دائماً (اليوزر فقط)
    sig_key = html.escape(f"{OUR_USERNAME}")
    idx = s.rfind(sig_key)

    tail = ""
    if idx != -1:
        # نأخذ من خط الفاصل "—" إلى النهاية (حتى يبقى التوقيع كامل)
        start = s.rfind("—", 0, idx)
        if start != -1:
            tail = s[start:].strip()
        else:
            tail = s[max(0, idx - 120):].strip()

    if tail and len(tail) < MAX_CAPTION - 50:
        head_limit = MAX_CAPTION - len(tail) - 2
        head = s[:head_limit].rstrip()
        return (head + "…\n" + tail).strip()

    return (s[:MAX_CAPTION - 1].rstrip() + "…").strip()

# =========================
# Download media
# =========================
def download_media_bytes(url: str) -> Optional[io.BytesIO]:
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
                    try:
                        buf.close()
                    except Exception:
                        pass
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

    if not media_type or not media_url:
        if not formatted:
            return False
        await send_text_html(bot, chat_id, formatted)
        return True

    # ✅ هنا صار الكابتشن يرجع نص واحد فقط (بدون extra)
    caption = build_caption_from_formatted(formatted)

    file_obj = await asyncio.to_thread(download_media_bytes, media_url)
    if not file_obj:
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
    return True

# =========================
# Main loop
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
    log.info(f"🏷️ Our username: {OUR_USERNAME}")

    backoff = 0.2
    last_prune = time.time()

    # تتبع آخر رسالة بكل قناة حتى نرسل فقط الجديد
    last_mid: Dict[str, int] = {src: -1 for src in SOURCES}
    sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)

    while True:
        sent_this_cycle = 0
        try:
            tasks = [fetch_channel_posts(src, FETCH_LIMIT_PER_SOURCE, sem) for src in SOURCES]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            new_posts: List[Dict] = []

            for idx, r in enumerate(results):
                src = SOURCES[idx]
                if isinstance(r, Exception):
                    continue

                lm = last_mid.get(src, -1)
                for p in r:
                    mid = p.get("mid", -1)
                    if mid > lm:
                        new_posts.append(p)

                max_mid_here = max([p.get("mid", -1) for p in r], default=lm)
                if max_mid_here > lm:
                    last_mid[src] = max_mid_here

            new_posts.sort(key=lambda x: (x.get("dt_utc") or now_utc(), x.get("mid", -1)))

            for p in new_posts:
                if sent_this_cycle >= MAX_POSTS_PER_CYCLE:
                    break

                if not is_fresh(p.get("dt_utc"), MAX_AGE_SECONDS):
                    continue

                fp = content_fingerprint(p.get("text", ""), p.get("media_type"), p.get("media_url"))
                if already_posted(con, p["id"], fp):
                    continue

                ok = await send_post(bot, TARGET_CHANNEL, p)
                if ok:
                    mark_posted(con, p["id"], fp)
                    sent_this_cycle += 1

            backoff = 0.2

        except TelegramError as e:
            log.warning(f"TELEGRAM error: {e}")
            backoff = min(backoff * 2, 5)
        except Exception as e:
            log.exception(f"Unexpected error: {e}")
            backoff = min(backoff * 2, 5)

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