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

def day_key_utc() -> str:
    return now_utc().strftime("%Y-%m-%d")

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

def stable_pick(seq: List[str], key: str) -> str:
    if not seq:
        return ""
    h = hashlib.sha256(key.encode("utf-8", errors="ignore")).hexdigest()
    idx = int(h[:8], 16) % len(seq)
    return seq[idx]

# =========================
# Config
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL", "@newssokl").strip()

OUR_USERNAME = os.environ.get("OUR_USERNAME", "@newssokl").strip()
_OUR_USER_PLAIN = OUR_USERNAME.lstrip("@").lower()

BRAND_NAME = os.environ.get("BRAND_NAME", "").strip()

# ✅ سريع بس مو سبام
CHECK_EVERY_SECONDS = int(os.environ.get("CHECK_EVERY_SECONDS", "10"))

# ✅ آمن ضد Flood
SLEEP_BETWEEN_SENDS = float(os.environ.get("SLEEP_BETWEEN_SENDS", "0.4"))

# ✅ لا تنشر أكثر من هذا بكل دورة
MAX_POSTS_PER_CYCLE = int(os.environ.get("MAX_POSTS_PER_CYCLE", "12"))

# ✅ نافذة حداثة أكبر حتى ما يفوتك خبر (5 دقائق)
MAX_AGE_SECONDS = int(os.environ.get("MAX_AGE_SECONDS", "300"))

# ✅ نجلب أكثر حتى نختار (لكن نشرنا محدود)
FETCH_LIMIT_PER_SOURCE = int(os.environ.get("FETCH_LIMIT_PER_SOURCE", "10"))

DISABLE_WEB_PREVIEW = env_bool("DISABLE_WEB_PREVIEW", True)
MAX_MEDIA_BYTES = int(os.environ.get("MAX_MEDIA_BYTES", str(15 * 1024 * 1024)))  # 15MB

# ✅ سقف يومي للنشر (افتراضي 35)
DAILY_POST_LIMIT = int(os.environ.get("DAILY_POST_LIMIT", "0"))

# ✅ ملخص دوري لغير العاجل (افتراضي 15 دقيقة)
DIGEST_EVERY_SECONDS = int(os.environ.get("DIGEST_EVERY_SECONDS", "180"))
DIGEST_MAX_ITEMS = int(os.environ.get("DIGEST_MAX_ITEMS", "12"))

# CTA خفيف (اختياري)
CTA_EVERY_N_POSTS = int(os.environ.get("CTA_EVERY_N_POSTS", "8"))

NON_URGENT_DELAY_SECONDS = int(os.environ.get("NON_URGENT_DELAY_SECONDS", "180"))

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
# Urgent detection + smart rewriting (no meaning change)
# =========================
URGENT_WORDS = [
    "عاجل", "الآن", "قصف", "انفجار", "صاروخ", "استهداف", "اغتيال", "اشتباكات",
    "غارات", "تحذير", "إغلاق", "هجوم", "اعتراض", "زلزال", "حريق"
]

URGENT_HEADERS = [
    "🟥 <b>عاجل</b>",
    "🟧 <b>تطور جديد</b>",
    "🟦 <b>خبر مهم</b>",
]

UPDATE_LEADS = [
    "📍 تحديث:",
    "📌 التفاصيل:",
    "🔎 متابعة:",
    "🗞️ أفادت المصادر:",
]


IRAN_SOURCES = {
    "IraninArabic",
    "iraninarabic_ir",
    "arabic_farsnews",
    "Tasnim_Ar",
    "Khamenei_arabi",
}

BOMBARDMENT_WORDS = [
    "قصف", "قصفت", "قصفوا", "غارات", "غارة", "صاروخ", "صواريخ",
    "قصف صاروخي", "ضربة", "ضربات", "استهداف", "هجوم", "هجمات", "قنابل"
]

SAFE_IRAN_HYPE_LINES = [
    "⚡ تصعيد ميداني",
    "⚡ متابعة ميدانية عاجلة",
    "⚡ تطور ميداني مهم",
    "⚡ مستجدات الميدان",
]

def is_iran_source(src: str) -> bool:
    return (src or "").strip() in IRAN_SOURCES

def has_bombardment_words(text: str) -> bool:
    t = text or ""
    return any(w in t for w in BOMBARDMENT_WORDS)

def build_context_line(post: Dict) -> str:
    src = post.get("src", "")
    text = post.get("text", "") or ""
    if is_iran_source(src) and has_bombardment_words(text):
        key = f"iran-bomb|{src}|{post.get('id','')}"
        return stable_pick(SAFE_IRAN_HYPE_LINES, key)
    return ""

def is_urgent(text: str) -> bool:
    t = text or ""
    return any(w in t for w in URGENT_WORDS)

def smart_clean(text: str) -> str:
    """
    تغيير ذكي = تنسيق + إزالة زوائد + تقصير حشو.
    لا يغير المعنى.
    """
    t = normalize_text(text)
    t = remove_hashtags_and_freq(t)

    # إزالة تكرار الرموز/الإيموجي بشكل مبالغ
    t = re.sub(r"(🟥|🟦|🟧|🔴|🚨){2,}", r"\1", t)

    # توحيد الفراغات
    t = re.sub(r"[ \t]{2,}", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()

    # لو النص طويل: نخليه مركز (بدون تحريف)
    if len(t) > 700:
        t = t[:680].rstrip() + "…"

    # لو يبدأ بـ "عاجل |" أو "عاجل:" إلخ، نشيلها لأن احنا نضيف هيدر
    t = re.sub(r"^\s*(عاجل|عاجل جداً|عاجل جدا)\s*[\|\:\-–—]+\s*", "", t, flags=re.IGNORECASE).strip()

    return t

def add_lead_line(text: str, key: str) -> str:
    """
    نضيف سطر مقدمة متغير (بدون تغيير الخبر)
    """
    lead = stable_pick(UPDATE_LEADS, key)
    # إذا النص أصلاً يبدأ بمقدمة مشابهة، لا نكرر
    if text.startswith(("📍", "📌", "🔎", "🗞️")):
        return text
    return f"{lead} {text}".strip()

def append_signature_and_cta(text: str, attach_cta: bool) -> str:
    base = (text or "").strip()

    # إزالة اسم العلامة من أي منشور
    if BRAND_NAME:
        base = base.replace(BRAND_NAME, "").strip()
        base = re.sub(r"\n{3,}", "\n\n", base)

    if _OUR_USER_PLAIN in base.lower():
        return base

    if attach_cta:
        sig = f"\n\n—\n📌 لمتابعة التنبيهات فعّل الإشعارات 🔔\n{OUR_USERNAME}".strip()
    else:
        sig = f"\n\n—\n{OUR_USERNAME}".strip()

    return (base + sig) if base else sig

# =========================
# Text cleaning: remove other channel usernames + keep ours
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
# DB (anti-duplicate + queued/sent)
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
            ts INTEGER,
            status TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_posted_ts ON posted(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_posted_fp ON posted(fp)")
    con.commit()
    return con

def already_seen(con: sqlite3.Connection, post_id: str, fp: str) -> bool:
    cur = con.cursor()
    cur.execute("SELECT 1 FROM posted WHERE fp = ?", (fp,))
    if cur.fetchone() is not None:
        return True
    cur.execute("SELECT 1 FROM posted WHERE post_id = ?", (post_id,))
    return cur.fetchone() is not None

def mark_seen(con: sqlite3.Connection, post_id: str, fp: str, status: str):
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO posted(post_id, fp, ts, status) VALUES(?, ?, ?, ?)",
        (post_id, fp, int(time.time()), status)
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

        # تنظيفات
        text = remove_hashtags_and_freq(text)
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
# Message formatting
# =========================
def format_urgent_html(post: Dict, attach_cta: bool) -> str:
    src = post.get("src", "")
    base_text = smart_clean(post.get("text", ""))

    # مقدمة متغيرة حسب الخبر
    key = f"urgent|{src}|{post.get('id','')}"
    base_text = add_lead_line(base_text, key)

    base_text = append_signature_and_cta(base_text, attach_cta=attach_cta)

    header = stable_pick(URGENT_HEADERS, key)
    sep = "—" * 18
    return f"{header}\n{sep}\n{html.escape(base_text)}".strip()

def format_digest_html(items: List[Dict], attach_cta: bool) -> str:
    # ملخص: نقاط قصيرة بدون حشو
    lines = []
    for it in items[-DIGEST_MAX_ITEMS:]:
        t = smart_clean(it.get("text", ""))
        t = re.sub(r"\s+", " ", t).strip()
        if len(t) > 160:
            t = t[:159] + "…"
        if t:
            lines.append("• " + t)

    if not lines:
        return ""

    key = f"digest|{int(time.time()//DIGEST_EVERY_SECONDS)}"
    header = "🟩 <b>ملخص سريع</b>"
    sep = "—" * 18

    body = "\n".join(lines)
    body = append_signature_and_cta(body, attach_cta=attach_cta)
    return f"{header}\n{sep}\n{html.escape(body)}".strip()

def build_caption_from_formatted(formatted_html: str) -> str:
    MAX_CAPTION = 900
    s = (formatted_html or "").strip()
    if len(s) <= MAX_CAPTION:
        return s

    sig_key = html.escape(f"{OUR_USERNAME}")
    idx = s.rfind(sig_key)

    tail = ""
    if idx != -1:
        start = s.rfind("—", 0, idx)
        if start != -1:
            tail = s[start:].strip()
        else:
            tail = s[max(0, idx - 140):].strip()

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

async def send_post(bot: Bot, chat_id: str, post: Dict, html_text: str) -> bool:
    if not html_text:
        return False

    media_type = post.get("media_type")
    media_url = post.get("media_url")

    if not media_type or not media_url:
        await send_text_html(bot, chat_id, html_text)
        return True

    caption = build_caption_from_formatted(html_text)

    file_obj = await asyncio.to_thread(download_media_bytes, media_url)
    if not file_obj:
        await send_text_html(bot, chat_id, html_text)
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
            await send_text_html(bot, chat_id, html_text)
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
    log.info(f"⏳ Non-urgent delay: {NON_URGENT_DELAY_SECONDS}s | Daily limit: {DAILY_POST_LIMIT}")

    backoff = 0.4
    last_prune = time.time()

    last_mid: Dict[str, int] = {src: -1 for src in SOURCES}
    sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)

    delayed_queue: List[Dict] = []

    today = day_key_utc()
    posted_today = 0
    posts_since_cta = 0

    while True:
        sent_this_cycle = 0
        try:
            new_day = day_key_utc()
            if new_day != today:
                today = new_day
                posted_today = 0
                posts_since_cta = 0
                log.info("🗓️ New day: counters reset.")

            # أولاً: أرسل الأخبار غير العاجلة التي انتهت مهلة انتظارها
            now_ts = time.time()
            ready_items = [x for x in delayed_queue if x.get("send_at", 0) <= now_ts]
            still_waiting = [x for x in delayed_queue if x.get("send_at", 0) > now_ts]
            delayed_queue = still_waiting

            ready_items.sort(key=lambda x: (x.get("send_at", 0), x.get("mid", -1)))
            for item in ready_items:
                if sent_this_cycle >= MAX_POSTS_PER_CYCLE:
                    delayed_queue.append(item)
                    continue

                post = item["post"]
                fp = item["fp"]

                attach_cta = (CTA_EVERY_N_POSTS > 0 and posts_since_cta >= CTA_EVERY_N_POSTS)
                html_text = format_normal_html(post, attach_cta=attach_cta)

                ok = await send_post(bot, TARGET_CHANNEL, post, html_text=html_text)
                if ok:
                    mark_seen(con, post["id"], fp, status="sent_delayed")
                    sent_this_cycle += 1
                    if DAILY_POST_LIMIT > 0:
                        posted_today += 1
                    posts_since_cta = 0 if attach_cta else (posts_since_cta + 1)
                else:
                    item["send_at"] = time.time() + 30
                    delayed_queue.append(item)

            tasks = [fetch_channel_posts(src, FETCH_LIMIT_PER_SOURCE, sem) for src in SOURCES]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            new_posts: List[Dict] = []

            for idx, r in enumerate(results):
                src = SOURCES[idx]
                if isinstance(r, Exception):
                    log.warning(f"Fetch failed for {src}: {r}")
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

            queued_ids = {item["post"]["id"] for item in delayed_queue}

            for p in new_posts:
                if sent_this_cycle >= MAX_POSTS_PER_CYCLE:
                    break
                if not is_fresh(p.get("dt_utc"), MAX_AGE_SECONDS):
                    continue

                fp = content_fingerprint(p.get("text", ""), p.get("media_type"), p.get("media_url"))
                if already_seen(con, p["id"], fp) or p["id"] in queued_ids:
                    continue

                # عاجل: ينشر فوراً
                if is_urgent(p.get("text", "")):
                    attach_cta = (CTA_EVERY_N_POSTS > 0 and posts_since_cta >= CTA_EVERY_N_POSTS)
                    html_text = format_urgent_html(p, attach_cta=attach_cta)

                    ok = await send_post(bot, TARGET_CHANNEL, p, html_text=html_text)
                    if ok:
                        mark_seen(con, p["id"], fp, status="sent")
                        sent_this_cycle += 1
                        if DAILY_POST_LIMIT > 0:
                            posted_today += 1
                        posts_since_cta = 0 if attach_cta else (posts_since_cta + 1)
                    else:
                        log.warning(f"Failed to send urgent post: {p['id']}")
                else:
                    delayed_queue.append({
                        "post": p,
                        "fp": fp,
                        "send_at": time.time() + NON_URGENT_DELAY_SECONDS,
                    })
                    mark_seen(con, p["id"], fp, status="queued_delayed")

            backoff = 0.4

        except TelegramError as e:
            log.warning(f"TELEGRAM error: {e}")
            backoff = min(backoff * 2, 8)
        except Exception as e:
            log.exception(f"Unexpected error: {e}")
            backoff = min(backoff * 2, 8)

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