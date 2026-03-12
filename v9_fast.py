import psycopg2
import psycopg2.extras
import psycopg2.pool
from concurrent.futures import ThreadPoolExecutor

# ── Global connection pool & executor ────────────────────────
_db_pool: psycopg2.pool.ThreadedConnectionPool = None
_executor = ThreadPoolExecutor(max_workers=20)

"""
╔══════════════════════════════════════════════════════╗
║         DATA SCANNER BOT v5.0 — FULL UPGRADE        ║
║   Smart Search | Clean Output | 3-Min Timer         ║
╚══════════════════════════════════════════════════════╝
"""

import os
import re
import json
import time
import signal
import shutil
import asyncio
import logging
import logging.handlers
import zipfile
import hashlib
import threading
import pandas as pd
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

# ════════════════════════════════════════════
#                   CONFIG
# ════════════════════════════════════════════
TOKEN       = os.environ.get("BOT_TOKEN")
ADMIN_IDS   = [int(x) for x in os.environ.get("ADMIN_IDS", "8546436162").split(",")]
FILES_DIR   = "data_files"
BACKUP_DIR  = "db_backups"
REFERRAL_CREDITS   = 3
MIN_KEYWORD_LEN    = 3
WHITELIST_MODE     = False
WHITELIST_IDS: set = set()
BOT_START_TIME     = None
MAINTENANCE_MODE   = False
MAINTENANCE_MSG    = "🔧 *Bot is under maintenance.*\n\nPlease try again later."
MAX_RESULT_LINES   = 1000000000

os.makedirs(FILES_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)
_log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(_log_dir, exist_ok=True)
_fh = logging.handlers.RotatingFileHandler(
    os.path.join(_log_dir, "bot_errors.log"),
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding="utf-8"
)
_fh.setLevel(logging.WARNING)
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
log.addHandler(_fh)
logging.getLogger("telegram").addHandler(_fh)

SEARCH_TIMEOUT = 180

_last_search_time: dict = {}
_rate_limit_lock  = threading.Lock()

_cb_store: dict = {}
_user_search_locks: dict = {}
_CB_TTL = 3600

# ── User cache (reduces DB hits dramatically) ─────────────
_user_cache: dict = {}
_USER_CACHE_TTL = 30  # seconds — cache user row for 30s

# ════════════════════════════════════════════
#           DATABASE CONNECTION
# ════════════════════════════════════════════
DB_CONFIG = {
    "dbname":   os.environ.get("DB_NAME",     "scanner"),
    "user":     os.environ.get("DB_USER",     "postgres"),
    "password": os.environ.get("DB_PASSWORD", "123456"),
    "host":     os.environ.get("DB_HOST",     "127.0.0.1"),
    "port":     os.environ.get("DB_PORT",     "5432"),
}

def init_pool():
    """Initialize the global connection pool."""
    global _db_pool
    _db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=3,
        maxconn=30,
        connect_timeout=10,
        **DB_CONFIG
    )
    log.info("✅ PostgreSQL connection pool initialized (3–30 connections)")

def get_db():
    """Get a connection from the pool."""
    return _db_pool.getconn()

def release_db(conn):
    """Return a connection back to the pool."""
    try:
        _db_pool.putconn(conn)
    except Exception:
        pass

class _PoolConn:
    """Context manager: auto-acquire and release a pool connection."""
    def __init__(self):
        self.conn = None
    def __enter__(self):
        self.conn = get_db()
        return self.conn
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            try:
                self.conn.rollback()
            except Exception:
                pass
        release_db(self.conn)
        return False

def pool_conn():
    return _PoolConn()


def get_user_cached(uid: int):
    """Get user row from cache, fall back to DB if expired."""
    now = time.monotonic()
    if uid in _user_cache:
        row, ts = _user_cache[uid]
        if now - ts < _USER_CACHE_TTL:
            return row
    row = get_user(uid)
    _user_cache[uid] = (row, now)
    return row

def invalidate_user_cache(uid: int):
    """Call this after any UPDATE to users table."""
    _user_cache.pop(uid, None)

def _get_user_lock(uid: int) -> asyncio.Lock:
    if uid not in _user_search_locks:
        _user_search_locks[uid] = asyncio.Lock()
    return _user_search_locks[uid]

def _cb_put(data: str) -> str:
    key = "cb_" + hashlib.md5(data.encode()).hexdigest()[:12]
    _cb_store[key] = (data, time.monotonic())
    if len(_cb_store) > 500:
        now_t = time.monotonic()
        expired = [k for k, (_, ts) in list(_cb_store.items()) if now_t - ts > _CB_TTL]
        for k in expired:
            _cb_store.pop(k, None)
    return key

def _cb_get(key: str) -> str:
    entry = _cb_store.get(key)
    if entry:
        return entry[0]
    return key

# ════════════════════════════════════════════
#           LANGUAGE STRINGS (i18n)
# ════════════════════════════════════════════
STRINGS = {
    "en": {
        "hello": "Hello",
        "plan": "Plan",
        "daily_left": "Daily searches left",
        "credits": "Credits",
        "db_records": "Database Records",
        "nameid_records": "Name/ID Records",
        "menu_hint": "Use the menu below to search the database.",
        "btn_search": "🔍 Search Database",
        "btn_nameid": "🪪 Name / National ID Search",
        "btn_account": "📊 My Account",
        "btn_plans": "💳 Plans",
        "btn_help": "ℹ️ Help",
        "btn_subscribe": "📋 Subscribe Request",
        "btn_language": "🌐 Language / اللغة",
        "banned": "🚫 *Your account has been banned.*\n\nContact the admin if you believe this is a mistake.",
        "no_searches": "❌ *No searches remaining.*\n\nUpgrade your plan or buy credits.",
        "choose_lang": "🌐 *Choose your language:*",
        "lang_set": "✅ Language set to *English*.",
        "sub_req_title": "📋 *Subscription Request*",
        "sub_req_prompt": "Choose the plan you want to subscribe to:",
        "sub_req_sent": "✅ *Request Sent!*\n\n📦 Plan: *{tier}*\n🔢 Request ID: `#{req_id}`\n\nWe'll notify you once approved.",
        "sub_req_exists": "⏳ You already have a pending request (`#{req_id}`).\n\nPlease wait for admin review.",
        "sub_approved_user": "✅ *Subscription Approved!*\n\nYour account has been upgraded to *{tier}*.\n\nPress /start to refresh.",
        "sub_rejected_user": "❌ *Subscription request rejected.*\n\nContact admin for more info.",
        "account_title": "📊 *My Account*",
        "plans_contact": "📩 Contact @yut3ev to upgrade.",
        "help_title": "ℹ️ *How to Use Data Scanner Bot*",
    },
    "ar": {
        "hello": "مرحباً",
        "plan": "الباقة",
        "daily_left": "البحوث المتبقية اليوم",
        "credits": "الرصيد",
        "db_records": "سجلات قاعدة البيانات",
        "nameid_records": "سجلات الاسم/الرقم القومي",
        "menu_hint": "استخدم القائمة أدناه للبحث في قاعدة البيانات.",
        "btn_search": "🔍 بحث في قاعدة البيانات",
        "btn_nameid": "🪪 بحث بالاسم / الرقم القومي",
        "btn_account": "📊 حسابي",
        "btn_plans": "💳 الباقات",
        "btn_help": "ℹ️ المساعدة",
        "btn_subscribe": "📋 طلب اشتراك",
        "btn_language": "🌐 Language / اللغة",
        "banned": "🚫 تم حظرك.",
        "no_searches": "❌ *لا توجد بحوث متبقية.*\n\nقم بترقية باقتك أو شراء رصيد.",
        "choose_lang": "🌐 *اختر لغتك:*",
        "lang_set": "✅ تم تعيين اللغة إلى *العربية*.",
        "sub_req_title": "📋 *طلب اشتراك*",
        "sub_req_prompt": "اختر الباقة التي تريد الاشتراك فيها:",
        "sub_req_sent": "✅ *تم إرسال الطلب!*\n\n📦 الباقة: *{tier}*\n🔢 رقم الطلب: `#{req_id}`\n\nسيتم إخطارك عند الموافقة.",
        "sub_req_exists": "⏳ لديك طلب قيد المراجعة بالفعل (`#{req_id}`).\n\nانتظر رد الأدمن.",
        "sub_approved_user": "✅ *تمت الموافقة على الاشتراك!*\n\nتمت ترقية حسابك إلى *{tier}*.\n\nاضغط /start لتحديث لوحتك.",
        "sub_rejected_user": "❌ *تم رفض طلب الاشتراك.*\n\nتواصل مع الأدمن لمزيد من المعلومات.",
        "account_title": "📊 *حسابي*",
        "plans_contact": "📩 تواصل مع @yut3ev للترقية.",
        "help_title": "ℹ️ *كيفية استخدام بوت DATA SCANNER*",
    },
}

def get_lang(uid: int) -> str:
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT lang FROM users WHERE user_id=%s", (uid,))
                row = cur.fetchone()
        return row[0] if row and row[0] else "en"
    except Exception:
        return "en"

def s(uid: int, key: str) -> str:
    lang = get_lang(uid)
    return STRINGS.get(lang, STRINGS["en"]).get(key, STRINGS["en"].get(key, key))

TIERS = {
    "free":    {"label": "🆓 Free",    "daily": 0,      "max_results": 0,       "full_scan": False},
    "basic":   {"label": "⭐ Basic",   "daily": 10,     "max_results": 200,     "full_scan": False},
    "premium": {"label": "💎 Premium", "daily": 15,     "max_results": 1000,    "full_scan": True},
    "vip":     {"label": "👑 VIP",     "daily": 1000000, "max_results": 10000000, "full_scan": True},
}

NAMEID_TIERS = {
    "free":    {"daily_nameid": 0,  "max_nameid": 0},
    "basic":   {"daily_nameid": 2,  "max_nameid": 100},
    "premium": {"daily_nameid": 5,  "max_nameid": 150},
    "vip":     {"daily_nameid": 100, "max_nameid": 2000000},
}

# ════════════════════════════════════════════
#                  DATABASE INIT
# ════════════════════════════════════════════
def init_db():
    conn = get_db()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            tier TEXT DEFAULT 'free',
            daily_limit INTEGER DEFAULT 5,
            credits INTEGER DEFAULT 0,
            is_banned INTEGER DEFAULT 0,
            expires_at TEXT,
            joined_at TEXT,
            lang TEXT DEFAULT 'en',
            referred_by BIGINT DEFAULT NULL,
            referral_count INTEGER DEFAULT 0,
            last_search_at TEXT DEFAULT NULL,
            daily_nameid_limit INTEGER DEFAULT 0,
            frozen_until TEXT DEFAULT NULL,
            updated_at TEXT DEFAULT NULL,
            last_search_type TEXT DEFAULT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_index (
            id BIGSERIAL PRIMARY KEY,
            line TEXT NOT NULL,
            source TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS name_id_index (
            id BIGSERIAL PRIMARY KEY,
            full_name TEXT NOT NULL,
            national_id TEXT NOT NULL,
            source TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS uploaded_files (
            id SERIAL PRIMARY KEY,
            saved_name TEXT,
            original_name TEXT,
            size_bytes BIGINT,
            records INTEGER DEFAULT 0,
            uploaded_by BIGINT,
            uploaded_at TEXT,
            file_md5 TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS search_logs (
            user_id BIGINT,
            keyword TEXT,
            category TEXT,
            results INTEGER,
            timestamp TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sub_history (
            user_id BIGINT,
            tier TEXT,
            amount INTEGER,
            admin_id BIGINT,
            timestamp TEXT,
            referral_source BIGINT DEFAULT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sub_requests (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            full_name TEXT,
            requested_tier TEXT,
            status TEXT DEFAULT 'pending',
            timestamp TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_op_logs (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT,
            action TEXT,
            target TEXT,
            details TEXT,
            timestamp TEXT
        )
    """)

    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_logs_uid  ON search_logs(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_logs_ts   ON search_logs(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_logs_cat  ON search_logs(category)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_tier       ON users(tier)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_banned     ON users(is_banned)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_expires    ON users(expires_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_uploaded_ts      ON uploaded_files(uploaded_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_data_line        ON data_index USING gin(to_tsvector('simple', line))")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nameid_name      ON name_id_index(full_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nameid_id        ON name_id_index(national_id)")

    conn.commit()
    cur.close()
    release_db(conn)

# ════════════════════════════════════════════
#                   HELPERS
# ════════════════════════════════════════════
def get_user(uid):
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users WHERE user_id=%s", (uid,))
                row = cur.fetchone()
        return row
    except Exception as e:
        log.error(f"get_user error: {e}")
        return None

def ensure_user(uid, username, full_name):
    now_iso = datetime.utcnow().isoformat()
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM users WHERE user_id=%s", (uid,))
                exists = cur.fetchone()
                if not exists:
                    cur.execute(
                        """INSERT INTO users
                           (user_id, username, full_name, tier, daily_limit, credits,
                            is_banned, expires_at, joined_at, lang, referred_by,
                            referral_count, last_search_at, daily_nameid_limit, updated_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (uid, username or "", full_name or "User", "free", 5, 0, 0,
                         None, now_iso, "en", None, 0, None,
                         NAMEID_TIERS["free"]["daily_nameid"], now_iso)
                    )
            conn.commit()
        invalidate_user_cache(uid)
    except Exception as e:
        log.error(f"ensure_user error: {e}")

def is_admin(uid):
    return uid in ADMIN_IDS

def is_banned(uid):
    u = get_user_cached(uid)
    if not u:
        return False
    # index 6 = is_banned, index 14 = frozen_until
    if u[6]:
        return True
    frozen_until = u[14] if len(u) > 14 else None
    if frozen_until:
        try:
            if datetime.fromisoformat(str(frozen_until)) > datetime.utcnow():
                return True
        except Exception:
            pass
    return False

def get_tier(uid):
    u = get_user_cached(uid)
    return u[3] if u else "free"

def _check_and_expire(uid: int):
    u = get_user_cached(uid)
    if not u or not u[7]:
        return
    try:
        if datetime.fromisoformat(str(u[7])) < datetime.utcnow():
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET tier='free', daily_limit=0, daily_nameid_limit=0, expires_at=NULL WHERE user_id=%s",
                        (uid,)
                    )
                conn.commit()
            log.info(f"⏰ User {uid} subscription expired — downgraded to free.")
    except Exception:
        pass

def can_search(uid):
    if is_admin(uid):
        return True
    _check_and_expire(uid)
    u = get_user_cached(uid)
    if not u:
        return False
    if u[3] in ("premium", "vip"):
        return True
    return u[4] > 0 or u[5] > 0

def deduct(uid):
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT daily_limit, credits FROM users WHERE user_id=%s", (uid,))
                u = cur.fetchone()
                if u:
                    if u[0] > 0:
                        cur.execute("UPDATE users SET daily_limit=daily_limit-1 WHERE user_id=%s", (uid,))
                    elif u[1] > 0:
                        cur.execute("UPDATE users SET credits=credits-1 WHERE user_id=%s", (uid,))
            conn.commit()
        invalidate_user_cache(uid)
    except Exception as e:
        log.error(f"deduct error: {e}")

def log_admin_op(admin_id: int, action: str, target: str, details: str = ""):
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO admin_op_logs (admin_id, action, target, details, timestamp) VALUES (%s,%s,%s,%s,%s)",
                    (admin_id, action, target, details, datetime.utcnow().isoformat())
                )
            conn.commit()
    except Exception as e:
        log.error(f"log_admin_op error: {e}")

# ── Spam protection ───────────────────────────────────────
SEARCH_COOLDOWN_SECS = 5

def is_search_spamming(uid: int) -> bool:
    if is_admin(uid):
        return False
    with _rate_limit_lock:
        last = _last_search_time.get(uid)
    if last is None:
        return False
    return (time.monotonic() - last) < SEARCH_COOLDOWN_SECS

def mark_search_time(uid: int):
    with _rate_limit_lock:
        _last_search_time[uid] = time.monotonic()
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET last_search_at=%s WHERE user_id=%s",
                            (datetime.utcnow().isoformat(), uid))
            conn.commit()
        invalidate_user_cache(uid)
    except Exception as e:
        log.error(f"mark_search_time error: {e}")

# ── Name/ID quota helpers ─────────────────────────────────
def can_search_nameid(uid: int) -> bool:
    if is_admin(uid):
        return True
    u = get_user_cached(uid)
    if not u:
        return False
    tier = u[3]
    nt = NAMEID_TIERS.get(tier, NAMEID_TIERS["free"])
    if nt["daily_nameid"] >= 100000:
        return True
    return u[13] > 0 if len(u) > 13 else False

def deduct_nameid(uid: int):
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET daily_nameid_limit=GREATEST(0, daily_nameid_limit-1) WHERE user_id=%s",
                    (uid,)
                )
            conn.commit()
        invalidate_user_cache(uid)
    except Exception as e:
        log.error(f"deduct_nameid error: {e}")

def get_nameid_limit(uid: int) -> int:
    if is_admin(uid):
        return 10_000_000
    u = get_user_cached(uid)
    tier = u[3] if u else "free"
    return NAMEID_TIERS.get(tier, NAMEID_TIERS["free"])["max_nameid"]

# ── Referral helpers ──────────────────────────────────────
def process_referral(new_uid: int, ref_uid: int):
    if new_uid == ref_uid:
        return
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT referred_by FROM users WHERE user_id=%s", (new_uid,))
                already = cur.fetchone()
                if already and already[0]:
                    return
                cur.execute("UPDATE users SET referred_by=%s WHERE user_id=%s", (ref_uid, new_uid))
                cur.execute(
                    "UPDATE users SET credits=credits+%s, referral_count=referral_count+1 WHERE user_id=%s",
                    (REFERRAL_CREDITS, ref_uid)
                )
            conn.commit()
    except Exception as e:
        log.error(f"process_referral error: {e}")

def get_referral_stats(uid: int):
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT referral_count FROM users WHERE user_id=%s", (uid,))
                row = cur.fetchone()
        return row[0] if row else 0
    except Exception:
        return 0

# ── DB Backup (file-based not applicable for PG, just log) ──
def backup_db():
    ts   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(BACKUP_DIR, f"pg_backup_note_{ts}.txt")
    with open(dest, "w") as f:
        f.write(f"PostgreSQL backup marker — {ts}\n")
        f.write("Run: pg_dump -U postgres scanner > backup.sql\n")
    backups = sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith(".txt")], reverse=True)
    for old in backups[7:]:
        os.remove(os.path.join(BACKUP_DIR, old))
    return dest

# ── Auto daily reset ──────────────────────────────────────
def do_daily_reset():
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                for tn, td in TIERS.items():
                    cur.execute("UPDATE users SET daily_limit=%s WHERE tier=%s", (td["daily"], tn))
                for tn, nd in NAMEID_TIERS.items():
                    cur.execute("UPDATE users SET daily_nameid_limit=%s WHERE tier=%s", (nd["daily_nameid"], tn))
            conn.commit()
    except Exception as e:
        log.error(f"do_daily_reset error: {e}")

def log_search(uid, keyword, category, count):
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO search_logs VALUES (%s,%s,%s,%s,%s)",
                    (uid, keyword, category, count, datetime.utcnow().isoformat())
                )
                if category and not category.startswith("nameid_"):
                    cur.execute(
                        "UPDATE users SET last_search_type=%s WHERE user_id=%s",
                        (category, uid)
                    )
            conn.commit()
    except Exception as e:
        log.error(f"log_search error: {e}")

# ════════════════════════════════════════════
#          RESULT COUNTER
# ════════════════════════════════════════════
def escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")

def count_matches_fast(keyword: str, stype: str) -> int:
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                # Use GIN full-text index for keywords >= 3 chars (fast), ILIKE for short ones
                if len(keyword) >= 3:
                    cur.execute(
                        "SELECT COUNT(*) FROM data_index WHERE to_tsvector('simple', line) @@ plainto_tsquery('simple', %s)",
                        (keyword,)
                    )
                else:
                    cur.execute(
                        "SELECT COUNT(*) FROM data_index WHERE line ILIKE %s",
                        (f"%{keyword}%",)
                    )
                row = cur.fetchone()
        return row[0] if row else 0
    except Exception as e:
        log.error(f"count_matches_fast error: {e}")
        return 0

def count_nameid_matches(query: str, qtype: str) -> int:
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                if qtype == "national_id":
                    cleaned = re.sub(r"\s", "", query)
                    cur.execute(
                        "SELECT COUNT(*) FROM name_id_index WHERE national_id=%s",
                        (cleaned,)
                    )
                elif qtype == "partial_id":
                    cleaned = re.sub(r"\s", "", query)
                    cur.execute(
                        "SELECT COUNT(*) FROM name_id_index WHERE national_id ILIKE %s",
                        (f"%{cleaned}%",)
                    )
                else:  # name
                    norm_q = normalize_arabic(query)
                    cur.execute(
                        "SELECT COUNT(*) FROM name_id_index WHERE full_name ILIKE %s",
                        (f"%{norm_q}%",)
                    )
                row = cur.fetchone()
        return row[0] if row else 0
    except Exception as e:
        log.error(f"count_nameid_matches error: {e}")
        return 0

# ════════════════════════════════════════════
#     NAME / NATIONAL-ID DETECTION & SEARCH
# ════════════════════════════════════════════
def normalize_arabic(text: str) -> str:
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"[ًٌٍَُِّْـ]", "", text)
    text = re.sub(r"[أإآ]", "ا", text)
    text = re.sub(r"ة", "ه", text)
    text = re.sub(r"ى", "ي", text)
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.lower()

def is_national_id(value: str) -> bool:
    cleaned = re.sub(r"\s", "", value)
    return bool(re.fullmatch(r"\d{14}", cleaned))

def is_partial_national_id(value: str) -> bool:
    cleaned = re.sub(r"\s", "", value)
    return bool(re.fullmatch(r"\d{4,13}", cleaned))

def search_by_name(query: str, limit: int = 50) -> list:
    norm_q = normalize_arabic(query)
    query_words = [w for w in norm_q.split() if len(w) >= 2]
    if not query_words:
        return []
    results = []
    seen = set()
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                # Use ILIKE for each word
                conditions = " AND ".join(["full_name ILIKE %s"] * len(query_words))
                params = [f"%{w}%" for w in query_words] + [limit * 5]
                cur.execute(
                    f"SELECT full_name, national_id FROM name_id_index WHERE {conditions} LIMIT %s",
                    params
                )
                rows = cur.fetchall()
        for name, nat_id in rows:
            key = (name.strip(), nat_id.strip())
            if key not in seen:
                seen.add(key)
                results.append({"name": name.strip(), "national_id": nat_id.strip()})
            if len(results) >= limit:
                break
    except Exception as e:
        log.error(f"search_by_name error: {e}")
    return results

def search_by_national_id(query: str, limit: int = 50) -> list:
    cleaned = re.sub(r"\s", "", query)
    results = []
    seen = set()
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                if is_national_id(query):
                    cur.execute(
                        "SELECT full_name, national_id FROM name_id_index WHERE national_id=%s LIMIT %s",
                        (cleaned, limit)
                    )
                else:
                    cur.execute(
                        "SELECT full_name, national_id FROM name_id_index WHERE national_id ILIKE %s LIMIT %s",
                        (f"%{cleaned}%", limit)
                    )
                rows = cur.fetchall()
        for name, nat_id in rows:
            key = (name.strip(), nat_id.strip())
            if key not in seen:
                seen.add(key)
                results.append({"name": name.strip(), "national_id": nat_id.strip()})
    except Exception as e:
        log.error(f"search_by_national_id error: {e}")
    return results

def detect_nameid_query_type(query: str) -> str:
    q = query.strip()
    if is_national_id(q):
        return "national_id"
    if is_partial_national_id(q):
        return "partial_id"
    return "name"

def parse_excel_for_name_id(path: str, original_name: str) -> list:
    try:
        df = pd.read_excel(path, dtype=str)
    except Exception:
        return []

    NAME_ALIASES = {"اسم", "الاسم", "اسم كامل", "الاسم الكامل",
                    "name", "full name", "fullname", "full_name", "customer name"}
    ID_ALIASES   = {"رقم قومي", "الرقم القومي", "رقم هوية",
                    "national id", "nationalid", "national_id", "id number",
                    "id_number", "nid", "ssn", "identity"}

    name_col = None
    id_col   = None

    for col in df.columns:
        col_norm = str(col).strip().lower().replace("_", " ")
        if col_norm in NAME_ALIASES or any(a in col_norm for a in ("اسم", "name")):
            name_col = col
            break

    for col in df.columns:
        col_norm = str(col).strip().lower().replace("_", " ")
        if col_norm in ID_ALIASES or any(a in col_norm for a in ("قومي", "national", "id", "هوية")):
            sample  = df[col].dropna().astype(str).head(20)
            matches = sample.apply(lambda x: bool(re.fullmatch(r"\d{6,14}", re.sub(r"\D", "", x))))
            if matches.mean() > 0.4:
                id_col = col
                break

    if id_col is None:
        for col in df.columns:
            sample  = df[col].dropna().astype(str).head(20)
            matches = sample.apply(lambda x: bool(re.fullmatch(r"\d{14}", re.sub(r"\D", "", x))))
            if matches.mean() > 0.5:
                id_col = col
                break

    if not name_col or not id_col:
        return []

    results = []
    seen = set()
    for _, row in df.iterrows():
        name   = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
        nat_id = str(row[id_col]).strip()   if pd.notna(row[id_col])   else ""
        nat_id = re.sub(r"\D", "", nat_id)
        if name and re.fullmatch(r"\d{6,14}", nat_id):
            key = (name.lower(), nat_id)
            if key not in seen:
                seen.add(key)
                results.append((name, nat_id, original_name))
    return results

# ════════════════════════════════════════════
#         SMART LINE PARSER
# ════════════════════════════════════════════
def is_email(v):
    return bool(re.match(r"[^@\s]{1,64}@[^@\s]+\.[^@\s]{2,}", v))

def is_url(v):
    return bool(re.match(r"https?://\S+", v, re.I))

def is_domain_str(v):
    return bool(re.match(r"^(www\.)?[\w\-]+\.[a-z]{2,}(/\S*)?$", v, re.I))

def is_phone_str(v):
    cleaned = re.sub(r"[\s\-().+]", "", v)
    if re.fullmatch(r"\d{14}", cleaned):
        return False
    return bool(re.match(r"^\+?[\d\s\-().]{6,15}$", v))

def parse_line_fields(line: str) -> dict:
    line = line.strip()
    if not line:
        return {}
    if line.startswith("{") and line.endswith("}"):
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                result = {}
                EMAIL_KEYS    = {"email", "mail", "e-mail", "e_mail"}
                PASS_KEYS     = {"password", "pass", "passwd", "pwd", "secret"}
                URL_KEYS      = {"url", "site", "domain", "host", "link", "website"}
                USERNAME_KEYS = {"username", "user", "login", "name", "uname", "nick"}
                PHONE_KEYS    = {"phone", "mobile", "tel", "cell", "number"}
                for k, v in obj.items():
                    kl = k.lower().strip()
                    sv = str(v).strip() if v else ""
                    if not sv or sv in ("null", "none", "nan"):
                        continue
                    if kl in EMAIL_KEYS and "email" not in result:
                        result["email"] = sv
                    elif kl in PASS_KEYS and "password" not in result:
                        result["password"] = sv
                    elif kl in URL_KEYS and "url" not in result:
                        result["url"] = sv
                    elif kl in USERNAME_KEYS and "username" not in result:
                        result["username"] = sv
                    elif kl in PHONE_KEYS and "phone" not in result:
                        result["phone"] = sv
                if result:
                    return result
        except (json.JSONDecodeError, Exception):
            pass

    if "|" in line or ";" in line or "\t" in line:
        parts = [p.strip() for p in re.split(r"[|;\t]", line) if p.strip()]
    else:
        url_match = re.match(r"(https?://[^:|\s]+)(.*)", line, re.I)
        if url_match:
            url_part = url_match.group(1)
            rest     = url_match.group(2).lstrip(":")
            parts    = [url_part] + [p.strip() for p in rest.split(":") if p.strip()]
        else:
            parts = [p.strip() for p in line.split(":") if p.strip()]

    result = {}
    for p in parts:
        if not p:
            continue
        if is_url(p) and "url" not in result:
            result["url"] = p
        elif is_email(p) and "email" not in result:
            result["email"] = p
        elif is_domain_str(p) and "url" not in result and "domain" not in result:
            result["domain"] = p
        elif is_phone_str(p) and "phone" not in result:
            result["phone"] = p
        else:
            if "username" not in result and "email" not in result:
                result["username"] = p
            elif ("username" in result or "email" in result) and "password" not in result:
                result["password"] = p
            elif "username" not in result:
                result["username"] = p
    return result

def line_matches_keyword(line: str, keyword: str) -> bool:
    return keyword.lower() in line.lower()

def extract_for_search_type(line: str, stype: str, keyword: str):
    if not line_matches_keyword(line, keyword):
        return None
    fields = parse_line_fields(line)
    if not fields:
        return None
    kw_lower = keyword.lower()
    if stype == "domain":
        url = fields.get("url", "") or fields.get("domain", "")
        if kw_lower not in url.lower():
            return None
    elif stype == "url":
        url = fields.get("url", "")
        if kw_lower not in url.lower():
            return None
    elif stype == "email":
        email = fields.get("email", "")
        if kw_lower not in email.lower():
            return None
    elif stype == "phone":
        phone = fields.get("phone", "")
        if kw_lower not in phone.lower():
            return None
    elif stype in ("username", "login"):
        uname = fields.get("username", "") or fields.get("email", "")
        if kw_lower not in uname.lower():
            return None
    elif stype == "password":
        pwd = fields.get("password", "")
        if kw_lower not in pwd.lower():
            return None
    return fields

def smart_search(keyword: str, stype: str, limit: int) -> list:
    fetch_limit = limit * 10
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                # Use GIN full-text index for keywords >= 3 chars (dramatically faster)
                if len(keyword) >= 3:
                    cur.execute(
                        "SELECT line FROM data_index WHERE to_tsvector('simple', line) @@ plainto_tsquery('simple', %s) LIMIT %s",
                        (keyword, fetch_limit)
                    )
                else:
                    cur.execute(
                        "SELECT line FROM data_index WHERE line ILIKE %s LIMIT %s",
                        (f"%{keyword}%", fetch_limit)
                    )
                rows = cur.fetchall()
    except Exception as e:
        log.error(f"smart_search query error: {e}")
        return []

    results = []
    seen = set()
    for (line,) in rows:
        fields = extract_for_search_type(line, stype, keyword)
        if fields is None:
            continue
        fp_parts = []
        for field in ("url", "domain", "email", "username", "phone", "password", "login"):
            val = fields.get(field, "").strip().lower()
            if val:
                fp_parts.append(val)
        if not fp_parts:
            fp_parts = [line.strip().lower()]
        fp = hashlib.md5("|".join(fp_parts).encode()).hexdigest()
        if fp in seen:
            continue
        seen.add(fp)
        results.append(fields)
        if len(results) >= limit:
            break
    return results

# ════════════════════════════════════════════
#         BUILD CLEAN RESULT FILE
# ════════════════════════════════════════════
async def safe_send_document(send_fn, path: str, filename: str, caption: str, reply_markup=None):
    MAX_CAPTION = 1024
    if len(caption) > MAX_CAPTION:
        caption = caption[:MAX_CAPTION - 10] + "\n_..._"

    last_err = None
    for attempt in range(3):
        try:
            with open(path, "rb") as f:
                await send_fn(
                    document=f, filename=filename,
                    caption=caption, parse_mode="Markdown",
                    reply_markup=reply_markup
                )
            return
        except BadRequest:
            plain = re.sub(r"[*_`\[\]]", "", caption)
            try:
                with open(path, "rb") as f:
                    await send_fn(
                        document=f, filename=filename,
                        caption=plain[:MAX_CAPTION],
                        reply_markup=reply_markup
                    )
                return
            except Exception as e:
                last_err = e
                break
        except RetryAfter as e:
            await asyncio.sleep(int(e.retry_after) + 1)
            last_err = e
        except TelegramError as e:
            last_err = e
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
        except Exception as e:
            last_err = e
            break

    log.error(f"safe_send_document failed: {last_err}")

def build_result_txt(keyword: str, results: list, stype: str) -> str:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    truncated = False
    if len(results) > MAX_RESULT_LINES:
        results = results[:MAX_RESULT_LINES]
        truncated = True

    email_pass  = []
    email_only  = []
    url_combo   = []
    login_combo = []
    phone_list  = []

    for r in results:
        url  = r.get("url", "") or r.get("domain", "")
        em   = r.get("email", "")
        user = r.get("username", "")
        pwd  = r.get("password", "")
        ph   = r.get("phone", "")
        if em and pwd:
            if url:
                url_combo.append((url, em, pwd))
            else:
                email_pass.append((em, pwd))
        elif url and (em or user) and pwd:
            url_combo.append((url, em or user, pwd))
        elif (em or user) and pwd:
            login_combo.append((em or user, pwd))
        elif em:
            email_only.append(em)
        elif ph:
            phone_list.append(ph)
        elif user:
            login_combo.append((user, pwd))

    total = len(results)
    lines = [
        "═" * 60,
        "  🔍 DATA SCANNER BOT v5.4 — SCAN RESULTS",
        "═" * 60,
        f"  📌 Target   : {keyword}",
        f"  📂 Type     : {stype.upper()}",
        f"  📊 Total    : {total:,} records" + (" (truncated to 100K)" if truncated else ""),
        f"  📧 Email:Pass : {len(email_pass) + len(url_combo):,}",
        f"  👤 Login:Pass : {len(login_combo):,}",
        f"  📱 Phone      : {len(phone_list):,}",
        f"  🕐 Generated  : {now} UTC",
        f"  🤖 Bot        : @DataScannerBot",
        "═" * 60, "",
    ]
    if truncated:
        lines.insert(3, f"  ⚠️  Results exceeded {MAX_RESULT_LINES:,} — showing first {MAX_RESULT_LINES:,} only")
    if email_pass:
        lines += [f"{'─'*55}", f"  📧 EMAIL:PASS — {len(email_pass)} results", f"{'─'*55}"]
        for em, pwd in email_pass:
            lines.append(f"{em}:{pwd}")
        lines.append("")
    if url_combo:
        lines += [f"{'─'*55}", f"  🌐 URL | USER:PASS — {len(url_combo)} results", f"{'─'*55}"]
        for url, user, pwd in url_combo:
            lines.append(f"{url}|{user}:{pwd}")
        lines.append("")
    if login_combo:
        lines += [f"{'─'*55}", f"  👤 USER:PASS — {len(login_combo)} results", f"{'─'*55}"]
        for user, pwd in login_combo:
            lines.append(f"{user}:{pwd}" if pwd else user)
        lines.append("")
    if email_only:
        lines += [f"{'─'*55}", f"  📧 EMAIL ONLY — {len(email_only)} results", f"{'─'*55}"]
        lines += email_only + [""]
    if phone_list:
        lines += [f"{'─'*55}", f"  📱 PHONE — {len(phone_list)} results", f"{'─'*55}"]
        lines += phone_list + [""]
    lines += ["═"*55, f"  ✅ Total: {total:,} clean records", "═"*55]
    return "\n".join(lines)

def build_nameid_result_txt(keyword: str, results: list, qtype: str) -> str:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    type_label = {"national_id": "🪪 رقم قومي", "partial_id": "🔢 رقم جزئي", "name": "👤 اسم"}.get(qtype, qtype)
    lines = [
        "═" * 55,
        "  🔍 DATA SCANNER BOT — NAME / NATIONAL ID RESULTS",
        "═" * 55,
        f"  📌 Query  : {keyword}",
        f"  📂 Type   : {type_label}",
        f"  📊 Total  : {len(results)} records",
        f"  🕐 Time   : {now}",
        "═" * 55, "",
        f"{'─'*55}",
        f"  {'الاسم':<35} {'الرقم القومي'}",
        f"{'─'*55}",
    ]
    for r in results:
        name   = r.get("name", "—")
        nat_id = r.get("national_id", "—")
        lines.append(f"  {name:<35} {nat_id}")
    lines += ["", "═"*55, f"  ✅ Total: {len(results)} records", "═"*55]
    return "\n".join(lines)

# ════════════════════════════════════════════
#               FILE PARSING
# ════════════════════════════════════════════
def _open_text_file(path: str):
    for enc in ("utf-8", "cp1256", "latin-1"):
        try:
            f = open(path, "r", encoding=enc, errors="strict")
            f.read(512)
            f.seek(0)
            return f, enc
        except (UnicodeDecodeError, Exception):
            try:
                f.close()
            except Exception:
                pass
    return open(path, "r", encoding="utf-8", errors="replace"), "utf-8(replace)"

def parse_file(path: str, original_name: str) -> list:
    results = []
    source  = original_name
    ext     = original_name.lower().rsplit(".", 1)[-1] if "." in original_name else ""
    seen    = set()

    def add_line(line: str):
        if not line:
            return
        line = line.strip()
        if not line or len(line) < 3:
            return
        if line.lower() in ("null", "none"):
            return
        if line not in seen:
            seen.add(line)
            results.append((line, source))

    if ext == "txt":
        f, enc = _open_text_file(path)
        log.info(f"Parsing TXT [{enc}]: {original_name}")
        with f:
            for line in f:
                add_line(line)
    elif ext == "csv":
        loaded = False
        for enc in ("utf-8", "cp1256", "latin-1"):
            try:
                df = pd.read_csv(path, dtype=str, on_bad_lines="skip", encoding=enc)
                loaded = True
                break
            except (UnicodeDecodeError, Exception):
                continue
        if not loaded:
            df = pd.read_csv(path, dtype=str, on_bad_lines="skip", encoding="latin-1", errors="replace")
        for _, row in df.iterrows():
            vals = [str(v).strip() for v in row if pd.notna(v) and str(v).strip()]
            if vals:
                add_line(":".join(vals))
    elif ext in ("xlsx", "xls"):
        df = pd.read_excel(path, dtype=str)
        for _, row in df.iterrows():
            vals = [str(v).strip() for v in row if pd.notna(v) and str(v).strip()]
            if vals:
                add_line(":".join(vals))
    elif ext == "json":
        f, enc = _open_text_file(path)
        with f:
            raw = f.read()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = []
            for ln in raw.splitlines():
                ln = ln.strip()
                if ln:
                    try:
                        data.append(json.loads(ln))
                    except Exception:
                        pass

        def flatten(obj):
            if isinstance(obj, dict):
                yield json.dumps(obj, ensure_ascii=False)
                for v in obj.values():
                    yield from flatten(v)
            elif isinstance(obj, list):
                for item in obj:
                    yield from flatten(item)
            elif isinstance(obj, str):
                yield obj

        for v in flatten(data):
            add_line(v)

    return results

# ════════════════════════════════════════════
#                  KEYBOARDS
# ════════════════════════════════════════════
def user_main_kb(uid: int = 0):
    lang  = get_lang(uid) if uid else "en"
    is_ar = lang == "ar"
    st    = STRINGS.get(lang, STRINGS["en"])
    rows = [
        [InlineKeyboardButton(st["btn_search"],    callback_data="go_search")],
        [InlineKeyboardButton(st["btn_nameid"],    callback_data="go_nameid")],
        [
            InlineKeyboardButton(st["btn_account"], callback_data="my_account"),
            InlineKeyboardButton(st["btn_plans"],   callback_data="show_plans"),
        ],
        [InlineKeyboardButton(st["btn_subscribe"], callback_data="user_subscribe")],
        [
            InlineKeyboardButton(st["btn_help"],     callback_data="show_help"),
            InlineKeyboardButton(st["btn_language"], callback_data="set_language"),
        ],
        [InlineKeyboardButton("📜 " + ("بحوثي الأخيرة" if is_ar else "My Search History"), callback_data="my_history")],
        [InlineKeyboardButton("🔗 " + ("دعوة صديق" if is_ar else "Referral Link"), callback_data="my_referral")],
        [InlineKeyboardButton("🆔 " + ("معرفي" if is_ar else "My ID"), callback_data="my_id")],
    ]
    if uid:
        u = get_user_cached(uid)
        if u:
            tier_v = u[3]
            exp_at = u[7]
            if tier_v == "free" or (exp_at and (datetime.fromisoformat(str(exp_at)) - datetime.utcnow()).days <= 3):
                renew_label = "🔄 تجديد الاشتراك" if is_ar else "🔄 Renew Subscription"
                rows.insert(3, [InlineKeyboardButton(renew_label, callback_data="user_subscribe")])
    return InlineKeyboardMarkup(rows)

def search_type_kb(uid: int = 0):
    last_type = None
    if uid:
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT last_search_type FROM users WHERE user_id=%s", (uid,))
                    row = cur.fetchone()
                    last_type = row[0] if row and row[0] else None
        except Exception:
            pass

    type_buttons = {
        "url":      InlineKeyboardButton("🌐 URL",      callback_data="st_url"),
        "domain":   InlineKeyboardButton("🌍 Domain",   callback_data="st_domain"),
        "login":    InlineKeyboardButton("👤 Login",    callback_data="st_login"),
        "username": InlineKeyboardButton("📝 Username", callback_data="st_username"),
        "email":    InlineKeyboardButton("📧 Email",    callback_data="st_email"),
        "phone":    InlineKeyboardButton("📱 Phone",    callback_data="st_phone"),
        "password": InlineKeyboardButton("🔑 Password", callback_data="st_password"),
    }
    rows = []
    if last_type and last_type in type_buttons:
        rows.append([InlineKeyboardButton(f"⭐ Last: {last_type.upper()}", callback_data=f"st_{last_type}")])
    rows += [
        [type_buttons["url"], type_buttons["domain"]],
        [type_buttons["login"], type_buttons["username"]],
        [type_buttons["email"], type_buttons["phone"]],
        [type_buttons["password"]],
        [InlineKeyboardButton("🔎 Full Scan 👑", callback_data="st_all")],
        [InlineKeyboardButton("🔙 Back",         callback_data="user_home")],
    ]
    return InlineKeyboardMarkup(rows)

def nameid_type_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 بحث بالاسم",        callback_data="ni_name")],
        [InlineKeyboardButton("🪪 بحث بالرقم القومي", callback_data="ni_national_id")],
        [InlineKeyboardButton("🔙 Back",              callback_data="user_home")],
    ])

def back_user_kb(uid: int = 0):
    label = "🔙 القائمة الرئيسية" if get_lang(uid) == "ar" else "🔙 Main Menu"
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data="user_home")]])

def new_search_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 New Search",     callback_data="go_search")],
        [InlineKeyboardButton("🪪 Name/ID Search", callback_data="go_nameid")],
        [InlineKeyboardButton("🏠 Main Menu",      callback_data="user_home")],
    ])

def result_share_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 New Search",        callback_data="go_search")],
        [InlineKeyboardButton("🏠 Main Menu",          callback_data="user_home")],
    ])

def admin_main_kb():
    maint_label = "🔧 Maintenance: ON ✅" if MAINTENANCE_MODE else "🔧 Maintenance: OFF"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔍 Search DB",      callback_data="go_search"),
            InlineKeyboardButton("🪪 Name/ID Search", callback_data="go_nameid"),
        ],
        [
            InlineKeyboardButton("📂 Upload Data",  callback_data="adm_upload_info"),
            InlineKeyboardButton("👥 Users",        callback_data="adm_users"),
        ],
        [
            InlineKeyboardButton("➕ Add User",     callback_data="adm_adduser_inline"),
            InlineKeyboardButton("🗑️ Del User",     callback_data="adm_deluser"),
        ],
        [
            InlineKeyboardButton("📊 Basic Stats",    callback_data="adm_stats"),
            InlineKeyboardButton("📈 Advanced Stats", callback_data="adm_advanced_stats"),
        ],
        [
            InlineKeyboardButton("💰 Add Credits",  callback_data="adm_add_credits"),
            InlineKeyboardButton("⬆️ Set Tier",     callback_data="adm_set_tier"),
        ],
        [
            InlineKeyboardButton("🔒 Ban User",     callback_data="adm_ban"),
            InlineKeyboardButton("✅ Unban User",   callback_data="adm_unban"),
        ],
        [InlineKeyboardButton("🧊 Freeze User",     callback_data="adm_freeze")],
        [
            InlineKeyboardButton("📜 Search Logs",  callback_data="adm_logs"),
            InlineKeyboardButton("🗂️ Files",         callback_data="adm_filelist"),
        ],
        [
            InlineKeyboardButton("🗑️ Delete File",   callback_data="adm_delete_file"),
            InlineKeyboardButton("🔄 Reset Daily",   callback_data="adm_reset_daily"),
        ],
        [InlineKeyboardButton("📢 Broadcast",        callback_data="adm_broadcast")],
        [InlineKeyboardButton("✉️ Message User",      callback_data="adm_msg_user")],
        [InlineKeyboardButton("📅 Set Expiry",        callback_data="adm_set_expiry")],
        [InlineKeyboardButton("🔎 Filter Logs",       callback_data="adm_filter_logs")],
        [
            InlineKeyboardButton("💾 Backup DB",         callback_data="adm_backup"),
            InlineKeyboardButton("📤 Export Users CSV",  callback_data="adm_export_csv"),
        ],
        [InlineKeyboardButton("📋 طلبات الاشتراك",   callback_data="adm_sub_requests")],
        [InlineKeyboardButton("📜 سجل عمليات الأدمن", callback_data="adm_op_logs")],
        [InlineKeyboardButton("⚡ Bot Status",        callback_data="adm_bot_status")],
        [InlineKeyboardButton(maint_label,            callback_data="adm_toggle_maintenance")],
    ])

def back_admin_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="adm_home")]])

# ════════════════════════════════════════════
#                 /START
# ════════════════════════════════════════════
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if MAINTENANCE_MODE and not is_admin(user.id):
        await update.message.reply_text(MAINTENANCE_MSG, parse_mode="Markdown")
        return

    is_new = get_user(user.id) is None

    if WHITELIST_MODE and user.id not in WHITELIST_IDS and not is_admin(user.id):
        await update.message.reply_text("🔒 This bot is in private mode. Access restricted.")
        return

    ensure_user(user.id, user.username or "", user.first_name or "")

    if is_new:
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=(
                        f"🆕 *New User Registered!*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🆔 ID       : `{user.id}`\n"
                        f"👤 Name     : {esc(user.first_name or 'N/A')}\n"
                        f"🔖 Username : @{esc(user.username or 'N/A')}"
                    ),
                    parse_mode="Markdown"
                )
            except Exception:
                pass

    if is_new and context.args:
        try:
            ref_uid = int(context.args[0])
            process_referral(user.id, ref_uid)
        except (ValueError, TypeError):
            pass

    if is_banned(user.id):
        await update.message.reply_text(
            "🚫 *Your account has been banned.*\n\nIf you believe this is a mistake, please contact the admin.",
            parse_mode="Markdown"
        )
        return

    if is_admin(user.id):
        await show_admin_home(update, context, send=True)
        return

    if is_new:
        name = esc(user.first_name or "there")
        await update.message.reply_text(
            f"👋 Welcome, <b>{name}</b>!\n\n"
            f"🤖 <b>DATA SCANNER YUTO BOT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔍 Search millions of database records\n"
            f"🪪 Name & National ID lookup\n"
            f"💎 Multiple subscription tiers\n\n"
            f"🌐 You can switch language anytime from the menu.\n\n"
            f"Press the button below to get started! 👇",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Get Started", callback_data="user_home")],
                [InlineKeyboardButton("🌐 العربية", callback_data="lang_ar")],
            ])
        )
        return

    await show_user_home(update, context, send=True)

def esc(text: str) -> str:
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))

def mesc(text: str) -> str:
    return (str(text)
            .replace("\\", "\\\\")
            .replace("*", "\\*")
            .replace("_", "\\_")
            .replace("`", "\\`")
            .replace("[", "\\["))

# ── DB Count Cache (avoids 9s COUNT(*) on 115M rows) ─────
_count_cache = {"data": 0, "nameid": 0, "users": 0, "files": 0, "searches": 0, "banned": 0, "ts": 0}

def get_cached_counts():
    """Cache COUNT queries for 5 minutes — COUNT(*) on 115M rows takes ~9s."""
    now = time.monotonic()
    if now - _count_cache["ts"] < 300:
        return _count_cache.copy()
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM data_index")
                _count_cache["data"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM name_id_index")
                _count_cache["nameid"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users")
                _count_cache["users"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM uploaded_files")
                _count_cache["files"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM search_logs")
                _count_cache["searches"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users WHERE is_banned=1")
                _count_cache["banned"] = cur.fetchone()[0]
        _count_cache["ts"] = now
    except Exception as e:
        log.error(f"get_cached_counts error: {e}")
    return _count_cache.copy()

async def show_user_home(update, context, send=False, query=None):
    uid = query.from_user.id if query else update.effective_user.id
    u   = get_user_cached(uid)
    tier_key   = u[3]  if u else "free"
    daily_left = u[4]  if u else 5
    credits    = u[5]  if u else 0
    full_name  = u[2]  if u else "User"
    expires_at = u[7]  if u else None
    t          = TIERS.get(tier_key, TIERS["free"])

    try:
        counts = get_cached_counts()
        nameid_count   = counts["nameid"]
        db_count       = counts["data"]
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM search_logs WHERE user_id=%s", (uid,))
                total_searches = cur.fetchone()[0]
    except Exception:
        nameid_count = total_searches = db_count = 0

    lang  = get_lang(uid)
    st    = STRINGS.get(lang, STRINGS["en"])
    is_ar = lang == "ar"
    nameid_left = u[13] if u and len(u) > 13 else 0

    now_utc      = datetime.utcnow()
    next_midnight = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    hrs_left  = int((next_midnight - now_utc).total_seconds() // 3600)
    mins_left = int(((next_midnight - now_utc).total_seconds() % 3600) // 60)
    renew_str = f"{hrs_left}h {mins_left}m" if not is_ar else f"{hrs_left}س {mins_left}د"

    exp_line = ""
    if expires_at:
        try:
            exp_dt   = datetime.fromisoformat(str(expires_at))
            days_rem = (exp_dt - now_utc).days
            exp_line = f"\n📅 {'ينتهي في' if is_ar else 'Expires'}: <code>{str(expires_at)[:10]}</code> ({days_rem}d)"
        except Exception:
            pass

    text = (
        f"🤖 <b>DATA SCANNER YUTO BOT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👋 {st['hello']}, <b>{esc(full_name)}</b>!\n\n"
        f"📦 {st['plan']}: {esc(t['label'])}{exp_line}\n"
        f"🔍 {st['daily_left']}: <code>{daily_left}</code>\n"
        f"🪪 {'Name/ID بحوث' if is_ar else 'Name/ID searches'}: <code>{nameid_left}</code>\n"
        f"⏰ {'يتجدد خلال' if is_ar else 'Renews in'}: <code>{renew_str}</code>\n"
        f"💰 {st['credits']}: <code>{credits}</code>\n"
        f"🔢 {'بحوثي الكلية' if is_ar else 'Total searches'}: <code>{total_searches:,}</code>\n"
        f"🗄️ {st['db_records']}: <code>{db_count:,}</code>\n"
        f"🪪 {st['nameid_records']}: <code>{nameid_count:,}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{st['menu_hint']}"
    )
    if send:
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=user_main_kb(uid))
    else:
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=user_main_kb(uid))

async def show_admin_home(update, context, send=False, query=None):
    try:
        counts = get_cached_counts()
        total_records  = counts["data"]
        total_nameid   = counts["nameid"]
        total_users    = counts["users"]
        total_files    = counts["files"]
        total_searches = counts["searches"]
        banned_count   = counts["banned"]
    except Exception as e:
        log.error(f"show_admin_home error: {e}")
        total_records = total_nameid = total_users = total_files = total_searches = banned_count = 0

    text = (
        f"⚙️ *ADMIN CONTROL PANEL*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🗄️ DB Records    : `{total_records:,}`\n"
        f"🪪 Name/ID Rows  : `{total_nameid:,}`\n"
        f"👥 Total Users   : `{total_users:,}`\n"
        f"🚫 Banned Users  : `{banned_count}`\n"
        f"📁 Indexed Files : `{total_files}`\n"
        f"🔍 Total Searches: `{total_searches:,}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📂 Send any TXT/CSV/XLSX/JSON file here to index it.\n"
        f"📋 Excel files with name + national ID columns are *also* indexed in the Name/ID table automatically."
    )
    if send:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=admin_main_kb())
    else:
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=admin_main_kb())

# ── Callback rate limiting ────────────────────────────────
_last_callback: dict = {}
_CALLBACK_COOLDOWN = 1.0

# ════════════════════════════════════════════
#            CALLBACK QUERY ROUTER
# ════════════════════════════════════════════
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = q.from_user.id
    data = _cb_get(q.data)

    now_t = time.monotonic()
    with _rate_limit_lock:
        last_cb = _last_callback.get(uid, 0)
        if not is_admin(uid) and (now_t - last_cb) < _CALLBACK_COOLDOWN:
            await q.answer("⏳ Too fast! Please slow down.", show_alert=False)
            return
        _last_callback[uid] = now_t

    await q.answer()
    ensure_user(uid, q.from_user.username or "", q.from_user.first_name or "")

    if is_banned(uid) and not is_admin(uid):
        await q.edit_message_text(
            "🚫 *Your account has been banned.*\n\nContact the admin for support.",
            parse_mode="Markdown"
        )
        return

    if data == "user_home":
        await show_user_home(update, context, query=q)
        return

    if data == "set_language":
        await q.edit_message_text(
            "🌐 *Choose your language / اختر لغتك:*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en"),
                 InlineKeyboardButton("🇸🇦 العربية", callback_data="lang_ar")],
                [InlineKeyboardButton("🔙 Back / رجوع", callback_data="user_home")],
            ])
        )
        return

    if data in ("lang_en", "lang_ar"):
        chosen = data.split("_")[1]
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET lang=%s WHERE user_id=%s", (chosen, uid))
                conn.commit()
        except Exception as e:
            log.error(f"lang update error: {e}")
        confirm = STRINGS[chosen]["lang_set"]
        await q.edit_message_text(
            confirm, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                "🔙 Main Menu" if chosen == "en" else "🔙 القائمة الرئيسية",
                callback_data="user_home"
            )]])
        )
        return

    if data == "user_subscribe":
        st = STRINGS.get(get_lang(uid), STRINGS["en"])
        tiers_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⭐ Basic",   callback_data="sub_req_basic"),
             InlineKeyboardButton("💎 Premium", callback_data="sub_req_premium")],
            [InlineKeyboardButton("👑 VIP",     callback_data="sub_req_vip")],
            [InlineKeyboardButton("🔙 Back / رجوع", callback_data="user_home")],
        ])
        await q.edit_message_text(
            f"{st['sub_req_title']}\n\n{st['sub_req_prompt']}",
            parse_mode="Markdown", reply_markup=tiers_kb
        )
        return

    if data.startswith("sub_req_"):
        tier = data.replace("sub_req_", "")
        if tier not in TIERS or tier == "free":
            await q.answer("❌ Invalid tier", show_alert=True)
            return
        user_obj = q.from_user

        async def reply_fn(text, parse_mode=None):
            await q.edit_message_text(text, parse_mode=parse_mode, reply_markup=back_user_kb(uid))

        await _do_subscribe_request(
            uid, user_obj.username or "", user_obj.first_name or "User",
            tier, context, reply_fn
        )
        return

    if data == "adm_home":
        if not is_admin(uid):
            return
        await show_admin_home(update, context, query=q)
        return

    if data == "my_account":
        u = get_user_cached(uid)
        if not u:
            await q.edit_message_text("❌ Account not found. Send /start first.", reply_markup=back_user_kb(uid))
            return
        lang  = get_lang(uid)
        is_ar = lang == "ar"
        tier_d   = TIERS.get(u[3], TIERS["free"])
        username = esc(u[1] or "N/A")
        fullname = esc(u[2] or "User")
        daily    = u[4]
        credits  = u[5]
        exp      = esc(str(u[7]) if u[7] else ("لا يوجد انتهاء" if is_ar else "No expiry"))
        tier_name = u[3]

        max_daily = TIERS.get(tier_name, TIERS["free"])["daily"]
        used      = max(0, max_daily - daily) if max_daily > 0 else 0
        if max_daily > 0 and max_daily < 100_000:
            pct   = min(100, int(used / max_daily * 100))
            bars  = int(pct / 10)
            bar   = "█" * bars + "░" * (10 - bars)
            daily_bar = f"`{bar}` {pct}% ({used}/{max_daily})"
        else:
            daily_bar = f"`{daily}` {'متبقي' if is_ar else 'remaining'}"

        max_ni  = NAMEID_TIERS.get(tier_name, NAMEID_TIERS["free"])["daily_nameid"]
        ni_left = u[13] if len(u) > 13 else 0
        ni_used = max(0, max_ni - ni_left) if max_ni > 0 else 0
        if max_ni > 0 and max_ni < 100_000:
            ni_pct  = min(100, int(ni_used / max_ni * 100))
            ni_bars = int(ni_pct / 10)
            ni_bar  = f"`{'█'*ni_bars}{'░'*(10-ni_bars)}` {ni_pct}% ({ni_used}/{max_ni})"
        else:
            ni_bar  = f"`{ni_left}` {'متبقي' if is_ar else 'remaining'}"

        if is_ar:
            text = (
                f"📊 <b>حسابي</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 الاسم    : <b>{fullname}</b>\n"
                f"🔖 المعرف   : @{username}\n"
                f"🆔 المعرف الرقمي: <code>{uid}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 الباقة   : {esc(tier_d['label'])}\n"
                f"🔍 البحث اليومي:\n  {daily_bar}\n"
                f"🪪 Name/ID اليومي:\n  {ni_bar}\n"
                f"💰 الرصيد   : <code>{credits}</code>\n"
                f"📅 الانتهاء : <code>{exp}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            )
        else:
            text = (
                f"📊 <b>My Account</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 Name    : <b>{fullname}</b>\n"
                f"🔖 Username: @{username}\n"
                f"🆔 User ID : <code>{uid}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 Plan    : {esc(tier_d['label'])}\n"
                f"🔍 Daily Search:\n  {daily_bar}\n"
                f"🪪 Daily Name/ID:\n  {ni_bar}\n"
                f"💰 Credits : <code>{credits}</code>\n"
                f"📅 Expires : <code>{exp}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            )
        await q.edit_message_text(text, parse_mode="HTML", reply_markup=back_user_kb(uid))
        return

    if data == "go_nameid":
        if not can_search_nameid(uid):
            is_ar = get_lang(uid) == "ar"
            msg_no = (
                "❌ *انتهت بحوث Name/ID لليوم.*\n\nقم بترقية باقتك للحصول على المزيد."
                if is_ar else
                "❌ *Name/ID searches used up for today.*\n\nUpgrade your plan for more."
            )
            await q.edit_message_text(
                msg_no, parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💳 View Plans", callback_data="show_plans")],
                    [InlineKeyboardButton("🔙 Back",       callback_data="user_home")],
                ])
            )
            return
        await q.edit_message_text(
            "🪪 *Name / National ID Search*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "اختار نوع البحث:",
            parse_mode="Markdown",
            reply_markup=nameid_type_kb()
        )
        return

    if data in ("ni_name", "ni_national_id"):
        if not can_search(uid):
            await q.edit_message_text("❌ No searches remaining.", reply_markup=back_user_kb(uid))
            return
        context.user_data["search_type"] = data
        if data == "ni_name":
            prompt = "👤 *بحث بالاسم*\n\n✏️ ابعت الاسم أو جزء منه:\n_مثال: عبد الفتاح السيسي_"
        else:
            prompt = "🪪 *بحث بالرقم القومي*\n\n✏️ ابعت الرقم القومي (14 رقم) أو جزء منه:\n_مثال: 3060_"
        await q.edit_message_text(
            prompt, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="go_nameid")]])
        )
        return

    if data == "go_search":
        await q.edit_message_text(
            "🔍 *Choose Search Type*\n\nSelect the type of data you want to search for:",
            parse_mode="Markdown", reply_markup=search_type_kb(uid)
        )
        return

    if data.startswith("st_"):
        stype = data[3:]
        if stype == "all":
            tier = get_tier(uid)
            if tier not in ("premium", "vip") and not is_admin(uid):
                await q.edit_message_text(
                    "👑 *Full Scan* is exclusive to *Premium* and *VIP* members.\n\nUpgrade to unlock!",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💳 View Plans", callback_data="show_plans")],
                        [InlineKeyboardButton("🔙 Back",       callback_data="go_search")],
                    ])
                )
                return
        if not can_search(uid):
            await q.edit_message_text(
                "❌ *No searches remaining.*\n\nUpgrade your plan or buy credits.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💳 View Plans", callback_data="show_plans")],
                    [InlineKeyboardButton("🔙 Back",       callback_data="user_home")],
                ])
            )
            return
        context.user_data["search_type"] = stype
        icons = {"url":"🌐","domain":"🌍","login":"👤","username":"📝",
                 "email":"📧","phone":"📱","password":"🔑","all":"🔎"}
        icon = icons.get(stype, "🔍")

        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT DISTINCT keyword FROM search_logs WHERE user_id=%s "
                        "ORDER BY timestamp DESC LIMIT 5",
                        (uid,)
                    )
                    recent = cur.fetchall()
        except Exception:
            recent = []

        kb_rows = []
        if recent:
            for (kw,) in recent:
                safe_kw = kw[:35]
                kb_rows.append([InlineKeyboardButton(
                    f"🕐 {safe_kw}", callback_data=_cb_put(f"confirm_search:{stype}:{safe_kw}")
                )])
        kb_rows.append([InlineKeyboardButton("🔙 Cancel", callback_data="go_search")])

        hint = "\n\n🕐 *Or pick a recent keyword:*" if recent else ""
        await q.edit_message_text(
            f"{icon} *{stype.upper()} Search*\n\n"
            f"✏️ Send your target keyword now:\n\n"
            f"⏱️ Search runs for up to *3 minutes* to find best results.{hint}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb_rows)
        )
        return

    if data == "show_plans":
        is_ar = get_lang(uid) == "ar"
        if is_ar:
            text = (
                f"💳 *باقات الاشتراك*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🆓 *Free* • 0 بحث DB/يوم • 0 بحث Name/ID\n\n"
                f"⭐ *Basic*\n"
                f"  🔍 Search DB: 10 بحث/يوم ← 200 نتيجة\n"
                f"  🪪 Name/ID: 5 بحوث/يوم ← 50 نتيجة\n\n"
                f"💎 *Premium*\n"
                f"  🔍 Search DB: 15 بحث/يوم ← 1,000 نتيجة + Full Scan ✅\n"
                f"  🪪 Name/ID: 10 بحوث/يوم ← 500 نتيجة\n\n"
                f"👑 *VIP*\n"
                f"  🔍 Search DB: غير محدود ← 1,000,000 نتيجة + Full Scan ✅\n"
                f"  🪪 Name/ID: غير محدود ← 100,000 نتيجة\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📩 تواصل مع @yut3ev للترقية."
            )
        else:
            text = (
                f"💳 *Subscription Plans*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🆓 *Free* • 0 DB searches/day • 0 Name/ID searches\n\n"
                f"⭐ *Basic*\n"
                f"  🔍 Search DB: 10/day → up to 200 results\n"
                f"  🪪 Name/ID: 5/day → up to 50 results\n\n"
                f"💎 *Premium*\n"
                f"  🔍 Search DB: 15/day → up to 1,000 results + Full Scan ✅\n"
                f"  🪪 Name/ID: 10/day → up to 500 results\n\n"
                f"👑 *VIP*\n"
                f"  🔍 Search DB: Unlimited → up to 1,000,000 results + Full Scan ✅\n"
                f"  🪪 Name/ID: Unlimited → up to 100,000 results\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📩 Contact @yut3ev to upgrade."
            )
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 طلب اشتراك / Subscribe" if is_ar else "📋 Subscribe Request", callback_data="user_subscribe")],
            [InlineKeyboardButton("🔙 القائمة / Menu", callback_data="user_home")],
        ]))
        return

    if data == "show_help":
        is_ar = get_lang(uid) == "ar"
        u     = get_user_cached(uid)
        tier_key = u[3] if u else "free"
        t  = TIERS.get(tier_key, TIERS["free"])
        nt = NAMEID_TIERS.get(tier_key, NAMEID_TIERS["free"])
        daily_left  = u[4]  if u else 0
        nameid_left = u[13] if u and len(u) > 13 else 0
        plan_info = (
            f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 {'باقتك الحالية' if is_ar else 'Your Current Plan'}: *{esc(t['label'])}*\n"
            f"🔍 {'البحث العادي' if is_ar else 'DB Search'}: `{daily_left}` {'متبقي' if is_ar else 'left today'} / `{t['daily']}` {'يومياً' if is_ar else '/day'}\n"
            f"🪪 Name/ID: `{nameid_left}` {'متبقي' if is_ar else 'left today'} / `{nt['daily_nameid']}` {'يومياً' if is_ar else '/day'}"
        )
        if is_ar:
            text = (
                f"ℹ️ *كيفية استخدام بوت DATA SCANNER*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"*🔍 البحث في قاعدة البيانات:*\n"
                f"1️⃣ اضغط *بحث في قاعدة البيانات*\n"
                f"2️⃣ اختر النوع: URL / دومين / إيميل / هاتف / إلخ\n"
                f"3️⃣ ابعت الكلمة المفتاحية ← البوت يُظهر العدد أولاً\n"
                f"4️⃣ أكّد لتحميل ملف `.txt` نظيف\n\n"
                f"*🪪 بحث بالاسم / الرقم القومي:*\n"
                f"1️⃣ اضغط *بحث بالاسم / الرقم القومي*\n"
                f"2️⃣ اختر *بحث بالاسم* أو *بحث بالرقم القومي*\n"
                f"3️⃣ ابعت الاسم أو الرقم القومي (14 رقماً)\n"
                f"4️⃣ البوت يُظهر العدد ← أكّد للتحميل\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ *ملاحظة:* الرقم القومي = 14 رقماً (مش رقم تليفون)"
                f"{plan_info}"
            )
        else:
            text = (
                f"ℹ️ *How to Use Data Scanner Bot*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"*🔍 Database Search:*\n"
                f"1️⃣ Tap *Search Database*\n"
                f"2️⃣ Choose type: URL / Domain / Email / Phone / etc.\n"
                f"3️⃣ Send keyword → bot shows result *count* first\n"
                f"4️⃣ Confirm to download clean `.txt` file\n\n"
                f"*🪪 Name / National ID Search:*\n"
                f"1️⃣ Tap *Name / National ID Search*\n"
                f"2️⃣ Choose *Search by Name* or *Search by National ID*\n"
                f"3️⃣ Send name or 14-digit national ID\n"
                f"4️⃣ Bot shows count → confirm to download\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ *Note:* National ID = 14 digits (not a phone number)"
                f"{plan_info}"
            )
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=back_user_kb(uid))
        return

    if data.startswith("confirm_search:"):
        parts_cb = data.split(":", 2)
        if len(parts_cb) < 3:
            await q.answer("❌ Invalid callback.", show_alert=True)
            return
        _, stype, keyword = parts_cb
        context.user_data["search_type"]  = stype
        context.user_data["confirmed_kw"] = keyword
        await q.edit_message_text(
            f"⏳ Starting scan for `{mesc(keyword)}`...",
            parse_mode="Markdown"
        )
        await do_search(update, context, keyword, stype, reply_to=q.message)
        return

    if data.startswith("confirm_nameid:"):
        parts_cb = data.split(":", 2)
        if len(parts_cb) < 3:
            await q.answer("❌ Invalid callback.", show_alert=True)
            return
        _, stype, keyword = parts_cb
        context.user_data["search_type"] = stype
        await q.edit_message_text("⏳ جاري تحضير الملف...", parse_mode="Markdown")
        await do_nameid_search(update, context, keyword, stype, reply_to=q.message)
        return

    if data.startswith("cancel_search"):
        await q.edit_message_text("❌ Search cancelled.", reply_markup=new_search_kb())
        return

    # ════ ADMIN CALLBACKS ════
    if not is_admin(uid):
        return

    if data == "adm_stats":
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM data_index")
                    tr = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM name_id_index")
                    tn = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM users")
                    tu = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM users WHERE is_banned=1")
                    tb = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM search_logs")
                    ts = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM uploaded_files")
                    tf = cur.fetchone()[0]
                    cur.execute("SELECT tier, COUNT(*) FROM users GROUP BY tier")
                    tc = cur.fetchall()
                    cur.execute(
                        "SELECT COALESCE(SUM(size_bytes),0), COALESCE(MAX(size_bytes),0), "
                        "COALESCE(AVG(records),0), COALESCE(MAX(records),0) FROM uploaded_files"
                    )
                    up_stats = cur.fetchone()
        except Exception as e:
            log.error(f"adm_stats error: {e}")
            await q.edit_message_text("❌ Stats error.", reply_markup=back_admin_kb())
            return

        total_sz, max_sz, avg_recs, max_recs = up_stats
        tier_lines = "\n".join([f"  {t}: `{c}`" for t, c in tc])
        await q.edit_message_text(
            f"📊 *Bot Statistics*\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Users: `{tu:,}` | 🚫 Banned: `{tb}`\n"
            f"🗄️ DB Records: `{tr:,}` | 🪪 Name/ID: `{tn:,}`\n"
            f"📁 Files: `{tf}` | 🔍 Searches: `{ts:,}`\n\n"
            f"📦 *Tier Breakdown:*\n{tier_lines}\n\n"
            f"📂 *Upload Stats:*\n"
            f"  💾 Total size: `{round((total_sz or 0)/1024/1024, 1)} MB`\n"
            f"  📄 Largest file: `{round((max_sz or 0)/1024/1024, 2)} MB`\n"
            f"  📊 Avg records/file: `{int(avg_recs or 0):,}`\n"
            f"  🏆 Max records: `{int(max_recs or 0):,}`",
            parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        return

    if data == "adm_users" or data.startswith("adm_users_p") or data.startswith("adm_users_f"):
        page = 0
        tier_filter = None
        if data.startswith("adm_users_p"):
            try:
                page = int(data.split("_p")[1].split("_f")[0])
            except Exception:
                page = 0
        if "_f" in data:
            tier_filter = data.split("_f")[-1] or None
        elif data.startswith("adm_users_f"):
            tier_filter = data.replace("adm_users_f", "") or None

        per_page = 10
        offset   = page * per_page

        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    if tier_filter:
                        cur.execute("SELECT COUNT(*) FROM users WHERE tier=%s", (tier_filter,))
                    else:
                        cur.execute("SELECT COUNT(*) FROM users")
                    total_u = cur.fetchone()[0]

                    if tier_filter:
                        cur.execute(
                            "SELECT user_id, username, full_name, tier, daily_limit, credits, is_banned, last_search_at "
                            "FROM users WHERE tier=%s ORDER BY user_id DESC LIMIT %s OFFSET %s",
                            (tier_filter, per_page, offset)
                        )
                    else:
                        cur.execute(
                            "SELECT user_id, username, full_name, tier, daily_limit, credits, is_banned, last_search_at "
                            "FROM users ORDER BY user_id DESC LIMIT %s OFFSET %s",
                            (per_page, offset)
                        )
                    rows = cur.fetchall()
        except Exception as e:
            log.error(f"adm_users error: {e}")
            await q.edit_message_text("❌ Error loading users.", reply_markup=back_admin_kb())
            return

        filter_label = f" [{tier_filter}]" if tier_filter else ""
        text = f"👥 <b>Users{filter_label} — Page {page+1}</b> ({total_u} total)\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        kb_rows = []
        for r in rows:
            uid_r, uname, fname, tier_r, dlimit, credits_r, banned, last_s = r
            icon = "🚫" if banned else "✅"
            last = (str(last_s) or "")[:10] if last_s else "never"
            text += (
                f"{icon} <code>{uid_r}</code> <b>{esc(tier_r)}</b> @{esc(uname or 'N/A')} "
                f"<i>{esc((fname or '')[:15])}</i> cr:{credits_r} last:{last}\n"
            )
            ban_lbl = "✅ Unban" if banned else "🔒 Ban"
            ban_cb  = f"quick_unban:{uid_r}" if banned else f"quick_ban:{uid_r}"
            kb_rows.append([
                InlineKeyboardButton(f"👤 {uid_r}", callback_data=f"quick_info:{uid_r}"),
                InlineKeyboardButton(ban_lbl,       callback_data=ban_cb),
            ])

        base_f = "adm_users_p0_f"
        filter_row = [
            InlineKeyboardButton("All",  callback_data="adm_users"),
            InlineKeyboardButton("⭐",   callback_data=f"{base_f}basic"),
            InlineKeyboardButton("💎",   callback_data=f"{base_f}premium"),
            InlineKeyboardButton("👑",   callback_data=f"{base_f}vip"),
        ]
        kb_rows.append(filter_row)
        nav_buttons = []
        f_suffix = f"_f{tier_filter}" if tier_filter else ""
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"adm_users_p{page-1}{f_suffix}"))
        if offset + per_page < total_u:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"adm_users_p{page+1}{f_suffix}"))
        if nav_buttons:
            kb_rows.append(nav_buttons)
        kb_rows.append([InlineKeyboardButton("🔙 Admin Panel", callback_data="adm_home")])
        await q.edit_message_text(text[:4000], parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb_rows))
        return

    if data.startswith("quick_info:"):
        target = int(data.split(":")[1])
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM users WHERE user_id=%s", (target,))
                    row = cur.fetchone()
        except Exception:
            row = None
        if not row:
            await q.answer("User not found.", show_alert=True)
            return
        uid_r, uname, fname, tier_r, daily, credits_r, banned, expires, joined, lang_r, ref_by, ref_cnt, last_s, nameid_lim = row[:14]
        status = "🚫 Banned" if banned else "✅ Active"
        await q.answer(
            f"ID:{uid_r} | {tier_r} | cr:{credits_r}\n"
            f"daily:{daily} | nameid:{nameid_lim}\n"
            f"exp:{str(expires or 'none')[:10]} | {status}",
            show_alert=True
        )
        return

    if data.startswith("quick_ban:") or data.startswith("quick_unban:"):
        action_q, target_s = data.split(":", 1)
        target  = int(target_s)
        new_ban = 1 if action_q == "quick_ban" else 0
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_banned=%s WHERE user_id=%s", (new_ban, target))
                conn.commit()
        except Exception as e:
            log.error(f"quick_ban error: {e}")
        log_admin_op(uid, "quick_ban" if new_ban else "quick_unban", str(target))
        action_label = "🚫 Banned" if new_ban else "✅ Unbanned"
        await q.answer(f"{action_label} user {target}", show_alert=False)
        data = "adm_users"

    if data == "adm_logs" or data.startswith("adm_logs_p"):
        page = 0
        if data.startswith("adm_logs_p"):
            try:
                page = int(data.split("_p")[1])
            except Exception:
                page = 0
        per_page = 20
        offset   = page * per_page
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM search_logs")
                    total_logs = cur.fetchone()[0]
                    cur.execute(
                        "SELECT user_id, keyword, category, results, timestamp "
                        "FROM search_logs ORDER BY timestamp DESC LIMIT %s OFFSET %s",
                        (per_page, offset)
                    )
                    rows = cur.fetchall()
        except Exception as e:
            log.error(f"adm_logs error: {e}")
            rows = []
            total_logs = 0

        text = f"📜 <b>Search Logs — Page {page+1}</b> ({total_logs:,} total)\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for r in rows:
            cat_icon = {"email":"📧","phone":"📱","url":"🌐","nameid_name":"🪪"}.get(r[2], "🔍")
            text += f"{cat_icon} <code>{r[0]}</code> <code>{esc(r[1][:20])}</code> [{esc(r[2])}] — {r[3]}r @ {str(r[4])[:16]}\n"
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"adm_logs_p{page-1}"))
        if offset + per_page < total_logs:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"adm_logs_p{page+1}"))
        kb_rows = [nav_buttons] if nav_buttons else []
        kb_rows.append([InlineKeyboardButton("🔙 Admin Panel", callback_data="adm_home")])
        await q.edit_message_text(text[:4000], parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb_rows))
        return

    if data == "adm_filelist" or data.startswith("adm_filelist_p"):
        page = 0
        if data.startswith("adm_filelist_p"):
            try:
                page = int(data.split("_p")[1])
            except Exception:
                page = 0
        per_page = 15
        offset   = page * per_page
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM uploaded_files")
                    total_files = cur.fetchone()[0]
                    cur.execute(
                        "SELECT id, original_name, records, size_bytes, uploaded_at "
                        "FROM uploaded_files ORDER BY uploaded_at DESC LIMIT %s OFFSET %s",
                        (per_page, offset)
                    )
                    rows = cur.fetchall()
        except Exception as e:
            log.error(f"adm_filelist error: {e}")
            rows = []
            total_files = 0

        if not rows:
            text = "📁 No files uploaded yet."
        else:
            text = f"🗂️ *Uploaded Files — Page {page+1}* ({total_files} total)\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for fid, fname, recs, sz, ts in rows:
                kb = round((sz or 0) / 1024, 1)
                text += f"`#{fid}` 📄 `{mesc(fname)}`\n    {(recs or 0):,} recs | {kb} KB | {str(ts)[:16]}\n\n"

        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"adm_filelist_p{page-1}"))
        if offset + per_page < total_files:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"adm_filelist_p{page+1}"))
        kb_rows = [nav_buttons] if nav_buttons else []
        kb_rows.append([InlineKeyboardButton("🔙 Admin Panel", callback_data="adm_home")])
        await q.edit_message_text(text[:4000], parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb_rows))
        return

    if data == "adm_upload_info":
        await q.edit_message_text(
            f"📂 *Upload Data to Database*\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Supported Formats:*\n"
            f"  • `.txt` — one entry per line\n"
            f"  • `.csv` — all columns indexed\n"
            f"  • `.xlsx/.xls` — all cells indexed\n"
            f"  • `.json` — all string values indexed\n\n"
            f"🪪 *Excel with Name + National ID columns:*\n"
            f"  Automatically also indexed in Name/ID table.\n"
            f"  Columns are auto-detected by header keywords.\n\n"
            f"📌 *Small files (under 20 MB):* Send directly here.\n"
            f"📦 *Large files:* Use `uploader.py` script.",
            parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        return

    if data == "adm_reset_daily":
        do_daily_reset()
        await q.edit_message_text("✅ Daily limits reset for all users (Search DB + Name/ID).", reply_markup=back_admin_kb())
        return

    if data == "adm_advanced_stats":
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT user_id, COUNT(*) as cnt FROM search_logs GROUP BY user_id ORDER BY cnt DESC LIMIT 5"
                    )
                    top_searchers = cur.fetchall()
                    today = datetime.utcnow().strftime("%Y-%m-%d")
                    cur.execute(
                        "SELECT COUNT(*) FROM search_logs WHERE timestamp LIKE %s", (f"{today}%",)
                    )
                    today_searches = cur.fetchone()[0]
                    cur.execute(
                        "SELECT keyword, COUNT(*) as cnt FROM search_logs GROUP BY keyword ORDER BY cnt DESC LIMIT 5"
                    )
                    top_keywords = cur.fetchall()
                    cur.execute(
                        "SELECT COUNT(*) FROM users WHERE joined_at LIKE %s", (f"{today}%",)
                    )
                    new_users_today = cur.fetchone()[0]
                    cur.execute(
                        "SELECT category, COUNT(*) FROM search_logs GROUP BY category ORDER BY COUNT(*) DESC LIMIT 5"
                    )
                    by_type = cur.fetchall()
                    cur.execute(
                        "SELECT SUBSTRING(timestamp, 12, 2) as hr, COUNT(*) as cnt "
                        "FROM search_logs GROUP BY hr ORDER BY cnt DESC LIMIT 3"
                    )
                    peak_hours = cur.fetchall()
                    cur.execute(
                        "SELECT user_id, referral_count FROM users WHERE referral_count > 0 "
                        "ORDER BY referral_count DESC LIMIT 5"
                    )
                    top_referrers = cur.fetchall()
                    cur.execute("SELECT COALESCE(SUM(referral_count), 0) FROM users")
                    total_referrals = cur.fetchone()[0]
                    cur.execute(
                        "SELECT SUBSTRING(timestamp, 1, 10) as day, COUNT(*) "
                        "FROM search_logs WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days' "
                        "GROUP BY day ORDER BY day DESC"
                    )
                    week_trend = cur.fetchall()
        except Exception as e:
            log.error(f"adm_advanced_stats error: {e}")
            await q.edit_message_text("❌ Stats error.", reply_markup=back_admin_kb())
            return

        top_s_lines   = "\n".join([f"  `{r[0]}` — {r[1]}x" for r in top_searchers]) or "  No data"
        top_k_lines   = "\n".join([f"  `{mesc(r[0][:20])}` — {r[1]}x" for r in top_keywords]) or "  No data"
        by_type_lines = "\n".join([f"  {r[0] or 'N/A'}: `{r[1]}`" for r in by_type]) or "  No data"
        peak_lines    = " | ".join([f"`{r[0]}:00` ({r[1]})" for r in peak_hours]) or "No data"
        ref_lines     = "\n".join([f"  `{r[0]}` — {r[1]} referrals" for r in top_referrers]) or "  No referrals yet"
        trend_lines   = " | ".join([f"`{str(r[0])[5:]}` {r[1]}" for r in week_trend]) or "No data"

        await q.edit_message_text(
            f"📈 *Advanced Statistics*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📅 *Today ({today}):*\n"
            f"  🔍 Searches: `{today_searches}` | 👤 New users: `{new_users_today}`\n\n"
            f"📊 *7-Day Trend:*\n  {trend_lines}\n\n"
            f"⏰ *Peak Hours (UTC):* {peak_lines}\n\n"
            f"🏆 *Top Searchers:*\n{top_s_lines}\n\n"
            f"🔑 *Top Keywords:*\n{top_k_lines}\n\n"
            f"📂 *Searches by Type:*\n{by_type_lines}\n\n"
            f"🔗 *Referrals:* `{total_referrals}` total\n{ref_lines}",
            parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        return

    if data == "adm_broadcast":
        context.user_data["admin_action"] = "broadcast"
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM users WHERE is_banned=0")
                    user_count = cur.fetchone()[0]
        except Exception:
            user_count = 0
        await q.edit_message_text(
            f"📢 *Broadcast Message*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 Will be sent to *{user_count}* active users.\n\n"
            f"✏️ Send your message now:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="adm_home")]])
        )
        return

    if data == "adm_msg_user":
        context.user_data["admin_action"] = "msg_user"
        await q.edit_message_text(
            "✉️ *Message Specific User*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send: `USER_ID Your message`\n"
            "Example: `123456789 Your account is ready!`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="adm_home")]])
        )
        return

    if data == "my_history":
        is_ar = get_lang(uid) == "ar"
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT keyword, category, results, timestamp FROM search_logs "
                        "WHERE user_id=%s ORDER BY timestamp DESC LIMIT 15",
                        (uid,)
                    )
                    rows = cur.fetchall()
                    cur.execute("SELECT COUNT(*) FROM search_logs WHERE user_id=%s", (uid,))
                    total = cur.fetchone()[0]
        except Exception:
            rows = []
            total = 0

        if not rows:
            txt = "📜 *بحوثي الأخيرة*\n\nلا توجد بحوث بعد." if is_ar else "📜 *My Search History*\n\nNo searches yet."
        else:
            title = "📜 *بحوثي الأخيرة*" if is_ar else "📜 *My Search History*"
            txt = f"{title}\n━━━━━━━━━━━━━━━━━━━━━━\n({'إجمالي' if is_ar else 'Total'}: {total})\n\n"
            type_icons = {
                "email":"📧","phone":"📱","url":"🌐","domain":"🌍",
                "login":"👤","username":"📝","password":"🔑","all":"🔎",
                "nameid_name":"🪪","nameid_national_id":"🪪","nameid_partial_id":"🔢",
            }
            for kw, cat, res, ts in rows:
                icon = type_icons.get(cat, "🔍")
                cat_label = cat.replace("nameid_", "").replace("_", " ").upper()
                txt += f"{icon} `{mesc(kw[:22])}` `{cat_label}` — *{res}* | {str(ts)[:10]}\n"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ مسح التاريخ" if is_ar else "🗑️ Clear History", callback_data="clear_history")],
            [InlineKeyboardButton("🔙 القائمة الرئيسية" if is_ar else "🔙 Main Menu", callback_data="user_home")],
        ])
        await q.edit_message_text(txt[:4000], parse_mode="Markdown", reply_markup=kb)
        return

    if data == "clear_history":
        is_ar = get_lang(uid) == "ar"
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM search_logs WHERE user_id=%s", (uid,))
                conn.commit()
        except Exception as e:
            log.error(f"clear_history error: {e}")
        txt = "✅ *تم مسح تاريخ البحث بنجاح.*" if is_ar else "✅ *Search history cleared.*"
        await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=back_user_kb(uid))
        return

    if data == "my_referral":
        is_ar     = get_lang(uid) == "ar"
        ref_count = get_referral_stats(uid)
        bot_username = (await context.bot.get_me()).username
        ref_link  = f"https://t.me/{bot_username}?start={uid}"
        if is_ar:
            txt = (
                f"🔗 *رابط الإحالة الخاص بك*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"شارك هذا الرابط مع أصدقائك:\n`{ref_link}`\n\n"
                f"👥 الأصدقاء الذين دعوتهم: `{ref_count}`\n"
                f"💰 تكسب *{REFERRAL_CREDITS}* رصيد لكل صديق جديد!\n\n"
                f"كل صديق يفتح البوت عبر رابطك تحصل على رصيد تلقائياً ✅"
            )
        else:
            txt = (
                f"🔗 *Your Referral Link*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Share this link with friends:\n`{ref_link}`\n\n"
                f"👥 Friends referred: `{ref_count}`\n"
                f"💰 Earn *{REFERRAL_CREDITS}* credits per new user!\n\n"
                f"Every friend who opens the bot via your link earns you credits ✅"
            )
        await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=back_user_kb(uid))
        return

    if data == "my_id":
        is_ar = get_lang(uid) == "ar"
        txt = (
            f"🆔 *معرفك على تيليجرام:*\n\n`{uid}`\n\n_شارك هذا مع الأدمن لإدارة حسابك_"
            if is_ar else
            f"🆔 *Your Telegram ID:*\n\n`{uid}`\n\n_Share this with the admin to manage your account_"
        )
        await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=back_user_kb(uid))
        return

    if data == "adm_set_expiry":
        context.user_data["admin_action"] = "set_expiry"
        await q.edit_message_text(
            "📅 *Set Subscription Expiry*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send: `USER_ID DAYS`\n"
            "Example: `123456789 30` → sets expiry to 30 days from now\n\n"
            "Send `USER_ID 0` to clear expiry.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="adm_home")]])
        )
        return

    if data == "adm_filter_logs":
        context.user_data["admin_action"] = "filter_logs"
        await q.edit_message_text(
            "🔎 *Filter Search Logs*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send a *User ID* to see their searches:\n`123456789`\n\n"
            "Or send a *keyword* to find searches containing it:\n`gmail.com`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="adm_home")]])
        )
        return

    if data == "adm_export_csv":
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT user_id, username, full_name, tier, daily_limit, credits, is_banned, "
                        "expires_at, joined_at, lang, referral_count FROM users ORDER BY user_id DESC"
                    )
                    rows = cur.fetchall()
        except Exception as e:
            log.error(f"adm_export_csv error: {e}")
            rows = []

        import csv
        import io as _io
        output = _io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["user_id","username","full_name","tier","daily_limit","credits","is_banned","expires_at","joined_at","lang","referral_count"])
        writer.writerows(rows)
        csv_bytes = output.getvalue().encode("utf-8-sig")
        filename  = f"users_export_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.csv"
        await context.bot.send_document(
            chat_id=uid,
            document=_io.BytesIO(csv_bytes),
            filename=filename,
            caption=f"📤 *Users Export*\n{len(rows):,} users exported.",
            parse_mode="Markdown"
        )
        log_admin_op(uid, "export_csv", "users", f"{len(rows)} rows")
        await q.edit_message_text("✅ CSV sent to you.", reply_markup=back_admin_kb())
        return

    if data == "adm_backup":
        try:
            dest = backup_db()
            backups = sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith(".txt")], reverse=True)
            await q.edit_message_text(
                f"💾 *Backup Note Created!*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📄 File: `{os.path.basename(dest)}`\n"
                f"ℹ️ For PostgreSQL, run: `pg_dump -U postgres scanner > backup.sql`\n"
                f"🗂️ Total markers kept: `{len(backups)}`",
                parse_mode="Markdown", reply_markup=back_admin_kb()
            )
            log_admin_op(uid, "backup_db", dest, "pg marker")
        except Exception as e:
            await q.edit_message_text(f"❌ Backup failed: `{e}`", parse_mode="Markdown", reply_markup=back_admin_kb())
        return

    if data == "adm_sub_requests":
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, user_id, username, full_name, requested_tier, status, timestamp "
                        "FROM sub_requests ORDER BY timestamp DESC LIMIT 20"
                    )
                    rows = cur.fetchall()
        except Exception:
            rows = []

        if not rows:
            text = "📋 *طلبات الاشتراك*\n\nلا توجد طلبات حتى الآن."
        else:
            text = "📋 *طلبات الاشتراك*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for r in rows:
                status_icon = "⏳" if r[5] == "pending" else ("✅" if r[5] == "approved" else "❌")
                text += (
                    f"{status_icon} `#{r[0]}` | <code>{r[1]}</code>\n"
                    f"  👤 {esc(r[3] or 'User')} (@{esc(r[2] or 'N/A')})\n"
                    f"  📦 Tier: `{esc(r[4])}` | {str(r[6])[:10]}\n\n"
                )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ قبول طلب",  callback_data="adm_approve_sub"),
             InlineKeyboardButton("❌ رفض طلب",   callback_data="adm_reject_sub")],
            [InlineKeyboardButton("🔙 Admin Panel", callback_data="adm_home")],
        ])
        await q.edit_message_text(text[:4000], parse_mode="HTML", reply_markup=kb)
        return

    if data == "adm_approve_sub":
        context.user_data["admin_action"] = "sub_request_approve"
        await q.edit_message_text(
            "✅ *قبول طلب اشتراك*\n\nأرسل: `REQUEST_ID` أو `REQUEST_ID TIER` لتغيير الباقة\nمثال: `5` أو `5 premium`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="adm_sub_requests")]])
        )
        return

    if data == "adm_reject_sub":
        context.user_data["admin_action"] = "sub_request_reject"
        await q.edit_message_text(
            "❌ *رفض طلب اشتراك*\n\nأرسل رقم الطلب:\nمثال: `5`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="adm_sub_requests")]])
        )
        return

    if data == "adm_op_logs":
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT admin_id, action, target, details, timestamp "
                        "FROM admin_op_logs ORDER BY timestamp DESC LIMIT 30"
                    )
                    rows = cur.fetchall()
        except Exception:
            rows = []

        if not rows:
            text = "📜 *سجل عمليات الأدمن*\n\nلا توجد عمليات مسجلة بعد."
        else:
            text = "📜 <b>سجل عمليات الأدمن</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for r in rows:
                text += (
                    f"👤 <code>{r[0]}</code> → <b>{esc(r[1])}</b>\n"
                    f"  🎯 {esc(r[2])} | {esc(r[3] or '')} | {str(r[4])[:16]}\n\n"
                )
        await q.edit_message_text(text[:4000], parse_mode="HTML", reply_markup=back_admin_kb())
        return

    if data == "adm_bot_status":
        import platform, sys
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM data_index")
                    total_records = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM name_id_index")
                    total_nameid = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM users")
                    total_users = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM uploaded_files")
                    total_files = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM search_logs")
                    total_searches = cur.fetchone()[0]
                    cur.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM uploaded_files")
                    files_size = cur.fetchone()[0]
        except Exception:
            total_records = total_nameid = total_users = total_files = total_searches = files_size = 0

        now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        await q.edit_message_text(
            f"⚡ *Bot Status*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🟢 *Status:* Online\n"
            f"🕐 *Time (UTC):* `{now_utc}`\n"
            f"🐍 *Python:* `{sys.version.split()[0]}`\n"
            f"🖥️ *OS:* `{platform.system()} {platform.release()}`\n"
            f"🐘 *DB:* PostgreSQL\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🗄️ DB Records: `{total_records:,}`\n"
            f"🪪 Name/ID:    `{total_nameid:,}`\n"
            f"👥 Users:      `{total_users:,}`\n"
            f"📁 Files:      `{total_files}`\n"
            f"🔍 Searches:   `{total_searches:,}`\n\n"
            f"📦 Files Size: `{round((files_size or 0)/1024/1024, 2)} MB`",
            parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        return

    if data == "adm_toggle_maintenance":
        global MAINTENANCE_MODE
        MAINTENANCE_MODE = not MAINTENANCE_MODE
        status = (
            "🔧 *Maintenance mode ENABLED.*\n\nAll non-admin users will see the maintenance message."
            if MAINTENANCE_MODE else
            "✅ *Maintenance mode DISABLED.*\n\nBot is open to all users again."
        )
        await q.edit_message_text(status, parse_mode="Markdown", reply_markup=back_admin_kb())
        return

    action_map = {
        "adm_add_credits":    ("add_credits",    "💰 *Add / Deduct Credits*\n\nSend: `USER_ID AMOUNT`\nExample: `123456789 500`\n\n_Use negative to deduct: `123456789 -50`_"),
        "adm_set_tier":       ("set_tier",        f"⬆️ *Set User Tier*\n\nSend: `USER_ID TIER`\n\nTiers: `free` | `basic` | `premium` | `vip`"),
        "adm_ban":            ("ban",             "🔒 *Ban User*\n\nSend the User ID to ban:"),
        "adm_unban":          ("unban",           "✅ *Unban User*\n\nSend the User ID to unban:"),
        "adm_freeze":         ("freeze",          "🧊 *Freeze User Temporarily*\n\nSend: `USER_ID HOURS`\nExample: `123456789 24` → freeze for 24 hours"),
        "adm_adduser_inline": ("adduser_inline",  "➕ *Add User*\n\nSend: `USER_ID TIER`\nExample: `123456789 basic`\n\nTiers: `free` | `basic` | `premium` | `vip`"),
        "adm_deluser":        ("deluser",         "🗑️ *Delete User*\n\nSend the User ID to permanently delete from DB:"),
    }
    if data in action_map:
        action, prompt = action_map[data]
        context.user_data["admin_action"] = action
        await q.edit_message_text(
            prompt, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="adm_home")]])
        )
        return

    if data == "adm_delete_file":
        context.user_data["admin_action"] = "delete_file"
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, original_name, records FROM uploaded_files ORDER BY uploaded_at DESC LIMIT 20"
                    )
                    files = cur.fetchall()
        except Exception:
            files = []

        if not files:
            await q.edit_message_text("No files to delete.", reply_markup=back_admin_kb())
            return
        text = "🗑️ *Delete File*\n\nSend the file ID:\n\n"
        for fid, fname, recs in files:
            text += f"`#{fid}` — `{fname}` ({(recs or 0):,} records)\n"
        await q.edit_message_text(
            text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="adm_home")]])
        )
        return

# ════════════════════════════════════════════
#          TEXT HANDLER + SEARCH LOGIC
# ════════════════════════════════════════════
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = update.message.text.strip()
    ensure_user(uid, update.effective_user.username or "", update.effective_user.first_name or "")

    if is_banned(uid) and not is_admin(uid):
        await update.message.reply_text(
            "🚫 *Your account has been banned.*\n\nPlease contact the admin if you have any questions.",
            parse_mode="Markdown"
        )
        return

    if is_admin(uid) and "admin_action" in context.user_data:
        await handle_admin_text(update, context, context.user_data.pop("admin_action"), text)
        return

    if "search_type" in context.user_data:
        stype = context.user_data.pop("search_type")
        if stype in ("ni_name", "ni_national_id"):
            await show_nameid_counter(update, context, text, stype)
            return
        await show_search_counter(update, context, text, stype)
        return

    if is_admin(uid):
        await update.message.reply_text("⚙️ Admin Panel:", reply_markup=admin_main_kb())
    else:
        await update.message.reply_text("Use the menu:", reply_markup=user_main_kb(uid))

# ════════════════════════════════════════════
#    COUNTER PREVIEW BEFORE DOWNLOAD
# ════════════════════════════════════════════
async def show_search_counter(update: Update, context: ContextTypes.DEFAULT_TYPE, keyword: str, stype: str):
    uid = update.effective_user.id

    if len(keyword.strip()) < MIN_KEYWORD_LEN:
        await update.message.reply_text(
            f"❌ *Keyword too short!*\n\nMinimum *{MIN_KEYWORD_LEN} characters* required.\nTry a more specific keyword.",
            parse_mode="Markdown", reply_markup=back_user_kb(uid)
        )
        return

    if is_search_spamming(uid):
        await update.message.reply_text("⏳ Please wait a few seconds before searching again.", reply_markup=back_user_kb(uid))
        return
    if not can_search(uid):
        await update.message.reply_text(
            "❌ No searches remaining. Upgrade your plan.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 View Plans", callback_data="show_plans")]])
        )
        return

    u     = get_user_cached(uid)
    tier  = u[3] if u else "free"
    limit = TIERS[tier]["max_results"] if not is_admin(uid) else 1_000_000
    mark_search_time(uid)

    msg = await update.message.reply_text(
        f"🔎 *Checking database...*\n\n`{mesc(keyword)}` [{stype.upper()}]",
        parse_mode="Markdown"
    )

    raw_count = await asyncio.get_running_loop().run_in_executor(
        _executor, lambda: count_matches_fast(keyword, stype)
    )
    capped = min(raw_count, limit)

    icons = {"url":"🌐","domain":"🌍","login":"👤","username":"📝",
             "email":"📧","phone":"📱","password":"🔑","all":"🔎"}
    icon = icons.get(stype, "🔍")

    if raw_count == 0:
        alt_types = [t for t in ["email","url","domain","login","username","phone","password","all"] if t != stype]
        suggestions = " | ".join(f"`{t}`" for t in alt_types[:4])
        await msg.edit_text(
            f"🔍 *No results found*\n\n"
            f"Target: `{mesc(keyword)}` | Type: `{stype.upper()}`\n\n"
            f"💡 *Suggestions:*\n"
            f"• Try a shorter keyword\n"
            f"• Try a different search type: {suggestions}\n"
            f"• Use `Full Scan` to search all types at once",
            parse_mode="Markdown", reply_markup=new_search_kb()
        )
        return

    safe_kw    = keyword[:40]
    cb_confirm = _cb_put(f"confirm_search:{stype}:{safe_kw}")
    cb_cancel  = "cancel_search"

    preview_results = await asyncio.get_running_loop().run_in_executor(
        _executor, lambda: smart_search(keyword, stype, 3)
    )
    preview_lines = ""
    if preview_results:
        preview_lines = "\n\n👁️ *Preview (first 3):*\n"
        for r in preview_results[:3]:
            em  = r.get("email") or r.get("username", "")
            pwd = r.get("password", "")
            url = r.get("url") or r.get("domain", "")
            ph  = r.get("phone", "")
            if em and pwd:
                preview_lines += f"• `{mesc(em[:30])}:{mesc(pwd[:20])}`\n"
            elif url and em:
                preview_lines += f"• `{mesc(url[:25])}` | `{mesc(em[:20])}`\n"
            elif ph:
                preview_lines += f"• `{mesc(ph)}`\n"
            elif em:
                preview_lines += f"• `{mesc(em[:40])}`\n"

    await msg.edit_text(
        f"{icon} *Search Preview*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Target    : `{mesc(keyword)}`\n"
        f"📂 Type      : `{stype.upper()}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Available : `{raw_count:,}` raw matches\n"
        f"✅ Your limit: `{capped:,}` records\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Tap *Download* to start the scan and get your `.txt` file.\n"
        f"⏱️ May take up to 3 minutes.{preview_lines}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📥 Download ({capped:,} records)", callback_data=cb_confirm)],
            [InlineKeyboardButton("❌ Cancel", callback_data=cb_cancel)],
        ])
    )

async def show_nameid_counter(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str, stype: str):
    uid = update.effective_user.id

    if stype == "ni_national_id":
        cleaned = re.sub(r"\s", "", query)
        if not re.fullmatch(r"\d{4,14}", cleaned):
            await update.message.reply_text(
                "❌ *رقم غير صالح!*\n\nأدخل رقماً يتكون من 4 إلى 14 رقماً فقط.\n_مثال: `30604` أو `30604150100123`_",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 رجوع", callback_data="go_nameid")]])
            )
            return
    elif stype == "ni_name":
        if len(query.strip()) < 2:
            await update.message.reply_text(
                "❌ *الاسم قصير جداً!*\n\nأدخل اسماً من حرفين على الأقل.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 رجوع", callback_data="go_nameid")]])
            )
            return
        if re.fullmatch(r"[\d\s]+", query.strip()):
            await update.message.reply_text(
                "❌ *الاسم يجب أن يحتوي على حروف!*\n\nلبحث بالرقم اختر 🪪 بحث بالرقم القومي.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 رجوع", callback_data="go_nameid")]])
            )
            return

    if is_search_spamming(uid):
        await update.message.reply_text("⏳ Please wait a few seconds before searching again.", reply_markup=back_user_kb(uid))
        return
    if not can_search_nameid(uid):
        is_ar = get_lang(uid) == "ar"
        msg_out = (
            "❌ *انتهت بحوث Name/ID لليوم.*\n\nقم بترقية باقتك للحصول على المزيد."
            if is_ar else
            "❌ *Name/ID searches used up for today.*\n\nUpgrade your plan for more."
        )
        await update.message.reply_text(
            msg_out, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Plans", callback_data="show_plans")],
                [InlineKeyboardButton("🔙 Back",  callback_data="user_home")],
            ])
        )
        return

    limit = get_nameid_limit(uid)
    mark_search_time(uid)

    if stype == "ni_national_id":
        qtype = "national_id" if is_national_id(query) else "partial_id"
    else:
        qtype = "name"

    type_labels = {
        "national_id": "🪪 رقم قومي",
        "partial_id":  "🔢 رقم جزئي",
        "name":        "👤 اسم",
    }

    msg = await update.message.reply_text(
        f"🔎 *جاري الفحص...*\n\n`{mesc(query)}`", parse_mode="Markdown"
    )

    raw_count = await asyncio.get_running_loop().run_in_executor(
        _executor, lambda: count_nameid_matches(query, qtype)
    )
    capped = min(raw_count, limit)

    if raw_count == 0:
        await msg.edit_text(
            f"🔍 *لا توجد نتائج*\n\nQuery: `{mesc(query)}`\n\n"
            f"💡 *اقتراحات:*\n• جرب اسماً أقصر أو جزءاً من الرقم\n• تأكد من الإملاء الصحيح",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🪪 بحث جديد", callback_data="go_nameid")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="user_home")],
            ])
        )
        return

    preview_results = await asyncio.get_running_loop().run_in_executor(
        _executor, lambda: (search_by_name(query, 3) if qtype == "name" else search_by_national_id(query, 3))
    )
    preview_lines = ""
    if preview_results:
        preview_lines = "\n\n👁️ *معاينة (أول 3):*\n"
        for r in preview_results[:3]:
            preview_lines += f"• 👤 `{mesc(r.get('name','')[:25])}` | 🪪 `{r.get('national_id','')}`\n"

    safe_q     = query[:40]
    cb_confirm = _cb_put(f"confirm_nameid:{stype}:{safe_q}")
    cb_cancel  = "cancel_search"

    await msg.edit_text(
        f"🪪 *نتائج البحث — معاينة*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Query  : `{mesc(query)}`\n"
        f"📂 Type   : {type_labels[qtype]}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 متاح   : `{raw_count:,}` سجل\n"
        f"✅ حدك    : `{capped:,}` سجل"
        f"{preview_lines}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"اضغط *تحميل* للحصول على الملف.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📥 تحميل ({capped:,} سجل)", callback_data=cb_confirm)],
            [InlineKeyboardButton("❌ إلغاء", callback_data=cb_cancel)],
        ])
    )

# ════════════════════════════════════════════
#       NAME / NATIONAL ID SEARCH HANDLER
# ════════════════════════════════════════════
async def do_nameid_search(update: Update, context: ContextTypes.DEFAULT_TYPE,
                           query: str, stype: str, reply_to=None):
    uid = update.effective_user.id
    if context.user_data.get("search_running"):
        send_fn = reply_to.reply_text if reply_to else update.message.reply_text
        await send_fn("⏳ A search is already in progress. Please wait.")
        return
    context.user_data["search_running"] = True
    try:
        if not can_search_nameid(uid):
            send_fn = reply_to.reply_text if reply_to else update.message.reply_text
            await send_fn(
                "❌ *Name/ID searches used up for today.*\n\nUpgrade your plan for more.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Plans", callback_data="show_plans")]])
            )
            return

        limit = get_nameid_limit(uid)

        if stype == "ni_national_id":
            qtype = "national_id" if is_national_id(query) else "partial_id"
        else:
            qtype = "name"

        type_labels = {
            "national_id": "🪪 رقم قومي (14 رقم)",
            "partial_id":  "🔢 رقم جزئي",
            "name":        "👤 اسم",
        }

        send_fn = reply_to.reply_text if reply_to else update.message.reply_text
        msg = await send_fn(
            f"⏳ *جاري البحث...*\n\n"
            f"🎯 Query: `{mesc(query)}`\n"
            f"📂 Type: {type_labels[qtype]}",
            parse_mode="Markdown"
        )

        if qtype == "name":
            results = await asyncio.get_running_loop().run_in_executor(
                _executor, lambda: search_by_name(query, limit)
            )
        else:
            results = await asyncio.get_running_loop().run_in_executor(
                _executor, lambda: search_by_national_id(query, limit)
            )

        if not is_admin(uid):
            deduct_nameid(uid)
        log_search(uid, query, f"nameid_{qtype}", len(results))

        if not results:
            await msg.edit_text(
                f"🔍 *لا توجد نتائج*\n\nQuery: `{mesc(query)}`",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🪪 بحث جديد", callback_data="go_nameid")],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="user_home")],
                ])
            )
            return

        if len(results) <= 3:
            lines = [f"✅ *نتائج البحث* — `{len(results)}` نتيجة\n"]
            for r in results:
                lines.append(f"👤 *{mesc(r['name'])}*\n🪪 `{r['national_id']}`\n")
            await msg.edit_text(
                "\n".join(lines), parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🪪 بحث جديد", callback_data="go_nameid")],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="user_home")],
                ])
            )
            return

        content  = build_nameid_result_txt(query, results, qtype)
        safe_kw  = re.sub(r"[^\w\-]", "_", query)[:30]
        filename = f"nameid_{safe_kw}_{len(results)}_results.txt"
        tmppath  = os.path.join(FILES_DIR, f"tmp_nameid_{uid}.txt")
        with open(tmppath, "w", encoding="utf-8") as f:
            f.write(content)

        caption = (
            f"✅ *نتائج Name/ID Search*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Query  : `{mesc(query)}`\n"
            f"📂 Type   : {type_labels[qtype]}\n"
            f"📊 Total  : `{len(results):,}` سجل\n"
            f"📄 File   : `{mesc(filename)}`"
        )

        await msg.delete()
        send_doc = reply_to.reply_document if reply_to else update.message.reply_document

        file_size_kb = os.path.getsize(tmppath) / 1024
        if file_size_kb > 200:
            zippath = tmppath + ".zip"
            with zipfile.ZipFile(zippath, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(tmppath, filename)
            send_path, send_filename = zippath, filename + ".zip"
            caption += f"\n📦 Compressed"
        else:
            send_path, send_filename = tmppath, filename

        await safe_send_document(
            send_doc, send_path, send_filename, caption,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🪪 بحث جديد", callback_data="go_nameid")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="user_home")],
            ])
        )
    finally:
        context.user_data["search_running"] = False

# ════════════════════════════════════════════
#         REGULAR SEARCH + TIMER
# ════════════════════════════════════════════
async def do_search(update: Update, context: ContextTypes.DEFAULT_TYPE,
                    keyword: str, stype: str, reply_to=None):
    uid = update.effective_user.id
    if context.user_data.get("search_running"):
        await update.message.reply_text("⏳ A search is already in progress. Please wait.")
        return
    context.user_data["search_running"] = True
    try:
        if not can_search(uid):
            await update.message.reply_text(
                "❌ No searches remaining. Upgrade your plan.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 View Plans", callback_data="show_plans")]])
            )
            return

        u     = get_user_cached(uid)
        tier  = u[3] if u else "free"
        limit = TIERS[tier]["max_results"] if not is_admin(uid) else 1_000_000

        send_fn = reply_to.reply_text if reply_to else update.message.reply_text
        msg = await send_fn(
            f"⏳ *Scanning database...*\n\n"
            f"🎯 Target: `{mesc(keyword)}`\n"
            f"📂 Type: `{stype.upper()}`\n\n"
            f"⏱️ Time remaining: *3:00*\n"
            f"`░░░░░░░░░░░░░░░░░░░░` 0%",
            parse_mode="Markdown"
        )

        results = await run_search_with_timer(msg, keyword, stype, limit)

        if not is_admin(uid):
            deduct(uid)
        log_search(uid, keyword, stype, len(results))

        if not results:
            alt_types = [t for t in ["email","url","domain","login","username","phone","password","all"] if t != stype]
            suggestions = " | ".join(f"`{t}`" for t in alt_types[:4])
            await msg.edit_text(
                f"🔍 *No results found*\n\n"
                f"Target: `{mesc(keyword)}` | Type: `{stype.upper()}`\n\n"
                f"💡 *Suggestions:*\n"
                f"• Try a shorter keyword\n"
                f"• Try a different type: {suggestions}\n"
                f"• Use `Full Scan` to search all types",
                parse_mode="Markdown", reply_markup=new_search_kb()
            )
            return

        content  = build_result_txt(keyword, results, stype)
        safe_kw  = re.sub(r"[^\w\-]", "_", keyword)[:30]
        filename = f"{safe_kw}_{len(results)}_results.txt"
        tmppath  = os.path.join(FILES_DIR, f"tmp_{uid}.txt")
        with open(tmppath, "w", encoding="utf-8") as f:
            f.write(content)

        email_count = sum(1 for r in results if r.get("email"))
        other_count = len(results) - email_count

        caption = (
            f"✅ *Scan Complete*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 Target    : `{mesc(keyword)}`\n"
            f"📂 Type      : `{stype.upper()}`\n"
            f"📊 Total     : `{len(results):,}` records\n"
            f"📧 With Email: `{email_count:,}` | 👤 Username: `{other_count:,}`\n"
            f"📄 File      : `{filename}`"
        )

        await msg.delete()
        send_doc = reply_to.reply_document if reply_to else update.message.reply_document

        file_size_kb = os.path.getsize(tmppath) / 1024
        if file_size_kb > 200:
            zippath = tmppath + ".zip"
            with zipfile.ZipFile(zippath, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(tmppath, filename)
            send_path, send_filename = zippath, filename + ".zip"
            caption += f"\n📦 Compressed"
        else:
            send_path, send_filename = tmppath, filename

        await safe_send_document(send_doc, send_path, send_filename, caption, reply_markup=new_search_kb())
    finally:
        context.user_data["search_running"] = False

async def run_search_with_timer(msg, keyword: str, stype: str, limit: int) -> list:
    total_secs = SEARCH_TIMEOUT
    bar_chars  = 20

    async def update_timer():
        intervals = [15, 30, 45, 60, 75, 90, 105, 120, 135, 150, 165, 175]
        for elapsed in intervals:
            await asyncio.sleep(15)
            remaining = total_secs - elapsed
            if remaining <= 0:
                break
            pct  = int((elapsed / total_secs) * 100)
            fill = int(bar_chars * pct / 100)
            bar  = "█" * fill + "░" * (bar_chars - fill)
            mins = remaining // 60
            secs = remaining % 60
            try:
                await msg.edit_text(
                    f"⏳ *Scanning database...*\n\n"
                    f"🎯 Target: `{mesc(keyword)}`\n"
                    f"📂 Type: `{stype.upper()}`\n\n"
                    f"⏱️ Time remaining: *{mins}:{secs:02d}*\n"
                    f"`{bar}` {pct}%",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

    timer_task = asyncio.create_task(update_timer())
    try:
        results = await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(
                _executor, lambda: smart_search(keyword, stype, limit)
            ),
            timeout=SEARCH_TIMEOUT + 10
        )
    except asyncio.TimeoutError:
        results = []
    finally:
        timer_task.cancel()
    return results

# ════════════════════════════════════════════
#         ADMIN TEXT ACTION HANDLER
# ════════════════════════════════════════════
async def handle_admin_text(update, context, action, text):
    uid   = update.effective_user.id
    parts = text.strip().split()

    if action == "add_credits":
        if len(parts) != 2 or not parts[1].lstrip("-").isdigit():
            await update.message.reply_text(
                "❌ Format: `USER_ID AMOUNT`\n\n_Use negative to deduct, e.g. `123456 -50`_",
                parse_mode="Markdown", reply_markup=back_admin_kb()
            )
            return
        target, amount = int(parts[0]), int(parts[1])
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    if amount < 0:
                        cur.execute(
                            "UPDATE users SET credits=GREATEST(0, credits+%s) WHERE user_id=%s",
                            (amount, target)
                        )
                        cur.execute("SELECT credits FROM users WHERE user_id=%s", (target,))
                        row = cur.fetchone()
                        new_balance = row[0] if row else 0
                        result_msg = f"✅ Deducted `{abs(amount)}` credits from user `{target}`. New balance: `{new_balance}`."
                    else:
                        cur.execute("UPDATE users SET credits=credits+%s WHERE user_id=%s", (amount, target))
                        result_msg = f"✅ Added `{amount}` credits to user `{target}`."
                    cur.execute("INSERT INTO sub_history VALUES (%s,%s,%s,%s,%s)",
                                (target, "credits", amount, uid, datetime.utcnow().isoformat()))
                conn.commit()
        except Exception as e:
            log.error(f"add_credits error: {e}")
            result_msg = f"❌ Error: {e}"
        await update.message.reply_text(result_msg, parse_mode="Markdown", reply_markup=back_admin_kb())
        log_admin_op(uid, "add_credits", str(target), f"{amount:+} credits")

    elif action == "set_tier":
        if len(parts) != 2 or parts[1] not in TIERS:
            await update.message.reply_text(
                f"❌ Format: `USER_ID TIER`\nTiers: {', '.join(TIERS)}",
                parse_mode="Markdown", reply_markup=back_admin_kb()
            )
            return
        target, new_tier = int(parts[0]), parts[1]
        t  = TIERS[new_tier]
        nt = NAMEID_TIERS[new_tier]
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM users WHERE user_id=%s", (target,))
                    if cur.fetchone():
                        cur.execute(
                            "UPDATE users SET tier=%s, daily_limit=%s, daily_nameid_limit=%s WHERE user_id=%s",
                            (new_tier, t["daily"], nt["daily_nameid"], target)
                        )
                    else:
                        cur.execute(
                            "INSERT INTO users (user_id, username, full_name, tier, daily_limit, credits, "
                            "is_banned, expires_at, joined_at, lang, daily_nameid_limit) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (target, "unknown", "User", new_tier, t["daily"], 0, 0, None,
                             datetime.utcnow().isoformat(), "en", nt["daily_nameid"])
                        )
                    cur.execute("INSERT INTO sub_history VALUES (%s,%s,%s,%s,%s)",
                                (target, new_tier, 0, uid, datetime.utcnow().isoformat()))
                conn.commit()
        except Exception as e:
            log.error(f"set_tier error: {e}")
        await update.message.reply_text(f"✅ User `{target}` → *{new_tier}*.", parse_mode="Markdown", reply_markup=back_admin_kb())
        log_admin_op(uid, "set_tier", str(target), f"tier={new_tier}")

    elif action == "ban":
        if not parts or not parts[0].lstrip("-").isdigit():
            await update.message.reply_text("❌ Send a valid User ID.", reply_markup=back_admin_kb())
            return
        target = int(parts[0])
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_banned=1 WHERE user_id=%s", (target,))
                conn.commit()
        except Exception as e:
            log.error(f"ban error: {e}")
        await update.message.reply_text(f"🔒 User `{target}` banned.", parse_mode="Markdown", reply_markup=back_admin_kb())
        log_admin_op(uid, "ban", str(target))

    elif action == "unban":
        if not parts or not parts[0].lstrip("-").isdigit():
            await update.message.reply_text("❌ Send a valid User ID.", reply_markup=back_admin_kb())
            return
        target = int(parts[0])
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_banned=0 WHERE user_id=%s", (target,))
                conn.commit()
        except Exception as e:
            log.error(f"unban error: {e}")
        await update.message.reply_text(f"✅ User `{target}` unbanned.", parse_mode="Markdown", reply_markup=back_admin_kb())
        log_admin_op(uid, "unban", str(target))

    elif action == "delete_file":
        if not parts or not parts[0].lstrip("#").isdigit():
            await update.message.reply_text("❌ Send a valid file ID.", reply_markup=back_admin_kb())
            return
        fid = int(parts[0].lstrip("#"))
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT saved_name, original_name, records FROM uploaded_files WHERE id=%s", (fid,))
                    row = cur.fetchone()
                    if not row:
                        await update.message.reply_text(f"❌ File `#{fid}` not found.", parse_mode="Markdown", reply_markup=back_admin_kb())
                        return
                    saved_name, orig_name, record_count = row
                    cur.execute("DELETE FROM data_index WHERE source=%s", (orig_name,))
                    cur.execute("DELETE FROM name_id_index WHERE source=%s", (orig_name,))
                    cur.execute("DELETE FROM uploaded_files WHERE id=%s", (fid,))
                conn.commit()
        except Exception as e:
            log.error(f"delete_file error: {e}")
            await update.message.reply_text(f"❌ Error: {e}", reply_markup=back_admin_kb())
            return
        fpath = os.path.join(FILES_DIR, saved_name)
        if os.path.exists(fpath):
            os.remove(fpath)
        await update.message.reply_text(
            f"🗑️ `{orig_name}` deleted. `{(record_count or 0):,}` records removed.",
            parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        log_admin_op(uid, "delete_file", orig_name, f"{record_count:,} records removed")

    elif action == "broadcast":
        msg_text = text.strip()
        if not msg_text:
            await update.message.reply_text("❌ Message is empty.", reply_markup=back_admin_kb())
            return
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT user_id FROM users WHERE is_banned=0")
                    user_ids = [r[0] for r in cur.fetchall()]
        except Exception:
            user_ids = []

        progress = await update.message.reply_text(
            f"📢 Broadcasting to *{len(user_ids)}* users...", parse_mode="Markdown"
        )
        sent = failed = flood_waits = 0
        for i, target_uid in enumerate(user_ids):
            try:
                await context.bot.send_message(
                    chat_id=target_uid,
                    text=f"📢 *Message from Admin:*\n\n{mesc(msg_text)}",
                    parse_mode="Markdown"
                )
                sent += 1
            except RetryAfter as e:
                flood_waits += 1
                wait_secs = int(e.retry_after) + 1
                await asyncio.sleep(wait_secs)
                try:
                    await context.bot.send_message(chat_id=target_uid, text=f"📢 Message from Admin:\n\n{msg_text}")
                    sent += 1
                except Exception:
                    failed += 1
            except (Forbidden, BadRequest):
                failed += 1
            except Exception:
                failed += 1
            if (i + 1) % 50 == 0:
                try:
                    await progress.edit_text(
                        f"📢 *Broadcasting...*\n\n"
                        f"📤 Sent: `{sent}` / `{len(user_ids)}`\n"
                        f"❌ Failed: `{failed}`",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
            await asyncio.sleep(0.05)

        await progress.edit_text(
            f"✅ *Broadcast Complete*\n\n"
            f"📤 Sent: `{sent}`\n"
            f"❌ Failed: `{failed}`\n"
            f"⏸️ FloodWaits: `{flood_waits}`\n"
            f"👥 Total: `{len(user_ids)}`",
            parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        log_admin_op(uid, "broadcast", "all_users", f"sent={sent}, failed={failed}")

    elif action == "freeze":
        if len(parts) != 2 or not parts[0].lstrip("-").isdigit() or not parts[1].isdigit():
            await update.message.reply_text("❌ Format: `USER_ID HOURS`", parse_mode="Markdown", reply_markup=back_admin_kb())
            return
        target, hours = int(parts[0]), int(parts[1])
        if hours <= 0:
            try:
                with pool_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE users SET frozen_until=NULL WHERE user_id=%s", (target,))
                    conn.commit()
            except Exception as e:
                log.error(f"unfreeze error: {e}")
            await update.message.reply_text(f"✅ User `{target}` unfrozen.", parse_mode="Markdown", reply_markup=back_admin_kb())
            log_admin_op(uid, "unfreeze", str(target))
        else:
            until = (datetime.utcnow() + timedelta(hours=hours)).isoformat()
            try:
                with pool_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE users SET frozen_until=%s WHERE user_id=%s", (until, target))
                    conn.commit()
            except Exception as e:
                log.error(f"freeze error: {e}")
            await update.message.reply_text(
                f"🧊 User `{target}` frozen for `{hours}` hours.",
                parse_mode="Markdown", reply_markup=back_admin_kb()
            )
            log_admin_op(uid, "freeze", str(target), f"{hours}h")
            try:
                await context.bot.send_message(
                    chat_id=target,
                    text=f"🧊 Your account has been temporarily frozen for *{hours}* hour(s).",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

    elif action == "msg_user":
        await handle_msg_user(update, context, text)

    elif action == "adduser_inline":
        if len(parts) != 2 or not parts[0].lstrip("-").isdigit() or parts[1] not in TIERS:
            await update.message.reply_text(
                f"❌ Format: `USER_ID TIER`\nTiers: {', '.join(TIERS)}",
                parse_mode="Markdown", reply_markup=back_admin_kb()
            )
            return
        target, tier_val = int(parts[0]), parts[1]
        t  = TIERS[tier_val]
        nt = NAMEID_TIERS[tier_val]
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM users WHERE user_id=%s", (target,))
                    if cur.fetchone():
                        cur.execute(
                            "UPDATE users SET tier=%s, daily_limit=%s, daily_nameid_limit=%s WHERE user_id=%s",
                            (tier_val, t["daily"], nt["daily_nameid"], target)
                        )
                        result_msg = f"✅ Updated user `{target}` → tier `{tier_val}`."
                    else:
                        cur.execute(
                            "INSERT INTO users (user_id, username, full_name, tier, daily_limit, credits, "
                            "is_banned, expires_at, joined_at, lang, daily_nameid_limit) "
                            "VALUES (%s,%s,%s,%s,%s,0,0,NULL,%s,%s,%s)",
                            (target, "", "", tier_val, t["daily"], datetime.utcnow().isoformat(), "en", nt["daily_nameid"])
                        )
                        result_msg = f"✅ User `{target}` added with tier `{tier_val}`."
                conn.commit()
        except Exception as e:
            log.error(f"adduser_inline error: {e}")
            result_msg = f"❌ Error: {e}"
        await update.message.reply_text(result_msg, parse_mode="Markdown", reply_markup=back_admin_kb())
        log_admin_op(uid, "adduser_inline", str(target), tier_val)

    elif action == "deluser":
        if len(parts) != 1 or not parts[0].lstrip("-").isdigit():
            await update.message.reply_text("❌ Send a valid User ID.", parse_mode="Markdown", reply_markup=back_admin_kb())
            return
        target = int(parts[0])
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT full_name, username FROM users WHERE user_id=%s", (target,))
                    exists = cur.fetchone()
                    if not exists:
                        await update.message.reply_text(f"❌ User `{target}` not found.", parse_mode="Markdown", reply_markup=back_admin_kb())
                        return
                    cur.execute("DELETE FROM users WHERE user_id=%s", (target,))
                    cur.execute("DELETE FROM search_logs WHERE user_id=%s", (target,))
                    cur.execute("DELETE FROM sub_history WHERE user_id=%s", (target,))
                    cur.execute("DELETE FROM sub_requests WHERE user_id=%s", (target,))
                conn.commit()
        except Exception as e:
            log.error(f"deluser error: {e}")
            await update.message.reply_text(f"❌ Error: {e}", reply_markup=back_admin_kb())
            return
        fname_del = esc(exists[0] or "N/A")
        await update.message.reply_text(
            f"✅ User `{target}` ({fname_del}) permanently deleted.",
            parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        log_admin_op(uid, "deluser", str(target), f"name={exists[0]}")

    elif action == "set_expiry":
        if len(parts) != 2 or not parts[0].lstrip("-").isdigit() or not parts[1].lstrip("-").isdigit():
            await update.message.reply_text("❌ Format: `USER_ID DAYS`", parse_mode="Markdown", reply_markup=back_admin_kb())
            return
        target, days = int(parts[0]), int(parts[1])
        if days <= 0:
            exp_val  = None
            msg_out  = f"✅ Expiry cleared for user `{target}`."
        else:
            exp_val  = (datetime.utcnow() + timedelta(days=days)).isoformat()
            msg_out  = f"✅ User `{target}` expires in `{days}` days (`{exp_val[:10]}`)."
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET expires_at=%s WHERE user_id=%s", (exp_val, target))
                conn.commit()
        except Exception as e:
            log.error(f"set_expiry error: {e}")
        await update.message.reply_text(msg_out, parse_mode="Markdown", reply_markup=back_admin_kb())
        log_admin_op(uid, "set_expiry", str(target), f"days={days}")

    elif action == "filter_logs":
        query_val = text.strip()
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    if query_val.lstrip("-").isdigit():
                        cur.execute(
                            "SELECT user_id, keyword, category, results, timestamp FROM search_logs "
                            "WHERE user_id=%s ORDER BY timestamp DESC LIMIT 30",
                            (int(query_val),)
                        )
                        title = f"🔎 Logs for user `{mesc(query_val)}`"
                    else:
                        cur.execute(
                            "SELECT user_id, keyword, category, results, timestamp FROM search_logs "
                            "WHERE keyword ILIKE %s ORDER BY timestamp DESC LIMIT 30",
                            (f"%{query_val}%",)
                        )
                        title = f"🔎 Logs matching `{esc(query_val)}`"
                    rows = cur.fetchall()
        except Exception as e:
            log.error(f"filter_logs error: {e}")
            rows = []
            title = "❌ Error"

        if not rows:
            await update.message.reply_text(f"📭 No logs found for `{esc(query_val)}`.", parse_mode="Markdown", reply_markup=back_admin_kb())
            return
        text_out = f"{title}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for r in rows:
            text_out += f"`{r[0]}` → `{esc(r[1][:20])}` [{esc(r[2])}] {r[3]} res @ {str(r[4])[:10]}\n"
        await update.message.reply_text(text_out[:4000], parse_mode="Markdown", reply_markup=back_admin_kb())

    elif action == "sub_request_approve":
        if not parts or not parts[0].isdigit():
            await update.message.reply_text("❌ Send a valid request ID.", reply_markup=back_admin_kb())
            return
        req_id        = int(parts[0])
        override_tier = parts[1] if len(parts) > 1 and parts[1] in TIERS else None
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT user_id, username, full_name, requested_tier FROM sub_requests WHERE id=%s", (req_id,))
                    req = cur.fetchone()
                    if not req:
                        await update.message.reply_text(f"❌ Request `#{req_id}` not found.", parse_mode="Markdown", reply_markup=back_admin_kb())
                        return
                    target_uid, uname, fname, req_tier = req
                    final_tier = override_tier or req_tier
                    t  = TIERS[final_tier]
                    nt = NAMEID_TIERS[final_tier]
                    cur.execute("SELECT 1 FROM users WHERE user_id=%s", (target_uid,))
                    if cur.fetchone():
                        cur.execute(
                            "UPDATE users SET tier=%s, daily_limit=%s, daily_nameid_limit=%s WHERE user_id=%s",
                            (final_tier, t["daily"], nt["daily_nameid"], target_uid)
                        )
                    else:
                        cur.execute(
                            "INSERT INTO users (user_id, username, full_name, tier, daily_limit, credits, "
                            "is_banned, expires_at, joined_at, lang, daily_nameid_limit) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (target_uid, uname, fname, final_tier, t["daily"], 0, 0, None,
                             datetime.utcnow().isoformat(), "en", nt["daily_nameid"])
                        )
                    cur.execute("UPDATE sub_requests SET status='approved' WHERE id=%s", (req_id,))
                    cur.execute("INSERT INTO sub_history VALUES (%s,%s,%s,%s,%s)",
                                (target_uid, final_tier, 0, uid, datetime.utcnow().isoformat()))
                conn.commit()
        except Exception as e:
            log.error(f"sub_request_approve error: {e}")
            await update.message.reply_text(f"❌ Error: {e}", reply_markup=back_admin_kb())
            return

        try:
            user_lang = get_lang(target_uid)
            user_st   = STRINGS.get(user_lang, STRINGS["en"])
            await context.bot.send_message(
                chat_id=target_uid,
                text=user_st["sub_approved_user"].format(tier=final_tier),
                parse_mode="Markdown"
            )
        except Exception:
            pass
        await update.message.reply_text(
            f"✅ Request `#{req_id}` approved → `{final_tier}` for user `{target_uid}`.",
            parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        log_admin_op(uid, "approve_sub", str(target_uid), f"req={req_id}, tier={final_tier}")

    elif action == "sub_request_reject":
        if not parts or not parts[0].isdigit():
            await update.message.reply_text("❌ Send a valid request ID.", reply_markup=back_admin_kb())
            return
        req_id = int(parts[0])
        try:
            with pool_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT user_id FROM sub_requests WHERE id=%s", (req_id,))
                    req = cur.fetchone()
                    if not req:
                        await update.message.reply_text(f"❌ Request `#{req_id}` not found.", parse_mode="Markdown", reply_markup=back_admin_kb())
                        return
                    target_uid = req[0]
                    cur.execute("UPDATE sub_requests SET status='rejected' WHERE id=%s", (req_id,))
                conn.commit()
        except Exception as e:
            log.error(f"sub_request_reject error: {e}")
            await update.message.reply_text(f"❌ Error: {e}", reply_markup=back_admin_kb())
            return

        try:
            user_lang = get_lang(target_uid)
            user_st   = STRINGS.get(user_lang, STRINGS["en"])
            await context.bot.send_message(
                chat_id=target_uid,
                text=user_st["sub_rejected_user"],
                parse_mode="Markdown"
            )
        except Exception:
            pass
        await update.message.reply_text(
            f"❌ Request `#{req_id}` rejected.",
            parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        log_admin_op(uid, "reject_sub", str(target_uid), f"req={req_id}")

# ════════════════════════════════════════════
#     FILE UPLOAD HANDLER
# ════════════════════════════════════════════
MAX_UPLOAD_MB  = 100
_last_upload: dict = {}
_UPLOAD_COOLDOWN   = 1

async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text(
            "📁 Only the admin can upload files.\n\nUse /start to search.",
            reply_markup=user_main_kb()
        )
        return

    now_t = time.monotonic()
    with _rate_limit_lock:
        last_up = _last_upload.get(uid, 0)
        if (now_t - last_up) < _UPLOAD_COOLDOWN:
            remaining = int(_UPLOAD_COOLDOWN - (now_t - last_up))
            await update.message.reply_text(
                f"⏳ Please wait *{remaining}s* before uploading another file.",
                parse_mode="Markdown"
            )
            return
        _last_upload[uid] = now_t

    doc   = update.message.document
    fname = doc.file_name or "upload.bin"
    ext   = fname.lower().rsplit(".", 1)[-1] if "." in fname else ""

    if ext not in ("txt", "csv", "xlsx", "xls", "json"):
        await update.message.reply_text(
            f"❌ Unsupported: `{mesc(fname)}`\nAllowed: TXT, CSV, XLSX, XLS, JSON",
            parse_mode="Markdown"
        )
        return

    file_size_bytes = doc.file_size or 0
    if file_size_bytes > MAX_UPLOAD_MB * 1024 * 1024:
        await update.message.reply_text(
            f"❌ *File too large!*\n\n"
            f"Max allowed: `{MAX_UPLOAD_MB} MB`\n"
            f"Your file: `{round(file_size_bytes/1024/1024, 1)} MB`",
            parse_mode="Markdown"
        )
        return

    msg = await update.message.reply_text(f"📥 *Downloading* `{fname}`...", parse_mode="Markdown")

    ts_prefix = datetime.utcnow().strftime("%Y%m%d_%H%M%S_")
    save_name = ts_prefix + fname
    save_path = os.path.join(FILES_DIR, save_name)

    try:
        file_obj = await doc.get_file()
        await file_obj.download_to_drive(save_path)
        file_size = os.path.getsize(save_path)
    except Exception as e:
        await msg.edit_text(f"❌ Download failed: `{e}`", parse_mode="Markdown")
        return

    await msg.edit_text(f"⚙️ *Parsing* `{fname}`...", parse_mode="Markdown")

    file_md5 = hashlib.md5(open(save_path, "rb").read()).hexdigest()
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT original_name FROM uploaded_files WHERE file_md5=%s", (file_md5,))
                dup = cur.fetchone()
                if dup:
                    await msg.edit_text(
                        f"⚠️ *Duplicate File Detected!*\n\n"
                        f"This file was already uploaded as `{mesc(dup[0])}`.\n"
                        f"Upload aborted to prevent duplicate records.",
                        parse_mode="Markdown", reply_markup=back_admin_kb()
                    )
                    os.remove(save_path)
                    return
    except Exception as e:
        log.error(f"dup check error: {e}")

    if ext in ("xlsx", "xls"):
        nameid_rows = parse_excel_for_name_id(save_path, fname)
        if nameid_rows:
            try:
                with pool_conn() as conn:
                    with conn.cursor() as cur:
                        for i in range(0, len(nameid_rows), 2000):
                            batch = nameid_rows[i:i+2000]
                            cur.executemany(
                                "INSERT INTO name_id_index (full_name, national_id, source) VALUES (%s,%s,%s) "
                                "ON CONFLICT DO NOTHING",
                                batch
                            )
                        cur.execute(
                            "INSERT INTO uploaded_files (saved_name, original_name, size_bytes, records, uploaded_by, uploaded_at, file_md5) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                            (save_name, fname, file_size, len(nameid_rows), uid, datetime.utcnow().isoformat(), file_md5)
                        )
                    conn.commit()
            except Exception as e:
                log.error(f"nameid insert error: {e}")
                await msg.edit_text(f"❌ DB error: `{mesc(str(e))}`", parse_mode="Markdown")
                return
            await msg.edit_text(
                f"✅ *Excel Indexed (Name/ID)*\n\n"
                f"📄 File    : `{mesc(fname)}`\n"
                f"🪪 Records : `{len(nameid_rows):,}` Name/ID rows\n"
                f"💾 Size    : `{round(file_size/1024,1)} KB`",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗂️ View Files",  callback_data="adm_filelist")],
                    [InlineKeyboardButton("🔙 Admin Panel", callback_data="adm_home")],
                ])
            )
            return
        await msg.edit_text("⚙️ *No Name/ID columns found. Indexing all data...*", parse_mode="Markdown")

    try:
        rows = parse_file(save_path, fname)
    except Exception as e:
        await msg.edit_text(f"❌ Parse failed: `{mesc(str(e))}`", parse_mode="Markdown")
        return

    if not rows:
        await msg.edit_text(f"⚠️ No valid data found in `{mesc(fname)}`.", parse_mode="Markdown")
        return

    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(rows), 2000):
                    cur.executemany(
                        "INSERT INTO data_index (line, source) VALUES (%s,%s)",
                        rows[i:i+2000]
                    )
                cur.execute(
                    "INSERT INTO uploaded_files (saved_name, original_name, size_bytes, records, uploaded_by, uploaded_at, file_md5) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (save_name, fname, file_size, len(rows), uid, datetime.utcnow().isoformat(), file_md5)
                )
            conn.commit()
    except Exception as e:
        log.error(f"data_index insert error: {e}")
        await msg.edit_text(f"❌ DB error: `{mesc(str(e))}`", parse_mode="Markdown")
        return

    await msg.edit_text(
        f"✅ *File Indexed Successfully!*\n\n"
        f"📄 File    : `{mesc(fname)}`\n"
        f"📊 Records : `{len(rows):,}` lines\n"
        f"💾 Size    : `{round(file_size/1024,1)} KB`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗂️ View Files",  callback_data="adm_filelist")],
            [InlineKeyboardButton("🔙 Admin Panel", callback_data="adm_home")],
        ])
    )
    log_admin_op(uid, "upload_file", fname, f"{len(rows):,} records | {round(file_size/1024,1)} KB")

# ════════════════════════════════════════════
#               COMMANDS
# ════════════════════════════════════════════
async def cmd_hello(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid  = user.id
    ensure_user(uid, user.username or "", user.first_name or "")
    if is_banned(uid):
        await update.message.reply_text("🚫 *Your account has been banned.*", parse_mode="Markdown")
        return
    if is_admin(uid):
        await show_admin_home(update, context, send=True)
    else:
        await show_user_home(update, context, send=True)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid, update.effective_user.username or "", update.effective_user.first_name or "")
    if is_admin(uid):
        await update.message.reply_text("⚙️ Admin Panel:", reply_markup=admin_main_kb())
    else:
        await update.message.reply_text("ℹ️ Use /start to open the main menu.", reply_markup=user_main_kb())

async def _do_subscribe_request(uid: int, username: str, full_name: str, tier: str, context, reply_fn):
    st = STRINGS.get(get_lang(uid), STRINGS["en"])
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM sub_requests WHERE user_id=%s AND status='pending'", (uid,)
                )
                existing = cur.fetchone()
                if existing:
                    msg = st["sub_req_exists"].format(req_id=existing[0])
                    await reply_fn(msg, parse_mode="Markdown")
                    return
                cur.execute(
                    "INSERT INTO sub_requests (user_id, username, full_name, requested_tier, timestamp) VALUES (%s,%s,%s,%s,%s)",
                    (uid, username or "", full_name or "User", tier, datetime.utcnow().isoformat())
                )
                cur.execute("SELECT lastval()")
                req_id = cur.fetchone()[0]
            conn.commit()
    except Exception as e:
        log.error(f"_do_subscribe_request error: {e}")
        await reply_fn("❌ Error submitting request.", parse_mode="Markdown")
        return

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"📋 *New Subscription Request!*\n\n"
                    f"🆔 Request ID: `#{req_id}`\n"
                    f"👤 User: `{uid}` — {esc(full_name or 'User')} (@{esc(username or 'N/A')})\n"
                    f"📦 Tier: *{tier}*\n\n"
                    f"Go to 📋 طلبات الاشتراك in the admin panel."
                ),
                parse_mode="Markdown"
            )
        except Exception:
            pass
    msg = st["sub_req_sent"].format(tier=tier, req_id=req_id)
    await reply_fn(msg, parse_mode="Markdown")

async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid  = user.id
    ensure_user(uid, user.username or "", user.first_name or "")
    if is_banned(uid):
        await update.message.reply_text("🚫 *Your account has been banned.*", parse_mode="Markdown")
        return

    u = get_user_cached(uid)
    current_tier = u[3] if u else "free"
    t_current    = TIERS.get(current_tier, TIERS["free"])

    args = context.args
    tier = args[0].lower() if args and args[0].lower() in TIERS else None
    if not tier or tier == "free":
        tiers_list = " | ".join(t for t in TIERS if t != "free")
        await update.message.reply_text(
            f"📋 *طلب اشتراك / Subscription Request*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 *Current Plan:* `{current_tier}` — {esc(t_current['label'])}\n\n"
            f"استخدم: `/subscribe TIER`\n"
            f"الباقات المتاحة: `{tiers_list}`\n\n"
            f"مثال: `/subscribe premium`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⭐ Basic",   callback_data="sub_req_basic"),
                 InlineKeyboardButton("💎 Premium", callback_data="sub_req_premium")],
                [InlineKeyboardButton("👑 VIP",     callback_data="sub_req_vip")],
            ])
        )
        return

    await _do_subscribe_request(
        uid, user.username or "", user.first_name or "User",
        tier, context, update.message.reply_text
    )

async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"🆔 *Your Telegram ID:*\n\n`{user.id}`\n\n"
        f"_(Share this with the admin to manage your account)_",
        parse_mode="Markdown"
    )

async def cmd_finduser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: `/finduser USER_ID` or `/finduser @username`", parse_mode="Markdown")
        return
    q_val = context.args[0].lstrip("@")
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                if q_val.lstrip("-").isdigit():
                    cur.execute("SELECT * FROM users WHERE user_id=%s", (int(q_val),))
                else:
                    cur.execute("SELECT * FROM users WHERE username ILIKE %s", (f"%{q_val}%",))
                row = cur.fetchone()
    except Exception as e:
        log.error(f"cmd_finduser error: {e}")
        row = None

    if not row:
        await update.message.reply_text(f"❌ User `{mesc(q_val)}` not found.", parse_mode="Markdown")
        return
    uid_r, uname, fname, tier, daily, credits, banned, expires, joined, lang_r, ref_by, ref_cnt, last_s, nameid_lim = row[:14]
    status = "🚫 Banned" if banned else "✅ Active"
    await update.message.reply_text(
        f"👤 *User Info*\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 ID       : `{uid_r}`\n"
        f"👤 Name     : {esc(fname or 'N/A')}\n"
        f"🔖 Username : @{esc(uname or 'N/A')}\n"
        f"📦 Tier     : `{tier}`\n"
        f"🔍 Daily    : `{daily}` left\n"
        f"🪪 Name/ID  : `{nameid_lim}` left\n"
        f"💰 Credits  : `{credits}`\n"
        f"📅 Expires  : `{str(expires or 'None')}`\n"
        f"📆 Joined   : `{str(joined or '')[:10]}`\n"
        f"🌐 Lang     : `{lang_r}`\n"
        f"🔗 Referred by: `{ref_by or 'None'}`\n"
        f"👥 Referrals: `{ref_cnt}`\n"
        f"Status     : {status}",
        parse_mode="Markdown", reply_markup=back_admin_kb()
    )

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("search_type", None)
    context.user_data.pop("confirmed_kw", None)
    context.user_data.pop("admin_action", None)
    await update.message.reply_text(
        "❌ Cancelled. Use the menu to start a new search.",
        reply_markup=user_main_kb(update.effective_user.id)
    )

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t0  = time.monotonic()
    msg = await update.message.reply_text("🏓 Pong!")
    latency_ms = round((time.monotonic() - t0) * 1000)
    await msg.edit_text(
        f"🏓 *Pong!*\n⚡ Latency: `{latency_ms}ms`\n🟢 Bot is online.",
        parse_mode="Markdown"
    )

async def cmd_version(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime_str = "N/A"
    if BOT_START_TIME:
        delta = datetime.utcnow() - BOT_START_TIME
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m, s   = divmod(rem, 60)
        uptime_str = f"{h}h {m}m {s}s"
    await update.message.reply_text(
        f"🤖 *DATA SCANNER BOT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Version : `v5.4-PG`\n"
        f"⏱️ Uptime  : `{uptime_str}`\n"
        f"🐍 Python  : `{__import__('sys').version.split()[0]}`\n"
        f"🐘 DB      : PostgreSQL",
        parse_mode="Markdown"
    )

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM data_index");    tr = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM name_id_index"); tn = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users");         tu = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users WHERE is_banned=1"); tb = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM search_logs");   ts = cur.fetchone()[0]
                today = datetime.utcnow().strftime("%Y-%m-%d")
                cur.execute("SELECT COUNT(*) FROM search_logs WHERE timestamp LIKE %s", (f"{today}%",))
                ts_today = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users WHERE joined_at LIKE %s", (f"{today}%",))
                new_today = cur.fetchone()[0]
    except Exception as e:
        log.error(f"cmd_stats error: {e}")
        await update.message.reply_text("❌ Stats error.")
        return

    await update.message.reply_text(
        f"📊 *Quick Stats*\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🗄️ DB Records : `{tr:,}`\n"
        f"🪪 Name/ID    : `{tn:,}`\n"
        f"👥 Users      : `{tu:,}` (🚫 {tb} banned)\n"
        f"🔍 Searches   : `{ts:,}` total | `{ts_today}` today\n"
        f"🆕 New today  : `{new_today}`",
        parse_mode="Markdown", reply_markup=back_admin_kb()
    )

async def cmd_adduser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /adduser USER_ID TIER [CREDITS]")
        return
    target_id = int(args[0])
    tier      = args[1] if args[1] in TIERS else "free"
    credits   = int(args[2]) if len(args) > 2 else 0
    t  = TIERS[tier]
    nt = NAMEID_TIERS[tier]
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM users WHERE user_id=%s", (target_id,))
                if cur.fetchone():
                    cur.execute(
                        "UPDATE users SET tier=%s, daily_limit=%s, credits=credits+%s WHERE user_id=%s",
                        (tier, t["daily"], credits, target_id)
                    )
                else:
                    cur.execute(
                        "INSERT INTO users (user_id, username, full_name, tier, daily_limit, credits, "
                        "is_banned, expires_at, joined_at, lang, referral_count, daily_nameid_limit) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (target_id, "unknown", "User", tier, t["daily"], credits, 0, None,
                         datetime.utcnow().isoformat(), "en", 0, nt["daily_nameid"])
                    )
            conn.commit()
    except Exception as e:
        log.error(f"cmd_adduser error: {e}")
    await update.message.reply_text(f"✅ User `{target_id}` → `{tier}` + `{credits}` credits.", parse_mode="Markdown")

# ════════════════════════════════════════════
#         GLOBAL ERROR HANDLER
# ════════════════════════════════════════════
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err_type = type(context.error).__name__
    err_msg  = str(context.error)
    log.error(f"Unhandled exception [{err_type}]: {err_msg}", exc_info=context.error)

    if isinstance(update, Update) and update.effective_message:
        uid_ctx = update.effective_user.id if update.effective_user else "?"
        try:
            if isinstance(context.error, (BadRequest, TelegramError)):
                user_msg = "⚠️ *Telegram error* — please try again."
            elif isinstance(context.error, asyncio.TimeoutError):
                user_msg = "⏱️ *Request timed out.* Try a more specific keyword."
            else:
                user_msg = "⚠️ *Unexpected error.* Please press /start and try again."
            await update.effective_message.reply_text(user_msg, parse_mode="Markdown")
        except Exception:
            pass
    else:
        uid_ctx = "N/A"

    import traceback
    tb_str   = "".join(traceback.format_exception(type(context.error), context.error, context.error.__traceback__))
    err_text = (
        f"⚠️ *Bot Error*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 User: `{uid_ctx}`\n"
        f"🔴 Error: `{mesc(err_type)}`\n"
        f"📝 Message: `{mesc(err_msg[:200])}`\n\n"
        f"```\n{mesc(tb_str[-500:])}\n```"
    )
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=err_text[:4000], parse_mode="Markdown")
        except Exception:
            pass

# ════════════════════════════════════════════
#     EXPIRY CHECKER
# ════════════════════════════════════════════
async def check_expiry_notifications(app):
    now = datetime.utcnow()
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT user_id, tier, expires_at, lang FROM users WHERE expires_at IS NOT NULL AND is_banned=0"
                )
                rows = cur.fetchall()
    except Exception:
        return

    for uid, tier, exp_str, lang in rows:
        try:
            exp       = datetime.fromisoformat(str(exp_str))
            days_left = (exp - now).days
            if 0 <= days_left <= 3:
                is_ar = (lang or "en") == "ar"
                if is_ar:
                    msg = (
                        f"⚠️ *تنبيه انتهاء الاشتراك!*\n\n"
                        f"📦 باقتك *{tier}* ستنتهي خلال *{days_left}* يوم.\n"
                        f"تواصل مع الأدمن للتجديد."
                    )
                else:
                    msg = (
                        f"⚠️ *Subscription Expiry Notice!*\n\n"
                        f"📦 Your *{tier}* plan expires in *{days_left}* day(s).\n"
                        f"Contact admin to renew."
                    )
                await app.bot.send_message(chat_id=uid, text=msg, parse_mode="Markdown")
        except Exception:
            pass

# ════════════════════════════════════════════
#         ADMIN: MESSAGE SPECIFIC USER
# ════════════════════════════════════════════
async def handle_msg_user(update, context, text):
    uid   = update.effective_user.id
    parts = text.strip().split(" ", 1)
    if len(parts) < 2 or not parts[0].lstrip("-").isdigit():
        await update.message.reply_text(
            "❌ Format: `USER_ID Your message here`\nExample: `123456789 Hello!`",
            parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        return
    target_uid = int(parts[0])
    msg_text   = parts[1].strip()
    try:
        await context.bot.send_message(
            chat_id=target_uid,
            text=f"📩 *Message from Admin:*\n\n{mesc(msg_text)}",
            parse_mode="Markdown"
        )
        await update.message.reply_text(
            f"✅ Message sent to `{target_uid}`.", parse_mode="Markdown", reply_markup=back_admin_kb()
        )
        log_admin_op(uid, "msg_user", str(target_uid), msg_text[:50])
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: `{e}`", parse_mode="Markdown", reply_markup=back_admin_kb())

# ════════════════════════════════════════════
#            AUTO EXPIRE & CLEANUP
# ════════════════════════════════════════════
def auto_expire_subscriptions():
    now = datetime.utcnow().isoformat()
    try:
        with pool_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT user_id, tier FROM users WHERE expires_at IS NOT NULL AND expires_at <= %s AND tier != 'free'",
                    (now,)
                )
                expired = cur.fetchall()
                if expired:
                    cur.execute(
                        "UPDATE users SET tier='free', daily_limit=0, daily_nameid_limit=0, expires_at=NULL "
                        "WHERE expires_at IS NOT NULL AND expires_at <= %s AND tier != 'free'",
                        (now,)
                    )
            conn.commit()
        return expired
    except Exception as e:
        log.error(f"auto_expire_subscriptions error: {e}")
        return []

def cleanup_temp_files():
    removed = 0
    try:
        for fname in os.listdir(FILES_DIR):
            if fname.startswith("tmp_"):
                fpath = os.path.join(FILES_DIR, fname)
                try:
                    age = time.time() - os.path.getmtime(fpath)
                    if age > 3600:
                        os.remove(fpath)
                        removed += 1
                except Exception:
                    pass
    except Exception:
        pass
    return removed

# ════════════════════════════════════════════
#                    MAIN
# ════════════════════════════════════════════
def main():
    global BOT_START_TIME
    BOT_START_TIME = datetime.utcnow()
    init_pool()
    init_db()

    if not TOKEN or not re.match(r"^\d+:[A-Za-z0-9_-]{35,}$", TOKEN):
        log.error("❌ BOT_TOKEN is missing or invalid! Set BOT_TOKEN environment variable.")
        return

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("hello",     cmd_hello))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("id",        cmd_id))
    app.add_handler(CommandHandler("cancel",    cmd_cancel))
    app.add_handler(CommandHandler("version",   cmd_version))
    app.add_handler(CommandHandler("ping",      cmd_ping))
    app.add_handler(CommandHandler("finduser",  cmd_finduser))
    app.add_handler(CommandHandler("stats",     cmd_stats))
    app.add_handler(CommandHandler("adduser",   cmd_adduser))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.Document.ALL, file_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_error_handler(error_handler)

    def _shutdown(signum, frame):
        log.info(f"🛑 Received signal {signum} — shutting down gracefully...")
        app.stop_running()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    async def post_init(application):
        await check_expiry_notifications(application)

        async def daily_job():
            while True:
                now           = datetime.utcnow()
                next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                wait_secs     = (next_midnight - now).total_seconds()
                await asyncio.sleep(wait_secs)

                do_daily_reset()
                log.info("✅ Daily limits reset.")

                expired_users = auto_expire_subscriptions()
                for uid_exp, tier_exp in expired_users:
                    try:
                        lang  = get_lang(uid_exp)
                        is_ar = lang == "ar"
                        msg   = (
                            f"⏰ *انتهى اشتراكك!*\n\nباقتك *{tier_exp}* انتهت.\nتواصل مع الأدمن للتجديد."
                            if is_ar else
                            f"⏰ *Subscription Expired!*\n\nYour *{tier_exp}* plan has ended.\nContact admin to renew."
                        )
                        await application.bot.send_message(chat_id=uid_exp, text=msg, parse_mode="Markdown")
                    except Exception:
                        pass
                if expired_users:
                    log.info(f"⏰ Auto-expired {len(expired_users)} subscriptions.")

                try:
                    backup_db()
                    log.info("✅ Auto backup marker done.")
                except Exception as e:
                    log.warning(f"Auto backup failed: {e}")

                await check_expiry_notifications(application)

                removed = cleanup_temp_files()
                if removed:
                    log.info(f"🗑️ Cleaned up {removed} temp files.")

        asyncio.create_task(daily_job())

    app.post_init = post_init
    log.info("🚀 Data Scanner Bot v5.4-PG running with PostgreSQL...")

    webhook_url = os.environ.get("WEBHOOK_URL", "").strip()
    if webhook_url:
        port = int(os.environ.get("PORT", 8443))
        log.info(f"🌐 Webhook mode: {webhook_url} on port {port}")
        app.run_webhook(listen="0.0.0.0", port=port, webhook_url=webhook_url)
    else:
        log.info("🔄 Polling mode")
        app.run_polling()

if __name__ == "__main__":
    main()
