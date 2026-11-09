"""
Privacy.com.br Scraper & Downloader - Consolidated Edition
All-in-one: sync users, scrape media metadata, download content.

Usage:
    python main.py sync       # Sync following list from API to DB
    python main.py scrape     # Scrape media metadata for all users
    python main.py download   # Download all media (images + videos)
    python main.py rescan     # Re-sync + re-scrape to find new content, then download new items
    python main.py chat       # Scrape + download all chat/message media
    python main.py chat-dl    # Download-only for chat media
    python main.py status     # Show progress stats
    python main.py all        # Run sync → scrape → download in sequence

Configuration:
    config.json  - download_dir, threads, ffmpeg path, etc.
    auth.json    - jwt_token, cookie_string
"""

import argparse
import json
import os
import re
import sys
import shutil
import sqlite3
import subprocess
import tempfile
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import unquote

import requests


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP tracking + failure log
# ═══════════════════════════════════════════════════════════════════════════════
# Every request goes through http_get/http_post so the last HTTP error of the
# current thread can be reported when a download fails.

_LAST_ERR = threading.local()


def _short_url(url) -> str:
    return str(url).split("?")[0].rstrip("/").split("/")[-1][:60]


def note_error(msg: str):
    _LAST_ERR.msg = msg


def reset_error():
    _LAST_ERR.msg = None


def last_error() -> str:
    return getattr(_LAST_ERR, "msg", None) or "unknown"


def _tracked(fn):
    def wrapper(url, *args, **kwargs):
        try:
            resp = fn(url, *args, **kwargs)
        except Exception as e:
            note_error(f"{type(e).__name__} on {_short_url(url)}")
            raise
        if resp.status_code >= 400:
            note_error(f"HTTP {resp.status_code} on {_short_url(url)}")
        return resp
    return wrapper


http_get = _tracked(requests.get)
http_post = _tracked(requests.post)


class FailureLog:
    """Tab-separated list of media that failed to download in the current run."""
    COLUMNS = ["time", "source", "creator", "type", "media_id", "reason",
               "media_url", "profile_url"]

    def __init__(self):
        self.path = None
        self._lock = threading.Lock()
        self.count = 0

    def start(self, path: str):
        """Begin a new run: the previous run's file is replaced."""
        self.path = path
        if os.path.exists(path):
            os.remove(path)

    def record(self, **fields):
        if not self.path:
            return
        fields.setdefault("time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        fields.setdefault("reason", last_error())
        row = "\t".join(str(fields.get(c) or "").replace("\t", " ").replace("\n", " ")
                        for c in self.COLUMNS)
        with self._lock:
            new = not os.path.exists(self.path)
            with open(self.path, "a", encoding="utf-8") as f:
                if new:
                    f.write("\t".join(self.COLUMNS) + "\n")
                f.write(row + "\n")
            self.count += 1


FAILURES = FailureLog()


# ═══════════════════════════════════════════════════════════════════════════════
# Configuration Loader
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_CONFIG = {
    "db_path": "../privacy_users.db",
    "download_dir": "./downloads",
    "num_threads": 12,
    "scrape_threads": 6,
    "scrape_max_pages": 9999,
    "progress_report_interval": 300,
    "ffmpeg_path": "ffmpeg",
}


# Creators never scraped/downloaded (matched case-insensitively against
# nickname or profile_name). Filled from config["skip_users"] in main().
SKIP_USERS: set = set()


def is_skipped(*names) -> bool:
    return any(n and str(n).strip().lower() in SKIP_USERS for n in names)


def load_config(path="config.json") -> dict:
    """Load settings from config.json, creating a default one if it doesn't exist."""
    if not os.path.exists(path):
        print(f"[INIT] {path} not found — creating default config...")
        with open(path, "w") as f:
            json.dump(DEFAULT_CONFIG, f, indent=4)
            f.write("\n")
        print(f"[INIT] Created {path}. Edit it to set download_dir, ffmpeg_path, etc.")
    with open(path, "r") as f:
        return json.load(f)


def load_auth(path="auth.json") -> dict:
    """Load credentials from auth.json"""
    if not os.path.exists(path):
        print(f"ERROR: {path} not found. Create it with jwt_token and cookie_string.")
        sys.exit(1)
    with open(path, "r") as f:
        data = json.load(f)
    if not data.get("jwt_token") or data["jwt_token"] == "YOUR_JWT_TOKEN_HERE":
        print("ERROR: auth.json has placeholder JWT. Update it with a real token.")
        sys.exit(1)
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# Database
# ═══════════════════════════════════════════════════════════════════════════════

class Database:
    def __init__(self, db_path: str, verbose: bool = True):
        self.db_path = db_path
        self.verbose = verbose
        # Ensure parent directory exists so SQLite can create the file
        db_dir = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(db_dir, exist_ok=True)
        self._init()

    def _init(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS users (
            aid TEXT PRIMARY KEY,
            uid INTEGER,
            ucid INTEGER,
            nickname TEXT,
            profile_name TEXT UNIQUE,
            profile_url TEXT,
            is_verified BOOLEAN,
            is_free BOOLEAN,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            still_following BOOLEAN DEFAULT 1,
            notes TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_aid TEXT,
            media_id TEXT UNIQUE,
            media_type TEXT,
            media_url TEXT,
            thumbnail_url TEXT,
            title TEXT,
            description TEXT,
            posted_date TIMESTAMP,
            is_locked BOOLEAN,
            price REAL,
            discovered_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            downloaded BOOLEAN DEFAULT 0,
            FOREIGN KEY (user_aid) REFERENCES users(aid)
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS sync_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            users_found INTEGER,
            new_users INTEGER,
            removed_users INTEGER,
            media_found INTEGER,
            new_media INTEGER
        )''')
        # Chat tables
        c.execute('''CREATE TABLE IF NOT EXISTS chat_rooms (
            room_id TEXT PRIMARY KEY,
            recipient_id INTEGER,
            recipient_name TEXT,
            recipient_profile TEXT,
            media_count INTEGER DEFAULT 0,
            last_scraped TIMESTAMP,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS chat_media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT,
            media_id TEXT UNIQUE,
            media_type TEXT,
            sender_id TEXT,
            sent_at TIMESTAMP,
            is_locked BOOLEAN DEFAULT 0,
            price REAL DEFAULT 0,
            discovered_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            downloaded BOOLEAN DEFAULT 0,
            FOREIGN KEY (room_id) REFERENCES chat_rooms(room_id)
        )''')
        conn.commit()
        conn.close()
        if self.verbose:
            print("[DB] Initialized")

    def add_or_update_user(self, user_data: dict) -> tuple:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        aid = user_data.get("aid")
        nickname = user_data.get("nickName", user_data.get("nickname", "Unknown"))
        profile_name = user_data.get("profileName", user_data.get("username", ""))
        profile_url = f"https://privacy.com.br/profile/{profile_name}" if profile_name else ""

        c.execute("SELECT aid FROM users WHERE aid = ?", (aid,))
        exists = c.fetchone() is not None

        if exists:
            c.execute('''UPDATE users SET nickname=?, profile_name=?, profile_url=?,
                is_verified=?, is_free=?, last_seen=CURRENT_TIMESTAMP, still_following=1,
                uid=?, ucid=? WHERE aid=?''',
                (nickname, profile_name, profile_url,
                 user_data.get("verified", False), user_data.get("isFree", False),
                 user_data.get("uid"), user_data.get("ucid"), aid))
            is_new = False
        else:
            c.execute('''INSERT INTO users (aid,uid,ucid,nickname,profile_name,profile_url,is_verified,is_free)
                VALUES (?,?,?,?,?,?,?,?)''',
                (aid, user_data.get("uid"), user_data.get("ucid"),
                 nickname, profile_name, profile_url,
                 user_data.get("verified", False), user_data.get("isFree", False)))
            is_new = True

        conn.commit()
        conn.close()
        return is_new, nickname

    def mark_unfollowed(self, current_aids: list) -> int:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        ph = ",".join("?" * len(current_aids))
        c.execute(f"UPDATE users SET still_following=0 WHERE aid NOT IN ({ph}) AND still_following=1", current_aids)
        count = c.rowcount
        conn.commit()
        conn.close()
        return count

    def get_all_users(self, only_following=True) -> list:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        if only_following:
            c.execute("SELECT * FROM users WHERE still_following=1 ORDER BY nickname")
        else:
            c.execute("SELECT * FROM users ORDER BY still_following DESC, nickname")
        rows = [dict(r) for r in c.fetchall()
                if not is_skipped(r["nickname"], r["profile_name"])]
        conn.close()
        return rows

    def add_media(self, data: dict) -> tuple:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        mid = data.get("media_id")
        c.execute("SELECT id FROM media WHERE media_id=?", (mid,))
        if c.fetchone() is None:
            c.execute('''INSERT INTO media (user_aid,media_id,media_type,media_url,thumbnail_url,
                title,description,posted_date,is_locked,price)
                VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (data.get("user_aid"), mid, data.get("media_type"), data.get("media_url"),
                 data.get("thumbnail_url"), data.get("title"), data.get("description"),
                 data.get("posted_date"), data.get("is_locked", False), data.get("price", 0)))
            conn.commit()
            conn.close()
            return True, mid
        conn.close()
        return False, mid

    def add_sync_record(self, stats: dict):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''INSERT INTO sync_history (users_found,new_users,removed_users,media_found,new_media)
            VALUES (?,?,?,?,?)''',
            (stats.get("users_found", 0), stats.get("new_users", 0),
             stats.get("removed_users", 0), stats.get("media_found", 0), stats.get("new_media", 0)))
        conn.commit()
        conn.close()

    def get_stats(self) -> dict:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users WHERE still_following=1")
        following = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM media")
        total_media = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM media WHERE downloaded=1")
        downloaded = c.fetchone()[0]
        c.execute("SELECT COUNT(DISTINCT user_aid) FROM media")
        users_with_media = c.fetchone()[0]
        c.execute("SELECT media_type, COUNT(*) FROM media GROUP BY media_type")
        by_type = dict(c.fetchall())
        conn.close()
        return {
            "following": following,
            "total_media": total_media,
            "downloaded": downloaded,
            "remaining": total_media - downloaded,
            "users_with_media": users_with_media,
            "by_type": by_type,
        }

    # ── Chat DB methods ────────────────────────────────────────────────────

    def add_chat_room(self, room_data: dict) -> bool:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        rid = room_data["room_id"]
        c.execute("SELECT room_id FROM chat_rooms WHERE room_id=?", (rid,))
        is_new = c.fetchone() is None
        if is_new:
            c.execute('''INSERT INTO chat_rooms (room_id, recipient_id, recipient_name,
                recipient_profile, media_count) VALUES (?,?,?,?,?)''',
                (rid, room_data.get("recipient_id"), room_data.get("recipient_name"),
                 room_data.get("recipient_profile"), room_data.get("media_count", 0)))
        else:
            c.execute('''UPDATE chat_rooms SET recipient_name=?, media_count=?,
                last_scraped=CURRENT_TIMESTAMP WHERE room_id=?''',
                (room_data.get("recipient_name"), room_data.get("media_count", 0), rid))
        conn.commit()
        conn.close()
        return is_new

    def add_chat_media(self, data: dict) -> tuple:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        mid = data["media_id"]
        c.execute("SELECT id FROM chat_media WHERE media_id=?", (mid,))
        if c.fetchone() is None:
            c.execute('''INSERT INTO chat_media (room_id, media_id, media_type, sender_id,
                sent_at, is_locked, price) VALUES (?,?,?,?,?,?,?)''',
                (data.get("room_id"), mid, data.get("media_type"),
                 data.get("sender_id"), data.get("sent_at"),
                 data.get("is_locked", False), data.get("price", 0)))
            conn.commit()
            conn.close()
            return True, mid
        conn.close()
        return False, mid

    def get_chat_stats(self) -> dict:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM chat_rooms")
        rooms = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM chat_media")
        total = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM chat_media WHERE downloaded=1")
        downloaded = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM chat_media WHERE is_locked=0")
        unlocked = c.fetchone()[0]
        c.execute("SELECT media_type, COUNT(*) FROM chat_media GROUP BY media_type")
        by_type = dict(c.fetchall())
        conn.close()
        return {
            "rooms": rooms, "total": total, "downloaded": downloaded,
            "remaining": unlocked - downloaded, "unlocked": unlocked,
            "by_type": by_type,
        }

    def get_chat_rooms(self) -> list:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM chat_rooms ORDER BY recipient_name").fetchall()
        conn.close()
        return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def parse_cookies(cookie_string: str) -> dict:
    cookies = {}
    for part in cookie_string.split("; "):
        if "=" in part:
            k, v = part.split("=", 1)
            cookies[k] = v
    return cookies


def sanitize(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', "_", name).strip(". ")


def extract_file_id(url: str) -> Optional[str]:
    """The video's file_id is the first path segment of the CDN URL:
    older videos use a UUID (video.privacy.com.br/<uuid>/hls/...), newer ones a
    24-hex id (video.privacy.com.br/6aed37cc2403f4122b33270b/hls/...)."""
    m = re.match(r"https?://[^/]+/([0-9a-f]{24}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/",
                 url or "", re.I)
    if m:
        return m.group(1)
    m = re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", url or "", re.I)
    return m.group(0) if m else None


def fmt_size(b: int) -> str:
    for u in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f}{u}"
        b /= 1024
    return f"{b:.1f}TB"


def make_headers(jwt: str) -> dict:
    return {
        "Authorization": f"Bearer {jwt}",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",  # NO brotli
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin": "https://privacy.com.br",
        "Referer": "https://privacy.com.br/",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# JWT Auto-Refresh
# ═══════════════════════════════════════════════════════════════════════════════

def refresh_jwt_from_cookies(cookie_string: str) -> Optional[str]:
    """Load privacy.com.br main page using session cookies and extract a fresh JWT.

    The site embeds the JWT directly in the HTML when loaded with valid ASP.NET
    session cookies.  This lets us refresh the ~6h JWT without opening a browser,
    as long as the session cookies are still valid (they last much longer).

    Returns the new JWT string on success, or None on failure.
    """
    try:
        cookies = parse_cookies(cookie_string)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "https://privacy.com.br/",
        }
        resp = http_get("https://privacy.com.br/", headers=headers, cookies=cookies, timeout=30)
        if resp.status_code != 200:
            return None
        # JWT pattern: standard HS256 header.payload.signature
        jwt_pattern = r'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+'
        matches = re.findall(jwt_pattern, resp.text)
        if not matches:
            return None
        # Return the longest match (most likely the full token)
        return max(matches, key=len)
    except Exception:
        return None


def save_auth(auth: dict, path: str = "auth.json"):
    """Write updated credentials back to auth.json."""
    with open(path, "w") as f:
        json.dump(auth, f, indent=4)
        f.write("\n")


# ═══════════════════════════════════════════════════════════════════════════════
# Command: sync
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_sync(config: dict, auth: dict):
    """Sync the following list from the API into the database."""
    db = Database(config["db_path"])
    headers = make_headers(auth["jwt_token"])
    cookies = parse_cookies(auth["cookie_string"])

    print("=" * 80)
    print("SYNC FOLLOWING LIST")
    print("=" * 80)

    all_users = []
    page = 0
    limit = 30

    while True:
        url = f"https://service.privacy.com.br/profile/UserFollowing?page={page}&limit={limit}&nickName="
        print(f"  Fetching page {page}...")
        try:
            resp = http_get(url, headers=headers, cookies=cookies, timeout=30)
            if resp.status_code != 200:
                print(f"  HTTP {resp.status_code} — stopping")
                break
            users = resp.json()
            if not users:
                break
            all_users.extend(users)
            print(f"  Got {len(users)} users")
            if len(users) < limit:
                break
            page += 1
        except Exception as e:
            print(f"  Error: {e}")
            break

    new_count = 0
    for u in all_users:
        is_new, nick = db.add_or_update_user(u)
        if is_new:
            print(f"  NEW: {nick}")
            new_count += 1

    unfollowed = db.mark_unfollowed([u["aid"] for u in all_users])
    db.add_sync_record({
        "users_found": len(all_users),
        "new_users": new_count,
        "removed_users": unfollowed,
    })

    print(f"\n  Total: {len(all_users)} users, {new_count} new, {unfollowed} unfollowed")
    print()


# ═══════════════════════════════════════════════════════════════════════════════
# Command: scrape
# ═══════════════════════════════════════════════════════════════════════════════

class MediaScraper:
    def __init__(self, config: dict, auth: dict):
        self.config = config
        self.auth = auth
        self.auth_path = "auth.json"
        self.headers = make_headers(auth["jwt_token"])
        self.cookies = parse_cookies(auth["cookie_string"])
        self.base_url = "https://service.privacy.com.br/timelinequeries/profile"
        self.token_expired = threading.Event()
        self._refresh_lock = threading.Lock()
        self._lock = threading.Lock()

    def _try_refresh_jwt(self) -> bool:
        """Attempt to refresh the JWT using session cookies. Returns True on success."""
        with self._refresh_lock:
            # Another thread may have already refreshed
            if not self.token_expired.is_set():
                return True
            self._log("  Attempting JWT auto-refresh from cookies...")
            new_jwt = refresh_jwt_from_cookies(self.auth["cookie_string"])
            if new_jwt:
                self.auth["jwt_token"] = new_jwt
                self.headers = make_headers(new_jwt)
                save_auth(self.auth, self.auth_path)
                self.token_expired.clear()
                self._log("  JWT REFRESHED successfully!")
                return True
            else:
                self._log("  JWT refresh FAILED — session cookies may have expired.")
                return False

    def _log(self, msg):
        with self._lock:
            print(msg, flush=True)

    def fetch_profile(self, profile_name: str, max_pages: int) -> Optional[list]:
        posts = []
        page = 0
        limit = 20
        while page < max_pages:
            if self.token_expired.is_set():
                return None
            # The API's first path parameter is an item offset, not a page index
            url = f"{self.base_url}/{page * limit}/{limit}/{profile_name}"
            try:
                resp = http_get(url, cookies=self.cookies, headers=self.headers)
                if resp.status_code == 200:
                    items = resp.json().get("items", [])
                    if not items:
                        break
                    posts.extend(items)
                    if page > 0 and page % 25 == 0:
                        self._log(f"    [{profile_name}] page {page}, {len(posts)} posts...")
                    if len(items) < limit:
                        break
                    page += 1
                    time.sleep(self.config.get("rate_limit_delay", 0.3))
                elif resp.status_code in (401, 403):
                    self.token_expired.set()
                    if self._try_refresh_jwt():
                        # Retry this page with the new token
                        continue
                    return None
                else:
                    break
            except Exception as e:
                self._log(f"  [{profile_name}] error page {page}: {e}")
                break
        return posts

    @staticmethod
    def extract_media(posts: list, user_aid: str) -> list:
        items = []
        for post in posts:
            desc = post.get("description", "")
            pub = post.get("publishedDate")
            price_info = post.get("price", {})
            price = None
            if price_info and isinstance(price_info, dict):
                amt = price_info.get("amount")
                sym = price_info.get("currencySymbol", "$")
                if amt:
                    price = f"{sym}{amt}"
            for m in post.get("medias", []):
                mtype = m.get("type", "unknown")
                if m.get("isLocked", True) and "url" not in m:
                    continue
                items.append({
                    "user_aid": user_aid,
                    "media_id": m.get("mediaId", post.get("postId")),
                    "media_type": "photo" if mtype == "image" else "video" if mtype == "video" else mtype,
                    "media_url": m.get("url"),
                    "thumbnail_url": m.get("thumbUrl"),
                    "title": desc[:200] if desc else None,
                    "description": desc,
                    "posted_date": pub,
                    "is_locked": m.get("isLocked", True),
                    "price": price,
                })
        return items

    def _fetch_user(self, user, idx, total, max_pages):
        if self.token_expired.is_set():
            return None
        posts = self.fetch_profile(user["profile_name"], max_pages)
        if posts is None:
            return None
        media = self.extract_media(posts, user["aid"])
        unlocked = sum(1 for m in media if not m["is_locked"])
        locked = len(media) - unlocked
        self._log(f"  [{idx}/{total}] {user['nickname']} — {len(media)} media ({unlocked} unlocked, {locked} locked)")
        return {"user": user, "media": media}

    def run(self):
        db = Database(self.config["db_path"])
        users = db.get_all_users(only_following=True)
        total = len(users)
        max_pages = self.config.get("scrape_max_pages", 9999)
        threads = self.config.get("scrape_threads", 6)

        print("=" * 80)
        print(f"SCRAPE MEDIA METADATA ({threads} threads, max {max_pages} pages/user)")
        print("=" * 80)
        print(f"  Users: {total}")
        print()

        total_new = 0
        completed = 0

        with ThreadPoolExecutor(max_workers=threads) as pool:
            futs = {pool.submit(self._fetch_user, u, i, total, max_pages): u
                    for i, u in enumerate(users, 1)}
            for fut in as_completed(futs):
                result = fut.result()
                completed += 1
                if result is None:
                    if self.token_expired.is_set():
                        if not self._try_refresh_jwt():
                            print("\n  TOKEN EXPIRED & REFRESH FAILED — stopping scrape")
                            pool.shutdown(wait=False, cancel_futures=True)
                            break
                    continue
                new_count = 0
                for m in result["media"]:
                    is_new, _ = db.add_media(m)
                    if is_new:
                        new_count += 1
                total_new += new_count
                if new_count > 0:
                    print(f"    -> {new_count} new items for {result['user']['nickname']}", flush=True)
                if completed % 10 == 0:
                    print(f"\n  --- {completed}/{total} users, {total_new} new media ---\n", flush=True)

        print(f"\n  Done: {completed}/{total} users, {total_new} new media items")
        print()


def cmd_scrape(config: dict, auth: dict):
    scraper = MediaScraper(config, auth)
    scraper.run()


# ═══════════════════════════════════════════════════════════════════════════════
# Command: rescan
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_rescan(config: dict, auth: dict):
    """Re-sync following list + re-scrape all users to find new content,
    then download any newly discovered items."""
    db = Database(config["db_path"], verbose=False)

    # --- Snapshot before ---
    before = db.get_stats()
    before_total = before["total_media"]
    before_dl = before["downloaded"]

    # Per-user counts before
    conn = sqlite3.connect(config["db_path"])
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT user_aid, COUNT(*) as cnt FROM media GROUP BY user_aid").fetchall()
    conn.close()
    before_per_user = {r["user_aid"]: r["cnt"] for r in rows}

    print("=" * 80)
    print("RESCAN — finding new content")
    print("=" * 80)
    print(f"  Before: {before_total} media ({before_dl} downloaded, {before['remaining']} remaining)")
    print()

    # --- Step 1: Sync following list ---
    print("[1/3] Syncing following list...")
    cmd_sync(config, auth)

    # --- Step 2: Scrape all users ---
    print("[2/3] Scraping all users for new media...")
    cmd_scrape(config, auth)

    # --- Snapshot after ---
    after = db.get_stats()
    after_total = after["total_media"]
    new_items = after_total - before_total

    # Per-user delta
    conn = sqlite3.connect(config["db_path"])
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT user_aid, COUNT(*) as cnt FROM media GROUP BY user_aid").fetchall()
    conn.close()
    after_per_user = {r["user_aid"]: r["cnt"] for r in rows}

    # Get user nicknames
    conn = sqlite3.connect(config["db_path"])
    conn.row_factory = sqlite3.Row
    user_rows = conn.execute("SELECT aid, nickname FROM users").fetchall()
    conn.close()
    nicknames = {r["aid"]: r["nickname"] for r in user_rows}

    print()
    print("=" * 80)
    print("RESCAN RESULTS")
    print("=" * 80)
    print(f"  Before:     {before_total} media")
    print(f"  After:      {after_total} media")
    print(f"  New items:  {new_items}")
    print(f"  Remaining:  {after['remaining']} (not downloaded)")
    print()

    if new_items > 0:
        print("  New items per user:")
        for aid in sorted(after_per_user.keys(), key=lambda a: nicknames.get(a, "").lower()):
            delta = after_per_user.get(aid, 0) - before_per_user.get(aid, 0)
            if delta > 0:
                print(f"    {nicknames.get(aid, aid):<30} +{delta}")
        print()

        # --- Step 3: Download new items ---
        print("[3/3] Downloading new/remaining items...")
        cmd_retry(config, auth)
    else:
        print("  No new content found — everything is up to date!")
    print()


# ═══════════════════════════════════════════════════════════════════════════════
# Command: pipeline  (scrape + download interleaved per user)
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_pipeline(config: dict, auth: dict):
    """Sync following list, then scrape each user and immediately download
    their new media before moving to the next.  Much more efficient than the
    old sync → scrape-all → download-all approach."""

    # ── Step 1: sync ──────────────────────────────────────────────────────
    cmd_sync(config, auth)

    # ── Step 2: scrape + download per user ────────────────────────────────
    db = Database(config["db_path"], verbose=False)
    scraper = MediaScraper(config, auth)
    dl = MediaDownloader(config, auth)

    users = db.get_all_users(only_following=True)
    total_users = len(users)
    max_pages = config.get("scrape_max_pages", 9999)
    scrape_threads = config.get("scrape_threads", 6)

    print("=" * 80)
    print(f"PIPELINE — SCRAPE + DOWNLOAD  ({scrape_threads} scrape threads, "
          f"{dl.num_threads} download threads)")
    print("=" * 80)
    print(f"  Users:       {total_users}")
    print(f"  Destination: {dl.download_dir}")
    print(f"  ffmpeg:      {dl.ffmpeg}")
    print()

    os.makedirs(dl.download_dir, exist_ok=True)

    # Stats
    start = time.time()
    _lock = threading.Lock()
    totals = {
        "users_done": 0, "new_media": 0, "dl_ok": 0, "dl_fail": 0,
        "photos_dl": 0, "videos_dl": 0, "bytes": 0,
    }

    # We keep a download pool alive for the whole run.
    # Scrape threads submit download work into it.
    dl_pool = ThreadPoolExecutor(max_workers=dl.num_threads)
    dl_futures: list = []

    def _pipeline_one_user(user, idx):
        """Scrape one user, insert to DB, download new items immediately."""
        if scraper.token_expired.is_set():
            return

        # -- Scrape --
        posts = scraper.fetch_profile(user["profile_name"], max_pages)
        if posts is None:
            return
        media_items = scraper.extract_media(posts, user["aid"])
        unlocked = sum(1 for m in media_items if not m["is_locked"])
        locked = len(media_items) - unlocked
        scraper._log(f"  [{idx}/{total_users}] {user['nickname']} — "
                      f"{len(media_items)} media ({unlocked} unlocked, {locked} locked)")

        # -- Insert to DB --
        new_items = []
        for m in media_items:
            is_new, _ = db.add_media(m)
            if is_new:
                new_items.append(m)

        with _lock:
            totals["new_media"] += len(new_items)

        if new_items:
            scraper._log(f"    -> {len(new_items)} new items for {user['nickname']}")

        # -- Download new + any previously-failed items for this user --
        conn = sqlite3.connect(config["db_path"])
        conn.row_factory = sqlite3.Row
        rows = conn.execute('''
            SELECT * FROM media WHERE user_aid=? AND downloaded=0
              AND is_locked=0 AND media_url IS NOT NULL
              AND media_type IN ('photo', 'video')
        ''', (user["aid"],)).fetchall()
        conn.close()
        pending = [dict(r) for r in rows]

        if not pending:
            with _lock:
                totals["users_done"] += 1
            return

        nick = user["nickname"]
        folder = os.path.join(dl.download_dir, sanitize(nick))
        os.makedirs(folder, exist_ok=True)

        def _dl_item(item, item_idx, item_total):
            if dl.token_expired.is_set():
                return None
            if dl._is_dl(db, item["media_id"]):
                dl._stat("photos_skip" if item["media_type"] == "photo" else "videos_skip")
                return True
            ok = dl._process(item, folder, nick, dl._tid(),
                             item_idx, item_total, idx, total_users)
            if ok:
                dl._mark_dl(db, item["media_id"])
                with _lock:
                    totals["dl_ok"] += 1
            else:
                with _lock:
                    totals["dl_fail"] += 1
            return ok

        # Submit downloads for this user into the shared download pool
        for i, item in enumerate(pending, 1):
            f = dl_pool.submit(_dl_item, item, i, len(pending))
            with _lock:
                dl_futures.append(f)

        with _lock:
            totals["users_done"] += 1

    # --- Run scrape threads (each scrapes → downloads) ---
    with ThreadPoolExecutor(max_workers=scrape_threads) as scrape_pool:
        futs = {scrape_pool.submit(_pipeline_one_user, u, i): u
                for i, u in enumerate(users, 1)}
        completed = 0
        for fut in as_completed(futs):
            completed += 1
            try:
                fut.result()
            except Exception as e:
                user = futs[fut]
                scraper._log(f"  ERROR {user['nickname']}: {e}")

            if scraper.token_expired.is_set():
                if not scraper._try_refresh_jwt():
                    print("\n  TOKEN EXPIRED & REFRESH FAILED — stopping.")
                    scrape_pool.shutdown(wait=False, cancel_futures=True)
                    break

            if completed % 10 == 0:
                elapsed = time.time() - start
                with _lock:
                    scraper._log(f"\n  --- {completed}/{total_users} users scraped | "
                                 f"+{totals['new_media']} new | "
                                 f"{totals['dl_ok']} downloaded / {totals['dl_fail']} failed | "
                                 f"{elapsed:.0f}s ---\n")

    # --- Wait for remaining downloads to finish ---
    scraper._log("\n  Scraping complete — waiting for remaining downloads...")
    with _lock:
        pending_futs = list(dl_futures)
    for f in pending_futs:
        try:
            f.result()
        except Exception:
            pass
    dl_pool.shutdown(wait=True)

    elapsed = time.time() - start
    print()
    print("=" * 80)
    print("PIPELINE COMPLETE")
    print("=" * 80)
    print(f"  Users:      {totals['users_done']}/{total_users}")
    print(f"  New media:  {totals['new_media']}")
    print(f"  Downloaded: {totals['dl_ok']}")
    print(f"  Failed:     {totals['dl_fail']}")
    print(f"  Photos:     {dl.stats['photos_dl']} dl / {dl.stats['photos_skip']} skip / {dl.stats['photos_fail']} fail")
    print(f"  Videos:     {dl.stats['videos_dl']} dl / {dl.stats['videos_skip']} skip / {dl.stats['videos_fail']} fail")
    print(f"  Data:       {fmt_size(dl.stats['bytes'])}")
    print(f"  Time:       {elapsed:.0f}s ({elapsed/60:.1f}m)")
    print()


# ═══════════════════════════════════════════════════════════════════════════════
# Command: download
# ═══════════════════════════════════════════════════════════════════════════════

class MediaDownloader:
    def __init__(self, config: dict, auth: dict):
        self.config = config
        self.auth = auth
        self.auth_path = "auth.json"
        self.cookies = parse_cookies(auth["cookie_string"])
        self.download_dir = config["download_dir"]
        self.ffmpeg = config.get("ffmpeg_path", "ffmpeg")
        self.num_threads = config.get("num_threads", 8)
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay", 2)
        self.progress_interval = config.get("progress_report_interval", 300)

        self.token_expired = threading.Event()
        self._refresh_lock = threading.Lock()
        self._print_lock = threading.Lock()
        self._db_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self._tid_lock = threading.Lock()
        self._tids: dict = {}
        self._next_tid = 1

        # Global progress tracking
        self._global_done = 0  # items downloaded this session
        self._global_skipped = 0  # items skipped (already done)
        self._global_total = 0  # total items across all users
        self._global_already = 0  # already downloaded before this run
        self._thread_status: Dict[int, str] = {}  # tid -> current status

        self.api_headers = {
            "Authorization": f"Bearer {auth['jwt_token']}",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Origin": "https://privacy.com.br",
            "Referer": "https://privacy.com.br/",
        }
        self.cdn_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://privacy.com.br/",
        }

        # Try to refresh JWT upfront if it looks expired
        self._try_refresh_jwt_if_needed()

        self.stats = {
            "photos_dl": 0, "photos_skip": 0, "photos_fail": 0,
            "videos_dl": 0, "videos_skip": 0, "videos_fail": 0,
            "bytes": 0,
        }

    def _try_refresh_jwt_if_needed(self):
        """Check if the JWT is expired and try to refresh it proactively."""
        import base64
        try:
            jwt = self.auth["jwt_token"]
            payload = jwt.split(".")[1]
            # Add padding
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += "=" * padding
            data = json.loads(base64.urlsafe_b64decode(payload))
            exp = data.get("exp", 0)
            remaining = exp - int(time.time())
            if remaining < 300:  # Less than 5 min left or already expired
                with self._print_lock:
                    print(f"  JWT {'expired' if remaining <= 0 else f'expires in {remaining}s'} — attempting auto-refresh...")
                self.token_expired.set()
                self._try_refresh_jwt()
            else:
                hours = remaining // 3600
                mins = (remaining % 3600) // 60
                with self._print_lock:
                    print(f"  JWT valid for {hours}h {mins}m")
        except Exception:
            pass  # Can't decode JWT, skip proactive check

    def _try_refresh_jwt(self) -> bool:
        """Attempt to refresh the JWT using session cookies. Returns True on success."""
        with self._refresh_lock:
            # Another thread may have already refreshed
            if not self.token_expired.is_set():
                return True
            self.log("Attempting JWT auto-refresh from cookies...", tid=0)
            new_jwt = refresh_jwt_from_cookies(self.auth["cookie_string"])
            if new_jwt:
                self.auth["jwt_token"] = new_jwt
                self.api_headers["Authorization"] = f"Bearer {new_jwt}"
                save_auth(self.auth, self.auth_path)
                self.token_expired.clear()
                self.log("JWT REFRESHED successfully!", tid=0)
                return True
            else:
                self.log("JWT refresh FAILED — session cookies may have expired.", tid=0)
                return False

    def _tid(self) -> int:
        name = threading.current_thread().name
        with self._tid_lock:
            if name not in self._tids:
                self._tids[name] = self._next_tid
                self._next_tid += 1
            return self._tids[name]

    def log(self, msg, tid=None):
        with self._print_lock:
            ts = datetime.now().strftime("%H:%M:%S")
            t = tid or self._tid()
            done_total = self._global_already + self._global_done
            pct = done_total / self._global_total * 100 if self._global_total else 0
            print(f"  [{ts}] [T{t:>2}] [{done_total}/{self._global_total} {pct:.1f}%] {msg}", flush=True)

    def _stat(self, key, val=1):
        with self._stats_lock:
            self.stats[key] += val
            if key in ("photos_dl", "videos_dl"):
                self._global_done += 1
            elif key in ("photos_skip", "videos_skip"):
                self._global_skipped += 1

    def _mark_dl(self, db, media_id):
        with self._db_lock:
            conn = sqlite3.connect(db.db_path)
            conn.execute("UPDATE media SET downloaded=1 WHERE media_id=?", (media_id,))
            conn.commit()
            conn.close()

    def _is_dl(self, db, media_id) -> bool:
        with self._db_lock:
            conn = sqlite3.connect(db.db_path)
            row = conn.execute("SELECT downloaded FROM media WHERE media_id=?", (media_id,)).fetchone()
            conn.close()
            return row and row[0] == 1

    # ── Image ─────────────────────────────────────────────────────────────

    def _dl_image(self, url, dest, tid):
        for attempt in range(self.max_retries):
            try:
                resp = http_get(url, headers=self.cdn_headers, stream=True, timeout=60)
                if resp.status_code == 200:
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    with open(dest, "wb") as f:
                        for chunk in resp.iter_content(8192):
                            f.write(chunk)
                    self._stat("bytes", os.path.getsize(dest))
                    return True
                elif resp.status_code in (401, 403):
                    return False
            except Exception:
                pass
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay * (attempt + 1))
        return False

    # ── Video token ───────────────────────────────────────────────────────

    def _video_token(self, file_id, tid):
        for attempt in range(self.max_retries):
            try:
                resp = http_post(
                    "https://service.privacy.com.br/mediavideotoken/generate",
                    json={"file_id": file_id},
                    cookies=self.cookies,
                    headers=self.api_headers,
                    timeout=15,
                )
                if resp.status_code == 200:
                    return resp.json().get("content", "")
                elif resp.status_code == 401:
                    self.log("JWT expired — trying auto-refresh...", tid)
                    self.token_expired.set()
                    if self._try_refresh_jwt():
                        continue  # Retry with new token
                    return None
            except Exception:
                pass
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        return None

    # ── HLS authenticated GET ─────────────────────────────────────────────

    def _hls_get(self, url, token, tid, stream=False):
        filename = url.split("/")[-1].split("?")[0]
        headers = {**self.cdn_headers, "content": token, "x-content-uri": filename}
        for attempt in range(self.max_retries):
            try:
                resp = http_get(url, headers=headers, stream=stream, timeout=60)
                if resp.status_code == 200:
                    return resp
            except Exception:
                pass
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay * (attempt + 1))
        return None

    # ── Video download (HLS or direct) ────────────────────────────────────

    def _dl_video(self, url, dest, tid):
        file_id = extract_file_id(url)
        if not file_id:
            note_error("could not extract file_id from URL")
            return False
        token = self._video_token(file_id, tid)
        if not token:
            return False
        base_url = "/".join(url.split("/")[:-1]) + "/"

        resp = self._hls_get(url, token, tid, stream=True)
        if not resp:
            return False

        ct = resp.headers.get("content-type", "").lower()
        if any(t in ct for t in ("video/", "octet-stream", "mp4")):
            self.log("  (direct file)", tid)
            return self._save_direct(resp, dest)

        first = b""
        for chunk in resp.iter_content(1024):
            first = chunk
            break
        if not first:
            resp.close()
            return False

        try:
            text = first.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            text = None

        if text and text.strip().startswith("#EXTM3U"):
            remaining = resp.content
            full = text + remaining.decode("utf-8", errors="replace")
            resp.close()
            return self._dl_hls(full, base_url, token, dest, tid)
        else:
            self.log("  (direct file, binary)", tid)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            total = 0
            with open(dest, "wb") as f:
                f.write(first)
                total += len(first)
                for chunk in resp.iter_content(65536):
                    f.write(chunk)
                    total += len(chunk)
            resp.close()
            self._stat("bytes", total)
            return total > 0

    def _save_direct(self, resp, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        total = 0
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(65536):
                f.write(chunk)
                total += len(chunk)
        self._stat("bytes", total)
        return total > 0

    def _dl_hls(self, master, base_url, token, dest, tid):
        # Pick best quality
        streams = []
        lines = master.strip().split("\n")
        for i, line in enumerate(lines):
            if line.startswith("#EXT-X-STREAM-INF:"):
                bw = re.search(r"BANDWIDTH=(\d+)", line)
                if bw and i + 1 < len(lines):
                    streams.append((int(bw.group(1)), lines[i + 1].strip()))

        if not streams:
            quality = master
        else:
            streams.sort(key=lambda x: x[0], reverse=True)
            resp2 = self._hls_get(base_url + streams[0][1], token, tid)
            if not resp2:
                return False
            quality = resp2.text

        segments = []
        key_url = None
        for line in quality.split("\n"):
            line = line.strip()
            if line.startswith("#EXT-X-KEY:"):
                km = re.search(r'URI="([^"]+)"', line)
                if km:
                    key_url = km.group(1)
            elif line and not line.startswith("#"):
                segments.append(line)

        if not segments:
            note_error("HLS playlist has no segments")
            return False

        tmp = tempfile.mkdtemp(prefix="priv_hls_")
        try:
            # AES key
            local_key = None
            if key_url:
                rk = self._hls_get(key_url, token, tid)
                if rk:
                    local_key = os.path.join(tmp, "key.aes")
                    with open(local_key, "wb") as f:
                        f.write(rk.content)
                else:
                    return False

            # Segments
            total_bytes = 0
            for idx, seg in enumerate(segments, 1):
                if self.token_expired.is_set():
                    return False
                rs = self._hls_get(base_url + seg, token, tid, stream=True)
                if not rs:
                    return False
                sp = os.path.join(tmp, seg)
                sb = 0
                with open(sp, "wb") as f:
                    for chunk in rs.iter_content(65536):
                        f.write(chunk)
                        sb += len(chunk)
                total_bytes += sb
                if idx % 10 == 0 or idx == len(segments):
                    self.log(f"  segments {idx}/{len(segments)} ({fmt_size(total_bytes)})", tid)

            self._stat("bytes", total_bytes)

            # Local m3u8
            local_m3u8 = os.path.join(tmp, "local.m3u8")
            content = quality
            if key_url and local_key:
                content = content.replace(key_url, local_key.replace("\\", "/"))
            for seg in segments:
                content = content.replace(seg, os.path.join(tmp, seg).replace("\\", "/"))
            with open(local_m3u8, "w") as f:
                f.write(content)

            # ffmpeg remux
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            tmp_out = dest + ".tmp.mp4"
            result = subprocess.run(
                [self.ffmpeg, "-y", "-allowed_extensions", "ALL",
                 "-i", local_m3u8, "-c", "copy", "-movflags", "+faststart", tmp_out],
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode == 0 and os.path.exists(tmp_out):
                shutil.move(tmp_out, dest)
                return True
            else:
                self.log(f"  ffmpeg error: {result.stderr[-200:]}", tid)
                note_error("ffmpeg failed: " + result.stderr.strip().splitlines()[-1][:150] if result.stderr.strip() else "ffmpeg failed")
                if os.path.exists(tmp_out):
                    os.remove(tmp_out)
                return False
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    # ── Process one media item ────────────────────────────────────────────

    def _process(self, media, folder, nick, tid, idx, total, uidx, utotal):
        if self.token_expired.is_set():
            return False  # run is stopping — not a failure of this item
        reset_error()
        ok = self._process_item(media, folder, nick, tid, idx, total, uidx, utotal)
        if not ok:
            reason = last_error()
            self.log(f"[{uidx}/{utotal}] [{idx}/{total}] FAIL {media['media_type']}: "
                     f"{nick}/{media['media_id']} — {reason}", tid)
            FAILURES.record(source="timeline", creator=nick, type=media["media_type"],
                            media_id=media["media_id"], reason=reason,
                            media_url=media.get("media_url"),
                            profile_url=self._profile_url(media.get("user_aid"), nick))
        return ok

    def _profile_url(self, user_aid, nick):
        try:
            conn = sqlite3.connect(self.config["db_path"])
            row = conn.execute("SELECT profile_url FROM users WHERE aid=?", (user_aid,)).fetchone()
            conn.close()
            if row and row[0]:
                return row[0]
        except Exception:
            pass
        return f"https://privacy.com.br/profile/{nick}"

    def _process_item(self, media, folder, nick, tid, idx, total, uidx, utotal):
        mid = media["media_id"]
        mtype = media["media_type"]
        url = media["media_url"]
        if not url:
            note_error("no media_url in database")
            return False

        ext = {"photo": ".jpg", "video": ".mp4", "audio": ".mp3"}.get(mtype, "")
        fname = f"{mid}{ext}"
        dest = os.path.join(folder, fname)

        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            self._stat("photos_skip" if mtype == "photo" else "videos_skip")
            return True

        tag = f"[{uidx}/{utotal}] [{idx}/{total}]"
        if mtype == "photo":
            with self._stats_lock:
                self._thread_status[tid] = f"DL photo: {nick}/{fname}"
            self.log(f"{tag} DL photo: {nick}/{fname}", tid)
            ok = self._dl_image(url, dest, tid)
            if ok:
                sz = os.path.getsize(dest)
                self.log(f"{tag} OK photo: {nick}/{fname} ({fmt_size(sz)})", tid)
                self._stat("photos_dl")
            else:
                self._stat("photos_fail")
            return ok
        elif mtype == "video":
            with self._stats_lock:
                self._thread_status[tid] = f"DL video: {nick}/{fname}"
            self.log(f"{tag} DL video: {nick}/{fname}", tid)
            ok = self._dl_video(url, dest, tid)
            if ok:
                sz = os.path.getsize(dest)
                self.log(f"{tag} OK video: {nick}/{fname} ({fmt_size(sz)})", tid)
                self._stat("videos_dl")
            else:
                self._stat("videos_fail")
            return ok
        return False

    # ── Process all items for one user ────────────────────────────────────

    def _process_user(self, user, items, uidx, utotal, db):
        tid = self._tid()
        nick = user["nickname"]
        folder = os.path.join(self.download_dir, sanitize(nick))
        os.makedirs(folder, exist_ok=True)
        total = len(items)
        ok_count = 0
        self.log(f"[{uidx}/{utotal}] START {nick} ({total} items)", tid)

        for idx, m in enumerate(items, 1):
            if self.token_expired.is_set():
                break
            if self._is_dl(db, m["media_id"]):
                self._stat("photos_skip" if m["media_type"] == "photo" else "videos_skip")
                continue
            ok = self._process(m, folder, nick, tid, idx, total, uidx, utotal)
            if ok:
                self._mark_dl(db, m["media_id"])
                ok_count += 1
            time.sleep(0.2)
        return ok_count

    # ── Orchestrator ──────────────────────────────────────────────────────

    def run(self):
        db = Database(self.config["db_path"], verbose=False)

        print("=" * 80)
        print("PRIVACY.COM.BR MEDIA DOWNLOADER")
        print("=" * 80)
        print(f"  Destination:  {self.download_dir}")
        print(f"  Threads:      {self.num_threads}")
        print(f"  ffmpeg:       {self.ffmpeg}")
        print()

        if not os.path.exists(self.download_dir):
            try:
                os.makedirs(self.download_dir, exist_ok=True)
            except Exception as e:
                print(f"ERROR: Cannot access {self.download_dir}: {e}")
                return

        users = db.get_all_users(only_following=True)
        if not users:
            print("No users in DB")
            return

        queue = []
        total_media = 0
        for u in users:
            conn = sqlite3.connect(db.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM media WHERE user_aid=? AND is_locked=0 AND media_url IS NOT NULL ORDER BY media_type, posted_date",
                (u["aid"],),
            ).fetchall()
            conn.close()
            items = [dict(r) for r in rows]
            if items:
                queue.append((u, items))
                total_media += len(items)

        utotal = len(queue)
        conn = sqlite3.connect(db.db_path)
        already = conn.execute("SELECT COUNT(*) FROM media WHERE downloaded=1").fetchone()[0]
        conn.close()

        self._global_total = total_media
        self._global_already = already
        self._global_done = 0
        self._global_skipped = 0

        print(f"  Users with media: {utotal}")
        print(f"  Total media:      {total_media}")
        print(f"  Already done:     {already}")
        print(f"  Remaining:        {total_media - already}")
        print()
        print("-" * 80)
        print()

        start = time.time()
        done_users = 0

        # Background progress reporter — prints every 60s
        def progress_reporter():
            while not self.token_expired.is_set():
                time.sleep(60)
                if self.token_expired.is_set():
                    break
                elapsed = time.time() - start
                done_total = self._global_already + self._global_done
                pct = done_total / self._global_total * 100 if self._global_total else 0
                rate = self._global_done / elapsed * 3600 if elapsed > 0 else 0
                eta_items = self._global_total - done_total
                eta_sec = eta_items / (self._global_done / elapsed) if self._global_done > 0 else 0
                eta_str = f"{eta_sec/3600:.1f}h" if eta_sec > 3600 else f"{eta_sec/60:.0f}m"
                with self._print_lock:
                    print(f"\n{'='*80}", flush=True)
                    print(f"  PROGRESS  [{done_total}/{self._global_total}] {pct:.1f}%  |  "
                          f"This session: +{self._global_done} items ({fmt_size(self.stats['bytes'])})  |  "
                          f"Rate: {rate:.0f}/hr  |  ETA: {eta_str}", flush=True)
                    print(f"  Users: {done_users}/{utotal} done  |  "
                          f"Photos: {self.stats['photos_dl']}dl/{self.stats['photos_skip']}skip/{self.stats['photos_fail']}fail  |  "
                          f"Videos: {self.stats['videos_dl']}dl/{self.stats['videos_skip']}skip/{self.stats['videos_fail']}fail", flush=True)
                    # Show what each thread is doing
                    active = {}
                    with self._stats_lock:
                        active = dict(self._thread_status)
                    if active:
                        print(f"  Active threads:", flush=True)
                        for t in sorted(active.keys()):
                            print(f"    T{t:>2}: {active[t]}", flush=True)
                    print(f"{'='*80}\n", flush=True)

        reporter = threading.Thread(target=progress_reporter, daemon=True)
        reporter.start()

        with ThreadPoolExecutor(max_workers=self.num_threads) as pool:
            futs = {}
            for uidx, (u, items) in enumerate(queue, 1):
                futs[pool.submit(self._process_user, u, items, uidx, utotal, db)] = (u, uidx)

            for fut in as_completed(futs):
                u, uidx = futs[fut]
                done_users += 1
                try:
                    fut.result()
                except Exception as e:
                    with self._print_lock:
                        print(f"  ERROR {u['nickname']}: {e}", flush=True)
                if self.token_expired.is_set():
                    if not self._try_refresh_jwt():
                        print("\n  JWT EXPIRED & REFRESH FAILED — stopping downloads.")
                        pool.shutdown(wait=False, cancel_futures=True)
                        break
                if done_users % 3 == 0:
                    elapsed = time.time() - start
                    done_total = self._global_already + self._global_done
                    pct = done_total / self._global_total * 100 if self._global_total else 0
                    with self._print_lock:
                        print(
                            f"\n  --- [{done_total}/{self._global_total} {pct:.1f}%] {done_users}/{utotal} users | "
                            f"+{self._global_done} new ({fmt_size(self.stats['bytes'])}) | "
                            f"Photos: {self.stats['photos_dl']}dl/{self.stats['photos_fail']}fail | "
                            f"Videos: {self.stats['videos_dl']}dl/{self.stats['videos_fail']}fail | "
                            f"Time: {elapsed:.0f}s ---\n",
                            flush=True,
                        )

        elapsed = time.time() - start
        print()
        print("=" * 80)
        print("DOWNLOAD COMPLETE")
        print("=" * 80)
        print(f"  Users:   {done_users}/{utotal}")
        print(f"  Photos:  {self.stats['photos_dl']} dl / {self.stats['photos_skip']} skip / {self.stats['photos_fail']} fail")
        print(f"  Videos:  {self.stats['videos_dl']} dl / {self.stats['videos_skip']} skip / {self.stats['videos_fail']} fail")
        print(f"  Data:    {fmt_size(self.stats['bytes'])}")
        print(f"  Time:    {elapsed:.0f}s ({elapsed/60:.1f}m)")
        print()

        # Write a progress summary file for CI visibility
        self._write_summary(db, elapsed)

    def _write_summary(self, db, elapsed):
        """Write a machine-readable progress summary (useful for CI/Actions)."""
        stats = db.get_stats()
        summary = {
            "timestamp": datetime.now().isoformat(),
            "elapsed_seconds": round(elapsed),
            "database": stats,
            "session": self.stats,
        }
        path = os.path.join(os.path.dirname(self.config["db_path"]) or ".", "progress.json")
        with open(path, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"  Progress summary: {path}")


def cmd_download(config: dict, auth: dict):
    dl = MediaDownloader(config, auth)
    dl.run()


# ═══════════════════════════════════════════════════════════════════════════════
# Command: retry
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_retry(config: dict, auth: dict):
    """Retry only the failed/remaining downloads (flat queue, all threads)."""
    db = Database(config["db_path"], verbose=False)
    dl = MediaDownloader(config, auth)

    print("=" * 80)
    print("RETRY FAILED DOWNLOADS")
    print("=" * 80)

    # Build a flat list of (user_dict, media_dict) for all un-downloaded photo/video
    conn = sqlite3.connect(config["db_path"])
    conn.row_factory = sqlite3.Row
    rows = conn.execute('''
        SELECT m.*, u.nickname, u.profile_name
        FROM media m
        JOIN users u ON m.user_aid = u.aid
        WHERE m.downloaded = 0
          AND m.is_locked = 0
          AND m.media_url IS NOT NULL
          AND m.media_type IN ('photo', 'video')
        ORDER BY m.media_type, u.nickname
    ''').fetchall()
    conn.close()

    items = [dict(r) for r in rows
             if not is_skipped(r["nickname"], r["profile_name"])]
    if not items:
        print("  Nothing to retry — all downloadable items are done!")
        return

    photos = sum(1 for i in items if i["media_type"] == "photo")
    videos = sum(1 for i in items if i["media_type"] == "video")
    users = len(set(i["user_aid"] for i in items))
    print(f"  Items to retry: {len(items)} ({photos} photos, {videos} videos)")
    print(f"  Across {users} users")
    print(f"  Threads: {dl.num_threads}")
    print()

    dl._global_total = len(items)
    dl._global_already = 0
    dl._global_done = 0
    dl._global_skipped = 0

    start = time.time()
    success = 0
    failed = 0
    failed_items = []

    def retry_one(item, idx, total):
        tid = dl._tid()
        nick = item["nickname"]
        folder = os.path.join(dl.download_dir, sanitize(nick))
        os.makedirs(folder, exist_ok=True)

        if dl.token_expired.is_set():
            return None
        if dl._is_dl(db, item["media_id"]):
            dl._stat("photos_skip" if item["media_type"] == "photo" else "videos_skip")
            return True

        ok = dl._process(item, folder, nick, tid, idx, total, idx, total)
        if ok:
            dl._mark_dl(db, item["media_id"])
        return ok

    total = len(items)
    with ThreadPoolExecutor(max_workers=dl.num_threads) as pool:
        futs = {pool.submit(retry_one, item, idx, total): item
                for idx, item in enumerate(items, 1)}
        for fut in as_completed(futs):
            item = futs[fut]
            try:
                result = fut.result()
                if result is True:
                    success += 1
                elif result is False:
                    failed += 1
                    failed_items.append(item)
                # result is None means token expired
            except Exception as e:
                failed += 1
                failed_items.append(item)
                with dl._print_lock:
                    print(f"  ERROR: {item['nickname']}/{item['media_id']}: {e}", flush=True)

            if dl.token_expired.is_set():
                if not dl._try_refresh_jwt():
                    print("\n  JWT EXPIRED & REFRESH FAILED — stopping retry.")
                    pool.shutdown(wait=False, cancel_futures=True)
                    break

    elapsed = time.time() - start
    print()
    print("=" * 80)
    print("RETRY COMPLETE")
    print("=" * 80)
    print(f"  Success: {success}")
    print(f"  Failed:  {failed}")
    print(f"  Photos:  {dl.stats['photos_dl']} dl / {dl.stats['photos_fail']} fail")
    print(f"  Videos:  {dl.stats['videos_dl']} dl / {dl.stats['videos_fail']} fail")
    print(f"  Data:    {fmt_size(dl.stats['bytes'])}")
    print(f"  Time:    {elapsed:.0f}s ({elapsed/60:.1f}m)")

    if failed_items:
        print(f"\n  Permanently failed items ({len(failed_items)}):")
        for it in failed_items[:50]:
            print(f"    {it['nickname']}: {it['media_type']} {it['media_id']}")
        if len(failed_items) > 50:
            print(f"    ... and {len(failed_items) - 50} more")
    print()


# ═══════════════════════════════════════════════════════════════════════════════
# Command: chat  — Scrape and download chat/message media
# ═══════════════════════════════════════════════════════════════════════════════

CHAT_API = "https://service.privacy.com.br/chat"
MEDIA_API = "https://service.privacy.com.br/media"


def cmd_chat(config: dict, auth: dict):
    """Scrape all chat rooms for media, then download images/videos/audio."""
    db = Database(config["db_path"], verbose=False)
    headers = make_headers(auth["jwt_token"])
    cookies = parse_cookies(auth["cookie_string"])

    # ── Phase 1: Discover rooms ──────────────────────────────────────────
    print("=" * 80)
    print("CHAT MEDIA SCRAPER")
    print("=" * 80)
    print()
    print("[1/3] Discovering chat rooms...")

    rooms = []
    page = 1
    while True:
        resp = http_get(
            f"{CHAT_API}/rooms",
            headers=headers,
            params={"page": page, "filter": "all", "sort": "newest"},
            timeout=15,
        )
        if resp.status_code == 401:
            print("  JWT expired, refreshing...")
            new_jwt = refresh_jwt_from_cookies(auth["cookie_string"])
            if new_jwt:
                auth["jwt_token"] = new_jwt
                headers = make_headers(new_jwt)
                save_auth(auth)
                continue
            else:
                print("  FATAL: Could not refresh JWT.")
                return
        if resp.status_code != 200:
            print(f"  Error fetching rooms page {page}: HTTP {resp.status_code}")
            break
        data = resp.json()
        batch = data.get("rooms", [])
        if not batch:
            break
        rooms.extend(batch)
        print(f"  Page {page}: {len(batch)} rooms (total: {len(rooms)})")
        page += 1
        time.sleep(0.3)

    print(f"  Total rooms: {len(rooms)}")

    # Save rooms to DB
    new_rooms = 0
    for r in rooms:
        recip = r.get("recipient", {})
        is_new = db.add_chat_room({
            "room_id": r["roomId"],
            "recipient_id": recip.get("id"),
            "recipient_name": recip.get("name", recip.get("profileName", "Unknown")),
            "recipient_profile": recip.get("profileName", ""),
        })
        if is_new:
            new_rooms += 1
    print(f"  New rooms: {new_rooms}")

    # ── Phase 2: Scrape media from each room ─────────────────────────────
    print()
    print("[2/3] Scraping media from chat rooms...")

    total_media = 0
    new_media = 0
    for idx, r in enumerate(rooms, 1):
        rid = r["roomId"]
        recip = r.get("recipient", {})
        name = recip.get("name", recip.get("profileName", "Unknown"))
        if is_skipped(name, recip.get("profileName")):
            print(f"  [{idx}/{len(rooms)}] {name}: skipped (skip_users)")
            continue

        # Paginate media using minorSentAt cursor
        room_items = []
        minor_sent_at = None
        while True:
            params = {"roomId": rid}
            if minor_sent_at:
                params["minorSentAt"] = minor_sent_at

            resp = http_get(
                f"{CHAT_API}/medias",
                headers=headers,
                params=params,
                timeout=15,
            )
            if resp.status_code == 401:
                new_jwt = refresh_jwt_from_cookies(auth["cookie_string"])
                if new_jwt:
                    auth["jwt_token"] = new_jwt
                    headers = make_headers(new_jwt)
                    save_auth(auth)
                    continue
                else:
                    print("  FATAL: Could not refresh JWT.")
                    return
            if resp.status_code != 200:
                break

            data = resp.json()
            items = data.get("items", [])
            if not items:
                break

            room_items.extend(items)
            # Use last item's sentAt as cursor for next page
            last_sent = items[-1].get("sentAt")
            if last_sent and last_sent != minor_sent_at:
                minor_sent_at = last_sent
            else:
                break
            time.sleep(0.2)

        if not room_items:
            continue

        # Store media in DB
        room_new = 0
        for item in room_items:
            is_locked = item.get("isLocked", False)
            price = item.get("price", 0)
            sender = item.get("senderId", "")
            sent_at = item.get("sentAt", "")

            for f in item.get("files", []):
                mid = f.get("mediaId", "")
                mtype = f.get("type", "")
                if not mid or not mtype:
                    continue

                is_new, _ = db.add_chat_media({
                    "room_id": rid,
                    "media_id": mid,
                    "media_type": mtype,
                    "sender_id": sender,
                    "sent_at": sent_at,
                    "is_locked": is_locked,
                    "price": price if is_locked else 0,
                })
                total_media += 1
                if is_new:
                    new_media += 1
                    room_new += 1

        # Update room media count
        db.add_chat_room({
            "room_id": rid,
            "recipient_id": recip.get("id"),
            "recipient_name": name,
            "recipient_profile": recip.get("profileName", ""),
            "media_count": len(room_items),
        })

        if room_new > 0:
            print(f"  [{idx}/{len(rooms)}] {name}: {len(room_items)} media items (+{room_new} new)")
        time.sleep(0.2)

    print(f"\n  Total media items: {total_media} ({new_media} new)")

    # ── Phase 3: Download chat media ─────────────────────────────────────
    print()
    print("[3/3] Downloading chat media...")
    _chat_download(config, auth, db)


def _chat_download(config: dict, auth: dict, db: Database = None):
    """Download all un-downloaded, unlocked chat media."""
    if db is None:
        db = Database(config["db_path"], verbose=False)

    download_dir = config["download_dir"]
    ffmpeg = config.get("ffmpeg_path", "ffmpeg")
    num_threads = config.get("num_threads", 8)
    max_retries = config.get("max_retries", 3)
    retry_delay = config.get("retry_delay", 2)

    cookies = parse_cookies(auth["cookie_string"])
    api_headers = make_headers(auth["jwt_token"])
    cdn_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://privacy.com.br/",
    }

    # Locks
    print_lock = threading.Lock()
    db_lock = threading.Lock()
    stats_lock = threading.Lock()
    refresh_lock = threading.Lock()
    token_expired = threading.Event()

    stats = {"images": 0, "videos": 0, "audio": 0, "skip": 0, "fail": 0, "bytes": 0}

    # Check JWT proactively
    import base64 as b64mod
    try:
        payload = auth["jwt_token"].split(".")[1]
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        data = json.loads(b64mod.urlsafe_b64decode(payload))
        remaining = data.get("exp", 0) - int(time.time())
        if remaining < 300:
            print(f"  JWT {'expired' if remaining <= 0 else f'expires in {remaining}s'} — refreshing...")
            new_jwt = refresh_jwt_from_cookies(auth["cookie_string"])
            if new_jwt:
                auth["jwt_token"] = new_jwt
                api_headers = make_headers(new_jwt)
                save_auth(auth)
                print("  JWT refreshed!")
            else:
                print("  WARNING: Could not refresh JWT.")
        else:
            print(f"  JWT valid for {remaining // 3600}h {(remaining % 3600) // 60}m")
    except Exception:
        pass

    # Build download queue
    conn = sqlite3.connect(db.db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute('''
        SELECT cm.*, cr.recipient_name
        FROM chat_media cm
        JOIN chat_rooms cr ON cm.room_id = cr.room_id
        WHERE cm.downloaded = 0 AND cm.is_locked = 0
        ORDER BY cr.recipient_name, cm.media_type
    ''').fetchall()
    conn.close()
    items = [dict(r) for r in rows if not is_skipped(r["recipient_name"])]

    if not items:
        print("  Nothing to download — all chat media is done!")
        return

    images = sum(1 for i in items if i["media_type"] == "image")
    videos = sum(1 for i in items if i["media_type"] == "video")
    audios = sum(1 for i in items if i["media_type"] == "audio")
    print(f"  To download: {len(items)} ({images} images, {videos} videos, {audios} audio)")
    print(f"  Threads: {num_threads}")
    print()

    def try_refresh():
        with refresh_lock:
            if not token_expired.is_set():
                return True
            new_jwt = refresh_jwt_from_cookies(auth["cookie_string"])
            if new_jwt:
                auth["jwt_token"] = new_jwt
                api_headers["Authorization"] = f"Bearer {new_jwt}"
                save_auth(auth)
                token_expired.clear()
                with print_lock:
                    print("  JWT refreshed!", flush=True)
                return True
            return False

    def resolve_chat_media_url(media_id, media_type):
        """Get CDN redirect URL for chat media."""
        url = f"{MEDIA_API}/{media_type}/chat/{media_id}?redirect=true"
        for attempt in range(max_retries):
            try:
                resp = http_get(url, headers=api_headers, allow_redirects=True,
                                    stream=True, timeout=30)
                if resp.status_code == 200:
                    return resp  # Return the response with CDN content
                elif resp.status_code == 401:
                    token_expired.set()
                    if try_refresh():
                        continue
                    return None
                elif resp.status_code == 403:
                    # Try refreshing JWT
                    token_expired.set()
                    if try_refresh():
                        continue
                    return None
            except Exception:
                pass
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
        return None

    def dl_chat_image(media_id, dest):
        """Download a chat image."""
        resp = resolve_chat_media_url(media_id, "image")
        if not resp:
            return False
        try:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            total = 0
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
                    total += len(chunk)
            with stats_lock:
                stats["bytes"] += total
            return total > 0
        except Exception:
            return False
        finally:
            resp.close()

    def dl_chat_audio(media_id, dest):
        """Download a chat audio file."""
        resp = resolve_chat_media_url(media_id, "audio")
        if not resp:
            return False
        try:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            total = 0
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
                    total += len(chunk)
            with stats_lock:
                stats["bytes"] += total
            return total > 0
        except Exception:
            return False
        finally:
            resp.close()

    def dl_chat_video(media_id, dest):
        """Download a chat video — get HLS URL, fetch token, then download."""
        # Step 1: Get redirect URL WITHOUT following (CDN needs token headers)
        url = f"{MEDIA_API}/video/chat/{media_id}?redirect=true"
        hls_url = None
        for attempt in range(max_retries):
            try:
                resp = http_get(url, headers=api_headers,
                                    allow_redirects=False, timeout=30)
                if resp.status_code in (301, 302, 307, 308):
                    hls_url = resp.headers.get("Location", "")
                    break
                elif resp.status_code == 200:
                    # Direct download (rare)
                    ct = resp.headers.get("content-type", "").lower()
                    if any(t in ct for t in ("video/", "octet-stream", "mp4")):
                        os.makedirs(os.path.dirname(dest), exist_ok=True)
                        total = int(resp.headers.get("content-length", 0))
                        with open(dest, "wb") as f:
                            f.write(resp.content)
                        with stats_lock:
                            stats["bytes"] += total
                        return total > 0
                elif resp.status_code in (401, 403):
                    token_expired.set()
                    if try_refresh():
                        continue
                    return False
            except Exception:
                pass
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))

        if not hls_url:
            return False

        # Step 2: Extract file_id and get video token
        file_id = extract_file_id(hls_url)
        if not file_id:
            return False

        token = None
        for attempt in range(max_retries):
            try:
                tr = http_post(
                    "https://service.privacy.com.br/mediavideotoken/generate",
                    json={"file_id": file_id},
                    cookies=cookies,
                    headers=api_headers,
                    timeout=15,
                )
                if tr.status_code == 200:
                    token = tr.json().get("content", "")
                    break
                elif tr.status_code == 401:
                    token_expired.set()
                    if try_refresh():
                        continue
                    return False
            except Exception:
                pass
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

        if not token:
            return False

        # Step 3: Fetch master m3u8 with token headers
        filename = hls_url.split("/")[-1].split("?")[0]
        hls_headers = {**cdn_headers, "content": token, "x-content-uri": filename}
        m3u8_resp = None
        for attempt in range(max_retries):
            try:
                m3u8_resp = http_get(hls_url, headers=hls_headers, timeout=30)
                if m3u8_resp.status_code == 200:
                    break
                m3u8_resp = None
            except Exception:
                pass
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))

        if not m3u8_resp:
            return False

        master_text = m3u8_resp.text
        if not master_text.strip().startswith("#EXTM3U"):
            # Not HLS — try saving as direct file
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(m3u8_resp.content)
            sz = len(m3u8_resp.content)
            with stats_lock:
                stats["bytes"] += sz
            return sz > 0

        # Step 4: HLS download via _dl_chat_hls
        base_url = "/".join(hls_url.split("/")[:-1]) + "/"
        return _dl_chat_hls(master_text, base_url, token, dest, cdn_headers,
                            max_retries, retry_delay, ffmpeg, stats, stats_lock)

    def process_item(item, idx, total):
        if token_expired.is_set():
            if not try_refresh():
                return None

        mid = item["media_id"]
        mtype = item["media_type"]
        name = item.get("recipient_name", "Unknown")
        chat_folder = os.path.join(download_dir, "_chat", sanitize(name))

        ext = {"image": ".jpg", "video": ".mp4", "audio": ".mp3", "gif": ".gif"}.get(mtype, "")
        fname = f"{mid}{ext}"
        dest = os.path.join(chat_folder, fname)

        # Skip if already exists on disk
        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            with stats_lock:
                stats["skip"] += 1
            return True

        # Check DB
        with db_lock:
            conn = sqlite3.connect(db.db_path)
            row = conn.execute("SELECT downloaded FROM chat_media WHERE media_id=?", (mid,)).fetchone()
            conn.close()
            if row and row[0]:
                with stats_lock:
                    stats["skip"] += 1
                return True

        with print_lock:
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] [{idx}/{total}] DL {mtype}: {name}/{fname}", flush=True)

        reset_error()
        ok = False
        if mtype not in ("image", "gif", "video", "audio"):
            note_error(f"unsupported media type '{mtype}'")
        if mtype in ("image", "gif"):
            ok = dl_chat_image(mid, dest)
        elif mtype == "video":
            ok = dl_chat_video(mid, dest)
        elif mtype == "audio":
            ok = dl_chat_audio(mid, dest)

        if ok:
            with db_lock:
                conn = sqlite3.connect(db.db_path)
                conn.execute("UPDATE chat_media SET downloaded=1 WHERE media_id=?", (mid,))
                conn.commit()
                conn.close()
            with stats_lock:
                key = {"image": "images", "gif": "images", "video": "videos", "audio": "audio"}.get(mtype, "images")
                stats[key] += 1
            sz = os.path.getsize(dest) if os.path.exists(dest) else 0
            with print_lock:
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"  [{ts}] [{idx}/{total}] OK {mtype}: {name}/{fname} ({fmt_size(sz)})", flush=True)
        elif not token_expired.is_set():
            reason = last_error()
            with stats_lock:
                stats["fail"] += 1
            with print_lock:
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"  [{ts}] [{idx}/{total}] FAIL {mtype}: {name}/{fname} — {reason}", flush=True)
            api_type = "image" if mtype == "gif" else mtype
            FAILURES.record(source="chat", creator=name, type=mtype, media_id=mid,
                            reason=reason,
                            media_url=f"{MEDIA_API}/{api_type}/chat/{mid}?redirect=true",
                            profile_url=f"https://privacy.com.br/chat/{item.get('room_id', '')}")
        else:
            with stats_lock:
                stats["fail"] += 1
        return ok

    start = time.time()
    total = len(items)
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futs = {pool.submit(process_item, item, idx, total): item
                for idx, item in enumerate(items, 1)}
        for fut in as_completed(futs):
            try:
                result = fut.result()
                if result is None and token_expired.is_set():
                    if not try_refresh():
                        print("\n  JWT EXPIRED & REFRESH FAILED — stopping.")
                        pool.shutdown(wait=False, cancel_futures=True)
                        break
            except Exception as e:
                with print_lock:
                    print(f"  ERROR: {e}", flush=True)

    elapsed = time.time() - start
    print()
    print("=" * 80)
    print("CHAT DOWNLOAD COMPLETE")
    print("=" * 80)
    print(f"  Images:  {stats['images']} dl / {stats['skip']} skip / {stats['fail']} fail")
    print(f"  Videos:  {stats['videos']}")
    print(f"  Audio:   {stats['audio']}")
    print(f"  Data:    {fmt_size(stats['bytes'])}")
    print(f"  Time:    {elapsed:.0f}s ({elapsed/60:.1f}m)")
    print()


def _dl_chat_hls(master, base_url, token, dest, cdn_headers,
                 max_retries, retry_delay, ffmpeg, stats, stats_lock):
    """Download HLS video: parse m3u8, download segments, ffmpeg remux."""

    def hls_get(url, stream=False):
        filename = url.split("/")[-1].split("?")[0]
        headers = {**cdn_headers, "content": token, "x-content-uri": filename}
        for attempt in range(max_retries):
            try:
                resp = http_get(url, headers=headers, stream=stream, timeout=60)
                if resp.status_code == 200:
                    return resp
            except Exception:
                pass
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
        return None

    # Pick best quality variant
    streams = []
    lines = master.strip().split("\n")
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF:"):
            bw = re.search(r"BANDWIDTH=(\d+)", line)
            if bw and i + 1 < len(lines):
                streams.append((int(bw.group(1)), lines[i + 1].strip()))

    if not streams:
        quality = master
    else:
        streams.sort(key=lambda x: x[0], reverse=True)
        resp2 = hls_get(base_url + streams[0][1])
        if not resp2:
            return False
        quality = resp2.text

    segments = []
    key_url = None
    for line in quality.split("\n"):
        line = line.strip()
        if line.startswith("#EXT-X-KEY:"):
            km = re.search(r'URI="([^"]+)"', line)
            if km:
                key_url = km.group(1)
        elif line and not line.startswith("#"):
            segments.append(line)

    if not segments:
        note_error("HLS playlist has no segments")
        return False

    tmp = tempfile.mkdtemp(prefix="priv_chat_hls_")
    try:
        # AES key
        local_key = None
        if key_url:
            rk = hls_get(key_url)
            if rk:
                local_key = os.path.join(tmp, "key.aes")
                with open(local_key, "wb") as f:
                    f.write(rk.content)
            else:
                return False

        # Download segments
        total_bytes = 0
        for idx, seg in enumerate(segments, 1):
            rs = hls_get(base_url + seg, stream=True)
            if not rs:
                return False
            sp = os.path.join(tmp, seg)
            with open(sp, "wb") as f:
                for chunk in rs.iter_content(65536):
                    f.write(chunk)
                    total_bytes += len(chunk)

        with stats_lock:
            stats["bytes"] += total_bytes

        # Write local m3u8
        local_m3u8 = os.path.join(tmp, "local.m3u8")
        content = quality
        if key_url and local_key:
            content = content.replace(key_url, local_key.replace("\\", "/"))
        for seg in segments:
            content = content.replace(seg, os.path.join(tmp, seg).replace("\\", "/"))
        with open(local_m3u8, "w") as f:
            f.write(content)

        # ffmpeg remux
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        tmp_out = dest + ".tmp.mp4"
        result = subprocess.run(
            [ffmpeg, "-y", "-allowed_extensions", "ALL",
             "-i", local_m3u8, "-c", "copy", "-movflags", "+faststart", tmp_out],
            capture_output=True, text=True, timeout=300,
        )
        if result.returncode == 0 and os.path.exists(tmp_out):
            # Retry move in case ffmpeg hasn't released file handle (Windows)
            for _retry in range(5):
                try:
                    shutil.move(tmp_out, dest)
                    return True
                except OSError:
                    time.sleep(0.5)
            return False
        else:
            if os.path.exists(tmp_out):
                try:
                    os.remove(tmp_out)
                except OSError:
                    pass
            return False
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def cmd_chat_download(config: dict, auth: dict):
    """Download-only for chat media (skip scraping)."""
    _chat_download(config, auth)


# ═══════════════════════════════════════════════════════════════════════════════
# Command: status
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_status(config: dict, _auth: dict):
    db = Database(config["db_path"], verbose=False)
    s = db.get_stats()

    print("=" * 80)
    print("DATABASE STATUS")
    print("=" * 80)
    print(f"  Following:        {s['following']} users")
    print(f"  Users with media: {s['users_with_media']}")
    print(f"  Total media:      {s['total_media']}")
    print(f"  Downloaded:       {s['downloaded']}")
    print(f"  Remaining:        {s['remaining']}")
    print(f"  By type:          {s['by_type']}")
    print()

    # Per-user breakdown
    conn = sqlite3.connect(config["db_path"])
    conn.row_factory = sqlite3.Row
    rows = conn.execute('''
        SELECT u.nickname, COUNT(m.id) as total,
               SUM(CASE WHEN m.downloaded=1 THEN 1 ELSE 0 END) as done
        FROM media m JOIN users u ON m.user_aid=u.aid
        GROUP BY m.user_aid ORDER BY u.nickname
    ''').fetchall()
    conn.close()

    print(f"  {'User':<30} {'Total':>7} {'Done':>7} {'%':>6}")
    print(f"  {'-'*30} {'-'*7} {'-'*7} {'-'*6}")
    for r in rows:
        pct = (r["done"] / r["total"] * 100) if r["total"] > 0 else 0
        marker = " *" if r["done"] == 0 else ""
        print(f"  {r['nickname']:<30} {r['total']:>7} {r['done']:>7} {pct:>5.1f}%{marker}")
    print()

    # Chat stats
    cs = db.get_chat_stats()
    if cs["rooms"] > 0:
        print("  CHAT MEDIA")
        print(f"  {'-'*50}")
        print(f"  Chat rooms:     {cs['rooms']}")
        print(f"  Chat media:     {cs['total']} ({cs['unlocked']} unlocked)")
        print(f"  Downloaded:     {cs['downloaded']}")
        print(f"  Remaining:      {cs['remaining']}")
        print(f"  By type:        {cs['by_type']}")

        conn = sqlite3.connect(config["db_path"])
        conn.row_factory = sqlite3.Row
        rows = conn.execute('''
            SELECT cr.recipient_name, COUNT(cm.id) as total,
                   SUM(CASE WHEN cm.downloaded=1 THEN 1 ELSE 0 END) as done,
                   SUM(CASE WHEN cm.is_locked=1 THEN 1 ELSE 0 END) as locked
            FROM chat_media cm JOIN chat_rooms cr ON cm.room_id = cr.room_id
            GROUP BY cm.room_id ORDER BY cr.recipient_name
        ''').fetchall()
        conn.close()

        print()
        print(f"  {'Chat Room':<30} {'Total':>7} {'Done':>7} {'Lock':>6} {'%':>6}")
        print(f"  {'-'*30} {'-'*7} {'-'*7} {'-'*6} {'-'*6}")
        for r in rows:
            unlocked = r['total'] - r['locked']
            pct = (r['done'] / unlocked * 100) if unlocked > 0 else 0
            print(f"  {r['recipient_name'] or 'Unknown':<30} {r['total']:>7} {r['done']:>7} {r['locked']:>6} {pct:>5.1f}%")
        print()


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Privacy.com.br Scraper & Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Commands:
  sync       Sync following list from API to database
  scrape     Scrape media metadata for all followed users
  download   Download all media (images + HLS videos)
  retry      Retry only failed/remaining downloads (flat queue)
  rescan     Re-sync + re-scrape to find new content, then download
  chat       Scrape + download all chat/message media
  chat-dl    Download-only for chat media (skip scraping)
  status     Show progress per user
  pipeline   Sync + scrape + download interleaved per user (recommended)
  all        Run sync → scrape → download (legacy sequential)
""",
    )
    parser.add_argument("command", choices=["sync", "scrape", "download", "retry", "rescan", "chat", "chat-dl", "status", "pipeline", "all"],
                        help="Action to perform")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    parser.add_argument("--auth", default="auth.json", help="Path to auth.json")
    args = parser.parse_args()

    config = load_config(args.config)
    auth = load_auth(args.auth)

    SKIP_USERS.update(str(n).strip().lower() for n in config.get("skip_users", []))
    if SKIP_USERS:
        print(f"[CONFIG] Skipping users: {', '.join(sorted(SKIP_USERS))}")

    # Failure list for this run (replaces the previous run's list of the same kind)
    if args.command in ("download", "retry", "rescan", "pipeline", "all"):
        FAILURES.start(config.get("failed_log", "failed_media.tsv"))
    elif args.command in ("chat", "chat-dl"):
        FAILURES.start(config.get("failed_chat_log", "failed_chat.tsv"))

    cmds = {
        "sync": cmd_sync,
        "scrape": cmd_scrape,
        "download": cmd_download,
        "retry": cmd_retry,
        "rescan": cmd_rescan,
        "chat": cmd_chat,
        "chat-dl": cmd_chat_download,
        "status": cmd_status,
        "pipeline": cmd_pipeline,
    }

    if args.command == "all":
        cmd_pipeline(config, auth)
    else:
        cmds[args.command](config, auth)

    if FAILURES.count:
        print(f"\n[FAILURES] {FAILURES.count} media failed — see {FAILURES.path}")


if __name__ == "__main__":
    main()
