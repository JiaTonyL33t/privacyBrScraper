#!/usr/bin/env python3
"""
MCP server for the privacy.com.br scraper (stdio transport).

Exposes the scraper's features as MCP tools so an AI assistant can drive it:
status/creators/failures/duplicates (read-only), auth and skip-list
management, and long-running commands (pipeline, download, retry, ...) as
background jobs whose progress can be polled.

Runs from the data directory (the folder with config.json, auth.json,
privacy_users.db and the downloads) — in Docker that is /data.

Credentials are never returned by any tool.
"""

import asyncio
import base64
import collections
import csv
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

from mcp.server.fastmcp import Context, FastMCP

DATA_DIR = os.environ.get("SCRAPER_DATA_DIR", os.getcwd())
MAIN_PY = os.environ.get("SCRAPER_MAIN", os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.py"))
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
AUTH_PATH = os.path.join(DATA_DIR, "auth.json")
JOB_LOG_DIR = os.path.join(DATA_DIR, "logs")

mcp = FastMCP("privacy-br-scraper", instructions="""\
Downloads the user's subscribed privacy.com.br content.
Long commands (pipeline, rescan, download, retry, chat...) run as background
jobs: start one with start_job, then call wait_for_job repeatedly until its
state is no longer "running". After each wait_for_job call, tell the user the
progress in one short line (downloaded/total, ETA) before calling it again,
so they can see the download is alive and keep the session open — jobs stop
if the session is closed. Never reveal credential values.""")


# ─── helpers ─────────────────────────────────────────────────────────────────

def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: str, data: dict):
    text = json.dumps(data, indent=4, ensure_ascii=False) + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _config() -> dict:
    return _load_json(CONFIG_PATH) if os.path.exists(CONFIG_PATH) else {}


def _db_path() -> str:
    return os.path.join(DATA_DIR, _config().get("db_path", "privacy_users.db"))


def _download_dir() -> str:
    return os.path.join(DATA_DIR, _config().get("download_dir", "./content"))


def _db() -> sqlite3.Connection:
    path = _db_path()
    if not os.path.exists(path):
        raise RuntimeError("Database not found yet — run the 'sync' or 'pipeline' job first.")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _jwt_claims(token: str) -> Optional[dict]:
    try:
        payload = token.split(".")[1]
        return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except Exception:
        return None


def _fmt_size(b: float) -> str:
    for u in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f}{u}"
        b /= 1024
    return f"{b:.1f}TB"


def _fmt_duration(seconds: float) -> str:
    s = int(seconds)
    h, m = s // 3600, (s % 3600) // 60
    return f"{h}h{m:02d}m" if h else f"{m}m{s % 60:02d}s"


def _run_main(args: list, timeout: int = 120) -> str:
    """Run a short main.py command synchronously and return its output."""
    res = subprocess.run([sys.executable, MAIN_PY, *args], cwd=DATA_DIR,
                         capture_output=True, text=True, timeout=timeout,
                         env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"})
    out = (res.stdout or "") + (("\n" + res.stderr) if res.stderr else "")
    return out.strip() + (f"\n[exit code {res.returncode}]" if res.returncode else "")


# ─── background jobs ─────────────────────────────────────────────────────────

def _dl_counts(creators: Optional[list] = None) -> tuple:
    """(downloaded, remaining) timeline media for the selected creators,
    honoring skip_users. remaining = unlocked photos/videos not downloaded."""
    try:
        conn = _db()
    except RuntimeError:
        return 0, 0
    skip = {s.lower() for s in _config().get("skip_users", [])}
    only = {c.lower() for c in creators or []}
    rows = conn.execute("""
        SELECT u.nickname, u.profile_name,
               SUM(CASE WHEN m.downloaded=1 THEN 1 ELSE 0 END) AS done,
               SUM(CASE WHEN m.downloaded=0 AND m.is_locked=0 AND m.media_url IS NOT NULL
                         AND m.media_type IN ('photo','video') THEN 1 ELSE 0 END) AS todo
        FROM users u JOIN media m ON m.user_aid = u.aid GROUP BY u.aid
    """).fetchall()
    conn.close()
    done = todo = 0
    for r in rows:
        names = {(r["nickname"] or "").lower(), (r["profile_name"] or "").lower()}
        if names & skip or (only and not names & only):
            continue
        done += r["done"] or 0
        todo += r["todo"] or 0
    return done, todo


class Job:
    def __init__(self, command: str, args: list, creators: Optional[list] = None):
        self.id = uuid.uuid4().hex[:8]
        self.command = command
        self.args = args
        self.creators = creators or []
        self.dl_at_start = _dl_counts(self.creators)[0]
        self.started = datetime.now()
        self.finished: Optional[datetime] = None
        os.makedirs(JOB_LOG_DIR, exist_ok=True)
        self.log_path = os.path.join(
            JOB_LOG_DIR, f"{self.started:%Y%m%d-%H%M%S}-{command}-{self.id}.log")
        self._log = open(self.log_path, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, MAIN_PY, command, *args], cwd=DATA_DIR,
            stdout=self._log, stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"})
        threading.Thread(target=self._wait, daemon=True).start()

    def _wait(self):
        self.proc.wait()
        self.finished = datetime.now()
        self._log.close()

    @property
    def state(self) -> str:
        rc = self.proc.poll()
        if rc is None:
            return "running"
        return "finished" if rc == 0 else f"failed (exit code {rc})"

    def tail(self, n: int) -> str:
        try:
            with open(self.log_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
        except FileNotFoundError:
            return ""
        # Segment progress lines are noise for a summary
        lines = [l for l in lines if "segments " not in l]
        return "".join(lines[-n:])

    def progress(self) -> dict:
        """Downloads done in this job vs. total known so far (grows while scraping)."""
        done_now, todo = _dl_counts(self.creators)
        done = max(0, done_now - self.dl_at_start)
        total = done + todo
        elapsed = ((self.finished or datetime.now()) - self.started).total_seconds()
        eta = None
        if done and todo and self.state == "running":
            eta = _fmt_duration(elapsed * todo / done)
        last = ""
        for line in reversed(self.tail(60).splitlines()):
            if line.strip() and "segments" not in line:
                last = line.strip()
                break
        return {"downloaded": done, "total": total, "remaining": todo,
                "percent": round(100 * done / total, 1) if total else None,
                "eta": eta, "elapsed": _fmt_duration(elapsed),
                "last_log_line": last[-200:]}

    def summary(self) -> dict:
        end = self.finished or datetime.now()
        return {
            "job_id": self.id,
            "command": " ".join([self.command, *self.args]),
            "state": self.state,
            "started": self.started.strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed": str(end - self.started).split(".")[0],
            "log_file": os.path.relpath(self.log_path, DATA_DIR),
        }


JOBS: "collections.OrderedDict[str, Job]" = collections.OrderedDict()


def _running_job() -> Optional[Job]:
    return next((j for j in JOBS.values() if j.state == "running"), None)


JobCommand = Literal["pipeline", "rescan", "scrape", "download", "retry", "chat", "chat-dl", "sync"]


@mcp.tool()
def start_job(command: JobCommand, creators: Optional[list[str]] = None) -> dict:
    """Start a scraper command in the background and return its job id.

    Commands:
      pipeline — sync following list, scrape every creator and download new media (main command)
      rescan   — re-sync + re-scrape everyone to find new content, then download it
      scrape   — only read posts and store media metadata (no downloads)
      download — only download media already in the database but not on disk
      retry    — retry only media that previously failed
      chat     — scrape + download media from private chats
      chat-dl  — download chat media already in the database
      sync     — only refresh the list of followed creators

    `creators` optionally restricts the run to these creators (nickname or
    profile name). Only one job runs at a time. Poll progress with job_status.
    """
    running = _running_job()
    if running:
        return {"error": "A job is already running — wait for it or stop it first.",
                "running": running.summary()}
    args = []
    for c in creators or []:
        args += ["--user", c]
    job = Job(command, args, creators)
    JOBS[job.id] = job
    time.sleep(1)
    return {**job.summary(),
            "hint": "Call wait_for_job(job_id) repeatedly to follow progress until it finishes."}


@mcp.tool()
async def wait_for_job(ctx: Context, job_id: Optional[str] = None,
                       max_wait_seconds: int = 240, update_every_seconds: int = 15) -> dict:
    """Follow a job, sending live progress updates (MCP progress notifications
    + log messages) every `update_every_seconds`, and return when the job ends
    or after `max_wait_seconds` (max 600). While the returned state is
    "running", tell the user the progress in one line and call this again —
    this keeps the user informed during long downloads."""
    job = JOBS.get(job_id) if job_id else (_running_job() or (next(reversed(JOBS.values())) if JOBS else None))
    if not job:
        return {"error": "No such job." if job_id else "No jobs started in this session."}
    max_wait = max(5, min(max_wait_seconds, 600))
    every = max(5, min(update_every_seconds, 120))
    deadline = time.monotonic() + max_wait
    while True:
        p = job.progress()
        msg = (f"{p['downloaded']}/{p['total']} downloaded"
               + (f" ({p['percent']}%)" if p["percent"] is not None else "")
               + (f", ETA {p['eta']}" if p["eta"] else "")
               + f" — {p['last_log_line'][:120]}")
        await ctx.report_progress(p["downloaded"], p["total"] or None, msg)
        await ctx.info(f"[job {job.id}] {msg}")
        if job.state != "running" or time.monotonic() >= deadline:
            break
        await asyncio.sleep(min(every, max(0.0, deadline - time.monotonic())))
    return {**job.summary(), "progress": job.progress(),
            "next": "Job still running — report progress to the user and call wait_for_job again."
                    if job.state == "running" else "Job ended — summarize the result for the user.",
            "log_tail": job.tail(10)}


@mcp.tool()
def job_status(job_id: Optional[str] = None, tail_lines: int = 25) -> dict:
    """Show a job's state and the last lines of its log (latest job if no id given).
    Download progress lines look like `[done/total pct%] OK video: creator/file.mp4`."""
    if not JOBS:
        return {"error": "No jobs started in this session."}
    job = JOBS.get(job_id) if job_id else next(reversed(JOBS.values()))
    if not job:
        return {"error": f"Unknown job id {job_id}", "jobs": list(JOBS)}
    return {**job.summary(), "progress": job.progress(),
            "log_tail": job.tail(max(1, min(tail_lines, 200)))}


@mcp.tool()
def list_jobs() -> list[dict]:
    """List the jobs started in this session."""
    return [j.summary() for j in JOBS.values()]


@mcp.tool()
def stop_job(job_id: Optional[str] = None) -> dict:
    """Stop a running job (the running one if no id given). Progress is kept:
    the database records every finished download, so the job can be resumed."""
    job = JOBS.get(job_id) if job_id else _running_job()
    if not job or job.state != "running":
        return {"error": "No such running job."}
    job.proc.terminate()
    try:
        job.proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        job.proc.kill()
    return job.summary()


# ─── read-only information ───────────────────────────────────────────────────

@mcp.tool()
def status() -> str:
    """Download progress per creator (followed creators, media found, downloaded, remaining)."""
    return _run_main(["status"])


@mcp.tool()
def list_creators() -> list[dict]:
    """Followed creators with their media counts and whether they are in skip_users."""
    skip = {s.lower() for s in _config().get("skip_users", [])}
    conn = _db()
    rows = conn.execute("""
        SELECT u.nickname, u.profile_name, u.profile_url, u.still_following,
               COUNT(m.id) AS media,
               SUM(CASE WHEN m.downloaded=1 THEN 1 ELSE 0 END) AS downloaded,
               SUM(CASE WHEN m.is_locked=1 THEN 1 ELSE 0 END) AS locked
        FROM users u LEFT JOIN media m ON m.user_aid = u.aid
        GROUP BY u.aid ORDER BY u.nickname
    """).fetchall()
    conn.close()
    return [{
        "nickname": r["nickname"], "profile_name": r["profile_name"],
        "profile_url": r["profile_url"], "following": bool(r["still_following"]),
        "media": r["media"], "downloaded": r["downloaded"] or 0, "locked": r["locked"] or 0,
        "remaining": r["media"] - (r["downloaded"] or 0) - (r["locked"] or 0),
        "skipped": bool({(r["nickname"] or "").lower(), (r["profile_name"] or "").lower()} & skip),
    } for r in rows]


@mcp.tool()
def list_failures(kind: Literal["timeline", "chat"] = "timeline", limit: int = 100) -> dict:
    """Media that failed in the last download run, with the reason and links
    (from failed_media.tsv or failed_chat.tsv)."""
    cfg = _config()
    name = cfg.get("failed_log", "failed_media.tsv") if kind == "timeline" \
        else cfg.get("failed_chat_log", "failed_chat.tsv")
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        return {"file": name, "count": 0, "failures": [],
                "note": "No failure list — the last run had no failures (or none ran yet)."}
    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    reasons = collections.Counter(r.get("reason", "") for r in rows)
    return {"file": name, "count": len(rows), "by_reason": dict(reasons.most_common()),
            "failures": rows[:max(1, limit)]}


@mcp.tool()
def find_duplicate_files() -> dict:
    """Find downloaded files with identical content (e.g. a creator posting the
    same file in two posts). Read-only: nothing is deleted."""
    root = _download_dir()
    by_size = collections.defaultdict(list)
    for d, _, files in os.walk(root):
        for fn in files:
            if ".tmp" in fn:
                continue
            p = os.path.join(d, fn)
            by_size[os.path.getsize(p)].append(p)
    groups = collections.defaultdict(list)
    for size, paths in by_size.items():
        if len(paths) < 2 or size == 0:
            continue
        for p in paths:
            h = hashlib.md5()
            with open(p, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
            groups[h.hexdigest()].append(p)
    dups = [sorted(os.path.relpath(p, root) for p in ps) for ps in groups.values() if len(ps) > 1]
    wasted = sum(os.path.getsize(os.path.join(root, g[0])) * (len(g) - 1) for g in dups)
    return {"groups": len(dups), "extra_copies": sum(len(g) - 1 for g in dups),
            "wasted": _fmt_size(wasted), "duplicates": dups}


# ─── auth ────────────────────────────────────────────────────────────────────

@mcp.tool()
def auth_status() -> dict:
    """Whether auth.json has a JWT, when it expires, and which cookies are set
    (cookie names only — values are never returned)."""
    if not os.path.exists(AUTH_PATH):
        return {"configured": False, "note": "auth.json not found — use update_auth."}
    auth = _load_json(AUTH_PATH)
    jwt = auth.get("jwt_token") or ""
    claims = _jwt_claims(jwt) if jwt else None
    out = {"configured": bool(claims)}
    if claims and "exp" in claims:
        exp = datetime.fromtimestamp(int(claims["exp"]), tz=timezone.utc)
        left = exp - datetime.now(timezone.utc)
        out.update({"jwt_expires_utc": exp.strftime("%Y-%m-%d %H:%M:%S"),
                    "jwt_expired": left.total_seconds() <= 0,
                    "jwt_time_left": str(left).split(".")[0] if left.total_seconds() > 0 else "0"})
    names = [c.split("=", 1)[0].strip() for c in (auth.get("cookie_string") or "").split(";") if "=" in c]
    out["cookie_names"] = names
    out["has_session_cookies"] = any(n.startswith(".AspNetCore") for n in names)
    return out


def _parse_curl(text: str) -> dict:
    # "Copy as cURL (cmd)" escapes with ^ — strip it; bash variant has none.
    if '^"' in text:
        text = text.replace("^", "")
    text = text.replace("\\\n", " ").replace("\n", " ")
    headers = {}
    for m in re.finditer(r"""(?:-H|--header)\s+(['"])(.*?)\1""", text):
        k, _, v = m.group(2).partition(":")
        headers[k.strip().lower()] = v.strip()
    cookie = headers.get("cookie")
    m = re.search(r"""(?:-b|--cookie)\s+(['"])(.*?)\1""", text)
    if m:
        cookie = m.group(2).strip()
    jwt = None
    auth = headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        jwt = auth[7:].strip()
    return {"jwt": jwt, "cookie": cookie}


@mcp.tool()
def update_auth(curl_command: Optional[str] = None, jwt_token: Optional[str] = None,
                cookie_string: Optional[str] = None) -> dict:
    """Update auth.json. Easiest: pass `curl_command` — in the browser DevTools
    Network tab, right-click any request to service.privacy.com.br →
    Copy → Copy as cURL (cmd or bash) — and the JWT (Authorization: Bearer ...)
    and full Cookie header are extracted from it. Alternatively pass
    jwt_token (with or without 'Bearer ') and/or cookie_string directly.
    Fields not provided are kept. Returns auth_status (no secret values)."""
    jwt, cookie = None, None
    if curl_command:
        parsed = _parse_curl(curl_command)
        jwt, cookie = parsed["jwt"], parsed["cookie"]
        if not jwt and not cookie:
            return {"error": "No Authorization or Cookie header found in the cURL command."}
    if jwt_token:
        jwt = jwt_token.strip()
        if jwt.lower().startswith("bearer "):
            jwt = jwt[7:].strip()
    if cookie_string:
        cookie = cookie_string.strip()
    if jwt and not _jwt_claims(jwt):
        return {"error": "The JWT does not look valid (could not decode its payload)."}
    auth = _load_json(AUTH_PATH) if os.path.exists(AUTH_PATH) else {}
    if jwt:
        auth["jwt_token"] = jwt
    if cookie:
        auth["cookie_string"] = cookie
    auth.setdefault("jwt_token", "")
    auth.setdefault("cookie_string", "")
    _save_json(AUTH_PATH, auth)
    return {"updated": [k for k, v in (("jwt_token", jwt), ("cookie_string", cookie)) if v],
            **auth_status()}


# ─── skip list ───────────────────────────────────────────────────────────────

@mcp.tool()
def get_skip_users() -> list[str]:
    """Creators that are never scraped or downloaded (config.json → skip_users)."""
    return _config().get("skip_users", [])


@mcp.tool()
def set_skip_user(name: str, skip: bool = True) -> list[str]:
    """Add (skip=True) or remove (skip=False) a creator from skip_users.
    Matching is by nickname or profile name, case-insensitive. Returns the new list."""
    cfg = _config()
    current = cfg.get("skip_users", [])
    rest = [s for s in current if s.lower() != name.strip().lower()]
    cfg["skip_users"] = rest + [name.strip()] if skip else rest
    _save_json(CONFIG_PATH, cfg)
    return cfg["skip_users"]


if __name__ == "__main__":
    mcp.run()
