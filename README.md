# Privacy.com.br Media Scraper

Single-file Python script that downloads all your subscribed content from privacy.com.br — timeline posts, images, videos (HLS), and chat/message media.

## Running with Docker (no local install)

Only Docker is required — Python, `requests` and `ffmpeg` live inside the image.

```powershell
docker compose build                      # once (and again after editing main.py)
docker compose run --rm scraper pipeline  # or: status, rescan, chat, retry, ...
```

The project folder is mounted as the container's working directory, so `config.json`, `auth.json`, `privacy_users.db` and `content/` are read and written right here on the host. Keep `config.json` with `"ffmpeg_path": "ffmpeg"` and a relative `download_dir` (e.g. `./content`). To save media elsewhere (e.g. another drive), add a volume such as `- D:/privacy:/media` in `docker-compose.yml` and set `"download_dir": "/media"`.

Fill in `auth.json` as described in step 3 below before running.

### MCP server (drive the tool from an AI assistant)

`mcp_server.py` exposes the scraper as an [MCP](https://modelcontextprotocol.io) server (stdio), running in the same Docker image.

- **Claude Code:** the project's `.mcp.json` registers it automatically — open Claude Code in this folder and approve the `privacy-br-scraper` server. Rebuild the image first (`docker compose build`).
- **Other clients** (e.g. Claude Desktop): run `docker compose -f <path-to-this-folder>/docker-compose.yml run --rm -T mcp` as the server command.

| Tool | What it does |
|------|--------------|
| `start_job(command, creators?)` | Runs `pipeline`, `rescan`, `scrape`, `download`, `retry`, `chat`, `chat-dl` or `sync` in the background, optionally only for some creators. One job at a time. |
| `wait_for_job(job_id?, max_wait_seconds=240, update_every_seconds=15)` | Follows a job, sending live MCP progress notifications (`downloaded/total`, %, ETA, last log line) every few seconds, and returns when the job ends or the wait expires. The server instructions tell the AI to report progress to the user and call it again until the job finishes. |
| `job_status(job_id?)`, `list_jobs()`, `stop_job(job_id?)` | Snapshot of a job (progress + log tail), list and stop jobs. Logs go to `logs/`. |
| `status()` | Per-creator progress table. |
| `list_creators()` | Followed creators with media / downloaded / remaining counts and skip flag. |
| `list_failures(kind)` | Contents of `failed_media.tsv` / `failed_chat.tsv`, grouped by reason. |
| `find_duplicate_files()` | Files with identical content (read-only). |
| `auth_status()` | JWT expiry and cookie **names** — secret values are never returned. |
| `update_auth(curl_command? \| jwt_token?, cookie_string?)` | Updates `auth.json`; easiest is pasting a DevTools *Copy as cURL* of any `service.privacy.com.br` request. |
| `get_skip_users()`, `set_skip_user(name, skip)` | Manage `skip_users` in `config.json`. |

Jobs run inside the MCP server's container, so they stop when the AI client session ends; progress is kept in the database and the next run resumes.

The CLI also accepts `--user NAME` (repeatable) to process only some creators, e.g. `docker compose run --rm scraper pipeline --user SomeCreator`.

### Skipping creators

`config.json` → `"skip_users": ["Privacy"]` — creators listed here (nickname or profile name, case-insensitive) are never scraped or downloaded, including their chat media.

### Failure list

Every download run writes the media that failed, with the reason and link, to a tab-separated file (opens in Excel):

- `failed_media.tsv` — timeline posts (`pipeline`, `download`, `retry`, `rescan`)
- `failed_chat.tsv` — chat media (`chat`, `chat-dl`)

Columns: `time, source, creator, type, media_id, reason, media_url, profile_url`. Each run replaces the previous list of the same kind, so it always reflects what is still failing.

## Prerequisites (running without Docker)

- **Python 3.10+**
- **ffmpeg** (required for video downloads — HLS stream merging)
- **A privacy.com.br account** with active subscriptions

## Setup from Scratch

### 1. Get the script

You only need **one file**: `main.py`. Copy it into a new folder:

```
mkdir privacy_scraper
cd privacy_scraper
```

### 2. Install the dependency

```bash
pip install requests
```

That's the only external package. Everything else is Python standard library.

### 3. Create `auth.json`

This is the **only user input** required. Create `auth.json` in the same folder as `main.py`:

```json
{
    "jwt_token": "eyJhbG...",
    "cookie_string": "_gcl_au=...; .AspNetCore.Cookies=CfDJ8...; .AspNetCore.Session=CfDJ8..."
}
```

**How to get these values:**
1. Log into [privacy.com.br](https://privacy.com.br) in your browser
2. Open DevTools (F12) → **Network** tab
3. Click on any API request to `service.privacy.com.br`
4. From the request headers:
   - **`Authorization`** header → copy the value after `Bearer ` → paste as `jwt_token`
   - **`Cookie`** header → copy the entire value → paste as `cookie_string`

> **Token lifespan:** ~6 hours. When it expires, the script stops gracefully. Update `auth.json` with fresh values and re-run. The script auto-refreshes the JWT when possible using the session cookies.

### 4. Create `config.json`

Create `config.json` in the same folder:

```json
{
    "download_dir": "./content",
    "ffmpeg_path": "ffmpeg",
    "db_path": "privacy_users.db",
    "num_threads": 12,
    "max_retries": 3,
    "retry_delay": 2,
    "rate_limit_delay": 0.3,
    "scrape_max_pages": 9999,
    "scrape_threads": 6,
    "progress_report_interval": 300
}
```

Adjust as needed:
- `download_dir` — where media files are saved (UNC network paths like `\\SERVER\share\privacy` work too)
- `ffmpeg_path` — full path to ffmpeg if it's not on your system PATH
- `num_threads` — concurrent download threads (12 is a good default)
- `scrape_threads` — concurrent threads for API scraping (6 is a good default)

### 5. Run

```bash
python main.py pipeline
```

That's it. The script will:
1. Sync your following list from the API
2. For each user: scrape their posts → immediately download new media
3. Media is saved to `content/{username}/`

## File Structure

After setup, your folder looks like this:

```
privacy_scraper/
├── main.py             → the single script (everything is here)
├── config.json         → settings (paths, threads, retries)
├── auth.json           → your credentials (JWT + cookies) — DO NOT SHARE
├── privacy_users.db    → SQLite database (auto-created on first run)
└── content/            → downloaded media (auto-created)
    ├── Ana Lotti/
    │   ├── UUID1.jpg
    │   ├── UUID2.mp4
    │   └── ...
    ├── Username2/
    └── ...
```

## Commands

```bash
python main.py <command> [--config config.json] [--auth auth.json]
```

### Main Commands

| Command | Description |
|---------|-------------|
| `pipeline` | **Recommended.** Sync + scrape + download interleaved per user. Downloads start immediately as each user is scraped. |
| `all` | Same as `pipeline`. |
| `chat` | Scrape + download all chat/message media. |
| `rescan` | Re-sync users, re-scrape all for new content, download new items. Use this for periodic updates. |
| `status` | Show download progress per user (table view). |

### Granular Commands

| Command | Description |
|---------|-------------|
| `sync` | Only sync the following list from the API into the local database. |
| `scrape` | Only scrape media metadata from all users (no downloads). |
| `download` | Only download media that's in the database but not yet on disk. |
| `retry` | Retry only failed/remaining downloads. |
| `chat-dl` | Download-only for chat media (skip scraping chat rooms). |

### Optional Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--config` | `config.json` | Path to configuration file |
| `--auth` | `auth.json` | Path to authentication file |

### Examples

```bash
# First run — download everything
python main.py pipeline

# Check on new content (periodic update)
python main.py rescan

# Download chat/message media
python main.py chat

# See what's downloaded
python main.py status

# Only retry items that previously failed
python main.py retry

# Use a custom config location
python main.py pipeline --config /path/to/config.json --auth /path/to/auth.json
```

## Configuration Reference

### `config.json`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `download_dir` | string | `"./content"` | Destination folder for media files |
| `ffmpeg_path` | string | `"ffmpeg"` | Path to ffmpeg binary |
| `db_path` | string | `"privacy_users.db"` | Path to SQLite database |
| `num_threads` | int | `12` | Concurrent download threads |
| `max_retries` | int | `3` | Retries per failed download |
| `retry_delay` | int | `2` | Base delay between retries (seconds, multiplied by attempt) |
| `rate_limit_delay` | float | `0.3` | Delay between API pages during scraping |
| `scrape_max_pages` | int | `9999` | Max pages per user (9999 = unlimited) |
| `scrape_threads` | int | `6` | Concurrent threads for scraping |
| `progress_report_interval` | int | `300` | Seconds between background progress reports |
| `skip_users` | list | `[]` | Creators (nickname or profile name, case-insensitive) never scraped or downloaded |
| `failed_log` | string | `"failed_media.tsv"` | Failure list for timeline downloads |
| `failed_chat_log` | string | `"failed_chat.tsv"` | Failure list for chat downloads |

### `auth.json`

| Key | Description |
|-----|-------------|
| `jwt_token` | JWT value only — without the `Bearer ` prefix |
| `cookie_string` | Full `Cookie` header value from browser DevTools |

## Console Output

Every log line shows global progress:
```
[HH:MM:SS] [T##] [done/total pct%] message
```

Periodic progress summary:
```
================================================================================
  PROGRESS  [done/total] pct%  |  This session: +N items (size)  |  Rate: X/hr  |  ETA: Xh
  Active threads:
    T 1: DL video: Username/UUID.mp4
    T 2: DL photo: Username/UUID.jpg
================================================================================
```

## Typical Workflow

### First time
```bash
python main.py pipeline    # downloads everything
```

### Periodic updates
```bash
# Update auth.json with fresh JWT + cookies, then:
python main.py rescan      # finds and downloads only new content
python main.py chat        # check for new chat media
```

### After a crash or interruption
```bash
python main.py retry       # picks up where it left off
```

The SQLite database tracks what's been downloaded, so you can safely stop and restart at any time.

## Technical Notes

1. **Accept-Encoding must NOT include `br`** (Brotli) — the API returns corrupt data. The script uses `gzip, deflate` only.
2. **Video CDN uses custom headers** named `content` and `x-content-uri` for video token auth (not standard HTTP headers).
3. **Videos are HLS streams** — the script downloads all `.ts` segments and uses ffmpeg to remux them into `.mp4`.
4. **JWT auto-refresh**: When the token expires mid-run, the script attempts to refresh it using the session cookies. If that fails, it stops gracefully and you need to update `auth.json`.
5. **Windows users**: Set `$env:PYTHONIOENCODING="utf-8"` before running, or Unicode characters in creator names may cause encoding errors.
6. **Resumable**: The database tracks every media item. Re-running any command skips already-downloaded files.
