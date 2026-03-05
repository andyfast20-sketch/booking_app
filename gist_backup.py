"""
GitHub Gist cloud backup for booking app data files.

Automatically backs up data files to a private GitHub Gist so they
survive Render's ephemeral filesystem resets between deploys.

How it works:
  - On startup: downloads latest data from the Gist -> writes to local files
  - On critical data changes (bookings, availability): immediately pushes (async)
  - Every 60 seconds: syncs any other changed files

Setup:
  1. Create a GitHub Personal Access Token (classic) at:
     https://github.com/settings/tokens/new
     - Select ONLY the 'gist' scope
     - Copy the token (starts with ghp_)

  2. Set the GIST_TOKEN environment variable on Render:
     GIST_TOKEN=ghp_your_token_here

  NEVER put tokens directly in source code.
"""

import json
import os
import threading
import time

try:
    import requests as _requests
except ImportError:
    _requests = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_GIST_DESCRIPTION = "booking_app_backup_v1"

# Files to back up (all data files the app uses)
BACKUP_FILES = [
    "bookings.txt",
    "availability.txt",
    "customer_slots.json",
    "reviews.json",
    "contacts.json",
    "visitor_log.json",
    "customer_settings.json",
    "telnyx_config.json",
    "smtp_config.json",
    "email_magic.json",
    "smsapi_config.json",
    "watchdog_config.json",
    "seo_config.json",
    "facebook_config.json",
    "facebook_alerts.json",
    "weather_config.json",
    "admin_auth.json",
    "autopilot.json",
    "chat_state.json",
    "verification_codes.json",
]

# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------

_token: str = ""
_gist_id: str = ""
_data_dir: str = ""
_headers: dict = {}
_lock = threading.Lock()
_enabled: bool = False
_last_mtimes: dict = {}  # filename -> mtime at last backup





# ---------------------------------------------------------------------------
# Gist API helpers
# ---------------------------------------------------------------------------

def _find_gist() -> str:
    """Find our backup gist by its description."""
    try:
        r = _requests.get(
            "https://api.github.com/gists?per_page=100",
            headers=_headers, timeout=15,
        )
        if r.status_code == 200:
            for g in r.json():
                if g.get("description") == _GIST_DESCRIPTION:
                    return g["id"]
    except Exception:
        pass
    return ""


def _create_gist() -> str:
    """Create a new private gist for backups."""
    try:
        r = _requests.post(
            "https://api.github.com/gists",
            headers=_headers,
            json={
                "description": _GIST_DESCRIPTION,
                "public": False,
                "files": {
                    "_info.txt": {
                        "content": "Booking app cloud backup. Do not edit manually."
                    }
                },
            },
            timeout=15,
        )
        if r.status_code == 201:
            gid = r.json()["id"]
            print(f"[Backup] Created backup gist: {gid}")
            return gid
    except Exception as e:
        print(f"[Backup] Failed to create gist: {e}")
    return ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init(data_dir: str) -> bool:
    """Initialize backup system. Call once at startup before loading data."""
    global _token, _data_dir, _gist_id, _headers, _enabled

    if not _requests:
        print("[Backup] 'requests' library not available — backup disabled.")
        return False

    _data_dir = data_dir

    # Token: from GIST_TOKEN environment variable only (never hardcode tokens)
    _token = os.getenv("GIST_TOKEN", "").strip()
    if not _token:
        print("[Backup] GIST_TOKEN env var not set — backup disabled.")
        return False

    # Gist ID: env var (optional optimisation, skips the search)
    _gist_id = os.getenv("GIST_ID", "").strip()

    if not _token:
        print("[Backup] No GIST_TOKEN set — cloud backup disabled.")
        print("[Backup] To enable backups that survive redeploys:")
        print("[Backup]   1. Create a GitHub PAT with 'gist' scope")
        print("[Backup]   2. Set GIST_TOKEN env var on Render")
        return False

    _headers = {
        "Authorization": f"token {_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Verify the token works
    try:
        r = _requests.get(
            "https://api.github.com/user", headers=_headers, timeout=10
        )
        if r.status_code != 200:
            print(f"[Backup] GitHub token invalid (HTTP {r.status_code}) — disabled.")
            return False
        print(f"[Backup] Authenticated as GitHub user: {r.json().get('login', '?')}")
    except Exception as e:
        print(f"[Backup] Cannot reach GitHub: {e} — disabled.")
        return False

    # Find or create the backup gist
    if not _gist_id:
        _gist_id = _find_gist()
    if not _gist_id:
        _gist_id = _create_gist()

    if _gist_id:
        _enabled = True
        print(f"[Backup] Cloud backup ACTIVE (gist {_gist_id[:12]}...)")
        return True

    print("[Backup] Could not find or create gist — disabled.")
    return False


def is_enabled() -> bool:
    """Return whether cloud backup is active."""
    return _enabled


def restore() -> int:
    """Download data files from the gist and write to local disk.

    Only overwrites local files that are missing or contain empty defaults.
    Returns the number of files restored.
    """
    if not _enabled:
        return 0

    try:
        r = _requests.get(
            f"https://api.github.com/gists/{_gist_id}",
            headers=_headers, timeout=30,
        )
        if r.status_code != 200:
            print(f"[Backup] Restore failed: HTTP {r.status_code}")
            return 0

        files = r.json().get("files", {})
        restored = 0

        for filename, info in files.items():
            if filename.startswith("_"):
                continue

            content = (info.get("content") or "").strip()
            if not content or content in ("[]", "{}", "null"):
                continue

            local_path = os.path.join(_data_dir, filename)

            # Don't overwrite local files that already have real data
            if os.path.exists(local_path):
                try:
                    with open(local_path, "r", encoding="utf-8") as f:
                        local = f.read().strip()
                    if local and local not in ("[]", "{}", "null", ""):
                        continue
                except Exception:
                    pass

            try:
                with open(local_path, "w", encoding="utf-8") as f:
                    f.write(info.get("content", ""))
                restored += 1
            except Exception as e:
                print(f"[Backup] Restore write error ({filename}): {e}")

        if restored:
            print(f"[Backup] Restored {restored} file(s) from cloud")
        else:
            print("[Backup] No files needed restoring (all up-to-date locally)")
        return restored

    except Exception as e:
        print(f"[Backup] Restore error: {e}")
        return 0


def save(filename: str):
    """Immediately back up a single file to the gist (async, non-blocking)."""
    if not _enabled:
        return
    threading.Thread(target=_save_one, args=(filename,), daemon=True).start()


def _save_one(filename: str):
    local_path = os.path.join(_data_dir, filename)
    if not os.path.exists(local_path):
        return
    try:
        with open(local_path, "r", encoding="utf-8") as f:
            content = f.read()
        with _lock:
            r = _requests.patch(
                f"https://api.github.com/gists/{_gist_id}",
                headers=_headers,
                json={"files": {filename: {"content": content}}},
                timeout=15,
            )
        if r.status_code == 200:
            _last_mtimes[filename] = os.path.getmtime(local_path)
        else:
            print(f"[Backup] Save {filename} failed: HTTP {r.status_code}")
    except Exception as e:
        print(f"[Backup] Save {filename} error: {e}")


def start_periodic_sync(interval: int = 60):
    """Start a daemon thread that syncs changed files every *interval* secs."""
    if not _enabled:
        return

    def _worker():
        # Initial full backup shortly after startup
        time.sleep(5)
        _sync_changed()
        while True:
            time.sleep(interval)
            _sync_changed()

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    print(f"[Backup] Periodic sync started (every {interval}s)")


def _sync_changed():
    """Upload any files whose mtime changed since last backup."""
    files_payload = {}
    for filename in BACKUP_FILES:
        path = os.path.join(_data_dir, filename)
        if not os.path.exists(path):
            continue
        try:
            mtime = os.path.getmtime(path)
            if filename in _last_mtimes and mtime <= _last_mtimes[filename]:
                continue
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            if content.strip():
                files_payload[filename] = {"content": content}
                _last_mtimes[filename] = mtime
        except Exception:
            pass

    if not files_payload:
        return

    try:
        with _lock:
            r = _requests.patch(
                f"https://api.github.com/gists/{_gist_id}",
                headers=_headers,
                json={"files": files_payload},
                timeout=30,
            )
        if r.status_code == 200:
            print(f"[Backup] Synced {len(files_payload)} file(s): {', '.join(files_payload.keys())}")
    except Exception as e:
        print(f"[Backup] Sync error: {e}")


def save_all():
    """Backup all data files now (blocking). Useful for manual trigger."""
    if not _enabled:
        return

    files = {}
    for filename in BACKUP_FILES:
        local_path = os.path.join(_data_dir, filename)
        if os.path.exists(local_path):
            try:
                with open(local_path, "r", encoding="utf-8") as f:
                    content = f.read()
                if content.strip():
                    files[filename] = {"content": content}
            except Exception:
                pass

    if not files:
        return

    try:
        with _lock:
            r = _requests.patch(
                f"https://api.github.com/gists/{_gist_id}",
                headers=_headers,
                json={"files": files},
                timeout=30,
            )
        if r.status_code == 200:
            print(f"[Backup] Full backup complete: {len(files)} files")
        else:
            print(f"[Backup] Full backup failed: HTTP {r.status_code}")
    except Exception as e:
        print(f"[Backup] Full backup error: {e}")


def status() -> dict:
    """Return backup status dict (for admin diagnostics endpoint)."""
    return {
        "enabled": _enabled,
        "gist_id": (_gist_id[:12] + "...") if _gist_id else "",
        "data_dir": _data_dir,
        "tracked_files": len(BACKUP_FILES),
        "last_synced_files": list(_last_mtimes.keys()),
    }


# ---------------------------------------------------------------------------
# CLI helper
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("gist_backup — cloud backup module for booking app")
    print()
    print("Set these environment variables on Render:")
    print("  GIST_TOKEN=ghp_your_github_personal_access_token")
    print("  GIST_ID=your_gist_id  (optional, auto-detected)")
