import json
from uuid import uuid4

from flask import Flask, request, jsonify, render_template_string, render_template
from flask_cors import CORS
import os
from datetime import datetime, timedelta
from threading import Lock
from ipaddress import ip_address
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import quote

app = Flask(__name__)

# ✅ Allow your connected sites
CORS(app, resources={r"/*": {"origins": [
    "https://payasyounow71.neocities.org",
    "https://booking-app-p8q8.onrender.com",
    "https://andyfast20-sketch.github.io"
]}})

BOOKINGS_FILE = "bookings.txt"
AVAIL_FILE = "availability.txt"
CONTACTS_FILE = "contacts.json"
CHAT_STATE_FILE = "chat_state.json"
AUTOPILOT_FILE = "autopilot.json"
BANNED_IPS_FILE = "banned_ips.json"
VISITOR_LOG_FILE = "visitor_log.json"
REVIEWS_FILE = "reviews.json"
CUSTOMER_SLOTS_FILE = "customer_slots.json"
CUSTOMER_SETTINGS_FILE = "customer_settings.json"
WEATHER_CONFIG_FILE = "weather_config.json"

CUSTOMER_ACCESS_CODE = os.getenv("CUSTOMER_ACCESS_CODE", "GARDENCARE2024")

DEFAULT_AUTOPILOT_MODEL = "deepseek-chat"
DEFAULT_AUTOPILOT_TEMPERATURE = 0.3
AUTOPILOT_PROFILE_LIMIT = 4000
AUTOPILOT_HISTORY_LIMIT = 12

VISITOR_TIMEOUT = timedelta(minutes=3)
LOCATION_CACHE_TTL = timedelta(hours=6)
WEATHER_CACHE_TTL = timedelta(minutes=45)
WEATHER_LOCATION_QUERY = "Audenshaw,Denton,UK"
INDEX_PAGES = {"/", "/index", "/index.html"}

_active_visitors = {}
_presence_lock = Lock()
_location_cache = {}
_chat_state_lock = Lock()
_chat_state = {"online": True, "sessions": {}}
_autopilot_lock = Lock()
_autopilot_config = {
    "enabled": False,
    "business_profile": "",
    "model": DEFAULT_AUTOPILOT_MODEL,
    "temperature": DEFAULT_AUTOPILOT_TEMPERATURE,
    "api_key": "",
    "api_keys": [],
}
_visitor_log_lock = Lock()
_visitor_log = {}
_banned_ips_lock = Lock()
_banned_ips = {}
_customer_settings_lock = Lock()
_customer_settings = {"access_code": CUSTOMER_ACCESS_CODE}
_weather_config_lock = Lock()
_weather_config = {"api_key": ""}
_weather_forecast_cache = {}


def _ensure_storage_file(path: str, *, default):
    """Ensure JSON-backed storage files always exist before use."""
    try:
        if os.path.exists(path):
            return
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(default, handle, indent=2)
    except OSError:
        # If the file cannot be created we silently ignore the error – the
        # in-memory store will continue to operate and future writes will
        # retry automatically.
        pass


def _default_reviews_payload():
    timestamp = datetime.utcnow().isoformat()
    return [
        {
            "id": str(uuid4()),
            "author": "Sarah Johnson",
            "location": "Northwood",
            "quote": "Pay As You Mow transformed my neglected garden into a beautiful oasis. Their flexible payment model meant I could get the help I needed without breaking the bank. Highly recommended!",
            "created_at": timestamp,
        },
        {
            "id": str(uuid4()),
            "author": "Michael Chen",
            "location": "Westfield",
            "quote": "As someone who travels frequently, I love that I can schedule garden maintenance only when I need it. The team is professional, reliable, and does an amazing job every time.",
            "created_at": timestamp,
        },
        {
            "id": str(uuid4()),
            "author": "Emma Davis",
            "location": "Riverside",
            "quote": "The quality of work is exceptional. My hedges have never looked better, and the lawn is always perfectly manicured. The pay-as-you-go model is perfect for my budget.",
            "created_at": timestamp,
        },
    ]


_ensure_storage_file(VISITOR_LOG_FILE, default={})
_ensure_storage_file(REVIEWS_FILE, default=_default_reviews_payload())
_ensure_storage_file(CUSTOMER_SLOTS_FILE, default=[])
_ensure_storage_file(CUSTOMER_SETTINGS_FILE, default={"access_code": CUSTOMER_ACCESS_CODE})
_ensure_storage_file(WEATHER_CONFIG_FILE, default={"api_key": ""})


def _load_customer_settings_from_disk() -> dict:
    if not os.path.exists(CUSTOMER_SETTINGS_FILE):
        return {"access_code": CUSTOMER_ACCESS_CODE}

    try:
        with open(CUSTOMER_SETTINGS_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
            if isinstance(payload, dict):
                return payload
    except (OSError, json.JSONDecodeError):
        return {"access_code": CUSTOMER_ACCESS_CODE}
    return {"access_code": CUSTOMER_ACCESS_CODE}


def _save_customer_settings_to_disk(settings: dict):
    try:
        with open(CUSTOMER_SETTINGS_FILE, "w", encoding="utf-8") as handle:
            json.dump(settings, handle, indent=2)
    except OSError:
        pass


def _get_customer_access_code() -> str:
    with _customer_settings_lock:
        code = _customer_settings.get("access_code") or CUSTOMER_ACCESS_CODE
    return code


def _update_customer_access_code(new_code: str):
    if not new_code:
        return
    global CUSTOMER_ACCESS_CODE
    with _customer_settings_lock:
        _customer_settings["access_code"] = new_code
        CUSTOMER_ACCESS_CODE = new_code
        _save_customer_settings_to_disk(_customer_settings)


def _load_weather_config_from_disk() -> dict:
    if not os.path.exists(WEATHER_CONFIG_FILE):
        return dict(_weather_config)

    try:
        with open(WEATHER_CONFIG_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return dict(_weather_config)

    if not isinstance(payload, dict):
        return dict(_weather_config)

    api_key = str(payload.get("api_key", "") or "")
    return {"api_key": api_key}


def _save_weather_config(config=None) -> None:
    snapshot = dict(config or _weather_config)
    try:
        with open(WEATHER_CONFIG_FILE, "w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2)
    except OSError:
        pass


def _weather_config_snapshot(*, include_secret: bool = False) -> dict:
    with _weather_config_lock:
        api_key = str(_weather_config.get("api_key", "") or "")

    env_key = os.environ.get("WEATHER_API_KEY", "").strip()
    has_api_key = bool(api_key or env_key)

    if include_secret:
        return {"api_key": api_key or env_key, "has_api_key": has_api_key}

    return {"has_api_key": has_api_key}


def _get_weather_api_key() -> str:
    with _weather_config_lock:
        configured = str(_weather_config.get("api_key", "") or "")

    env_key = os.environ.get("WEATHER_API_KEY", "").strip()
    return configured or env_key


def _fetch_forecast_for_date(date_obj: datetime, *, api_key: str):
    if not api_key or not isinstance(date_obj, datetime):
        return None

    cache_key = date_obj.strftime("%Y-%m-%d")
    cached = _weather_forecast_cache.get(cache_key)
    if cached and datetime.utcnow() - cached.get("timestamp", datetime.min) < WEATHER_CACHE_TTL:
        return cached.get("data")

    query_date = date_obj.strftime("%Y-%m-%d")
    encoded_location = quote(WEATHER_LOCATION_QUERY)
    url = (
        "https://api.weatherapi.com/v1/forecast.json"
        f"?key={api_key}&q={encoded_location}&dt={query_date}&aqi=no&alerts=no"
    )

    try:
        request = Request(url, headers={"User-Agent": "pay-as-you-mow-weather/1.0"})
        with urlopen(request, timeout=8) as response:
            if response.status != 200:
                return None
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, ValueError, OSError):
        return None

    _weather_forecast_cache[cache_key] = {"timestamp": datetime.utcnow(), "data": payload}
    return payload


def _summarize_hour_condition(hour_data: dict | None, day_data=None) -> dict:
    if hour_data is None:
        hour_data = {}
    if day_data is None:
        day_data = {}

    condition = hour_data.get("condition") or day_data.get("condition") or {}
    condition_text = str(condition.get("text") or "").strip()
    lower_text = condition_text.lower()

    try:
        chance_of_rain = float(hour_data.get("chance_of_rain", 0))
    except (TypeError, ValueError):
        chance_of_rain = 0.0

    try:
        cloud_cover = float(hour_data.get("cloud", 0))
    except (TypeError, ValueError):
        cloud_cover = 0.0

    precipitation = float(hour_data.get("precip_mm") or 0)

    if (
        "rain" in lower_text
        or "shower" in lower_text
        or "drizzle" in lower_text
        or chance_of_rain >= 50
        or precipitation > 0.05
    ):
        symbol = "🌧️"
        summary = condition_text or "Showers expected"
    elif "sun" in lower_text or ("clear" in lower_text and cloud_cover <= 50):
        symbol = "☀️"
        summary = condition_text or "Sunshine expected"
    else:
        symbol = "☁️"
        summary = condition_text or "Cloudy"

    return {"symbol": symbol, "summary": summary}


def _forecast_for_slot(slot: str, *, api_key: str | None = None):
    if not slot or " " not in slot:
        return None

    try:
        slot_dt = datetime.strptime(slot, "%Y-%m-%d %H:%M")
    except ValueError:
        return None

    api_key = api_key or _get_weather_api_key()
    if not api_key:
        return None

    forecast_payload = _fetch_forecast_for_date(slot_dt, api_key=api_key)
    if not forecast_payload:
        return None

    date_key = slot_dt.strftime("%Y-%m-%d")
    time_key = slot_dt.strftime("%H:00")

    forecast_block = None
    forecast_days = forecast_payload.get("forecast", {}).get("forecastday", [])
    for day_block in forecast_days:
        if day_block.get("date") != date_key:
            continue
        hours = day_block.get("hour") or []
        for hour_entry in hours:
            if str(hour_entry.get("time", "")).endswith(time_key):
                forecast_block = hour_entry
                break
        day_condition = (day_block.get("day") or {}).get("condition") or {}
        return _summarize_hour_condition(forecast_block, day_condition)

    return None


def _is_private_ip(ip_str: str) -> bool:
    if not ip_str:
        return True
    try:
        ip_obj = ip_address(ip_str)
    except ValueError:
        return True
    return any(
        [
            ip_obj.is_private,
            ip_obj.is_loopback,
            ip_obj.is_reserved,
            ip_obj.is_unspecified,
        ]
    )


def _lookup_location(ip_str: str) -> str:
    if not ip_str:
        return "Unknown location"
    if _is_private_ip(ip_str):
        return "Local network"

    now = datetime.utcnow()
    cached = _location_cache.get(ip_str)
    if cached and now - cached["timestamp"] < LOCATION_CACHE_TTL:
        return cached["location"]

    location = "Unknown location"
    try:
        request = Request(
            f"https://ipapi.co/{ip_str}/json/",
            headers={"User-Agent": "booking-app-presence/1.0"},
        )
        with urlopen(request, timeout=3) as response:
            if response.status == 200:
                payload = json.loads(response.read().decode("utf-8"))
                pieces = [
                    (payload.get("city") or "").strip(),
                    (payload.get("region") or "").strip(),
                    (payload.get("country_name") or "").strip(),
                ]
                location = ", ".join([piece for piece in pieces if piece]) or (
                    (payload.get("country_name") or "").strip() or "Unknown location"
                )
    except (HTTPError, URLError, TimeoutError, ValueError, OSError):
        location = "Unknown location"

    _location_cache[ip_str] = {"location": location, "timestamp": now}
    return location


def _client_ip() -> str:
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    remote_addr = request.remote_addr
    if remote_addr:
        return remote_addr
    return "unknown"


def _normalize_page_identifier(page: str) -> str:
    if not page:
        return "/"
    clean = str(page).strip()
    if not clean:
        return "/"
    clean = clean.split("#", 1)[0].split("?", 1)[0].strip()
    if not clean:
        return "/"
    if not clean.startswith("/"):
        clean = "/" + clean
    return clean


def _page_is_index(page: str) -> bool:
    normalized = _normalize_page_identifier(page).lower()
    return normalized in INDEX_PAGES


def _load_visitor_log_from_disk() -> dict:
    if not os.path.exists(VISITOR_LOG_FILE):
        return {}

    try:
        with open(VISITOR_LOG_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}

    if not isinstance(payload, dict):
        return {}

    entries = {}
    for ip_key, details in payload.items():
        record = details if isinstance(details, dict) else {}
        ip_value = (record.get("ip") or ip_key or "").strip()
        if not ip_value:
            continue

        record = dict(record)
        record["ip"] = ip_value

        pages = record.get("pages")
        if isinstance(pages, (list, tuple, set)):
            normalized_pages = {
                _normalize_page_identifier(page)
                for page in pages
                if isinstance(page, str) and page
            }
            record["pages"] = sorted(normalized_pages)

        visits = record.get("visits")
        if isinstance(visits, list):
            cleaned_visits = []
            for raw_visit in visits:
                if not isinstance(raw_visit, dict):
                    continue
                cleaned_visits.append(
                    {
                        "first_seen": raw_visit.get("first_seen") or "",
                        "last_seen": raw_visit.get("last_seen") or "",
                        "duration_seconds": float(raw_visit.get("duration_seconds") or 0.0),
                        "pages": sorted(
                            {
                                _normalize_page_identifier(page)
                                for page in raw_visit.get("pages", [])
                                if isinstance(page, str) and page
                            }
                        ),
                        "location": raw_visit.get("location") or "",
                        "user_agent": raw_visit.get("user_agent") or "",
                    }
                )
            record["visits"] = cleaned_visits

        record["visit_count"] = int(record.get("visit_count") or 0)
        record["total_duration_seconds"] = float(record.get("total_duration_seconds") or 0.0)
        record["visited_index"] = bool(record.get("visited_index", False))

        entries[ip_value] = record

    return entries


def _load_banned_ips_from_disk() -> dict:
    if not os.path.exists(BANNED_IPS_FILE):
        return {}
    try:
        with open(BANNED_IPS_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}

    entries = {}
    if isinstance(payload, dict):
        for ip_str, details in payload.items():
            clean_ip = (ip_str or "").strip()
            if not clean_ip:
                continue
            record = details if isinstance(details, dict) else {}
            entries[clean_ip] = {
                "ip": clean_ip,
                "banned_at": (record.get("banned_at") or ""),
                "reason": (record.get("reason") or ""),
            }
    elif isinstance(payload, list):
        for raw_entry in payload:
            if not isinstance(raw_entry, dict):
                continue
            clean_ip = (raw_entry.get("ip") or "").strip()
            if not clean_ip:
                continue
            entries[clean_ip] = {
                "ip": clean_ip,
                "banned_at": (raw_entry.get("banned_at") or ""),
                "reason": (raw_entry.get("reason") or ""),
            }
    return entries


def _save_visitor_log(snapshot=None) -> None:
    payload = dict(snapshot or _visitor_log)
    try:
        with open(VISITOR_LOG_FILE, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    except OSError:
        pass


def _save_banned_ips(snapshot=None) -> None:
    payload = dict(snapshot or _banned_ips)
    try:
        with open(BANNED_IPS_FILE, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    except OSError:
        pass


def _update_visitor_history(ip_str: str, visitor_entry: dict) -> None:
    if not ip_str or not visitor_entry:
        return

    first_seen = visitor_entry.get("first_seen")
    last_seen = visitor_entry.get("last_seen")
    if not isinstance(first_seen, datetime) or not isinstance(last_seen, datetime):
        return

    pages = visitor_entry.get("pages")
    if isinstance(pages, set):
        pages = list(pages)
    elif not isinstance(pages, (list, tuple)):
        pages = []

    normalized_pages = {_normalize_page_identifier(page) for page in pages if page}
    page = visitor_entry.get("page") or ""
    normalized_page = _normalize_page_identifier(page)
    normalized_pages.add(normalized_page)

    visited_index = visitor_entry.get("visited_index") or _page_is_index(normalized_page)

    first_seen_iso = first_seen.isoformat() + "Z"
    last_seen_iso = last_seen.isoformat() + "Z"
    duration_seconds = max(0.0, (last_seen - first_seen).total_seconds())
    location = visitor_entry.get("location") or ""
    user_agent = visitor_entry.get("user_agent") or ""

    with _visitor_log_lock:
        record = _visitor_log.setdefault(
            ip_str,
            {
                "ip": ip_str,
                "first_seen": first_seen_iso,
                "last_seen": last_seen_iso,
                "location": location,
                "user_agent": user_agent,
                "pages": [],
                "visits": [],
                "visit_count": 0,
                "total_duration_seconds": 0.0,
                "current_visit": None,
                "visited_index": visited_index,
            },
        )

        record["ip"] = ip_str
        if record.get("first_seen") in {"", None} or record["first_seen"] > first_seen_iso:
            record["first_seen"] = first_seen_iso
        if record.get("last_seen") in {"", None} or record["last_seen"] < last_seen_iso:
            record["last_seen"] = last_seen_iso

        if location and location != "Unknown location":
            record["location"] = location
        if user_agent:
            record["user_agent"] = user_agent

        existing_visits = record.get("visits")
        if not isinstance(existing_visits, list):
            existing_visits = []
            record["visits"] = existing_visits

        combined_pages = set(record.get("pages", []))
        combined_pages.update(normalized_pages)
        record["pages"] = sorted(combined_pages)

        record["visited_index"] = bool(record.get("visited_index")) or visited_index
        record["current_visit"] = {
            "first_seen": first_seen_iso,
            "last_seen": last_seen_iso,
            "duration_seconds": duration_seconds,
            "pages": sorted(normalized_pages),
            "location": location,
        }

        # Include the active visit in the running count so live visitors appear
        # in the index history with a meaningful visit number.
        record["visit_count"] = max(len(existing_visits) + 1, int(record.get("visit_count", 0)))
        record["total_duration_seconds"] = float(record.get("total_duration_seconds", 0.0))

        _visitor_log[ip_str] = record

    _save_visitor_log()


def _finalize_visitor_session(ip_str: str, visitor_entry: dict) -> None:
    if not ip_str or not visitor_entry:
        return

    first_seen = visitor_entry.get("first_seen")
    last_seen = visitor_entry.get("last_seen")
    if not isinstance(first_seen, datetime) or not isinstance(last_seen, datetime):
        return

    pages = visitor_entry.get("pages")
    if isinstance(pages, set):
        pages = list(pages)
    elif not isinstance(pages, (list, tuple)):
        pages = []

    normalized_pages = {_normalize_page_identifier(page) for page in pages if page}
    page = visitor_entry.get("page") or ""
    normalized_page = _normalize_page_identifier(page)
    normalized_pages.add(normalized_page)

    visited_index = visitor_entry.get("visited_index") or any(
        _page_is_index(candidate) for candidate in normalized_pages
    )
    if not visited_index:
        return

    first_seen_iso = first_seen.isoformat() + "Z"
    last_seen_iso = last_seen.isoformat() + "Z"
    duration_seconds = max(0.0, (last_seen - first_seen).total_seconds())
    location = visitor_entry.get("location") or ""
    user_agent = visitor_entry.get("user_agent") or ""

    visit_entry = {
        "first_seen": first_seen_iso,
        "last_seen": last_seen_iso,
        "duration_seconds": duration_seconds,
        "pages": sorted(normalized_pages),
        "location": location,
        "user_agent": user_agent,
    }

    with _visitor_log_lock:
        record = _visitor_log.setdefault(
            ip_str,
            {
                "ip": ip_str,
                "first_seen": first_seen_iso,
                "last_seen": last_seen_iso,
                "location": location,
                "user_agent": user_agent,
                "pages": sorted(normalized_pages),
                "visits": [],
                "visit_count": 0,
                "total_duration_seconds": 0.0,
                "current_visit": None,
                "visited_index": True,
            },
        )

        record["ip"] = ip_str
        if record.get("first_seen") in {"", None} or record["first_seen"] > first_seen_iso:
            record["first_seen"] = first_seen_iso
        if record.get("last_seen") in {"", None} or record["last_seen"] < last_seen_iso:
            record["last_seen"] = last_seen_iso

        if location and location != "Unknown location":
            record["location"] = location
        if user_agent:
            record["user_agent"] = user_agent

        combined_pages = set(record.get("pages", []))
        combined_pages.update(normalized_pages)
        record["pages"] = sorted(combined_pages)

        record_visits = record.setdefault("visits", [])
        record_visits.append(visit_entry)
        record["visits"] = record_visits
        record["visit_count"] = len(record_visits)
        record["total_duration_seconds"] = float(record.get("total_duration_seconds", 0.0)) + duration_seconds
        record["current_visit"] = None
        record["visited_index"] = True

        _visitor_log[ip_str] = record

    _save_visitor_log()


def _visitor_log_snapshot(include_current=True) -> list:
    with _visitor_log_lock:
        records = []
        for ip_key, record in _visitor_log.items():
            ip_value = (record.get("ip") or ip_key or "").strip()
            entry = {
                "ip": ip_value,
                "first_seen": record.get("first_seen"),
                "last_seen": record.get("last_seen"),
                "location": record.get("location", ""),
                "user_agent": record.get("user_agent", ""),
                "pages": list(record.get("pages", [])),
                "visits": list(record.get("visits", [])),
                "visit_count": int(record.get("visit_count", 0)),
                "total_duration_seconds": float(record.get("total_duration_seconds", 0.0)),
                "current_visit": record.get("current_visit") if include_current else None,
                "visited_index": bool(record.get("visited_index", False)),
            }
            records.append(entry)
    records.sort(key=lambda item: item.get("last_seen") or "", reverse=True)
    return records


def _prune_visitors(now: datetime) -> None:
    stale_entries = []
    with _presence_lock:
        stale_keys = [
            key
            for key, details in _active_visitors.items()
            if now - details["last_seen"] > VISITOR_TIMEOUT
        ]
        for key in stale_keys:
            entry = _active_visitors.pop(key, None)
            if entry:
                stale_entries.append((key, entry))

    for ip_str, visitor_entry in stale_entries:
        _finalize_visitor_session(ip_str, visitor_entry)


def _record_presence(data: dict) -> None:
    ip_str = _client_ip()
    now = datetime.utcnow()
    location = _lookup_location(ip_str)
    page = (data.get("page") or "").strip()
    user_agent = (request.headers.get("User-Agent") or "").strip()
    normalized_page = _normalize_page_identifier(page)

    with _presence_lock:
        entry = _active_visitors.get(ip_str)
        if entry:
            entry["last_seen"] = now
            entry["page"] = normalized_page
            if location and location != "Unknown location":
                entry["location"] = location
            if user_agent:
                entry["user_agent"] = user_agent
            pages = entry.setdefault("pages", set())
            pages.add(normalized_page)
            if _page_is_index(normalized_page):
                entry["visited_index"] = True
            _active_visitors[ip_str] = entry
        else:
            entry = {
                "ip": ip_str,
                "location": location,
                "page": normalized_page,
                "user_agent": user_agent,
                "first_seen": now,
                "last_seen": now,
                "pages": {normalized_page},
                "visited_index": _page_is_index(normalized_page),
            }
            _active_visitors[ip_str] = entry

    if entry.get("visited_index"):
        _update_visitor_history(ip_str, entry)


def _is_ip_banned(ip_str: str) -> bool:
    if not ip_str:
        return False
    with _banned_ips_lock:
        return ip_str in _banned_ips


def _discard_active_visitor(ip_str: str) -> None:
    if not ip_str:
        return
    with _presence_lock:
        _active_visitors.pop(ip_str, None)


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_api_keys(raw_keys):
    normalized = []
    seen = set()
    if isinstance(raw_keys, list):
        for entry in raw_keys:
            if isinstance(entry, dict):
                value = str(entry.get("value") or entry.get("api_key") or "").strip()
                key_id = str(entry.get("id") or uuid4())
                created_at = str(entry.get("created_at") or datetime.utcnow().isoformat())
            elif isinstance(entry, str):
                value = entry.strip()
                if not value:
                    continue
                key_id = str(uuid4())
                created_at = datetime.utcnow().isoformat()
            else:
                continue

            if not value or value in seen:
                continue

            normalized.append({"id": key_id, "value": value, "created_at": created_at})
            seen.add(value)

    return normalized


def _merge_api_key(value: str, existing=None):
    normalized_existing = _normalize_api_keys(existing or [])
    if not value:
        return normalized_existing

    deduped = [entry for entry in normalized_existing if entry.get("value") != value]
    deduped.insert(
        0,
        {
            "id": str(uuid4()),
            "value": value,
            "created_at": datetime.utcnow().isoformat(),
        },
    )
    return deduped


def _coerce_autopilot_config(payload: dict, *, base=None) -> dict:
    reference = dict(base or _autopilot_config)
    api_keys = _normalize_api_keys(reference.get("api_keys", []))
    result = {
        "enabled": bool(reference.get("enabled", False)),
        "business_profile": str(reference.get("business_profile", "") or ""),
        "model": str(reference.get("model", DEFAULT_AUTOPILOT_MODEL) or DEFAULT_AUTOPILOT_MODEL),
        "temperature": float(reference.get("temperature", DEFAULT_AUTOPILOT_TEMPERATURE)),
        "api_key": str(reference.get("api_key", "") or ""),
        "api_keys": api_keys,
    }

    if payload is None:
        payload = {}

    if "enabled" in payload:
        requested = payload.get("enabled")
        if isinstance(requested, str):
            requested = requested.strip().lower() in {"1", "true", "yes", "on"}
        result["enabled"] = bool(requested)

    if "business_profile" in payload:
        text = str(payload.get("business_profile") or "").strip()
        if len(text) > AUTOPILOT_PROFILE_LIMIT:
            text = text[:AUTOPILOT_PROFILE_LIMIT]
        result["business_profile"] = text

    if "model" in payload:
        model = str(payload.get("model") or "").strip() or DEFAULT_AUTOPILOT_MODEL
        result["model"] = model

    if "temperature" in payload:
        temperature = payload.get("temperature")
        try:
            temperature = float(temperature)
        except (TypeError, ValueError):
            temperature = reference.get("temperature", DEFAULT_AUTOPILOT_TEMPERATURE)
        temperature = max(0.0, min(2.0, temperature))
        result["temperature"] = temperature

    if "api_keys" in payload:
        incoming_keys = _normalize_api_keys(payload.get("api_keys") or [])
        if incoming_keys:
            result["api_keys"] = incoming_keys

    if "api_key" in payload:
        incoming_key = str(payload.get("api_key") or "").strip()
        if incoming_key:
            result["api_key"] = incoming_key
            result["api_keys"] = _merge_api_key(incoming_key, api_keys)
        elif "api_keys" not in payload:
            result["api_key"] = ""
            result["api_keys"] = []

    return result


def _load_autopilot_config_from_disk() -> dict:
    if not os.path.exists(AUTOPILOT_FILE):
        return dict(_autopilot_config)

    try:
        with open(AUTOPILOT_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return dict(_autopilot_config)

    if not isinstance(payload, dict):
        return dict(_autopilot_config)

    return _coerce_autopilot_config(payload, base=_autopilot_config)


def _save_autopilot_config(config=None) -> None:
    snapshot = dict(config or _autopilot_config)
    try:
        with open(AUTOPILOT_FILE, "w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2)
    except OSError:
        pass


def _autopilot_config_snapshot(*, include_secret: bool = False) -> dict:
    with _autopilot_lock:
        snapshot = {
            "enabled": bool(_autopilot_config.get("enabled", False)),
            "business_profile": str(_autopilot_config.get("business_profile", "") or ""),
            "model": str(_autopilot_config.get("model", DEFAULT_AUTOPILOT_MODEL) or DEFAULT_AUTOPILOT_MODEL),
            "temperature": float(_autopilot_config.get("temperature", DEFAULT_AUTOPILOT_TEMPERATURE)),
            "api_key": str(_autopilot_config.get("api_key", "") or ""),
            "api_keys": list(_autopilot_config.get("api_keys", [])),
        }

    if include_secret:
        return snapshot

    env_key_present = bool(
        (
            os.environ.get("DEEPSEEK_API_KEY")
            or os.environ.get("OPENAI_API_KEY")
            or ""
        ).strip()
    )
    keys_payload = snapshot.get("api_keys", [])
    if not isinstance(keys_payload, list):
        keys_payload = []

    visible_keys = []
    for entry in keys_payload:
        value = str(entry.get("value") or "").strip()
        if not value:
            continue
        visible_keys.append(
            {
                "id": entry.get("id"),
                "label": f"Key ending {value[-4:]}" if len(value) >= 4 else "Saved API key",
                "created_at": entry.get("created_at", ""),
                "last4": value[-4:] if len(value) >= 4 else value,
            }
        )

    has_api_key = bool(snapshot.get("api_key")) or bool(visible_keys) or env_key_present
    snapshot.pop("api_key", None)
    snapshot["api_keys"] = visible_keys
    snapshot["has_api_key"] = has_api_key
    return snapshot


def _load_chat_state_from_disk():
    if not os.path.exists(CHAT_STATE_FILE):
        return {"online": True, "sessions": {}}

    try:
        with open(CHAT_STATE_FILE, "r", encoding="utf-8") as file:
            payload = json.load(file)
    except (OSError, json.JSONDecodeError):
        return {"online": True, "sessions": {}}

    sessions_payload = payload.get("sessions")
    sessions = {}
    if isinstance(sessions_payload, dict):
        for key, raw_session in sessions_payload.items():
            if not isinstance(raw_session, dict):
                continue

            session_id = str(raw_session.get("session_id") or key)
            created_at = raw_session.get("created_at") or datetime.utcnow().isoformat()
            last_seen = raw_session.get("last_seen") or created_at
            visitor = raw_session.get("visitor") if isinstance(raw_session.get("visitor"), dict) else {}

            messages = []
            raw_messages = raw_session.get("messages")
            if isinstance(raw_messages, list):
                for raw_message in raw_messages:
                    if not isinstance(raw_message, dict):
                        continue

                    text = raw_message.get("text")
                    if text is None:
                        continue

                    message_id = _safe_int(raw_message.get("id"), default=None)
                    if message_id is None:
                        continue

                    timestamp = raw_message.get("timestamp") or datetime.utcnow().isoformat()
                    sender = "admin" if raw_message.get("sender") == "admin" else "visitor"
                    messages.append(
                        {
                            "id": message_id,
                            "sender": sender,
                            "text": str(text),
                            "timestamp": timestamp,
                        }
                    )

            messages.sort(key=lambda entry: entry["id"])
            next_id = messages[-1]["id"] + 1 if messages else 1

            sessions[session_id] = {
                "session_id": session_id,
                "created_at": created_at,
                "last_seen": last_seen,
                "visitor": visitor,
                "messages": messages,
                "next_id": next_id,
                "last_admin_read": _safe_int(raw_session.get("last_admin_read"), 0),
                "last_visitor_read": _safe_int(raw_session.get("last_visitor_read"), 0),
            }

    return {"online": bool(payload.get("online", True)), "sessions": sessions}


def _save_chat_state():
    with _chat_state_lock:
        payload = {
            "online": bool(_chat_state.get("online", True)),
            "sessions": {},
        }

        for session_id, session in _chat_state.get("sessions", {}).items():
            payload["sessions"][session_id] = {
                "session_id": session.get("session_id", session_id),
                "created_at": session.get("created_at"),
                "last_seen": session.get("last_seen"),
                "visitor": session.get("visitor", {}),
                "messages": list(session.get("messages", [])),
                "next_id": session.get("next_id", 1),
                "last_admin_read": session.get("last_admin_read", 0),
                "last_visitor_read": session.get("last_visitor_read", 0),
            }

    try:
        with open(CHAT_STATE_FILE, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2)
    except OSError:
        pass


with _chat_state_lock:
    stored_chat_state = _load_chat_state_from_disk()
    _chat_state.update(stored_chat_state)


with _autopilot_lock:
    stored_autopilot = _load_autopilot_config_from_disk()
    _autopilot_config.update(stored_autopilot)


with _weather_config_lock:
    stored_weather = _load_weather_config_from_disk()
    _weather_config.update(stored_weather)


with _customer_settings_lock:
    stored_customer_settings = _load_customer_settings_from_disk()
    if isinstance(stored_customer_settings, dict):
        _customer_settings.update(stored_customer_settings)
        CUSTOMER_ACCESS_CODE = _customer_settings.get("access_code", CUSTOMER_ACCESS_CODE)


with _visitor_log_lock:
    stored_visitor_log = _load_visitor_log_from_disk()
    if isinstance(stored_visitor_log, dict):
        _visitor_log.update(stored_visitor_log)


with _banned_ips_lock:
    stored_banned_ips = _load_banned_ips_from_disk()
    if isinstance(stored_banned_ips, dict):
        _banned_ips.update(stored_banned_ips)


@app.before_request
def enforce_banned_ips():
    ip_str = _client_ip()
    if not ip_str:
        return None
    if not _is_ip_banned(ip_str):
        return None

    accepts_json = (
        request.accept_mimetypes["application/json"]
        >= request.accept_mimetypes["text/html"]
    )
    message = {"message": "Access revoked."}

    if accepts_json:
        return jsonify(message), 403

    page = render_template_string(
        """
        <!doctype html>
        <html lang="en">
        <head>
            <meta charset="utf-8" />
            <title>Access revoked</title>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                    background: #0b1623;
                    color: #f8fbff;
                    min-height: 100vh;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    margin: 0;
                }
                main {
                    text-align: center;
                    padding: 3rem 2rem;
                    border-radius: 18px;
                    background: rgba(255, 255, 255, 0.06);
                    border: 1px solid rgba(255, 255, 255, 0.12);
                    max-width: 480px;
                    box-shadow: 0 18px 40px rgba(8, 16, 44, 0.4);
                }
                h1 {
                    margin-bottom: 1rem;
                    font-size: 1.85rem;
                    letter-spacing: 0.03em;
                }
                p {
                    margin: 0;
                    font-size: 1rem;
                    line-height: 1.6;
                }
            </style>
        </head>
        <body>
            <main>
                <h1>Access revoked</h1>
                <p>Your IP address has been blocked from accessing this site.</p>
            </main>
        </body>
        </html>
        """
    )
    return page, 403


def _append_chat_message(
    session: dict,
    sender: str,
    text: str,
    message_type: str = "message",
) -> dict:
    clean_text = (text or "").strip()
    if not clean_text:
        raise ValueError("Message text is required")

    timestamp = datetime.utcnow().isoformat()
    message_id = session.get("next_id", 1)
    entry = {
        "id": message_id,
        "sender": sender,
        "text": clean_text,
        "timestamp": timestamp,
        "type": message_type or "message",
    }

    session.setdefault("messages", []).append(entry)
    session["next_id"] = message_id + 1
    session["last_seen"] = timestamp
    return entry


def _ensure_chat_session(session_id: str = "", *, page: str = "", ip_str: str = "", location: str = "", user_agent: str = ""):
    cleaned_id = (session_id or "").strip()
    now_iso = datetime.utcnow().isoformat()
    with _chat_state_lock:
        sessions = _chat_state.setdefault("sessions", {})
        session = sessions.get(cleaned_id)
        if not session:
            cleaned_id = cleaned_id or str(uuid4())
            session = {
                "session_id": cleaned_id,
                "created_at": now_iso,
                "last_seen": now_iso,
                "visitor": {},
                "messages": [],
                "next_id": 1,
                "last_admin_read": 0,
                "last_visitor_read": 0,
            }
            sessions[cleaned_id] = session
        else:
            session.setdefault("created_at", now_iso)
            session["last_seen"] = now_iso

        visitor = session.setdefault("visitor", {})
        if ip_str:
            visitor["ip"] = ip_str
        if location:
            visitor["location"] = location
        if user_agent:
            visitor["user_agent"] = user_agent[:280]
        if page:
            visitor["last_page"] = page

        snapshot = {
            "session_id": session.get("session_id", cleaned_id),
            "created_at": session.get("created_at", now_iso),
            "last_seen": session.get("last_seen", now_iso),
            "visitor": dict(session.get("visitor", {})),
            "messages": list(session.get("messages", [])),
            "next_id": session.get("next_id", 1),
            "last_admin_read": session.get("last_admin_read", 0),
            "last_visitor_read": session.get("last_visitor_read", 0),
        }

    return cleaned_id, snapshot


# Helper used by admin tooling to locate a visitor session by IP
def _get_session_id_for_ip(ip_str: str) -> str:
    if not ip_str:
        return ""

    best_id = ""
    best_last_seen = ""
    with _chat_state_lock:
        for session_id, session in _chat_state.get("sessions", {}).items():
            visitor = session.get("visitor", {})
            if visitor.get("ip") != ip_str:
                continue

            last_seen = session.get("last_seen") or session.get("created_at") or ""
            if not best_id or (last_seen and last_seen > best_last_seen):
                best_id = session_id
                best_last_seen = last_seen

    return best_id


# --- Autopilot helpers ---


def _build_autopilot_messages(conversation, config: dict) -> list:
    business_profile = str(config.get("business_profile", "") or "").strip()
    if not business_profile:
        business_profile = "No additional business context has been provided."

    instructions = (
        "You are Autopilot, a friendly live chat assistant for a gardening and maintenance business. "
        "Answer website visitor questions using the business knowledge provided below and the ongoing conversation. "
        "If you are unsure or the visitor asks for something that is not covered, politely let them know a member of the team will follow up. "
        "Keep replies concise, helpful and avoid making up details."
    )

    messages = [
        {
            "role": "system",
            "content": f"{instructions}\n\nBusiness knowledge:\n{business_profile}",
        }
    ]

    history = list(conversation or [])[-AUTOPILOT_HISTORY_LIMIT:]
    for entry in history:
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("text") or "").strip()
        if not text:
            continue
        if str(entry.get("type") or "") == "invite":
            continue
        sender = entry.get("sender")
        role = "assistant" if sender in {"admin", "autopilot"} else "user"
        messages.append({"role": role, "content": text})

    return messages


def _request_autopilot_reply(messages, *, model: str, temperature: float, api_key: str) -> str:
    if not api_key or not messages:
        return ""

    payload = {
        "model": model or DEFAULT_AUTOPILOT_MODEL,
        "messages": messages,
        "temperature": float(temperature),
        "max_tokens": 350,
    }

    request = Request(
        "https://api.deepseek.com/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )

    try:
        with urlopen(request, timeout=15) as response:
            if getattr(response, "status", 200) != 200:
                return ""
            raw = response.read().decode("utf-8")
    except (HTTPError, URLError, TimeoutError, ValueError, OSError):
        return ""

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return ""

    choices = parsed.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""

    message_payload = choices[0].get("message") if isinstance(choices[0], dict) else {}
    if not isinstance(message_payload, dict):
        return ""

    content = message_payload.get("content")
    if isinstance(content, str):
        return content.strip()

    return ""


def _resolve_autopilot_api_key() -> str:
    env_key = (os.environ.get("DEEPSEEK_API_KEY") or "").strip() or (
        os.environ.get("OPENAI_API_KEY") or ""
    ).strip()
    with _autopilot_lock:
        stored_keys = _autopilot_config.get("api_keys") or []
        primary_key = ""
        if stored_keys:
            first_entry = next(
                (entry for entry in stored_keys if str(entry.get("value") or "").strip()),
                None,
            )
            primary_key = str(first_entry.get("value") or "").strip() if first_entry else ""
        stored_key = primary_key or str(_autopilot_config.get("api_key") or "").strip()
    return stored_key or env_key


def _maybe_send_autopilot_reply(session_id: str, conversation=None):
    config = _autopilot_config_snapshot(include_secret=True)
    if not config.get("enabled"):
        return None

    api_key = _resolve_autopilot_api_key()
    if not api_key:
        return None

    messages = _build_autopilot_messages(conversation or [], config)
    if len(messages) <= 1:
        return None

    reply_text = _request_autopilot_reply(
        messages,
        model=config.get("model", DEFAULT_AUTOPILOT_MODEL),
        temperature=config.get("temperature", DEFAULT_AUTOPILOT_TEMPERATURE),
        api_key=api_key,
    )

    clean_reply = (reply_text or "").strip()
    if not clean_reply:
        return None

    with _chat_state_lock:
        session = _chat_state.get("sessions", {}).get(session_id)
        if not session:
            return None
        try:
            entry = _append_chat_message(session, "autopilot", clean_reply, message_type="autopilot")
        except ValueError:
            return None

    _save_chat_state()
    return entry


# --- Live chat endpoints ---


@app.route("/chat/status", methods=["GET"])
def chat_status():
    with _chat_state_lock:
        online = bool(_chat_state.get("online", True))
    return jsonify({"online": online})


@app.route("/chat/session", methods=["POST"])
def chat_session():
    payload = request.get_json(silent=True) or {}
    requested_id = (payload.get("session_id") or "").strip()
    page = (payload.get("page") or "").strip()
    user_agent = (request.headers.get("User-Agent") or "").strip()
    ip_str = _client_ip()

    with _chat_state_lock:
        existing_session = _chat_state.get("sessions", {}).get(requested_id)

    location = _lookup_location(ip_str) if not existing_session else existing_session.get("visitor", {}).get("location", "")

    session_id, _ = _ensure_chat_session(
        requested_id,
        page=page,
        ip_str=ip_str,
        location=location,
        user_agent=user_agent,
    )

    with _chat_state_lock:
        session = _chat_state.get("sessions", {}).get(session_id)
        if not session:
            return jsonify({"message": "Unable to create chat session."}), 500

        messages = list(session.get("messages", []))
        if messages:
            session["last_visitor_read"] = max(session.get("last_visitor_read", 0), messages[-1]["id"])
        online = bool(_chat_state.get("online", True))

    _save_chat_state()
    return jsonify({"session_id": session_id, "online": online, "messages": messages[-50:]})


@app.route("/chat/messages", methods=["GET"])
def chat_messages():
    session_id = (request.args.get("session_id") or "").strip()
    if not session_id:
        return jsonify({"message": "Session ID is required."}), 400

    after_id = _safe_int(request.args.get("after"), 0)
    page = (request.args.get("page") or "").strip()
    user_agent = (request.headers.get("User-Agent") or "").strip()
    ip_str = _client_ip()

    with _chat_state_lock:
        session_exists = session_id in _chat_state.get("sessions", {})

    location = _lookup_location(ip_str) if not session_exists else ""

    session_id, _ = _ensure_chat_session(
        session_id,
        page=page,
        ip_str=ip_str,
        location=location,
        user_agent=user_agent,
    )

    response_payload = {"session_id": session_id, "messages": [], "online": True}

    with _chat_state_lock:
        session = _chat_state.get("sessions", {}).get(session_id)
        if not session:
            return jsonify({"message": "Session not found."}), 404

        messages = [message for message in session.get("messages", []) if message.get("id", 0) > after_id]
        response_payload["messages"] = messages
        response_payload["online"] = bool(_chat_state.get("online", True))

        if messages:
            session["last_visitor_read"] = max(session.get("last_visitor_read", 0), messages[-1]["id"])
        session["last_seen"] = datetime.utcnow().isoformat()

    _save_chat_state()
    return jsonify(response_payload)


@app.route("/chat/send", methods=["POST"])
def chat_send():
    payload = request.get_json(silent=True) or {}
    session_id = (payload.get("session_id") or "").strip()
    message = (payload.get("message") or "").strip()
    page = (payload.get("page") or "").strip()

    if not session_id:
        return jsonify({"message": "Session ID is required."}), 400
    if not message:
        return jsonify({"message": "Message text is required."}), 400

    user_agent = (request.headers.get("User-Agent") or "").strip()
    ip_str = _client_ip()

    with _chat_state_lock:
        online = bool(_chat_state.get("online", True))
    if not online:
        return jsonify({"message": "Live chat is currently offline."}), 503

    with _chat_state_lock:
        existing_session = _chat_state.get("sessions", {}).get(session_id)

    location = _lookup_location(ip_str) if not existing_session else ""

    session_id, _ = _ensure_chat_session(
        session_id,
        page=page,
        ip_str=ip_str,
        location=location,
        user_agent=user_agent,
    )

    if not session_id:
        return jsonify({"message": "Unable to create chat session."}), 500

    conversation_snapshot = []
    with _chat_state_lock:
        session = _chat_state.get("sessions", {}).get(session_id)
        if not session:
            return jsonify({"message": "Session not found."}), 404

        try:
            entry = _append_chat_message(session, "visitor", message)
        except ValueError:
            return jsonify({"message": "Message text is required."}), 400

        session["last_visitor_read"] = entry["id"]
        visitor = session.setdefault("visitor", {})
        visitor["ip"] = ip_str
        if location:
            visitor["location"] = location
        if user_agent:
            visitor["user_agent"] = user_agent[:280]
        if page:
            visitor["last_page"] = page
        conversation_snapshot = [dict(item) for item in session.get("messages", [])]

    _save_chat_state()
    autopilot_entry = _maybe_send_autopilot_reply(session_id, conversation_snapshot)

    response_payload = {"message": "Message sent.", "entry": entry, "session_id": session_id}
    if autopilot_entry:
        response_payload["autopilot_entry"] = autopilot_entry

    return jsonify(response_payload)


@app.route("/admin/chat/status", methods=["POST"])
def admin_chat_status():
    payload = request.get_json(silent=True) or {}
    requested_state = payload.get("online")
    if isinstance(requested_state, str):
        requested_state = requested_state.strip().lower() in {"1", "true", "yes", "on"}
    else:
        requested_state = bool(requested_state)

    with _chat_state_lock:
        _chat_state["online"] = bool(requested_state)
        online = bool(_chat_state["online"])

    _save_chat_state()
    return jsonify({"message": "Chat status updated.", "online": online})


@app.route("/admin/chat/close", methods=["POST"])
def admin_chat_close():
    payload = request.get_json(silent=True) or {}
    session_id = (payload.get("session_id") or "").strip()

    if not session_id:
        return jsonify({"message": "Session ID is required."}), 400

    with _chat_state_lock:
        removed = _chat_state.get("sessions", {}).pop(session_id, None)
        online = bool(_chat_state.get("online", True))

    if not removed:
        return jsonify({"message": "Session not found."}), 404

    _save_chat_state()
    return jsonify({"message": "Chat session closed and hidden from the panel.", "online": online})


@app.route("/admin/autopilot/config", methods=["GET", "POST"])
def admin_autopilot_config():
    if request.method == "GET":
        return jsonify({"config": _autopilot_config_snapshot()})

    payload = request.get_json(silent=True) or {}

    with _autopilot_lock:
        updated = _coerce_autopilot_config(payload, base=_autopilot_config)
        _autopilot_config.update(updated)
        snapshot = dict(_autopilot_config)

    _save_autopilot_config(snapshot)
    return jsonify({"message": "Autopilot settings updated.", "config": _autopilot_config_snapshot()})


@app.route("/admin/weather/config", methods=["GET", "POST"])
def admin_weather_config():
    if request.method == "GET":
        return jsonify({"config": _weather_config_snapshot()})

    payload = request.get_json(silent=True) or {}
    incoming_key = str(payload.get("api_key", "") or "").strip()

    with _weather_config_lock:
        if incoming_key:
            _weather_config["api_key"] = incoming_key
        elif "api_key" in payload:
            _weather_config["api_key"] = ""
        snapshot = dict(_weather_config)

    _save_weather_config(snapshot)
    _weather_forecast_cache.clear()
    return jsonify({"message": "Weather settings updated.", "config": _weather_config_snapshot()})


@app.route("/admin/chat/sessions", methods=["GET"])
def admin_chat_sessions():
    with _chat_state_lock:
        online = bool(_chat_state.get("online", True))
        sessions = []
        for session in _chat_state.get("sessions", {}).values():
            messages = list(session.get("messages", []))
            last_message = messages[-1] if messages else {}
            last_admin_read = _safe_int(session.get("last_admin_read"), 0)
            unread = sum(
                1
                for message in messages
                if message.get("sender") == "visitor" and _safe_int(message.get("id"), 0) > last_admin_read
            )

            sessions.append(
                {
                    "session_id": session.get("session_id"),
                    "created_at": session.get("created_at"),
                    "last_seen": session.get("last_seen"),
                    "visitor": session.get("visitor", {}),
                    "last_message": last_message.get("text", ""),
                    "last_message_timestamp": last_message.get("timestamp", session.get("last_seen")),
                    "unread_from_visitor": unread,
                    "message_count": len(messages),
                }
            )

    sessions.sort(key=lambda entry: entry.get("last_message_timestamp") or entry.get("last_seen") or "", reverse=True)
    return jsonify({"online": online, "sessions": sessions})


@app.route("/admin/chat/messages/<session_id>", methods=["GET"])
def admin_chat_messages(session_id):
    after_id = _safe_int(request.args.get("after"), 0)

    with _chat_state_lock:
        session = _chat_state.get("sessions", {}).get(session_id)
        if not session:
            return jsonify({"message": "Session not found."}), 404

        messages = [message for message in session.get("messages", []) if message.get("id", 0) > after_id]
        if messages:
            session["last_admin_read"] = max(session.get("last_admin_read", 0), messages[-1]["id"])
        online = bool(_chat_state.get("online", True))

    _save_chat_state()
    return jsonify({"session_id": session_id, "messages": messages, "online": online})


@app.route("/admin/chat/send", methods=["POST"])
def admin_chat_send():
    payload = request.get_json(silent=True) or {}
    session_id = (payload.get("session_id") or "").strip()
    message = (payload.get("message") or "").strip()

    if not session_id:
        return jsonify({"message": "Session ID is required."}), 400
    if not message:
        return jsonify({"message": "Message text is required."}), 400

    with _chat_state_lock:
        session = _chat_state.get("sessions", {}).get(session_id)
        if not session:
            return jsonify({"message": "Session not found."}), 404

        try:
            entry = _append_chat_message(session, "admin", message)
        except ValueError:
            return jsonify({"message": "Message text is required."}), 400

        session["last_admin_read"] = entry["id"]

    _save_chat_state()
    return jsonify({"message": "Message sent.", "entry": entry, "session_id": session_id})


@app.route("/admin/chat/invite", methods=["POST"])
def admin_chat_invite():
    payload = request.get_json(silent=True) or {}
    ip_str = (payload.get("ip") or "").strip()
    message = (payload.get("message") or "").strip()

    if not ip_str:
        return jsonify({"message": "IP address is required."}), 400

    if not message:
        message = "Hello! We're online if you have any questions about our services."

    page = ""
    location = ""
    user_agent = ""
    with _presence_lock:
        visitor_entry = _active_visitors.get(ip_str)
        if visitor_entry:
            page = visitor_entry.get("page", "")
            location = visitor_entry.get("location", "")
            user_agent = visitor_entry.get("user_agent", "")

    if not location:
        location = _lookup_location(ip_str)

    session_id = _get_session_id_for_ip(ip_str)
    session_id, _ = _ensure_chat_session(
        session_id,
        page=page,
        ip_str=ip_str,
        location=location,
        user_agent=user_agent,
    )

    if not session_id:
        return jsonify({"message": "Unable to create chat session."}), 500

    with _chat_state_lock:
        session = _chat_state.get("sessions", {}).get(session_id)
        if not session:
            return jsonify({"message": "Session not found."}), 404

        try:
            entry = _append_chat_message(session, "admin", message, message_type="invite")
        except ValueError:
            return jsonify({"message": "Message text is required."}), 400

        session["last_admin_read"] = entry["id"]
        visitor = session.setdefault("visitor", {})
        visitor.setdefault("ip", ip_str)
        if location:
            visitor["location"] = location
        if page:
            visitor["last_page"] = page

    _save_chat_state()
    return jsonify({"message": "Invite sent.", "entry": entry, "session_id": session_id})

def _read_text_file(path: str) -> str:
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as file:
        return file.read()


def load_reviews():
    raw = _read_text_file(REVIEWS_FILE).strip()
    if not raw:
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []

    if not isinstance(data, list):
        return []

    reviews = []
    needs_save = False
    for entry in data:
        if not isinstance(entry, dict):
            continue
        review_id = entry.get("id")
        if not isinstance(review_id, str) or not review_id.strip():
            review_id = str(uuid4())
            needs_save = True
        quote = entry.get("quote")
        author = entry.get("author")
        location = entry.get("location")
        created_at = entry.get("created_at")
        updated_at = entry.get("updated_at")

        if not isinstance(quote, str):
            quote = ""
        if not isinstance(author, str):
            author = ""
        if not isinstance(location, str):
            location = ""
        if not isinstance(created_at, str) or not created_at.strip():
            created_at = datetime.utcnow().isoformat()
            needs_save = True
        if updated_at is not None and not isinstance(updated_at, str):
            updated_at = None
            needs_save = True

        reviews.append(
            {
                "id": review_id.strip(),
                "quote": quote.strip(),
                "author": author.strip(),
                "location": location.strip(),
                "created_at": created_at,
                **({"updated_at": updated_at} if updated_at else {}),
            }
        )

    if needs_save:
        save_reviews(reviews)

    return reviews


def save_reviews(reviews):
    with open(REVIEWS_FILE, "w", encoding="utf-8") as file:
        json.dump(reviews, file, indent=2)


def load_bookings():
    raw = _read_text_file(BOOKINGS_FILE).strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError:
        pass

    bookings = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [segment.strip() for segment in line.split(",") if segment.strip()]
        entry = {}
        for segment in parts:
            if ":" in segment:
                key, value = segment.split(":", 1)
                entry[key.strip().lower()] = value.strip()
        if entry:
            entry.setdefault("name", "")
            entry.setdefault("time", "")
            entry.setdefault("location", "")
            entry.setdefault("email", "")
            entry.setdefault("phone", "")
            entry.setdefault("id", str(uuid4()))
            entry.setdefault("created_at", datetime.utcnow().isoformat())
            bookings.append(entry)
    if bookings:
        save_bookings(bookings)
    return bookings


def save_bookings(bookings):
    with open(BOOKINGS_FILE, "w", encoding="utf-8") as file:
        json.dump(bookings, file, indent=2)


def load_contacts():
    raw = _read_text_file(CONTACTS_FILE).strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError:
        pass
    return []


def save_contacts(contacts):
    with open(CONTACTS_FILE, "w", encoding="utf-8") as file:
        json.dump(contacts, file, indent=2)


def _normalize_customer_slot(slot: dict) -> dict:
    if not isinstance(slot, dict):
        return {}

    date_value = (slot.get("date") or "").strip()
    time_value = (slot.get("time") or "").strip()
    label = (slot.get("label") or "").strip()
    default_label = f"{date_value} {time_value}".strip()
    if not label or label == default_label:
        label = _customer_slot_label(date_value, time_value)

    status = (slot.get("status") or "available").strip().lower()
    if status not in {"available", "booked", "confirmed"}:
        status = "available"

    entry = {
        "id": (slot.get("id") or str(uuid4())).strip(),
        "date": date_value,
        "time": time_value,
        "label": label,
        "status": status,
        "created_at": slot.get("created_at")
        or datetime.utcnow().isoformat(),
    }

    customer_name = slot.get("customer_name") or ""
    customer_phone = slot.get("customer_phone") or ""
    if customer_name:
        entry["customer_name"] = customer_name.strip()
    if customer_phone:
        entry["customer_phone"] = customer_phone.strip()
    if slot.get("booked_at"):
        entry["booked_at"] = slot.get("booked_at")

    return entry


def load_customer_slots():
    raw = _read_text_file(CUSTOMER_SLOTS_FILE).strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
        if isinstance(payload, list):
            return [_normalize_customer_slot(item) for item in payload]
    except json.JSONDecodeError:
        return []
    return []


def save_customer_slots(slots):
    with open(CUSTOMER_SLOTS_FILE, "w", encoding="utf-8") as handle:
        json.dump(slots, handle, indent=2)


def _customer_slot_label(date_value: str, time_value: str) -> str:
    date_value = (date_value or "").strip()
    time_value = (time_value or "").strip()

    try:
        date_obj = datetime.strptime(date_value, "%Y-%m-%d")
    except ValueError:
        return f"{date_value} {time_value}".strip()

    month_abbr = date_obj.strftime("%b").lower()
    formatted_date = date_obj.strftime("%a %d/%b/%y").replace(
        date_obj.strftime("%b"), month_abbr
    )
    return f"{formatted_date} {time_value}".strip()


def load_availability():
    if not os.path.exists(AVAIL_FILE):
        return []
    with open(AVAIL_FILE, "r", encoding="utf-8") as file:
        return [line.strip() for line in file if line.strip()]


def save_availability(slots):
    with open(AVAIL_FILE, "w", encoding="utf-8") as file:
        file.write("\n".join(slots) + ("\n" if slots else ""))


def remove_availability_slot(slot):
    slots = load_availability()
    if slot in slots:
        slots.remove(slot)
        save_availability(slots)


def add_availability_slot(slot):
    slots = load_availability()
    if slot not in slots:
        slots.append(slot)
        save_availability(slots)


def reinstate_availability(slot):
    if not slot:
        return
    slots = load_availability()
    if slot not in slots:
        slots.append(slot)
        save_availability(slots)


def _customer_slot_conflicts(slot_label: str) -> bool:
    if not slot_label:
        return True

    existing_booking_times = {booking.get("time", "") for booking in load_bookings()}
    availability = set(load_availability())
    customer_slots = {slot.get("label", "") for slot in load_customer_slots()}

    return slot_label in availability or slot_label in existing_booking_times or slot_label in customer_slots


def _find_customer_slot(slot_id: str):
    for slot in load_customer_slots():
        if slot.get("id") == slot_id:
            return slot
    return None


def _set_customer_slot_status(slot_id: str, status: str):
    if not slot_id or status not in {"available", "booked", "confirmed"}:
        return {"message": "Slot ID and a valid status are required."}, 400

    slots = load_customer_slots()
    updated = False
    for slot in slots:
        if slot.get("id") != slot_id:
            continue

        slot["status"] = status
        if status == "available":
            slot.pop("customer_name", None)
            slot.pop("customer_phone", None)
            slot.pop("booked_at", None)
        updated = True
        break

    if not updated:
        return {"message": "Slot not found."}, 404

    save_customer_slots(slots)
    return {"message": "Slot updated.", "status": status}, 200


def _delete_customer_slot_by_id(slot_id: str):
    if not slot_id:
        return {"message": "Slot ID is required."}, 400

    slots = load_customer_slots()
    remaining = [slot for slot in slots if slot.get("id") != slot_id]

    if len(remaining) == len(slots):
        return {"message": "Slot not found."}, 404

    save_customer_slots(remaining)
    return {"message": "Slot deleted."}, 200


@app.route("/")
def home():
    return render_template('index.html')


@app.route("/customer-login")
def customer_login_page():
    return render_template('customer_login.html')


@app.route("/customer/login", methods=["POST"])
def customer_login():
    payload = request.get_json(silent=True) or {}
    code = (payload.get("code") or "").strip()

    if code != _get_customer_access_code():
        return jsonify({"message": "Access code is incorrect."}), 401

    return jsonify({"message": "Access granted."})


@app.route("/customer/slots", methods=["GET", "POST"])
def customer_slots():
    if request.method == "POST":
        payload = request.get_json(silent=True) or request.form
        date_value = (payload.get("date") or "").strip()
        time_value = (payload.get("time") or "").strip()

        if not date_value or not time_value:
            return jsonify({"message": "Date and time are required."}), 400

        try:
            datetime.strptime(date_value, "%Y-%m-%d")
        except ValueError:
            return jsonify({"message": "Date format should be YYYY-MM-DD."}), 400

        slot_label = _customer_slot_label(date_value, time_value)
        if _customer_slot_conflicts(slot_label):
            return (
                jsonify(
                    {
                        "message": "This slot conflicts with an existing quote slot or another customer slot.",
                        "slot": slot_label,
                    }
                ),
                400,
            )

        slots = load_customer_slots()
        slots.append(
            {
                "id": str(uuid4()),
                "date": date_value,
                "time": time_value,
                "label": slot_label,
                "status": "available",
                "created_at": datetime.utcnow().isoformat(),
            }
        )
        save_customer_slots(slots)

        return jsonify({"message": "Customer slot added.", "slot": slots[-1]}), 201

    available_only = (request.args.get("available_only") or "").lower() in {"1", "true", "yes"}
    slots = load_customer_slots()
    if available_only:
        slots = [slot for slot in slots if slot.get("status") == "available"]

    slots.sort(key=lambda entry: (entry.get("date", ""), entry.get("time", "")))
    return jsonify({"slots": slots})


@app.route("/customer/slots/book", methods=["POST"])
def book_customer_slot():
    payload = request.get_json(silent=True) or {}
    slot_id = (payload.get("slot_id") or "").strip()
    name = (payload.get("name") or "").strip()
    phone = (payload.get("phone") or "").strip()

    if not slot_id or not name or not phone:
        return jsonify({"message": "Name, phone, and slot are required."}), 400

    slots = load_customer_slots()
    updated = False
    for slot in slots:
        if slot.get("id") != slot_id:
            continue
        if slot.get("status") != "available":
            return jsonify({"message": "This slot is no longer available."}), 409
        slot["status"] = "booked"
        slot["customer_name"] = name
        slot["customer_phone"] = phone
        slot["booked_at"] = datetime.utcnow().isoformat()
        updated = True
        break

    if not updated:
        return jsonify({"message": "Slot not found."}), 404

    save_customer_slots(slots)
    return jsonify({"message": "Your slot is reserved. We will confirm shortly."})


@app.route("/customer/slots/status", methods=["POST"])
def update_customer_slot_status():
    payload = request.get_json(silent=True) or request.form
    slot_id = (payload.get("slot_id") or "").strip()
    status = (payload.get("status") or "").strip().lower()

    message, status_code = _set_customer_slot_status(slot_id, status)
    return jsonify(message), status_code


@app.route("/api/customer/slots", methods=["GET", "POST"])
def api_customer_slots():
    if request.method == "GET":
        slots = load_customer_slots()
        slots.sort(key=lambda entry: (entry.get("date", ""), entry.get("time", "")))
        return jsonify({"slots": slots})

    payload = request.get_json(silent=True) or {}
    date_value = (payload.get("date") or "").strip()
    time_value = (payload.get("time") or "").strip()

    if not date_value or not time_value:
        return jsonify({"message": "Date and time are required."}), 400

    try:
        datetime.strptime(date_value, "%Y-%m-%d")
    except ValueError:
        return jsonify({"message": "Date format should be YYYY-MM-DD."}), 400

    slot_label = _customer_slot_label(date_value, time_value)
    if _customer_slot_conflicts(slot_label):
        return (
            jsonify(
                {
                    "message": "This slot conflicts with an existing quote slot or another customer slot.",
                    "slot": slot_label,
                }
            ),
            400,
        )

    slots = load_customer_slots()
    slots.append(
        {
            "id": str(uuid4()),
            "date": date_value,
            "time": time_value,
            "label": slot_label,
            "status": "available",
            "created_at": datetime.utcnow().isoformat(),
        }
    )
    save_customer_slots(slots)

    return jsonify({"message": "Customer slot added.", "slot": slots[-1]}), 201


@app.route("/api/customer/slots/<slot_id>", methods=["PATCH", "DELETE"])
def api_customer_slot_detail(slot_id):
    slot_id = (slot_id or "").strip()
    if not slot_id:
        return jsonify({"message": "Slot ID is required."}), 400

    if request.method == "DELETE":
        message, status_code = _delete_customer_slot_by_id(slot_id)
        return jsonify(message), status_code

    payload = request.get_json(silent=True) or {}
    status = (payload.get("status") or "").strip().lower()
    message, status_code = _set_customer_slot_status(slot_id, status)
    return jsonify(message), status_code


# --- Handle booking submissions ---
@app.route("/book", methods=["POST"])
def book():
    data = request.get_json(silent=True) or {}
    allowed_locations = {"Audenshaw", "Denton", "Dukinfield"}

    name = data.get("name", "").strip()
    time = data.get("time", "").strip()
    location = data.get("location", "").strip()
    email = data.get("email", "").strip()
    phone = data.get("phone", "").strip()

    if not name or not time or not email or not phone:
        return jsonify({"message": "❌ Please complete all booking details."}), 400

    if location and location not in allowed_locations:
        return jsonify({"message": "❌ Please choose a valid service location."}), 400

    if any(slot.get("label") == time for slot in load_customer_slots()):
        return jsonify({"message": "❌ This time is reserved for existing customers."}), 400

    bookings = load_bookings()
    booking_entry = {
        "id": str(uuid4()),
        "name": name,
        "time": time,
        "location": location,
        "email": email,
        "phone": phone,
        "created_at": datetime.utcnow().isoformat(),
    }
    bookings.append(booking_entry)
    save_bookings(bookings)

    # Remove the booked slot from available times
    if time:
        remove_availability_slot(time)

    return jsonify({"message": f"✅ Booking confirmed for {name} at {time}!"})


# --- Get available times for dropdown ---
@app.route("/availability")
def get_availability():
    return jsonify(load_availability())


@app.route("/weather/slots", methods=["POST"])
def weather_for_slots():
    payload = request.get_json(silent=True) or {}
    slots = payload.get("slots") or []
    if not isinstance(slots, list):
        return jsonify({"message": "Slots must be provided as a list."}), 400

    api_key = _get_weather_api_key()
    if not api_key:
        return jsonify({"forecasts": {}, "has_api_key": False})

    forecasts = {}
    for slot in slots:
        forecast = _forecast_for_slot(slot, api_key=api_key)
        if forecast:
            forecasts[slot] = forecast

    return jsonify({"forecasts": forecasts, "has_api_key": True})


# --- Admin page: manage bookings + set available times ---
@app.route("/bookings", methods=["GET", "POST"])
def view_bookings():
    # Add new slot
    if request.method == "POST":
        form_type = request.form.get("form_type", "")
        if form_type == "customer_slot":
            date = request.form.get("customer_date")
            time = request.form.get("customer_time")
            if date and time:
                try:
                    datetime.strptime(date, "%Y-%m-%d")
                except ValueError:
                    date = None
                else:
                    slot_label = _customer_slot_label(date, time)
                    if _customer_slot_conflicts(slot_label):
                        return (
                            jsonify(
                                {
                                    "message": "This slot conflicts with an existing quote slot or another customer slot.",
                                    "slot": slot_label,
                                }
                            ),
                            400,
                        )
                    slots = load_customer_slots()
                    slots.append(
                        {
                            "id": str(uuid4()),
                            "date": date,
                            "time": time,
                            "label": slot_label,
                            "status": "available",
                            "created_at": datetime.utcnow().isoformat(),
                        }
                    )
                    save_customer_slots(slots)
        else:
            date = request.form.get("date")
            time = request.form.get("time")
            if date and time:
                try:
                    datetime.strptime(date, "%Y-%m-%d")
                    slot = f"{date} {time}"
                    add_availability_slot(slot)
                except ValueError:
                    pass

    bookings = load_bookings()
    avail = load_availability()
    customer_slots_data = load_customer_slots()
    available_customer_slots = [slot for slot in customer_slots_data if slot.get("status") == "available"]
    booked_customer_slots = [slot for slot in customer_slots_data if slot.get("status") == "booked"]
    confirmed_customer_slots = [slot for slot in customer_slots_data if slot.get("status") == "confirmed"]

    # --- Pretty Admin Page ---
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <title>Manage Bookings</title>
      <style>
        body {
          font-family: 'Segoe UI', sans-serif;
          background: linear-gradient(135deg, #74ABE2, #5563DE);
          color: white;
          text-align: center;
          padding: 2rem;
        }
        table {
          margin: auto;
          border-collapse: collapse;
          background-color: rgba(255,255,255,0.15);
          box-shadow: 0 4px 10px rgba(0,0,0,0.3);
          border-radius: 12px;
          overflow: hidden;
        }
        th, td { padding: 10px 20px; }
        th { background: rgba(255,255,255,0.25); }
        tr:nth-child(even){ background: rgba(255,255,255,0.1); }
        input, select, button {
          margin-top: 1rem; padding: 10px 15px;
          border: none; border-radius: 6px; font-size: 1rem;
        }
        input[type="date"] { width: 180px; }
        select { width: 100px; }
        button { background: #00C9A7; color: white; cursor: pointer; }
        button:hover { background: #00A387; }
        .section { margin-top: 2rem; }
      </style>
    </head>
    <body>
      <h1>📘 Current Quote Bookings</h1>
      {% if bookings %}
      <table>
        <tr><th>Name</th><th>Time</th><th>Location</th><th>Email</th><th>Phone</th></tr>
        {% for booking in bookings %}
        <tr>
          <td>{{ booking.get('name', '') }}</td>
          <td>{{ booking.get('time', '') }}</td>
          <td>{{ booking.get('location', '') }}</td>
          <td>{{ booking.get('email', '') }}</td>
          <td>{{ booking.get('phone', '') }}</td>
        </tr>
        {% endfor %}
      </table>
      {% else %}
      <p>No bookings yet 😅</p>
      {% endif %}

      <div class="section">
        <h2>🗓️ Set Available Times</h2>
        <form method="POST">
          <input type="date" name="date" required>
          <select name="time" required>
            <option value="18:00">6:00 PM</option>
            <option value="20:00">8:00 PM</option>
          </select>
          <button type="submit">Add Slot</button>
        </form>

        {% if avail %}
          <h3>Current Free Slots</h3>
          <table>
            <tr><th>Time</th></tr>
            {% for t in avail %}
            <tr><td>{{ t }}</td></tr>
            {% endfor %}
          </table>
        {% else %}
        <p>No free times set yet</p>
        {% endif %}
      </div>

      <div class="section">
        <h2>👥 Existing Customer Slots</h2>
        <p>Slots added here will never overlap with quote booking slots.</p>
        <form method="POST">
          <input type="hidden" name="form_type" value="customer_slot">
          <input type="date" name="customer_date" required>
          <select name="customer_time" required>
            <option value="09:00">9:00 AM</option>
            <option value="11:00">11:00 AM</option>
            <option value="13:00">1:00 PM</option>
            <option value="15:00">3:00 PM</option>
            <option value="17:00">5:00 PM</option>
          </select>
          <button type="submit">Add Customer Slot</button>
        </form>

        <h3>Available for Customers</h3>
        {% if available_customer_slots %}
        <table>
          <tr><th>Slot</th><th>Actions</th></tr>
          {% for slot in available_customer_slots %}
          <tr>
            <td>{{ slot.get('label') }}</td>
            <td>
              <form method="POST" action="/customer/slots/status" style="display:inline-block;">
                <input type="hidden" name="slot_id" value="{{ slot.get('id') }}">
                <input type="hidden" name="status" value="booked">
                <button type="submit">Mark Reserved</button>
              </form>
            </td>
          </tr>
          {% endfor %}
        </table>
        {% else %}
        <p>No available customer slots yet</p>
        {% endif %}

        <h3>Booked by Customers</h3>
        {% if booked_customer_slots %}
        <table>
          <tr><th>Slot</th><th>Customer</th><th>Phone</th><th>Actions</th></tr>
          {% for slot in booked_customer_slots %}
          <tr>
            <td>{{ slot.get('label') }}</td>
            <td>{{ slot.get('customer_name', '—') }}</td>
            <td>{{ slot.get('customer_phone', '—') }}</td>
            <td>
              <form method="POST" action="/customer/slots/status" style="display:inline-block;">
                <input type="hidden" name="slot_id" value="{{ slot.get('id') }}">
                <input type="hidden" name="status" value="confirmed">
                <button type="submit">Confirm with Customer</button>
              </form>
              <form method="POST" action="/customer/slots/status" style="display:inline-block; margin-left:8px;">
                <input type="hidden" name="slot_id" value="{{ slot.get('id') }}">
                <input type="hidden" name="status" value="available">
                <button type="submit">Release Slot</button>
              </form>
            </td>
          </tr>
          {% endfor %}
        </table>
        {% else %}
        <p>No customer bookings yet</p>
        {% endif %}

        <h3>Confirmed with Customers</h3>
        {% if confirmed_customer_slots %}
        <table>
          <tr><th>Slot</th><th>Customer</th><th>Phone</th><th>Actions</th></tr>
          {% for slot in confirmed_customer_slots %}
          <tr>
            <td>{{ slot.get('label') }}</td>
            <td>{{ slot.get('customer_name', '—') }}</td>
            <td>{{ slot.get('customer_phone', '—') }}</td>
            <td>
              <form method="POST" action="/customer/slots/status" style="display:inline-block;">
                <input type="hidden" name="slot_id" value="{{ slot.get('id') }}">
                <input type="hidden" name="status" value="booked">
                <button type="submit">Mark Pending</button>
              </form>
              <form method="POST" action="/customer/slots/status" style="display:inline-block; margin-left:8px;">
                <input type="hidden" name="slot_id" value="{{ slot.get('id') }}">
                <input type="hidden" name="status" value="available">
                <button type="submit">Release Slot</button>
              </form>
            </td>
          </tr>
          {% endfor %}
        </table>
        {% else %}
        <p>No confirmed customer slots</p>
        {% endif %}
      </div>
    </body>
    </html>
    """
    return render_template_string(
        html,
        bookings=bookings,
        avail=avail,
        available_customer_slots=available_customer_slots,
        booked_customer_slots=booked_customer_slots,
        confirmed_customer_slots=confirmed_customer_slots,
    )


# --- NEW: JSON endpoint for bookings dashboard ---
@app.route("/bookings_json", methods=["GET"])
def get_bookings_json():
    bookings = load_bookings()
    simplified = [
        {
            "id": booking.get("id", ""),
            "name": booking.get("name", ""),
            "time": booking.get("time", ""),
            "location": booking.get("location", ""),
            "email": booking.get("email", ""),
            "phone": booking.get("phone", ""),
            "created_at": booking.get("created_at", ""),
        }
        for booking in bookings
    ]
    return jsonify({"bookings": simplified})


@app.route("/api/bookings", methods=["GET"])
def api_get_bookings():
    return jsonify({"bookings": load_bookings()})


@app.route("/api/bookings/<booking_id>", methods=["PUT", "DELETE"])
def api_update_booking(booking_id):
    bookings = load_bookings()
    for index, booking in enumerate(bookings):
        if booking.get("id") != booking_id:
            continue

        if request.method == "DELETE":
            removed = bookings.pop(index)
            save_bookings(bookings)
            reinstate_availability(removed.get("time", ""))
            return jsonify({"message": "Booking deleted."})

        data = request.get_json(silent=True) or {}

        name = data.get("name", booking.get("name", ""))
        time = data.get("time", booking.get("time", ""))
        location = data.get("location", booking.get("location", ""))
        email = data.get("email", booking.get("email", ""))
        phone = data.get("phone", booking.get("phone", ""))

        cleaned = {
            "name": name.strip() if isinstance(name, str) else booking.get("name", ""),
            "time": time.strip() if isinstance(time, str) else booking.get("time", ""),
            "location": location.strip() if isinstance(location, str) else booking.get("location", ""),
            "email": email.strip() if isinstance(email, str) else booking.get("email", ""),
            "phone": phone.strip() if isinstance(phone, str) else booking.get("phone", ""),
        }

        allowed_locations = {"Audenshaw", "Denton", "Dukinfield"}
        if cleaned["location"] and cleaned["location"] not in allowed_locations:
            return jsonify({"message": "❌ Please choose a valid service location."}), 400

        previous_time = booking.get("time", "")

        booking.update(cleaned)
        booking["updated_at"] = datetime.utcnow().isoformat()
        bookings[index] = booking
        save_bookings(bookings)
        if previous_time and previous_time != booking.get("time", ""):
            reinstate_availability(previous_time)
        if booking.get("time"):
            remove_availability_slot(booking["time"])
        return jsonify({"message": "Booking updated.", "booking": booking})

    return jsonify({"message": "Booking not found."}), 404


@app.route("/contact", methods=["POST"])
def submit_contact():
    data = request.get_json(silent=True) or {}

    name = data.get("name", "").strip()
    phone = data.get("phone", "").strip()
    email = data.get("email", "").strip()
    enquiry = data.get("enquiry", "").strip()

    if not name or not phone or not email or not enquiry:
        return (
            jsonify({"message": "❌ Please provide your name, phone, email and enquiry."}),
            400,
        )

    contacts = load_contacts()
    entry = {
        "id": str(uuid4()),
        "name": name,
        "phone": phone,
        "email": email,
        "enquiry": enquiry,
        "created_at": datetime.utcnow().isoformat(),
        "status": "new",
    }
    contacts.append(entry)
    save_contacts(contacts)

    return jsonify({"message": "✅ Thanks! We'll be in touch shortly."})


@app.route("/api/customer/settings", methods=["GET", "POST"])
def api_customer_settings():
    if request.method == "GET":
        return jsonify({"access_code": _get_customer_access_code()})

    payload = request.get_json(silent=True) or {}
    access_code = (payload.get("access_code") or "").strip()

    if not access_code:
        return jsonify({"message": "Access code is required."}), 400

    _update_customer_access_code(access_code)
    return jsonify({"message": "Customer access code updated.", "access_code": access_code})


@app.route("/api/contacts", methods=["GET"])
def api_get_contacts():
    return jsonify({"contacts": load_contacts()})


@app.route("/api/contacts/<contact_id>", methods=["PATCH", "DELETE"])
def api_modify_contact(contact_id):
    contacts = load_contacts()
    for index, contact in enumerate(contacts):
        if contact.get("id") != contact_id:
            continue

        if request.method == "DELETE":
            contacts.pop(index)
            save_contacts(contacts)
            return jsonify({"message": "Enquiry removed."})

        data = request.get_json(silent=True) or {}
        status = data.get("status")
        if status:
            contact["status"] = status
            contact["updated_at"] = datetime.utcnow().isoformat()
            contacts[index] = contact
            save_contacts(contacts)
        return jsonify({"message": "Enquiry updated.", "contact": contact})

    return jsonify({"message": "Enquiry not found."}), 404


@app.route("/api/reviews", methods=["GET", "POST"])
def api_reviews():
    if request.method == "GET":
        return jsonify({"reviews": load_reviews()})

    data = request.get_json(silent=True) or {}
    quote = (data.get("quote") or "").strip()
    author = (data.get("author") or "").strip()
    location = (data.get("location") or "").strip()

    if not quote or not author:
        return (
            jsonify({"message": "Review quote and author name are required."}),
            400,
        )

    review = {
        "id": str(uuid4()),
        "quote": quote,
        "author": author,
        "location": location,
        "created_at": datetime.utcnow().isoformat(),
    }

    reviews = load_reviews()
    reviews.append(review)
    save_reviews(reviews)

    return jsonify({"message": "Review created.", "review": review}), 201


@app.route("/api/reviews/<review_id>", methods=["PUT", "PATCH", "DELETE"])
def api_review_detail(review_id):
    review_id = (review_id or "").strip()
    if not review_id:
        return jsonify({"message": "Review ID is required."}), 400

    reviews = load_reviews()
    match = next((item for item in reviews if item.get("id") == review_id), None)

    if not match:
        return jsonify({"message": "Review not found."}), 404

    if request.method == "DELETE":
        updated = [item for item in reviews if item.get("id") != review_id]
        save_reviews(updated)
        return jsonify({"message": "Review deleted."})

    data = request.get_json(silent=True) or {}
    quote = (data.get("quote") or "").strip()
    author = (data.get("author") or "").strip()
    location = (data.get("location") or "").strip()

    if not quote or not author:
        return (
            jsonify({"message": "Review quote and author name are required."}),
            400,
        )

    match.update(
        {
            "quote": quote,
            "author": author,
            "location": location,
            "updated_at": datetime.utcnow().isoformat(),
        }
    )
    save_reviews(reviews)

    return jsonify({"message": "Review updated.", "review": match})


@app.route("/availability", methods=["DELETE"])
def delete_availability_slot():
    data = request.get_json(silent=True) or {}
    slot = (data.get("slot") or "").strip()
    if not slot:
        return jsonify({"message": "Slot is required."}), 400

    slots = load_availability()
    if slot not in slots:
        return jsonify({"message": "Slot not found."}), 404

    slots.remove(slot)
    save_availability(slots)
    return jsonify({"message": "Slot removed."})


def _remove_visitor(ip_str: str) -> None:
    if not ip_str:
        return
    entry = None
    with _presence_lock:
        entry = _active_visitors.pop(ip_str, None)
    if entry:
        _finalize_visitor_session(ip_str, entry)


@app.route("/presence", methods=["GET", "POST"])
def presence():
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        status = (payload.get("status") or "online").strip().lower()
        try:
            if status == "offline":
                _remove_visitor(_client_ip())
            else:
                _record_presence(payload)
        finally:
            _prune_visitors(datetime.utcnow())
        return jsonify({"status": "ok"})

    now = datetime.utcnow()
    _prune_visitors(now)
    with _presence_lock:
        visitors = [
            {
                "ip": details["ip"],
                "location": details.get("location", "Unknown location"),
                "page": details.get("page", ""),
                "user_agent": details.get("user_agent", ""),
                "first_seen": details["first_seen"].isoformat() + "Z",
                "last_seen": details["last_seen"].isoformat() + "Z",
            }
            for details in _active_visitors.values()
        ]

    visitors.sort(key=lambda entry: entry["last_seen"], reverse=True)
    return jsonify({"visitors": visitors, "generated_at": now.isoformat() + "Z"})


@app.route("/admin/visitors", methods=["GET"])
def admin_visitors():
    now = datetime.utcnow()
    _prune_visitors(now)
    snapshot = _visitor_log_snapshot()
    with _presence_lock:
        active_snapshot = {
            ip: details
            for ip, details in _active_visitors.items()
            if isinstance(details, dict)
        }

    with _banned_ips_lock:
        banned_snapshot = {
            ip: dict(details)
            for ip, details in _banned_ips.items()
            if isinstance(details, dict)
        }

    results = []
    for record in snapshot:
        if not record.get("visited_index"):
            continue

        combined = dict(record)
        ip_str = combined.get("ip") or ""
        combined["banned"] = ip_str in banned_snapshot
        active_entry = active_snapshot.get(ip_str)
        if active_entry and isinstance(active_entry.get("first_seen"), datetime):
            first_seen = active_entry.get("first_seen")
            last_seen = active_entry.get("last_seen")
            if isinstance(last_seen, datetime):
                first_seen_iso = first_seen.isoformat() + "Z"
                last_seen_iso = last_seen.isoformat() + "Z"
                duration_seconds = max(0.0, (last_seen - first_seen).total_seconds())
                combined["current_session"] = {
                    "first_seen": first_seen_iso,
                    "last_seen": last_seen_iso,
                    "duration_seconds": duration_seconds,
                }
                if not combined.get("last_seen") or combined["last_seen"] < last_seen_iso:
                    combined["last_seen"] = last_seen_iso
            else:
                combined["current_session"] = combined.get("current_visit")
        else:
            combined["current_session"] = combined.get("current_visit")

        combined.pop("current_visit", None)
        results.append(combined)

    results.sort(key=lambda item: item.get("last_seen") or "", reverse=True)
    banned_list = list(banned_snapshot.values())
    banned_list.sort(key=lambda item: item.get("banned_at") or "", reverse=True)
    return jsonify(
        {
            "visitors": results,
            "banned": banned_list,
            "generated_at": now.isoformat() + "Z",
        }
    )


@app.route("/admin/visitors/<ip_str>", methods=["DELETE"])
def admin_delete_visitor(ip_str):
    ip_clean = (ip_str or "").strip()
    if not ip_clean:
        return jsonify({"message": "IP address is required."}), 400

    removed = False
    snapshot = None
    with _visitor_log_lock:
        if ip_clean in _visitor_log:
            _visitor_log.pop(ip_clean, None)
            removed = True
            snapshot = dict(_visitor_log)

    if snapshot is not None:
        _save_visitor_log(snapshot=snapshot)

    _discard_active_visitor(ip_clean)

    if not removed:
        return jsonify({"message": "Visitor history not found."}), 404

    return jsonify({"message": "Visitor history deleted."})


@app.route("/admin/visitors/banned", methods=["GET"])
def admin_list_banned_visitors():
    with _banned_ips_lock:
        banned_list = [dict(details) for details in _banned_ips.values()]
    banned_list.sort(key=lambda item: item.get("banned_at") or "", reverse=True)
    return jsonify({"banned": banned_list})


@app.route("/admin/visitors/banned", methods=["POST"])
def admin_ban_visitor():
    data = request.get_json(silent=True) or {}
    ip_clean = (data.get("ip") or "").strip()
    reason = (data.get("reason") or "").strip()

    if not ip_clean:
        return jsonify({"message": "IP address is required."}), 400

    now_iso = datetime.utcnow().isoformat() + "Z"
    entry = {"ip": ip_clean, "banned_at": now_iso, "reason": reason}

    with _banned_ips_lock:
        if ip_clean in _banned_ips:
            existing = dict(_banned_ips[ip_clean])
            return jsonify({"message": "IP address already banned.", "banned": existing})
        _banned_ips[ip_clean] = entry
        snapshot = dict(_banned_ips)

    _save_banned_ips(snapshot=snapshot)
    _discard_active_visitor(ip_clean)

    return jsonify({"message": "IP address banned.", "banned": entry})


@app.route("/admin/visitors/banned/<ip_str>", methods=["DELETE"])
def admin_unban_visitor(ip_str):
    ip_clean = (ip_str or "").strip()
    if not ip_clean:
        return jsonify({"message": "IP address is required."}), 400

    removed_entry = None
    snapshot = None
    with _banned_ips_lock:
        if ip_clean in _banned_ips:
            removed_entry = dict(_banned_ips.pop(ip_clean))
            snapshot = dict(_banned_ips)

    if snapshot is not None:
        _save_banned_ips(snapshot=snapshot)

    if not removed_entry:
        return jsonify({"message": "IP address was not banned."}), 404

    return jsonify({"message": "IP address unbanned.", "unbanned": removed_entry})
# --- Serve admin.html file ---
@app.route("/admin")
def admin_page():
    return app.send_static_file("admin.html")

if __name__ == '__main__':
    import sys
    # Use 5002 if no port is specified, or read from command line
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5002
    app.run(host='0.0.0.0', port=port, debug=True)

