import json
from uuid import uuid4

from flask import Flask, request, jsonify, render_template_string, render_template, session, redirect, Response, abort
from flask_cors import CORS
from flask_compress import Compress
import os
import re
import socket
import base64
import ssl
import smtplib
from email.message import EmailMessage
from collections import deque
from datetime import datetime, timedelta
from threading import Lock
from ipaddress import ip_address
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import quote, urljoin, urlparse
from werkzeug.utils import secure_filename

# Import admin security
from admin_auth import (
    require_admin_auth, init_admin_security, 
    hash_password, verify_password,
    is_ip_locked, record_failed_login
)

app = Flask(__name__)
Compress(app)  # Enable gzip compression for all responses

# Initialize admin security (must be before routes)
ADMIN_PASSWORD_HASH = init_admin_security(app)

# ✅ CORS
# Public API endpoints are intentionally callable from various front-end hosts.
# Admin endpoints remain protected by admin auth.
CORS(
    app,
    resources={
        # NOTE: Flask-CORS expects regex patterns. "/api/*" only matches "/api/".
        r"/api/.*": {"origins": "*"},
        r"/verify-email": {"origins": "*"},
        r"/.*": {
            "origins": [
                "https://payasyoumow.org",
                "https://www.payasyoumow.org",
                "https://callansweringandy.uk",
                "https://www.callansweringandy.uk",
                "https://payasyounow71.neocities.org",
                "https://booking-app-p8q8.onrender.com",
                "https://andyfast20-sketch.github.io",
            ]
        },
    },
)

# Use absolute paths so data persists regardless of the current working directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _data_path(filename: str) -> str:
    return os.path.join(BASE_DIR, filename)

BOOKINGS_FILE = _data_path("bookings.txt")
AVAIL_FILE = _data_path("availability.txt")
CONTACTS_FILE = _data_path("contacts.json")
CHAT_STATE_FILE = _data_path("chat_state.json")
AUTOPILOT_FILE = _data_path("autopilot.json")
BANNED_IPS_FILE = _data_path("banned_ips.json")
VISITOR_LOG_FILE = _data_path("visitor_log.json")
REVIEWS_FILE = _data_path("reviews.json")
CUSTOMER_SLOTS_FILE = _data_path("customer_slots.json")
CUSTOMER_SETTINGS_FILE = _data_path("customer_settings.json")
WEATHER_CONFIG_FILE = _data_path("weather_config.json")
SMSAPI_CONFIG_FILE = _data_path("smsapi_config.json")
TELNYX_CONFIG_FILE = _data_path("telnyx_config.json")
SMTP_CONFIG_FILE = _data_path("smtp_config.json")
EMAIL_MAGIC_FILE = _data_path("email_magic.json")
ADMIN_AUTH_FILE = _data_path("admin_auth.json")
WATCHDOG_CONFIG_FILE = _data_path("watchdog_config.json")
SEO_CONFIG_FILE = _data_path("seo_config.json")
FACEBOOK_CONFIG_FILE = _data_path("facebook_config.json")
FACEBOOK_ALERTS_FILE = _data_path("facebook_alerts.json")

CUSTOMER_ACCESS_CODE = os.getenv("CUSTOMER_ACCESS_CODE", "GARDENCARE2024")

DEFAULT_AUTOPILOT_MODEL = "deepseek-chat"
DEFAULT_AUTOPILOT_TEMPERATURE = 0.3
DEFAULT_AUTOPILOT_PROVIDER = "deepseek"  # "deepseek" or "openrouter"
AUTOPILOT_PROFILE_LIMIT = 4000
AUTOPILOT_WEBSITE_KNOWLEDGE_LIMIT = 16000
AUTOPILOT_WEBSITE_FETCH_BYTES_LIMIT = 1_500_000
AUTOPILOT_WEBSITE_MAX_PAGES = 25
AUTOPILOT_WEBSITE_MAX_DEPTH = 3
AUTOPILOT_WEBSITE_MAX_LINKS_PER_PAGE = 120
AUTOPILOT_HISTORY_LIMIT = 12

# Safety: scraping private/localhost URLs can be used for SSRF. Keep disabled by default.
ALLOW_PRIVATE_WEBSITE_SCRAPE = str(os.getenv("ALLOW_PRIVATE_WEBSITE_SCRAPE", "") or "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

VISITOR_TIMEOUT = timedelta(minutes=3)
LOCATION_CACHE_TTL = timedelta(hours=6)
WEATHER_CACHE_TTL = timedelta(minutes=45)
WEATHER_LOCATION_QUERY = "Audenshaw,Denton,UK"
INDEX_PAGES = {"/", "/index", "/index.html"}
STATIC_IMAGES_DIR = os.path.join(app.root_path, "static", "images")
ALLOWED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico"}
RASTER_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}

# Safety cap for uploads (10MB)
app.config.setdefault("MAX_CONTENT_LENGTH", 10 * 1024 * 1024)

_active_visitors = {}
_presence_lock = Lock()
_location_cache = {}
_chat_state_lock = Lock()
_chat_state = {"online": True, "sessions": {}}
_autopilot_lock = Lock()
_autopilot_config = {
    "enabled": False,
    "business_profile": "",
    "business_website_url": "",
    "business_website_knowledge": "",
    "business_website_last_scraped": "",
    "provider": DEFAULT_AUTOPILOT_PROVIDER,
    "model": DEFAULT_AUTOPILOT_MODEL,
    "temperature": DEFAULT_AUTOPILOT_TEMPERATURE,
    "api_key": "",
    "api_keys": [],
    "openrouter_api_key": "",
    "openrouter_api_keys": [],
}
_visitor_log_lock = Lock()
_visitor_log = {}
_banned_ips_lock = Lock()
_banned_ips = {}
_customer_settings_lock = Lock()
_customer_settings = {"access_code": CUSTOMER_ACCESS_CODE}
_weather_config_lock = Lock()
_weather_config = {"api_key": "", "api_keys": []}
_weather_forecast_cache = {}
_smsapi_config_lock = Lock()
_smsapi_config = {"oauth_token": "", "sender_name": ""}
_telnyx_config_lock = Lock()
_telnyx_config = {"api_key": "", "from_number": "", "messaging_profile_id": ""}
_verification_codes = {}  # Store verification codes temporarily
_watchdog_config_lock = Lock()
_watchdog_config = {"enabled": False, "to_number": "+447595289669", "last_sent": None}

_SEO_DEFAULTS: dict = {
    "enabled": False,
    "business_name": "Pay As You Mow",
    "tagline": "Professional Lawn & Garden Maintenance in Manchester, UK",
    "meta_description": "Pay As You Mow – flexible, pay-as-you-go lawn mowing and garden maintenance in Manchester, UK. No contracts, no fuss. Book a free quote today.",
    "keywords": "lawn mowing Manchester, garden maintenance Manchester, grass cutting Manchester, gardener Manchester, pay as you go gardening, lawn care Audenshaw, garden service Denton, Tameside gardener, mowing Salford",
    "city": "Manchester",
    "region": "Greater Manchester",
    "country": "GB",
    "postcode": "",
    "lat": "53.479",
    "lng": "-2.2426",
    "service_area": "Manchester, Salford, Trafford, Tameside, Oldham, Audenshaw, Denton, Stockport",
    "google_verification": "",
    "bing_verification": "",
    "schema_enabled": True,
    "sitemap_enabled": True,
    "robots_index": True,
    "og_image": "",
    "canonical_url": "",
}
_seo_config_lock = Lock()
_seo_config: dict = dict(_SEO_DEFAULTS)

# Facebook Group post monitoring
_FACEBOOK_CONFIG_DEFAULTS: dict = {
    "enabled": False,
    "group_id": "",
    "access_token": "",
    "keywords": "gardener,lawn mowing,grass cutting,garden maintenance,lawn care,hedge trimming,mowing,grass cutter,landscaping",
    "poll_interval_minutes": 30,
    "notify_sms": False,
    "last_checked": None,
    "token_expiry_date": "",
}
_facebook_config_lock = Lock()
_facebook_config: dict = dict(_FACEBOOK_CONFIG_DEFAULTS)
_facebook_alerts_lock = Lock()
_facebook_alerts: list = []
_facebook_known_post_ids: set = set()

# Local area SEO landing pages — each generates a separately indexed, location-targeted page
_LOCAL_AREA_PAGES: dict[str, dict] = {
    "manchester": {
        "area": "Manchester",
        "title": "Lawn Mowing & Garden Maintenance Manchester | Pay As You Mow",
        "description": "Professional lawn mowing and garden maintenance across Manchester. Pay-as-you-go, no contracts. Book your free quote from Pay As You Mow — Manchester's flexible local gardening service.",
        "h1": "Lawn Mowing in Manchester",
        "intro": "Looking for reliable, affordable lawn mowing in Manchester? Pay As You Mow delivers flexible, pay-as-you-go garden maintenance across Manchester — no contracts, no hidden fees, just great results.",
    },
    "salford": {
        "area": "Salford",
        "title": "Lawn Mowing & Garden Maintenance Salford | Pay As You Mow",
        "description": "Pay As You Mow provides professional lawn mowing and garden maintenance in Salford. No contracts, pay per visit. Get your free quote today.",
        "h1": "Lawn Mowing in Salford",
        "intro": "Need a reliable gardener in Salford? Pay As You Mow offers pay-as-you-go lawn mowing and garden maintenance throughout Salford — book just when you need it.",
    },
    "trafford": {
        "area": "Trafford",
        "title": "Lawn Mowing & Garden Maintenance Trafford | Pay As You Mow",
        "description": "Flexible, pay-as-you-go lawn mowing and garden maintenance in Trafford. Professional service, no subscription. Book a free quote with Pay As You Mow.",
        "h1": "Lawn Mowing in Trafford",
        "intro": "Pay As You Mow covers Trafford with pay-per-visit lawn mowing and garden maintenance. No long-term commitment — book exactly when your garden needs it.",
    },
    "tameside": {
        "area": "Tameside",
        "title": "Lawn Mowing & Garden Maintenance Tameside | Pay As You Mow",
        "description": "Professional lawn mowing and garden care in Tameside. Pay-as-you-go with no contracts. Pay As You Mow covers Tameside and all surrounding areas.",
        "h1": "Lawn Mowing in Tameside",
        "intro": "Pay As You Mow provides flexible lawn and garden maintenance across Tameside — including Audenshaw, Denton, Ashton-under-Lyne and Hyde. No contract needed.",
    },
    "audenshaw": {
        "area": "Audenshaw",
        "title": "Lawn Mowing & Garden Maintenance Audenshaw | Pay As You Mow",
        "description": "Local lawn mowing and garden maintenance in Audenshaw. Flexible pay-as-you-go service with no contracts. Book a free quote from Pay As You Mow.",
        "h1": "Lawn Mowing in Audenshaw",
        "intro": "Looking for a local gardener in Audenshaw? Pay As You Mow is based in the Audenshaw and Denton area, offering fast, friendly garden maintenance whenever you need it.",
    },
    "denton": {
        "area": "Denton",
        "title": "Lawn Mowing & Garden Maintenance Denton | Pay As You Mow",
        "description": "Professional lawn mowing and garden care in Denton, Greater Manchester. No contracts, pay per visit. Pay As You Mow — your local gardening service.",
        "h1": "Lawn Mowing in Denton",
        "intro": "Pay As You Mow is your local gardening service in Denton. Whether it's a one-off tidy-up or regular grass cutting, we're available when you need us — no contract required.",
    },
    "stockport": {
        "area": "Stockport",
        "title": "Lawn Mowing & Garden Maintenance Stockport | Pay As You Mow",
        "description": "Flexible lawn mowing and garden maintenance in Stockport. Pay-as-you-go, no contracts. Professional service from Pay As You Mow across Stockport.",
        "h1": "Lawn Mowing in Stockport",
        "intro": "Pay As You Mow offers professional lawn mowing and garden maintenance in Stockport. Book a single visit or as many as you like — completely flexible, no tie-in.",
    },
    "oldham": {
        "area": "Oldham",
        "title": "Lawn Mowing & Garden Maintenance Oldham | Pay As You Mow",
        "description": "Professional lawn care and garden maintenance in Oldham. Pay-as-you-go service with no contracts. Get a free quote from Pay As You Mow today.",
        "h1": "Lawn Mowing in Oldham",
        "intro": "Need a gardener in Oldham? Pay As You Mow provides flexible, pay-per-visit lawn mowing and garden maintenance across Oldham. No subscription, no hassle.",
    },
    "ashton-under-lyne": {
        "area": "Ashton-under-Lyne",
        "title": "Lawn Mowing & Garden Maintenance Ashton-under-Lyne | Pay As You Mow",
        "description": "Local lawn mowing and garden maintenance in Ashton-under-Lyne. Flexible pay-as-you-go with no contracts. Book a free quote from Pay As You Mow.",
        "h1": "Lawn Mowing in Ashton-under-Lyne",
        "intro": "Pay As You Mow covers Ashton-under-Lyne with professional, pay-per-visit lawn mowing and garden maintenance. Great results, fully flexible — book when it suits you.",
    },
    "stretford": {
        "area": "Stretford",
        "title": "Lawn Mowing & Garden Maintenance Stretford | Pay As You Mow",
        "description": "Professional lawn mowing and garden care in Stretford. Pay-as-you-go, no contracts. Pay As You Mow serves Stretford and the wider Trafford area.",
        "h1": "Lawn Mowing in Stretford",
        "intro": "Pay As You Mow provides flexible lawn and garden maintenance in Stretford. Perfect for busy homeowners who want a tidy garden without any long-term commitment.",
    },
}

_smtp_config_lock = Lock()
_smtp_config = {
    "host": "",
    "port": 587,
    "username": "",
    "password": "",
    "from_email": "",
    "from_name": "Pay As You Mow",
    "use_starttls": True,
}

_email_magic_lock = Lock()
_email_magic_tokens = {}  # token -> {email, expires, created_at}
_verified_emails = {}  # email -> {verified_at, expires}
_email_send_rate_limit = {}  # key -> datetime


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
_ensure_storage_file(SMSAPI_CONFIG_FILE, default={"oauth_token": "", "sender_name": ""})
_ensure_storage_file(TELNYX_CONFIG_FILE, default={"api_key": "", "from_number": ""})
_ensure_storage_file(WATCHDOG_CONFIG_FILE, default={"enabled": False, "to_number": "+447595289669", "last_sent": None})
_ensure_storage_file(SEO_CONFIG_FILE, default=dict(_SEO_DEFAULTS))
_ensure_storage_file(FACEBOOK_CONFIG_FILE, default=dict(_FACEBOOK_CONFIG_DEFAULTS))
_ensure_storage_file(FACEBOOK_ALERTS_FILE, default=[])
_ensure_storage_file(
    SMTP_CONFIG_FILE,
    default={
        "host": "",
        "port": 587,
        "username": "",
        "password": "",
        "from_email": "",
        "from_name": "Pay As You Mow",
        "use_starttls": True,
    },
)
_ensure_storage_file(EMAIL_MAGIC_FILE, default={"tokens": {}, "verified": {}})


def _load_smtp_config_from_disk() -> dict:
    if not os.path.exists(SMTP_CONFIG_FILE):
        return dict(_smtp_config)
    try:
        with open(SMTP_CONFIG_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
            if isinstance(payload, dict):
                merged = dict(_smtp_config)
                merged.update(payload)
                return merged
    except (OSError, json.JSONDecodeError):
        return dict(_smtp_config)
    return dict(_smtp_config)


def _save_smtp_config(snapshot: dict):
    try:
        with open(SMTP_CONFIG_FILE, "w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2)
    except OSError:
        pass


def _smtp_config_snapshot(*, include_secret: bool = False) -> dict:
    with _smtp_config_lock:
        cfg = dict(_smtp_config)
    password = (cfg.get("password") or "").strip()
    has_config = bool((cfg.get("host") or "").strip() and (cfg.get("from_email") or "").strip() and password)
    if not include_secret:
        cfg.pop("password", None)
        cfg["has_password"] = bool(password)
    cfg["has_config"] = has_config
    return cfg


def _load_email_magic_from_disk() -> tuple[dict, dict]:
    if not os.path.exists(EMAIL_MAGIC_FILE):
        return {}, {}
    try:
        with open(EMAIL_MAGIC_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            return {}, {}
        tokens = payload.get("tokens")
        verified = payload.get("verified")
        return (tokens if isinstance(tokens, dict) else {}), (verified if isinstance(verified, dict) else {})
    except (OSError, json.JSONDecodeError):
        return {}, {}


def _save_email_magic_to_disk(tokens: dict, verified: dict):
    try:
        with open(EMAIL_MAGIC_FILE, "w", encoding="utf-8") as handle:
            json.dump({"tokens": tokens, "verified": verified}, handle, indent=2)
    except OSError:
        pass


def _purge_expired_email_magic(now: datetime | None = None):
    if now is None:
        now = datetime.utcnow()
    changed = False
    with _email_magic_lock:
        # purge tokens
        for token, entry in list(_email_magic_tokens.items()):
            expires_raw = entry.get("expires")
            try:
                expires = datetime.fromisoformat(expires_raw) if expires_raw else None
            except Exception:
                expires = None
            if not expires or now > expires:
                _email_magic_tokens.pop(token, None)
                changed = True

        # purge verified emails
        for email, entry in list(_verified_emails.items()):
            expires_raw = entry.get("expires")
            try:
                expires = datetime.fromisoformat(expires_raw) if expires_raw else None
            except Exception:
                expires = None
            if not expires or now > expires:
                _verified_emails.pop(email, None)
                changed = True

        if changed:
            _save_email_magic_to_disk(dict(_email_magic_tokens), dict(_verified_emails))


def _is_valid_email(value: str) -> bool:
    email = (value or "").strip()
    if not email or len(email) > 254:
        return False
    # Simple, pragmatic validation.
    return bool(re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email))


def _send_email_via_smtp(*, to_email: str, subject: str, text_body: str, html_body: str | None = None) -> tuple[bool, str]:
    cfg = _smtp_config_snapshot(include_secret=True)
    if not cfg.get("has_config"):
        return False, "Email verification is not configured in the admin panel."

    host = (cfg.get("host") or "").strip()
    port = int(cfg.get("port") or 587)
    username = (cfg.get("username") or "").strip()
    password = (cfg.get("password") or "").strip()
    from_email = (cfg.get("from_email") or "").strip()
    from_name = (cfg.get("from_name") or "Pay As You Mow").strip() or "Pay As You Mow"
    use_starttls = bool(cfg.get("use_starttls", True))

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{from_email}>"
    msg["To"] = to_email
    msg.set_content(text_body)
    if html_body:
        msg.add_alternative(html_body, subtype="html")

    try:
        if port == 465 and not use_starttls:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=context, timeout=12) as server:
                if username:
                    server.login(username, password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=12) as server:
                server.ehlo()
                if use_starttls:
                    context = ssl.create_default_context()
                    server.starttls(context=context)
                    server.ehlo()
                if username:
                    server.login(username, password)
                server.send_message(msg)
        return True, ""
    except Exception as exc:
        print(f"SMTP email error: {exc}")
        return False, "Unable to send email right now."


# Load persisted email + SMTP settings at startup.
with _smtp_config_lock:
    _smtp_config.update(_load_smtp_config_from_disk())


def _load_admin_password_hash_from_disk() -> str:
    try:
        if not os.path.exists(ADMIN_AUTH_FILE):
            return ""
        with open(ADMIN_AUTH_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle) or {}
        return str(data.get("admin_password_hash") or "").strip()
    except Exception:
        return ""


def _save_admin_password_hash_to_disk(password_hash: str) -> None:
    try:
        with open(ADMIN_AUTH_FILE, "w", encoding="utf-8") as handle:
            json.dump({"admin_password_hash": password_hash}, handle)
    except Exception:
        pass


def _apply_smtp_env_overrides() -> None:
    """Allow SMTP settings to be configured via environment variables (Render-friendly)."""
    host = str(os.getenv("SMTP_HOST", "") or "").strip()
    port_raw = os.getenv("SMTP_PORT")
    username = str(os.getenv("SMTP_USERNAME", "") or "").strip()
    password = str(os.getenv("SMTP_PASSWORD", "") or "").strip()
    from_email = str(os.getenv("SMTP_FROM_EMAIL", "") or "").strip()
    from_name = str(os.getenv("SMTP_FROM_NAME", "") or "").strip()
    starttls_raw = str(os.getenv("SMTP_USE_STARTTLS", "") or "").strip().lower()

    use_starttls = None
    if starttls_raw in {"1", "true", "yes", "on"}:
        use_starttls = True
    elif starttls_raw in {"0", "false", "no", "off"}:
        use_starttls = False

    port = None
    if port_raw is not None and str(port_raw).strip() != "":
        try:
            port = int(str(port_raw).strip())
        except Exception:
            port = None

    with _smtp_config_lock:
        if host:
            _smtp_config["host"] = host
        if port is not None and 1 <= port <= 65535:
            _smtp_config["port"] = port
        if username:
            _smtp_config["username"] = username
        if password:
            _smtp_config["password"] = password
        if from_email:
            _smtp_config["from_email"] = from_email
        if from_name:
            _smtp_config["from_name"] = from_name
        if use_starttls is not None:
            _smtp_config["use_starttls"] = use_starttls


_apply_smtp_env_overrides()

# If admin password hash isn't provided via config/env, allow loading from disk.
if not ADMIN_PASSWORD_HASH:
    disk_hash = _load_admin_password_hash_from_disk()
    if disk_hash:
        ADMIN_PASSWORD_HASH = disk_hash

tokens, verified = _load_email_magic_from_disk()
with _email_magic_lock:
    _email_magic_tokens.update(tokens)
    _verified_emails.update(verified)


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
    api_keys = _normalize_api_keys(payload.get("api_keys", []))
    if api_key:
        api_keys = _merge_api_key(api_key, api_keys)
    return {"api_key": api_key, "api_keys": api_keys}


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
        api_keys = list(_weather_config.get("api_keys", []))

    env_key = os.environ.get("WEATHER_API_KEY", "").strip()
    has_api_key = bool(api_key or api_keys or env_key)

    if include_secret:
        return {"api_key": api_key or env_key, "has_api_key": has_api_key, "api_keys": api_keys}

    visible_keys = []
    if isinstance(api_keys, list):
        for entry in api_keys:
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

    return {"has_api_key": has_api_key, "api_keys": visible_keys}


def _get_weather_api_key() -> str:
    with _weather_config_lock:
        configured = str(_weather_config.get("api_key", "") or "")

    env_key = os.environ.get("WEATHER_API_KEY", "").strip()
    return configured or env_key


# SMSAPI.com Configuration Functions (Polish provider, cheap, zero compliance)
def _load_smsapi_config_from_disk() -> dict:
    if not os.path.exists(SMSAPI_CONFIG_FILE):
        return {"oauth_token": "", "sender_name": ""}

    try:
        with open(SMSAPI_CONFIG_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {"oauth_token": "", "sender_name": ""}

    if not isinstance(payload, dict):
        return {"oauth_token": "", "sender_name": ""}

    return {
        "oauth_token": str(payload.get("oauth_token", "") or ""),
        "sender_name": str(payload.get("sender_name", "") or "")
    }


def _save_smsapi_config(config=None) -> None:
    snapshot = dict(config or _smsapi_config)
    try:
        with open(SMSAPI_CONFIG_FILE, "w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2)
    except OSError:
        pass


def _smsapi_config_snapshot(*, include_secret: bool = False) -> dict:
    with _smsapi_config_lock:
        oauth_token = str(_smsapi_config.get("oauth_token", "") or "")
        sender_name = str(_smsapi_config.get("sender_name", "") or "")

    has_config = bool(oauth_token)

    if include_secret:
        return {
            "oauth_token": oauth_token,
            "sender_name": sender_name,
            "has_config": has_config
        }

    masked_token = ("*" * (len(oauth_token) - 8) + oauth_token[-8:]) if len(oauth_token) >= 8 else "****"
    
    return {
        "oauth_token": masked_token,
        "sender_name": sender_name,
        "has_config": has_config
    }


def _load_telnyx_config_from_disk() -> dict:
    if not os.path.exists(TELNYX_CONFIG_FILE):
        return {"api_key": "", "from_number": "", "messaging_profile_id": ""}

    try:
        with open(TELNYX_CONFIG_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {"api_key": "", "from_number": "", "messaging_profile_id": ""}

    if not isinstance(payload, dict):
        return {"api_key": "", "from_number": "", "messaging_profile_id": ""}

    return {
        "api_key": str(payload.get("api_key", "") or ""),
        "from_number": str(payload.get("from_number", "") or ""),
        "messaging_profile_id": str(payload.get("messaging_profile_id", "") or ""),
    }


def _save_telnyx_config(config=None) -> None:
    snapshot = dict(config or _telnyx_config)
    try:
        with open(TELNYX_CONFIG_FILE, "w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2)
    except OSError:
        pass


def _telnyx_config_snapshot(*, include_secret: bool = False) -> dict:
    with _telnyx_config_lock:
        api_key = str(_telnyx_config.get("api_key", "") or "")
        from_number = str(_telnyx_config.get("from_number", "") or "")
        messaging_profile_id = str(_telnyx_config.get("messaging_profile_id", "") or "")

    env_key = os.environ.get("TELNYX_API_KEY", "").strip()
    env_from = os.environ.get("TELNYX_FROM_NUMBER", "").strip()
    env_profile = os.environ.get("TELNYX_MESSAGING_PROFILE_ID", "").strip()

    effective_key = api_key or env_key
    effective_from = from_number or env_from
    effective_profile = messaging_profile_id or env_profile

    has_config = bool(effective_key and effective_from)

    if include_secret:
        return {
            "api_key": effective_key,
            "from_number": effective_from,
            "messaging_profile_id": effective_profile,
            "has_config": has_config,
        }

    masked = ""
    if effective_key:
        masked = ("*" * (len(effective_key) - 6) + effective_key[-6:]) if len(effective_key) >= 6 else "****"

    return {
        "api_key": masked,
        "from_number": effective_from,
        "messaging_profile_id": effective_profile,
        "has_config": has_config,
    }


# ---------------------------------------------------------------------------
# Server-Down Watchdog
# ---------------------------------------------------------------------------

def _load_watchdog_config_from_disk() -> dict:
    defaults = {"enabled": False, "to_number": "+447595289669", "last_sent": None}
    if not os.path.exists(WATCHDOG_CONFIG_FILE):
        return dict(defaults)
    try:
        with open(WATCHDOG_CONFIG_FILE, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict):
            return dict(defaults)
        merged = dict(defaults)
        merged.update(payload)
        return merged
    except (OSError, json.JSONDecodeError):
        return dict(defaults)


def _save_watchdog_config(config: dict | None = None) -> None:
    snapshot = dict(config or _watchdog_config)
    try:
        with open(WATCHDOG_CONFIG_FILE, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, indent=2)
    except OSError:
        pass


def _load_seo_config_from_disk() -> dict:
    defaults = dict(_SEO_DEFAULTS)
    if not os.path.exists(SEO_CONFIG_FILE):
        return defaults
    try:
        with open(SEO_CONFIG_FILE, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict):
            return defaults
        merged = dict(defaults)
        merged.update(payload)
        return merged
    except (OSError, json.JSONDecodeError):
        return defaults


def _save_seo_config(config: dict | None = None) -> None:
    snapshot = dict(config or _seo_config)
    try:
        with open(SEO_CONFIG_FILE, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, indent=2)
    except OSError:
        pass


def _seo_snapshot() -> dict:
    with _seo_config_lock:
        return dict(_seo_config)


def _start_watchdog_thread() -> None:
    """Launch the server-down watchdog in a daemon background thread."""
    import threading
    import urllib.request

    def _watchdog_loop():
        # Short initial delay so the server is ready before the first ping
        import time
        time.sleep(15)
        while True:
            try:
                with _watchdog_config_lock:
                    cfg = dict(_watchdog_config)

                if cfg.get("enabled"):
                    port = int(os.environ.get("PORT", 5015))
                    url = f"http://127.0.0.1:{port}/health"
                    server_ok = False
                    try:
                        req = urllib.request.urlopen(url, timeout=10)
                        server_ok = req.getcode() == 200
                    except Exception:
                        server_ok = False

                    if not server_ok:
                        # Check 24-hour throttle
                        last_sent_str = cfg.get("last_sent")
                        now = datetime.utcnow()
                        can_send = True
                        if last_sent_str:
                            try:
                                last_dt = datetime.fromisoformat(last_sent_str)
                                if (now - last_dt).total_seconds() < 86400:
                                    can_send = False
                            except ValueError:
                                pass

                        if can_send:
                            to_number = cfg.get("to_number") or "+447595289669"
                            msg = "ALERT: Pay As You Mow server may be down. Please check your server."
                            ok, _detail = _send_sms_via_telnyx(to_number, msg)
                            if ok:
                                new_last = now.isoformat()
                                with _watchdog_config_lock:
                                    _watchdog_config["last_sent"] = new_last
                                _save_watchdog_config()

            except Exception:
                pass  # Never let the watchdog thread die

            import time
            time.sleep(60)  # Check every 60 seconds

    t = threading.Thread(target=_watchdog_loop, name="server-watchdog", daemon=True)
    t.start()


def _normalize_phone_number(phone: str) -> str:
    raw = str(phone or "").strip()
    if not raw:
        return ""

    # Keep digits and leading + only.
    cleaned = []
    for ch in raw:
        if ch.isdigit():
            cleaned.append(ch)
        elif ch == "+" and not cleaned:
            cleaned.append(ch)
    normalized = "".join(cleaned)

    # Convert 00 prefix to +
    if normalized.startswith("00"):
        normalized = "+" + normalized[2:]

    # UK-friendly normalization for common mobile formats.
    if normalized.startswith("0") and len(normalized) == 11:
        normalized = "+44" + normalized[1:]
    elif normalized.startswith("44") and len(normalized) in {12, 13}:
        normalized = "+" + normalized
    elif normalized.startswith("7") and len(normalized) == 10:
        normalized = "+44" + normalized

    return normalized


def _send_sms_via_smsapi(to_number: str, message: str) -> tuple[bool, str]:
    """Send SMS using SMSAPI.com (Polish provider, ~2-3p per SMS, zero compliance)"""
    config = _smsapi_config_snapshot(include_secret=True)

    if not config.get("has_config"):
        return False, "SMS verification is not configured in the admin panel."

    try:
        import requests

        # SMSAPI.com simple REST API
        url = "https://api.smsapi.com/sms.do"
        
        # Clean phone number (remove spaces, keep +)
        to_clean = to_number.replace(" ", "")
        
        payload = {
            "oauth_token": config["oauth_token"],
            "to": to_clean,
            "message": message,
            "from": config.get("sender_name", "Info"),
            "format": "json"
        }

        response = requests.post(url, data=payload, timeout=10)
        
        if response.status_code != 200:
            return False, f"SMSAPI returned HTTP {response.status_code}."
        
        data = response.json() if response.content else {}
        
        # SMSAPI returns count > 0 on success
        count = data.get("count", 0)
        if count > 0:
            return True, ""
        
        # Check for error
        error_code = data.get("error")
        error_msg = data.get("message", "Unknown error")
        
        if error_code:
            return False, f"SMSAPI error {error_code}: {error_msg}"
        
        return False, "SMSAPI rejected the SMS request."
    except Exception as e:
        print(f"SMSAPI SMS error: {e}")
        return False, "Unable to send SMS right now."


def _send_sms_via_telnyx(to_number: str, message: str) -> tuple[bool, str]:
    """Send SMS using Telnyx Messages API."""
    config = _telnyx_config_snapshot(include_secret=True)

    if not config.get("has_config"):
        return False, "SMS verification is not configured in the admin panel."

    api_key = str(config.get("api_key") or "").strip()
    from_number = _normalize_phone_number(str(config.get("from_number") or "").strip())
    to_clean = _normalize_phone_number(to_number)

    if not api_key or not from_number:
        return False, "Telnyx SMS is missing API key or from number."
    if not to_clean or not to_clean.startswith("+"):
        return False, "Invalid destination number."

    try:
        import requests

        url = "https://api.telnyx.com/v2/messages"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "from": from_number,
            "to": to_clean,
            "text": message,
            "type": "SMS",
        }
        profile_id = str(config.get("messaging_profile_id") or "").strip()
        if profile_id:
            payload["messaging_profile_id"] = profile_id
        print(f"[Telnyx] Sending SMS from {from_number} to {to_clean}")
        response = requests.post(url, headers=headers, json=payload, timeout=15)

        if 200 <= response.status_code < 300:
            print(f"[Telnyx] SMS sent successfully to {to_clean}")
            return True, ""

        detail = ""
        try:
            body = response.json() if response.content else {}
            errors = body.get("errors")
            if isinstance(errors, list) and errors:
                first = errors[0] if isinstance(errors[0], dict) else {}
                title = str(first.get("title") or "").strip()
                err_detail = str(first.get("detail") or "").strip()
                detail = title or err_detail
                if title and err_detail and title != err_detail:
                    detail = f"{title}: {err_detail}"
        except Exception:
            detail = ""

        if detail:
            print(f"[Telnyx] Error: {detail} (HTTP {response.status_code})")
            return False, f"Telnyx error: {detail}"
        print(f"[Telnyx] Unexpected HTTP {response.status_code}: {response.text[:300]}")
        return False, f"Telnyx returned HTTP {response.status_code}."
    except Exception as exc:
        print(f"[Telnyx] SMS exception: {exc}")
        return False, f"Unable to send SMS right now. ({type(exc).__name__})"


def _send_sms_for_verification(to_number: str, message: str) -> tuple[bool, str]:
    """Send SMS using the best configured provider.

    Preference order:
    1) Telnyx (if configured)
    2) SMSAPI.com (legacy fallback)
    """
    telnyx_cfg = _telnyx_config_snapshot(include_secret=False)
    if telnyx_cfg.get("has_config"):
        return _send_sms_via_telnyx(to_number, message)
    return _send_sms_via_smsapi(to_number, message)


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
    openrouter_api_keys = _normalize_api_keys(reference.get("openrouter_api_keys", []))
    result = {
        "enabled": bool(reference.get("enabled", False)),
        "business_profile": str(reference.get("business_profile", "") or ""),
        "business_website_url": str(reference.get("business_website_url", "") or ""),
        "business_website_knowledge": str(reference.get("business_website_knowledge", "") or ""),
        "business_website_last_scraped": str(reference.get("business_website_last_scraped", "") or ""),
        "provider": str(reference.get("provider", DEFAULT_AUTOPILOT_PROVIDER) or DEFAULT_AUTOPILOT_PROVIDER),
        "model": str(reference.get("model", DEFAULT_AUTOPILOT_MODEL) or DEFAULT_AUTOPILOT_MODEL),
        "temperature": float(reference.get("temperature", DEFAULT_AUTOPILOT_TEMPERATURE)),
        "api_key": str(reference.get("api_key", "") or ""),
        "api_keys": api_keys,
        "openrouter_api_key": str(reference.get("openrouter_api_key", "") or ""),
        "openrouter_api_keys": openrouter_api_keys,
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

    if "business_website_url" in payload:
        url_text = str(payload.get("business_website_url") or "").strip()
        # Keep this as a plain string; the scrape endpoint performs stricter validation.
        result["business_website_url"] = url_text[:500]

    if payload.get("clear_business_website_knowledge") is True:
        result["business_website_knowledge"] = ""
        result["business_website_last_scraped"] = ""

    if "provider" in payload:
        provider = str(payload.get("provider") or "").strip().lower()
        if provider not in {"deepseek", "openrouter"}:
            provider = DEFAULT_AUTOPILOT_PROVIDER
        result["provider"] = provider

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

    if "openrouter_api_keys" in payload:
        incoming_keys = _normalize_api_keys(payload.get("openrouter_api_keys") or [])
        if incoming_keys:
            result["openrouter_api_keys"] = incoming_keys

    if "openrouter_api_key" in payload:
        incoming_key = str(payload.get("openrouter_api_key") or "").strip()
        if incoming_key:
            result["openrouter_api_key"] = incoming_key
            result["openrouter_api_keys"] = _merge_api_key(incoming_key, openrouter_api_keys)
        elif "openrouter_api_keys" not in payload:
            result["openrouter_api_key"] = ""
            result["openrouter_api_keys"] = []

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
            "business_website_url": str(_autopilot_config.get("business_website_url", "") or ""),
            "business_website_last_scraped": str(_autopilot_config.get("business_website_last_scraped", "") or ""),
            "provider": str(_autopilot_config.get("provider", DEFAULT_AUTOPILOT_PROVIDER) or DEFAULT_AUTOPILOT_PROVIDER),
            "model": str(_autopilot_config.get("model", DEFAULT_AUTOPILOT_MODEL) or DEFAULT_AUTOPILOT_MODEL),
            "temperature": float(_autopilot_config.get("temperature", DEFAULT_AUTOPILOT_TEMPERATURE)),
            "api_key": str(_autopilot_config.get("api_key", "") or ""),
            "api_keys": list(_autopilot_config.get("api_keys", [])),
            "openrouter_api_key": str(_autopilot_config.get("openrouter_api_key", "") or ""),
            "openrouter_api_keys": list(_autopilot_config.get("openrouter_api_keys", [])),
        }

        website_knowledge = str(_autopilot_config.get("business_website_knowledge", "") or "")
        website_knowledge_preview = website_knowledge.strip()[:1200]
        website_chars = len(website_knowledge.strip())

    if include_secret:
        snapshot["business_website_knowledge"] = str(_autopilot_config.get("business_website_knowledge", "") or "")
        return snapshot

    env_deepseek_present = bool(
        (
            os.environ.get("DEEPSEEK_API_KEY")
            or os.environ.get("OPENAI_API_KEY")
            or ""
        ).strip()
    )
    env_openrouter_present = bool((os.environ.get("OPENROUTER_API_KEY") or "").strip())

    deepseek_keys_payload = snapshot.get("api_keys", [])
    if not isinstance(deepseek_keys_payload, list):
        deepseek_keys_payload = []

    deepseek_visible_keys = []
    for entry in deepseek_keys_payload:
        value = str(entry.get("value") or "").strip()
        if not value:
            continue
        deepseek_visible_keys.append(
            {
                "id": entry.get("id"),
                "label": f"Key ending {value[-4:]}" if len(value) >= 4 else "Saved API key",
                "created_at": entry.get("created_at", ""),
                "last4": value[-4:] if len(value) >= 4 else value,
            }
        )

    openrouter_keys_payload = snapshot.get("openrouter_api_keys", [])
    if not isinstance(openrouter_keys_payload, list):
        openrouter_keys_payload = []

    openrouter_visible_keys = []
    for entry in openrouter_keys_payload:
        value = str(entry.get("value") or "").strip()
        if not value:
            continue
        openrouter_visible_keys.append(
            {
                "id": entry.get("id"),
                "label": f"Key ending {value[-4:]}" if len(value) >= 4 else "Saved API key",
                "created_at": entry.get("created_at", ""),
                "last4": value[-4:] if len(value) >= 4 else value,
            }
        )

    has_api_key = bool(snapshot.get("api_key")) or bool(deepseek_visible_keys) or env_deepseek_present
    has_openrouter_api_key = (
        bool(snapshot.get("openrouter_api_key"))
        or bool(openrouter_visible_keys)
        or env_openrouter_present
    )

    snapshot.pop("api_key", None)
    snapshot.pop("openrouter_api_key", None)
    snapshot["api_keys"] = deepseek_visible_keys
    snapshot["openrouter_api_keys"] = openrouter_visible_keys
    snapshot["has_api_key"] = has_api_key
    snapshot["has_openrouter_api_key"] = has_openrouter_api_key
    snapshot["has_business_website_knowledge"] = website_chars > 0
    snapshot["business_website_knowledge_chars"] = website_chars
    snapshot["business_website_knowledge_preview"] = website_knowledge_preview
    return snapshot


def _normalize_website_url(raw_url: str) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""

    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", text):
        text = f"https://{text}"

    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http(s) URLs are allowed.")
    if not parsed.netloc:
        raise ValueError("Please enter a full website URL.")

    # Strip fragments/query to avoid crawl explosions and tracking URLs.
    parsed = parsed._replace(fragment="", query="")
    return parsed.geturl()


def _is_disallowed_ip_address(candidate: str) -> bool:
    try:
        ip_obj = ip_address(candidate)
    except ValueError:
        return True

    return bool(
        ip_obj.is_private
        or ip_obj.is_loopback
        or ip_obj.is_link_local
        or ip_obj.is_multicast
        or ip_obj.is_reserved
    )


def _assert_safe_public_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http(s) URLs are allowed.")
    hostname = (parsed.hostname or "").strip()
    if not hostname:
        raise ValueError("Invalid website URL.")

    lowered = hostname.lower()
    if lowered in {"localhost"} or lowered.endswith(".local"):
        if not ALLOW_PRIVATE_WEBSITE_SCRAPE:
            raise ValueError(
                "Localhost/private website scraping is blocked for safety. "
                "To allow it for local testing, set ALLOW_PRIVATE_WEBSITE_SCRAPE=1 on the server."
            )
        return

    # If the hostname is an IP literal, validate directly.
    try:
        ip_address(hostname)
        if _is_disallowed_ip_address(hostname) and not ALLOW_PRIVATE_WEBSITE_SCRAPE:
            raise ValueError(
                "Private IP website scraping is blocked for safety. "
                "To allow it for local testing, set ALLOW_PRIVATE_WEBSITE_SCRAPE=1 on the server."
            )
        return
    except ValueError:
        # Not an IP literal; continue to DNS resolution.
        pass

    try:
        resolved = socket.getaddrinfo(hostname, None)
    except OSError:
        raise ValueError("Unable to resolve that website address.")

    for entry in resolved:
        sockaddr = entry[4]
        if not sockaddr:
            continue
        ip_value = sockaddr[0]
        if _is_disallowed_ip_address(ip_value) and not ALLOW_PRIVATE_WEBSITE_SCRAPE:
            raise ValueError(
                "That website resolves to a private/loopback IP which is blocked for safety. "
                "To allow it for local testing, set ALLOW_PRIVATE_WEBSITE_SCRAPE=1 on the server."
            )


def _fetch_html(url: str) -> tuple[str, str]:
    import requests

    _assert_safe_public_url(url)
    headers = {
        "User-Agent": "BookingAppAutopilotScraper/1.0",
        "Accept": "text/html,application/xhtml+xml",
    }

    response = requests.get(url, timeout=12, headers=headers, allow_redirects=True, stream=True)
    final_url = response.url or url
    _assert_safe_public_url(final_url)

    content_type = str(response.headers.get("content-type") or "").lower()
    if "text/html" not in content_type and "application/xhtml" not in content_type:
        raise ValueError("Only HTML pages can be scraped.")

    collected = bytearray()
    for chunk in response.iter_content(chunk_size=8192):
        if not chunk:
            continue
        collected.extend(chunk)
        if len(collected) > AUTOPILOT_WEBSITE_FETCH_BYTES_LIMIT:
            raise ValueError("That page is too large to scrape.")

    encoding = response.encoding or "utf-8"
    try:
        html_text = collected.decode(encoding, errors="ignore")
    except LookupError:
        html_text = collected.decode("utf-8", errors="ignore")

    return html_text, final_url


def _extract_website_text(html: str) -> dict:
    title = ""
    description = ""
    headings = []
    plain_text = ""

    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html or "", "html.parser")
        for tag in soup(["script", "style", "noscript", "svg", "canvas"]):
            tag.decompose()

        if soup.title and soup.title.string:
            title = str(soup.title.string).strip()

        meta = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        if meta and meta.get("content"):
            description = str(meta.get("content") or "").strip()

        for h in soup.find_all(["h1", "h2", "h3"], limit=30):
            text = h.get_text(" ", strip=True)
            if text:
                headings.append(text)

        plain_text = soup.get_text(" ", strip=True)
    except Exception:
        # Fallback: very basic stripping.
        plain_text = re.sub(r"<[^>]+>", " ", html or "")

    plain_text = re.sub(r"\s+", " ", plain_text).strip()

    sections = []
    if title:
        sections.append(f"Title: {title}")
    if description:
        sections.append(f"Description: {description}")
    if headings:
        sections.append("Headings: " + "; ".join(headings[:20]))

    if plain_text:
        sections.append("Content: " + plain_text)

    combined = "\n".join(sections).strip()
    return {
        "title": title,
        "description": description,
        "text": combined,
    }


def _canonicalize_crawl_url(candidate: str) -> str:
    """Normalize URLs so we don't crawl duplicates.

    We intentionally drop query/fragment to avoid crawl explosions.
    """
    parsed = urlparse(candidate)
    cleaned = parsed._replace(query="", fragment="")
    return cleaned.geturl()


def _is_scrapable_internal_url(candidate: str, *, base_host: str) -> bool:
    try:
        parsed = urlparse(candidate)
    except Exception:
        return False

    if parsed.scheme not in {"http", "https"}:
        return False

    hostname = (parsed.hostname or "").lower()
    if not hostname or hostname != (base_host or "").lower():
        return False

    path = (parsed.path or "").lower()
    if not path:
        path = "/"

    # Skip any admin-ish areas.
    if path.startswith("/admin") or "/admin/" in path:
        return False
    if "wp-admin" in path:
        return False

    # Skip obvious non-content / app endpoints.
    if path.startswith("/api") or "/api/" in path:
        return False
    if path.startswith("/static") or "/static/" in path:
        return False

    # Skip common binary assets by extension.
    for ext in (
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".svg",
        ".ico",
        ".pdf",
        ".zip",
        ".rar",
        ".7z",
        ".mp3",
        ".mp4",
        ".mov",
        ".avi",
        ".woff",
        ".woff2",
        ".ttf",
        ".eot",
        ".css",
        ".js",
    ):
        if path.endswith(ext):
            return False

    return True


def _scrape_website_knowledge(start_url: str) -> dict:
    # Crawl internal pages on the same domain, excluding admin areas.
    start_html, start_final_url = _fetch_html(start_url)
    base_host = (urlparse(start_final_url).hostname or "").lower()

    queue = deque([(start_final_url, start_html, 0)])
    visited = set()
    pages = []
    combined_sections = []

    while queue and len(pages) < AUTOPILOT_WEBSITE_MAX_PAGES:
        current_url, current_html, depth = queue.popleft()
        canonical = _canonicalize_crawl_url(current_url)
        if canonical in visited:
            continue
        visited.add(canonical)

        # Safety: apply URL filters again.
        if not _is_scrapable_internal_url(canonical, base_host=base_host):
            continue

        extracted = _extract_website_text(current_html)
        extracted_text = extracted.get("text") or ""

        combined_sections.append(f"Source: {canonical}\n{extracted_text}")
        pages.append({"url": canonical, "title": extracted.get("title") or "", "chars": len(extracted_text)})

        # Stop expanding if we hit depth limit or are near the knowledge cap.
        approx_len = sum(len(section) for section in combined_sections)
        if depth >= AUTOPILOT_WEBSITE_MAX_DEPTH or approx_len >= AUTOPILOT_WEBSITE_KNOWLEDGE_LIMIT:
            continue

        # Extract and enqueue next links.
        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(current_html or "", "html.parser")
            found = 0
            for a in soup.find_all("a", href=True):
                if found >= AUTOPILOT_WEBSITE_MAX_LINKS_PER_PAGE:
                    break
                href = str(a.get("href") or "").strip()
                if not href:
                    continue
                absolute = urljoin(canonical, href)
                next_url = _canonicalize_crawl_url(absolute)
                if next_url in visited:
                    continue
                if not _is_scrapable_internal_url(next_url, base_host=base_host):
                    continue

                try:
                    next_html, next_final = _fetch_html(next_url)
                except Exception:
                    continue

                next_final_canonical = _canonicalize_crawl_url(next_final)
                if next_final_canonical in visited:
                    continue
                if not _is_scrapable_internal_url(next_final_canonical, base_host=base_host):
                    continue

                queue.append((next_final_canonical, next_html, depth + 1))
                found += 1
        except Exception:
            continue

    combined_text = "\n\n".join([section for section in combined_sections if section]).strip()
    if len(combined_text) > AUTOPILOT_WEBSITE_KNOWLEDGE_LIMIT:
        combined_text = combined_text[:AUTOPILOT_WEBSITE_KNOWLEDGE_LIMIT]

    return {
        "url": start_url,
        "final_url": start_final_url,
        "pages": pages,
        "text": combined_text,
    }


def _ensure_static_images_dir() -> None:
    try:
        os.makedirs(STATIC_IMAGES_DIR, exist_ok=True)
    except OSError:
        pass


def _is_allowed_image_filename(filename: str) -> bool:
    name = str(filename or "")
    _, ext = os.path.splitext(name.lower())
    return ext in ALLOWED_IMAGE_EXTENSIONS


def _guess_image_extension(filename: str, mimetype: str) -> str:
    _, ext = os.path.splitext(str(filename or "").lower())
    if ext in ALLOWED_IMAGE_EXTENSIONS:
        return ext

    mt = str(mimetype or "").lower()
    if mt == "image/png":
        return ".png"
    if mt in {"image/jpg", "image/jpeg"}:
        return ".jpg"
    if mt == "image/gif":
        return ".gif"
    if mt == "image/webp":
        return ".webp"
    if mt == "image/svg+xml":
        return ".svg"
    if mt == "image/x-icon" or mt == "image/vnd.microsoft.icon":
        return ".ico"
    return ""


def _wrap_raster_bytes_in_svg(image_bytes: bytes, mimetype: str) -> str:
    mt = str(mimetype or "").strip().lower() or "image/png"
    # Basic hardening: only allow common raster types.
    if mt not in {"image/png", "image/jpeg", "image/jpg", "image/gif", "image/webp"}:
        mt = "image/png"
    b64 = base64.b64encode(image_bytes or b"").decode("ascii")
    # Use a generic viewBox; preserveAspectRatio keeps it nicely cropped/contained by CSS.
    return (
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"1600\" height=\"900\" viewBox=\"0 0 1600 900\" preserveAspectRatio=\"xMidYMid slice\">"
        f"<image href=\"data:{mt};base64,{b64}\" x=\"0\" y=\"0\" width=\"1600\" height=\"900\" preserveAspectRatio=\"xMidYMid slice\"/>"
        "</svg>"
    )


def _safe_image_relative_path(raw_path: str) -> str:
    """Return a safe relative path under static/images.

    Allows subfolders but blocks traversal and strips dangerous names.
    """
    text = str(raw_path or "").strip().replace("\\", "/")
    if not text:
        raise ValueError("Image path is required.")

    if text.startswith("/"):
        text = text.lstrip("/")

    # Allow callers to pass either "ai.png" or "images/ai.png".
    if text.lower().startswith("images/"):
        text = text[7:]

    parts = [p for p in text.split("/") if p and p not in {".", ".."}]
    if not parts:
        raise ValueError("Invalid image path.")

    sanitized_parts = []
    for part in parts[:-1]:
        safe_part = secure_filename(part)
        if not safe_part:
            continue
        sanitized_parts.append(safe_part)

    filename = secure_filename(parts[-1])
    if not filename:
        raise ValueError("Invalid image filename.")
    if not _is_allowed_image_filename(filename):
        raise ValueError("Only common image types are allowed (png/jpg/webp/svg/etc).")

    sanitized_parts.append(filename)
    rel_path = "/".join(sanitized_parts)

    # Final traversal safety check.
    abs_path = os.path.abspath(os.path.join(STATIC_IMAGES_DIR, *sanitized_parts))
    base_abs = os.path.abspath(STATIC_IMAGES_DIR)
    if os.path.commonpath([abs_path, base_abs]) != base_abs:
        raise ValueError("Invalid image path.")

    return rel_path


def _image_abs_path_from_rel(rel_path: str) -> str:
    parts = [p for p in str(rel_path).split("/") if p]
    return os.path.abspath(os.path.join(STATIC_IMAGES_DIR, *parts))


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


with _smsapi_config_lock:
    stored_smsapi = _load_smsapi_config_from_disk()
    _smsapi_config.update(stored_smsapi)


with _telnyx_config_lock:
    stored_telnyx = _load_telnyx_config_from_disk()
    _telnyx_config.update(stored_telnyx)


with _watchdog_config_lock:
    stored_watchdog = _load_watchdog_config_from_disk()
    _watchdog_config.update(stored_watchdog)


with _seo_config_lock:
    stored_seo = _load_seo_config_from_disk()
    _seo_config.update(stored_seo)


def _load_facebook_config_from_disk() -> dict:
    defaults = dict(_FACEBOOK_CONFIG_DEFAULTS)
    if not os.path.exists(FACEBOOK_CONFIG_FILE):
        return defaults
    try:
        with open(FACEBOOK_CONFIG_FILE, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict):
            return defaults
        merged = dict(defaults)
        merged.update(payload)
        return merged
    except (OSError, json.JSONDecodeError):
        return defaults


def _save_facebook_config(config: dict | None = None) -> None:
    snapshot = dict(config or _facebook_config)
    try:
        with open(FACEBOOK_CONFIG_FILE, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, indent=2)
    except OSError:
        pass


def _load_facebook_alerts_from_disk() -> list:
    if not os.path.exists(FACEBOOK_ALERTS_FILE):
        return []
    try:
        with open(FACEBOOK_ALERTS_FILE, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, list) else []
    except (OSError, json.JSONDecodeError):
        return []


def _save_facebook_alerts(alerts: list | None = None) -> None:
    data = alerts if alerts is not None else _facebook_alerts
    try:
        with open(FACEBOOK_ALERTS_FILE, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
    except OSError:
        pass


def _facebook_poll_once() -> dict:
    """Fetch recent group posts and create alerts for keyword matches. Returns a summary dict."""
    global _facebook_alerts, _facebook_known_post_ids

    with _facebook_config_lock:
        cfg = dict(_facebook_config)

    group_id = (cfg.get("group_id") or "").strip()
    access_token = (cfg.get("access_token") or "").strip()
    keywords_raw = cfg.get("keywords") or ""
    notify_sms = bool(cfg.get("notify_sms", False))

    if not group_id or not access_token:
        return {"ok": False, "error": "Group ID or access token not configured."}

    keywords = [k.strip().lower() for k in keywords_raw.split(",") if k.strip()]
    if not keywords:
        return {"ok": False, "error": "No keywords configured."}

    api_url = (
        f"https://graph.facebook.com/v21.0/{group_id}/feed"
        f"?fields=id,message,from,created_time,permalink_url"
        f"&access_token={access_token}"
        f"&limit=50"
    )

    try:
        req = Request(api_url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8")[:400]
        except Exception:
            pass
        error_msg = f"HTTP {e.code}: {body}"
        now_str = datetime.utcnow().isoformat()
        with _facebook_config_lock:
            _facebook_config["last_checked"] = now_str
            _facebook_config["last_error"] = error_msg
        _save_facebook_config()
        return {"ok": False, "error": error_msg}
    except Exception as exc:
        error_msg = str(exc)[:300]
        now_str = datetime.utcnow().isoformat()
        with _facebook_config_lock:
            _facebook_config["last_checked"] = now_str
            _facebook_config["last_error"] = error_msg
        _save_facebook_config()
        return {"ok": False, "error": error_msg}

    posts = raw.get("data") or []
    new_alerts = []

    with _facebook_alerts_lock:
        known = set(_facebook_known_post_ids)

        for post in posts:
            post_id = post.get("id") or ""
            message = (post.get("message") or "").strip()
            if not post_id or not message or post_id in known:
                continue

            message_lower = message.lower()
            matched = [kw for kw in keywords if kw in message_lower]
            if not matched:
                continue

            author_name = ""
            if isinstance(post.get("from"), dict):
                author_name = post["from"].get("name") or ""

            alert = {
                "id": str(uuid4()),
                "post_id": post_id,
                "message_preview": message[:400],
                "author_name": author_name,
                "created_time": post.get("created_time") or "",
                "permalink_url": post.get("permalink_url") or f"https://www.facebook.com/groups/{group_id}",
                "matched_keywords": matched,
                "seen": False,
                "alerted_at": datetime.utcnow().isoformat(),
            }
            _facebook_alerts.insert(0, alert)
            _facebook_known_post_ids.add(post_id)
            new_alerts.append(alert)

        # Keep only the newest 200 alerts to prevent unbounded growth
        if len(_facebook_alerts) > 200:
            _facebook_alerts = _facebook_alerts[:200]
            _facebook_known_post_ids = {a["post_id"] for a in _facebook_alerts}

        _save_facebook_alerts(_facebook_alerts)

    # Update last_checked and clear any previous error
    now_str = datetime.utcnow().isoformat()
    with _facebook_config_lock:
        _facebook_config["last_checked"] = now_str
        _facebook_config["last_error"] = ""
    _save_facebook_config()

    # Optional SMS notification
    if notify_sms and new_alerts:
        for alert in new_alerts[:3]:  # Batch max 3 SMS per poll
            sms_body = (
                f"Facebook alert: {alert['author_name']} posted in your group.\n"
                f"\"{alert['message_preview'][:120]}\"\n"
                f"Keywords: {', '.join(alert['matched_keywords'])}"
            )
            watchdog_to = _watchdog_config.get("to_number") or "+447595289669"
            _send_sms_via_telnyx(watchdog_to, sms_body)

    return {"ok": True, "new_alerts": len(new_alerts), "posts_checked": len(posts)}


def _start_facebook_poller() -> None:
    """Launch the Facebook group monitoring poller in a daemon background thread."""
    import threading
    import time

    def _poller_loop():
        time.sleep(30)  # Initial delay — let server fully start
        while True:
            try:
                with _facebook_config_lock:
                    cfg = dict(_facebook_config)

                if cfg.get("enabled") and cfg.get("group_id") and cfg.get("access_token"):
                    _facebook_poll_once()

                interval = int(cfg.get("poll_interval_minutes") or 30)
                interval = max(5, min(interval, 120))  # clamp 5–120 min
            except Exception:
                interval = 30

            time.sleep(interval * 60)

    t = threading.Thread(target=_poller_loop, name="fb-group-poller", daemon=True)
    t.start()


with _facebook_config_lock:
    _stored_fb_cfg = _load_facebook_config_from_disk()
    _facebook_config.update(_stored_fb_cfg)

with _facebook_alerts_lock:
    _stored_fb_alerts = _load_facebook_alerts_from_disk()
    _facebook_alerts.extend(_stored_fb_alerts)
    _facebook_known_post_ids.update(
        a["post_id"] for a in _stored_fb_alerts if a.get("post_id")
    )


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

    website_knowledge = str(config.get("business_website_knowledge", "") or "").strip()
    if not website_knowledge:
        website_knowledge = "No website knowledge has been scraped yet."

    instructions = (
        "You are Autopilot, a friendly, down-to-earth live chat assistant for a UK gardening and maintenance business. "
        "Write like a real person: use natural phrasing and contractions, and keep it warm and helpful (not robotic or overly formal). "
        "Keep replies short (usually 1–3 sentences). Ask one quick clarifying question if it helps. "
        "Use the business knowledge below and the conversation; do not invent details, prices, or availability. "
        "If you are unsure, say you will pass it to the team and offer to take their postcode and preferred day/time."
    )

    messages = [
        {
            "role": "system",
            "content": f"{instructions}\n\nBusiness knowledge (manual notes):\n{business_profile}\n\nBusiness knowledge (website):\n{website_knowledge}",
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


def _request_autopilot_reply(messages, *, provider: str, model: str, temperature: float, api_key: str) -> str:
    if not api_key or not messages:
        return ""

    provider = (provider or DEFAULT_AUTOPILOT_PROVIDER).strip().lower()
    resolved_model = (model or DEFAULT_AUTOPILOT_MODEL).strip() or DEFAULT_AUTOPILOT_MODEL
    if provider == "openrouter" and "/" not in resolved_model:
        # OpenRouter model identifiers are typically namespaced (e.g. deepseek/deepseek-chat).
        # Keep backwards compatibility with existing DeepSeek defaults.
        if resolved_model == "deepseek-chat":
            resolved_model = "deepseek/deepseek-chat"

    payload = {
        "model": resolved_model,
        "messages": messages,
        "temperature": float(temperature),
        "max_tokens": 350,
    }

    if provider == "openrouter":
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        try:
            referer = str(request.host_url or "").strip()
        except Exception:
            referer = ""
        if referer:
            headers["HTTP-Referer"] = referer
        headers["X-Title"] = "Booking App Autopilot"
    else:
        url = "https://api.deepseek.com/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
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


def _resolve_primary_api_key_from_list(keys_payload) -> str:
    if not isinstance(keys_payload, list):
        return ""
    entry = next(
        (item for item in keys_payload if isinstance(item, dict) and str(item.get("value") or "").strip()),
        None,
    )
    return str(entry.get("value") or "").strip() if entry else ""


def _resolve_autopilot_api_key(provider: str, config: dict) -> str:
    provider = (provider or DEFAULT_AUTOPILOT_PROVIDER).strip().lower()
    if provider == "openrouter":
        env_key = (os.environ.get("OPENROUTER_API_KEY") or "").strip()
        stored_key = (
            _resolve_primary_api_key_from_list(config.get("openrouter_api_keys"))
            or str(config.get("openrouter_api_key") or "").strip()
        )
        return stored_key or env_key

    env_key = (os.environ.get("DEEPSEEK_API_KEY") or "").strip() or (
        os.environ.get("OPENAI_API_KEY") or ""
    ).strip()
    stored_key = (
        _resolve_primary_api_key_from_list(config.get("api_keys"))
        or str(config.get("api_key") or "").strip()
    )
    return stored_key or env_key


def _maybe_send_autopilot_reply(session_id: str, conversation=None):
    config = _autopilot_config_snapshot(include_secret=True)
    if not config.get("enabled"):
        return None

    provider = str(config.get("provider", DEFAULT_AUTOPILOT_PROVIDER) or DEFAULT_AUTOPILOT_PROVIDER)
    api_key = _resolve_autopilot_api_key(provider, config)
    if not api_key:
        return None

    messages = _build_autopilot_messages(conversation or [], config)
    if len(messages) <= 1:
        return None

    reply_text = _request_autopilot_reply(
        messages,
        provider=provider,
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
@require_admin_auth
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
@require_admin_auth
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
@require_admin_auth
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


@app.route("/admin/autopilot/scrape", methods=["POST"])
@require_admin_auth
def admin_autopilot_scrape_website():
    payload = request.get_json(silent=True) or {}
    raw_url = payload.get("url") or payload.get("business_website_url") or ""

    try:
        normalized = _normalize_website_url(str(raw_url))
        if not normalized:
            return jsonify({"message": "Website address is required."}), 400

        scraped = _scrape_website_knowledge(normalized)
        scraped_text = str(scraped.get("text") or "").strip()
        if not scraped_text:
            return jsonify({"message": "We couldn't extract any useful text from that page."}), 400

        now = datetime.utcnow().isoformat()

        with _autopilot_lock:
            _autopilot_config["business_website_url"] = str(scraped.get("final_url") or normalized)
            _autopilot_config["business_website_knowledge"] = scraped_text
            _autopilot_config["business_website_last_scraped"] = now
            snapshot = dict(_autopilot_config)

        _save_autopilot_config(snapshot)
        return jsonify(
            {
                "message": "Website scraped and saved for autopilot.",
                "pages": scraped.get("pages") or [],
                "config": _autopilot_config_snapshot(),
            }
        )
    except ValueError as error:
        return jsonify({"message": str(error) or "Unable to scrape that website."}), 400
    except ModuleNotFoundError:
        return (
            jsonify(
                {
                    "message": (
                        "Website scraping dependencies are not installed on the server. "
                        "Install `requests` and `beautifulsoup4` (or run `pip install -r requirements.txt`) and restart the app."
                    )
                }
            ),
            500,
        )
    except Exception:
        return jsonify({"message": "Unable to scrape that website right now."}), 500


@app.route("/admin/autopilot/scrape/clear", methods=["POST"])
@require_admin_auth
def admin_autopilot_clear_website_knowledge():
    with _autopilot_lock:
        _autopilot_config["business_website_knowledge"] = ""
        _autopilot_config["business_website_last_scraped"] = ""
        snapshot = dict(_autopilot_config)
    _save_autopilot_config(snapshot)
    return jsonify({"message": "Website knowledge cleared.", "config": _autopilot_config_snapshot()})


@app.route("/admin/assets/images", methods=["GET"])
@require_admin_auth
def admin_list_site_images():
    _ensure_static_images_dir()

    images = []
    for root, _, files in os.walk(STATIC_IMAGES_DIR):
        for filename in files:
            if not _is_allowed_image_filename(filename):
                continue

            abs_path = os.path.join(root, filename)
            try:
                stat = os.stat(abs_path)
            except OSError:
                continue

            rel_under_images = os.path.relpath(abs_path, STATIC_IMAGES_DIR).replace("\\", "/")
            url_path = f"/static/images/{rel_under_images}"
            images.append(
                {
                    "path": f"images/{rel_under_images}",
                    "url": url_path,
                    "size": int(getattr(stat, "st_size", 0) or 0),
                    "modified": datetime.utcfromtimestamp(getattr(stat, "st_mtime", 0) or 0).isoformat(),
                }
            )

    images.sort(key=lambda item: item.get("path") or "")
    return jsonify({"images": images})


@app.route("/admin/assets/images/upload", methods=["POST"])
@require_admin_auth
def admin_upload_site_image():
    _ensure_static_images_dir()

    uploaded = request.files.get("file")
    if not uploaded:
        return jsonify({"message": "No file uploaded."}), 400

    upload_ext = _guess_image_extension(uploaded.filename or "", getattr(uploaded, "mimetype", "") or "")
    if not upload_ext:
        return jsonify({"message": "Unsupported image type. Please upload png/jpg/webp/gif/svg."}), 400

    target_path = (request.form.get("target_path") or request.form.get("path") or "").strip()
    if target_path:
        try:
            rel_path = _safe_image_relative_path(target_path)
        except ValueError as error:
            return jsonify({"message": str(error)}), 400
    else:
        filename = secure_filename(uploaded.filename or "")
        if not filename:
            return jsonify({"message": "Invalid filename."}), 400
        if not _is_allowed_image_filename(filename):
            return jsonify({"message": "Only common image types are allowed (png/jpg/webp/svg/etc)."}), 400
        rel_path = filename

    _, target_ext = os.path.splitext(rel_path.lower())
    if target_path:
        # Allow replacing an .svg slot with a raster upload by wrapping it into an SVG container.
        if target_ext == ".svg" and upload_ext in RASTER_IMAGE_EXTENSIONS:
            try:
                content = uploaded.read()
            except Exception:
                return jsonify({"message": "Unable to read uploaded image."}), 400

            try:
                svg_payload = _wrap_raster_bytes_in_svg(content, getattr(uploaded, "mimetype", "") or "")
            except Exception:
                return jsonify({"message": "Unable to process uploaded image."}), 400

            abs_path = _image_abs_path_from_rel(rel_path)
            abs_dir = os.path.dirname(abs_path)
            try:
                os.makedirs(abs_dir, exist_ok=True)
                with open(abs_path, "w", encoding="utf-8") as handle:
                    handle.write(svg_payload)
            except OSError:
                return jsonify({"message": "Unable to save uploaded image."}), 500

            rel_url = rel_path.replace("\\", "/")
            return jsonify(
                {
                    "message": "Image uploaded.",
                    "image": {
                        "path": f"images/{rel_url}",
                        "url": f"/static/images/{rel_url}",
                    },
                }
            )

        # For other targets, require the extension to match so existing references remain valid.
        if target_ext and upload_ext and target_ext != upload_ext:
            return (
                jsonify(
                    {
                        "message": (
                            f"This image slot expects a {target_ext.upper()} file. "
                            f"Upload a matching file type or upload as a new image instead."
                        )
                    }
                ),
                400,
            )

    abs_path = _image_abs_path_from_rel(rel_path)
    abs_dir = os.path.dirname(abs_path)
    try:
        os.makedirs(abs_dir, exist_ok=True)
    except OSError:
        return jsonify({"message": "Unable to create image folder."}), 500

    try:
        # Ensure stream is positioned at start (in case it was inspected).
        try:
            uploaded.stream.seek(0)
        except Exception:
            pass
        uploaded.save(abs_path)
    except Exception:
        return jsonify({"message": "Unable to save uploaded image."}), 500

    rel_url = rel_path.replace("\\", "/")
    return jsonify(
        {
            "message": "Image uploaded.",
            "image": {
                "path": f"images/{rel_url}",
                "url": f"/static/images/{rel_url}",
            },
        }
    )


@app.route("/admin/assets/images/delete", methods=["POST"])
@require_admin_auth
def admin_delete_site_image():
    payload = request.get_json(silent=True) or {}
    raw_path = payload.get("path") or ""
    try:
        rel_path = _safe_image_relative_path(raw_path)
    except ValueError as error:
        return jsonify({"message": str(error)}), 400

    abs_path = _image_abs_path_from_rel(rel_path)
    if not os.path.exists(abs_path):
        return jsonify({"message": "Image not found."}), 404

    try:
        os.remove(abs_path)
    except OSError:
        return jsonify({"message": "Unable to delete image."}), 500

    return jsonify({"message": "Image deleted.", "path": f"images/{rel_path}"})


@app.route("/admin/weather/config", methods=["GET", "POST"])
@require_admin_auth
def admin_weather_config():
    if request.method == "GET":
        return jsonify({"config": _weather_config_snapshot()})

    payload = request.get_json(silent=True) or {}
    incoming_key = str(payload.get("api_key", "") or "").strip()

    with _weather_config_lock:
        if incoming_key:
            _weather_config["api_key"] = incoming_key
            _weather_config["api_keys"] = _merge_api_key(
                incoming_key, _weather_config.get("api_keys", [])
            )
        elif "api_key" in payload:
            _weather_config["api_key"] = ""
            _weather_config["api_keys"] = []
        snapshot = dict(_weather_config)

    _save_weather_config(snapshot)
    _weather_forecast_cache.clear()
    return jsonify({"message": "Weather settings updated.", "config": _weather_config_snapshot()})


@app.route("/admin/smsapi/config", methods=["GET", "POST"])
@require_admin_auth
def admin_smsapi_config():
    if request.method == "GET":
        return jsonify({"config": _smsapi_config_snapshot()})

    payload = request.get_json(silent=True) or {}
    oauth_token = str(payload.get("oauth_token", "") or "").strip()
    sender_name = str(payload.get("sender_name", "") or "").strip()

    with _smsapi_config_lock:
        _smsapi_config["oauth_token"] = oauth_token
        _smsapi_config["sender_name"] = sender_name
        snapshot = dict(_smsapi_config)

    _save_smsapi_config(snapshot)
    return jsonify({"message": "SMSAPI settings updated.", "config": _smsapi_config_snapshot()})


@app.route("/admin/telnyx/config", methods=["GET", "POST"])
@require_admin_auth
def admin_telnyx_config():
    if request.method == "GET":
        return jsonify({"config": _telnyx_config_snapshot()})

    payload = request.get_json(silent=True) or {}
    api_key_present = "api_key" in payload
    from_present = "from_number" in payload
    profile_present = "messaging_profile_id" in payload
    api_key = str(payload.get("api_key", "") or "").strip()
    from_number = str(payload.get("from_number", "") or "").strip()
    messaging_profile_id = str(payload.get("messaging_profile_id", "") or "").strip()

    # Normalize from number if provided.
    if from_number:
        from_number = _normalize_phone_number(from_number)

    with _telnyx_config_lock:
        # Only overwrite stored values if the caller provided a value.
        # This prevents the UI from accidentally clearing secrets when the admin
        # clicks save without re-typing them.
        if api_key_present:
            if api_key:
                _telnyx_config["api_key"] = api_key
            elif api_key_present:
                # Explicit clear.
                _telnyx_config["api_key"] = ""

        if from_present:
            if from_number:
                _telnyx_config["from_number"] = from_number
            elif from_present:
                _telnyx_config["from_number"] = ""

        if profile_present:
            _telnyx_config["messaging_profile_id"] = messaging_profile_id

        snapshot = dict(_telnyx_config)

    _save_telnyx_config(snapshot)
    return jsonify({"message": "Telnyx settings updated.", "config": _telnyx_config_snapshot()})


@app.route("/admin/telnyx/test", methods=["POST"])
@require_admin_auth
def admin_telnyx_test():
    """Send a real test SMS to verify Telnyx credentials are working."""
    payload = request.get_json(silent=True) or {}
    test_phone = str(payload.get("phone", "") or "").strip()

    if not test_phone:
        return jsonify({"ok": False, "message": "Please enter a phone number to send the test SMS to."}), 400

    # Diagnostic: check what's configured
    cfg = _telnyx_config_snapshot(include_secret=True)
    diagnostics = {
        "api_key_set": bool((cfg.get("api_key") or "").strip()),
        "api_key_length": len((cfg.get("api_key") or "").strip()),
        "from_number": cfg.get("from_number", ""),
        "messaging_profile_id": cfg.get("messaging_profile_id", ""),
        "has_config": cfg.get("has_config", False),
    }

    if not diagnostics["api_key_set"]:
        return jsonify({
            "ok": False,
            "message": "API key is empty. Please save your Telnyx API key first.",
            "diagnostics": diagnostics,
        }), 400

    if not diagnostics["from_number"]:
        return jsonify({
            "ok": False,
            "message": "From number is empty. Please save your Telnyx phone number first.",
            "diagnostics": diagnostics,
        }), 400

    # Normalize and send
    normalized_phone = _normalize_phone_number(test_phone)
    if not normalized_phone or not normalized_phone.startswith("+"):
        return jsonify({"ok": False, "message": f"Invalid phone number format: '{test_phone}'. Use e.g. 07123456789 or +447123456789."}), 400

    test_message = "Test from Pay As You Mow admin panel. If you see this, Telnyx SMS is working!"
    print(f"[Telnyx-Test] Sending test SMS to {normalized_phone}")

    success, error_msg = _send_sms_via_telnyx(normalized_phone, test_message)

    if success:
        return jsonify({
            "ok": True,
            "message": f"Test SMS sent successfully to {normalized_phone}. Check your phone!",
            "diagnostics": diagnostics,
        })
    else:
        return jsonify({
            "ok": False,
            "message": f"Failed to send: {error_msg}",
            "diagnostics": diagnostics,
        }), 500


@app.route("/admin/telnyx/diagnostics", methods=["GET"])
@require_admin_auth
def admin_telnyx_diagnostics():
    """Return diagnostic info about Telnyx configuration without sending anything."""
    cfg = _telnyx_config_snapshot(include_secret=False)
    cfg_secret = _telnyx_config_snapshot(include_secret=True)

    api_key_raw = (cfg_secret.get("api_key") or "").strip()
    from_number_raw = (cfg_secret.get("from_number") or "").strip()

    checks = []
    all_ok = True

    # Check 1: API key
    if api_key_raw:
        if api_key_raw.startswith("KEY"):
            checks.append({"label": "API Key", "status": "ok", "detail": f"Set ({len(api_key_raw)} chars, starts with KEY...)"})
        else:
            checks.append({"label": "API Key", "status": "warn", "detail": f"Set ({len(api_key_raw)} chars) but doesn't start with 'KEY' — double-check it's correct"})
    else:
        checks.append({"label": "API Key", "status": "fail", "detail": "Not set"})
        all_ok = False

    # Check 2: From number
    normalized_from = _normalize_phone_number(from_number_raw)
    if normalized_from and normalized_from.startswith("+"):
        checks.append({"label": "From Number", "status": "ok", "detail": normalized_from})
    elif from_number_raw:
        checks.append({"label": "From Number", "status": "warn", "detail": f"Set as '{from_number_raw}' but may not be in valid E.164 format"})
    else:
        checks.append({"label": "From Number", "status": "fail", "detail": "Not set"})
        all_ok = False

    # Check 3: Messaging profile
    profile_id = (cfg.get("messaging_profile_id") or "").strip()
    if profile_id:
        checks.append({"label": "Messaging Profile ID", "status": "ok", "detail": profile_id})
    else:
        checks.append({"label": "Messaging Profile ID", "status": "info", "detail": "Not set (optional)"})

    # Check 4: Environment variables
    env_key = os.environ.get("TELNYX_API_KEY", "").strip()
    env_from = os.environ.get("TELNYX_FROM_NUMBER", "").strip()
    if env_key or env_from:
        checks.append({"label": "Environment Variables", "status": "info", "detail": f"TELNYX_API_KEY={'set' if env_key else 'not set'}, TELNYX_FROM_NUMBER={'set' if env_from else 'not set'}"})

    # Check 5: Config file existence
    config_file_exists = os.path.exists(TELNYX_CONFIG_FILE)
    checks.append({"label": "Config File", "status": "ok" if config_file_exists else "warn", "detail": f"{'Exists' if config_file_exists else 'Not found'} ({TELNYX_CONFIG_FILE})"})

    return jsonify({
        "all_ok": all_ok,
        "has_config": cfg.get("has_config", False),
        "checks": checks,
    })


# ---------------------------------------------------------------------------
# Health endpoint (used by watchdog self-ping)
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Admin: Server-Down Watchdog config
# ---------------------------------------------------------------------------

@app.route("/admin/watchdog/config", methods=["GET", "POST"])
@require_admin_auth
def admin_watchdog_config():
    global _watchdog_config

    if request.method == "GET":
        with _watchdog_config_lock:
            cfg = dict(_watchdog_config)
        return jsonify({"config": cfg})

    payload = request.get_json(silent=True) or {}
    enabled = bool(payload.get("enabled", False))
    to_number_raw = str(payload.get("to_number", "") or "").strip() or "+447595289669"
    to_number = _normalize_phone_number(to_number_raw) or to_number_raw

    with _watchdog_config_lock:
        _watchdog_config["enabled"] = enabled
        _watchdog_config["to_number"] = to_number
        cfg = dict(_watchdog_config)

    _save_watchdog_config(cfg)
    return jsonify({"message": "Watchdog settings saved.", "config": cfg})


@app.route("/admin/watchdog/reset-timer", methods=["POST"])
@require_admin_auth
def admin_watchdog_reset_timer():
    """Clear the 24-hour cooldown so the next failure will send immediately."""
    global _watchdog_config
    with _watchdog_config_lock:
        _watchdog_config["last_sent"] = None
        cfg = dict(_watchdog_config)
    _save_watchdog_config(cfg)
    return jsonify({"message": "Watchdog cooldown timer reset.", "config": cfg})


@app.route("/admin/watchdog/test", methods=["POST"])
@require_admin_auth
def admin_watchdog_test():
    """Send an immediate test watchdog alert, bypassing the 24-hour cooldown."""
    with _watchdog_config_lock:
        cfg = dict(_watchdog_config)

    to_number = cfg.get("to_number") or "+447595289669"
    msg = "TEST ALERT: Pay As You Mow server-down watchdog test. If you see this, it's working!"
    ok, detail = _send_sms_via_telnyx(to_number, msg)
    if ok:
        return jsonify({"ok": True, "message": f"Test alert sent to {to_number}."})
    return jsonify({"ok": False, "message": f"Failed: {detail}"}), 500


# ---------------------------------------------------------------------------
# SEO: sitemap.xml + robots.txt
# ---------------------------------------------------------------------------

@app.route("/sitemap.xml", methods=["GET"])
def sitemap_xml():
    seo = _seo_snapshot()
    if not seo.get("sitemap_enabled", True):
        return "", 404
    canonical = (seo.get("canonical_url") or "").rstrip("/")
    if not canonical:
        canonical = request.url_root.rstrip("/")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    entries = [
        f'  <url>\n    <loc>{canonical}/</loc>\n    <lastmod>{today}</lastmod>\n    <changefreq>weekly</changefreq>\n    <priority>1.0</priority>\n  </url>'
    ]
    for slug in _LOCAL_AREA_PAGES:
        entries.append(
            f'  <url>\n    <loc>{canonical}/lawn-mowing-{slug}</loc>\n    <lastmod>{today}</lastmod>\n    <changefreq>monthly</changefreq>\n    <priority>0.8</priority>\n  </url>'
        )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(entries)
        + "\n</urlset>"
    )
    return Response(xml, mimetype="application/xml")


@app.route("/lawn-mowing-<area_slug>")
def local_area_page(area_slug: str):
    """Location-specific SEO landing page, e.g. /lawn-mowing-manchester"""
    area_data = _LOCAL_AREA_PAGES.get(area_slug.lower())
    if not area_data:
        abort(404)
    seo = _seo_snapshot()
    reviews = load_reviews()
    base_url = (seo.get("canonical_url") or "").rstrip("/")
    return render_template(
        "area_service.html",
        area=area_data,
        area_slug=area_slug,
        seo=seo,
        reviews=reviews,
        base_url=base_url,
    )


@app.route("/robots.txt", methods=["GET"])
def robots_txt():
    seo = _seo_snapshot()
    canonical = (seo.get("canonical_url") or "").rstrip("/")
    if not canonical:
        canonical = request.url_root.rstrip("/")
    if seo.get("enabled", False) and seo.get("robots_index", True):
        content = (
            "User-agent: *\n"
            "Allow: /\n"
            "Disallow: /admin\n"
            "Disallow: /admin/\n"
            f"Sitemap: {canonical}/sitemap.xml\n"
        )
    else:
        content = "User-agent: *\nDisallow: /\n"
    return Response(content, mimetype="text/plain")


# ---------------------------------------------------------------------------
# Admin: SEO configuration
# ---------------------------------------------------------------------------

@app.route("/admin/seo/config", methods=["GET", "POST"])
@require_admin_auth
def admin_seo_config():
    global _seo_config

    if request.method == "GET":
        return jsonify({"config": _seo_snapshot()})

    payload = request.get_json(silent=True) or {}

    with _seo_config_lock:
        for key in _SEO_DEFAULTS:
            if key in payload:
                val = payload[key]
                if isinstance(_SEO_DEFAULTS[key], bool):
                    _seo_config[key] = bool(val)
                else:
                    _seo_config[key] = str(val) if val is not None else ""
        cfg = dict(_seo_config)

    _save_seo_config(cfg)
    return jsonify({"message": "SEO settings saved.", "config": cfg})


# ── Facebook Group Monitor admin routes ──────────────────────────────────────

@app.route("/admin/facebook/config", methods=["GET", "POST"])
@require_admin_auth
def admin_facebook_config():
    if request.method == "GET":
        with _facebook_config_lock:
            snap = dict(_facebook_config)
        return jsonify({"config": snap})

    payload = request.get_json(silent=True) or {}
    with _facebook_config_lock:
        if "enabled" in payload:
            _facebook_config["enabled"] = bool(payload["enabled"])
        for key in ("group_id", "access_token", "token_expiry_date"):
            if key in payload:
                _facebook_config[key] = str(payload[key]).strip()
        if "keywords" in payload:
            raw_kw = payload["keywords"]
            if isinstance(raw_kw, list):
                _facebook_config["keywords"] = [str(k).strip() for k in raw_kw if str(k).strip()]
            else:
                _facebook_config["keywords"] = [k.strip() for k in str(raw_kw).split(",") if k.strip()]
        if "poll_interval_minutes" in payload:
            try:
                _facebook_config["poll_interval_minutes"] = max(5, min(120, int(payload["poll_interval_minutes"])))
            except (ValueError, TypeError):
                pass
        if "notify_sms" in payload:
            _facebook_config["notify_sms"] = bool(payload["notify_sms"])
        cfg = dict(_facebook_config)
    _save_facebook_config(cfg)
    return jsonify({"message": "Facebook settings saved.", "config": cfg})


@app.route("/admin/facebook/alerts", methods=["GET"])
@require_admin_auth
def admin_facebook_alerts():
    with _facebook_alerts_lock:
        alerts_copy = list(_facebook_alerts)
    unseen = sum(1 for a in alerts_copy if not a.get("seen", False))
    return jsonify({"alerts": alerts_copy, "unseen_count": unseen})


@app.route("/admin/facebook/alerts/mark-seen", methods=["POST"])
@require_admin_auth
def admin_facebook_mark_seen():
    payload = request.get_json(silent=True) or {}
    mark_all = payload.get("all", False)
    ids = payload.get("ids", [])
    with _facebook_alerts_lock:
        for alert in _facebook_alerts:
            if mark_all or alert.get("id") in ids:
                alert["seen"] = True
        _save_facebook_alerts()
    return jsonify({"message": "Alerts marked as seen."})


@app.route("/admin/facebook/alerts/clear", methods=["POST"])
@require_admin_auth
def admin_facebook_clear_alerts():
    global _facebook_alerts, _facebook_known_post_ids
    with _facebook_alerts_lock:
        _facebook_alerts.clear()
        _facebook_known_post_ids.clear()
        _save_facebook_alerts()
    return jsonify({"message": "All alerts cleared."})


@app.route("/admin/facebook/test", methods=["POST"])
@require_admin_auth
def admin_facebook_test():
    result = _facebook_poll_once()
    return jsonify(result)


# ── End Facebook Group Monitor admin routes ───────────────────────────────────


@app.route("/admin/email/config", methods=["GET", "POST"])
@require_admin_auth
def admin_email_config():
    if request.method == "GET":
        return jsonify({"config": _smtp_config_snapshot()})

    payload = request.get_json(silent=True) or {}

    host = str(payload.get("host", "") or "").strip()
    port_raw = payload.get("port", 587)
    username = str(payload.get("username", "") or "").strip()
    password = str(payload.get("password", "") or "").strip()
    from_email = str(payload.get("from_email", "") or "").strip()
    from_name = str(payload.get("from_name", "") or "").strip()
    use_starttls = bool(payload.get("use_starttls", True))

    # Helpful defaults for Gmail-style setups.
    if not from_email and _is_valid_email(username):
        from_email = username

    try:
        port = int(port_raw)
    except Exception:
        port = 587
    if port <= 0 or port > 65535:
        return jsonify({"message": "SMTP port must be between 1 and 65535."}), 400

    if from_email and not _is_valid_email(from_email):
        return jsonify({"message": "From email address is not valid."}), 400

    with _smtp_config_lock:
        _smtp_config["host"] = host
        _smtp_config["port"] = port
        _smtp_config["username"] = username
        # Only overwrite password if provided, so admin can edit other fields safely.
        if password:
            _smtp_config["password"] = password
        _smtp_config["from_email"] = from_email
        if from_name:
            _smtp_config["from_name"] = from_name
        _smtp_config["use_starttls"] = use_starttls
        snapshot = dict(_smtp_config)

    _save_smtp_config(snapshot)
    return jsonify({"message": "Email settings updated.", "config": _smtp_config_snapshot()})


@app.route("/api/send-email-verification", methods=["POST"])
def api_send_email_verification():
    payload = request.get_json(silent=True) or {}
    email = str(payload.get("email", "") or "").strip().lower()
    if not _is_valid_email(email):
        return jsonify({"message": "Please enter a valid email address."}), 400

    _purge_expired_email_magic()

    # Very small rate-limit to avoid abuse.
    now = datetime.utcnow()
    ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
    rate_key_email = f"email:{email}"
    rate_key_ip = f"ip:{ip}"
    with _email_magic_lock:
        last_email = _email_send_rate_limit.get(rate_key_email)
        last_ip = _email_send_rate_limit.get(rate_key_ip)
        if last_email and (now - last_email) < timedelta(seconds=25):
            return jsonify({"message": "Please wait a moment before requesting another email."}), 429
        if last_ip and (now - last_ip) < timedelta(seconds=10):
            return jsonify({"message": "Please wait a moment before requesting another email."}), 429
        _email_send_rate_limit[rate_key_email] = now
        _email_send_rate_limit[rate_key_ip] = now

    token = str(uuid4())
    expires = now + timedelta(minutes=30)

    verify_url = urljoin(request.host_url, f"verify-email?token={token}")
    subject = "Confirm your quote request"
    text_body = (
        "Please confirm your quote request by clicking the link below:\n\n"
        f"{verify_url}\n\n"
        "If you didn't request this, you can ignore this email."
    )
    html_body = f"""
    <div style="font-family: Arial, sans-serif; line-height: 1.5;">
      <h2 style="margin: 0 0 12px 0;">Confirm your quote request</h2>
      <p style="margin: 0 0 14px 0;">Please confirm it's really you by clicking this link:</p>
      <p style="margin: 0 0 18px 0;"><a href="{verify_url}">{verify_url}</a></p>
      <p style="color: #666; font-size: 13px; margin: 0;">If you didn't request this, you can ignore this email.</p>
    </div>
    """

    ok, err = _send_email_via_smtp(to_email=email, subject=subject, text_body=text_body, html_body=html_body)
    if not ok:
        return jsonify({"message": err or "Unable to send verification email."}), 500

    with _email_magic_lock:
        _email_magic_tokens[token] = {
            "email": email,
            "created_at": now.isoformat(),
            "expires": expires.isoformat(),
        }
        _save_email_magic_to_disk(dict(_email_magic_tokens), dict(_verified_emails))

    return jsonify({"message": "Verification email sent.", "email": email})


@app.route("/verify-email", methods=["GET"])
def verify_email_magic_link():
    token = str(request.args.get("token", "") or "").strip()
    if not token:
        return "Missing token.", 400

    _purge_expired_email_magic()
    now = datetime.utcnow()

    with _email_magic_lock:
        record = _email_magic_tokens.get(token)

        if not record:
            return (
                "This verification link is invalid or has expired. Please go back and request a new one.",
                400,
            )

        email = str(record.get("email") or "").strip().lower()
        # Mark verified for 30 minutes.
        _verified_emails[email] = {
            "verified_at": now.isoformat(),
            "expires": (now + timedelta(minutes=30)).isoformat(),
        }
        # One-time link
        _email_magic_tokens.pop(token, None)
        _save_email_magic_to_disk(dict(_email_magic_tokens), dict(_verified_emails))

    html = f"""
    <!doctype html>
    <html lang=\"en\">
      <head>
        <meta charset=\"utf-8\" />
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
        <title>Email verified</title>
        <style>
          body {{ font-family: Arial, sans-serif; padding: 24px; max-width: 720px; margin: 0 auto; }}
          .card {{ border: 1px solid #e6e6e6; border-radius: 14px; padding: 18px 20px; }}
          .ok {{ color: #2e7d32; font-weight: 700; }}
          a {{ color: #2e7d32; }}
        </style>
      </head>
      <body>
        <div class=\"card\">
          <p class=\"ok\">✅ Email verified</p>
          <p>Thanks — you can now return to the website and continue your quote request.</p>
          <p style=\"color:#666; font-size: 13px;\">Verified for: {email}</p>
        </div>
      </body>
    </html>
    """
    return html


@app.route("/api/email-verification-status", methods=["GET"])
def api_email_verification_status():
    email = str(request.args.get("email", "") or "").strip().lower()
    if not _is_valid_email(email):
        return jsonify({"verified": False, "message": "Invalid email."}), 400

    _purge_expired_email_magic()
    with _email_magic_lock:
        entry = _verified_emails.get(email)
        if not entry:
            return jsonify({"verified": False})

        try:
            expires = datetime.fromisoformat(entry.get("expires") or "")
        except Exception:
            expires = None

        if not expires or datetime.utcnow() > expires:
            _verified_emails.pop(email, None)
            _save_email_magic_to_disk(dict(_email_magic_tokens), dict(_verified_emails))
            return jsonify({"verified": False})

    return jsonify({"verified": True})


@app.route("/api/health", methods=["GET"])
def api_health():
    """Lightweight health/version endpoint for deployment verification."""
    return jsonify(
        {
            "ok": True,
            "utc": datetime.utcnow().isoformat() + "Z",
            "render_git_commit": os.getenv("RENDER_GIT_COMMIT", ""),
            "email_magic_enabled": True,
        }
    )


@app.route("/api/send-verification", methods=["POST"])
def send_verification_code():
    try:
        data = request.get_json(silent=True) or {}
        raw_phone = data.get("phone", "")
        phone = _normalize_phone_number(raw_phone)

        if not phone:
            return jsonify({"message": "Phone number is required"}), 400

        if not phone.startswith("+"):
            return jsonify({"message": "Please enter a valid mobile number (e.g. 07123 456789)."}), 400

        # Generate 4-digit code
        import random
        code = str(random.randint(1000, 9999))

        # Store code with expiry (5 minutes)
        _verification_codes[phone] = {
            "code": code,
            "expires": (datetime.utcnow() + timedelta(minutes=5)).isoformat()
        }

        # Send SMS
        message = f"Your Pay As You Mow verification code is: {code}. Valid for 5 minutes."
        print(f"[Verify] Sending code to {phone} (raw input: {raw_phone})")
        success, error_message = _send_sms_for_verification(phone, message)

        if success:
            return jsonify({"message": "Verification code sent successfully"})
        else:
            print(f"[Verify] SMS failed for {phone}: {error_message}")
            return jsonify({"message": error_message or "Failed to send verification code. Please check SMS settings."}), 500
    except Exception as exc:
        print(f"[Verify] Unexpected error: {exc}")
        return jsonify({"message": "An unexpected error occurred. Please try again."}), 500


@app.route("/api/sms-config-status", methods=["GET"])
def sms_config_status():
    """Public, non-secret diagnostic endpoint.

    Helps confirm whether the running backend instance has SMS configured.
    Does not expose secrets (API keys are masked).
    """
    telnyx_cfg = _telnyx_config_snapshot(include_secret=False)
    smsapi_cfg = _smsapi_config_snapshot(include_secret=False)

    provider = "none"
    if telnyx_cfg.get("has_config"):
        provider = "telnyx"
    elif smsapi_cfg.get("has_config"):
        provider = "smsapi"

    return jsonify(
        {
            "provider": provider,
            "telnyx": telnyx_cfg,
            "smsapi": smsapi_cfg,
        }
    )


@app.route("/api/verify-code", methods=["POST"])
def verify_code():
    data = request.get_json(silent=True) or {}
    phone = _normalize_phone_number(data.get("phone", ""))
    code = data.get("code", "").strip()
    
    if not phone or not code:
        return jsonify({"message": "Phone and code are required"}), 400
    
    stored = _verification_codes.get(phone)
    
    if not stored:
        return jsonify({"message": "No verification code found for this number"}), 400
    
    # Check if expired
    expires = datetime.fromisoformat(stored["expires"])
    if datetime.utcnow() > expires:
        del _verification_codes[phone]
        return jsonify({"message": "Verification code has expired"}), 400
    
    # Check if code matches
    if stored["code"] != code:
        return jsonify({"message": "Invalid verification code"}), 400
    
    # Code is valid, remove it
    del _verification_codes[phone]
    return jsonify({"message": "Phone number verified successfully"})


@app.route("/admin/chat/sessions", methods=["GET"])
@require_admin_auth
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
@require_admin_auth
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
@require_admin_auth
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
@require_admin_auth
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
    return render_template('index.html', reviews=load_reviews(), availability=load_availability(), seo=_seo_snapshot())


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
    service_type = data.get("service_type", "").strip()
    photos = data.get("photos", [])
    verified = data.get("verified", False)

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
        "service_type": service_type,
        "photos": photos if isinstance(photos, list) else [],
        "verified": verified,
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
        reviews = load_reviews()

        def _parse_review_time(value):
            if not isinstance(value, str) or not value.strip():
                return 0.0
            text = value.strip()
            # Support common ISO forms including trailing 'Z'.
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(text).timestamp()
            except ValueError:
                return 0.0

        # Show newest/most-recently-updated testimonials first.
        reviews.sort(
            key=lambda item: _parse_review_time((item or {}).get("updated_at") or (item or {}).get("created_at")),
            reverse=True,
        )

        response = jsonify({"reviews": reviews})
        # Prevent browser/proxy caching so the homepage always shows the latest admin edits.
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

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
@require_admin_auth
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
@require_admin_auth
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
@require_admin_auth
def admin_list_banned_visitors():
    with _banned_ips_lock:
        banned_list = [dict(details) for details in _banned_ips.values()]
    banned_list.sort(key=lambda item: item.get("banned_at") or "", reverse=True)
    return jsonify({"banned": banned_list})


@app.route("/admin/visitors/banned", methods=["POST"])
@require_admin_auth
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
@require_admin_auth
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
    # Check if authenticated
    if not session.get('admin_authenticated'):
        # Return login page instead
        return render_template_string('''
<!DOCTYPE html>
<html>
<head>
    <title>Admin Login - Pay As You Mow</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            background: linear-gradient(135deg, #2e7d32 0%, #4caf50 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .login-container {
            background: white;
            padding: 40px;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            width: 90%;
            max-width: 400px;
        }
        h1 {
            color: #2e7d32;
            margin-bottom: 10px;
            font-size: 24px;
        }
        .subtitle {
            color: #666;
            margin-bottom: 30px;
            font-size: 14px;
        }
        .error {
            background: #fee;
            color: #c33;
            padding: 12px;
            border-radius: 6px;
            margin-bottom: 20px;
            font-size: 14px;
            display: none;
        }
        .form-group {
            margin-bottom: 20px;
        }
        label {
            display: block;
            margin-bottom: 8px;
            color: #333;
            font-weight: 500;
        }
        input[type="password"] {
            width: 100%;
            padding: 12px;
            border: 2px solid #ddd;
            border-radius: 6px;
            font-size: 16px;
            transition: border-color 0.3s;
        }
        input[type="password"]:focus {
            outline: none;
            border-color: #4caf50;
        }
        button {
            width: 100%;
            padding: 14px;
            background: #4caf50;
            color: white;
            border: none;
            border-radius: 6px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.3s;
        }
        button:hover {
            background: #2e7d32;
        }
        button:disabled {
            background: #ccc;
            cursor: not-allowed;
        }
        .security-note {
            margin-top: 20px;
            padding: 12px;
            background: #f5f5f5;
            border-radius: 6px;
            font-size: 12px;
            color: #666;
        }
        .icon {
            font-size: 48px;
            text-align: center;
            margin-bottom: 20px;
        }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="icon">🔒</div>
        <h1>Admin Login</h1>
        <p class="subtitle">Pay As You Mow</p>
        <div class="error" id="error"></div>
        <form id="loginForm">
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password" autocomplete="current-password" required autofocus>
            </div>
            <button type="submit" id="submitBtn">Login</button>
        </form>
        <div class="security-note">
            🛡️ <strong>Security:</strong> This admin panel is protected with rate limiting, IP tracking, and session timeouts.
        </div>
    </div>
    <script>
        const form = document.getElementById('loginForm');
        const error = document.getElementById('error');
        const submitBtn = document.getElementById('submitBtn');
        
        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            error.style.display = 'none';
            submitBtn.disabled = true;
            submitBtn.textContent = 'Logging in...';
            
            try {
                const password = document.getElementById('password').value;
                const res = await fetch('/admin/login', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({password})
                });
                
                const data = await res.json();
                
                if (res.ok) {
                    window.location.href = '/admin';
                } else {
                    error.textContent = data.error || 'Login failed';
                    error.style.display = 'block';
                    document.getElementById('password').value = '';
                }
            } catch (err) {
                error.textContent = 'Connection error. Please try again.';
                error.style.display = 'block';
            } finally {
                submitBtn.disabled = false;
                submitBtn.textContent = 'Login';
            }
        });
    </script>
</body>
</html>
        ''')
    
    return app.send_static_file("admin.html")

@app.route("/admin/login", methods=["POST"])
def admin_login():
    global ADMIN_PASSWORD_HASH
    
    client_ip = request.remote_addr
    
    # Check if IP is locked
    if is_ip_locked(client_ip):
        return jsonify({"error": "Too many failed attempts. Please try again later."}), 429
    
    data = request.get_json()
    password = data.get('password', '')
    
    # If no password is set yet, set it now (first-time setup)
    if not ADMIN_PASSWORD_HASH:
        if len(password) < 8:
            return jsonify({"error": "Password must be at least 8 characters"}), 400
        
        ADMIN_PASSWORD_HASH = hash_password(password)

        _save_admin_password_hash_to_disk(ADMIN_PASSWORD_HASH)
        
        session['admin_authenticated'] = True
        session['last_activity'] = datetime.utcnow().timestamp()
        return jsonify({"success": True, "message": "Password set successfully"})
    
    # Verify password
    if verify_password(password, ADMIN_PASSWORD_HASH):
        session['admin_authenticated'] = True
        session['last_activity'] = datetime.utcnow().timestamp()
        return jsonify({"success": True})
    else:
        # Record failed attempt
        locked = record_failed_login(client_ip)
        if locked:
            return jsonify({"error": "Too many failed attempts. Account locked for 30 minutes."}), 429
        return jsonify({"error": "Invalid password"}), 401

@app.route("/admin/logout", methods=["POST"])
def admin_logout():
    session.clear()
    return jsonify({"success": True})


# Start the server-down watchdog (works with both direct run and gunicorn)
_start_watchdog_thread()

# Start Facebook Group post monitor
_start_facebook_poller()

if __name__ == '__main__':
    import sys
    # Port selection order:
    # 1) CLI arg (python app.py 5015)
    # 2) PORT env var (common in hosting providers)
    # 3) Default to 5015 (matches configure_tunnel_route.py)
    port = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv('PORT', '5015'))
    app.run(host='0.0.0.0', port=port, debug=True)

