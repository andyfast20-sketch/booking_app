import os
import secrets
import hashlib
import time
from datetime import datetime, timedelta
from functools import wraps
from flask import session, request, jsonify, redirect

try:
    import bcrypt as _bcrypt
    _BCRYPT_AVAILABLE = True
except ImportError:
    _BCRYPT_AVAILABLE = False

# Import or create security config
try:
    from admin_security import (
        ADMIN_PASSWORD_HASH, SECRET_KEY, MAX_LOGIN_ATTEMPTS,
        LOCKOUT_DURATION_MINUTES, ALLOWED_ADMIN_IPS, SESSION_TIMEOUT_MINUTES
    )
except ImportError:
    ADMIN_PASSWORD_HASH = ""
    SECRET_KEY = ""
    MAX_LOGIN_ATTEMPTS = 5
    LOCKOUT_DURATION_MINUTES = 30
    ALLOWED_ADMIN_IPS = []
    SESSION_TIMEOUT_MINUTES = 60

# In-memory storage for failed login attempts
_login_attempts = {}  # {ip: [timestamp1, timestamp2, ...]}
_locked_ips = {}  # {ip: lockout_until_timestamp}

def hash_password(password: str) -> str:
    """Create a bcrypt hash of the password (bcrypt preferred, SHA-256 fallback)."""
    if _BCRYPT_AVAILABLE:
        return _bcrypt.hashpw(password.encode("utf-8"), _bcrypt.gensalt(rounds=12)).decode("utf-8")
    # Fallback — should not reach here in production
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against a bcrypt *or* legacy SHA-256 hash."""
    if not password or not password_hash:
        return False
    # bcrypt hashes start with $2b$ (or $2a$ / $2y$)
    if _BCRYPT_AVAILABLE and password_hash.startswith(("$2b$", "$2a$", "$2y$")):
        try:
            return _bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
        except Exception:
            return False
    # Legacy SHA-256 path (temporary migration support)
    return hashlib.sha256(password.encode("utf-8")).hexdigest() == password_hash

def is_ip_locked(ip):
    """Check if an IP is currently locked out"""
    if ip in _locked_ips:
        if time.time() < _locked_ips[ip]:
            return True
        else:
            # Lockout expired
            del _locked_ips[ip]
            if ip in _login_attempts:
                del _login_attempts[ip]
    return False

def record_failed_login(ip):
    """Record a failed login attempt"""
    now = time.time()
    
    # Clean old attempts (older than lockout duration)
    if ip in _login_attempts:
        _login_attempts[ip] = [t for t in _login_attempts[ip] 
                               if now - t < LOCKOUT_DURATION_MINUTES * 60]
    else:
        _login_attempts[ip] = []
    
    _login_attempts[ip].append(now)
    
    # Check if should be locked
    if len(_login_attempts[ip]) >= MAX_LOGIN_ATTEMPTS:
        _locked_ips[ip] = now + (LOCKOUT_DURATION_MINUTES * 60)
        return True
    return False

def check_ip_whitelist(ip):
    """Check if IP is in whitelist (if whitelist is enabled)"""
    if not ALLOWED_ADMIN_IPS:
        return True  # No whitelist, allow all
    return ip in ALLOWED_ADMIN_IPS

def require_admin_auth(f):
    """Decorator to protect admin routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        client_ip = request.remote_addr
        
        # Check IP whitelist
        if not check_ip_whitelist(client_ip):
            return jsonify({"error": "Access denied from this IP address"}), 403
        
        # Check if IP is locked out
        if is_ip_locked(client_ip):
            remaining = int((_locked_ips[client_ip] - time.time()) / 60)
            return jsonify({
                "error": f"Too many failed attempts. Try again in {remaining} minutes."
            }), 429
        
        # Check session authentication
        if not session.get('admin_authenticated'):
            return jsonify({"error": "Authentication required"}), 401
        
        # Check session timeout
        last_activity = session.get('last_activity')
        if last_activity:
            if time.time() - last_activity > SESSION_TIMEOUT_MINUTES * 60:
                session.clear()
                return jsonify({"error": "Session expired. Please login again."}), 401
        
        # Update last activity
        session['last_activity'] = time.time()
        
        return f(*args, **kwargs)
    return decorated_function

def init_admin_security(app):
    """Initialize admin security with the Flask app"""
    global SECRET_KEY, ADMIN_PASSWORD_HASH
    
    # Generate secret key if not set.
    # In production, provide FLASK_SECRET_KEY/SECRET_KEY via environment variables.
    if not SECRET_KEY:
        SECRET_KEY = secrets.token_hex(32)
    
    app.secret_key = SECRET_KEY
    
    # Set secure session cookie settings
    app.config.update(
        SESSION_COOKIE_SECURE=True,  # Only send over HTTPS
        SESSION_COOKIE_HTTPONLY=True,  # Prevent JavaScript access
        SESSION_COOKIE_SAMESITE='Lax',  # CSRF protection
        PERMANENT_SESSION_LIFETIME=timedelta(minutes=SESSION_TIMEOUT_MINUTES)
    )
    
    return ADMIN_PASSWORD_HASH
