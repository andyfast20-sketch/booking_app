import os
import secrets
import hashlib
import time
from datetime import datetime, timedelta
from functools import wraps
from flask import session, request, jsonify, redirect

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

def hash_password(password):
    """Create a secure hash of the password"""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

def verify_password(password, password_hash):
    """Verify a password against its hash"""
    return hash_password(password) == password_hash

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
