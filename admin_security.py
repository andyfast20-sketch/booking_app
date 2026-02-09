"""Admin Security Configuration.

This module is safe to commit.

Secrets MUST be supplied via environment variables in production (e.g. Render):
- FLASK_SECRET_KEY (or SECRET_KEY)
- ADMIN_PASSWORD_HASH (optional if you set an admin password via the UI and persist it)
"""

import os


# Admin password hash (sha256). Prefer env var in production.
ADMIN_PASSWORD_HASH = str(os.getenv("ADMIN_PASSWORD_HASH", "") or "").strip()

# Flask session signing secret. Prefer env var in production.
SECRET_KEY = str(os.getenv("FLASK_SECRET_KEY", os.getenv("SECRET_KEY", "")) or "").strip()

# Rate limiting
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_DURATION_MINUTES = 30

# Optional: Whitelist IP addresses that can access admin
# Leave empty to allow from anywhere
ALLOWED_ADMIN_IPS = []
# Example: ALLOWED_ADMIN_IPS = ["192.168.1.100", "203.0.113.0"]

# Session timeout (minutes)
SESSION_TIMEOUT_MINUTES = 60
