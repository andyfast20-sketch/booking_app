# Admin Panel Security Guide

## 🔒 Security Features

Your admin panel is now protected with multiple layers of security:

### 1. **Password Authentication**
- Strong password hashing using SHA-256
- First-time setup: password is set on your first login
- Passwords are never stored in plain text

### 2. **Rate Limiting**
- Maximum 5 failed login attempts allowed
- After 5 failures, IP is locked for 30 minutes
- Protects against brute-force attacks

### 3. **Session Management**
- Secure session cookies (HTTPOnly, Secure, SameSite)
- 60-minute session timeout
- Automatic logout on inactivity

### 4. **IP Tracking**
- All login attempts are tracked by IP address
- Failed attempts are logged
- Optional IP whitelist available

---

## 🚀 First-Time Setup

1. **Start your Flask server** (via the Booking App Manager or manually)

2. **Visit** `https://payasyoumow.org/admin`

3. **Enter a strong password** - This will be your admin password going forward
   - Use at least 12 characters
   - Mix uppercase, lowercase, numbers, and symbols
   - Example: `MyGarden2024!Secure#`

4. **Login** - You'll be taken to the admin dashboard

---

## 🔑 How to Reset Your Password

If you forget your admin password or need to change it:

1. Stop your Flask server

2. Open `admin_security.py` in a text editor

3. Change the `ADMIN_PASSWORD_HASH` to `None`:
   ```python
   ADMIN_PASSWORD_HASH = None
   ```

4. Save the file

5. Restart your Flask server

6. Visit `/admin` and enter a new password (it will be set as your new password)

---

## 🛡️ IP Whitelist (Optional)

To restrict admin access to specific IP addresses only:

1. Open `admin_security.py`

2. Add your trusted IP addresses to the `ALLOWED_ADMIN_IPS` list:
   ```python
   ALLOWED_ADMIN_IPS = [
       '192.168.1.100',  # Your home computer
       '203.0.113.45',   # Your office
   ]
   ```

3. Save and restart the server

**Note:** If the list is empty `[]`, all IPs are allowed (default)

---

## 🔐 Security Settings

All security settings are in `admin_security.py`:

| Setting | Default | Description |
|---------|---------|-------------|
| `MAX_LOGIN_ATTEMPTS` | 5 | Failed attempts before lockout |
| `LOCKOUT_DURATION_MINUTES` | 30 | How long IP is locked |
| `SESSION_TIMEOUT_MINUTES` | 60 | Auto-logout after inactivity |
| `ALLOWED_ADMIN_IPS` | `[]` | Whitelist (empty = all allowed) |

---

## ⚠️ What if I Get Locked Out?

If you've exceeded login attempts and are locked out:

**Option 1 - Wait it out:**
- Wait 30 minutes for the lockout to expire

**Option 2 - Clear the lockout manually:**
1. Stop the Flask server
2. Delete the `admin_failed_logins.json` file (if it exists)
3. Restart the server

---

## 🔍 Monitoring Failed Login Attempts

Failed login attempts are stored in `admin_failed_logins.json`

You can check this file to see:
- Which IPs have tried to login
- How many failed attempts from each IP
- When the lockout expires

---

## ✅ Best Practices

1. **Use a strong, unique password** - Don't reuse passwords from other sites
2. **Keep `admin_security.py` secure** - Never share this file
3. **Consider IP whitelisting** - If you only access admin from specific locations
4. **Monitor failed logins** - Check `admin_failed_logins.json` regularly
5. **Logout when done** - Click the logout button in the top-right corner
6. **Keep your server updated** - Regular updates improve security

---

## 📞 Support

If you have security concerns or questions:
- Review the security code in `admin_auth.py`
- Check Flask session documentation
- Consider additional security measures like 2FA (future enhancement)

---

**Last Updated:** January 2025
**Security Level:** High - Multi-layered protection with password hashing, rate limiting, and session management
