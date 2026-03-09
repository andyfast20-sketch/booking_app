"""
Secure Admin Password Setup
===========================
Run this script locally to set (or reset) the admin password.

Usage:
    python set_admin_password.py

The script:
  - Prompts for a new password WITHOUT echoing it to the terminal
  - Hashes it with bcrypt (12 rounds)
  - Writes ONLY the hash to admin_auth.json  (never the plaintext)
  - admin_auth.json is .gitignored and excluded from Gist backup

On Render / production:
  - Set the ADMIN_PASSWORD_HASH env var to the bcrypt hash printed by this script
  - The app will read it from the environment and admin_auth.json is not needed
"""

import getpass
import json
import os
import sys

try:
    import bcrypt
except ImportError:
    sys.exit("ERROR: bcrypt is not installed. Run:  pip install bcrypt")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ADMIN_AUTH_FILE = os.path.join(SCRIPT_DIR, "admin_auth.json")


def main():
    print("=== Admin Password Setup ===")
    print("Password will NOT be shown as you type.\n")

    while True:
        password = getpass.getpass("Enter new admin password: ")
        if len(password) < 10:
            print("  [!] Password must be at least 10 characters. Try again.\n")
            continue
        confirm = getpass.getpass("Confirm password:         ")
        if password != confirm:
            print("  [!] Passwords do not match. Try again.\n")
            continue
        break

    print("\nHashing password (bcrypt 12 rounds) ...")
    hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")

    # Write hash to local file
    with open(ADMIN_AUTH_FILE, "w", encoding="utf-8") as fh:
        json.dump({"admin_password_hash": hashed}, fh)

    print(f"\n[OK] Hash written to:  {ADMIN_AUTH_FILE}")
    print("\nFor Render / production, set this environment variable:")
    print(f"\n  ADMIN_PASSWORD_HASH={hashed}\n")
    print("Keep this value private — do NOT commit it to git or share it.")


if __name__ == "__main__":
    main()
