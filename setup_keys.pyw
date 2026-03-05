"""
Secure API Key Setup Tool
=========================
Saves your API keys encrypted with Windows DPAPI and writes them to
the app's config files (which are .gitignored and never pushed to GitHub).

Double-click this file to run — no console window will appear.
"""

import ctypes
import ctypes.wintypes
import json
import os
import struct
import sys
import tkinter as tk
from tkinter import messagebox, ttk

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
VAULT_FILE = os.path.join(SCRIPT_DIR, "secrets.vault")
TELNYX_CONFIG = os.path.join(SCRIPT_DIR, "telnyx_config.json")
AUTOPILOT_CONFIG = os.path.join(SCRIPT_DIR, "autopilot.json")
GIST_BACKUP_TOKEN_FILE = os.path.join(SCRIPT_DIR, "gist_token.vault")

# ---------------------------------------------------------------------------
# Windows DPAPI encryption (CryptProtectData / CryptUnprotectData)
# Uses the logged-in Windows user's credentials — only YOU can decrypt.
# ---------------------------------------------------------------------------

class DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", ctypes.wintypes.DWORD),
                 ("pbData", ctypes.POINTER(ctypes.c_char))]

_crypt32 = ctypes.windll.crypt32
_kernel32 = ctypes.windll.kernel32


def _dpapi_encrypt(plaintext: str) -> bytes:
    """Encrypt a string using Windows DPAPI. Returns raw encrypted bytes."""
    data = plaintext.encode("utf-8")
    blob_in = DATA_BLOB(len(data), ctypes.create_string_buffer(data, len(data)))
    blob_out = DATA_BLOB()

    if not _crypt32.CryptProtectData(
        ctypes.byref(blob_in), None, None, None, None, 0,
        ctypes.byref(blob_out)
    ):
        raise OSError("DPAPI CryptProtectData failed")

    encrypted = ctypes.string_at(blob_out.pbData, blob_out.cbData)
    _kernel32.LocalFree(blob_out.pbData)
    return encrypted


def _dpapi_decrypt(encrypted: bytes) -> str:
    """Decrypt DPAPI-encrypted bytes back to a string."""
    blob_in = DATA_BLOB(len(encrypted),
                        ctypes.create_string_buffer(encrypted, len(encrypted)))
    blob_out = DATA_BLOB()

    if not _crypt32.CryptUnprotectData(
        ctypes.byref(blob_in), None, None, None, None, 0,
        ctypes.byref(blob_out)
    ):
        raise OSError("DPAPI CryptUnprotectData failed")

    plaintext = ctypes.string_at(blob_out.pbData, blob_out.cbData).decode("utf-8")
    _kernel32.LocalFree(blob_out.pbData)
    return plaintext


# ---------------------------------------------------------------------------
# Vault file helpers  (binary format: repeated [key_len][key][val_len][val])
# Each value is DPAPI-encrypted independently.
# ---------------------------------------------------------------------------

def _save_vault(secrets: dict):
    """Save secrets dict to the vault file, each value DPAPI-encrypted."""
    with open(VAULT_FILE, "wb") as f:
        for key, value in secrets.items():
            key_bytes = key.encode("utf-8")
            enc_value = _dpapi_encrypt(value) if value else b""
            f.write(struct.pack("<I", len(key_bytes)))
            f.write(key_bytes)
            f.write(struct.pack("<I", len(enc_value)))
            f.write(enc_value)


def _load_vault() -> dict:
    """Load and decrypt all secrets from the vault file."""
    if not os.path.exists(VAULT_FILE):
        return {}
    secrets = {}
    try:
        with open(VAULT_FILE, "rb") as f:
            data = f.read()
        pos = 0
        while pos < len(data):
            key_len = struct.unpack_from("<I", data, pos)[0]
            pos += 4
            key = data[pos:pos + key_len].decode("utf-8")
            pos += key_len
            val_len = struct.unpack_from("<I", data, pos)[0]
            pos += 4
            enc_val = data[pos:pos + val_len]
            pos += val_len
            secrets[key] = _dpapi_decrypt(enc_val) if enc_val else ""
    except Exception:
        pass
    return secrets


# ---------------------------------------------------------------------------
# Config file writers
# ---------------------------------------------------------------------------

def _read_json(path, default=None):
    if default is None:
        default = {}
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _update_telnyx(api_key: str):
    """Write the Telnyx API key to telnyx_config.json (gitignored)."""
    cfg = _read_json(TELNYX_CONFIG, {
        "api_key": "",
        "from_number": "",
        "messaging_profile_id": "",
        "verification_method": "call",
        "voice_connection_id": ""
    })
    cfg["api_key"] = api_key
    _write_json(TELNYX_CONFIG, cfg)


def _update_deepseek(api_key: str):
    """Write the DeepSeek API key to autopilot.json (gitignored)."""
    cfg = _read_json(AUTOPILOT_CONFIG, {
        "enabled": True,
        "provider": "deepseek",
        "model": "deepseek-chat",
        "temperature": 0.3,
        "api_key": "",
        "api_keys": []
    })
    cfg["api_key"] = api_key

    # Also update api_keys list
    import uuid, datetime
    existing_keys = cfg.get("api_keys", [])
    if not isinstance(existing_keys, list):
        existing_keys = []

    # Replace existing or add new
    if existing_keys:
        existing_keys[0]["value"] = api_key
    else:
        existing_keys.append({
            "id": str(uuid.uuid4()),
            "value": api_key,
            "created_at": datetime.datetime.utcnow().isoformat()
        })
    cfg["api_keys"] = existing_keys
    _write_json(AUTOPILOT_CONFIG, cfg)


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------

class KeySetupApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Pay As You Mow — Secure Key Setup")
        self.root.geometry("620x520")
        self.root.resizable(False, False)
        self.root.configure(bg="#1a1a2e")

        # Try to set icon
        try:
            self.root.iconbitmap(default="")
        except Exception:
            pass

        # Style
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Title.TLabel", font=("Segoe UI", 16, "bold"),
                        foreground="#00d4aa", background="#1a1a2e")
        style.configure("Sub.TLabel", font=("Segoe UI", 9),
                        foreground="#aaaaaa", background="#1a1a2e")
        style.configure("Field.TLabel", font=("Segoe UI", 10, "bold"),
                        foreground="#ffffff", background="#1a1a2e")
        style.configure("Status.TLabel", font=("Segoe UI", 9),
                        foreground="#00d4aa", background="#1a1a2e")
        style.configure("Warn.TLabel", font=("Segoe UI", 9),
                        foreground="#ff6b6b", background="#1a1a2e")
        style.configure("Green.TButton", font=("Segoe UI", 11, "bold"),
                        foreground="#ffffff", background="#00d4aa")

        # Main frame
        main = tk.Frame(self.root, bg="#1a1a2e", padx=30, pady=20)
        main.pack(fill="both", expand=True)

        # Title
        ttk.Label(main, text="\U0001f512  Secure API Key Setup",
                  style="Title.TLabel").pack(anchor="w")
        ttk.Label(main,
                  text="Keys are encrypted with Windows DPAPI and saved to gitignored config files.\n"
                       "They NEVER appear in your GitHub repository.",
                  style="Sub.TLabel").pack(anchor="w", pady=(2, 15))

        # Load existing
        vault = _load_vault()

        # --- Telnyx API Key ---
        ttk.Label(main, text="Telnyx API Key", style="Field.TLabel").pack(anchor="w")
        ttk.Label(main, text="Starts with KEY… — get it from portal.telnyx.com → API Keys",
                  style="Sub.TLabel").pack(anchor="w")
        self.telnyx_var = tk.StringVar(value=vault.get("TELNYX_API_KEY", ""))
        self.telnyx_entry = tk.Entry(main, textvariable=self.telnyx_var,
                                     font=("Consolas", 11), show="•",
                                     bg="#16213e", fg="#00d4aa",
                                     insertbackground="#00d4aa",
                                     relief="flat", bd=5, width=60)
        self.telnyx_entry.pack(anchor="w", pady=(3, 2), ipady=4)
        self._add_show_toggle(main, self.telnyx_entry)

        # Spacer
        tk.Frame(main, bg="#1a1a2e", height=10).pack()

        # --- DeepSeek API Key ---
        ttk.Label(main, text="DeepSeek API Key", style="Field.TLabel").pack(anchor="w")
        ttk.Label(main, text="Starts with sk-… — get it from platform.deepseek.com → API Keys",
                  style="Sub.TLabel").pack(anchor="w")
        self.deepseek_var = tk.StringVar(value=vault.get("DEEPSEEK_API_KEY", ""))
        self.deepseek_entry = tk.Entry(main, textvariable=self.deepseek_var,
                                       font=("Consolas", 11), show="•",
                                       bg="#16213e", fg="#00d4aa",
                                       insertbackground="#00d4aa",
                                       relief="flat", bd=5, width=60)
        self.deepseek_entry.pack(anchor="w", pady=(3, 2), ipady=4)
        self._add_show_toggle(main, self.deepseek_entry)

        # Spacer
        tk.Frame(main, bg="#1a1a2e", height=10).pack()

        # --- GIST Token (optional) ---
        ttk.Label(main, text="GitHub Gist Token (for cloud backup)",
                  style="Field.TLabel").pack(anchor="w")
        ttk.Label(main, text="Optional — starts with ghp_… — get it from github.com/settings/tokens",
                  style="Sub.TLabel").pack(anchor="w")
        self.gist_var = tk.StringVar(value=vault.get("GIST_TOKEN", ""))
        self.gist_entry = tk.Entry(main, textvariable=self.gist_var,
                                   font=("Consolas", 11), show="•",
                                   bg="#16213e", fg="#00d4aa",
                                   insertbackground="#00d4aa",
                                   relief="flat", bd=5, width=60)
        self.gist_entry.pack(anchor="w", pady=(3, 2), ipady=4)
        self._add_show_toggle(main, self.gist_entry)

        # Spacer
        tk.Frame(main, bg="#1a1a2e", height=15).pack()

        # Buttons
        btn_frame = tk.Frame(main, bg="#1a1a2e")
        btn_frame.pack(fill="x")

        save_btn = tk.Button(btn_frame, text="  \U0001f512  Save & Encrypt Keys  ",
                             font=("Segoe UI", 12, "bold"),
                             bg="#00d4aa", fg="#1a1a2e", activebackground="#00b894",
                             relief="flat", bd=0, cursor="hand2",
                             command=self._save)
        save_btn.pack(side="left")

        render_btn = tk.Button(btn_frame, text="  \U0001f4cb  Copy Render Env Vars  ",
                               font=("Segoe UI", 10),
                               bg="#3d5a80", fg="#ffffff", activebackground="#2d4a70",
                               relief="flat", bd=0, cursor="hand2",
                               command=self._copy_render_vars)
        render_btn.pack(side="left", padx=(15, 0))

        # Status
        self.status_var = tk.StringVar(value="")
        self.status_label = ttk.Label(main, textvariable=self.status_var,
                                      style="Status.TLabel")
        self.status_label.pack(anchor="w", pady=(10, 0))

    def _add_show_toggle(self, parent, entry):
        """Add a show/hide toggle for a password entry."""
        var = tk.BooleanVar(value=False)

        def toggle():
            entry.config(show="" if var.get() else "•")

        cb = tk.Checkbutton(parent, text="Show", variable=var, command=toggle,
                            bg="#1a1a2e", fg="#888888", selectcolor="#16213e",
                            activebackground="#1a1a2e", activeforeground="#aaaaaa",
                            font=("Segoe UI", 8))
        cb.pack(anchor="w")

    def _save(self):
        telnyx_key = self.telnyx_var.get().strip()
        deepseek_key = self.deepseek_var.get().strip()
        gist_token = self.gist_var.get().strip()

        # Validate
        errors = []
        if telnyx_key and not telnyx_key.startswith("KEY"):
            errors.append("Telnyx API key should start with 'KEY'")
        if deepseek_key and not deepseek_key.startswith("sk-"):
            errors.append("DeepSeek API key should start with 'sk-'")
        if gist_token and not (gist_token.startswith("ghp_") or
                               gist_token.startswith("gho_") or
                               gist_token.startswith("github_pat_")):
            errors.append("GitHub token should start with 'ghp_', 'gho_', or 'github_pat_'")

        if errors:
            messagebox.showwarning("Validation", "\n".join(errors))
            return

        if not telnyx_key and not deepseek_key:
            messagebox.showinfo("Nothing to save", "Please enter at least one API key.")
            return

        try:
            # 1. Save to encrypted vault (DPAPI)
            vault = {}
            if telnyx_key:
                vault["TELNYX_API_KEY"] = telnyx_key
            if deepseek_key:
                vault["DEEPSEEK_API_KEY"] = deepseek_key
            if gist_token:
                vault["GIST_TOKEN"] = gist_token
            _save_vault(vault)

            # 2. Write to config files (gitignored)
            if telnyx_key:
                _update_telnyx(telnyx_key)
            if deepseek_key:
                _update_deepseek(deepseek_key)

            saved = []
            if telnyx_key:
                saved.append("Telnyx")
            if deepseek_key:
                saved.append("DeepSeek")
            if gist_token:
                saved.append("Gist Token")

            self.status_var.set(f"\u2705  {', '.join(saved)} key(s) encrypted & saved!")
            self.status_label.configure(style="Status.TLabel")

            messagebox.showinfo(
                "Keys Saved",
                f"{', '.join(saved)} key(s) saved successfully!\n\n"
                f"\u2022 Encrypted vault: secrets.vault\n"
                f"\u2022 Config files updated (gitignored)\n\n"
                f"For Render deployment, click 'Copy Render Env Vars'\n"
                f"and paste them into Render → Environment."
            )

        except Exception as e:
            self.status_var.set(f"\u274c Error: {e}")
            self.status_label.configure(style="Warn.TLabel")
            messagebox.showerror("Error", f"Failed to save keys:\n{e}")

    def _copy_render_vars(self):
        """Copy env var settings to clipboard for Render."""
        telnyx_key = self.telnyx_var.get().strip()
        deepseek_key = self.deepseek_var.get().strip()
        gist_token = self.gist_var.get().strip()

        lines = []
        if telnyx_key:
            lines.append(f"TELNYX_API_KEY={telnyx_key}")
        if deepseek_key:
            lines.append(f"DEEPSEEK_API_KEY={deepseek_key}")
        if gist_token:
            lines.append(f"GIST_TOKEN={gist_token}")

        if not lines:
            messagebox.showinfo("No keys", "Enter your keys first, then copy.")
            return

        text = "\n".join(lines)
        self.root.clipboard_clear()
        self.root.clipboard_append(text)

        self.status_var.set("\U0001f4cb  Copied to clipboard! Paste into Render → Environment")
        messagebox.showinfo(
            "Copied to Clipboard",
            "Env vars copied!\n\n"
            "Go to: dashboard.render.com → your service → Environment\n"
            "Add each variable as a key-value pair.\n\n"
            "Variables copied:\n" + "\n".join(f"  {l.split('=')[0]}" for l in lines)
        )

    def run(self):
        self.root.mainloop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app = KeySetupApp()
    app.run()
