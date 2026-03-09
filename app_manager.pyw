"""
Pay As You Mow — App Manager & Watchdog
========================================
Monitor and auto-recover the Flask server and Cloudflare tunnel.

Features
--------
• Live status cards for Server and Tunnel (green / red indicator)
• Manual Start / Stop buttons for each
• Watchdog thread that fires at RANDOM intervals so it doesn't look
  like a cron job (configurable min / max minutes in the UI)
• If the server OR tunnel is found to be down during a watchdog check,
  BOTH are restarted so they come up in a consistent state
• Scrollable activity log with timestamps

Double-click this .pyw file — no console window appears.
"""

import os
import sys
import random
import subprocess
import threading
import time
import tkinter as tk
from tkinter import ttk, font as tkfont
from datetime import datetime

# ── Standard library HTTP (no third-party deps needed) ────────────────────
import urllib.request
import urllib.error

# ── Paths ──────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
APP_SCRIPT  = os.path.join(SCRIPT_DIR, "app.py")
PYTHON_EXE  = sys.executable          # same interpreter that launched this file

# ── Server settings ────────────────────────────────────────────────────────
SERVER_PORT       = 5015
SERVER_HEALTH_URL = f"http://127.0.0.1:{SERVER_PORT}/health"

# ── Cloudflare Tunnel ──────────────────────────────────────────────────────
TUNNEL_ID = "81e73a38-7bab-4838-8802-a358d34ae8ae"

# ── Watchdog defaults (seconds) ────────────────────────────────────────────
DEFAULT_MIN_SECS = 180   # 3 min
DEFAULT_MAX_SECS = 540   # 9 min

# ── Colours ────────────────────────────────────────────────────────────────
BG          = "#1a1a2e"
PANEL       = "#16213e"
BORDER      = "#0f3460"
ACCENT      = "#00d4aa"
TEXT        = "#ffffff"
SUBTEXT     = "#aaaaaa"
RED         = "#ff6b6b"
AMBER       = "#f0a500"
GREEN       = "#00d4aa"

# ──────────────────────────────────────────────────────────────────────────
# Process helpers (Windows)
# ──────────────────────────────────────────────────────────────────────────

def _process_running(name_fragment: str) -> bool:
    """Return True if any process whose image name contains *name_fragment* is running."""
    try:
        out = subprocess.check_output(
            ["tasklist", "/FO", "CSV", "/NH"],
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW,
        ).decode(errors="replace")
        return name_fragment.lower() in out.lower()
    except Exception:
        return False


def _server_responding() -> bool:
    """Return True if the Flask /health endpoint answers 200."""
    try:
        with urllib.request.urlopen(SERVER_HEALTH_URL, timeout=8) as r:
            return r.getcode() == 200
    except Exception:
        return False


def _tunnel_running() -> bool:
    """Return True if a cloudflared process is active."""
    return _process_running("cloudflared")


def _start_server() -> bool:
    """Launch app.py in a detached process. Returns True if spawned ok."""
    try:
        subprocess.Popen(
            [PYTHON_EXE, APP_SCRIPT],
            cwd=SCRIPT_DIR,
            creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except Exception:
        return False


def _start_tunnel() -> bool:
    """Launch cloudflared tunnel run in a detached process."""
    try:
        subprocess.Popen(
            ["cloudflared", "tunnel", "run", TUNNEL_ID],
            cwd=SCRIPT_DIR,
            creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except FileNotFoundError:
        # cloudflared not on PATH — try common install location
        for candidate in [
            r"C:\Program Files\cloudflared\cloudflared.exe",
            r"C:\cloudflared\cloudflared.exe",
            os.path.join(SCRIPT_DIR, "cloudflared.exe"),
        ]:
            if os.path.exists(candidate):
                subprocess.Popen(
                    [candidate, "tunnel", "run", TUNNEL_ID],
                    cwd=SCRIPT_DIR,
                    creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                return True
        return False
    except Exception:
        return False


def _stop_process(name_fragment: str):
    """Kill all processes whose image name contains *name_fragment*."""
    try:
        subprocess.call(
            ["taskkill", "/F", "/IM", f"*{name_fragment}*"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
    except Exception:
        pass


def _stop_server():
    _stop_process("python.exe")   # kills the Flask process


def _stop_tunnel():
    _stop_process("cloudflared.exe")


# ──────────────────────────────────────────────────────────────────────────
# Main GUI application
# ──────────────────────────────────────────────────────────────────────────

class AppManagerApp:

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Pay As You Mow — App Manager")
        self.root.geometry("640x620")
        self.root.resizable(False, False)
        self.root.configure(bg=BG)

        # ── Shared state ──
        self._watchdog_enabled    = tk.BooleanVar(value=False)
        self._watchdog_min_var    = tk.StringVar(value="3")
        self._watchdog_max_var    = tk.StringVar(value="9")
        self._server_status_text  = tk.StringVar(value="Unknown")
        self._tunnel_status_text  = tk.StringVar(value="Unknown")
        self._next_check_var      = tk.StringVar(value="—")
        self._last_check_var      = tk.StringVar(value="—")

        self._server_dot: tk.Label | None = None
        self._tunnel_dot: tk.Label | None = None

        self._watchdog_thread: threading.Thread | None = None
        self._watchdog_stop   = threading.Event()
        self._log_lock        = threading.Lock()

        self._build_ui()

        # Initial status check (non-blocking)
        threading.Thread(target=self._initial_check, daemon=True).start()

        # Poll UI updates every 500 ms
        self.root.after(500, self._poll_ui)

    # ── UI construction ───────────────────────────────────────────────────

    def _build_ui(self):
        main = tk.Frame(self.root, bg=BG, padx=24, pady=16)
        main.pack(fill="both", expand=True)

        # Title
        tk.Label(main, text="⚙  Pay As You Mow — App Manager",
                 bg=BG, fg=ACCENT, font=("Segoe UI", 15, "bold")).pack(anchor="w")
        tk.Label(main, text="Monitor & auto-recover the server and Cloudflare tunnel.",
                 bg=BG, fg=SUBTEXT, font=("Segoe UI", 9)).pack(anchor="w", pady=(2, 14))

        # ── Status cards row ──────────────────────────────────────────────
        cards = tk.Frame(main, bg=BG)
        cards.pack(fill="x", pady=(0, 12))
        cards.columnconfigure(0, weight=1)
        cards.columnconfigure(1, weight=1)

        # Server card
        srv_frame = self._make_card(cards, "🖥  Flask Server", col=0)
        self._server_dot = tk.Label(srv_frame, text="●", bg=PANEL, fg=AMBER,
                                    font=("Segoe UI", 22))
        self._server_dot.pack(pady=(10, 2))
        tk.Label(srv_frame, textvariable=self._server_status_text,
                 bg=PANEL, fg=TEXT, font=("Segoe UI", 10)).pack()
        tk.Label(srv_frame, text=f"Port {SERVER_PORT}", bg=PANEL, fg=SUBTEXT,
                 font=("Segoe UI", 8)).pack(pady=(2, 8))

        btn_row_srv = tk.Frame(srv_frame, bg=PANEL)
        btn_row_srv.pack(pady=(0, 10))
        self._make_btn(btn_row_srv, "▶ Start", GREEN, lambda: self._manually_start("server")).pack(side="left", padx=4)
        self._make_btn(btn_row_srv, "■ Stop",  RED,   lambda: self._manually_stop("server")).pack(side="left", padx=4)

        # Tunnel card
        tun_frame = self._make_card(cards, "🌐  Cloudflare Tunnel", col=1)
        self._tunnel_dot = tk.Label(tun_frame, text="●", bg=PANEL, fg=AMBER,
                                    font=("Segoe UI", 22))
        self._tunnel_dot.pack(pady=(10, 2))
        tk.Label(tun_frame, textvariable=self._tunnel_status_text,
                 bg=PANEL, fg=TEXT, font=("Segoe UI", 10)).pack()
        tk.Label(tun_frame, text=TUNNEL_ID[:18] + "…", bg=PANEL, fg=SUBTEXT,
                 font=("Segoe UI", 8)).pack(pady=(2, 8))

        btn_row_tun = tk.Frame(tun_frame, bg=PANEL)
        btn_row_tun.pack(pady=(0, 10))
        self._make_btn(btn_row_tun, "▶ Start", GREEN, lambda: self._manually_start("tunnel")).pack(side="left", padx=4)
        self._make_btn(btn_row_tun, "■ Stop",  RED,   lambda: self._manually_stop("tunnel")).pack(side="left", padx=4)

        # Refresh button
        self._make_btn(main, "🔄  Refresh Status Now", ACCENT,
                       self._refresh_status).pack(anchor="w", pady=(0, 14))

        # ── Watchdog section ──────────────────────────────────────────────
        wd_outer = tk.Frame(main, bg=PANEL, bd=1, relief="flat",
                            highlightbackground=BORDER, highlightthickness=1)
        wd_outer.pack(fill="x", pady=(0, 10))
        wd_inner = tk.Frame(wd_outer, bg=PANEL, padx=14, pady=10)
        wd_inner.pack(fill="x")

        hdr = tk.Frame(wd_inner, bg=PANEL)
        hdr.pack(fill="x")
        tk.Label(hdr, text="🐕  Auto-Watchdog", bg=PANEL, fg=ACCENT,
                 font=("Segoe UI", 11, "bold")).pack(side="left")

        wd_toggle = tk.Checkbutton(
            hdr, text="Enable", variable=self._watchdog_enabled,
            bg=PANEL, fg=TEXT, selectcolor=BORDER, activebackground=PANEL,
            activeforeground=ACCENT, font=("Segoe UI", 9),
            command=self._on_watchdog_toggle,
        )
        wd_toggle.pack(side="right")

        tk.Label(wd_inner,
                 text="Fires at random intervals and auto-restarts both services if either is down.",
                 bg=PANEL, fg=SUBTEXT, font=("Segoe UI", 8)).pack(anchor="w", pady=(2, 8))

        # Interval row
        ivl = tk.Frame(wd_inner, bg=PANEL)
        ivl.pack(anchor="w")
        tk.Label(ivl, text="Check every", bg=PANEL, fg=TEXT, font=("Segoe UI", 9)).pack(side="left")
        tk.Entry(ivl, textvariable=self._watchdog_min_var, width=4,
                 bg=BORDER, fg=TEXT, insertbackground=ACCENT,
                 relief="flat").pack(side="left", padx=(6, 2))
        tk.Label(ivl, text="to", bg=PANEL, fg=TEXT, font=("Segoe UI", 9)).pack(side="left", padx=2)
        tk.Entry(ivl, textvariable=self._watchdog_max_var, width=4,
                 bg=BORDER, fg=TEXT, insertbackground=ACCENT,
                 relief="flat").pack(side="left", padx=(2, 6))
        tk.Label(ivl, text="minutes (random)", bg=PANEL, fg=SUBTEXT, font=("Segoe UI", 9)).pack(side="left")

        # Timing info
        timing = tk.Frame(wd_inner, bg=PANEL)
        timing.pack(anchor="w", pady=(8, 0))
        tk.Label(timing, text="Last check:", bg=PANEL, fg=SUBTEXT, font=("Segoe UI", 8)).pack(side="left")
        tk.Label(timing, textvariable=self._last_check_var, bg=PANEL, fg=TEXT,
                 font=("Segoe UI", 8, "bold")).pack(side="left", padx=(4, 18))
        tk.Label(timing, text="Next check:", bg=PANEL, fg=SUBTEXT, font=("Segoe UI", 8)).pack(side="left")
        tk.Label(timing, textvariable=self._next_check_var, bg=PANEL, fg=ACCENT,
                 font=("Segoe UI", 8, "bold")).pack(side="left", padx=4)

        # ── Activity log ──────────────────────────────────────────────────
        tk.Label(main, text="Activity Log", bg=BG, fg=SUBTEXT,
                 font=("Segoe UI", 9)).pack(anchor="w", pady=(6, 2))

        self._log = tk.Text(
            main, height=10, bg="#0d1117", fg="#bfefbc",
            font=("Consolas", 8), relief="flat", wrap="word",
            state="disabled", insertbackground=ACCENT,
        )
        self._log.pack(fill="x")
        scrollbar = ttk.Scrollbar(main, command=self._log.yview)
        self._log["yscrollcommand"] = scrollbar.set
        scrollbar.pack(side="right", fill="y")

        self._log_line("App Manager started.")

    # ── Helpers ───────────────────────────────────────────────────────────

    def _make_card(self, parent, title, col):
        outer = tk.Frame(parent, bg=PANEL, bd=0,
                         highlightbackground=BORDER, highlightthickness=1)
        outer.grid(row=0, column=col, padx=(0 if col > 0 else 0, 6 if col == 0 else 0), sticky="nsew")
        tk.Label(outer, text=title, bg=PANEL, fg=ACCENT,
                 font=("Segoe UI", 10, "bold")).pack(pady=(10, 0))
        return outer

    def _make_btn(self, parent, text, colour, cmd):
        return tk.Button(
            parent, text=text, command=cmd,
            bg=colour, fg="#1a1a2e" if colour == ACCENT else "#ffffff",
            activebackground=colour, font=("Segoe UI", 9, "bold"),
            relief="flat", bd=0, cursor="hand2", padx=10, pady=4
        )

    def _log_line(self, msg: str):
        ts  = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}]  {msg}\n"
        with self._log_lock:
            self._log.config(state="normal")
            self._log.insert("end", line)
            self._log.see("end")
            self._log.config(state="disabled")

    # ── Status update (thread-safe via after) ─────────────────────────────

    def _update_card(self, which: str, ok: bool):
        colour = GREEN if ok else RED
        text   = "Running" if ok else "Stopped"
        if which == "server":
            self.root.after(0, lambda: self._server_dot.config(fg=colour))
            self.root.after(0, lambda: self._server_status_text.set(text))
        else:
            self.root.after(0, lambda: self._tunnel_dot.config(fg=colour))
            self.root.after(0, lambda: self._tunnel_status_text.set(text))

    def _initial_check(self):
        srv = _server_responding()
        tun = _tunnel_running()
        self._update_card("server", srv)
        self._update_card("tunnel", tun)
        self._log_line(
            f"Initial status — Server: {'✓ Running' if srv else '✗ Stopped'}, "
            f"Tunnel: {'✓ Running' if tun else '✗ Stopped'}"
        )

    def _refresh_status(self):
        self._server_status_text.set("Checking…")
        self._tunnel_status_text.set("Checking…")
        self._server_dot.config(fg=AMBER)
        self._tunnel_dot.config(fg=AMBER)
        threading.Thread(target=self._initial_check, daemon=True).start()

    def _poll_ui(self):
        """Called every 500 ms to keep the UI alive under the watchdog."""
        self.root.after(500, self._poll_ui)

    # ── Manual controls ───────────────────────────────────────────────────

    def _manually_start(self, which: str):
        def _go():
            if which == "server":
                self._log_line("Starting server…")
                ok = _start_server()
                time.sleep(3)
                responding = _server_responding()
                self._update_card("server", responding)
                self._log_line(f"Server start {'OK ✓' if responding else 'sent (not yet responding)'}")
            else:
                self._log_line("Starting Cloudflare tunnel…")
                ok = _start_tunnel()
                time.sleep(4)
                running = _tunnel_running()
                self._update_card("tunnel", running)
                if not ok:
                    self._log_line("⚠  cloudflared not found on PATH or common locations.")
                else:
                    self._log_line(f"Tunnel start {'OK ✓' if running else 'sent (process not yet visible)'}")
        threading.Thread(target=_go, daemon=True).start()

    def _manually_stop(self, which: str):
        def _go():
            if which == "server":
                self._log_line("Stopping server…")
                _stop_server()
                time.sleep(2)
                self._update_card("server", False)
                self._log_line("Server stopped.")
            else:
                self._log_line("Stopping Cloudflare tunnel…")
                _stop_tunnel()
                time.sleep(2)
                self._update_card("tunnel", False)
                self._log_line("Tunnel stopped.")
        threading.Thread(target=_go, daemon=True).start()

    # ── Watchdog ──────────────────────────────────────────────────────────

    def _on_watchdog_toggle(self):
        if self._watchdog_enabled.get():
            self._start_watchdog()
        else:
            self._stop_watchdog()

    def _get_interval_secs(self):
        try:
            lo = max(1, int(self._watchdog_min_var.get())) * 60
            hi = max(lo + 60, int(self._watchdog_max_var.get())) * 60
        except ValueError:
            lo, hi = DEFAULT_MIN_SECS, DEFAULT_MAX_SECS
        return lo, hi

    def _start_watchdog(self):
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            return
        self._watchdog_stop.clear()
        self._log_line("Watchdog ENABLED — will check at random intervals.")
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, daemon=True, name="app-watchdog"
        )
        self._watchdog_thread.start()

    def _stop_watchdog(self):
        self._watchdog_stop.set()
        self._next_check_var.set("—")
        self._log_line("Watchdog DISABLED.")

    def _watchdog_loop(self):
        while not self._watchdog_stop.is_set():
            lo, hi = self._get_interval_secs()
            wait = random.randint(lo, hi)
            wake_at = datetime.now().replace(microsecond=0)
            # Show countdown target
            import datetime as dt
            next_time = dt.datetime.now() + dt.timedelta(seconds=wait)
            self.root.after(0, lambda t=next_time.strftime("%H:%M:%S"):
                            self._next_check_var.set(t))

            # Wait in 1-second slices so stop event is reacted to quickly
            for _ in range(wait):
                if self._watchdog_stop.is_set():
                    return
                time.sleep(1)

            if self._watchdog_stop.is_set():
                return

            # ── Perform the health check ──
            self._perform_watchdog_check()

    def _perform_watchdog_check(self):
        now_str = datetime.now().strftime("%H:%M:%S")
        self.root.after(0, lambda: self._last_check_var.set(now_str))
        self.root.after(0, lambda: self._next_check_var.set("checking…"))

        srv_ok = _server_responding()
        tun_ok = _tunnel_running()

        self._update_card("server", srv_ok)
        self._update_card("tunnel", tun_ok)

        self._log_line(
            f"Watchdog check — Server: {'✓' if srv_ok else '✗'}  "
            f"Tunnel: {'✓' if tun_ok else '✗'}"
        )

        if not srv_ok or not tun_ok:
            self._log_line("⚠  Problem detected — restarting services…")

            # Stop both cleanly first
            if not tun_ok:
                _stop_tunnel()
                time.sleep(1)

            # Restart tunnel first, then server
            self._log_line("  → Starting Cloudflare tunnel…")
            tunnel_spawned = _start_tunnel()
            if not tunnel_spawned:
                self._log_line("  ⚠  cloudflared not found — check it is installed and on PATH.")

            time.sleep(4)  # Give tunnel time to initialise

            self._log_line("  → Starting Flask server…")
            _start_server()
            time.sleep(5)  # Give server time to start

            # Re-verify
            srv_ok2 = _server_responding()
            tun_ok2 = _tunnel_running()
            self._update_card("server", srv_ok2)
            self._update_card("tunnel", tun_ok2)

            self._log_line(
                f"  Recovery result — Server: {'✓ OK' if srv_ok2 else '✗ still down'}  "
                f"Tunnel: {'✓ OK' if tun_ok2 else '✗ still down'}"
            )

    # ── Entry point ───────────────────────────────────────────────────────

    def run(self):
        self.root.mainloop()


# ──────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    AppManagerApp().run()
