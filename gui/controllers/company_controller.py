"""
gui/controllers/company_controller.py
=======================================
Loads and persists per-company scheduler configuration.

Since your DB doesn't have a dedicated scheduler table, we store
schedule config in a simple JSON file: scheduler_config.json
(next to run_gui.py). This keeps it lightweight — no schema change needed.

Format:
{
  "ABC Traders Pvt Ltd": {
    "enabled":   true,
    "interval":  "hourly",    // "hourly" | "daily" | "minutes"
    "value":     1,           // every N hours/minutes
    "time":      "09:00",     // HH:MM for daily
    "vouchers": ["sales", "purchase", "ledger", ...]
  },
  ...
}
"""

import json
import os
from typing import Optional

from gui.state import AppState, CompanyState

SCHEDULER_CONFIG_FILE = "scheduler_config.json"


class CompanyController:

    def __init__(self, state: AppState):
        self._state = state

    # ─────────────────────────────────────────────────────────────────────────
    #  Load scheduler config from file → into state
    # ─────────────────────────────────────────────────────────────────────────
    def load_scheduler_config(self):
        """
        Read scheduler_config.json and apply settings to matching CompanyState objects.
        Safe to call even if file doesn't exist.
        """
        if not os.path.exists(SCHEDULER_CONFIG_FILE):
            return

        try:
            with open(SCHEDULER_CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"[CompanyController] Could not read scheduler config: {e}")
            return

        for name, cfg in data.items():
            co = self._state.companies.get(name)
            if co:
                co.schedule_enabled  = cfg.get("enabled",  False)
                co.schedule_interval = cfg.get("interval", "hourly")
                co.schedule_value    = int(cfg.get("value", 1))
                co.schedule_time     = cfg.get("time",     "09:00")

    # ─────────────────────────────────────────────────────────────────────────
    #  Save all scheduler config from state → file
    # ─────────────────────────────────────────────────────────────────────────
    def save_scheduler_config(self):
        """Persist all company schedule settings to scheduler_config.json."""
        data = {}
        for name, co in self._state.companies.items():
            data[name] = {
                "enabled":  co.schedule_enabled,
                "interval": co.schedule_interval,
                "value":    co.schedule_value,
                "time":     co.schedule_time,
            }
        try:
            with open(SCHEDULER_CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[CompanyController] Could not save scheduler config: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    #  Save one company
    # ─────────────────────────────────────────────────────────────────────────
    def save_one(self, name: str):
        """Save just one company's schedule. Reads existing file to preserve others."""
        existing = {}
        if os.path.exists(SCHEDULER_CONFIG_FILE):
            try:
                with open(SCHEDULER_CONFIG_FILE, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except Exception:
                pass

        co = self._state.companies.get(name)
        if co:
            existing[name] = {
                "enabled":  co.schedule_enabled,
                "interval": co.schedule_interval,
                "value":    co.schedule_value,
                "time":     co.schedule_time,
            }

        try:
            with open(SCHEDULER_CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)
        except Exception as e:
            print(f"[CompanyController] Could not save config for {name}: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    #  Compute next run time string for display
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    def next_run_label(co: CompanyState) -> str:
        """Return a human-readable 'Next run: ...' string."""
        from datetime import datetime, timedelta

        if not co.schedule_enabled:
            return "—"

        now = datetime.now()

        if co.schedule_interval == "minutes":
            delta = timedelta(minutes=co.schedule_value)
            next_run = now + delta
            return next_run.strftime("%d %b %Y  %H:%M")

        elif co.schedule_interval == "hourly":
            delta = timedelta(hours=co.schedule_value)
            next_run = now + delta
            return next_run.strftime("%d %b %Y  %H:%M")

        elif co.schedule_interval == "daily":
            try:
                h, m    = map(int, co.schedule_time.split(":"))
                target  = now.replace(hour=h, minute=m, second=0, microsecond=0)
                if target <= now:
                    target += timedelta(days=1)
                return target.strftime("%d %b %Y  %H:%M")
            except Exception:
                return "—"

        return "—"