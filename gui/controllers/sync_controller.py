"""
gui/controllers/sync_controller.py
=====================================
Bridges the GUI sync page with the existing sync_service.py.

Responsibilities:
  - Accept sync parameters from the GUI (companies, mode, dates, vouchers)
  - Run each company sync in a background thread
  - Post progress events to a queue that the GUI polls
  - Never block the main thread
  - Support cancel mid-run
  - Update AppState.company status throughout

Thread model:
  GUI thread  ──────────────────────────────────────────────►
                │  start_sync()                             │
                │  ↓ spawns SyncThread                     │
  SyncThread  ──►  runs sync_company()                     │
                │  ↓ puts msgs in queue                    │
  GUI thread  ◄──  polls queue every 100ms via .after()    │

Queue message format (tuples):
  ("log",      company, message, level)
  ("progress", company, pct, label)
  ("status",   company, status_str)
  ("done",     company, success_bool)
  ("all_done",)
"""

import threading
import queue
from datetime import datetime
from typing import Optional

from gui.state  import AppState, CompanyStatus, SyncMode, VoucherSelection
from logging_config import logger


# Voucher types in the order they sync (matches VOUCHER_CONFIG in sync_service)
VOUCHER_ORDER = [
    "ledger",
    "trial_balance",
    "sales",
    "purchase",
    "credit_note",
    "debit_note",
    "receipt",
    "payment",
    "journal",
    "contra",
]


class SyncController:
    """
    Instantiated once per sync run.
    Call start() to kick off, cancel() to request stop.
    Pass a queue.Queue that the GUI polls for messages.
    """

    def __init__(
        self,
        state:       AppState,
        out_queue:   queue.Queue,    # GUI polls this
        companies:   list[str],      # company names to sync
        sync_mode:   str,            # SyncMode.INCREMENTAL | SNAPSHOT
        from_date:   Optional[str],  # YYYYMMDD (snapshot only)
        to_date:     str,            # YYYYMMDD
        vouchers:    VoucherSelection,
        sequential:  bool = True,    # True=one at a time, False=parallel
    ):
        self._state      = state
        self._q          = out_queue
        self._companies  = companies
        self._sync_mode  = sync_mode
        self._from_date  = from_date
        self._to_date    = to_date
        self._vouchers   = vouchers
        self._sequential = sequential
        self._cancelled  = False
        self._threads: list[threading.Thread] = []

    # ─────────────────────────────────────────────────────────────────────────
    #  Public API
    # ─────────────────────────────────────────────────────────────────────────
    def start(self):
        """Start the sync. Returns immediately — work happens in background."""
        self._state.sync_active    = True
        self._state.sync_cancelled = False

        if self._sequential:
            t = threading.Thread(target=self._run_sequential, daemon=True)
            t.start()
            self._threads = [t]
        else:
            self._run_parallel()

    def cancel(self):
        """Signal all threads to stop after current operation."""
        self._cancelled            = True
        self._state.sync_cancelled = True
        self._log_all("Cancellation requested — stopping after current step...", "WARNING")

    # ─────────────────────────────────────────────────────────────────────────
    #  Sequential run — one company at a time
    # ─────────────────────────────────────────────────────────────────────────
    def _run_sequential(self):
        total = len(self._companies)
        for idx, name in enumerate(self._companies):
            if self._cancelled:
                break
            self._post("log", name, f"[{idx+1}/{total}] Starting sync for {name}", "INFO")
            self._sync_one(name)

        self._finish()

    # ─────────────────────────────────────────────────────────────────────────
    #  Parallel run — all companies simultaneously
    # ─────────────────────────────────────────────────────────────────────────
    def _run_parallel(self):
        threads = []
        for name in self._companies:
            t = threading.Thread(target=self._sync_one, args=(name,), daemon=True)
            threads.append(t)
            self._threads = threads

        for t in threads:
            t.start()

        # Wait for all in a watcher thread so we don't block GUI
        def watcher():
            for t in threads:
                t.join()
            self._finish()

        threading.Thread(target=watcher, daemon=True).start()

    # ─────────────────────────────────────────────────────────────────────────
    #  Sync one company — calls existing sync_service functions
    # ─────────────────────────────────────────────────────────────────────────
    def _sync_one(self, company_name: str):
        engine = self._state.db_engine
        if not engine:
            self._post("log", company_name, "No DB engine — cannot sync", "ERROR")
            self._post("status", company_name, CompanyStatus.SYNC_ERROR)
            self._post("done", company_name, False)
            return

        self._post("status",   company_name, CompanyStatus.SYNCING)
        self._post("progress", company_name, 0.0, "Connecting to Tally...")

        # ── Step 1: connect to Tally ──────────────────────
        try:
            from services.tally_connector import TallyConnector
            co_state = self._state.get_company(company_name)
            host = co_state.tally_host if co_state else self._state.tally.host
            port = co_state.tally_port if co_state else self._state.tally.port

            tally = TallyConnector(host=host, port=port)
            if tally.status != "Connected":
                raise ConnectionError(f"Tally not reachable at {host}:{port}")

            self._post("log", company_name, "✓ Tally connected", "SUCCESS")
            self._post("progress", company_name, 5.0, "Tally connected")

        except Exception as e:
            self._post("log",    company_name, f"✗ Tally connection failed: {e}", "ERROR")
            self._post("status", company_name, CompanyStatus.TALLY_OFFLINE)
            self._post("done",   company_name, False)
            return

        if self._cancelled:
            self._post("log",    company_name, "Cancelled before sync started", "WARNING")
            self._post("status", company_name, CompanyStatus.CONFIGURED)
            self._post("done",   company_name, False)
            return

        # ── Step 2: determine dates ───────────────────────
        company_dict = self._build_company_dict(company_name)
        to_date      = self._to_date
        from_date    = self._from_date  # None = let sync_service resolve

        # If incremental, check if initial is done; force snapshot if not
        if self._sync_mode == SyncMode.INCREMENTAL:
            co_state = self._state.get_company(company_name)
            if co_state and not co_state.is_initial_done:
                self._post(
                    "log", company_name,
                    "Initial sync not done — running full snapshot first", "WARNING"
                )
                from_date = co_state.starting_from  # Use company start date

        # ── Step 3: determine which vouchers to sync ──────
        selected = self._vouchers.selected_types()
        self._post(
            "log", company_name,
            f"Syncing: {', '.join(selected)}", "INFO"
        )

        # ── Step 4: run sync via sync_service ─────────────
        try:
            total_steps = len(selected)
            done_steps  = 0

            # Build filtered VOUCHER_CONFIG based on selection
            from services.sync_service import (
                VOUCHER_CONFIG, _sync_ledgers, _sync_trial_balance, _sync_voucher
            )

            # Ledgers (special — always done first if selected)
            if "ledger" in selected:
                if self._cancelled:
                    raise InterruptedError("Cancelled")
                self._post("progress", company_name, 10.0, "Syncing ledgers...")
                self._post("log",      company_name, "→ Ledgers", "INFO")
                _sync_ledgers(company_name, tally, engine)
                done_steps += 1
                pct = 10 + (done_steps / total_steps) * 80
                self._post("progress", company_name, pct, "Ledgers done")
                self._post("log",      company_name, "✓ Ledgers done", "SUCCESS")

            # Trial balance (special — also done directly)
            if "trial_balance" in selected:
                if self._cancelled:
                    raise InterruptedError("Cancelled")
                pct = 10 + (done_steps / max(total_steps, 1)) * 80
                self._post("progress", company_name, pct, "Syncing trial balance...")
                self._post("log",      company_name, "→ Trial Balance", "INFO")
                fd = from_date or company_dict.get('starting_from', '20240401')
                _sync_trial_balance(company_name, tally, engine, fd, to_date)
                done_steps += 1
                self._post("log", company_name, "✓ Trial Balance done", "SUCCESS")

            # All other voucher types
            voucher_configs = [
                cfg for cfg in VOUCHER_CONFIG
                if cfg["voucher_type"] in selected
            ]

            for cfg in voucher_configs:
                if self._cancelled:
                    raise InterruptedError("Cancelled")

                vtype = cfg["voucher_type"]
                label = cfg["parser_type_name"]
                pct   = 10 + (done_steps / max(total_steps, 1)) * 80

                self._post("progress", company_name, pct,  f"Syncing {label}...")
                self._post("log",      company_name, f"→ {label}", "INFO")

                fd = from_date or company_dict.get('starting_from', '20240401')
                _sync_voucher(
                    company_name = company_name,
                    config       = cfg,
                    tally        = tally,
                    engine       = engine,
                    from_date    = fd,
                    to_date      = to_date,
                )

                done_steps += 1
                self._post("log", company_name, f"✓ {label} done", "SUCCESS")

            # ── Done ──────────────────────────────────────
            self._post("progress", company_name, 100.0, "Complete ✓")
            self._post("status",   company_name, CompanyStatus.SYNC_DONE)
            self._post("log",      company_name, f"✓ {company_name} sync complete", "SUCCESS")
            self._post("done",     company_name, True)

            # Update state
            self._state.set_company_status(
                company_name,
                CompanyStatus.SYNC_DONE,
                last_sync_time=datetime.now(),
            )

        except InterruptedError:
            self._post("log",    company_name, "Sync cancelled by user", "WARNING")
            self._post("status", company_name, CompanyStatus.CONFIGURED)
            self._post("done",   company_name, False)

        except Exception as e:
            logger.exception(f"[SyncController][{company_name}] Unexpected error")
            self._post("log",    company_name, f"✗ Error: {e}", "ERROR")
            self._post("status", company_name, CompanyStatus.SYNC_ERROR)
            self._post("done",   company_name, False)

    # ─────────────────────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _finish(self):
        self._state.sync_active = False
        self._q.put(("all_done",))
        logger.info("[SyncController] All company syncs finished")

    def _post(self, *args):
        self._q.put(args)

    def _log_all(self, message: str, level: str = "INFO"):
        for name in self._companies:
            self._post("log", name, message, level)

    def _build_company_dict(self, name: str) -> dict:
        """Build the dict format that sync_service.sync_company expects."""
        co = self._state.get_company(name)
        if co:
            return {
                "name":          co.name,
                "starting_from": co.starting_from or "20240401",
            }
        return {"name": name, "starting_from": "20240401"}