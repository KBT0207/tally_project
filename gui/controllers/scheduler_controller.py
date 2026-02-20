"""
gui/controllers/scheduler_controller.py
=========================================
Manages APScheduler background jobs — one job per company.

Features:
  - Start / stop / restart individual company jobs
  - Persists jobs in MySQL using SQLAlchemyJobStore
    (survives app restart — scheduler resumes automatically)
  - Integrates with SyncController for actual sync execution
  - Posts status updates to app queue for GUI feedback

Job ID convention:  "sync_<company_name_slug>"
"""

import threading
import queue
import re
from datetime import datetime
from typing import Optional

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.jobstores.sqlalchemy  import SQLAlchemyJobStore
    from apscheduler.triggers.interval     import IntervalTrigger
    from apscheduler.triggers.cron         import CronTrigger
    from apscheduler.events import (
        EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
    )
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False

from gui.state import AppState, CompanyState, CompanyStatus
from logging_config import logger


def _slug(name: str) -> str:
    """Convert company name to a safe job ID."""
    return "sync_" + re.sub(r"[^a-zA-Z0-9_]", "_", name)


class SchedulerController:
    """
    One instance per app session.
    Call start() once on app launch.
    """

    def __init__(self, state: AppState, app_queue: queue.Queue):
        self._state    = state
        self._q        = app_queue
        self._scheduler: Optional[object] = None
        self._lock     = threading.Lock()

    # ─────────────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    def start(self):
        """Start the APScheduler. Call once on app launch."""
        if not HAS_APSCHEDULER:
            logger.warning("[Scheduler] APScheduler not installed — scheduler disabled")
            return

        try:
            # Store jobs in MySQL so they survive restarts
            jobstores = {}
            if self._state.db_engine:
                jobstores["default"] = SQLAlchemyJobStore(
                    engine=self._state.db_engine,
                    tablename="apscheduler_jobs",
                )

            self._scheduler = BackgroundScheduler(
                jobstores    = jobstores if jobstores else None,
                job_defaults = {
                    "coalesce":       True,   # merge missed runs into one
                    "max_instances":  1,      # never run same job twice at once
                    "misfire_grace_time": 300,# allow 5min late start
                },
                timezone="Asia/Kolkata",
            )

            # Listen to job events
            self._scheduler.add_listener(
                self._on_job_event,
                EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
            )

            self._scheduler.start()
            logger.info("[Scheduler] APScheduler started")

            # Re-add jobs for all enabled companies (in case jobstore is empty)
            self._sync_all_jobs()

        except Exception as e:
            logger.error(f"[Scheduler] Failed to start: {e}")

    def shutdown(self):
        """Gracefully stop the scheduler."""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("[Scheduler] Shutdown complete")

    # ─────────────────────────────────────────────────────────────────────────
    #  Job management
    # ─────────────────────────────────────────────────────────────────────────
    def add_or_update_job(self, company_name: str):
        """
        Add or reschedule a job for a company based on its current state config.
        Safe to call even if job already exists — it replaces it.
        """
        if not HAS_APSCHEDULER or not self._scheduler:
            return

        co = self._state.get_company(company_name)
        if not co or not co.schedule_enabled:
            self.remove_job(company_name)
            return

        job_id  = _slug(company_name)
        trigger = self._build_trigger(co)

        with self._lock:
            try:
                self._scheduler.add_job(
                    func            = self._run_sync_job,
                    trigger         = trigger,
                    id              = job_id,
                    name            = f"Sync: {company_name}",
                    kwargs          = {"company_name": company_name},
                    replace_existing= True,
                )
                logger.info(
                    f"[Scheduler] Job added/updated: {job_id} "
                    f"({co.schedule_interval} × {co.schedule_value})"
                )
                self._post_schedule_update(company_name)
            except Exception as e:
                logger.error(f"[Scheduler] Failed to add job for {company_name}: {e}")

    def remove_job(self, company_name: str):
        """Remove a scheduled job for a company."""
        if not HAS_APSCHEDULER or not self._scheduler:
            return

        job_id = _slug(company_name)
        try:
            if self._scheduler.get_job(job_id):
                self._scheduler.remove_job(job_id)
                logger.info(f"[Scheduler] Job removed: {job_id}")
                self._post_schedule_update(company_name)
        except Exception as e:
            logger.error(f"[Scheduler] Failed to remove job for {company_name}: {e}")

    def pause_job(self, company_name: str):
        if not self._scheduler:
            return
        try:
            self._scheduler.pause_job(_slug(company_name))
        except Exception:
            pass

    def resume_job(self, company_name: str):
        if not self._scheduler:
            return
        try:
            self._scheduler.resume_job(_slug(company_name))
        except Exception:
            pass

    def get_next_run(self, company_name: str) -> Optional[datetime]:
        """Return next scheduled run time for a company, or None."""
        if not self._scheduler:
            return None
        try:
            job = self._scheduler.get_job(_slug(company_name))
            return job.next_run_time if job else None
        except Exception:
            return None

    def get_all_jobs(self) -> list:
        """Return all active APScheduler jobs."""
        if not self._scheduler:
            return []
        try:
            return self._scheduler.get_jobs()
        except Exception:
            return []

    def is_running(self) -> bool:
        return bool(self._scheduler and self._scheduler.running)

    # ─────────────────────────────────────────────────────────────────────────
    #  Trigger builder
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _build_trigger(co: CompanyState):
        if co.schedule_interval == "minutes":
            return IntervalTrigger(minutes=max(1, co.schedule_value))

        elif co.schedule_interval == "hourly":
            return IntervalTrigger(hours=max(1, co.schedule_value))

        elif co.schedule_interval == "daily":
            try:
                h, m = map(int, co.schedule_time.split(":"))
            except Exception:
                h, m = 9, 0
            return CronTrigger(hour=h, minute=m)

        # Fallback — hourly
        return IntervalTrigger(hours=1)

    # ─────────────────────────────────────────────────────────────────────────
    #  Job execution (runs in APScheduler thread)
    # ─────────────────────────────────────────────────────────────────────────
    def _run_sync_job(self, company_name: str):
        """
        Called by APScheduler in a background thread.
        Creates a SyncController and runs it synchronously (blocking the job thread).
        """
        logger.info(f"[Scheduler] Triggered sync for: {company_name}")

        if self._state.sync_active:
            logger.warning(
                f"[Scheduler] Skipping {company_name} — manual sync already running"
            )
            return

        from gui.controllers.sync_controller import SyncController

        job_queue = queue.Queue()

        controller = SyncController(
            state      = self._state,
            out_queue  = job_queue,
            companies  = [company_name],
            sync_mode  = "incremental",
            from_date  = None,
            to_date    = datetime.now().strftime("%Y%m%d"),
            vouchers   = self._state.voucher_selection,
            sequential = True,
        )
        controller.start()

        # Drain queue and forward log lines to GUI
        import time
        while self._state.sync_active or not job_queue.empty():
            try:
                msg = job_queue.get(timeout=0.5)
                if msg[0] in ("log", "progress", "status", "done"):
                    self._q.put(msg)
                elif msg[0] == "all_done":
                    self._q.put(("scheduler_sync_done", company_name))
                    break
            except queue.Empty:
                pass
            time.sleep(0.1)

    # ─────────────────────────────────────────────────────────────────────────
    #  APScheduler event listener
    # ─────────────────────────────────────────────────────────────────────────
    def _on_job_event(self, event):
        job_id = getattr(event, "job_id", "")
        if not job_id.startswith("sync_"):
            return

        # Extract company name from job_id
        job = None
        try:
            job = self._scheduler.get_job(job_id)
        except Exception:
            pass

        company_name = (job.kwargs.get("company_name") if job else None) or job_id

        if hasattr(event, "exception") and event.exception:
            logger.error(f"[Scheduler] Job {job_id} failed: {event.exception}")
            self._q.put(("scheduler_job_error", company_name, str(event.exception)))
        else:
            self._post_schedule_update(company_name)

    def _post_schedule_update(self, company_name: str):
        """Tell the GUI the schedule changed for a company."""
        self._q.put(("scheduler_updated", company_name))

    # ─────────────────────────────────────────────────────────────────────────
    #  Sync all enabled jobs on startup
    # ─────────────────────────────────────────────────────────────────────────
    def _sync_all_jobs(self):
        """Add/update jobs for all currently enabled companies."""
        for name, co in self._state.companies.items():
            if co.schedule_enabled:
                self.add_or_update_job(name)