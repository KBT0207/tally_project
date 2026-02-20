"""
gui/pages/sync_page.py
========================
Two-phase page:

Phase A — Options Form:
  - Selected companies list (read from state.selected_companies)
  - Sync type: Incremental (CDC) or Initial Snapshot
  - Date range picker (shown only for snapshot)
  - Voucher type checkboxes
  - Batch mode: Sequential or Parallel
  - [Start Sync] button

Phase B — Progress Screen (replaces form during active sync):
  - One SyncProgressPanel per company
  - Live log per company
  - Cancel All button
  - Auto-returns to form when done
"""

import queue
import threading
import tkinter as tk
from tkinter import messagebox
from datetime import datetime

from gui.state      import AppState, CompanyStatus, SyncMode
from gui.styles     import Color, Font, Spacing
from gui.components.voucher_selector    import VoucherSelector
from gui.components.date_range_picker   import DateRangePicker
from gui.components.sync_progress_panel import SyncProgressPanel
from gui.controllers.sync_controller    import SyncController


class SyncPage(tk.Frame):

    def __init__(self, parent, state: AppState, navigate, app):
        super().__init__(parent, bg=Color.BG_ROOT)
        self.state    = state
        self.navigate = navigate
        self.app      = app

        self._sync_queue:    queue.Queue = queue.Queue()
        self._controller:    SyncController = None
        self._panels: dict[str, SyncProgressPanel] = {}

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self._build_options_view()
        self._build_progress_view()

        # Start in options view
        self._show_options()

    # ─────────────────────────────────────────────────────────────────────────
    #  Options view (Phase A)
    # ─────────────────────────────────────────────────────────────────────────
    def _build_options_view(self):
        self._options_frame = tk.Frame(self, bg=Color.BG_ROOT)
        self._options_frame.grid(row=0, column=0, sticky="nsew")
        self._options_frame.columnconfigure(0, weight=1)
        self._options_frame.rowconfigure(0, weight=1)

        # Scrollable canvas so it works on small screens
        canvas = tk.Canvas(
            self._options_frame, bg=Color.BG_ROOT,
            highlightthickness=0, bd=0,
        )
        canvas.grid(row=0, column=0, sticky="nsew")

        vsb = tk.Scrollbar(self._options_frame, orient="vertical", command=canvas.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vsb.set)

        inner = tk.Frame(canvas, bg=Color.BG_ROOT)
        cw = canvas.create_window((0, 0), window=inner, anchor="nw")

        inner.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.bind(
            "<Configure>",
            lambda e: canvas.itemconfig(cw, width=e.width)
        )
        canvas.bind_all(
            "<MouseWheel>",
            lambda e: canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
        )

        self._build_options_content(inner)

    def _build_options_content(self, parent):
        pad = Spacing.XL
        parent.columnconfigure(0, weight=1)

        # ── Section: Selected Companies ───────────────────
        sec1 = self._make_card(parent, row=0)
        tk.Label(sec1, text="Selected Companies",
                 font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
                 ).pack(anchor="w", pady=(0, Spacing.SM))

        self._companies_lbl = tk.Label(
            sec1, text="None selected",
            font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY, anchor="w",
        )
        self._companies_lbl.pack(anchor="w")

        tk.Button(
            sec1, text="← Change Selection",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_SECONDARY,
            relief="flat", bd=0, padx=0, pady=0, cursor="hand2",
            command=lambda: self.navigate("home"),
        ).pack(anchor="w", pady=(Spacing.SM, 0))

        # ── Section: Sync Type ────────────────────────────
        sec2 = self._make_card(parent, row=1)
        tk.Label(sec2, text="Sync Type",
                 font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
                 ).pack(anchor="w", pady=(0, Spacing.SM))

        self._sync_mode_var = tk.StringVar(value=SyncMode.INCREMENTAL)

        modes = [
            (SyncMode.INCREMENTAL, "Incremental  (CDC — uses alter_id, fastest)",
             "Fetches only records changed since last sync.\nRecommended for daily/hourly runs."),
            (SyncMode.SNAPSHOT,    "Initial Snapshot  (full date range pull)",
             "Fetches all data in the selected date range.\nUse for first-time setup."),
        ]

        for val, label, hint in modes:
            row_f = tk.Frame(sec2, bg=Color.BG_CARD)
            row_f.pack(anchor="w", pady=2)
            tk.Radiobutton(
                row_f,
                text=label,
                variable=self._sync_mode_var,
                value=val,
                font=Font.BODY_BOLD,
                bg=Color.BG_CARD, activebackground=Color.BG_CARD,
                fg=Color.TEXT_PRIMARY,
                command=self._on_sync_mode_change,
            ).pack(side="left")
            tk.Label(
                row_f, text=f"  {hint.split(chr(10))[0]}",
                font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            ).pack(side="left")

        # ── Section: Date Range (snapshot only) ───────────
        self._date_card = self._make_card(parent, row=2)
        tk.Label(
            self._date_card, text="Date Range  (for Initial Snapshot)",
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).pack(anchor="w", pady=(0, Spacing.SM))

        self._date_picker = DateRangePicker(
            self._date_card,
            label="",
        )
        self._date_picker.pack(anchor="w")

        self._date_override_var = tk.BooleanVar(value=False)
        over_row = tk.Frame(self._date_card, bg=Color.BG_CARD)
        over_row.pack(anchor="w", pady=(Spacing.SM, 0))
        tk.Checkbutton(
            over_row,
            text="Override company 'starting_from' date manually",
            variable=self._date_override_var,
            font=Font.BODY_SM, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
            fg=Color.TEXT_SECONDARY,
        ).pack(side="left")

        # ── Section: What to Sync ─────────────────────────
        sec3 = self._make_card(parent, row=3)
        self._voucher_sel = VoucherSelector(sec3, selection=self.state.voucher_selection)
        self._voucher_sel.pack(fill="x")

        # ── Section: Batch Mode ───────────────────────────
        sec4 = self._make_card(parent, row=4)
        tk.Label(sec4, text="Batch Mode",
                 font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
                 ).pack(anchor="w", pady=(0, Spacing.SM))

        self._batch_var = tk.BooleanVar(value=True)  # True = sequential

        for val, label, hint in [
            (True,  "Sequential  (one company at a time)",
             "Safer. Good for 1–5 companies."),
            (False, "Parallel  (all companies simultaneously)",
             "Faster. Use with caution on many companies."),
        ]:
            rf = tk.Frame(sec4, bg=Color.BG_CARD)
            rf.pack(anchor="w", pady=2)
            tk.Radiobutton(
                rf, text=label, variable=self._batch_var, value=val,
                font=Font.BODY_BOLD, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
                fg=Color.TEXT_PRIMARY,
            ).pack(side="left")
            tk.Label(
                rf, text=f"  {hint}",
                font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            ).pack(side="left")

        # ── Action buttons ────────────────────────────────
        btn_bar = tk.Frame(parent, bg=Color.BG_ROOT, pady=Spacing.LG)
        btn_bar.grid(row=5, column=0, sticky="ew", padx=pad)

        tk.Button(
            btn_bar, text="← Back",
            font=Font.BUTTON, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=Spacing.LG, pady=Spacing.SM,
            cursor="hand2", command=lambda: self.navigate("home"),
        ).pack(side="left")

        self._start_btn = tk.Button(
            btn_bar, text="▶  Start Sync",
            font=Font.BUTTON, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.XL, pady=Spacing.SM,
            cursor="hand2", command=self._on_start_sync,
        )
        self._start_btn.pack(side="right")

        # Initial visibility
        self._on_sync_mode_change()

    # ─────────────────────────────────────────────────────────────────────────
    #  Progress view (Phase B)
    # ─────────────────────────────────────────────────────────────────────────
    def _build_progress_view(self):
        self._progress_frame = tk.Frame(self, bg=Color.BG_ROOT)
        self._progress_frame.grid(row=0, column=0, sticky="nsew")
        self._progress_frame.columnconfigure(0, weight=1)
        self._progress_frame.rowconfigure(1, weight=1)

        # Top bar
        top = tk.Frame(self._progress_frame, bg=Color.BG_ROOT, pady=Spacing.MD)
        top.grid(row=0, column=0, sticky="ew", padx=Spacing.XL)
        top.columnconfigure(0, weight=1)

        self._progress_title = tk.Label(
            top, text="Sync in Progress...",
            font=Font.HEADING_4, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY, anchor="w",
        )
        self._progress_title.grid(row=0, column=0, sticky="w")

        self._cancel_btn = tk.Button(
            top, text="✖  Cancel All",
            font=Font.BUTTON, bg=Color.DANGER_BG, fg=Color.DANGER_FG,
            relief="flat", bd=0, padx=Spacing.LG, pady=Spacing.SM,
            cursor="hand2", command=self._on_cancel,
        )
        self._cancel_btn.grid(row=0, column=1)

        # Scrollable panels area
        canvas = tk.Canvas(
            self._progress_frame, bg=Color.BG_ROOT,
            highlightthickness=0, bd=0,
        )
        canvas.grid(row=1, column=0, sticky="nsew", padx=Spacing.XL, pady=Spacing.MD)

        vsb = tk.Scrollbar(self._progress_frame, orient="vertical", command=canvas.yview)
        vsb.grid(row=1, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vsb.set)

        self._panels_frame = tk.Frame(canvas, bg=Color.BG_ROOT)
        pw = canvas.create_window((0, 0), window=self._panels_frame, anchor="nw")
        self._panels_frame.columnconfigure(0, weight=1)

        self._panels_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(pw, width=e.width))

        # Done button (hidden until sync finishes)
        self._done_btn = tk.Button(
            self._progress_frame,
            text="✓  Done — Back to Companies",
            font=Font.BUTTON,
            bg=Color.SUCCESS, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.XL, pady=Spacing.MD,
            cursor="hand2",
            command=lambda: self.navigate("home"),
        )

    # ─────────────────────────────────────────────────────────────────────────
    #  UI helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _make_card(self, parent, row: int) -> tk.Frame:
        card = tk.Frame(
            parent, bg=Color.BG_CARD,
            highlightthickness=1, highlightbackground=Color.BORDER,
            padx=Spacing.XL, pady=Spacing.LG,
        )
        card.grid(row=row, column=0, sticky="ew",
                  padx=Spacing.XL, pady=(0, Spacing.MD))
        card.columnconfigure(0, weight=1)
        return card

    def _show_options(self):
        self._progress_frame.lower()
        self._options_frame.tkraise()

    def _show_progress(self):
        self._options_frame.lower()
        self._progress_frame.tkraise()

    def _on_sync_mode_change(self):
        mode = self._sync_mode_var.get()
        if mode == SyncMode.SNAPSHOT:
            self._date_card.grid()
        else:
            self._date_card.grid_remove()

    # ─────────────────────────────────────────────────────────────────────────
    #  Start sync
    # ─────────────────────────────────────────────────────────────────────────
    def _on_start_sync(self):
        companies = self.state.selected_companies
        if not companies:
            messagebox.showwarning("No Companies", "Please select at least one company.")
            self.navigate("home")
            return

        if self.state.sync_active:
            messagebox.showwarning("Already Running", "A sync is already in progress.")
            return

        mode = self._sync_mode_var.get()

        # Validate dates for snapshot
        from_date = None
        to_date   = datetime.now().strftime("%Y%m%d")

        if mode == SyncMode.SNAPSHOT:
            valid, err = self._date_picker.validate()
            if not valid:
                messagebox.showerror("Invalid Dates", err)
                return
            from_date = self._date_picker.from_date
            to_date   = self._date_picker.to_date

        # Save to state
        self.state.sync_mode        = mode
        self.state.sync_from_date   = from_date
        self.state.sync_to_date     = to_date
        self.state.batch_sequential = self._batch_var.get()

        # ── Build progress panels ─────────────────────────
        for w in self._panels_frame.winfo_children():
            w.destroy()
        self._panels.clear()

        for i, name in enumerate(companies):
            panel = SyncProgressPanel(
                self._panels_frame,
                company_name=name,
                on_cancel=self._on_cancel_one,
            )
            panel.grid(
                row=i, column=0, sticky="ew",
                pady=(0, Spacing.SM),
            )
            self._panels[name] = panel

            if i > 0 and self.state.batch_sequential:
                panel.mark_waiting()

        self._done_btn.grid_remove()
        self._progress_title.configure(text="Sync in Progress...")
        self._cancel_btn.configure(state="normal", text="✖  Cancel All")
        self._show_progress()

        # ── Clear sync queue ──────────────────────────────
        while not self._sync_queue.empty():
            try: self._sync_queue.get_nowait()
            except queue.Empty: break

        # ── Create and start controller ───────────────────
        self._controller = SyncController(
            state      = self.state,
            out_queue  = self._sync_queue,
            companies  = list(companies),
            sync_mode  = mode,
            from_date  = from_date,
            to_date    = to_date,
            vouchers   = self.state.voucher_selection,
            sequential = self.state.batch_sequential,
        )
        self._controller.start()

        # Start queue polling
        self._poll_sync_queue()

    # ─────────────────────────────────────────────────────────────────────────
    #  Queue polling — runs on main thread via .after()
    # ─────────────────────────────────────────────────────────────────────────
    def _poll_sync_queue(self):
        try:
            while True:
                msg = self._sync_queue.get_nowait()
                self._handle_sync_msg(msg)
        except queue.Empty:
            pass

        # Keep polling while sync is active
        if self.state.sync_active:
            self.after(100, self._poll_sync_queue)

    def _handle_sync_msg(self, msg: tuple):
        event = msg[0]

        if event == "log":
            _, company, line, level = msg
            panel = self._panels.get(company)
            if panel:
                ts = datetime.now().strftime("%H:%M:%S")
                panel.append_log(f"{ts}  {line}", level)
            # Also forward to global logs page
            self.app.post("sync_log", f"[{company}] {line}")

        elif event == "progress":
            _, company, pct, label = msg
            panel = self._panels.get(company)
            if panel:
                panel.set_progress(pct, label)
            # Also update home page cards
            self.state.set_company_progress(company, pct, label)

        elif event == "status":
            _, company, status = msg
            panel = self._panels.get(company)
            if panel:
                panel.set_status(status)
            self.state.set_company_status(company, status)

        elif event == "done":
            _, company, success = msg
            panel = self._panels.get(company)
            if panel:
                panel.mark_done(success)

        elif event == "all_done":
            self._on_all_done()

    # ─────────────────────────────────────────────────────────────────────────
    #  Cancel
    # ─────────────────────────────────────────────────────────────────────────
    def _on_cancel(self):
        if self._controller:
            self._controller.cancel()
        self._cancel_btn.configure(state="disabled", text="Cancelling...")

    def _on_cancel_one(self, company_name: str):
        # For now, cancel all (per-company cancel is complex with shared threads)
        self._on_cancel()

    # ─────────────────────────────────────────────────────────────────────────
    #  All done
    # ─────────────────────────────────────────────────────────────────────────
    def _on_all_done(self):
        self.state.sync_active = False
        self.state.emit("sync_finished")

        self._progress_title.configure(text="Sync Complete ✓")
        self._cancel_btn.configure(state="disabled", text="✖  Cancel All")

        self._done_btn.grid(
            row=2, column=0, columnspan=2,
            padx=Spacing.XL, pady=Spacing.LG, sticky="e",
        )

    # ─────────────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    def on_show(self):
        """Called every time this page is navigated to."""
        if self.state.sync_active:
            # Already running — show progress
            self._show_progress()
            return

        # Update selected companies label
        selected = self.state.selected_companies
        if selected:
            if len(selected) <= 4:
                names = "\n".join(f"  • {n}" for n in selected)
            else:
                names = "\n".join(f"  • {n}" for n in selected[:3])
                names += f"\n  • ... and {len(selected) - 3} more"
            self._companies_lbl.configure(text=names, justify="left")
        else:
            self._companies_lbl.configure(text="No companies selected")

        # Auto-fill from date from first selected company
        if selected:
            co = self.state.get_company(selected[0])
            if co and co.starting_from:
                self._date_picker.set_from_date(co.starting_from)

        self._voucher_sel.refresh()
        self._show_options()