"""
gui/pages/home_page.py
=======================
Home page — full company list with:
  - Configured companies (from DB) with status, last sync, alter_id
  - Not-configured companies (in Tally but not in DB)
  - Checkboxes for single / multi-select
  - Per-card Sync button (quick single company sync)
  - Bulk action bar: Sync Selected, Schedule Selected
  - Select All / Deselect All
  - Search/filter bar
  - Refresh button (re-queries DB)
  - Live progress bar per card during sync
  - Status auto-refreshes via AppState event system
"""

import threading
import tkinter as tk
from tkinter import messagebox

from gui.state  import AppState, CompanyState, CompanyStatus
from gui.styles import Color, Font, Spacing
from gui.components.company_card import CompanyCard


class HomePage(tk.Frame):

    def __init__(self, parent, state: AppState, navigate, app):
        super().__init__(parent, bg=Color.BG_ROOT)
        self.state    = state
        self.navigate = navigate
        self.app      = app

        self._cards: dict[str, CompanyCard] = {}
        self._filter_var = tk.StringVar()
        self._filter_var.trace_add("write", self._on_filter_change)

        self._build()

        # Register state event listeners
        self.state.on("company_updated",  self._on_company_updated)
        self.state.on("company_progress", self._on_company_progress)
        self.state.on("sync_finished",    self._on_sync_finished)

    # ─────────────────────────────────────────────────────────────────────────
    #  Layout
    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        self._build_toolbar()
        self._build_list_area()
        self._build_action_bar()

    def _build_toolbar(self):
        bar = tk.Frame(self, bg=Color.BG_ROOT, pady=Spacing.MD)
        bar.grid(row=0, column=0, sticky="ew", padx=Spacing.XL)
        bar.columnconfigure(1, weight=1)

        self._summary_lbl = tk.Label(
            bar, text="Loading companies...",
            font=Font.BODY, bg=Color.BG_ROOT, fg=Color.TEXT_SECONDARY,
        )
        self._summary_lbl.grid(row=0, column=0, sticky="w")

        right = tk.Frame(bar, bg=Color.BG_ROOT)
        right.grid(row=0, column=1, sticky="e")

        # Search
        tk.Label(right, text="🔍", font=Font.BODY,
                 bg=Color.BG_ROOT, fg=Color.TEXT_MUTED).pack(side="left", padx=(0, 4))

        tk.Entry(
            right, textvariable=self._filter_var,
            font=Font.BODY, bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, width=22,
        ).pack(side="left")

        # Select All / Clear
        for label, cmd in [("Select All", self._select_all), ("Clear", self._deselect_all)]:
            tk.Button(
                right, text=label, font=Font.BUTTON_SM,
                bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1, padx=Spacing.SM, pady=2,
                cursor="hand2", command=cmd,
            ).pack(side="left", padx=(Spacing.MD if label == "Select All" else 2, 2))

        # Refresh
        self._refresh_btn = tk.Button(
            right, text="⟳  Refresh", font=Font.BUTTON_SM,
            bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.MD, pady=4,
            cursor="hand2", command=self._on_refresh,
        )
        self._refresh_btn.pack(side="left", padx=(Spacing.MD, 0))

    def _build_list_area(self):
        # Outer card container
        container = tk.Frame(
            self, bg=Color.BG_CARD, relief="flat",
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        container.grid(
            row=1, column=0, sticky="nsew",
            padx=Spacing.XL, pady=(0, Spacing.MD),
        )
        container.rowconfigure(1, weight=1)
        container.columnconfigure(0, weight=1)

        # Column header
        hdr = tk.Frame(container, bg=Color.BG_TABLE_HEADER)
        hdr.grid(row=0, column=0, columnspan=2, sticky="ew")

        for col, (text, w) in enumerate([
            ("",              3),
            ("Company Name", 34),
            ("Status",       14),
            ("Sync Progress",14),
            ("Actions",      10),
        ]):
            tk.Label(
                hdr, text=text, font=Font.BODY_SM_BOLD,
                bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_SECONDARY,
                anchor="w", padx=Spacing.SM, pady=Spacing.SM, width=w,
            ).grid(row=0, column=col, sticky="ew")

        tk.Frame(container, bg=Color.BORDER, height=1).grid(
            row=0, column=0, columnspan=2, sticky="sew",
        )

        # Scrollable canvas
        self._canvas = tk.Canvas(
            container, bg=Color.BG_CARD, highlightthickness=0, bd=0,
        )
        self._canvas.grid(row=1, column=0, sticky="nsew")

        vsb = tk.Scrollbar(container, orient="vertical", command=self._canvas.yview)
        vsb.grid(row=1, column=1, sticky="ns")
        self._canvas.configure(yscrollcommand=vsb.set)

        self._list_frame = tk.Frame(self._canvas, bg=Color.BG_CARD)
        self._list_frame.columnconfigure(0, weight=1)

        cw = self._canvas.create_window((0, 0), window=self._list_frame, anchor="nw")

        self._list_frame.bind(
            "<Configure>",
            lambda e: self._canvas.configure(scrollregion=self._canvas.bbox("all"))
        )
        self._canvas.bind(
            "<Configure>",
            lambda e: self._canvas.itemconfig(cw, width=e.width)
        )
        self._canvas.bind_all(
            "<MouseWheel>",
            lambda e: self._canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
        )

    def _build_action_bar(self):
        bar = tk.Frame(
            self, bg=Color.BG_HEADER,
            highlightthickness=1, highlightbackground=Color.BORDER,
            pady=Spacing.MD, padx=Spacing.XL,
        )
        bar.grid(row=2, column=0, sticky="ew")
        bar.columnconfigure(0, weight=1)

        self._selection_lbl = tk.Label(
            bar, text="No companies selected",
            font=Font.BODY, bg=Color.BG_HEADER, fg=Color.TEXT_SECONDARY,
        )
        self._selection_lbl.grid(row=0, column=0, sticky="w")

        btns = tk.Frame(bar, bg=Color.BG_HEADER)
        btns.grid(row=0, column=1)

        self._sync_sel_btn = tk.Button(
            btns, text="▶  Sync Selected",
            font=Font.BUTTON, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.LG, pady=Spacing.SM,
            cursor="hand2", state="disabled",
            command=self._on_sync_selected,
        )
        self._sync_sel_btn.pack(side="left", padx=(0, Spacing.SM))

        self._sched_sel_btn = tk.Button(
            btns, text="⏰  Schedule Selected",
            font=Font.BUTTON, bg=Color.INFO_BG, fg=Color.INFO_FG,
            relief="flat", bd=0, padx=Spacing.LG, pady=Spacing.SM,
            cursor="hand2", state="disabled",
            command=self._on_schedule_selected,
        )
        self._sched_sel_btn.pack(side="left")

    # ─────────────────────────────────────────────────────────────────────────
    #  Render cards
    # ─────────────────────────────────────────────────────────────────────────
    def refresh_companies(self):
        """Called by app.py after DB load completes."""
        self._render_cards()
        self._update_summary()

    def _render_cards(self, filter_text: str = ""):
        for w in self._list_frame.winfo_children():
            w.destroy()
        self._cards.clear()

        companies = list(self.state.companies.values())

        if filter_text:
            ft = filter_text.lower()
            companies = [c for c in companies if ft in c.name.lower()]

        if not companies:
            msg = "No companies match your search." if filter_text else \
                  "No companies found.\nClick ⟳ Refresh to load from database."
            tk.Label(
                self._list_frame, text=msg,
                font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                justify="center", pady=60,
            ).pack(fill="both", expand=True)
            return

        # Sort: configured first, then alphabetical
        companies.sort(key=lambda c: (
            0 if c.status != CompanyStatus.NOT_CONFIGURED else 1,
            c.name.lower(),
        ))

        for i, co in enumerate(companies):
            card = CompanyCard(
                parent      = self._list_frame,
                company     = co,
                on_select   = self._on_card_select,
                on_sync     = self._on_single_sync,
                on_schedule = self._on_single_schedule,
                selected    = co.name in self.state.selected_companies,
            )
            # Alternating row color
            bg = Color.BG_TABLE_ODD if i % 2 == 0 else Color.BG_TABLE_EVEN
            card.configure(bg=bg)
            card.pack(fill="x")
            self._cards[co.name] = card

            # Divider
            tk.Frame(self._list_frame, bg=Color.BORDER_LIGHT, height=1).pack(fill="x")

    # ─────────────────────────────────────────────────────────────────────────
    #  Selection
    # ─────────────────────────────────────────────────────────────────────────
    def _on_card_select(self, name: str, selected: bool):
        sel = self.state.selected_companies
        if selected:
            if name not in sel:
                sel.append(name)
        else:
            self.state.selected_companies = [n for n in sel if n != name]
        self._update_action_bar()

    def _select_all(self):
        self.state.selected_companies = list(self.state.companies.keys())
        for card in self._cards.values():
            card.set_selected(True)
        self._update_action_bar()

    def _deselect_all(self):
        self.state.selected_companies = []
        for card in self._cards.values():
            card.set_selected(False)
        self._update_action_bar()

    def _update_action_bar(self):
        n = len(self.state.selected_companies)
        if n == 0:
            self._selection_lbl.configure(text="No companies selected")
            self._sync_sel_btn.configure(state="disabled")
            self._sched_sel_btn.configure(state="disabled")
        else:
            plural = "company" if n == 1 else "companies"
            self._selection_lbl.configure(text=f"{n} {plural} selected")
            self._sync_sel_btn.configure(state="normal")
            self._sched_sel_btn.configure(state="normal")

    def _update_summary(self):
        total      = len(self.state.companies)
        configured = len(self.state.configured_companies())
        syncing    = sum(
            1 for c in self.state.companies.values()
            if c.status == CompanyStatus.SYNCING
        )
        parts = [f"{total} total", f"{configured} configured"]
        if syncing:
            parts.append(f"{syncing} syncing")
        self._summary_lbl.configure(text="  ·  ".join(parts))

    # ─────────────────────────────────────────────────────────────────────────
    #  Button handlers
    # ─────────────────────────────────────────────────────────────────────────
    def _on_refresh(self):
        self._refresh_btn.configure(text="⟳  Refreshing...", state="disabled")

        def worker():
            try:
                self.app._load_companies_from_db(self.state.db_engine)
                self.app.post("companies_loaded", None)
            except Exception as e:
                self.app.post("error", f"Refresh failed: {e}")
            finally:
                self.after(0, lambda: self._refresh_btn.configure(
                    text="⟳  Refresh", state="normal"
                ))

        threading.Thread(target=worker, daemon=True).start()

    def _on_sync_selected(self):
        if not self.state.selected_companies:
            messagebox.showwarning("No Selection", "Please select at least one company.")
            return
        if self.state.sync_active:
            messagebox.showwarning("Sync Running", "A sync is already in progress.")
            return
        self.navigate("sync")

    def _on_single_sync(self, name: str):
        self.state.selected_companies = [name]
        for n, card in self._cards.items():
            card.set_selected(n == name)
        self._update_action_bar()
        self.navigate("sync")

    def _on_schedule_selected(self):
        if not self.state.selected_companies:
            messagebox.showwarning("No Selection", "Please select at least one company.")
            return
        self.navigate("scheduler")

    def _on_single_schedule(self, name: str):
        self.state.selected_companies = [name]
        for n, card in self._cards.items():
            card.set_selected(n == name)
        self._update_action_bar()
        self.navigate("scheduler")

    # ─────────────────────────────────────────────────────────────────────────
    #  Filter
    # ─────────────────────────────────────────────────────────────────────────
    def _on_filter_change(self, *args):
        self._render_cards(filter_text=self._filter_var.get().strip())

    # ─────────────────────────────────────────────────────────────────────────
    #  AppState event callbacks  (always use .after(0) for thread safety)
    # ─────────────────────────────────────────────────────────────────────────
    def _on_company_updated(self, name: str, company: CompanyState):
        def _do():
            if name in self._cards:
                self._cards[name].update_status(company.status)
            self._update_summary()
        self.after(0, _do)

    def _on_company_progress(self, name: str, pct: float, label: str):
        def _do():
            if name in self._cards:
                self._cards[name].update_progress(pct, label)
        self.after(0, _do)

    def _on_sync_finished(self):
        def _do():
            self._update_summary()
            for card in self._cards.values():
                card.update_progress(0.0, "")
        self.after(0, _do)

    # ─────────────────────────────────────────────────────────────────────────
    def on_show(self):
        """Called every time this page is navigated to."""
        self._update_summary()
        self._update_action_bar()
        if self.state.companies:
            self._render_cards(filter_text=self._filter_var.get().strip())