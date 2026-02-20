"""
gui/components/company_card.py
================================
A single company row card for the home page list.

Layout per row:
┌──────────────────────────────────────────────────────────────────┐
│ [☑]  ABC Traders Pvt Ltd          ● Configured      [Sync] [▶]  │
│      Last sync: 20-Feb-2026 10:30  alter_id: 4521   Books: Apr  │
└──────────────────────────────────────────────────────────────────┘
"""

import tkinter as tk
from datetime import datetime
from typing import Callable

from gui.state  import CompanyState, CompanyStatus
from gui.styles import Color, Font, Spacing, STATUS_STYLE
from gui.components.status_badge import StatusBadge


class CompanyCard(tk.Frame):

    def __init__(
        self,
        parent,
        company:     CompanyState,
        on_select:   Callable,
        on_sync:     Callable,
        on_schedule: Callable,
        selected:    bool = False,
        **kwargs,
    ):
        super().__init__(parent, bg=Color.BG_CARD, relief="flat", bd=0, **kwargs)
        self.company     = company
        self.on_select   = on_select
        self.on_sync     = on_sync
        self.on_schedule = on_schedule
        self._selected_var = tk.BooleanVar(value=selected)
        self._bg_frames    = []
        self._build()
        self._bind_hover()

    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        co = self.company

        outer = tk.Frame(self, bg=Color.BG_CARD, padx=Spacing.LG, pady=10)
        outer.pack(fill="x")
        outer.columnconfigure(1, weight=1)
        self._bg_frames = [self, outer]

        # ── Checkbox ──────────────────────────────────────
        chk = tk.Checkbutton(
            outer,
            variable=self._selected_var,
            bg=Color.BG_CARD,
            activebackground=Color.BG_CARD,
            relief="flat", bd=0,
            command=self._on_toggle,
        )
        chk.grid(row=0, column=0, rowspan=2, sticky="ns", padx=(0, Spacing.MD))
        self._bg_frames.append(chk)

        # ── Company name row ──────────────────────────────
        name_row = tk.Frame(outer, bg=Color.BG_CARD)
        name_row.grid(row=0, column=1, sticky="ew")
        self._bg_frames.append(name_row)

        self._name_lbl = tk.Label(
            name_row, text=co.name,
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY, anchor="w",
        )
        self._name_lbl.pack(side="left")
        self._bg_frames.append(self._name_lbl)

        if co.schedule_enabled:
            tk.Label(
                name_row, text="⏰ Scheduled",
                font=Font.BADGE, bg=Color.INFO_BG, fg=Color.INFO_FG,
                padx=5, pady=1,
            ).pack(side="left", padx=(Spacing.SM, 0))

        # ── Meta info row ─────────────────────────────────
        meta = tk.Frame(outer, bg=Color.BG_CARD)
        meta.grid(row=1, column=1, sticky="ew", pady=(2, 0))
        self._bg_frames.append(meta)

        self._meta_lbl = tk.Label(
            meta, text=f"Last sync: {self._fmt_sync_time(co.last_sync_time)}",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY, anchor="w",
        )
        self._meta_lbl.pack(side="left")
        self._bg_frames.append(self._meta_lbl)

        if co.last_alter_id:
            sep = tk.Label(meta, text="  ·  ", font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED)
            sep.pack(side="left")
            al = tk.Label(meta, text=f"alter_id: {co.last_alter_id:,}",
                          font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED)
            al.pack(side="left")
            self._bg_frames += [sep, al]

        if co.starting_from:
            sep2 = tk.Label(meta, text="  ·  ", font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED)
            sep2.pack(side="left")
            fl = tk.Label(meta, text=f"Books from: {self._fmt_date_str(co.starting_from)}",
                          font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED)
            fl.pack(side="left")
            self._bg_frames += [sep2, fl]

        # ── Status badge ──────────────────────────────────
        badge_wrap = tk.Frame(outer, bg=Color.BG_CARD)
        badge_wrap.grid(row=0, column=2, rowspan=2, padx=(Spacing.LG, Spacing.MD), sticky="ns")
        self._bg_frames.append(badge_wrap)
        self._badge = StatusBadge(badge_wrap, status=co.status)
        self._badge.pack(anchor="center", expand=True)

        # ── Progress bar column ───────────────────────────
        prog_wrap = tk.Frame(outer, bg=Color.BG_CARD, width=140)
        prog_wrap.grid(row=0, column=3, rowspan=2, padx=(0, Spacing.MD), sticky="ns")
        prog_wrap.grid_propagate(False)
        self._bg_frames.append(prog_wrap)

        self._prog_lbl = tk.Label(
            prog_wrap, text="", font=Font.BODY_SM,
            bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
        )
        self._prog_lbl.pack(anchor="w")
        self._bg_frames.append(self._prog_lbl)

        self._prog_canvas = tk.Canvas(
            prog_wrap, height=6, width=130,
            bg=Color.PROGRESS_BG, highlightthickness=0, bd=0,
        )
        self._prog_canvas.pack(anchor="w", pady=(2, 0))
        self._prog_bar = self._prog_canvas.create_rectangle(
            0, 0, 0, 6, fill=Color.PROGRESS_FILL, width=0,
        )
        self._bg_frames.append(self._prog_canvas)

        # ── Action buttons ────────────────────────────────
        btn_wrap = tk.Frame(outer, bg=Color.BG_CARD)
        btn_wrap.grid(row=0, column=4, rowspan=2, sticky="ns")
        self._bg_frames.append(btn_wrap)

        is_configured = (co.status != CompanyStatus.NOT_CONFIGURED)

        self._sync_btn = tk.Button(
            btn_wrap,
            text="▶ Sync",
            font=Font.BUTTON_SM,
            bg=Color.PRIMARY if is_configured else Color.MUTED,
            fg=Color.TEXT_WHITE,
            relief="flat", bd=0,
            padx=Spacing.MD, pady=Spacing.XS,
            cursor="hand2" if is_configured else "arrow",
            command=self._on_sync_click if is_configured else None,
        )
        self._sync_btn.pack(pady=(0, Spacing.XS))

        tk.Button(
            btn_wrap,
            text="⏰",
            font=Font.BUTTON_SM,
            bg=Color.INFO_BG, fg=Color.INFO_FG,
            relief="flat", bd=0,
            padx=Spacing.SM, pady=Spacing.XS,
            cursor="hand2",
            command=self._on_sched_click,
        ).pack()

    # ─────────────────────────────────────────────────────────────────────────
    def _bind_hover(self):
        def on_enter(e):
            for w in self._bg_frames:
                try: w.configure(bg=Color.BG_CARD_HOVER)
                except Exception: pass
            self._prog_canvas.configure(bg=Color.BG_CARD_HOVER)

        def on_leave(e):
            for w in self._bg_frames:
                try: w.configure(bg=Color.BG_CARD)
                except Exception: pass
            self._prog_canvas.configure(bg=Color.BG_CARD)

        for w in [self] + self._bg_frames:
            try:
                w.bind("<Enter>", on_enter)
                w.bind("<Leave>", on_leave)
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────────────────────
    def _on_toggle(self):
        self.on_select(self.company.name, self._selected_var.get())

    def _on_sync_click(self):
        self.on_sync(self.company.name)

    def _on_sched_click(self):
        self.on_schedule(self.company.name)

    # ─────────────────────────────────────────────────────────────────────────
    #  Public update API
    # ─────────────────────────────────────────────────────────────────────────
    def update_status(self, status: str):
        self._badge.set_status(status)

    def update_progress(self, pct: float, label: str = ""):
        self._prog_lbl.configure(text=label or (f"{pct:.0f}%" if pct > 0 else ""))
        bar_w = int(130 * min(pct, 100) / 100)
        fill  = Color.PROGRESS_SUCCESS if pct >= 100 else Color.PROGRESS_FILL
        self._prog_canvas.coords(self._prog_bar, 0, 0, bar_w, 6)
        self._prog_canvas.itemconfig(self._prog_bar, fill=fill)

    def update_sync_time(self, dt):
        self._meta_lbl.configure(text=f"Last sync: {self._fmt_sync_time(dt)}")

    def set_selected(self, value: bool):
        self._selected_var.set(value)

    def is_selected(self) -> bool:
        return self._selected_var.get()

    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _fmt_sync_time(dt) -> str:
        if not dt:
            return "Never"
        try:
            if isinstance(dt, str):
                dt = datetime.fromisoformat(dt)
            return dt.strftime("%d %b %Y  %H:%M")
        except Exception:
            return str(dt)

    @staticmethod
    def _fmt_date_str(s: str) -> str:
        try:
            return datetime.strptime(str(s)[:8], "%Y%m%d").strftime("%d %b %Y")
        except Exception:
            return str(s)