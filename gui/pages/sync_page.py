"""
gui/pages/sync_page.py
========================
Two-phase sync page.

Phase A — Options:
  ┌── 1. Sync Type ──────────────────────────────────────────────────────────┐
  │  [⚡ Incremental]  [📷 Snapshot]                                          │
  └──────────────────────────────────────────────────────────────────────────┘
  ┌── 2. Companies & Sync Configuration ────────────────────────────────────┐
  │  Global date range (snapshot only): From [...] To [...]                  │
  │                                                                          │
  │  Company          │  Date Range          │ Ld It Sl Pu CN DN Rc Pm Jn Co TB │ All │
  │  ─ Apply to all ─ │  (same as global)    │ ☑  ☑  ☑ ...                  │ [☑] │
  │  ABC Traders      │  [Global▼] 01Apr→Today│ ☑  ☑  ☑ ...                  │ [☑] │
  │  XYZ Enterprises  │  [Custom▼] 01Apr→Mar24│ ☑  ☑  ☐ ...                  │ [☐] │
  └──────────────────────────────────────────────────────────────────────────┘
  ┌── 3. Batch Mode ─────────────────────────────────────────────────────────┐
  │  [🔁 Sequential]  [⚡ Parallel]                                           │
  └──────────────────────────────────────────────────────────────────────────┘
                                [← Back]         [▶ Start Sync]

Phase B — Progress (one panel per company, live logs).
"""

import copy
import queue
import tkinter as tk
from tkinter import messagebox
from datetime import datetime, date

from gui.state      import AppState, CompanyStatus, SyncMode, VoucherSelection
from gui.styles     import Color, Font, Spacing
from gui.components.sync_progress_panel import SyncProgressPanel
from gui.controllers.sync_controller    import SyncController


# ─────────────────────────────────────────────────────────────────────────────
#  Voucher column definitions
# ─────────────────────────────────────────────────────────────────────────────
VOUCHER_COLS = [
    ("Ld",  "ledgers",       "Ledgers"),
    ("It",  "items",         "Items / Stock"),
    ("Sl",  "sales",         "Sales"),
    ("Pu",  "purchase",      "Purchase"),
    ("CN",  "credit_note",   "Credit Note"),
    ("DN",  "debit_note",    "Debit Note"),
    ("Rc",  "receipt",       "Receipt"),
    ("Pm",  "payment",       "Payment"),
    ("Jn",  "journal",       "Journal"),
    ("Co",  "contra",        "Contra"),
    ("TB",  "trial_balance", "Trial Balance"),
]
_NVC = len(VOUCHER_COLS)


# ─────────────────────────────────────────────────────────────────────────────
#  Date helpers
# ─────────────────────────────────────────────────────────────────────────────
def _parse(s: str):
    try:
        return datetime.strptime(str(s).strip()[:8], "%Y%m%d").date()
    except Exception:
        return None

def _disp(d) -> str:
    return d.strftime("%d-%b-%Y") if d else ""

def _yyyymmdd(d) -> str:
    return d.strftime("%Y%m%d") if d else ""

def _parse_display(s: str):
    if not s:
        return None
    for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None

def _fy_start(ref: date = None) -> date:
    """Return April 1 of the financial year containing ref (default today)."""
    d = ref or date.today()
    return date(d.year if d.month >= 4 else d.year - 1, 4, 1)


# ─────────────────────────────────────────────────────────────────────────────
#  Tooltip helper
# ─────────────────────────────────────────────────────────────────────────────
def _tip(widget, text: str):
    t = [None]
    def show(e):
        tw = tk.Toplevel(widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{widget.winfo_rootx()+8}+{widget.winfo_rooty()-30}")
        tk.Label(tw, text=text, font=Font.BODY_SM,
                 bg="#FFFBE6", fg="#333", relief="solid", bd=1,
                 padx=6, pady=3).pack()
        t[0] = tw
    def hide(e):
        if t[0]:
            t[0].destroy()
            t[0] = None
    widget.bind("<Enter>", show)
    widget.bind("<Leave>", hide)


# ─────────────────────────────────────────────────────────────────────────────
#  CompanySyncRow
#  One row in the configuration table — handles dates + voucher selection
# ─────────────────────────────────────────────────────────────────────────────
class CompanySyncRow(tk.Frame):
    """
    Layout (snapshot mode):
    ┌──────────────────────────────┬─────────────────────────────────────────┬───────────────────┐
    │ Company Name                 │ [Global ▼]  01-Apr-2023 → 27-Feb-2026  │ ☑ ☑ ☑ ☑ ... │ ☑ │
    │ Books from: 01-Apr-2023      │    (click to switch to Custom)          │                   │
    └──────────────────────────────┴─────────────────────────────────────────┴───────────────────┘

    When "Custom" is selected:
    ┌──────────────────────────────┬─────────────────────────────────────────┬───────────────────┐
    │ Company Name                 │ [Custom ▼]  From:[_________] To:[_____] │ ☑ ☑ ☑ ☑ ... │ ☑ │
    │ Books from: 01-Apr-2023      │                                         │                   │
    └──────────────────────────────┴─────────────────────────────────────────┴───────────────────┘
    """

    _COL_NAME  = 0
    _COL_DATE  = 1
    _COL_V0    = 2
    _COL_ALL   = 2 + _NVC

    def __init__(self, parent, company_name: str, co_state,
                 selection: VoucherSelection,
                 global_from_var: tk.StringVar,
                 global_to_var:   tk.StringVar,
                 row_bg: str,
                 show_dates: bool = True,
                 **kwargs):
        super().__init__(parent, bg=row_bg, **kwargs)
        self.company_name   = company_name
        self._co            = co_state
        self.selection      = selection
        self._gfrom         = global_from_var
        self._gto           = global_to_var
        self._bg            = row_bg
        self._show_dates    = show_dates

        # Date mode: "global" or "custom"
        self._date_mode     = "global"

        # Custom date vars — pre-filled from company state
        self._custom_from_var = tk.StringVar()
        self._custom_to_var   = tk.StringVar()
        self._prefill_custom_dates()

        # Voucher vars
        self._v_vars: dict[str, tk.BooleanVar] = {}
        self._all_var = tk.BooleanVar(value=selection.all_selected())

        self.columnconfigure(self._COL_NAME, weight=1)
        self._build()

    def _prefill_custom_dates(self):
        co = self._co
        today = date.today()
        if co:
            raw = co.starting_from or co.books_from
            fd = _parse(raw) if raw else _fy_start()
        else:
            fd = _fy_start()
        self._custom_from_var.set(_disp(fd))
        self._custom_to_var.set(_disp(today))

    # ── Build ─────────────────────────────────────────────────────────────────
    def _build(self):
        bg = self._bg

        # Col 0: Company name + info
        name_f = tk.Frame(self, bg=bg, padx=Spacing.MD)
        name_f.grid(row=0, column=self._COL_NAME, sticky="ew", ipady=8)

        co = self._co
        tk.Label(name_f, text=self.company_name,
                 font=Font.BODY_BOLD, bg=bg, fg=Color.TEXT_PRIMARY,
                 anchor="w").pack(anchor="w")

        if co and co.last_sync_time:
            sub = f"⟳ Last sync: {co.last_sync_time.strftime('%d-%b-%Y %H:%M')}"
            sub_color = Color.SUCCESS if hasattr(Color, 'SUCCESS') else "#16A34A"
        elif co and (co.starting_from or co.books_from):
            raw = co.starting_from or co.books_from
            sub = f"📅 Books from: {_disp(_parse(raw))}"
            sub_color = Color.TEXT_MUTED
        else:
            sub = "⚠  No prior sync on record"
            sub_color = Color.WARNING_FG if hasattr(Color, 'WARNING_FG') else "#D97706"
        tk.Label(name_f, text=sub, font=Font.BODY_SM,
                 bg=bg, fg=sub_color, anchor="w").pack(anchor="w")

        # Col 1: Date range widget (built separately, hidden in incremental)
        self._date_cell = tk.Frame(self, bg=bg, padx=4)
        self._date_cell.grid(row=0, column=self._COL_DATE, sticky="ew", ipady=8)
        self._build_date_cell(self._date_cell, bg)

        if not self._show_dates:
            self._date_cell.grid_remove()

        # Cols 2..12: Voucher checkboxes
        for i, (short, attr, tip) in enumerate(VOUCHER_COLS):
            var = tk.BooleanVar(value=getattr(self.selection, attr, True))
            self._v_vars[attr] = var
            cb = tk.Checkbutton(self, variable=var,
                                bg=bg, activebackground=bg,
                                relief="flat", bd=0,
                                command=lambda a=attr, v=var: self._on_v(a, v))
            cb.grid(row=0, column=self._COL_V0 + i, padx=2)
            _tip(cb, tip)

        # Col 13: "All" toggle
        all_cb = tk.Checkbutton(self, variable=self._all_var,
                                bg=bg, activebackground=bg,
                                relief="flat", bd=0, command=self._toggle_all)
        all_cb.grid(row=0, column=self._COL_ALL, padx=(4, Spacing.SM))
        _tip(all_cb, "Toggle all vouchers for this company")

    def _build_date_cell(self, parent, bg: str):
        """
        The date cell has two states rendered in one frame:
          • Global state: shows [🌐 Global ▼] pill + "01-Apr-2023 → 27-Feb-2026"
          • Custom state: shows [✏ Custom ▼] pill + From:[entry] To:[entry]
        Clicking the pill toggles between them.
        """

        # ── Mode toggle pill ──────────────────────────────────────────────────
        pill_frame = tk.Frame(parent, bg=bg)
        pill_frame.pack(anchor="w", pady=(4, 2))

        self._pill_btn = tk.Button(
            pill_frame,
            text="🌐 Global ▼",
            font=Font.BODY_SM_BOLD,
            bg="#E0E7FF",        # indigo-100
            fg="#4338CA",        # indigo-700
            relief="flat", bd=0,
            padx=8, pady=3,
            cursor="hand2",
            command=self._toggle_date_mode,
        )
        self._pill_btn.pack(side="left")
        _tip(self._pill_btn,
             "Click to switch between Global dates (same as header)\n"
             "and Custom dates specific to this company")

        self._pill_hint = tk.Label(
            pill_frame, text="same as header",
            font=Font.BODY_SM, bg=bg, fg=Color.TEXT_MUTED)
        self._pill_hint.pack(side="left", padx=(6, 0))

        # ── Global date display (read-only labels) ────────────────────────────
        self._global_disp = tk.Frame(parent, bg=bg)
        self._global_disp.pack(anchor="w")

        self._global_range_lbl = tk.Label(
            self._global_disp, text="",
            font=Font.BODY_SM, bg=bg, fg=Color.TEXT_SECONDARY)
        self._global_range_lbl.pack(anchor="w")
        self._update_global_disp()

        # Bind global var changes → refresh label
        self._gfrom.trace_add("write", lambda *_: self._update_global_disp())
        self._gto.trace_add("write",   lambda *_: self._update_global_disp())

        # ── Custom date entry fields (hidden until Custom mode) ───────────────
        self._custom_frame = tk.Frame(parent, bg=bg)
        # (not packed yet — hidden by default)

        cf1 = tk.Frame(self._custom_frame, bg=bg)
        cf1.pack(anchor="w")

        tk.Label(cf1, text="From:", font=Font.BODY_SM,
                 bg=bg, fg=Color.TEXT_SECONDARY, width=5, anchor="w").pack(side="left")
        self._from_entry = tk.Entry(
            cf1, textvariable=self._custom_from_var,
            font=Font.BODY_SM, width=12,
            bg="white", fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
            insertbackground=Color.TEXT_PRIMARY,
        )
        self._from_entry.pack(side="left")
        _tip(self._from_entry, "Custom From date for this company\nFormat: DD-Mon-YYYY  e.g. 01-Apr-2023")

        cf2 = tk.Frame(self._custom_frame, bg=bg)
        cf2.pack(anchor="w", pady=(3, 0))

        tk.Label(cf2, text="To:", font=Font.BODY_SM,
                 bg=bg, fg=Color.TEXT_SECONDARY, width=5, anchor="w").pack(side="left")
        self._to_entry = tk.Entry(
            cf2, textvariable=self._custom_to_var,
            font=Font.BODY_SM, width=12,
            bg="white", fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
            insertbackground=Color.TEXT_PRIMARY,
        )
        self._to_entry.pack(side="left")
        _tip(self._to_entry, "Custom To date for this company\nFormat: DD-Mon-YYYY  e.g. 27-Feb-2026")

    def _update_global_disp(self):
        try:
            fd = self._gfrom.get()
            td = self._gto.get()
            self._global_range_lbl.configure(text=f"{fd}  →  {td}")
        except Exception:
            pass

    def _toggle_date_mode(self):
        self._date_mode = "custom" if self._date_mode == "global" else "global"
        self._apply_date_mode()

    def _apply_date_mode(self):
        if self._date_mode == "global":
            # Show global display, hide custom entries
            self._global_disp.pack(anchor="w")
            self._custom_frame.pack_forget()
            self._pill_btn.configure(
                text="🌐 Global  ▼", bg="#E0E7FF", fg="#4338CA")
            self._pill_hint.configure(text="same as header")
        else:
            # Show custom entries, hide global display
            self._global_disp.pack_forget()
            self._custom_frame.pack(anchor="w")
            self._pill_btn.configure(
                text="✏ Custom  ▼", bg="#FEF3C7", fg="#92400E")
            self._pill_hint.configure(text="")
            # Focus from-entry for convenience
            try:
                self._from_entry.focus_set()
                self._from_entry.icursor("end")
            except Exception:
                pass

    def set_show_dates(self, show: bool):
        self._show_dates = show
        if show:
            self._date_cell.grid()
        else:
            self._date_cell.grid_remove()

    # ── Voucher callbacks ─────────────────────────────────────────────────────
    def _on_v(self, attr: str, var: tk.BooleanVar):
        setattr(self.selection, attr, var.get())
        self._all_var.set(self.selection.all_selected())

    def _toggle_all(self):
        val = self._all_var.get()
        for attr, var in self._v_vars.items():
            var.set(val)
            setattr(self.selection, attr, val)

    def set_voucher_attr(self, attr: str, val: bool):
        if attr in self._v_vars:
            self._v_vars[attr].set(val)
            setattr(self.selection, attr, val)
        self._all_var.set(self.selection.all_selected())

    # ── Date API ──────────────────────────────────────────────────────────────
    def get_from_date(self, fallback: str) -> str:
        if not self._show_dates or self._date_mode == "global":
            return fallback
        d = _parse_display(self._custom_from_var.get())
        return _yyyymmdd(d) if d else fallback

    def get_to_date(self, fallback: str) -> str:
        if not self._show_dates or self._date_mode == "global":
            return fallback
        d = _parse_display(self._custom_to_var.get())
        return _yyyymmdd(d) if d else fallback

    def validate_dates(self) -> tuple[bool, str]:
        if not self._show_dates or self._date_mode == "global":
            return True, ""
        fd_raw = self._custom_from_var.get().strip()
        td_raw = self._custom_to_var.get().strip()
        fd = _parse_display(fd_raw)
        td = _parse_display(td_raw)
        if not fd:
            return False, (f"'{self.company_name}':\nFrom date '{fd_raw}' is invalid.\n"
                           "Use format: DD-Mon-YYYY  e.g. 01-Apr-2023")
        if not td:
            return False, (f"'{self.company_name}':\nTo date '{td_raw}' is invalid.\n"
                           "Use format: DD-Mon-YYYY  e.g. 27-Feb-2026")
        if fd > td:
            return False, (f"'{self.company_name}':\nFrom date ({_disp(fd)}) "
                           f"cannot be after To date ({_disp(td)}).")
        return True, ""

    @property
    def is_custom(self) -> bool:
        return self._date_mode == "custom"


# ─────────────────────────────────────────────────────────────────────────────
#  SyncPage
# ─────────────────────────────────────────────────────────────────────────────
class SyncPage(tk.Frame):

    def __init__(self, parent, state: AppState, navigate, app):
        super().__init__(parent, bg=Color.BG_ROOT)
        self.state    = state
        self.navigate = navigate
        self.app      = app

        self._sync_queue  = queue.Queue()
        self._controller: SyncController = None
        self._panels:     dict[str, SyncProgressPanel] = {}

        self._per_company_vouchers: dict[str, VoucherSelection] = {}
        self._company_rows:         dict[str, CompanySyncRow]   = {}

        self._global_col_vars: dict[str, tk.BooleanVar] = {}
        self._global_all_var = tk.BooleanVar(value=True)

        today = date.today()
        self._global_from_var = tk.StringVar(value=_disp(_fy_start()))
        self._global_to_var   = tk.StringVar(value=_disp(today))

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self._build_options_view()
        self._build_progress_view()
        self._show_options()

    # ─────────────────────────────────────────────────────────────────────────
    #  PHASE A — Options
    # ─────────────────────────────────────────────────────────────────────────
    def _build_options_view(self):
        self._opt_frame = tk.Frame(self, bg=Color.BG_ROOT)
        self._opt_frame.grid(row=0, column=0, sticky="nsew")
        self._opt_frame.columnconfigure(0, weight=1)
        self._opt_frame.rowconfigure(0, weight=1)

        canvas = tk.Canvas(self._opt_frame, bg=Color.BG_ROOT,
                           highlightthickness=0, bd=0)
        canvas.grid(row=0, column=0, sticky="nsew")
        vsb = tk.Scrollbar(self._opt_frame, orient="vertical", command=canvas.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vsb.set)

        inner = tk.Frame(canvas, bg=Color.BG_ROOT)
        inner.columnconfigure(0, weight=1)
        cw = canvas.create_window((0, 0), window=inner, anchor="nw")

        inner.bind("<Configure>",
                   lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfig(cw, width=e.width))
        canvas.bind_all("<MouseWheel>",
                        lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        self._build_content(inner)

    def _build_content(self, parent):
        # ── SECTION 1: Sync Type ──────────────────────────────────────────────
        s1_body = self._section(parent, "1.  Sync Type", row=0)
        self._sync_mode_var = tk.StringVar(value=SyncMode.INCREMENTAL)

        mode_row = tk.Frame(s1_body, bg=Color.BG_CARD)
        mode_row.pack(fill="x", pady=(4, 0))

        self._mode_cards = {}
        for val, icon, title, desc1, desc2 in [
            (SyncMode.INCREMENTAL, "⚡", "Incremental  (CDC)",
             "Only syncs records changed since last run — uses alter_id.",
             "Best for scheduled / daily runs. Extremely fast."),
            (SyncMode.SNAPSHOT, "📷", "Initial Snapshot  (full range)",
             "Fetches all records within the selected date range.",
             "Use for first-time setup or historical backfill."),
        ]:
            card = tk.Frame(mode_row, cursor="hand2",
                            highlightthickness=2,
                            highlightbackground=Color.BORDER,
                            padx=Spacing.LG, pady=Spacing.MD)
            card.pack(side="left", fill="both", expand=True,
                      padx=(0, Spacing.MD), pady=2)
            card.bind("<Button-1>", lambda e, v=val: self._set_mode(v))

            rb_row = tk.Frame(card)
            rb_row.pack(anchor="w")
            tk.Radiobutton(
                rb_row, text=f"{icon}  {title}",
                variable=self._sync_mode_var, value=val,
                font=Font.BODY_BOLD,
                activebackground=card.cget("bg"),
                command=lambda v=val: self._set_mode(v),
            ).pack(side="left")
            tk.Label(card, text=desc1, font=Font.BODY_SM,
                     fg=Color.TEXT_SECONDARY, anchor="w").pack(anchor="w", pady=(6, 0))
            tk.Label(card, text=desc2, font=Font.BODY_SM,
                     fg=Color.TEXT_MUTED, anchor="w").pack(anchor="w")

            self._mode_cards[val] = card

        # ── SECTION 2: Companies & Config ─────────────────────────────────────
        s2_body = self._section(parent, "2.  Companies & Sync Configuration", row=1)

        # Global date strip (snapshot only)
        self._global_date_strip = tk.Frame(s2_body, bg="#EEF2FF",
                                           padx=Spacing.LG, pady=8,
                                           highlightthickness=1,
                                           highlightbackground="#C7D2FE")
        self._global_date_strip.pack(fill="x", pady=(0, Spacing.SM))

        gd_left = tk.Frame(self._global_date_strip, bg="#EEF2FF")
        gd_left.pack(side="left", padx=(0, Spacing.XL))
        tk.Label(gd_left, text="🌐  Global Date Range",
                 font=Font.LABEL_BOLD, bg="#EEF2FF", fg="#4338CA").pack(anchor="w")
        tk.Label(gd_left,
                 text="Applied to all companies unless they switch to Custom ↓",
                 font=Font.BODY_SM, bg="#EEF2FF", fg="#6366F1").pack(anchor="w")

        gd_right = tk.Frame(self._global_date_strip, bg="#EEF2FF")
        gd_right.pack(side="left")

        for lbl, var, tip in [
            ("From:", self._global_from_var,
             "Global From date — auto-filled from earliest company books_from\nFormat: DD-Mon-YYYY"),
            ("To:",   self._global_to_var,
             "Global To date — defaults to today\nFormat: DD-Mon-YYYY"),
        ]:
            rf = tk.Frame(gd_right, bg="#EEF2FF")
            rf.pack(side="left", padx=(0, Spacing.LG))
            tk.Label(rf, text=lbl, font=Font.BODY_BOLD,
                     bg="#EEF2FF", fg="#4338CA").pack(side="left")
            e = tk.Entry(rf, textvariable=var, font=Font.BODY, width=13,
                         bg="white", fg="#1E1B4B",
                         relief="solid", bd=1,
                         insertbackground="#4338CA")
            e.pack(side="left", padx=(Spacing.SM, 0))
            _tip(e, tip)

        tk.Label(self._global_date_strip,
                 text="Format: DD-Mon-YYYY", font=Font.BODY_SM,
                 bg="#EEF2FF", fg="#818CF8").pack(side="left", padx=Spacing.MD)

        # Table header + global voucher toggles
        self._tbl_frame = tk.Frame(s2_body, bg=Color.BG_CARD)
        self._tbl_frame.pack(fill="x")
        self._tbl_frame.columnconfigure(0, weight=1)
        self._build_table_header(self._tbl_frame)

        # Table body (rebuilt in _rebuild_rows)
        self._tbl_body = tk.Frame(s2_body, bg=Color.BG_CARD)
        self._tbl_body.pack(fill="x")
        self._tbl_body.columnconfigure(0, weight=1)

        # ── SECTION 3: Batch Mode ─────────────────────────────────────────────
        s3_body = self._section(parent, "3.  Batch Mode", row=2)
        self._batch_var = tk.BooleanVar(value=True)

        batch_row = tk.Frame(s3_body, bg=Color.BG_CARD)
        batch_row.pack(fill="x", pady=(4, 0))

        for val, icon, title, hint in [
            (True,  "🔁", "Sequential  (one at a time)",
             "Safer. Lower Tally load. Recommended for 1–5 companies."),
            (False, "⚡", "Parallel  (all simultaneously)",
             "Faster. Higher Tally load. Use with caution."),
        ]:
            bf = tk.Frame(batch_row, cursor="hand2",
                          highlightthickness=1, highlightbackground=Color.BORDER,
                          padx=Spacing.LG, pady=Spacing.SM)
            bf.pack(side="left", padx=(0, Spacing.MD))
            bf.bind("<Button-1>", lambda e, v=val: self._batch_var.set(v))
            tk.Radiobutton(bf, text=f"{icon}  {title}",
                           variable=self._batch_var, value=val,
                           font=Font.BODY_BOLD,
                           activebackground=bf.cget("bg"),
                           command=lambda v=val: self._batch_var.set(v)).pack(anchor="w")
            tk.Label(bf, text=hint, font=Font.BODY_SM,
                     fg=Color.TEXT_MUTED).pack(anchor="w")

        # ── Action buttons ────────────────────────────────────────────────────
        btns = tk.Frame(parent, bg=Color.BG_ROOT, pady=Spacing.LG)
        btns.grid(row=3, column=0, sticky="ew", padx=Spacing.XL)

        tk.Button(btns, text="← Back to Companies",
                  font=Font.BUTTON, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
                  relief="solid", bd=1, padx=Spacing.LG, pady=Spacing.SM,
                  cursor="hand2",
                  command=lambda: self.navigate("home")).pack(side="left")

        self._start_btn = tk.Button(
            btns, text="▶  Start Sync",
            font=Font.BUTTON, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.XL, pady=Spacing.SM,
            cursor="hand2", command=self._on_start_sync)
        self._start_btn.pack(side="right")

        self._set_mode(SyncMode.INCREMENTAL, init=True)

    # ── Table header ──────────────────────────────────────────────────────────
    def _build_table_header(self, parent):
        # Header row
        hdr = tk.Frame(parent, bg=Color.BG_TABLE_HEADER)
        hdr.grid(row=0, column=0, sticky="ew")
        hdr.columnconfigure(CompanySyncRow._COL_NAME, weight=1)

        tk.Label(hdr, text="Company",
                 font=Font.BODY_SM_BOLD, bg=Color.BG_TABLE_HEADER,
                 fg=Color.TEXT_SECONDARY, pady=7,
                 padx=Spacing.MD, anchor="w").grid(
                     row=0, column=CompanySyncRow._COL_NAME, sticky="ew")

        self._hdr_date_lbl = tk.Label(hdr, text="Date Range",
                 font=Font.BODY_SM_BOLD, bg=Color.BG_TABLE_HEADER,
                 fg=Color.TEXT_SECONDARY, pady=7, padx=8,
                 anchor="w", width=22)
        self._hdr_date_lbl.grid(row=0, column=CompanySyncRow._COL_DATE)

        for i, (short, attr, tip) in enumerate(VOUCHER_COLS):
            lbl = tk.Label(hdr, text=short,
                           font=Font.BODY_SM_BOLD, bg=Color.BG_TABLE_HEADER,
                           fg=Color.TEXT_SECONDARY, pady=7,
                           width=3, anchor="center")
            lbl.grid(row=0, column=CompanySyncRow._COL_V0 + i, padx=2)
            _tip(lbl, tip)

        tk.Label(hdr, text="All",
                 font=Font.BODY_SM_BOLD, bg=Color.BG_TABLE_HEADER,
                 fg=Color.TEXT_SECONDARY, pady=7,
                 width=4, anchor="center",
                 padx=Spacing.SM).grid(row=0, column=CompanySyncRow._COL_ALL)

        tk.Frame(parent, bg=Color.BORDER, height=1).grid(
            row=1, column=0, sticky="ew")

        # Global toggle row (indigo tint)
        GTBG = "#EEF2FF"
        gf = tk.Frame(parent, bg=GTBG)
        gf.grid(row=2, column=0, sticky="ew")
        gf.columnconfigure(CompanySyncRow._COL_NAME, weight=1)

        tk.Label(gf, text="▸  Apply to ALL companies:",
                 font=Font.BODY_SM_BOLD, bg=GTBG, fg="#4338CA",
                 pady=6, padx=Spacing.MD, anchor="w").grid(
                     row=0, column=CompanySyncRow._COL_NAME, sticky="w")

        self._gtbl_date_lbl = tk.Label(gf, text="(dates set above)",
                 font=Font.BODY_SM, bg=GTBG, fg="#818CF8",
                 pady=6, padx=8, anchor="w", width=22)
        self._gtbl_date_lbl.grid(row=0, column=CompanySyncRow._COL_DATE)

        for i, (short, attr, tip) in enumerate(VOUCHER_COLS):
            var = tk.BooleanVar(value=True)
            self._global_col_vars[attr] = var
            cb = tk.Checkbutton(gf, variable=var, bg=GTBG,
                                activebackground=GTBG,
                                relief="flat", bd=0,
                                command=lambda a=attr, v=var: self._gcol_toggle(a, v))
            cb.grid(row=0, column=CompanySyncRow._COL_V0 + i, padx=2)
            _tip(cb, f"Toggle '{tip}' for ALL companies")

        all_cb = tk.Checkbutton(gf, variable=self._global_all_var,
                                bg=GTBG, activebackground=GTBG,
                                relief="flat", bd=0, command=self._gall_toggle)
        all_cb.grid(row=0, column=CompanySyncRow._COL_ALL, padx=Spacing.SM)
        _tip(all_cb, "Toggle ALL voucher types for ALL companies")

        tk.Frame(parent, bg=Color.BORDER_LIGHT, height=1).grid(
            row=3, column=0, sticky="ew")

    # ─────────────────────────────────────────────────────────────────────────
    #  Mode switching
    # ─────────────────────────────────────────────────────────────────────────
    def _set_mode(self, mode: str, init: bool = False):
        self._sync_mode_var.set(mode)
        is_snap = (mode == SyncMode.SNAPSHOT)

        for val, card in self._mode_cards.items():
            active = (val == mode)
            card.configure(
                bg="#F0F4FF" if active else Color.BG_ROOT,
                highlightbackground=Color.PRIMARY if active else Color.BORDER,
                highlightthickness=2 if active else 1,
            )
            # Update radio bg to match card
            for child in card.winfo_children():
                try:
                    child.configure(bg=card.cget("bg"))
                    for cc in child.winfo_children():
                        try: cc.configure(bg=card.cget("bg"))
                        except: pass
                except: pass

        # Global date strip
        if is_snap:
            self._global_date_strip.pack(fill="x", pady=(0, Spacing.SM),
                                         before=self._tbl_frame)
        else:
            self._global_date_strip.pack_forget()

        # Date column header
        if is_snap:
            self._hdr_date_lbl.grid()
            self._gtbl_date_lbl.grid()
        else:
            self._hdr_date_lbl.grid_remove()
            self._gtbl_date_lbl.grid_remove()

        # Per-company rows
        for row in self._company_rows.values():
            row.set_show_dates(is_snap)

    # ─────────────────────────────────────────────────────────────────────────
    #  Rebuild per-company rows
    # ─────────────────────────────────────────────────────────────────────────
    def _rebuild_rows(self):
        for w in self._tbl_body.winfo_children():
            w.destroy()
        self._company_rows.clear()

        selected = self.state.selected_companies
        if not selected:
            tk.Label(self._tbl_body,
                     text="No companies selected — go back and select companies first.",
                     font=Font.BODY_SM, bg=Color.BG_CARD,
                     fg=Color.TEXT_MUTED, pady=20, padx=Spacing.MD).pack(anchor="w")
            return

        # Auto-fill global from = earliest company starting_from
        earliest = None
        for name in selected:
            co = self.state.get_company(name)
            if co:
                d = _parse(co.starting_from or co.books_from)
                if d and (earliest is None or d < earliest):
                    earliest = d
        if earliest:
            self._global_from_var.set(_disp(earliest))
        self._global_to_var.set(_disp(date.today()))

        is_snap = (self._sync_mode_var.get() == SyncMode.SNAPSHOT)

        for i, name in enumerate(selected):
            if name not in self._per_company_vouchers:
                self._per_company_vouchers[name] = copy.deepcopy(
                    self.state.voucher_selection)

            sel = self._per_company_vouchers[name]
            co  = self.state.get_company(name)
            bg  = Color.BG_TABLE_ODD if i % 2 == 0 else Color.BG_TABLE_EVEN

            row = CompanySyncRow(
                self._tbl_body,
                company_name=name, co_state=co,
                selection=sel,
                global_from_var=self._global_from_var,
                global_to_var=self._global_to_var,
                row_bg=bg,
                show_dates=is_snap,
            )
            row.columnconfigure(CompanySyncRow._COL_NAME, weight=1)
            row.pack(fill="x")
            self._company_rows[name] = row

            tk.Frame(self._tbl_body, bg=Color.BORDER_LIGHT,
                     height=1).pack(fill="x")

        # Footer
        n = len(selected)
        mode_hint = (
            "Each company uses the global date range unless you switch it to Custom ↑"
            if is_snap else
            "Incremental: only records changed since last sync will be fetched (alter_id)."
        )
        tk.Label(self._tbl_body,
                 text=f"ℹ  {n} {'company' if n==1 else 'companies'} selected  —  {mode_hint}",
                 font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                 padx=Spacing.MD, pady=8, anchor="w",
                 wraplength=900, justify="left").pack(anchor="w")

        self._sync_global_state()

    # ─────────────────────────────────────────────────────────────────────────
    #  Global voucher toggles
    # ─────────────────────────────────────────────────────────────────────────
    def _gcol_toggle(self, attr: str, var: tk.BooleanVar):
        val = var.get()
        for row in self._company_rows.values():
            row.set_voucher_attr(attr, val)
        self._global_all_var.set(all(v.get() for v in self._global_col_vars.values()))

    def _gall_toggle(self):
        val = self._global_all_var.get()
        for col_var in self._global_col_vars.values():
            col_var.set(val)
        for attr in self._global_col_vars:
            for row in self._company_rows.values():
                row.set_voucher_attr(attr, val)

    def _sync_global_state(self):
        for attr, col_var in self._global_col_vars.items():
            all_on = all(
                getattr(self._per_company_vouchers.get(n, VoucherSelection()), attr, True)
                for n in self.state.selected_companies
            )
            col_var.set(all_on)
        self._global_all_var.set(all(v.get() for v in self._global_col_vars.values()))

    # ─────────────────────────────────────────────────────────────────────────
    #  Section builder
    # ─────────────────────────────────────────────────────────────────────────
    def _section(self, parent, title: str, row: int) -> tk.Frame:
        outer = tk.Frame(parent, bg=Color.BG_CARD,
                         highlightthickness=1, highlightbackground=Color.BORDER)
        outer.grid(row=row, column=0, sticky="ew",
                   padx=Spacing.XL, pady=(0, Spacing.MD))
        outer.columnconfigure(0, weight=1)

        tk.Frame(outer, bg="#4338CA", height=3).pack(fill="x")   # accent top bar

        title_bar = tk.Frame(outer, bg=Color.BG_TABLE_HEADER, padx=Spacing.LG, pady=8)
        title_bar.pack(fill="x")
        tk.Label(title_bar, text=title, font=Font.LABEL_BOLD,
                 bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_PRIMARY,
                 anchor="w").pack(anchor="w")

        body = tk.Frame(outer, bg=Color.BG_CARD, padx=Spacing.LG, pady=Spacing.MD)
        body.pack(fill="x")
        body.columnconfigure(0, weight=1)
        return body

    # ─────────────────────────────────────────────────────────────────────────
    #  PHASE B — Progress view
    # ─────────────────────────────────────────────────────────────────────────
    def _build_progress_view(self):
        self._prog_frame = tk.Frame(self, bg=Color.BG_ROOT)
        self._prog_frame.grid(row=0, column=0, sticky="nsew")
        self._prog_frame.columnconfigure(0, weight=1)
        self._prog_frame.rowconfigure(1, weight=1)

        top = tk.Frame(self._prog_frame, bg=Color.BG_ROOT, pady=Spacing.MD)
        top.grid(row=0, column=0, sticky="ew", padx=Spacing.XL)
        top.columnconfigure(0, weight=1)

        self._prog_title = tk.Label(top, text="Sync in Progress...",
                                    font=Font.HEADING_4, bg=Color.BG_ROOT,
                                    fg=Color.TEXT_PRIMARY, anchor="w")
        self._prog_title.grid(row=0, column=0, sticky="w")

        self._cancel_btn = tk.Button(top, text="✖  Cancel All",
                                     font=Font.BUTTON, bg=Color.DANGER_BG,
                                     fg=Color.DANGER_FG, relief="flat", bd=0,
                                     padx=Spacing.LG, pady=Spacing.SM,
                                     cursor="hand2", command=self._on_cancel)
        self._cancel_btn.grid(row=0, column=1)

        canvas = tk.Canvas(self._prog_frame, bg=Color.BG_ROOT,
                           highlightthickness=0, bd=0)
        canvas.grid(row=1, column=0, sticky="nsew",
                    padx=Spacing.XL, pady=Spacing.MD)
        vsb = tk.Scrollbar(self._prog_frame, orient="vertical", command=canvas.yview)
        vsb.grid(row=1, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vsb.set)

        self._panels_frame = tk.Frame(canvas, bg=Color.BG_ROOT)
        pw = canvas.create_window((0, 0), window=self._panels_frame, anchor="nw")
        self._panels_frame.columnconfigure(0, weight=1)
        self._panels_frame.bind("<Configure>",
                                lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(pw, width=e.width))

        self._done_btn = tk.Button(
            self._prog_frame, text="✓  Done — Back to Companies",
            font=Font.BUTTON, bg=Color.SUCCESS, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.XL, pady=Spacing.MD,
            cursor="hand2", command=lambda: self.navigate("home"))

    def _show_options(self):
        self._prog_frame.lower()
        self._opt_frame.tkraise()

    def _show_progress(self):
        self._opt_frame.lower()
        self._prog_frame.tkraise()

    # ─────────────────────────────────────────────────────────────────────────
    #  Start sync
    # ─────────────────────────────────────────────────────────────────────────
    def _on_start_sync(self):
        companies = list(self.state.selected_companies)
        if not companies:
            messagebox.showwarning("No Companies",
                "Please go back and select at least one company.")
            self.navigate("home")
            return

        if self.state.sync_active:
            messagebox.showwarning("Already Running", "A sync is already in progress.")
            return

        # Validate: ≥1 voucher per company
        no_v = [n for n in companies
                if not self._per_company_vouchers.get(n, VoucherSelection()).selected_types()]
        if no_v:
            messagebox.showwarning("No Vouchers Selected",
                "Please select at least one voucher type for:\n\n  • " +
                "\n  • ".join(no_v[:5]))
            return

        mode    = self._sync_mode_var.get()
        is_snap = (mode == SyncMode.SNAPSHOT)

        # Validate global dates
        global_from = ""
        global_to   = datetime.now().strftime("%Y%m%d")

        if is_snap:
            gf = _parse_display(self._global_from_var.get())
            gt = _parse_display(self._global_to_var.get())
            if not gf:
                messagebox.showerror("Invalid Date",
                    "Global From date is missing or invalid.\nFormat: DD-Mon-YYYY")
                return
            if not gt:
                messagebox.showerror("Invalid Date",
                    "Global To date is missing or invalid.\nFormat: DD-Mon-YYYY")
                return
            if gf > gt:
                messagebox.showerror("Invalid Date Range",
                    "Global From date cannot be after To date.")
                return
            global_from = _yyyymmdd(gf)
            global_to   = _yyyymmdd(gt)

            # Validate custom rows
            for name, row in self._company_rows.items():
                ok, err = row.validate_dates()
                if not ok:
                    messagebox.showerror("Invalid Date", err)
                    return

        # Collect per-company config
        vouchers_map:   dict[str, VoucherSelection] = {}
        from_dates_map: dict[str, str] = {}
        to_dates_map:   dict[str, str] = {}

        for name in companies:
            vouchers_map[name] = copy.deepcopy(
                self._per_company_vouchers.get(name, VoucherSelection()))
            row = self._company_rows.get(name)
            if row and is_snap:
                from_dates_map[name] = row.get_from_date(global_from)
                to_dates_map[name]   = row.get_to_date(global_to)
            else:
                from_dates_map[name] = None
                to_dates_map[name]   = global_to

        # Build progress panels
        for w in self._panels_frame.winfo_children():
            w.destroy()
        self._panels.clear()

        for i, name in enumerate(companies):
            panel = SyncProgressPanel(self._panels_frame, company_name=name,
                                      on_cancel=self._on_cancel_one)
            panel.grid(row=i, column=0, sticky="ew", pady=(0, Spacing.SM))
            self._panels[name] = panel
            if i > 0 and self.state.batch_sequential:
                panel.mark_waiting()

        self._done_btn.grid_remove()
        self._prog_title.configure(text="Sync in Progress...")
        self._cancel_btn.configure(state="normal", text="✖  Cancel All")
        self._show_progress()

        while not self._sync_queue.empty():
            try: self._sync_queue.get_nowait()
            except queue.Empty: break

        self.state.sync_mode        = mode
        self.state.sync_from_date   = global_from or None
        self.state.sync_to_date     = global_to
        self.state.batch_sequential = self._batch_var.get()

        self._controller = SyncController(
            state          = self.state,
            out_queue      = self._sync_queue,
            companies      = companies,
            sync_mode      = mode,
            from_date      = global_from or None,
            to_date        = global_to,
            vouchers       = vouchers_map,
            from_dates_map = from_dates_map,
            to_dates_map   = to_dates_map,
            sequential     = self.state.batch_sequential,
        )
        self._controller.start()
        self._poll_sync_queue()

    # ─────────────────────────────────────────────────────────────────────────
    #  Queue polling
    # ─────────────────────────────────────────────────────────────────────────
    def _poll_sync_queue(self):
        try:
            while True:
                self._handle_msg(self._sync_queue.get_nowait())
        except queue.Empty:
            pass
        if self.state.sync_active:
            self.after(100, self._poll_sync_queue)

    def _handle_msg(self, msg: tuple):
        ev = msg[0]
        if ev == "log":
            _, co, line, level = msg
            p = self._panels.get(co)
            if p:
                p.append_log(f"{datetime.now().strftime('%H:%M:%S')}  {line}", level)
            self.app.post("sync_log", f"[{co}] {line}")
        elif ev == "progress":
            _, co, pct, lbl = msg
            p = self._panels.get(co)
            if p: p.set_progress(pct, lbl)
            self.state.set_company_progress(co, pct, lbl)
        elif ev == "status":
            _, co, status = msg
            p = self._panels.get(co)
            if p: p.set_status(status)
            self.state.set_company_status(co, status)
        elif ev == "done":
            _, co, ok = msg
            p = self._panels.get(co)
            if p: p.mark_done(ok)
        elif ev == "all_done":
            self._on_all_done()

    def _on_cancel(self):
        if self._controller:
            self._controller.cancel()
        self._cancel_btn.configure(state="disabled", text="Cancelling...")

    def _on_cancel_one(self, _):
        self._on_cancel()

    def _on_all_done(self):
        self.state.sync_active = False
        self.state.emit("sync_finished")
        self._prog_title.configure(text="Sync Complete ✓")
        self._cancel_btn.configure(state="disabled", text="✖  Cancel All")
        self._done_btn.grid(row=len(self._panels), column=0,
                            padx=Spacing.XL, pady=Spacing.LG, sticky="e")

    # ─────────────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    def on_show(self):
        if self.state.sync_active:
            self._show_progress()
            return
        if hasattr(self.state, 'sync_mode') and self.state.sync_mode:
            self._sync_mode_var.set(self.state.sync_mode)
        self._rebuild_rows()
        self._set_mode(self._sync_mode_var.get(), init=True)
        self._show_options()