"""
gui/ui_helpers.py — shared premium UI building blocks
Matches the HTML preview design system exactly.
"""
import tkinter as tk
from gui.styles import Color, Font, Spacing


def make_page_header(parent, title, subtitle=""):
    bar = tk.Frame(parent, bg=Color.BG_HEADER,
                   highlightthickness=1, highlightbackground=Color.BORDER)
    inner = tk.Frame(bar, bg=Color.BG_HEADER, padx=Spacing.XL, pady=Spacing.MD)
    inner.pack(fill="x")
    tk.Label(inner, text=title, font=("Segoe UI", 15, "bold"),
             bg=Color.BG_HEADER, fg=Color.TEXT_PRIMARY).pack(side="left")
    if subtitle:
        tk.Label(inner, text=subtitle, font=Font.BODY_SM,
                 bg=Color.BG_HEADER, fg=Color.TEXT_PLACEHOLDER).pack(side="left", padx=(14, 0))
    return bar


def make_section_card(parent, row, icon, title, subtitle="", two_col=True):
    outer = tk.Frame(parent, bg=Color.BG_CARD,
                     highlightthickness=1, highlightbackground=Color.BORDER)
    outer.grid(row=row, column=0, sticky="ew", padx=Spacing.XL, pady=(0, Spacing.MD))
    outer.columnconfigure(0, weight=1)

    hdr = tk.Frame(outer, bg=Color.BG_SECTION_HDR)
    hdr.grid(row=0, column=0, sticky="ew")
    hdr.columnconfigure(2, weight=1)

    tk.Frame(hdr, bg=Color.PRIMARY, width=4).grid(row=0, column=0, sticky="ns")
    tk.Label(hdr, text=icon, font=("Segoe UI", 13),
             bg=Color.BG_SECTION_HDR, fg=Color.PRIMARY,
             padx=12, pady=11).grid(row=0, column=1)

    tw = tk.Frame(hdr, bg=Color.BG_SECTION_HDR)
    tw.grid(row=0, column=2, sticky="w")
    tk.Label(tw, text=title, font=("Segoe UI", 11, "bold"),
             bg=Color.BG_SECTION_HDR, fg=Color.TEXT_PRIMARY,
             anchor="w").pack(anchor="w", pady=(10, 0))
    if subtitle:
        tk.Label(tw, text=subtitle, font=Font.BODY_SM,
                 bg=Color.BG_SECTION_HDR, fg=Color.TEXT_PLACEHOLDER,
                 anchor="w").pack(anchor="w", pady=(0, 10))
    else:
        tk.Frame(tw, bg=Color.BG_SECTION_HDR, height=10).pack()

    tk.Frame(outer, bg=Color.BORDER, height=1).grid(row=1, column=0, sticky="ew")

    body = tk.Frame(outer, bg=Color.BG_CARD, padx=Spacing.XL, pady=Spacing.LG)
    body.grid(row=2, column=0, sticky="ew")

    if two_col:
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)
        left  = tk.Frame(body, bg=Color.BG_CARD)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, Spacing.XL))
        right = tk.Frame(body, bg=Color.BG_CARD)
        right.grid(row=0, column=1, sticky="nsew", padx=(Spacing.XL, 0))
        return outer, left, right
    else:
        body.columnconfigure(0, weight=1)
        left = tk.Frame(body, bg=Color.BG_CARD)
        left.grid(row=0, column=0, sticky="nsew")
        return outer, left, None


def make_field(parent, label, var, hint="", secret=False, readonly=False, width=0):
    wrap = tk.Frame(parent, bg=Color.BG_CARD)
    tk.Label(wrap, text=label, font=Font.BODY_SM,
             bg=Color.BG_CARD, fg=Color.TEXT_MUTED, anchor="w").pack(fill="x", pady=(0, 4))

    border = tk.Frame(wrap, bg=Color.BORDER, padx=1, pady=1)
    if width: border.pack(anchor="w")
    else:     border.pack(fill="x")

    accent = tk.Frame(border, bg=Color.BORDER, width=3)
    accent.pack(side="left", fill="y")

    bg = Color.BG_READONLY if readonly else Color.BG_INPUT
    fg = Color.TEXT_MUTED  if readonly else Color.TEXT_PRIMARY

    entry = tk.Entry(border, textvariable=var, font=Font.BODY,
                     bg=bg, fg=fg, relief="flat", bd=0,
                     show="\u25cf" if secret else "",
                     state="normal" if not readonly else "readonly",
                     insertbackground=Color.PRIMARY,
                     width=width // 7 if width else 1)
    entry.pack(side="left", fill="both" if not width else None,
               expand=not width, ipady=7, ipadx=8)

    if not readonly:
        def on_in(e):  accent.configure(bg=Color.PRIMARY); border.configure(bg=Color.PRIMARY)
        def on_out(e): accent.configure(bg=Color.BORDER);  border.configure(bg=Color.BORDER)
        entry.bind("<FocusIn>",  on_in)
        entry.bind("<FocusOut>", on_out)

    if hint:
        tk.Label(wrap, text=hint, font=("Segoe UI", 8),
                 bg=Color.BG_CARD, fg=Color.TEXT_PLACEHOLDER,
                 anchor="w").pack(fill="x", pady=(3, 0))
    return wrap


BUTTON_STYLES = {
    "primary": {"bg": "#2C3E7A", "fg": "#FFFFFF", "hover": "#1A2B5E", "border": "#2C3E7A"},
    "outline": {"bg": "#FFFFFF", "fg": "#334155", "hover": "#F8FAFC", "border": "#E2E8F0"},
    "success": {"bg": "#ECFDF5", "fg": "#065F46", "hover": "#D1FAE5", "border": "#ECFDF5"},
    "danger":  {"bg": "#FEF2F2", "fg": "#991B1B", "hover": "#FEE2E2", "border": "#FEF2F2"},
    "ghost":   {"bg": "#F0F2F5", "fg": "#64748B", "hover": "#E2E8F0", "border": "#F0F2F5"},
    "info":    {"bg": "#EFF6FF", "fg": "#1D4ED8", "hover": "#DBEAFE", "border": "#EFF6FF"},
}


def make_button(parent, text, command, style="primary", small=False):
    c   = BUTTON_STYLES.get(style, BUTTON_STYLES["primary"])
    px  = Spacing.MD if small else Spacing.LG
    py  = Spacing.XS if small else Spacing.SM
    fnt = Font.BUTTON_SM if small else Font.BUTTON
    btn = tk.Button(parent, text=text, font=fnt,
                    bg=c["bg"], fg=c["fg"],
                    relief="flat", bd=0, padx=px, pady=py,
                    cursor="hand2", command=command,
                    highlightthickness=1, highlightbackground=c["border"],
                    activebackground=c["hover"], activeforeground=c["fg"])
    btn.bind("<Enter>", lambda e: btn.configure(bg=c["hover"]))
    btn.bind("<Leave>", lambda e: btn.configure(bg=c["bg"]))
    return btn


def make_save_bar(parent, grid_row):
    bar = tk.Frame(parent, bg=Color.BG_HEADER,
                   highlightthickness=1, highlightbackground=Color.BORDER)
    bar.grid(row=grid_row, column=0, columnspan=2, sticky="ew")
    bar.columnconfigure(0, weight=1)
    inner = tk.Frame(bar, bg=Color.BG_HEADER, padx=Spacing.XL, pady=Spacing.MD)
    inner.grid(row=0, column=0, sticky="ew")
    inner.columnconfigure(0, weight=1)
    status_lbl = tk.Label(inner, text="", font=Font.BODY_SM,
                          bg=Color.BG_HEADER, fg=Color.TEXT_MUTED)
    status_lbl.grid(row=0, column=0, sticky="w")
    btn_frame = tk.Frame(inner, bg=Color.BG_HEADER)
    btn_frame.grid(row=0, column=1)
    return bar, status_lbl, btn_frame


def make_note_box(parent, text, style="info"):
    palettes = {
        "info":    {"bg": "#EFF6FF", "fg": "#1D4ED8", "border": "#BFDBFE"},
        "warning": {"bg": "#FFFBEB", "fg": "#92400E", "border": "#FDE68A"},
        "success": {"bg": "#ECFDF5", "fg": "#065F46", "border": "#A7F3D0"},
        "danger":  {"bg": "#FEF2F2", "fg": "#991B1B", "border": "#FECACA"},
    }
    p = palettes.get(style, palettes["info"])
    box = tk.Frame(parent, bg=p["bg"],
                   highlightthickness=1, highlightbackground=p["border"],
                   padx=12, pady=10)
    tk.Label(box, text=text, font=Font.BODY_SM, bg=p["bg"], fg=p["fg"],
             anchor="w", wraplength=320, justify="left").pack(fill="x")
    return box


def make_info_box(parent, title, rows):
    box = tk.Frame(parent, bg=Color.BG_INFO_BOX,
                   highlightthickness=1, highlightbackground=Color.BORDER,
                   padx=Spacing.MD, pady=Spacing.MD)
    tk.Label(box, text=title, font=Font.BODY_SM_BOLD,
             bg=Color.BG_INFO_BOX, fg=Color.TEXT_SECONDARY).pack(anchor="w", pady=(0, Spacing.SM))
    for k, v in rows:
        row = tk.Frame(box, bg=Color.BG_INFO_BOX)
        row.pack(fill="x", pady=2)
        tk.Label(row, text=k, font=Font.BODY_SM, bg=Color.BG_INFO_BOX,
                 fg=Color.TEXT_MUTED, width=18, anchor="w").pack(side="left")
        tk.Label(row, text=v, font=Font.MONO_SM, bg=Color.BG_INFO_BOX,
                 fg=Color.TEXT_SECONDARY, anchor="w").pack(side="left")
    return box


def make_plain_card(parent, row, padx=Spacing.XL, pady=(0, Spacing.MD)):
    card = tk.Frame(parent, bg=Color.BG_CARD,
                    highlightthickness=1, highlightbackground=Color.BORDER,
                    padx=Spacing.XL, pady=Spacing.LG)
    card.grid(row=row, column=0, sticky="ew", padx=padx, pady=pady)
    card.columnconfigure(0, weight=1)
    return card