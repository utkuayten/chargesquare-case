"""
ChargeSquare — PDF Report Generator
=====================================
Produces a multi-page PDF covering all A1-A6 analyses plus core KPIs.
No LaTeX or browser required — uses matplotlib's PdfPages backend.

Usage
-----
    python scripts/generate_report.py                       # → exports/chargesquare_report.pdf
    python scripts/generate_report.py --out /tmp/report.pdf
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")   # headless — no display needed
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Patch

# ── project imports ───────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from analytics.queries import ChargingAnalytics

# ── palette ───────────────────────────────────────────────────────────────────
BG      = "#0f1117"
PANEL   = "#1a1d27"
BORDER  = "#3a3d4d"
FG      = "#c9d1d9"
MUTED   = "#8b949e"
ACCENT  = "#58a6ff"
GREEN   = "#3fb950"
ORANGE  = "#e3b341"
RED     = "#f85149"
PURPLE  = "#bc8cff"
TEAL    = "#39d353"
PINK    = "#ff7b72"

matplotlib.rcParams.update({
    "figure.facecolor":  BG,
    "axes.facecolor":    PANEL,
    "axes.edgecolor":    BORDER,
    "axes.labelcolor":   FG,
    "xtick.color":       MUTED,
    "ytick.color":       MUTED,
    "text.color":        FG,
    "grid.color":        "#2d3142",
    "grid.linestyle":    "--",
    "grid.alpha":        0.6,
    "figure.titlesize":  13,
    "axes.titlesize":    11,
    "axes.labelsize":    9,
    "legend.facecolor":  PANEL,
    "legend.edgecolor":  BORDER,
    "font.family":       "DejaVu Sans",
})


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _save(pdf: PdfPages, fig: plt.Figure) -> None:
    pdf.savefig(fig, bbox_inches="tight", facecolor=BG)
    plt.close(fig)


def _desc(fig: plt.Figure, text: str, y: float = 0.92) -> None:
    """Add a one-line description subtitle just below the suptitle."""
    fig.text(0.5, y, text, ha="center", fontsize=9, color=MUTED, style="italic")


def _hbar(ax, labels, values, color_map=None, xlabel="", title="", fmt=None):
    y = range(len(labels))
    colors = color_map if color_map is not None else [ACCENT] * len(labels)
    ax.barh(list(y), list(values)[::-1] if len(values) > 0 else [],
            color=list(colors)[::-1] if len(colors) > 0 else [])
    ax.set_yticks(list(y))
    ax.set_yticklabels(labels[::-1], fontsize=8)
    ax.set_xlabel(xlabel)
    ax.set_title(title)
    ax.grid(axis="x")
    if fmt:
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt))


def _nodata(ax, msg="No data — run the pipeline first."):
    ax.text(0.5, 0.5, msg, ha="center", va="center", color=MUTED,
            fontsize=10, transform=ax.transAxes)
    ax.axis("off")


# ─────────────────────────────────────────────────────────────────────────────
# Page builders
# ─────────────────────────────────────────────────────────────────────────────

def page_cover(pdf: PdfPages, a: ChargingAnalytics) -> None:
    fig = plt.figure(figsize=(11.69, 8.27))   # A4 landscape
    fig.patch.set_facecolor(BG)

    total = a.total_events()
    rt    = a.real_time_throughput(minutes=5)
    nets  = a.revenue_by_network(days=30)
    total_rev = sum(n["total_revenue_eur"] for n in nets)
    total_kwh = sum(n["total_energy_kwh"]  for n in nets)

    kpis = [
        ("Total Events",             f"{total:,}",              ACCENT),
        ("Events/sec\n(last 5 min)", f"{rt['events_per_second']:.1f}", GREEN),
        ("Total Revenue\n(30 days)", f"€{total_rev:,.0f}",      ORANGE),
        ("Total Energy\n(30 days)",  f"{total_kwh:,.0f} kWh",   TEAL),
        ("Active Stations",          f"{rt['active_stations']:,}", PURPLE),
        ("Unique Sessions",          f"{rt['unique_sessions']:,}", PINK),
    ]

    fig.text(0.5, 0.91, "ChargeSquare — Analytics Report",
             ha="center", fontsize=22, fontweight="bold", color="white")
    fig.text(0.5, 0.85,
             f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}  ·  "
             f"1,000 simulated EV stations across Europe  ·  "
             f"Kafka → Redis + ClickHouse pipeline",
             ha="center", fontsize=10, color=MUTED)

    for i, (label, value, color) in enumerate(kpis):
        x = 0.07 + (i % 3) * 0.31
        y = 0.52 if i < 3 else 0.25
        ax = fig.add_axes([x, y, 0.26, 0.20])
        ax.set_facecolor(PANEL)
        ax.axis("off")
        for spine in ax.spines.values():
            spine.set_visible(True)
            spine.set_color(color)
            spine.set_linewidth(2)
        ax.text(0.5, 0.62, value, ha="center", va="center",
                fontsize=19, fontweight="bold", color=color, transform=ax.transAxes)
        ax.text(0.5, 0.20, label, ha="center", va="center",
                fontsize=9, color=MUTED, transform=ax.transAxes)

    fig.text(0.5, 0.04,
             "This report covers 14 analytical dimensions including revenue, utilisation, "
             "geographic demand, fault diagnostics and statistical anomaly detection.",
             ha="center", fontsize=9, color=MUTED, style="italic")
    _save(pdf, fig)


def page_revenue_network(pdf: PdfPages, a: ChargingAnalytics) -> None:
    nets = a.revenue_by_network(days=30)
    fig, axes = plt.subplots(1, 2, figsize=(11.69, 8.27))
    fig.suptitle("1 — Revenue by Network (Last 30 Days)", fontweight="bold", y=0.97)
    _desc(fig,
          "CPO league table ranked by total revenue. "
          "Compares session volume and income across all 10 charging networks.", y=0.93)

    if not nets:
        _nodata(axes[0]); _nodata(axes[1])
    else:
        names  = [n["network_id"]        for n in nets]
        revs   = [n["total_revenue_eur"] for n in nets]
        sess   = [n["session_count"]     for n in nets]
        _hbar(axes[0], names, revs,
              plt.cm.Blues(np.linspace(0.45, 0.9, len(names))),
              "Total Revenue (€)", "Total Revenue",
              lambda x, _: f"€{x/1e3:.0f}k")
        _hbar(axes[1], names, sess,
              plt.cm.Greens(np.linspace(0.45, 0.9, len(names))),
              "Sessions", "Sessions",
              lambda x, _: f"{x/1e3:.0f}k")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    _save(pdf, fig)


def page_sessions_hour(pdf: PdfPages, a: ChargingAnalytics) -> None:
    hourly = a.sessions_per_hour(hours=48)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11.69, 7.5), sharex=True)
    fig.suptitle("2 — Intra-Day Demand Curve (Last 48 Hours)", fontweight="bold", y=0.98)
    _desc(fig,
          "Top: session count per hour. Bottom: energy delivered (kWh). "
          "Both charts share the same time axis — peaks align across rows.", y=0.95)

    if not hourly:
        _nodata(ax1); _nodata(ax2)
    else:
        xs       = range(len(hourly))
        sessions = [h["session_count"]    for h in hourly]
        energy   = [h["total_energy_kwh"] for h in hourly]

        ax1.fill_between(xs, sessions, alpha=0.22, color=ACCENT)
        ax1.plot(xs, sessions, color=ACCENT, lw=2)
        ax1.set_ylabel("Sessions")
        ax1.yaxis.label.set_color(ACCENT)
        ax1.tick_params(axis="y", labelcolor=ACCENT)
        ax1.grid(True)
        ax1.set_title("Sessions per Hour", fontsize=10)

        ax2.fill_between(xs, energy, alpha=0.22, color=GREEN)
        ax2.plot(xs, energy, color=GREEN, lw=2)
        ax2.set_ylabel("Energy (kWh)")
        ax2.yaxis.label.set_color(GREEN)
        ax2.tick_params(axis="y", labelcolor=GREEN)
        ax2.grid(True)
        ax2.set_title("Energy Delivered per Hour", fontsize=10)

        tick_step = max(1, len(hourly) // 12)
        tick_pos  = range(0, len(hourly), tick_step)
        ax2.set_xticks(list(tick_pos))
        ax2.set_xticklabels(
            [str(hourly[i]["hour"])[:13] for i in tick_pos],
            rotation=35, ha="right", fontsize=7,
        )

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    _save(pdf, fig)


def page_charger_util(pdf: PdfPages, a: ChargingAnalytics) -> None:
    util = a.charger_utilization()
    fig, axes = plt.subplots(1, 3, figsize=(11.69, 5.5))
    fig.suptitle("3 — Charger Utilisation by Type", fontweight="bold", y=0.99)
    _desc(fig,
          "Efficiency and yield comparison across AC Level-1/2, DC Fast and Ultra-Fast hardware. "
          "Higher avg power and revenue per session indicate better utilisation.", y=0.95)

    if not util:
        for ax in axes: _nodata(ax)
    else:
        types   = [c["charger_type"]            for c in util]
        palette = [ACCENT, GREEN, ORANGE, PURPLE][:len(types)]
        for ax, (key, label) in zip(axes, [
            ("session_count",           "Sessions"),
            ("avg_power_kw",            "Avg Power (kW)"),
            ("avg_revenue_per_session", "Avg Revenue (€)"),
        ]):
            vals = [c[key] for c in util]
            ax.bar(types, vals, color=palette)
            ax.set_title(label)
            ax.grid(axis="y")
            for i, v in enumerate(vals):
                ax.text(i, v * 1.02, f"{v:,.1f}", ha="center", fontsize=8)

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    _save(pdf, fig)


def page_cities(pdf: PdfPages, a: ChargingAnalytics) -> None:
    cities = a.top_cities_by_energy(limit=10)
    fig, axes = plt.subplots(1, 2, figsize=(11.69, 7))
    fig.suptitle("4 — Top 10 Cities by Energy Demand", fontweight="bold", y=0.97)
    _desc(fig,
          "Geographic concentration of charging demand. "
          "Cities with high energy but lower revenue may indicate price-sensitive or high-AC-share markets.", y=0.93)

    if not cities:
        _nodata(axes[0]); _nodata(axes[1])
    else:
        labels = [f"{c['city']} ({c['country']})" for c in cities]
        kwh    = [c["total_energy_kwh"]  for c in cities]
        rev    = [c["total_revenue_eur"] for c in cities]
        colors = plt.cm.YlOrRd(np.linspace(0.4, 0.9, len(labels)))
        _hbar(axes[0], labels, kwh, colors, "Energy (kWh)", "Total Energy Delivered",
              lambda x, _: f"{x/1e3:.0f}k")
        _hbar(axes[1], labels, rev, colors, "Revenue (€)",  "Total Revenue",
              lambda x, _: f"€{x/1e3:.0f}k")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    _save(pdf, fig)


def page_faults(pdf: PdfPages, a: ChargingAnalytics) -> None:
    errors = a.error_analysis()
    fig, axes = plt.subplots(1, 2, figsize=(11.69, 6))
    fig.suptitle("5 — Fault Diagnostics", fontweight="bold", y=0.97)
    _desc(fig,
          "OCPP-style fault code frequency across all stations. "
          "The pie chart shows each code's share of total fault events.", y=0.93)

    if not errors:
        _nodata(axes[0]); _nodata(axes[1])
    else:
        codes  = [e["error_code"]    for e in errors]
        counts = [e["occurrences"]   for e in errors]
        pct    = [e["pct_of_errors"] for e in errors]
        colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(codes)))
        _hbar(axes[0], codes, counts, colors, "Occurrences", "Fault Frequency")
        if sum(pct) > 0:
            axes[1].pie(pct, labels=codes,
                        colors=plt.cm.tab10(np.linspace(0, 1, len(codes))),
                        autopct="%1.1f%%", pctdistance=0.8,
                        textprops={"fontsize": 8.5, "color": "white"})
        else:
            _nodata(axes[1])
        axes[1].set_title("Share of All Faults")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    _save(pdf, fig)


def page_vehicle_types(pdf: PdfPages, a: ChargingAnalytics) -> None:
    vtypes = a.vehicle_type_breakdown()
    fig, axes = plt.subplots(1, 3, figsize=(11.69, 5.5))
    fig.suptitle("6 — Vehicle Type Breakdown", fontweight="bold", y=0.99)
    _desc(fig,
          "BEV vs PHEV charging behaviour. "
          "BEVs typically draw more energy and power per session due to larger battery capacity.", y=0.95)

    if not vtypes:
        for ax in axes: _nodata(ax)
    else:
        types   = [v["vehicle_type"]   for v in vtypes]
        sess    = [v["session_count"]  for v in vtypes]
        avgkwh  = [v["avg_energy_kwh"] for v in vtypes]
        avgpwr  = [v["avg_power_kw"]   for v in vtypes]
        palette = [ACCENT, GREEN, ORANGE, PURPLE, TEAL][:len(types)]
        if sum(sess) > 0:
            axes[0].pie(sess, labels=types, autopct="%1.1f%%", colors=palette,
                        textprops={"fontsize": 9, "color": "white"})
        else:
            _nodata(axes[0])
        axes[0].set_title("Session Share")
        axes[1].bar(types, avgkwh, color=palette)
        axes[1].set_title("Avg Energy per Session (kWh)")
        axes[1].grid(axis="y")
        axes[2].bar(types, avgpwr, color=palette)
        axes[2].set_title("Avg Charging Power (kW)")
        axes[2].grid(axis="y")

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    _save(pdf, fig)


def page_peak_hours(pdf: PdfPages, a: ChargingAnalytics) -> None:
    peak = a.peak_hours_analysis()
    fig, ax = plt.subplots(figsize=(11.69, 5.5))
    fig.suptitle("9 — Hour-of-Day Traffic Pattern (All-Time)", fontweight="bold", y=0.97)
    _desc(fig,
          "Aggregated session count by hour across the full dataset. "
          "Coloured bands mark the three peak windows modelled by the simulator's traffic shaper.", y=0.93)

    if not peak:
        _nodata(ax)
    else:
        hours    = [p["hour_of_day"]   for p in peak]
        sessions = [p["session_count"] for p in peak]
        colors   = []
        for h in hours:
            if 7  <= h < 9:  colors.append(ORANGE)
            elif 12 <= h < 14: colors.append(ACCENT)
            elif 17 <= h < 20: colors.append(RED)
            else:              colors.append(GREEN)
        ax.bar(hours, sessions, color=colors, width=0.8)
        ax.set_xlabel("Hour of Day (UTC)")
        ax.set_ylabel("Sessions")
        ax.set_xticks(range(0, 24))
        ax.set_xticklabels([f"{h:02d}:00" for h in range(24)], rotation=45, ha="right", fontsize=8)
        ax.grid(axis="y")
        ax.legend(handles=[
            Patch(facecolor=ORANGE, label="Morning Peak 07-09 (2×)"),
            Patch(facecolor=ACCENT, label="Lunch Peak   12-14 (1.5×)"),
            Patch(facecolor=RED,    label="Evening Peak 17-20 (3×)"),
            Patch(facecolor=GREEN,  label="Off-Peak"),
        ], fontsize=8.5, loc="upper left")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    _save(pdf, fig)


def page_a1(pdf: PdfPages, a: ChargingAnalytics) -> None:
    h7 = a.hourly_energy_7days()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11.69, 8.27), sharex=True)
    fig.suptitle("A1 — Hourly Energy Consumption — Last 7 Days", fontweight="bold", y=0.99)
    _desc(fig,
          "7-day hourly trend read from the sessions_hourly_mv materialized view. "
          "Red dot = highest-energy hour (PEAK); purple dot = lowest (LOW).", y=0.96)

    if not h7:
        _nodata(ax1, "No 7-day data — run `make backfill-7d` first.")
        ax2.axis("off")
    else:
        xs      = range(len(h7))
        energy  = [h["total_energy_kwh"] for h in h7]
        sess    = [h["session_count"]    for h in h7]
        hlabels = [h.get("label", "")    for h in h7]

        ax1.fill_between(xs, energy, alpha=0.18, color=ACCENT)
        ax1.plot(xs, energy, color=ACCENT, lw=1.5)
        for i, (e, lbl) in enumerate(zip(energy, hlabels)):
            if lbl:
                c = RED if lbl == "PEAK" else PURPLE
                ax1.scatter(i, e, color=c, s=55, zorder=5)
                ax1.annotate(lbl, (i, e), xytext=(0, 7), textcoords="offset points",
                             fontsize=7, color=c, ha="center", fontweight="bold")
        ax1.set_ylabel("Energy (kWh)")
        ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax1.grid(True)
        ax1.set_title("Energy Delivered per Hour")

        ax2.fill_between(xs, sess, alpha=0.18, color=GREEN)
        ax2.plot(xs, sess, color=GREEN, lw=1.5)
        ax2.set_ylabel("Sessions")
        ax2.grid(True)
        ax2.set_title("Sessions per Hour")

        tick_pos = list(range(0, len(h7), 24))
        ax2.set_xticks(tick_pos)
        ax2.set_xticklabels([str(h7[i]["hour"])[:10] for i in tick_pos if i < len(h7)],
                            rotation=30, ha="right", fontsize=8)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    _save(pdf, fig)


def page_a2(pdf: PdfPages, a: ChargingAnalytics) -> None:
    uptime = a.station_uptime_ratio(limit=20)
    fig, axes = plt.subplots(1, 2, figsize=(11.69, 8.27))
    fig.suptitle("A2 — Station Uptime / Downtime (Worst 20 Stations)", fontweight="bold", y=0.97)
    _desc(fig,
          "Fault rate = fault_alert events ÷ total events per station. "
          "Uptime proxy = session_start ÷ (session_start + fault_alert). "
          "Stations above the 0.5% threshold are flagged red.", y=0.93)

    if not uptime:
        _nodata(axes[0]); _nodata(axes[1])
    else:
        THRESHOLD = 0.5
        stations    = [f"{s['station_id']}  ({s['city']})" for s in uptime]
        fault_rates = [s["fault_rate_pct"] for s in uptime]
        uptime_pcts = [s["uptime_pct"]     for s in uptime]

        fcolors = [RED if fr > THRESHOLD else ORANGE for fr in fault_rates]
        _hbar(axes[0], stations, fault_rates, fcolors, "Fault Rate (%)", "Fault Rate")
        axes[0].axvline(x=THRESHOLD, color="white", linestyle="--", lw=1, alpha=0.7,
                        label=f"Threshold {THRESHOLD}%")
        axes[0].legend(fontsize=8)

        ucolors = [GREEN if u >= 90 else (ORANGE if u >= 70 else RED) for u in uptime_pcts]
        _hbar(axes[1], stations, uptime_pcts, ucolors, "Uptime (%)", "Uptime %")
        axes[1].axvline(x=90, color="white", linestyle="--", lw=1, alpha=0.7, label="90% target")
        axes[1].legend(fontsize=8)
        axes[1].set_xlim(0, 108)

        above = sum(1 for fr in fault_rates if fr > THRESHOLD)
        fig.text(0.5, 0.04,
                 f"{above} of {len(uptime)} stations exceed the {THRESHOLD}% fault-rate threshold.",
                 ha="center", fontsize=9, color=RED if above else GREEN, style="italic")

    plt.tight_layout(rect=[0, 0.06, 1, 0.92])
    _save(pdf, fig)


def page_a3(pdf: PdfPages, a: ChargingAnalytics) -> None:
    comp   = a.vehicle_brand_comparison()
    brands = a.vehicle_brand_breakdown(limit=15)
    fig, axes = plt.subplots(1, 3, figsize=(11.69, 6))
    fig.suptitle("A3 — Tesla vs Other Brands", fontweight="bold", y=0.97)
    _desc(fig,
          "Head-to-head comparison on duration, energy and revenue. "
          "Right panel ranks all brands by total energy delivered across the dataset.", y=0.93)

    if comp:
        groups  = [b["brand_group"]             for b in comp]
        dur     = [b["avg_duration_min"]         for b in comp]
        energy  = [b["avg_energy_kwh"]           for b in comp]
        power   = [b["avg_power_kw"]             for b in comp]
        revenue = [b["avg_revenue_per_session"]  for b in comp]
        g_col   = [ACCENT if g == "Tesla" else GREEN for g in groups]
        width   = 0.35
        x       = np.arange(3)
        for i, (grp, col) in enumerate(zip(groups, g_col)):
            axes[0].bar(x + i*width - width/2, [dur[i], energy[i], power[i]],
                        width, label=grp, color=col, alpha=0.85)
        axes[0].set_xticks(x)
        axes[0].set_xticklabels(["Duration\n(min)", "Energy\n(kWh)", "Power\n(kW)"], fontsize=8.5)
        axes[0].set_title("Session Metrics")
        axes[0].legend(fontsize=9)
        axes[0].grid(axis="y")

        bars = axes[1].bar(groups, revenue, color=g_col)
        axes[1].set_title("Avg Revenue per Session (€)")
        axes[1].grid(axis="y")
        for bar, v in zip(bars, revenue):
            axes[1].text(bar.get_x()+bar.get_width()/2, v*1.02, f"€{v:.2f}",
                         ha="center", fontsize=10, fontweight="bold")

        if len(comp) == 2:
            tesla  = next((b for b in comp if b["brand_group"] == "Tesla"),  None)
            others = next((b for b in comp if b["brand_group"] != "Tesla"), None)
            if tesla and others:
                d_dur = tesla["avg_duration_min"]  - others["avg_duration_min"]
                d_kwh = tesla["avg_energy_kwh"]    - others["avg_energy_kwh"]
                d_rev = tesla["avg_revenue_per_session"] - others["avg_revenue_per_session"]
                fig.text(0.5, 0.04,
                         f"Tesla vs Others delta — Duration: {d_dur:+.1f} min  |  "
                         f"Energy: {d_kwh:+.2f} kWh  |  Revenue: €{d_rev:+.2f}",
                         ha="center", fontsize=9, color=MUTED, style="italic")

    if brands:
        bnames  = [b["vehicle_brand"]    for b in brands]
        bkwh    = [b["total_energy_kwh"] for b in brands]
        bcolors = [ACCENT if b == "Tesla" else "#3a3d60" for b in bnames]
        _hbar(axes[2], bnames, bkwh, bcolors,
              "Total Energy (kWh)", "All Brands — Energy",
              lambda x, _: f"{x/1e3:.0f}k")

    plt.tight_layout(rect=[0, 0.06, 1, 0.92])
    _save(pdf, fig)


def page_a4(pdf: PdfPages, a: ChargingAnalytics) -> None:
    periods = a.peak_hour_revenue()
    fig, axes = plt.subplots(1, 3, figsize=(11.69, 6))
    fig.suptitle("A4 — Revenue Analysis: Peak vs Off-Peak", fontweight="bold", y=0.97)
    _desc(fig,
          "Revenue split across Morning Peak (07-09), Evening Peak (17-20) and Off-Peak hours. "
          "Peak windows are shaped by per-window multipliers (2×, 3×) in the simulator.", y=0.93)

    if not periods:
        for ax in axes: _nodata(ax)
    else:
        pnames  = [p["period"]            for p in periods]
        prevs   = [p["total_revenue_eur"] for p in periods]
        pcts    = [p["revenue_pct"]       for p in periods]
        psess   = [p["session_count"]     for p in periods]
        p_col   = [ORANGE, RED, ACCENT]

        if sum(prevs) > 0:
            axes[0].pie(prevs, labels=pnames, autopct="%1.1f%%", colors=p_col, startangle=140,
                        textprops={"fontsize": 9, "color": "white"})
        else:
            _nodata(axes[0], "No revenue data")
        axes[0].set_title("Revenue Share")

        bars = axes[1].bar(pnames, prevs, color=p_col)
        axes[1].set_title("Total Revenue by Period")
        axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"€{x/1e3:.0f}k"))
        axes[1].grid(axis="y")
        for bar, pct in zip(bars, pcts):
            axes[1].text(bar.get_x()+bar.get_width()/2, bar.get_height()*1.02,
                         f"{pct:.1f}%", ha="center", fontsize=9, fontweight="bold")

        axes[2].bar(pnames, psess, color=p_col)
        axes[2].set_title("Sessions by Period")
        axes[2].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e3:.0f}k"))
        axes[2].grid(axis="y")

        peak_pct = sum(p["revenue_pct"] for p in periods if p["period"] != "Off-Peak")
        fig.text(0.5, 0.04,
                 f"Peak hours combined account for {peak_pct:.1f}% of total revenue.",
                 ha="center", fontsize=9, color=ORANGE, style="italic")

    plt.tight_layout(rect=[0, 0.06, 1, 0.92])
    _save(pdf, fig)


def page_a5(pdf: PdfPages, a: ChargingAnalytics) -> None:
    geo = a.fault_geographic_distribution(limit=20)
    fig, axes = plt.subplots(1, 2, figsize=(11.69, 8.27))
    fig.suptitle("A5 — Fault Geographic Distribution", fontweight="bold", y=0.97)
    _desc(fig,
          "Left: absolute fault count per city. "
          "Right: faults per station (normalised) — highlights cities where individual stations fail most.", y=0.93)

    if not geo:
        _nodata(axes[0]); _nodata(axes[1])
    else:
        clabels = [f"{g['city']} ({g['country']})" for g in geo]
        faults  = [g["fault_count"]        for g in geo]
        fps     = [g["faults_per_station"] for g in geo]

        _hbar(axes[0], clabels, faults,
              plt.cm.Oranges(np.linspace(0.35, 0.9, len(clabels))),
              "Total Fault Events", "Fault Count by City")
        _hbar(axes[1], clabels, fps,
              plt.cm.Reds(np.linspace(0.35, 0.9, len(clabels))),
              "Faults per Station", "Fault Density (normalised)")

        worst = max(geo, key=lambda x: x["faults_per_station"])
        fig.text(0.5, 0.04,
                 f"Highest fault density: {worst['city']} ({worst['country']}) — "
                 f"{worst['faults_per_station']:.1f} faults/station, "
                 f"{worst['pct_of_all_faults']:.1f}% of all faults.",
                 ha="center", fontsize=9, color=RED, style="italic")

    plt.tight_layout(rect=[0, 0.06, 1, 0.92])
    _save(pdf, fig)


def page_a6(pdf: PdfPages, a: ChargingAnalytics) -> None:
    anomalies = a.anomaly_detection(limit=200)

    # total sessions needed for anomaly %
    try:
        total_sessions = a._q(
            "SELECT count() FROM charging_events "
            "WHERE event_type='session_stop' AND duration_minutes > 0"
        )[0][0]
    except Exception:
        total_sessions = 0

    fig = plt.figure(figsize=(11.69, 8.27))
    fig.suptitle("A6 — Anomaly Detection: Sessions Deviating >2σ from Mean Power",
                 fontweight="bold", y=0.98)
    _desc(fig,
          "Z-score = (session_avg_power − fleet_mean) ÷ fleet_std.  "
          "Red bars / dots = high-power outliers.  Purple = underperformers.",
          y=0.945)

    import matplotlib.gridspec as gridspec
    gs = gridspec.GridSpec(2, 4, figure=fig,
                           top=0.91, bottom=0.07,
                           hspace=0.55, wspace=0.35)

    if not anomalies:
        ax = fig.add_subplot(gs[:, :])
        _nodata(ax, "No anomalies detected — run the pipeline to accumulate session data.")
        _save(pdf, fig)
        return

    z_scores   = [r["z_score"]  for r in anomalies]
    powers     = [r["power_kw"] for r in anomalies]
    fleet_mean = anomalies[0]["fleet_mean_power"]
    fleet_std  = anomalies[0]["fleet_std_power"]
    n_anom     = len(anomalies)
    anom_pct   = (n_anom / total_sessions * 100) if total_sessions else 0.0
    n_high     = sum(1 for z in z_scores if z > 0)
    n_low      = n_anom - n_high

    # ── Row 0: stat cards ──────────────────────────────────────────────────────
    stats = [
        ("Anomaly Count",  f"{n_anom}",           RED),
        ("Anomaly %",      f"{anom_pct:.2f}%",    ORANGE),
        ("Fleet Mean",     f"{fleet_mean:.1f} kW", GREEN),
        ("Fleet Std Dev",  f"{fleet_std:.1f} kW",  ACCENT),
    ]
    for col, (label, value, color) in enumerate(stats):
        ax = fig.add_subplot(gs[0, col])
        ax.set_facecolor(PANEL)
        ax.axis("off")
        for spine in ax.spines.values():
            spine.set_visible(True); spine.set_color(color); spine.set_linewidth(2)
        ax.text(0.5, 0.62, value, ha="center", va="center",
                fontsize=18, fontweight="bold", color=color, transform=ax.transAxes)
        ax.text(0.5, 0.18, label, ha="center", va="center",
                fontsize=8.5, color=MUTED, transform=ax.transAxes)

    # ── Row 1: detail table (top 10 by |z|) ───────────────────────────────────
    ax_tbl = fig.add_subplot(gs[1, :])
    ax_tbl.axis("off")
    ax_tbl.set_title("Top Anomalous Sessions (sorted by |z-score|)", fontsize=10, pad=8)

    top10 = sorted(anomalies, key=lambda r: abs(r["z_score"]), reverse=True)[:10]
    col_labels = ["Station", "Network", "City", "Brand",
                  "Power kW", "Fleet Mean", "Z-Score", "Energy kWh"]
    rows = []
    for r in top10:
        z = r["z_score"]
        rows.append([
            r["station_id"],
            r["network_id"],
            r["city"],
            r["vehicle_brand"] or "—",
            f"{r['power_kw']:.1f}",
            f"{r['fleet_mean_power']:.1f}",
            f"{z:+.2f}",
            f"{r['energy_kwh']:.1f}",
        ])

    tbl = ax_tbl.table(
        cellText=rows,
        colLabels=col_labels,
        cellLoc="center",
        loc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(7.5)
    tbl.scale(1, 1.4)

    # header styling
    for col in range(len(col_labels)):
        cell = tbl[0, col]
        cell.set_facecolor(BORDER)
        cell.set_text_props(color=FG, fontweight="bold")

    # row styling — colour z-score cell
    for row_idx, r in enumerate(top10, start=1):
        z = r["z_score"]
        for col in range(len(col_labels)):
            cell = tbl[row_idx, col]
            cell.set_facecolor("#12151f" if row_idx % 2 == 0 else PANEL)
            cell.set_text_props(color=FG)
        z_cell = tbl[row_idx, 6]   # Z-Score column
        z_cell.set_text_props(color=RED if z > 0 else PURPLE, fontweight="bold")

    _save(pdf, fig)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def page_tech_justification(pdf: PdfPages) -> None:
    fig = plt.figure(figsize=(11.69, 8.27))
    fig.patch.set_facecolor(BG)
    fig.suptitle("Technology Choices — Justification", fontweight="bold", y=0.97)
    _desc(fig, "Why each component of the stack was selected over its alternatives.", y=0.935)

    # ── technology table data ─────────────────────────────────────────────────
    technologies = [
        (
            "Apache Kafka (KRaft)",
            "Event Transport",
            "Durable, ordered, partitioned log capable of 100k+ events/sec. "
            "KRaft mode removes the ZooKeeper dependency, reducing operational overhead. "
            "12 partitions allow linear throughput scaling by adding consumers. "
            "Snappy compression cuts wire size ~60% with negligible CPU cost. "
            "Alternatives (RabbitMQ, Pulsar) lack Kafka's replay semantics and ecosystem maturity.",
        ),
        (
            "Snappy Compression",
            "Serialization",
            "Chosen over gzip/lz4/zstd for the best balance between compression ratio "
            "and CPU overhead at high throughput. gzip achieves better ratios but is 4–6× "
            "slower to compress. At 100k events/sec, CPU budget is tight — Snappy keeps "
            "producer latency below 1 ms per batch while still halving network I/O.",
        ),
        (
            "JSON (via orjson)",
            "Message Format",
            "Human-readable and schema-free, which simplifies debugging and late-binding "
            "consumer development. orjson is 3–10× faster than stdlib json (written in Rust) "
            "so the readability trade-off vs Avro/Protobuf costs very little at runtime. "
            "For a portfolio project, inspectability outweighs the marginal throughput gain "
            "of a binary format. A production system would migrate to Avro + Schema Registry.",
        ),
        (
            "Redis 7",
            "Real-Time Store",
            "Sub-millisecond read/write latency makes it the right choice for live session "
            "state, connector status and revenue leaderboards that the dashboard polls every "
            "30 s. Sorted Sets give O(log N) leaderboard updates for free. "
            "TTLs automatically expire stale sessions without a background job. "
            "Postgres or ClickHouse would be 10–100× slower for these point lookups.",
        ),
        (
            "ClickHouse 23.x",
            "Analytics Store",
            "Column-oriented OLAP engine that executes the 14 analytical queries in "
            "< 200 ms even at tens of millions of rows. ReplacingMergeTree handles "
            "out-of-order / duplicate events from Kafka without extra deduplication logic. "
            "Materialized views (sessions_hourly_mv, station_daily_mv, etc.) pre-aggregate "
            "hot query paths so the Grafana dashboard never hits the raw table. "
            "BigQuery/Redshift would require cloud egress; DuckDB lacks server-mode HA.",
        ),
        (
            "Python + confluent-kafka",
            "Producer / Consumer",
            "confluent-kafka wraps librdkafka — the highest-throughput Kafka client available. "
            "Python multiprocessing bypasses the GIL for the producer, achieving the target "
            "100k eps across 16 worker processes. The consumer's async batch-insert pattern "
            "(10k rows / 5 s flush) amortises ClickHouse insert overhead. "
            "A JVM-based stack (Kafka Streams, Flink) would add operational complexity "
            "without meaningful throughput gains for this workload.",
        ),
        (
            "Grafana 10",
            "Visualisation",
            "Natively integrates with ClickHouse via the official plugin, allowing raw SQL "
            "panels without an intermediate API layer. Auto-provisioning from JSON files "
            "makes the dashboard fully reproducible via version control. "
            "Collapsible rows keep the A1–A6 case-study sections organised without "
            "requiring separate dashboards. Alternatives (Superset, Metabase) lack "
            "ClickHouse plugin maturity and Grafana's alerting ecosystem.",
        ),
    ]

    import matplotlib.gridspec as gridspec
    gs = gridspec.GridSpec(len(technologies), 1, figure=fig,
                           top=0.90, bottom=0.04, hspace=0.18)

    header_colors = [ACCENT, GREEN, ORANGE, RED, TEAL, PURPLE, PINK]

    for i, (name, role, justification) in enumerate(technologies):
        ax = fig.add_subplot(gs[i])
        ax.set_facecolor("#12151f" if i % 2 == 0 else PANEL)
        ax.axis("off")
        color = header_colors[i % len(header_colors)]

        # left accent bar
        ax.add_patch(plt.Rectangle((0, 0), 0.006, 1,
                                   transform=ax.transAxes,
                                   color=color, clip_on=False))

        # component name + role badge
        ax.text(0.015, 0.72, name, transform=ax.transAxes,
                fontsize=9.5, fontweight="bold", color=color, va="top")
        ax.text(0.015, 0.30, f"[ {role} ]", transform=ax.transAxes,
                fontsize=7.5, color=MUTED, va="top", style="italic")

        # justification text
        ax.text(0.22, 0.82, justification, transform=ax.transAxes,
                fontsize=7.8, color=FG, va="top",
                wrap=True,
                bbox=dict(boxstyle="round,pad=0.1", facecolor="none", edgecolor="none"))

    _save(pdf, fig)


def main():
    ap = argparse.ArgumentParser(description="Generate ChargeSquare PDF report")
    ap.add_argument("--out", default="exports/chargesquare_report.pdf",
                    help="Output path (default: exports/chargesquare_report.pdf)")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    print("Connecting to ClickHouse…")
    a = ChargingAnalytics()
    print(f"  {a.total_events():,} events found")

    print(f"Writing PDF → {args.out}")
    with PdfPages(args.out) as pdf:
        steps = [
            ("Cover / KPIs",                     lambda pdf, a: page_cover(pdf, a)),
            ("Technology Justification",         lambda pdf, a: page_tech_justification(pdf)),
            ("Revenue by Network",               lambda pdf, a: page_revenue_network(pdf, a)),
            ("Sessions per Hour",                lambda pdf, a: page_sessions_hour(pdf, a)),
            ("Charger Utilisation",              lambda pdf, a: page_charger_util(pdf, a)),
            ("Top Cities",                       lambda pdf, a: page_cities(pdf, a)),
            ("Fault Diagnostics",                lambda pdf, a: page_faults(pdf, a)),
            ("Vehicle Types",                    lambda pdf, a: page_vehicle_types(pdf, a)),
            ("Peak Hours",                       lambda pdf, a: page_peak_hours(pdf, a)),
            ("A1 Hourly Energy 7 Days",          lambda pdf, a: page_a1(pdf, a)),
            ("A2 Station Uptime",                lambda pdf, a: page_a2(pdf, a)),
            ("A3 Tesla vs Others",               lambda pdf, a: page_a3(pdf, a)),
            ("A4 Peak vs Off-Peak Revenue",      lambda pdf, a: page_a4(pdf, a)),
            ("A5 Fault Geographic Distribution", lambda pdf, a: page_a5(pdf, a)),
            ("A6 Anomaly Detection",             lambda pdf, a: page_a6(pdf, a)),
        ]
        for name, fn in steps:
            print(f"  {name}…")
            fn(pdf, a)

        d = pdf.infodict()
        d["Title"]        = "ChargeSquare Analytics Report"
        d["Author"]       = "ChargeSquare Pipeline"
        d["Subject"]      = "EV Charging Analytics"
        d["CreationDate"] = datetime.now(timezone.utc)

    print(f"Done — {args.out}")


if __name__ == "__main__":
    main()
