"""
ChargeSquare Analytics Dashboard
==================================

Two modes:
  --mode report     One-shot report printed to stdout (default).
  --mode dashboard  Live terminal dashboard that refreshes every N seconds.

Usage
-----
    python -m analytics.dashboard --mode report
    python -m analytics.dashboard --mode dashboard --refresh 30
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from typing import Any

import redis
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from analytics.queries import ChargingAnalytics
from config.settings import REDIS

console = Console()


# ─────────────────────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fmt(n: Any) -> str:
    try:
        return f"{float(n):>12,.0f}"
    except (TypeError, ValueError):
        return str(n)

def _eur(n: Any) -> str:
    try:
        return f"€{float(n):>12,.2f}"
    except (TypeError, ValueError):
        return "€0.00"

def _bar(value: float, maximum: float, width: int = 20) -> str:
    if maximum == 0:
        return " " * width
    filled = int(round(value / maximum * width))
    return "█" * filled + "░" * (width - filled)


# ─────────────────────────────────────────────────────────────────────────────
# Redis live panel
# ─────────────────────────────────────────────────────────────────────────────

def _redis_panel(r: redis.Redis) -> Panel:
    t = Table(box=box.SIMPLE_HEAVY, show_header=False, expand=True)
    t.add_column("Metric", style="cyan", min_width=28)
    t.add_column("Value",  style="bold green", justify="right", min_width=14)

    keys = {
        "Total Events":           "global:events:total",
        "Active Sessions":        "global:active_sessions",
        "Session Starts":         "global:events:session_start",
        "Meter Updates":          "global:events:meter_update",
        "Session Stops":          "global:events:session_stop",
        "Status Changes":         "global:events:status_change",
        "Heartbeats":             "global:events:heartbeat",
        "Fault Alerts":           "global:events:fault_alert",
        "Total Faults":           "global:errors:total",
    }
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for label, key in keys.items():
        val = r.get(key) or "0"
        # Active sessions can go negative in simulation (events aren't paired)
        if key == "global:active_sessions":
            val = str(max(0, int(val)))
        t.add_row(label, _fmt(val))

    energy = r.get(f"global:energy_kwh:{today}") or "0"
    t.add_row("Energy Delivered Today", f"{float(energy):>12,.1f} kWh")

    total_ev  = int(r.get("global:events:total") or 1)
    total_err = int(r.get("global:errors:total") or 0)
    err_rate  = total_err / total_ev * 100
    t.add_row("Error Rate", f"{err_rate:>12.3f} %")

    return Panel(t, title="[bold cyan]Live Metrics (Redis)", border_style="green")


# ─────────────────────────────────────────────────────────────────────────────
# ClickHouse panels
# ─────────────────────────────────────────────────────────────────────────────

def _networks_panel(a: ChargingAnalytics) -> Panel:
    data = a.revenue_by_network(days=1)[:6]
    t = Table(box=box.SIMPLE_HEAVY, expand=True)
    t.add_column("Network",      style="cyan",  min_width=14)
    t.add_column("Sessions",     justify="right")
    t.add_column("Revenue",      style="green", justify="right")
    t.add_column("Energy kWh",   justify="right")
    t.add_column("Avg Duration", justify="right")
    for n in data:
        t.add_row(
            n["network_id"],
            _fmt(n["session_count"]),
            _eur(n["total_revenue_eur"]),
            _fmt(n["total_energy_kwh"]),
            f"{n['avg_duration_min']:.0f} min",
        )
    return Panel(t, title="[bold]Network Performance (last 24 h)", border_style="blue")


def _charger_panel(a: ChargingAnalytics) -> Panel:
    data = a.charger_utilization()
    t = Table(box=box.SIMPLE_HEAVY, expand=True)
    t.add_column("Charger Type", style="cyan")
    t.add_column("Sessions",     justify="right")
    t.add_column("Avg kW",       justify="right")
    t.add_column("Avg kWh",      justify="right")
    t.add_column("Avg Rev.",     style="green", justify="right")
    for c in data:
        t.add_row(
            c["charger_type"],
            _fmt(c["session_count"]),
            f"{c['avg_power_kw']:.1f}",
            f"{c['avg_energy_kwh']:.1f}",
            _eur(c["avg_revenue_per_session"]),
        )
    return Panel(t, title="[bold]Charger Utilisation", border_style="yellow")


def _cities_panel(a: ChargingAnalytics) -> Panel:
    data = a.top_cities_by_energy(limit=8)
    t = Table(box=box.SIMPLE_HEAVY, expand=True)
    t.add_column("City",     style="cyan")
    t.add_column("Country")
    t.add_column("Sessions", justify="right")
    t.add_column("kWh",      justify="right")
    t.add_column("Revenue",  style="green", justify="right")
    for c in data:
        t.add_row(
            c["city"], c["country"],
            _fmt(c["session_count"]),
            _fmt(c["total_energy_kwh"]),
            _eur(c["total_revenue_eur"]),
        )
    return Panel(t, title="[bold]Top Cities by Energy", border_style="magenta")


def _errors_panel(a: ChargingAnalytics) -> Panel:
    data = a.error_analysis()[:6]
    t = Table(box=box.SIMPLE_HEAVY, expand=True)
    t.add_column("Error Code",         style="red")
    t.add_column("Occurrences",        justify="right")
    t.add_column("Stations Affected",  justify="right")
    t.add_column("% of Errors",        justify="right")
    for e in data:
        t.add_row(
            e["error_code"],
            _fmt(e["occurrences"]),
            _fmt(e["affected_stations"]),
            f"{e['pct_of_errors']:.1f}%",
        )
    return Panel(t, title="[bold]Fault Diagnostics", border_style="red")


def _vehicle_brand_panel(a: ChargingAnalytics) -> Panel:
    data = a.vehicle_brand_breakdown(limit=8)
    t = Table(box=box.SIMPLE_HEAVY, expand=True)
    t.add_column("Brand",    style="cyan")
    t.add_column("Sessions", justify="right")
    t.add_column("Avg kWh",  justify="right")
    t.add_column("Total kWh",justify="right")
    t.add_column("Revenue",  style="green", justify="right")
    for v in data:
        t.add_row(
            v["vehicle_brand"],
            _fmt(v["session_count"]),
            f"{v['avg_energy_kwh']:.1f}",
            _fmt(v["total_energy_kwh"]),
            _eur(v["total_revenue_eur"]),
        )
    return Panel(t, title="[bold]Vehicle Brand Breakdown", border_style="bright_blue")


def _tariff_panel(a: ChargingAnalytics) -> Panel:
    data = a.tariff_analysis(limit=8)
    t = Table(box=box.SIMPLE_HEAVY, expand=True)
    t.add_column("Tariff ID",   style="cyan")
    t.add_column("Sessions",    justify="right")
    t.add_column("Avg Rev.",    style="green", justify="right")
    t.add_column("Avg kWh",     justify="right")
    t.add_column("€/kWh",       justify="right")
    for tr in data:
        t.add_row(
            tr["tariff_id"],
            _fmt(tr["session_count"]),
            _eur(tr["avg_revenue_per_session"]),
            f"{tr['avg_energy_kwh']:.1f}",
            f"{tr['avg_price_per_kwh']:.3f}",
        )
    return Panel(t, title="[bold]Tariff Performance", border_style="bright_magenta")


def _throughput_panel(a: ChargingAnalytics) -> Panel:
    rt = a.real_time_throughput(minutes=5)
    t = Table(box=box.SIMPLE_HEAVY, show_header=False, expand=True)
    t.add_column("K", style="cyan", min_width=22)
    t.add_column("V", style="bold white", justify="right", min_width=14)
    t.add_row("Events (last 5 min)",  _fmt(rt["event_count"]))
    t.add_row("Events / second",       f"{rt['events_per_second']:.1f}")
    t.add_row("Unique Sessions",       _fmt(rt["unique_sessions"]))
    t.add_row("Active Stations",       _fmt(rt["active_stations"]))
    t.add_row("Revenue",               _eur(rt["revenue_eur"]))
    t.add_row("Energy Delivered",      f"{rt['energy_kwh']:,.1f} kWh")
    return Panel(t, title="[bold]Pipeline Throughput (ClickHouse)", border_style="cyan")


# ─────────────────────────────────────────────────────────────────────────────
# Live dashboard
# ─────────────────────────────────────────────────────────────────────────────

def show_dashboard(refresh: int = 30) -> None:
    r = redis.Redis(host=REDIS.host, port=REDIS.port, db=REDIS.db, decode_responses=True)
    a = ChargingAnalytics()

    def _build_layout() -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header",  size=3),
            Layout(name="row1",    size=14),
            Layout(name="row2",    size=14),
            Layout(name="row3",    size=14),
            Layout(name="row4"),
        )
        layout["row1"].split_row(Layout(name="live"), Layout(name="throughput"))
        layout["row2"].split_row(Layout(name="networks"), Layout(name="chargers"))
        layout["row3"].split_row(Layout(name="cities"),   Layout(name="errors"))
        layout["row4"].split_row(Layout(name="vehicle_brands"), Layout(name="tariffs"))

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        layout["header"].update(Panel(
            Text(f"⚡  ChargeSquare — EV Charging Analytics Dashboard   [{ts}]",
                 justify="center", style="bold cyan"),
            style="cyan",
        ))

        try:
            layout["live"].update(_redis_panel(r))
        except Exception as exc:
            layout["live"].update(Panel(f"Redis unavailable:\n{exc}", style="red"))

        try:
            layout["throughput"].update(_throughput_panel(a))
        except Exception as exc:
            layout["throughput"].update(Panel(f"ClickHouse unavailable:\n{exc}", style="red"))

        for name, fn in [("networks",      _networks_panel),
                         ("chargers",      _charger_panel),
                         ("cities",        _cities_panel),
                         ("errors",        _errors_panel),
                         ("vehicle_brands",_vehicle_brand_panel),
                         ("tariffs",       _tariff_panel)]:
            try:
                layout[name].update(fn(a))
            except Exception as exc:
                layout[name].update(Panel(f"Error: {exc}", style="red"))

        return layout

    with Live(console=console, refresh_per_second=0.2, screen=True) as live:
        while True:
            live.update(_build_layout())
            time.sleep(refresh)


# ─────────────────────────────────────────────────────────────────────────────
# One-shot report
# ─────────────────────────────────────────────────────────────────────────────

def print_report() -> None:
    a = ChargingAnalytics()

    console.rule("[bold cyan]ChargeSquare — Analytics Report[/bold cyan]")
    console.print(f"  Generated: {datetime.now(timezone.utc).isoformat()}\n")

    # ── Total events ──────────────────────────────────────────────────────────
    try:
        total = a.total_events()
        console.print(f"[bold]Total events in store:[/bold] {total:,}\n")
    except Exception as exc:
        console.print(f"[red]Cannot connect to ClickHouse: {exc}[/red]")
        return

    # ── Real-time throughput ──────────────────────────────────────────────────
    rt = a.real_time_throughput(minutes=5)
    console.print("[bold]Pipeline throughput (last 5 min)[/bold]")
    console.print(f"  Events:           {rt['event_count']:,}")
    console.print(f"  Events/sec:       {rt['events_per_second']}")
    console.print(f"  Unique sessions:  {rt['unique_sessions']:,}")
    console.print(f"  Active stations:  {rt['active_stations']:,}")
    console.print(f"  Revenue:          €{rt['revenue_eur']:,.2f}")
    console.print(f"  Energy:           {rt['energy_kwh']:,.2f} kWh\n")

    # ── Revenue by network ────────────────────────────────────────────────────
    console.rule("[bold]1. Revenue by Network (last 7 days)[/bold]")
    t = Table(box=box.SIMPLE)
    for col, kw in [("Network",{}),("Sessions",{"justify":"right"}),
                    ("Revenue",{"justify":"right","style":"green"}),
                    ("Energy kWh",{"justify":"right"}),("Avg Duration",{"justify":"right"})]:
        t.add_column(col, **kw)
    for n in a.revenue_by_network(days=7):
        t.add_row(n["network_id"],f"{n['session_count']:,}",
                  f"€{n['total_revenue_eur']:,.2f}",
                  f"{n['total_energy_kwh']:,.1f}",
                  f"{n['avg_duration_min']:.0f} min")
    console.print(t)

    # ── Charger utilisation ───────────────────────────────────────────────────
    console.rule("[bold]2. Charger Utilisation[/bold]")
    t = Table(box=box.SIMPLE)
    for col, kw in [("Type",{}),("Sessions",{"justify":"right"}),
                    ("Avg kW",{"justify":"right"}),("Avg kWh",{"justify":"right"}),
                    ("Avg Revenue",{"justify":"right","style":"green"})]:
        t.add_column(col, **kw)
    for c in a.charger_utilization():
        t.add_row(c["charger_type"],f"{c['session_count']:,}",
                  f"{c['avg_power_kw']:.1f}",f"{c['avg_energy_kwh']:.1f}",
                  f"€{c['avg_revenue_per_session']:.2f}")
    console.print(t)

    # ── Top cities ────────────────────────────────────────────────────────────
    console.rule("[bold]3. Top Cities by Energy Demand[/bold]")
    t = Table(box=box.SIMPLE)
    for col, kw in [("City",{}),("Country",{}),("Sessions",{"justify":"right"}),
                    ("Energy kWh",{"justify":"right"}),
                    ("Revenue",{"justify":"right","style":"green"})]:
        t.add_column(col, **kw)
    for c in a.top_cities_by_energy(limit=10):
        t.add_row(c["city"],c["country"],f"{c['session_count']:,}",
                  f"{c['total_energy_kwh']:,.1f}",f"€{c['total_revenue_eur']:,.2f}")
    console.print(t)

    # ── Error analysis ────────────────────────────────────────────────────────
    console.rule("[bold]4. Fault Diagnostics[/bold]")
    t = Table(box=box.SIMPLE)
    for col, kw in [("Error Code",{"style":"red"}),
                    ("Count",{"justify":"right"}),
                    ("Stations",{"justify":"right"}),
                    ("% of Errors",{"justify":"right"})]:
        t.add_column(col, **kw)
    for e in a.error_analysis():
        t.add_row(e["error_code"],f"{e['occurrences']:,}",
                  f"{e['affected_stations']:,}",f"{e['pct_of_errors']:.1f}%")
    console.print(t)

    # ── Vehicle type ──────────────────────────────────────────────────────────
    console.rule("[bold]5. Vehicle Type Breakdown[/bold]")
    t = Table(box=box.SIMPLE)
    for col, kw in [("Type",{}),("Sessions",{"justify":"right"}),
                    ("Avg kWh",{"justify":"right"}),("Avg Duration",{"justify":"right"}),
                    ("Revenue",{"justify":"right","style":"green"})]:
        t.add_column(col, **kw)
    for v in a.vehicle_type_breakdown():
        t.add_row(v["vehicle_type"],f"{v['session_count']:,}",
                  f"{v['avg_energy_kwh']:.1f}",f"{v['avg_duration_min']:.0f} min",
                  f"€{v['total_revenue_eur']:,.2f}")
    console.print(t)

    # ── Vehicle brand breakdown ───────────────────────────────────────────────
    console.rule("[bold]6. Vehicle Brand Breakdown[/bold]")
    t = Table(box=box.SIMPLE)
    for col, kw in [("Brand",{}),("Sessions",{"justify":"right"}),
                    ("Avg kWh",{"justify":"right"}),("Total kWh",{"justify":"right"}),
                    ("Avg Duration",{"justify":"right"}),
                    ("Revenue",{"justify":"right","style":"green"})]:
        t.add_column(col, **kw)
    for v in a.vehicle_brand_breakdown():
        t.add_row(v["vehicle_brand"],f"{v['session_count']:,}",
                  f"{v['avg_energy_kwh']:.1f}",f"{v['total_energy_kwh']:,.1f}",
                  f"{v['avg_duration_min']:.0f} min",f"€{v['total_revenue_eur']:,.2f}")
    console.print(t)

    # ── Tariff analysis ───────────────────────────────────────────────────────
    console.rule("[bold]7. Tariff Analysis[/bold]")
    t = Table(box=box.SIMPLE)
    for col, kw in [("Tariff ID",{}),("Sessions",{"justify":"right"}),
                    ("Avg Revenue",{"justify":"right","style":"green"}),
                    ("Total Revenue",{"justify":"right","style":"green"}),
                    ("Avg kWh",{"justify":"right"}),("€/kWh",{"justify":"right"})]:
        t.add_column(col, **kw)
    for tr in a.tariff_analysis():
        t.add_row(tr["tariff_id"],f"{tr['session_count']:,}",
                  f"€{tr['avg_revenue_per_session']:.2f}",f"€{tr['total_revenue_eur']:,.2f}",
                  f"{tr['avg_energy_kwh']:.1f}",f"{tr['avg_price_per_kwh']:.3f}")
    console.print(t)

    # ── Session duration distribution ──────────────────────────────────────────
    console.rule("[bold]8. Session Duration Distribution[/bold]")
    t = Table(box=box.SIMPLE)
    for col, kw in [("Duration",{}),("Sessions",{"justify":"right"}),
                    ("Avg kWh",{"justify":"right"}),("Avg Revenue",{"justify":"right"})]:
        t.add_column(col, **kw)
    for d in a.session_duration_distribution():
        t.add_row(d["bucket"],f"{d['sessions']:,}",
                  f"{d['avg_energy_kwh']:.1f}",f"€{d['avg_revenue_eur']:.2f}")
    console.print(t)

    # ── Peak hours bar chart ──────────────────────────────────────────────────
    console.rule("[bold]9. Peak Hours (Hour-of-Day Traffic)[/bold]")
    hours = a.peak_hours_analysis()
    max_s = max((h["session_count"] for h in hours), default=1)
    t = Table(box=box.SIMPLE)
    t.add_column("Hour")
    t.add_column("Sessions", justify="right")
    t.add_column("Traffic", min_width=24)
    t.add_column("Avg kWh", justify="right")
    for h in hours:
        bar = _bar(h["session_count"], max_s, 24)
        t.add_row(f"{h['hour_of_day']:02d}:00",
                  f"{h['session_count']:,}", bar,
                  f"{h['avg_energy_kwh']:.1f}")
    console.print(t)

    # ── Network error rates ───────────────────────────────────────────────────
    console.rule("[bold]10. Network Reliability (Error Rate)[/bold]")
    t = Table(box=box.SIMPLE)
    for col, kw in [("Network",{}),("Errors",{"justify":"right"}),
                    ("Total Events",{"justify":"right"}),
                    ("Error Rate",{"justify":"right","style":"red"})]:
        t.add_column(col, **kw)
    for n in a.network_error_rate():
        t.add_row(n["network_id"],f"{n['error_count']:,}",
                  f"{n['total_events']:,}",f"{n['error_rate_pct']:.3f}%")
    console.print(t)

    # ── Country summary ───────────────────────────────────────────────────────
    console.rule("[bold]11. Country Summary[/bold]")
    t = Table(box=box.SIMPLE)
    for col, kw in [("Country",{}),("Sessions",{"justify":"right"}),
                    ("Stations",{"justify":"right"}),("Energy kWh",{"justify":"right"}),
                    ("Revenue",{"justify":"right","style":"green"})]:
        t.add_column(col, **kw)
    for c in a.country_summary():
        t.add_row(c["country"],f"{c['session_count']:,}",
                  f"{c['station_count']:,}",f"{c['total_energy_kwh']:,.1f}",
                  f"€{c['total_revenue_eur']:,.2f}")
    console.print(t)

    # ── A1. Hourly energy — last 7 days ──────────────────────────────────────
    console.rule("[bold cyan]A1. Hourly Energy Consumption — Last 7 Days[/bold cyan]")
    hourly = a.hourly_energy_7days()
    if not hourly:
        console.print("  [dim]No data yet — run pipeline longer to accumulate 7-day history.[/dim]")
    else:
        max_e = max(r["total_energy_kwh"] for r in hourly)
        t = Table(box=box.SIMPLE)
        t.add_column("Hour (UTC)")
        t.add_column("Sessions",   justify="right")
        t.add_column("Energy kWh", justify="right")
        t.add_column("Revenue",    justify="right", style="green")
        t.add_column("Trend",      min_width=20)
        t.add_column("")
        for h in hourly:
            bar   = _bar(h["total_energy_kwh"], max_e, 20)
            label = h.get("label", "")
            style = "bold yellow" if label == "PEAK" else ("dim" if label == "LOW" else "")
            t.add_row(
                str(h["hour"]),
                f"{h['session_count']:,}",
                f"{h['total_energy_kwh']:,.1f}",
                f"€{h['total_revenue_eur']:,.2f}",
                bar,
                f"[{style}]{label}[/{style}]" if label else "",
            )
        console.print(t)

    # ── A2. Station uptime / downtime ─────────────────────────────────────────
    console.rule("[bold cyan]A2. Most Problematic Stations (Uptime / Downtime)[/bold cyan]")
    uptime = a.station_uptime_ratio(limit=20)
    if not uptime:
        console.print("  [dim]No fault data yet.[/dim]")
    else:
        t = Table(box=box.SIMPLE)
        t.add_column("Station",       style="cyan")
        t.add_column("Network")
        t.add_column("City")
        t.add_column("Sessions",      justify="right")
        t.add_column("Faults",        justify="right", style="red")
        t.add_column("Fault Rate %",  justify="right", style="red")
        t.add_column("Uptime %",      justify="right", style="green")
        for s in uptime:
            t.add_row(
                s["station_id"], s["network_id"], s["city"],
                f"{s['session_starts']:,}",
                f"{s['fault_events']:,}",
                f"{s['fault_rate_pct']:.3f}%",
                f"{s['uptime_pct']:.1f}%",
            )
        console.print(t)

    # ── A3. Tesla vs other brands ─────────────────────────────────────────────
    console.rule("[bold cyan]A3. Charging Behaviour — Tesla vs Other Brands[/bold cyan]")
    brands = a.vehicle_brand_comparison()
    if not brands:
        console.print("  [dim]No session data yet.[/dim]")
    else:
        t = Table(box=box.SIMPLE)
        t.add_column("Brand Group",    style="cyan")
        t.add_column("Sessions",       justify="right")
        t.add_column("Avg Duration",   justify="right")
        t.add_column("Avg Energy kWh", justify="right")
        t.add_column("Avg Power kW",   justify="right")
        t.add_column("Avg Revenue",    justify="right", style="green")
        t.add_column("Total kWh",      justify="right")
        for b in brands:
            t.add_row(
                b["brand_group"],
                f"{b['session_count']:,}",
                f"{b['avg_duration_min']:.1f} min",
                f"{b['avg_energy_kwh']:.2f}",
                f"{b['avg_power_kw']:.1f}",
                f"€{b['avg_revenue_per_session']:.2f}",
                f"{b['total_energy_kwh']:,.1f}",
            )
        # Delta row
        if len(brands) == 2:
            tesla  = next((b for b in brands if b["brand_group"] == "Tesla"),  None)
            others = next((b for b in brands if b["brand_group"] != "Tesla"), None)
            if tesla and others:
                def _d(a_val, b_val, fmt=".2f"):
                    diff = a_val - b_val
                    sign = "+" if diff >= 0 else ""
                    return f"{sign}{diff:{fmt}}"
                t.add_row(
                    "[bold]Delta (Tesla − Others)[/bold]", "",
                    _d(tesla["avg_duration_min"],       others["avg_duration_min"], ".1f") + " min",
                    _d(tesla["avg_energy_kwh"],         others["avg_energy_kwh"]),
                    _d(tesla["avg_power_kw"],           others["avg_power_kw"],     ".1f"),
                    "€" + _d(tesla["avg_revenue_per_session"], others["avg_revenue_per_session"]),
                    "",
                )
        console.print(t)

    # ── A4. Peak-hour revenue contribution ───────────────────────────────────
    console.rule("[bold cyan]A4. Revenue Analysis — Peak vs Off-Peak[/bold cyan]")
    periods = a.peak_hour_revenue()
    if not periods:
        console.print("  [dim]No revenue data yet.[/dim]")
    else:
        t = Table(box=box.SIMPLE)
        t.add_column("Period",         style="cyan")
        t.add_column("Sessions",       justify="right")
        t.add_column("Total Revenue",  justify="right", style="green")
        t.add_column("Revenue %",      justify="right", style="bold green")
        t.add_column("Total kWh",      justify="right")
        t.add_column("Avg kWh",        justify="right")
        t.add_column("Avg Rev/Session",justify="right")
        max_rev = max(p["total_revenue_eur"] for p in periods)
        for p in periods:
            bar = _bar(p["total_revenue_eur"], max_rev, 16)
            t.add_row(
                p["period"],
                f"{p['session_count']:,}",
                f"€{p['total_revenue_eur']:,.2f}",
                f"{p['revenue_pct']:.1f}%  {bar}",
                f"{p['total_energy_kwh']:,.1f}",
                f"{p['avg_energy_kwh']:.2f}",
                f"€{p['avg_revenue_per_session']:.2f}",
            )
        console.print(t)
        peak_pct = sum(p["revenue_pct"] for p in periods
                       if p["period"] != "Off-Peak")
        console.print(f"  → Peak hours account for [bold green]{peak_pct:.1f}%[/bold green] "
                      f"of total revenue.\n")

    # ── A5. Geographic distribution of FAULT events ──────────────────────────
    console.rule("[bold cyan]A5. Fault Event Geographic Distribution[/bold cyan]")
    geo = a.fault_geographic_distribution(limit=20)
    if not geo:
        console.print("  [dim]No fault data yet.[/dim]")
    else:
        t = Table(box=box.SIMPLE)
        t.add_column("City",              style="cyan")
        t.add_column("Country")
        t.add_column("Fault Count",       justify="right", style="red")
        t.add_column("Stations Affected", justify="right")
        t.add_column("Error Types",       justify="right")
        t.add_column("% of All Faults",   justify="right")
        t.add_column("Faults/Station",    justify="right", style="red")
        for g in geo:
            t.add_row(
                g["city"], g["country"],
                f"{g['fault_count']:,}",
                f"{g['affected_stations']:,}",
                f"{g['distinct_error_types']:,}",
                f"{g['pct_of_all_faults']:.2f}%",
                f"{g['faults_per_station']:.2f}",
            )
        console.print(t)

    # ── A6. Anomaly detection (bonus) ─────────────────────────────────────────
    console.rule("[bold cyan]A6. Anomaly Detection — Sessions Deviating >2σ from Mean Power[/bold cyan]")
    anomalies = a.anomaly_detection(limit=20)
    if not anomalies:
        console.print("  [dim]No anomalies detected (or insufficient data).[/dim]")
    else:
        t = Table(box=box.SIMPLE)
        t.add_column("Session ID",      style="dim",   max_width=20)
        t.add_column("Station",         style="cyan")
        t.add_column("Network")
        t.add_column("City")
        t.add_column("Brand")
        t.add_column("Power kW",        justify="right", style="yellow")
        t.add_column("Fleet Mean kW",   justify="right")
        t.add_column("Std Dev",         justify="right")
        t.add_column("Z-Score",         justify="right", style="bold red")
        t.add_column("Energy kWh",      justify="right")
        for row in anomalies:
            t.add_row(
                row["session_id"][:18] + ".." if len(row["session_id"]) > 18 else row["session_id"],
                row["station_id"],
                row["network_id"],
                row["city"],
                row["vehicle_brand"],
                f"{row['power_kw']:.1f}",
                f"{row['fleet_mean_power']:.1f}",
                f"{row['fleet_std_power']:.1f}",
                f"{row['z_score']:+.2f}",
                f"{row['energy_kwh']:.1f}",
            )
        console.print(t)
        console.print(f"  → {len(anomalies)} anomalous sessions found "
                      f"(|z| > 2, fleet mean = "
                      f"{anomalies[0]['fleet_mean_power']:.1f} kW ± "
                      f"{anomalies[0]['fleet_std_power']:.1f} kW)\n")

    console.rule()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="ChargeSquare Analytics Dashboard")
    ap.add_argument("--mode",    choices=["dashboard", "report"], default="report")
    ap.add_argument("--refresh", type=int, default=30,
                    help="Dashboard auto-refresh interval (seconds)")
    args = ap.parse_args()

    if args.mode == "dashboard":
        show_dashboard(args.refresh)
    else:
        print_report()
