#!/usr/bin/env python3
"""
Incremental scale test: 1,000 → 10,000 → 50,000 → 100,000 events/sec.

Usage:
    python scripts/scale_test.py
    python scripts/scale_test.py --duration 30
    python scripts/scale_test.py --tiers 1000 10000 50000 100000
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from rich.console import Console
from rich.table import Table
from rich import box

console = Console()

TIERS = [
    (1_000,   1),
    (10_000,  4),
    (50_000,  8),
    (100_000, 16),
]


def run_tier(eps: int, workers: int, duration: int) -> dict:
    console.rule(f"[bold cyan]Tier {eps:,} eps — {workers} workers — {duration}s[/bold cyan]")

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "simulator.producer",
            "--eps",      str(eps),
            "--workers",  str(workers),
            "--duration", str(duration),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    total_sent = 0
    t0 = time.monotonic()

    for line in proc.stdout:
        line = line.rstrip()
        console.print(f"  [dim]{line}[/dim]")

        # Sum total sent across all workers
        m = re.search(r"Total sent:\s*([\d,]+)", line)
        if m:
            total_sent += int(m.group(1).replace(",", ""))

    proc.wait()
    elapsed = time.monotonic() - t0

    actual_eps = total_sent / duration if duration else 0
    pct = (actual_eps / eps * 100) if eps else 0

    console.print(
        f"\n  [bold]Result:[/bold] "
        f"[green]{actual_eps:,.0f}[/green] eps avg  "
        f"({pct:.1f}% of target)  "
        f"total sent: [bold]{total_sent:,}[/bold]\n"
    )

    return {
        "target_eps": eps,
        "workers":    workers,
        "actual_eps": actual_eps,
        "pct":        pct,
        "sent":       total_sent,
        "rc":         proc.returncode,
    }


def print_summary(results: list[dict]) -> None:
    t = Table(title="Scale Test — Summary", box=box.HEAVY_EDGE)
    t.add_column("Target eps",  justify="right", style="cyan")
    t.add_column("Workers",     justify="right")
    t.add_column("Actual eps",  justify="right", style="bold green")
    t.add_column("% of Target", justify="right")
    t.add_column("Total Sent",  justify="right")
    t.add_column("Pass?",       justify="center")

    for r in results:
        pct_color = "green" if r["pct"] >= 90 else "yellow" if r["pct"] >= 70 else "red"
        status    = "[green]YES[/green]" if r["rc"] == 0 else "[red]NO[/red]"
        t.add_row(
            f"{r['target_eps']:,}",
            str(r["workers"]),
            f"{r['actual_eps']:,.0f}",
            f"[{pct_color}]{r['pct']:.1f}%[/{pct_color}]",
            f"{r['sent']:,}",
            status,
        )

    console.print()
    console.print(t)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=30,
                    help="Seconds per tier (default: 30)")
    ap.add_argument("--tiers", type=int, nargs="+",
                    help="Custom EPS targets (e.g. --tiers 1000 10000)")
    args = ap.parse_args()

    tiers = TIERS
    if args.tiers:
        tiers = [(eps, max(1, eps // 5000)) for eps in args.tiers]

    console.rule("[bold cyan]ChargeSquare — Incremental Scale Test[/bold cyan]")

    results = []
    for eps, workers in tiers:
        r = run_tier(eps, workers, args.duration)
        results.append(r)
        if r != tiers[-1]:
            time.sleep(2)

    print_summary(results)