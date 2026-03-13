#!/usr/bin/env python3
"""
ChargeSquare Throughput Benchmark
====================================

Measures two things independently:

  1. Generator throughput  — how many events/sec can be serialised in-process
     (no Kafka, no network I/O).

  2. Kafka producer throughput — how many events/sec can be delivered to the
     broker (requires `make up` to be running first).

Run from the repo root:
    python scripts/benchmark.py
    python scripts/benchmark.py --kafka          # also benchmark Kafka
    python scripts/benchmark.py --duration 30    # run each phase for 30 s
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Allow running from the repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent))

from rich.console import Console
from rich.table import Table
from rich import box

console = Console()


# ─────────────────────────────────────────────────────────────────────────────
# Generator benchmark (pure CPU / memory)
# ─────────────────────────────────────────────────────────────────────────────

def bench_generator(duration_s: int = 10) -> dict:
    from config.settings import SIMULATOR
    from simulator.generators import EventGenerator, StationRegistry

    console.print("\n[bold cyan]Phase 1: Generator benchmark[/bold cyan]")
    console.print(f"  Building station registry ({SIMULATOR.num_networks} networks × "
                  f"{SIMULATOR.stations_per_network} stations)…")

    registry = StationRegistry(
        SIMULATOR.num_networks,
        SIMULATOR.stations_per_network,
        SIMULATOR.connectors_per_station,
    )
    generator = EventGenerator(registry)

    batch_size = 5_000
    total      = 0
    t_end      = time.monotonic() + duration_s

    with console.status("[bold green]Generating events…"):
        while time.monotonic() < t_end:
            events = generator.generate_batch(batch_size)
            # Force JSON serialisation (simulates producer hot path)
            for ev in events:
                _ = ev.to_json()
            total += batch_size

    eps = total / duration_s
    console.print(f"  [green]✓[/green] Generated [bold]{total:,}[/bold] events "
                  f"in {duration_s}s → [bold]{eps:,.0f} events/sec[/bold]")
    return {"total": total, "duration_s": duration_s, "eps": eps}


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer benchmark
# ─────────────────────────────────────────────────────────────────────────────

def bench_kafka(duration_s: int = 10) -> dict:
    from config.settings import KAFKA, SIMULATOR
    from confluent_kafka import Producer
    from simulator.generators import EventGenerator, StationRegistry

    console.print("\n[bold cyan]Phase 2: Kafka producer benchmark[/bold cyan]")
    console.print(f"  Connecting to {KAFKA.bootstrap_servers}…")

    try:
        producer = Producer({
            "bootstrap.servers":    KAFKA.bootstrap_servers,
            "batch.size":           KAFKA.producer_batch_size,
            "linger.ms":            KAFKA.producer_linger_ms,
            "compression.type":     KAFKA.producer_compression_type,
            "acks":                 KAFKA.producer_acks,
            "queue.buffering.max.messages": 1_000_000,
        })
    except Exception as exc:
        console.print(f"  [red]Cannot create producer: {exc}[/red]")
        return {}

    registry  = StationRegistry(
        SIMULATOR.num_networks,
        SIMULATOR.stations_per_network,
        SIMULATOR.connectors_per_station,
    )
    generator = EventGenerator(registry)
    topic     = KAFKA.topic_charging_events
    batch_sz  = 2_000
    sent      = 0
    failed    = 0

    def _cb(err, _msg):
        nonlocal failed
        if err:
            failed += 1

    t_end = time.monotonic() + duration_s

    with console.status("[bold green]Producing to Kafka…"):
        while time.monotonic() < t_end:
            events = generator.generate_batch(batch_sz)
            for ev in events:
                try:
                    producer.produce(
                        topic=topic,
                        key=ev.station_id.encode(),
                        value=ev.to_json(),
                        callback=_cb,
                    )
                except Exception:
                    failed += 1
            producer.poll(0)
            sent += batch_sz

    remaining = producer.flush(timeout=30)
    total_delivered = sent - failed - remaining

    eps = total_delivered / duration_s
    console.print(f"  [green]✓[/green] Produced [bold]{total_delivered:,}[/bold] events "
                  f"in {duration_s}s → [bold]{eps:,.0f} events/sec[/bold]  "
                  f"(failed={failed}, not-flushed={remaining})")
    return {"sent": sent, "delivered": total_delivered,
            "failed": failed, "duration_s": duration_s, "eps": eps}


# ─────────────────────────────────────────────────────────────────────────────
# Summary table
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(gen_result: dict, kafka_result: dict) -> None:
    console.print()
    t = Table(title="Benchmark Summary", box=box.HEAVY_EDGE)
    t.add_column("Phase",         style="cyan")
    t.add_column("Events",        justify="right")
    t.add_column("Duration",      justify="right")
    t.add_column("Events/sec",    justify="right", style="bold green")

    if gen_result:
        t.add_row("Generator (in-process)",
                  f"{gen_result['total']:,}",
                  f"{gen_result['duration_s']} s",
                  f"{gen_result['eps']:,.0f}")

    if kafka_result:
        t.add_row("Kafka Producer (end-to-end)",
                  f"{kafka_result.get('delivered', 0):,}",
                  f"{kafka_result['duration_s']} s",
                  f"{kafka_result['eps']:,.0f}")

    console.print(t)
    console.print()
    console.print("[dim]Tip: run multiple producers in parallel (make producer-fast) "
                  "to saturate the broker.[/dim]")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="ChargeSquare Throughput Benchmark")
    ap.add_argument("--kafka",    action="store_true",
                    help="Also benchmark Kafka producer (requires running broker)")
    ap.add_argument("--duration", type=int, default=10,
                    help="Seconds to run each phase (default: 10)")
    args = ap.parse_args()

    console.rule("[bold cyan]ChargeSquare — Throughput Benchmark[/bold cyan]")

    gen_r   = bench_generator(args.duration)
    kafka_r = bench_kafka(args.duration) if args.kafka else {}

    print_summary(gen_r, kafka_r)
