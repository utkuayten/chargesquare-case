"""
ChargeSquare Event Producer
===========================

Launches N worker processes, each driving its own Kafka producer,
to generate and publish EV charging events at a configurable rate.

Target throughput: 10,000 – 100,000 events / second.

Usage
-----
    # Via Makefile
    make producer            # 10 k eps, 4 workers
    make producer-fast       # 50 k eps, 8 workers
    make producer-max        # 100 k eps, 16 workers

    # Direct
    python -m simulator.producer --eps 20000 --workers 4 --duration 60
"""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import os
import random
import signal
import sys
import time
from typing import List, Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config.settings import KAFKA, SIMULATOR
from simulator.generators import StationRegistry
from simulator.session_manager import SessionManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(processName)-18s] %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers":                  bootstrap_servers,
        "batch.size":                         KAFKA.producer_batch_size,
        "linger.ms":                          KAFKA.producer_linger_ms,
        "compression.type":                   KAFKA.producer_compression_type,
        "acks":                               KAFKA.producer_acks,
        "queue.buffering.max.messages":       2_000_000,
        "queue.buffering.max.kbytes":         KAFKA.producer_buffer_mb * 1024,
        "max.in.flight.requests.per.connection": 5,
        "socket.send.buffer.bytes":           8_388_608,  # 8 MB
    })


def _ensure_topics(bootstrap_servers: str) -> None:
    """Create Kafka topics if they do not yet exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    new_topics = [
        NewTopic(KAFKA.topic_charging_events, num_partitions=12, replication_factor=1),
        NewTopic(KAFKA.topic_station_status,  num_partitions=4,  replication_factor=1),
    ]
    futures = admin.create_topics(new_topics)
    for topic, fut in futures.items():
        try:
            fut.result()
            log.info("Topic '%s' created.", topic)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                log.info("Topic '%s' already exists.", topic)
            else:
                log.warning("Topic '%s': %s", topic, exc)


def _delivery_error(err, _msg) -> None:
    if err:
        log.error("Delivery failed: %s", err)


# ─────────────────────────────────────────────────────────────────────────────
# Worker process
# ─────────────────────────────────────────────────────────────────────────────

def _worker(
    worker_id:        int,
    target_eps:       int,
    bootstrap_servers: str,
    station_data:     List[dict],
    shutdown:         mp.Event,
    total_counter:    mp.Value,
) -> None:
    """
    Runs in a child process.
    Generates events and produces them to Kafka at *target_eps* events/sec.
    """
    # Each worker gets its own random seed → diverse data
    random.seed(worker_id * 31337 + int(time.time()))

    log.info("Worker-%d starting at %d eps", worker_id, target_eps)

    producer = _make_producer(bootstrap_servers)

    # Rehydrate registry from the shared station list
    registry = StationRegistry.__new__(StationRegistry)
    registry.stations = station_data
    registry.connectors_per_station = SIMULATOR.connectors_per_station
    generator = SessionManager(registry, SIMULATOR)

    topic = KAFKA.topic_charging_events

    # Calculate batch size so we hit exactly *target_eps* events/sec
    # We aim for ~50 ms intervals to keep latency low
    interval_s     = 0.05
    batch_size     = max(1, int(target_eps * interval_s))
    actual_interval = batch_size / target_eps

    sent     = 0
    t_start  = time.monotonic()
    t_report = t_start

    try:
        while not shutdown.is_set():
            t_batch = time.monotonic()

            events = generator.generate_batch(batch_size)

            for ev in events:
                try:
                    producer.produce(
                        topic=topic,
                        key=ev.station_id.encode(),
                        value=ev.to_json(),
                        callback=_delivery_error,
                    )
                except BufferError:
                    # Buffer full — poll to release space, then retry once
                    producer.poll(0.1)
                    producer.produce(
                        topic=topic,
                        key=ev.station_id.encode(),
                        value=ev.to_json(),
                    )

            # Non-blocking poll — triggers delivery callbacks
            producer.poll(0)
            sent += batch_size

            # Rate throttling
            elapsed = time.monotonic() - t_batch
            if elapsed < actual_interval:
                time.sleep(actual_interval - elapsed)

            # Periodic log
            now = time.monotonic()
            if now - t_report >= 10.0:
                eps = sent / (now - t_start)
                log.info("Worker-%d  sent=%s  actual_eps=%.0f", worker_id, f"{sent:,}", eps)
                t_report = now

    except KeyboardInterrupt:
        pass

    # Ignore further SIGINT but keep SIGTERM so main process can kill us
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Graceful shutdown — flush pending messages (short timeout so we exit fast)
    remaining = producer.flush(timeout=2)
    if remaining:
        log.warning("Worker-%d: %d messages not flushed on shutdown.", worker_id, remaining)
    with total_counter.get_lock():
        total_counter.value += sent
    log.info("Worker-%d stopped. Total sent: %s", worker_id, f"{sent:,}")


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def run_producer(
    target_eps:        int            = SIMULATOR.target_events_per_second,
    num_workers:       int            = SIMULATOR.num_producer_processes,
    bootstrap_servers: str            = KAFKA.bootstrap_servers,
    duration_seconds:  Optional[int]  = None,
) -> None:
    log.info("=" * 60)
    log.info("ChargeSquare Event Producer")
    log.info("  bootstrap    : %s", bootstrap_servers)
    log.info("  target       : %s events/sec", f"{target_eps:,}")
    log.info("  workers      : %d", num_workers)
    log.info("  session mode : lifecycle (START → METER_UPDATEs → STOP)")
    log.info("  peak hours   : %s  (×%.1f)", SIMULATOR.peak_hours, SIMULATOR.peak_multiplier)
    log.info("  duration     : %s", f"{duration_seconds}s" if duration_seconds else "until SIGINT")
    log.info("=" * 60)

    _ensure_topics(bootstrap_servers)

    log.info("Building station registry (%d networks × %d stations)…",
             SIMULATOR.num_networks, SIMULATOR.stations_per_network)
    registry = StationRegistry(
        SIMULATOR.num_networks,
        SIMULATOR.stations_per_network,
        SIMULATOR.connectors_per_station,
    )
    log.info("Registry ready: %s stations", f"{len(registry.stations):,}")

    eps_per_worker  = target_eps // num_workers
    shutdown        = mp.Event()
    total_counter   = mp.Value('L', 0)

    processes = [
        mp.Process(
            target=_worker,
            args=(i, eps_per_worker, bootstrap_servers, registry.stations, shutdown, total_counter),
            name=f"Producer-{i}",
            daemon=False,
        )
        for i in range(num_workers)
    ]

    def _handle_signal(_sig, _frame):
        shutdown.set()

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    for p in processes:
        p.start()

    try:
        if duration_seconds:
            deadline = time.monotonic() + duration_seconds
            while time.monotonic() < deadline and not shutdown.is_set():
                time.sleep(0.2)
        else:
            while not shutdown.is_set():
                time.sleep(0.2)
    except (KeyboardInterrupt, SystemExit):
        shutdown.set()

    for p in processes:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()

    log.info("=" * 60)
    log.info("Producer stopped. Grand total: %s events", f"{total_counter.value:,}")
    log.info("=" * 60)
    os._exit(0)


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ChargeSquare — EV Charging Event Producer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--eps",               type=int, default=SIMULATOR.target_events_per_second,
                   help="Target events per second")
    p.add_argument("--workers",           type=int, default=SIMULATOR.num_producer_processes,
                   help="Number of parallel producer processes")
    p.add_argument("--duration",          type=int, default=None,
                   help="Run for N seconds then exit (default: run until SIGINT)")
    p.add_argument("--bootstrap-servers", default=KAFKA.bootstrap_servers,
                   help="Kafka bootstrap servers")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_producer(args.eps, args.workers, args.bootstrap_servers, args.duration)
