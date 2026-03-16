"""
Backfill historical charging data
===================================
Generates events for every hour of every requested day so that all analytics
time-buckets (Morning Peak 07-09, Evening Peak 17-20, Off-Peak) have data.

Uses a fixed simulated clock per (day, hour) so that:
  - Peak-hour detection inside SessionManager is based on simulated time
  - All events in a session lifecycle share a coherent timestamp

Usage:
    make backfill              # yesterday only (1 day)
    make backfill-7d           # last 7 days
    make backfill-30d          # last 30 days
    python scripts/backfill.py --days 14
    python scripts/backfill.py --days 1 --events-per-hour 50000
"""
from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from datetime import date, datetime, timezone, timedelta

sys.path.insert(0, ".")

from confluent_kafka import Producer
from config.settings import KAFKA, SIMULATOR
from simulator.generators import StationRegistry
from simulator.session_manager import SessionManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers":          KAFKA.bootstrap_servers,
        "batch.size":                 KAFKA.producer_batch_size,
        "linger.ms":                  50,
        "compression.type":           KAFKA.producer_compression_type,
        "acks":                       "1",
        "queue.buffering.max.kbytes": 512 * 1024,
    })


def _fixed_clock(day_start: datetime, hour: int):
    """
    Return a clock function that always returns a random minute/second
    within the given UTC hour.  Called once per batch so all events in
    a single generate_batch() call share a stable simulated hour (which
    is what SessionManager uses for peak-hour detection).
    """
    base = day_start + timedelta(hours=hour)

    def _clock() -> datetime:
        return base + timedelta(
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
    return _clock


def backfill(days: int = 1, events_per_hour: int = 100_000) -> None:
    today = date.today()
    # days=1 → just yesterday; days=7 → last 7 days (oldest first)
    day_list = [
        datetime(*(today - timedelta(days=n)).timetuple()[:3], tzinfo=timezone.utc)
        for n in range(days, 0, -1)   # oldest → newest
    ]

    registry = StationRegistry(
        SIMULATOR.num_networks,
        SIMULATOR.stations_per_network,
        SIMULATOR.connectors_per_station,
    )
    producer    = _make_producer()
    topic       = KAFKA.topic_charging_events
    grand_total = 0
    t0          = time.monotonic()

    log.info("Backfill: %d day(s) — %s → %s  (%d events/hour base)",
             days,
             day_list[0].date(),
             day_list[-1].date(),
             events_per_hour)

    for day_start in day_list:
        day_total = 0
        for hour in range(24):
            # Build a new SessionManager for each hour so the clock_fn
            # reflects the correct simulated hour (used for peak detection).
            clock_fn = _fixed_clock(day_start, hour)
            manager  = SessionManager(registry, SIMULATOR, clock_fn=clock_fn)

            count  = int(events_per_hour * random.uniform(0.90, 1.10))  # ±10% jitter
            events = manager.generate_batch(count)
            for ev in events:
                while True:
                    try:
                        producer.produce(
                            topic=topic,
                            key=ev.station_id.encode(),
                            value=ev.to_json(),
                        )
                        break
                    except BufferError:
                        producer.poll(0.1)
            producer.poll(0)
            day_total += len(events)

        grand_total += day_total
        log.info("Day %s — %d events queued (running total: %d)",
                 day_start.date(), day_total, grand_total)

    log.info("Flushing remaining Kafka buffer…")
    remaining = producer.flush(timeout=600)
    if remaining:
        log.warning("%d messages were not flushed — Kafka may be slow.", remaining)
    log.info("Backfill complete — %d events in %.1fs", grand_total, time.monotonic() - t0)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Inject historical charging data into Kafka")
    p.add_argument(
        "--days", type=int, default=1,
        help="Number of days to backfill (1 = yesterday only, 7 = last 7 days, etc.)",
    )
    p.add_argument(
        "--events-per-hour", type=int, default=100_000,
        help="Base events per UTC hour per day (SessionManager applies peak-hour multiplier internally)",
    )
    args = p.parse_args()
    backfill(days=args.days, events_per_hour=args.events_per_hour)
