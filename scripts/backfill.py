"""
Backfill 24 h of historical data
=================================
Generates events for every hour of yesterday so that all analytics
time-buckets (Morning Peak 07-09, Evening Peak 17-20, Off-Peak) have data.

Usage:
    python scripts/backfill.py [--events-per-hour 100000]
    make backfill
"""
from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from datetime import datetime, timezone, timedelta

sys.path.insert(0, ".")

from confluent_kafka import Producer
from config.settings import KAFKA, SIMULATOR
from simulator.generators import StationRegistry
from simulator.session_manager import SessionManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# Yesterday 00:00 UTC
YESTERDAY = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)


def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers":          KAFKA.bootstrap_servers,
        "batch.size":                 KAFKA.producer_batch_size,
        "linger.ms":                  50,
        "compression.type":           KAFKA.producer_compression_type,
        "acks":                       "1",
        "queue.buffering.max.kbytes": 512 * 1024,
    })


def _ts_in_hour(hour: int) -> str:
    """Random ISO-8601 timestamp within the given UTC hour of yesterday."""
    dt = YESTERDAY + timedelta(
        hours=hour,
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return dt.isoformat()


def backfill(events_per_hour: int = 100_000) -> None:
    registry = StationRegistry(
        SIMULATOR.num_networks,
        SIMULATOR.stations_per_network,
        SIMULATOR.connectors_per_station,
    )
    manager  = SessionManager(registry, SIMULATOR)
    producer = _make_producer()
    topic    = KAFKA.topic_charging_events
    total    = 0
    t0       = time.monotonic()

    for hour in range(24):
        if 7 <= hour < 9:
            label, multiplier = "MORNING PEAK", 2.5
        elif 17 <= hour < 20:
            label, multiplier = "EVENING PEAK", 3.0
        elif 12 <= hour < 14:
            label, multiplier = "LUNCH PEAK",   1.5
        else:
            label, multiplier = "off-peak",     1.0

        count  = int(events_per_hour * multiplier)
        events = manager.generate_batch(count)
        # Rewrite every event's timestamp to fall within this UTC hour
        for ev in events:
            ev.timestamp = _ts_in_hour(hour)
            while True:
                try:
                    producer.produce(
                        topic=topic,
                        key=ev.station_id.encode(),
                        value=ev.to_json(),
                    )
                    break
                except BufferError:
                    producer.poll(0.1)  # drain callbacks, then retry
        producer.poll(0)
        total += len(events)
        log.info("Hour %02d:xx  %-14s  x%.1f  %d events queued", hour, label, multiplier, count)

    log.info("Flushing %d events to Kafka…", total)
    remaining = producer.flush(timeout=120)
    if remaining:
        log.warning("%d messages were not flushed.", remaining)
    log.info("Backfill complete — %d events in %.1fs", total, time.monotonic() - t0)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Inject 24 h of historical charging data into Kafka")
    p.add_argument(
        "--events-per-hour", type=int, default=100_000,
        help="Events to generate per UTC hour (default 100,000 → 2.4 M total)",
    )
    args = p.parse_args()
    backfill(args.events_per_hour)
