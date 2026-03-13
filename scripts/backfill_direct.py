"""
Direct ClickHouse Backfill — bypasses Kafka
============================================
Generates session_stop events for every hour of yesterday and inserts them
directly into ClickHouse. Useful when the Kafka consumer has a large backlog
and you need data across all time periods quickly.

Usage:
    python scripts/backfill_direct.py [--sessions-per-hour 10000]
    make backfill-direct
"""
from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from datetime import datetime, timezone, timedelta

sys.path.insert(0, ".")

from clickhouse_driver import Client as CH
from config.settings import CLICKHOUSE, SIMULATOR
from simulator.generators import StationRegistry
from simulator.session_manager import SessionManager
from simulator.models import EventType
from consumers.clickhouse_consumer import event_to_row, _INSERT_SQL

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# Yesterday 00:00 UTC
YESTERDAY = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)

BATCH_SIZE = 50_000


def _ts_in_hour(hour: int) -> str:
    dt = YESTERDAY + timedelta(
        hours=hour,
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return dt.isoformat()


def backfill_direct(sessions_per_hour: int = 10_000) -> None:
    client = CH(
        host=CLICKHOUSE.host,
        port=CLICKHOUSE.port,
        database=CLICKHOUSE.database,
        user=CLICKHOUSE.user,
        password=CLICKHOUSE.password,
        settings={"max_insert_block_size": 200_000},
    )

    registry = StationRegistry(
        SIMULATOR.num_networks,
        SIMULATOR.stations_per_network,
        SIMULATOR.connectors_per_station,
    )
    # We need enough total events to yield sessions_per_hour session_stops.
    # A lifecycle = 1 start + 3-15 meter updates + 1 stop ≈ avg 9 events.
    # So we need sessions_per_hour * 9 events to get sessions_per_hour stops.
    events_to_generate = sessions_per_hour * 12  # generous headroom

    manager  = SessionManager(registry, SIMULATOR)
    total    = 0
    t0       = time.monotonic()

    for hour in range(24):
        events = manager.generate_batch(events_to_generate)

        # Keep only session_stop events and rewrite their timestamps
        stops = [ev for ev in events if ev.event_type == EventType.SESSION_STOP]
        for ev in stops:
            ev.timestamp = _ts_in_hour(hour)

        # Insert in batches
        rows = [event_to_row(ev.to_dict()) for ev in stops]
        for i in range(0, len(rows), BATCH_SIZE):
            chunk = rows[i : i + BATCH_SIZE]
            client.execute(_INSERT_SQL, chunk)

        total += len(stops)
        label = ("MORNING PEAK" if 7 <= hour < 9 else
                 "EVENING PEAK" if 17 <= hour < 20 else
                 "off-peak")
        log.info("Hour %02d:xx  %-14s  %d session_stops inserted", hour, label, len(stops))

    log.info("Done — %d session_stops in %.1fs", total, time.monotonic() - t0)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Insert 24 h of session_stop data directly into ClickHouse")
    p.add_argument(
        "--sessions-per-hour", type=int, default=10_000,
        help="Target session_stop events per UTC hour (default 10,000)",
    )
    args = p.parse_args()
    backfill_direct(args.sessions_per_hour)
