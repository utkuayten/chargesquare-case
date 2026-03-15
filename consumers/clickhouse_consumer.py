"""
ClickHouse Consumer — Analytics Store
======================================

Reads from the `charging-events` Kafka topic and bulk-inserts rows into
ClickHouse for long-term analytics.

2d — Analytics Store: Why ClickHouse?
--------------------------------------
ClickHouse is chosen over Parquet+DuckDB / BigQuery / Redshift because:

  • Columnar storage: queries that aggregate over energy_kwh or revenue_eur
    across millions of rows touch only the relevant column — 10–100× faster
    than row-oriented stores.

  • MergeTree family: SummingMergeTree for pre-aggregated views, and
    ReplacingMergeTree for deduplication — both are built-in and require no
    external tooling.

  • Sub-second queries on billions of rows: required for live dashboard
    refresh; BigQuery/Redshift have higher per-query latency and cold-start
    costs.

  • On-premise / Docker deployment: no cloud dependency; fits the case study
    setup.

Partition strategy
  PARTITION BY toYYYYMM(timestamp):
    - Monthly partitions align with billing cycles and data-retention TTL
      (90 days = 3 partitions).
    - Dropping old data is a zero-cost DROP PARTITION, not a DELETE.
    - Common time-range queries span 1–3 partitions, keeping IO minimal.

  Secondary sort key: ORDER BY (timestamp, network_id, station_id)
    - Time-range scans are the dominant access pattern.
    - Nested ORDER BY on network_id and station_id enables efficient
      per-CPO and per-station aggregations within a partition.

  Additional partition columns considered and rejected:
    - operator_id: same as network_id in this schema.
    - city: too many partitions (20 cities × N months = partition explosion).
    - event_type: most queries span all event types within a time range.

ClickHouse insert strategy
--------------------------
- Events are accumulated in an in-memory buffer.
- A background thread flushes every CH_FLUSH_INTERVAL seconds **or**
  whenever CH_BATCH_SIZE rows accumulate — whichever comes first.
- Flush is thread-safe: a lock guards the buffer swap.

Why batching matters for ClickHouse
  ClickHouse is optimised for bulk inserts.  Inserting row-by-row is
  ~1000× slower.  Batching 10 k rows every 5 s is comfortable at > 100 k eps.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import List, Optional

from clickhouse_driver import Client as CH
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_END

from config.settings import CLICKHOUSE, KAFKA
from consumers.validator import DeadLetterWriter, validate
from consumers.deduplicator import Deduplicator
from consumers.watermark import Watermark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [CHConsumer] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Row conversion
# ─────────────────────────────────────────────────────────────────────────────

def _parse_ts(s: Optional[str]) -> datetime:
    if not s:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def event_to_row(ev: dict) -> tuple:
    """Convert a decoded event dict to a ClickHouse insert row tuple."""
    return (
        ev.get("event_id", ""),
        ev.get("event_type", ""),
        _parse_ts(ev.get("timestamp")),
        ev.get("station_id", ""),
        int(ev.get("connector_id") or 0),
        ev.get("session_id", ""),
        ev.get("network_id", ""),
        ev.get("city", ""),
        ev.get("country", ""),
        float(ev.get("latitude")  or 0.0),
        float(ev.get("longitude") or 0.0),
        ev.get("charger_type", ""),
        float(ev.get("power_kw")  or 0.0),
        float(ev.get("energy_kwh") or 0.0),
        int(ev.get("soc_percent")  or 0),
        ev.get("vehicle_type", ""),
        int(ev.get("duration_minutes") or 0),
        float(ev.get("price_per_kwh")  or 0.0),
        float(ev.get("revenue_eur")    or 0.0),
        ev.get("status", ""),
        ev.get("error_code") or "",
        ev.get("component") or "",
        float(ev.get("voltage_v") or 0.0),
        float(ev.get("current_a") or 0.0),
        ev.get("vehicle_brand") or "",
        ev.get("vehicle_model") or "",
        ev.get("vehicle_ev_id") or "",
        ev.get("tariff_id") or "",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Batch writer
# ─────────────────────────────────────────────────────────────────────────────

_INSERT_SQL = """
INSERT INTO {db}.charging_events (
    event_id, event_type, timestamp,
    station_id, connector_id, session_id, network_id,
    city, country, latitude, longitude,
    charger_type, power_kw, energy_kwh, soc_percent, vehicle_type,
    duration_minutes, price_per_kwh, revenue_eur,
    status, error_code, component, voltage_v, current_a,
    vehicle_brand, vehicle_model, vehicle_ev_id, tariff_id
) VALUES
""".format(db=CLICKHOUSE.database)


class BatchWriter:
    """Thread-safe accumulator + flusher for ClickHouse inserts."""

    def __init__(self) -> None:
        self._client = CH(
            host=CLICKHOUSE.host,
            port=CLICKHOUSE.port,
            database=CLICKHOUSE.database,
            user=CLICKHOUSE.user,
            password=CLICKHOUSE.password,
            settings={
                "max_insert_block_size": 200_000,
                "insert_quorum":         0,
            },
        )
        self._buf:  List[tuple] = []
        self._lock = threading.Lock()
        self._total_rows = 0
        self._flush_errors = 0

        log.info("Connected to ClickHouse at %s:%d  db=%s",
                 CLICKHOUSE.host, CLICKHOUSE.port, CLICKHOUSE.database)

    # ── public API ────────────────────────────────────────────────────────────

    def add(self, row: tuple) -> None:
        flush_now = False
        with self._lock:
            self._buf.append(row)
            if len(self._buf) >= CLICKHOUSE.insert_batch_size:
                flush_now = True

        if flush_now:
            self._do_flush()

    def flush(self) -> None:
        """Force-flush whatever is in the buffer."""
        self._do_flush()

    def flush_loop(self, stop_event: threading.Event) -> None:
        """Background thread: flush on a timer."""
        while not stop_event.is_set():
            time.sleep(CLICKHOUSE.insert_flush_interval_s)
            self._do_flush()

    @property
    def total_rows(self) -> int:
        return self._total_rows

    # ── private ───────────────────────────────────────────────────────────────

    def _do_flush(self) -> None:
        with self._lock:
            if not self._buf:
                return
            batch, self._buf = self._buf, []
            try:
                self._client.execute(_INSERT_SQL, batch)
                self._total_rows += len(batch)
                log.debug("Flushed %d rows (total %s)", len(batch), f"{self._total_rows:,}")
            except Exception as exc:
                self._flush_errors += 1
                log.error("ClickHouse insert error (%d total): %s", self._flush_errors, exc)
                # Re-queue up to 5 k rows to avoid data loss on transient errors
                self._buf = batch[:5_000] + self._buf


# ─────────────────────────────────────────────────────────────────────────────
# Consumer loop
# ─────────────────────────────────────────────────────────────────────────────

def run(
    bootstrap_servers: str = KAFKA.bootstrap_servers,
    group_id:          str = KAFKA.consumer_group_clickhouse,
    batch_size:        int = KAFKA.consumer_batch_size,
) -> None:
    log.info("Starting ClickHouse consumer  group=%s  batch=%d", group_id, batch_size)

    consumer = Consumer({
        "bootstrap.servers":      bootstrap_servers,
        "group.id":               group_id,
        "auto.offset.reset":      "latest",
        "enable.auto.commit":     True,
        "auto.commit.interval.ms":5000,
        "fetch.min.bytes":        65_536,
        "fetch.wait.max.ms":      1000,
        "max.poll.interval.ms":   300_000,
    })
    for attempt in range(30):
        meta = consumer.list_topics(KAFKA.topic_charging_events, timeout=5)
        topic_meta = meta.topics.get(KAFKA.topic_charging_events)
        if topic_meta and topic_meta.partitions and not topic_meta.error:
            break
        log.info("Waiting for topic '%s' to be ready... (%d/30)", KAFKA.topic_charging_events, attempt + 1)
        time.sleep(2)
    partitions = [
        TopicPartition(KAFKA.topic_charging_events, p, OFFSET_END)
        for p in meta.topics[KAFKA.topic_charging_events].partitions
    ]
    consumer.assign(partitions)
    log.info("Assigned %d partitions (from latest) for topic %s", len(partitions), KAFKA.topic_charging_events)

    writer      = BatchWriter()
    dead_letter = DeadLetterWriter()
    # In-memory-only dedup: ClickHouse ReplacingMergeTree handles any
    # slip-through duplicates, so no Redis round-trip needed here.
    dedup       = Deduplicator()
    watermark   = Watermark()
    stop_evt    = threading.Event()
    flush_thr   = threading.Thread(target=writer.flush_loop, args=(stop_evt,), daemon=True)
    flush_thr.start()

    shutdown = False

    def _sig(_s, _f):
        nonlocal shutdown
        log.info("Shutdown requested.")
        shutdown = True
        stop_evt.set()

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    total   = 0
    t_start = time.monotonic()
    t_report = t_start

    try:
        while not shutdown:
            try:
                msgs = consumer.consume(num_messages=batch_size, timeout=1.0)
            except KeyboardInterrupt:
                break
            if not msgs:
                continue

            for m in msgs:
                if m.error():
                    log.error("Kafka error: %s", m.error())
                    continue

                # ── deserialise ───────────────────────────────────────────
                try:
                    ev = json.loads(m.value())
                except json.JSONDecodeError as exc:
                    dead_letter.write(
                        m.value().decode("utf-8", errors="replace"),
                        f"json_decode_error:{exc}",
                    )
                    continue

                # ── schema validation ─────────────────────────────────────
                reason = validate(ev)
                if reason:
                    dead_letter.write(ev, reason)
                    continue

                # ── deduplication ─────────────────────────────────────────
                if dedup.is_duplicate(ev["event_id"]):
                    continue

                # ── late-event detection (event-time watermark) ───────────
                event_ts = _parse_ts(ev.get("timestamp"))
                watermark.advance(event_ts)
                if watermark.is_late(event_ts):
                    dead_letter.write(
                        ev,
                        f"late_event:{watermark.seconds_late(event_ts):.0f}s",
                    )
                    continue

                try:
                    writer.add(event_to_row(ev))
                    total += 1
                except Exception as exc:
                    log.warning("Row conversion failed: %s", exc)
                    dead_letter.write(ev, f"row_conversion_error:{exc}")

            now = time.monotonic()
            if now - t_report >= 15.0:
                eps = total / (now - t_start)
                log.info(
                    "Processed %s events (%.0f eps)  CH rows: %s  errors: %d  "
                    "dead_letter=%d  dedup=%.1f%%  late=%.1f%%",
                    f"{total:,}", eps, f"{writer.total_rows:,}", writer._flush_errors,
                    dead_letter.count, dedup.hit_rate * 100, watermark.late_rate * 100,
                )
                t_report = now
    finally:
        writer.flush()
        consumer.close()
        log.info("ClickHouse consumer stopped. Total: %s  CH rows: %s",
                 f"{total:,}", f"{writer.total_rows:,}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="ChargeSquare ClickHouse Consumer")
    ap.add_argument("--bootstrap-servers", default=KAFKA.bootstrap_servers)
    ap.add_argument("--group-id",          default=KAFKA.consumer_group_clickhouse)
    ap.add_argument("--batch-size", type=int, default=KAFKA.consumer_batch_size)
    args = ap.parse_args()
    run(args.bootstrap_servers, args.group_id, args.batch_size)
