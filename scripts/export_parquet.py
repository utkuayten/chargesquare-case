"""
Export ClickHouse charging_events to Parquet
=============================================
Reads all (or a date-filtered slice of) events from ClickHouse and writes
them to a Parquet file using PyArrow.

Usage:
    python scripts/export_parquet.py                        # full export
    python scripts/export_parquet.py --date 2026-03-14      # single day
    python scripts/export_parquet.py --out /tmp/events.parquet
    make export-parquet
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from clickhouse_driver import Client

sys.path.insert(0, ".")
from config.settings import CLICKHOUSE

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# Column name → PyArrow type mapping (matches charging_events DDL)
SCHEMA = pa.schema([
    ("event_id",        pa.string()),
    ("event_type",      pa.string()),
    ("timestamp",       pa.timestamp("ms", tz="UTC")),
    ("station_id",      pa.string()),
    ("connector_id",    pa.uint8()),
    ("session_id",      pa.string()),
    ("network_id",      pa.string()),
    ("city",            pa.string()),
    ("country",         pa.string()),
    ("latitude",        pa.float32()),
    ("longitude",       pa.float32()),
    ("charger_type",    pa.string()),
    ("power_kw",        pa.float32()),
    ("energy_kwh",      pa.float32()),
    ("soc_percent",     pa.uint8()),
    ("vehicle_type",    pa.string()),
    ("duration_minutes",pa.uint16()),
    ("price_per_kwh",   pa.float32()),
    ("revenue_eur",     pa.float32()),
    ("status",          pa.string()),
    ("error_code",      pa.string()),
    ("component",       pa.string()),
    ("voltage_v",       pa.float32()),
    ("current_a",       pa.float32()),
    ("vehicle_brand",   pa.string()),
    ("vehicle_model",   pa.string()),
    ("vehicle_ev_id",   pa.string()),
    ("tariff_id",       pa.string()),
])

COLUMNS = [field.name for field in SCHEMA]


def _flush(writer: pq.ParquetWriter, rows: list) -> None:
    """Convert a list of row-tuples to a PyArrow table and write one row-group."""
    cols = list(zip(*rows))
    arrays = [pa.array(col, type=SCHEMA.field(name).type)
              for col, name in zip(cols, COLUMNS)]
    writer.write_table(pa.table({n: a for n, a in zip(COLUMNS, arrays)}, schema=SCHEMA))


def export(out_path: Path, date_filter: str | None = None, batch_size: int = 200_000) -> None:
    client = Client(
        host=CLICKHOUSE.host,
        port=CLICKHOUSE.port,
        database=CLICKHOUSE.database,
        user=CLICKHOUSE.user,
        password=CLICKHOUSE.password,
    )

    where = f"WHERE toDate(timestamp) = '{date_filter}'" if date_filter else ""
    count_row = client.execute(
        f"SELECT count() FROM {CLICKHOUSE.database}.charging_events {where}"
    )
    total = count_row[0][0]
    log.info("Exporting %d rows → %s", total, out_path)

    query = f"SELECT {', '.join(COLUMNS)} FROM {CLICKHOUSE.database}.charging_events {where} ORDER BY timestamp"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(str(out_path), SCHEMA, compression="snappy")
    exported = 0
    t0 = time.monotonic()
    buf: list = []

    try:
        for row in client.execute_iter(query, settings={"max_block_size": 10_000}):
            buf.append(row)
            if len(buf) >= batch_size:
                _flush(writer, buf)
                exported += len(buf)
                buf = []
                log.info("  written %d / %d rows (%.1f%%)", exported, total,
                         exported * 100 / max(total, 1))
        if buf:
            _flush(writer, buf)
            exported += len(buf)
    finally:
        writer.close()

    elapsed = time.monotonic() - t0
    size_mb = out_path.stat().st_size / 1_048_576
    log.info("Done — %d rows, %.1f MB, %.1fs", exported, size_mb, elapsed)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Export charging_events to Parquet")
    p.add_argument("--out",  default="exports/charging_events.parquet",
                   help="Output file path (default: exports/charging_events.parquet)")
    p.add_argument("--date", default=None,
                   help="Filter to a single date YYYY-MM-DD (default: all data)")
    p.add_argument("--batch-size", type=int, default=200_000,
                   help="Rows per Parquet row-group (default: 200,000)")
    args = p.parse_args()
    export(Path(args.out), date_filter=args.date, batch_size=args.batch_size)
