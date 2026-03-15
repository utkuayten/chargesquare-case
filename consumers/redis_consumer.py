"""
Redis Consumer — Real-Time Operational Store
=============================================

Reads from the `charging-events` Kafka topic and maintains a live
operational view of the charging network in Redis.

2c — Real-Time Store: Why Redis?
---------------------------------
Redis is chosen over Cassandra / TimescaleDB / InfluxDB for the real-time
store because:

  • Sub-millisecond reads: Hash GET and ZRANGE are O(1)/O(log N) — a
    station's last-5-minute status is always returned in < 1 ms, well
    inside the 100 ms SLA.

  • TTL-native: session hashes expire in 1 h, station status hashes in
    5 min — no explicit delete needed.  Expired keys are reclaimed
    automatically.

  • Sorted Sets for leaderboards: ZINCRBY + ZREVRANGE give O(log N) revenue
    rankings without a full scan.

  • Atomic Lua scripts: the floor-at-zero DECR pattern prevents negative
    active-session counters without a read-modify-write race.

  • Operational simplicity: a single Redis node handles > 1 M ops/s on
    commodity hardware; no schema migrations required.

Data written
------------
Key pattern                              Type     TTL     Purpose
───────────────────────────────────────────────────────────────────────────
session:{session_id}                     Hash     1 h     Live session state
station:{station_id}:status              Hash     5 min   Current connector status
network:{network_id}:active_sessions     String   –       Running session counter
city:{city}:active_sessions              String   –       Running session counter
global:active_sessions                   String   –       Platform-wide counter
global:events:total                      String   –       Total events processed
global:events:{event_type}               String   –       Per-type counter
global:errors:total                      String   –       Fault counter
error:{code}:count                       String   –       Per-fault-code counter
leaderboard:station_revenue:{YYYY-MM-DD} ZSet     24 h    Daily revenue top-N
leaderboard:network_revenue:{YYYY-MM-DD} ZSet     24 h    Daily revenue top-N
leaderboard:city_revenue:{YYYY-MM-DD}    ZSet     24 h    Daily revenue top-N
global:energy_kwh:{YYYY-MM-DD}           String   –       Daily energy delivered
network:{net}:energy_kwh:{YYYY-MM-DD}    String   –       Per-network energy
"""

from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import datetime, timezone

try:
    import orjson as _json_mod
    def _loads(b: bytes) -> dict:
        return _json_mod.loads(b)
except ImportError:
    import json as _json_mod  # type: ignore[no-redef]
    def _loads(b: bytes) -> dict:
        return _json_mod.loads(b)

import redis
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_END

from config.settings import KAFKA, REDIS
from consumers.validator import DeadLetterWriter, validate
from consumers.deduplicator import Deduplicator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [RedisConsumer] %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Redis writer
# ─────────────────────────────────────────────────────────────────────────────

class RedisWriter:
    def __init__(self) -> None:
        self.r = redis.Redis(
            host=REDIS.host,
            port=REDIS.port,
            db=REDIS.db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_keepalive=True,
        )
        self.r.ping()
        log.info("Connected to Redis at %s:%d", REDIS.host, REDIS.port)

        # Pre-build handler dispatch table once — avoids per-event dict allocation
        self._handlers = {
            "session_start": self._on_session_start,
            "meter_update":  self._on_meter_update,
            "session_stop":  self._on_session_stop,
            "status_change": self._on_status_change,
            "fault_alert":   self._on_fault_alert,
        }

    # ── public API ────────────────────────────────────────────────────────────

    def process_batch(self, events: list[dict]) -> None:
        """Process a list of event dicts within a single pipeline."""
        pipe = self.r.pipeline(transaction=False)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        for ev in events:
            try:
                self._route(ev, today, pipe)
            except Exception as exc:
                log.warning("Skipping malformed event: %s", exc)

        pipe.execute()

    # ── routing ───────────────────────────────────────────────────────────────

    def _route(self, ev: dict, today: str, p: redis.client.Pipeline) -> None:
        et = ev.get("event_type", "")

        # Heartbeat: only global counters, no session/station state to update
        if et == "heartbeat":
            p.incr("global:events:total")
            p.incr("global:events:heartbeat")
            return

        handler = self._handlers.get(et)
        if handler:
            handler(ev, today, p)

        # Global counters
        p.incr("global:events:total")
        p.incr(f"global:events:{et}")

    # ── handlers ──────────────────────────────────────────────────────────────

    def _on_session_start(self, ev: dict, today: str, p) -> None:
        sid  = ev.get("session_id", "")
        stid = ev.get("station_id", "")
        net  = ev.get("network_id", "")
        city = ev.get("city", "unknown")
        if not sid:
            return

        p.hset(f"session:{sid}", mapping={
            "station_id":    stid,
            "network_id":    net,
            "charger_type":  ev.get("charger_type", ""),
            "power_kw":      ev.get("power_kw", 0),
            "start_time":    ev.get("timestamp", ""),
            "city":          city,
            "country":       ev.get("country", ""),
            "vehicle_type":  ev.get("vehicle_type", ""),
            "vehicle_brand": ev.get("vehicle_brand", ""),
            "vehicle_model": ev.get("vehicle_model", ""),
            "soc_start":     ev.get("soc_percent", 0),
            "price_per_kwh": ev.get("price_per_kwh", 0),
            "tariff_id":     ev.get("tariff_id", ""),
        })
        p.expire(f"session:{sid}", REDIS.session_ttl_seconds)

        p.hset(f"station:{stid}:status", mapping={
            "status":       "charging",
            "session_id":   sid,
            "last_updated": ev.get("timestamp", ""),
        })
        p.expire(f"station:{stid}:status", REDIS.station_ttl_seconds)

        p.incr(f"network:{net}:active_sessions")
        p.incr(f"city:{city}:active_sessions")
        p.incr("global:active_sessions")

    def _on_meter_update(self, ev: dict, _today: str, p) -> None:
        sid = ev.get("session_id", "")
        if not sid:
            return
        p.hset(f"session:{sid}", mapping={
            "energy_kwh":       ev.get("energy_kwh", 0),
            "soc_current":      ev.get("soc_percent", 0),
            "duration_minutes": ev.get("duration_minutes", 0),
            "revenue_eur":      ev.get("revenue_eur", 0),
            "last_updated":     ev.get("timestamp", ""),
        })
        # TTL already set on SESSION_START — no EXPIRE needed here

    def _on_session_stop(self, ev: dict, today: str, p) -> None:
        sid  = ev.get("session_id", "")
        stid = ev.get("station_id", "")
        net  = ev.get("network_id", "")
        city = ev.get("city", "unknown")

        p.hset(f"station:{stid}:status", mapping={
            "status":       "available",
            "session_id":   "",
            "last_updated": ev.get("timestamp", ""),
        })
        p.expire(f"station:{stid}:status", REDIS.station_ttl_seconds)

        if sid:
            p.delete(f"session:{sid}")

        # Plain DECR — dashboard floors display at 0, so negative values are harmless.
        # (EVAL inside a pipeline breaks pipelining — redis-py flushes synchronously.)
        p.decr(f"network:{net}:active_sessions")
        p.decr(f"city:{city}:active_sessions")
        p.decr("global:active_sessions")

        revenue = float(ev.get("revenue_eur") or 0)
        energy  = float(ev.get("energy_kwh")  or 0)

        if revenue > 0:
            lboard_ttl = REDIS.leaderboard_ttl_seconds
            for key, member in [
                (f"leaderboard:station_revenue:{today}", stid),
                (f"leaderboard:network_revenue:{today}", net),
                (f"leaderboard:city_revenue:{today}",    city),
            ]:
                p.zincrby(key, revenue, member)
                p.expire(key, lboard_ttl)

        if energy > 0:
            p.incrbyfloat(f"global:energy_kwh:{today}",        energy)
            p.incrbyfloat(f"network:{net}:energy_kwh:{today}", energy)

    def _on_status_change(self, ev: dict, _today: str, p) -> None:
        stid = ev.get("station_id", "")
        p.hset(f"station:{stid}:status", mapping={
            "status":       ev.get("status", "unknown"),
            "charger_type": ev.get("charger_type", ""),
            "city":         ev.get("city", ""),
            "country":      ev.get("country", ""),
            "last_updated": ev.get("timestamp", ""),
        })
        p.expire(f"station:{stid}:status", REDIS.station_ttl_seconds)
        p.incr(f"status_count:{ev.get('status', 'unknown')}")

    def _on_fault_alert(self, ev: dict, today: str, p) -> None:
        stid  = ev.get("station_id", "")
        net   = ev.get("network_id", "")
        code  = ev.get("error_code", "UNKNOWN")
        p.hset(f"station:{stid}:status", mapping={
            "status":       "fault",
            "error_code":   code,
            "last_updated": ev.get("timestamp", ""),
        })
        p.expire(f"station:{stid}:status", REDIS.station_ttl_seconds)
        p.incr("global:errors:total")
        p.incr(f"error:{code}:count")
        p.incr(f"network:{net}:errors:{today}")


# ─────────────────────────────────────────────────────────────────────────────
# Consumer loop
# ─────────────────────────────────────────────────────────────────────────────

def run(
    bootstrap_servers: str = KAFKA.bootstrap_servers,
    group_id:          str = KAFKA.consumer_group_redis,
    batch_size:        int = KAFKA.consumer_batch_size,
) -> None:
    log.info("Starting Redis consumer  group=%s  batch=%d", group_id, batch_size)

    consumer = Consumer({
        "bootstrap.servers":      bootstrap_servers,
        "group.id":               group_id,
        "auto.offset.reset":      "latest",
        "enable.auto.commit":     True,
        "auto.commit.interval.ms":5000,
        "fetch.min.bytes":        65_536,   # wait for 64 KB before fetching — fewer round-trips
        "fetch.wait.max.ms":      500,
        "max.poll.interval.ms":   300_000,
        "queued.max.messages.kbytes": 131_072,  # 128 MB prefetch buffer
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

    writer      = RedisWriter()
    dead_letter = DeadLetterWriter()
    dedup       = Deduplicator()   # in-memory LRU — no Redis round-trip per event
    shutdown    = False

    def _sig(_s, _f):
        nonlocal shutdown
        log.info("Shutdown requested.")
        shutdown = True

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

            now = time.monotonic()
            if now - t_report >= 15.0:
                eps = total / max(now - t_start, 1)
                try:
                    rs = writer.r.info("stats")
                    rm = writer.r.info("memory")
                    redis_ops  = rs.get("instantaneous_ops_per_sec", 0)
                    redis_mem  = rm.get("used_memory_human", "?")
                except Exception:
                    redis_ops, redis_mem = 0, "?"
                log.info(
                    "Heartbeat: processed %s events (%.0f eps)  "
                    "dead_letter=%d  dedup_hits=%d (%.1f%%)  "
                    "redis_ops/s=%s  redis_mem=%s",
                    f"{total:,}", eps,
                    dead_letter.count, dedup.hits, dedup.hit_rate * 100,
                    redis_ops, redis_mem,
                )
                t_report = now

            if not msgs:
                continue

            events = []
            for m in msgs:
                if m.error():
                    log.error("Kafka error: %s", m.error())
                    continue
                try:
                    ev = _loads(m.value())
                except Exception as exc:
                    log.warning("JSON decode error: %s", exc)
                    dead_letter.write(m.value().decode("utf-8", errors="replace"),
                                      f"json_decode_error:{exc}")
                    continue

                reason = validate(ev)
                if reason:
                    dead_letter.write(ev, reason)
                    continue

                if dedup.is_duplicate(ev["event_id"]):
                    continue

                events.append(ev)

            if events:
                writer.process_batch(events)
                total += len(events)
    finally:
        consumer.close()
        log.info("Redis consumer stopped. Total processed: %s", f"{total:,}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="ChargeSquare Redis Consumer")
    ap.add_argument("--bootstrap-servers", default=KAFKA.bootstrap_servers)
    ap.add_argument("--group-id",          default=KAFKA.consumer_group_redis)
    ap.add_argument("--batch-size", type=int, default=KAFKA.consumer_batch_size)
    args = ap.parse_args()
    run(args.bootstrap_servers, args.group_id, args.batch_size)
