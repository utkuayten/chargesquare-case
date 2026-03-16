"""
ChargeSquare Pipeline Configuration

Priority order (highest → lowest):
  1. Environment variables
  2. config/simulator.yaml
  3. Hard-coded defaults below
"""

import os
from dataclasses import dataclass, field
from typing import List, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# YAML loader (optional — falls back gracefully if PyYAML not installed or
# if the file is absent)
# ─────────────────────────────────────────────────────────────────────────────

def _load_yaml() -> dict:
    yaml_path = os.path.join(os.path.dirname(__file__), "simulator.yaml")
    try:
        import yaml
        with open(yaml_path) as f:
            return (yaml.safe_load(f) or {}).get("simulator", {})
    except (FileNotFoundError, ImportError, Exception):
        return {}


_Y = _load_yaml()


def _y(*keys, default=None):
    """Retrieve a nested value from the YAML dict, e.g. _y('session','duration_minutes','min')."""
    node = _Y
    for k in keys:
        if not isinstance(node, dict):
            return default
        node = node.get(k)
        if node is None:
            return default
    return node


# ─────────────────────────────────────────────────────────────────────────────
# Config dataclasses
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

    topic_charging_events: str = "charging-events"
    topic_station_status:  str = "station-status"

    # Producer tuning — optimised for high throughput
    producer_batch_size:        int = int(os.getenv("KAFKA_BATCH_SIZE",    "131072"))
    producer_linger_ms:         int = int(os.getenv("KAFKA_LINGER_MS",     "20"))
    producer_compression_type:  str = os.getenv("KAFKA_COMPRESSION",       "snappy")
    producer_acks:              str = os.getenv("KAFKA_ACKS",               "1")
    producer_buffer_mb:         int = int(os.getenv("KAFKA_BUFFER_MB",     "512"))

    # Consumer groups
    consumer_group_redis:      str = "redis-writer-v1"
    consumer_group_clickhouse: str = "clickhouse-writer-v1"

    consumer_batch_size: int = int(os.getenv("KAFKA_CONSUMER_BATCH", "5000"))


@dataclass
class RedisConfig:
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db:   int = 0

    session_ttl_seconds:      int = 3600
    station_ttl_seconds:      int = 300
    leaderboard_ttl_seconds:  int = 86400


@dataclass
class ClickHouseConfig:
    host:     str = os.getenv("CLICKHOUSE_HOST",     "localhost")
    port:     int = int(os.getenv("CLICKHOUSE_PORT", "9000"))
    database: str = os.getenv("CLICKHOUSE_DB",       "chargesquare")
    user:     str = os.getenv("CLICKHOUSE_USER",     "default")
    password: str = os.getenv("CLICKHOUSE_PASSWORD", "")

    insert_batch_size:       int = int(os.getenv("CH_BATCH_SIZE",    "10000"))
    insert_flush_interval_s: int = int(os.getenv("CH_FLUSH_INTERVAL","5"))


@dataclass
class SimulatorConfig:
    # ── Station topology ──────────────────────────────────────────────────────
    num_networks: int = int(os.getenv(
        "SIM_NETWORKS", str(_y("stations", "networks", default=10))))
    stations_per_network: int = int(os.getenv(
        "SIM_STATIONS_PER_NET", str(_y("stations", "per_network", default=100))))
    connectors_per_station: int = int(os.getenv(
        "SIM_CONNECTORS", str(_y("stations", "connectors_per_station", default=4))))
    fault_rate_pct: float = float(os.getenv(
        "SIM_FAULT_RATE_PCT", str(_y("stations", "fault_rate_pct", default=0.3))))

    # ── Session lifecycle ─────────────────────────────────────────────────────
    session_duration_min: int = int(os.getenv(
        "SIM_SESSION_DUR_MIN", str(_y("session", "duration_minutes", "min", default=10))))
    session_duration_max: int = int(os.getenv(
        "SIM_SESSION_DUR_MAX", str(_y("session", "duration_minutes", "max", default=120))))
    meter_updates_min: int = int(os.getenv(
        "SIM_METER_MIN", str(_y("session", "meter_updates", "min", default=3))))
    meter_updates_max: int = int(os.getenv(
        "SIM_METER_MAX", str(_y("session", "meter_updates", "max", default=15))))

    soc_start_range: Tuple[int, int] = field(default_factory=lambda: tuple(
        _y("session", "soc_start_pct") or [5, 50]))
    soc_end_range:   Tuple[int, int] = field(default_factory=lambda: tuple(
        _y("session", "soc_end_pct") or [60, 100]))

    # ── Traffic shaping ───────────────────────────────────────────────────────
    peak_hours: List[Tuple] = field(default_factory=lambda: [
        tuple(h) for h in (_y("traffic", "peak_hours") or [[7, 9, 2.0], [12, 14, 1.5], [17, 20, 3.0]])])
    peak_multiplier:  float = float(os.getenv(
        "SIM_PEAK_MULT", str(_y("traffic", "peak_multiplier", default=3.0))))
    session_fraction: float = float(os.getenv(
        "SIM_SESSION_FRAC", str(_y("traffic", "session_fraction", default=0.08))))

    # ── Producer ──────────────────────────────────────────────────────────────
    target_events_per_second: int = int(os.getenv(
        "TARGET_EPS", str(_y("producer", "target_eps", default=10000))))
    num_producer_processes: int = int(os.getenv(
        "NUM_PRODUCERS", str(_y("producer", "num_workers", default=4))))


# ─────────────────────────────────────────────────────────────────────────────
# Singletons
# ─────────────────────────────────────────────────────────────────────────────

KAFKA      = KafkaConfig()
REDIS      = RedisConfig()
CLICKHOUSE = ClickHouseConfig()
SIMULATOR  = SimulatorConfig()
