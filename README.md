# ChargeSquare — EV Charging Data Pipeline

End-to-end real-time and analytics data pipeline for EV charging station events,
covering data generation, transport, storage, and insights.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Simulator  (10 k – 100 k events/sec)                                   │
│  N × Python processes, each with a confluent-kafka Producer             │
└────────────────────────────┬────────────────────────────────────────────┘
                             │  Kafka topic: charging-events (12 partitions)
                             ▼
               ┌─────────────────────────┐
               │         Kafka           │
               │  KRaft mode, snappy     │
               │  compression, 24 h TTL  │
               └──────┬──────────────────┘
              ┌───────┘         └───────────────────┐
              ▼                                     ▼
 ┌────────────────────────┐         ┌───────────────────────────────┐
 │  Redis Consumer         │         │  ClickHouse Consumer          │
 │  Pipeline batching      │         │  Batch insert 10 k rows/5 s   │
 └────────────┬────────────┘         └──────────────┬────────────────┘
              ▼                                     ▼
 ┌────────────────────────┐         ┌───────────────────────────────┐
 │  Redis 7               │         │  ClickHouse 23.x              │
 │  Real-time store        │         │  Analytics store              │
 │  Active sessions        │         │  90-day MergeTree             │
 │  Station status         │         │  Materialized views           │
 │  Revenue leaderboards   │         │  (hourly / daily aggregates)  │
 └────────────────────────┘         └───────────────────────────────┘
              │                                     │
              └──────────────┬──────────────────────┘
                             ▼
               ┌─────────────────────────┐
               │  Analytics Dashboard     │
               │  Rich terminal UI        │
               │  9 insight dimensions    │
               └─────────────────────────┘
```

### Components

| Component | Technology | Role |
|-----------|-----------|------|
| **Simulator** | Python + confluent-kafka | Generates 10 k – 100 k events/sec across 10 k stations |
| **Transport** | Apache Kafka (KRaft) | Durable ordered message bus, 12 partitions |
| **Real-time store** | Redis 7 | Operational view: active sessions, station status, leaderboards |
| **Analytics store** | ClickHouse | Historical analysis, materialized views, sub-second OLAP queries |
| **Dashboard** | Rich (Python) | Terminal UI with 9 analytical dimensions |

---

## Data Model

### Event Schema

Every event published to Kafka follows this JSON schema:

```json
{
  "event_id":         "uuid4",
  "event_type":       "session_start | session_update | session_end | station_status | error",
  "timestamp":        "2024-06-15T14:30:00.123+00:00",
  "station_id":       "IONITY_S00042",
  "connector_id":     2,
  "session_id":       "uuid4",
  "network_id":       "IONITY",
  "city":             "Berlin",
  "country":          "DE",
  "latitude":         52.519,
  "longitude":        13.411,
  "charger_type":     "DC_FAST",
  "power_kw":         150.0,
  "energy_kwh":       45.2,
  "soc_percent":      80,
  "vehicle_type":     "BEV",
  "duration_minutes": 30,
  "price_per_kwh":    0.79,
  "revenue_eur":      35.71,
  "status":           "charging",
  "error_code":       null
}
```

### Event Types & Distribution

| Event Type | Share | Description |
|------------|------:|-------------|
| `session_update` | 58% | Heartbeat with current energy / SoC reading |
| `session_end`    | 18% | Session completed — full revenue and energy data |
| `session_start`  | 12% | EV connects, SoC reading |
| `station_status` |  9% | Connector availability broadcast |
| `error`          |  3% | Fault with OCPP-style error code |

### Charging Networks (CPOs)

10 European operators: IONITY, TESLA_SC, ChargePoint, EVBox, Allego,
Fastned, Recharge, Shell Recharge, bp pulse, Virta.

### Geographic Coverage

20 cities across DE, NL, FR, GB, NO, SE, DK, BE, AT, CH, ES.

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+

### 1 — Start infrastructure

```bash
make up          # starts Kafka, Redis, ClickHouse, Kafka-UI
```

Services:
- Kafka broker     → `localhost:9094`
- Redis            → `localhost:6379`
- ClickHouse HTTP  → `http://localhost:8123`
- Kafka UI         → `http://localhost:8080`

### 2 — Install Python dependencies

```bash
make install
```

### 3 — Start consumers (separate terminals)

```bash
make consumer-redis   # real-time Redis writer
make consumer-ch      # ClickHouse analytics writer
```

### 4 — Run the simulator

```bash
make producer          # 10,000 events/sec (4 workers)
make producer-fast     # 50,000 events/sec (8 workers)
make producer-max      # 100,000 events/sec (16 workers)
```

Or with custom parameters:

```bash
python -m simulator.producer --eps 25000 --workers 6 --duration 120
```

### 5 — View analytics

```bash
make report            # one-shot analytics report (9 dimensions)
make dashboard         # live terminal dashboard (refreshes every 30s)
```

### 6 — Run benchmark

```bash
python scripts/benchmark.py              # generator-only (no Kafka needed)
python scripts/benchmark.py --kafka      # also measures Kafka throughput
```

---

## Analytics Insights

The pipeline surfaces 9 analytical dimensions:

1. **Revenue by Network** — CPO league table (sessions, revenue, energy, avg duration)
2. **Sessions per Hour** — intra-day demand curve (last 24 h)
3. **Charger Utilisation** — utilisation & yield by hardware class (AC L1/L2, DC Fast, Ultra-Fast)
4. **Top Cities by Energy** — geographic demand heat-map (top 10 cities)
5. **Fault Diagnostics** — error frequency, affected stations, % share per fault code
6. **Vehicle Type Breakdown** — BEV vs PHEV session behaviour
7. **Session Duration Distribution** — dwell-time buckets (< 15 min to ≥ 2 h)
8. **Peak Hours Analysis** — hour-of-day traffic pattern with ASCII bar chart
9. **Network Reliability** — error rate % per CPO

---

## Redis Operational Store

Key patterns maintained in Redis:

| Key | Type | TTL | Content |
|-----|------|-----|---------|
| `session:{id}` | Hash | 1 h | Live session state (energy, SoC, duration) |
| `station:{id}:status` | Hash | 5 min | Current connector status |
| `network:{id}:active_sessions` | String | — | Running session counter |
| `global:active_sessions` | String | — | Platform-wide counter |
| `global:events:total` | String | — | Total events processed |
| `leaderboard:station_revenue:{date}` | Sorted Set | 24 h | Daily revenue top-N |
| `leaderboard:network_revenue:{date}` | Sorted Set | 24 h | CPO revenue ranking |
| `global:energy_kwh:{date}` | String | — | Daily energy delivered |

---

## ClickHouse Schema

### Main table: `charging_events`

- Engine: `MergeTree`, partitioned by `toYYYYMM(timestamp)`
- Sort key: `(timestamp, network_id, station_id)` — optimises time-range + CPO queries
- TTL: 90 days
- `LowCardinality` applied to high-repetition string columns for dictionary compression

### Materialized views (pre-computed aggregates)

| View | Engine | Aggregation | Purpose |
|------|--------|-------------|---------|
| `sessions_hourly_mv` | SummingMergeTree | Per-hour, per-network, per-city | Trend charts |
| `station_daily_mv` | SummingMergeTree | Per-day, per-station | Station performance |
| `errors_daily_mv` | SummingMergeTree | Per-day, per-error-code | Fault tracking |
| `network_revenue_daily_mv` | SummingMergeTree | Per-day, per-network | Revenue reporting |

---

## Configuration

All settings are in `config/settings.py` and can be overridden via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka broker address |
| `REDIS_HOST` | `localhost` | Redis host |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `TARGET_EPS` | `10000` | Simulator target events/sec |
| `NUM_PRODUCERS` | `4` | Number of producer worker processes |
| `KAFKA_BATCH_SIZE` | `131072` | Producer batch size (bytes) |
| `KAFKA_LINGER_MS` | `20` | Producer linger before sending |
| `CH_BATCH_SIZE` | `10000` | ClickHouse insert batch size (rows) |
| `CH_FLUSH_INTERVAL` | `5` | ClickHouse flush interval (seconds) |

---

## Performance Notes

### Achieving 100 k events/sec

The key bottleneck is the Python GIL. To reach 100 k eps:

1. **Multiple producer processes** — each process owns a Kafka producer and bypasses the GIL
2. **Batch generation** — `generate_batch(N)` amortises function call overhead
3. **Pre-computed weights** — `random.choices` with pre-built weight lists is faster than per-call sampling
4. **Kafka producer tuning** — `linger.ms=20`, `batch.size=128 KB`, `compression.type=snappy` raise throughput significantly
5. **Snappy compression** — reduces network bandwidth by ~60% for JSON payloads

Typical throughput (4-core laptop):
- Generator alone: ~500 k–800 k events/sec
- With Kafka: ~60 k–120 k events/sec (4 workers)

### ClickHouse write strategy

ClickHouse is designed for bulk inserts. The consumer accumulates rows in memory
and flushes in batches of 10 k rows every 5 seconds, which gives optimal
column compression and avoids too-many-parts merges.

---

## Running Tests

```bash
make test
# or
python -m pytest tests/ -v
```

Tests cover the generator (event shapes, distributions, serialisation) and
the ClickHouse row converter (type coercion, null handling, timestamp parsing).
No external services required for the unit tests.

---

## Directory Structure

```
chargesquare/
├── docker-compose.yml          Infrastructure (Kafka, Redis, ClickHouse, Kafka-UI)
├── requirements.txt
├── Makefile                    Convenience targets
│
├── config/
│   └── settings.py             Centralised configuration (env-var overridable)
│
├── simulator/
│   ├── models.py               ChargingEvent dataclass + type constants
│   ├── generators.py           EventGenerator + StationRegistry
│   └── producer.py             Multi-process Kafka producer (CLI entry point)
│
├── consumers/
│   ├── redis_consumer.py       Reads Kafka → writes Redis real-time state
│   └── clickhouse_consumer.py  Reads Kafka → bulk inserts ClickHouse
│
├── analytics/
│   ├── queries.py              12 analytical query methods (ChargingAnalytics)
│   └── dashboard.py            Rich terminal dashboard + one-shot report (CLI)
│
├── scripts/
│   ├── init_clickhouse.sql     Schema DDL + 4 materialized views
│   └── benchmark.py            Generator + Kafka throughput benchmarking
│
└── tests/
    ├── test_generators.py      Unit tests for event generation
    └── test_clickhouse_consumer.py  Unit tests for row conversion
```
