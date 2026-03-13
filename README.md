# ChargeSquare — EV Charging Data Pipeline

End-to-end real-time and analytics pipeline for EV charging station events — generation, transport, storage, and insights.

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│  Simulator  (1k – 100k events/sec)                   │
│  N × Python processes, each with a Kafka Producer    │
└─────────────────────┬────────────────────────────────┘
                      │  charging-events (12 partitions)
                      ▼
          ┌─────────────────────┐
          │        Kafka        │
          │  KRaft, snappy, 24h │
          └──────┬──────────────┘
         ┌───────┘       └────────────────────┐
         ▼                                    ▼
┌─────────────────────┐        ┌──────────────────────────┐
│   Redis Consumer    │        │   ClickHouse Consumer    │
│   Pipeline batching │        │   Batch insert 10k/5s    │
└──────────┬──────────┘        └─────────────┬────────────┘
           ▼                                 ▼
┌─────────────────────┐        ┌──────────────────────────┐
│   Redis 7           │        │   ClickHouse 23.x        │
│   Real-time store   │        │   Analytics store        │
│   Sessions, status  │        │   MergeTree + MVs        │
└─────────────────────┘        └──────────────────────────┘
         └──────────────────┬──────────────────────────────┘
                            ▼
              ┌─────────────────────────┐
              │   Analytics Dashboard   │
              │   Rich terminal UI      │
              └─────────────────────────┘
```

| Component | Technology | Role |
|-----------|-----------|------|
| Simulator | Python + confluent-kafka | Generates 1k–100k events/sec across 1k stations |
| Transport | Apache Kafka (KRaft) | Durable ordered bus, 12 partitions, snappy compression |
| Real-time store | Redis 7 | Active sessions, station status, revenue leaderboards |
| Analytics store | ClickHouse | Historical OLAP, materialized views, sub-second queries |
| Dashboard | Rich (Python) | Terminal UI — live dashboard + one-shot report |

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.10+

### 1 — Install & start infrastructure

```bash
make install     # install Python dependencies
make up          # start Kafka, Redis, ClickHouse, Kafka-UI
```

Services:
- Kafka → `localhost:9094`
- Redis → `localhost:6379`
- ClickHouse → `localhost:9000` / HTTP `localhost:8123`
- Kafka UI → `http://localhost:8080`

### 2 — Start consumers (each in its own terminal)

```bash
make consumer-redis    # real-time Redis writer
make consumer-ch       # ClickHouse analytics writer
```

### 3 — Run the producer

```bash
make producer          # 10,000 eps, 4 workers
make producer-fast     # 50,000 eps, 8 workers
make producer-max      # 100,000 eps, 16 workers

# or custom
python -m simulator.producer --eps 25000 --workers 6 --duration 120
```

### 4 — View analytics

```bash
make dashboard         # live terminal dashboard (refreshes every 30s)
make report            # one-shot analytics report
```

---

## Common Workflows

### Reset everything

```bash
make down && make up
```

Wipes Kafka, Redis, and ClickHouse. Start fresh from here.

### Backfill 24h of historical data

Populates all time buckets (Morning Peak, Evening Peak, Off-Peak) so reports show complete data.

```bash
# Start CH consumer with watermark disabled (backfill uses yesterday's timestamps)
WATERMARK_MAX_LATENESS_S=999999 make consumer-ch

# In another terminal
make consumer-redis
make backfill
```

After backfill finishes, restart `consumer-ch` normally for live events.

### Incremental scale test (1k → 10k → 50k → 100k eps)

```bash
python scripts/scale_test.py             # 30s per tier
python scripts/scale_test.py --duration 60
```

Runs all 4 tiers consecutively and prints a summary table.

### Benchmark (no consumers needed)

```bash
python scripts/benchmark.py             # generator throughput only
python scripts/benchmark.py --kafka     # also measures Kafka delivery
```

---

## Analytics Insights

The pipeline surfaces these dimensions via `make report` and `make dashboard`:

| # | Insight | Source |
|---|---------|--------|
| 1 | Revenue by Network | ClickHouse |
| 2 | Sessions per Hour | ClickHouse |
| 3 | Charger Utilisation | ClickHouse |
| 4 | Top Cities by Energy | ClickHouse |
| 5 | Fault Diagnostics | ClickHouse |
| 6 | Vehicle Type Breakdown | ClickHouse |
| 7 | Session Duration Distribution | ClickHouse |
| 8 | Peak Hours Analysis | ClickHouse |
| 9 | Network Reliability | ClickHouse |
| A1 | Hourly Energy (7 days) | ClickHouse |
| A2 | Station Uptime Ratio | ClickHouse |
| A3 | Tesla vs Other Brands | ClickHouse |
| A4 | Peak vs Off-Peak Revenue | ClickHouse |
| A5 | Fault Geographic Distribution | ClickHouse |
| A6 | Anomaly Detection (Z-score) | ClickHouse |

---

## Data Model

### Event Schema

```json
{
  "event_id":         "uuid4",
  "event_type":       "session_start | meter_update | session_stop | status_change | fault_alert | heartbeat",
  "timestamp":        "2024-06-15T14:30:00.123+00:00",
  "station_id":       "IONITY_S00042",
  "connector_id":     2,
  "session_id":       "uuid4",
  "network_id":       "IONITY",
  "city":             "Berlin",
  "country":          "DE",
  "charger_type":     "DC_FAST",
  "power_kw":         150.0,
  "energy_kwh":       45.2,
  "duration_minutes": 30,
  "revenue_eur":      35.71,
  "vehicle_brand":    "BMW",
  "error_code":       null
}
```

### Event Distribution

| Event Type | Share | Description |
|------------|------:|-------------|
| `meter_update` | 58% | Energy/SoC reading during session |
| `session_stop` | 18% | Session ended — full revenue & energy |
| `session_start` | 12% | EV connected |
| `status_change` | 9% | Connector availability |
| `fault_alert` | 3% | OCPP-style fault |

### Networks & Coverage

10 European CPOs: IONITY, TESLA_SC, ChargePoint, EVBox, Allego, Fastned, Recharge, Shell Recharge, bp pulse, Virta.

20 cities across DE, NL, FR, GB, NO, SE, DK, BE, AT, CH, ES.

---

## Redis Key Patterns

| Key | Type | Content |
|-----|------|---------|
| `session:{id}` | Hash | Live session state |
| `station:{id}:status` | Hash | Connector status |
| `global:active_sessions` | String | Platform-wide counter |
| `global:events:total` | String | Total events processed |
| `leaderboard:station_revenue:{date}` | Sorted Set | Daily revenue top-N |
| `global:energy_kwh:{date}` | String | Daily energy delivered |

---

## ClickHouse Schema

**Main table:** `charging_events`
- Engine: `ReplacingMergeTree`, partitioned by month
- Sort key: `(timestamp, network_id, station_id)`
- TTL: 90 days

**Materialized views:**

| View | Aggregation |
|------|-------------|
| `sessions_hourly_mv` | Per-hour, per-network, per-city |
| `station_daily_mv` | Per-day, per-station |
| `errors_daily_mv` | Per-day, per-error-code |
| `network_revenue_daily_mv` | Per-day, per-network |

---

## Configuration

Key env-var overrides (all in `config/settings.py`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka broker |
| `REDIS_HOST` | `localhost` | Redis host |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `TARGET_EPS` | `10000` | Simulator target eps |
| `NUM_PRODUCERS` | `4` | Producer worker processes |
| `CH_BATCH_SIZE` | `10000` | ClickHouse insert batch (rows) |
| `WATERMARK_MAX_LATENESS_S` | `300` | Late event tolerance (seconds) |

---

## Tests

```bash
make test
```

Unit tests cover event generation (shapes, distributions, serialisation) and ClickHouse row conversion. No external services needed.

---

## Directory Structure

```
chargesquare/
├── docker-compose.yml
├── Makefile
├── config/settings.py
├── simulator/
│   ├── models.py               ChargingEvent dataclass
│   ├── generators.py           EventGenerator + StationRegistry
│   ├── session_manager.py      Lifecycle: START → METER_UPDATEs → STOP
│   └── producer.py             Multi-process Kafka producer
├── consumers/
│   ├── redis_consumer.py       Kafka → Redis real-time state
│   └── clickhouse_consumer.py  Kafka → ClickHouse analytics
├── analytics/
│   ├── queries.py              15 analytical query methods
│   └── dashboard.py            Rich terminal UI
├── scripts/
│   ├── init_clickhouse.sql     Schema DDL + materialized views
│   ├── backfill.py             Inject 24h of historical data
│   ├── benchmark.py            Throughput benchmarking
│   └── scale_test.py           Incremental scale test (1k→100k eps)
└── tests/
    ├── test_generators.py
    └── test_clickhouse_consumer.py
```