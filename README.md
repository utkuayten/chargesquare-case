# ChargeSquare — EV Charging Data Pipeline

A real-time event streaming and analytics pipeline simulating 1,000 EV charging stations across Europe. Events flow at up to 100,000/sec through Kafka into Redis (real-time state) and ClickHouse (analytics). Includes realistic traffic shaping — morning, lunch, and evening peaks — a live terminal dashboard, Grafana UI, and a full analytics report. Supports both live simulation and historical backfill for any number of days.

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
              ┌──────────────────────────────┐
              │  Analytics Dashboard         │
              │  Terminal UI  +  Grafana     │
              └──────────────────────────────┘
```

| Component | Technology | Role |
|-----------|-----------|------|
| Simulator | Python + confluent-kafka | Generates 1k–100k events/sec across 1,000 stations |
| Transport | Apache Kafka (KRaft) | Durable ordered bus, 12 partitions, snappy compression |
| Real-time store | Redis 7 | Active sessions, station status, revenue leaderboards |
| Analytics store | ClickHouse | Historical OLAP, materialized views, sub-second queries |
| Dashboard | Rich (Python) | Terminal UI — live dashboard + one-shot report |
| Grafana | Grafana 10 | Web dashboard auto-provisioned from ClickHouse |

---

## Getting Started

**Prerequisites:** Docker & Docker Compose (that's it — everything runs in containers).

```bash
make build   # build the app Docker image (run once, or after code changes)
make up      # start Kafka, Redis, ClickHouse, Grafana, Kafka UI
```

Web UIs:
- **Grafana** → http://localhost:3000 &nbsp;`admin / chargesquare`
- **Kafka UI** → http://localhost:8080

> Consumers automatically wait for the Kafka topic to be ready — you can start them immediately after `make up`.

---

## Mode 1 — Live Simulation

Generates events in real time and streams them through the full pipeline.

**Terminal 1 — Redis consumer** (real-time state)
```bash
make consumer-redis
```

**Terminal 2 — ClickHouse consumer** (analytics writer)
```bash
make consumer-ch
```

**Terminal 3 — Producer** (start after both consumers show `Assigned 12 partitions`)
```bash
make producer          # 10,000 eps, 4 workers
make producer-fast     # 50,000 eps, 8 workers
make producer-max      # 100,000 eps, 16 workers
```

**View results:**
```bash
make report       # one-shot analytics report in the terminal
make dashboard    # live dashboard, auto-refreshes every 30s
```
Or open Grafana at http://localhost:3000 — set the time picker to **Last 1 hour** or **Last 24 hours**.

---

## Mode 2 — Backfill (Historical Data)

Generates historical events with correct timestamps so all time buckets (Morning Peak, Lunch Peak, Evening Peak, Off-Peak) appear in reports and Grafana. The ClickHouse consumer runs with the watermark disabled to accept old timestamps.

**Terminal 1 — Redis consumer**
```bash
make consumer-redis
```

**Terminal 2 — ClickHouse consumer with watermark disabled**
```bash
make consumer-ch-backfill
```

**Terminal 3 — Backfill** (start after both consumers show `Assigned 12 partitions`)
```bash
make backfill        # yesterday only (1 day)
make backfill-7d     # last 7 days
make backfill-30d    # last 30 days
```

You can also run it directly with any duration or volume:
```bash
docker-compose run --rm app python scripts/backfill.py --days 14 --events-per-hour 200000
```

Want to tweak the simulation? Override any parameter via environment variables:
```bash
docker-compose run --rm \
  -e SIM_FAULT_RATE_PCT=2.0 \
  -e SIM_PEAK_MULT=4.0 \
  app python scripts/backfill.py --days 7
```

**View results** (once consumers finish processing):
```bash
make report
```
In Grafana, set the time picker to **Yesterday**, **Last 7 days**, etc. — backfill data uses real historical timestamps.

---

## All Commands

### Infrastructure

| Command | Description |
|---------|-------------|
| `make build` | Build the app Docker image |
| `make up` | Start all services (Kafka, Redis, ClickHouse, Grafana, Kafka UI) |
| `make down` | Stop everything and wipe all data volumes |
| `make status` | Show container health |
| `make logs` | Stream all container logs |

### Consumers

| Command | Description |
|---------|-------------|
| `make consumer-redis` | Start Redis consumer — writes real-time state (sessions, status, counters) |
| `make consumer-ch` | Start ClickHouse consumer — writes analytics data (live events only) |
| `make consumer-ch-backfill` | Start ClickHouse consumer with watermark disabled — use this for backfill |
| `make pipeline` | Start both consumers in the background (for live mode) |

### Producer / Data Ingestion

| Command | Description |
|---------|-------------|
| `make producer` | Run simulator at 10,000 events/sec (4 workers) |
| `make producer-fast` | Run simulator at 50,000 events/sec (8 workers) |
| `make producer-max` | Run simulator at 100,000 events/sec (16 workers) |
| `make backfill` | Inject 1 day of historical data — use with `consumer-ch-backfill` |
| `make backfill-7d` | Inject last 7 days of historical data |
| `make backfill-30d` | Inject last 30 days of historical data |

### Analytics

| Command | Description |
|---------|-------------|
| `make report` | Print a full one-shot analytics report to the terminal |
| `make dashboard` | Live terminal dashboard, refreshes every 30s |

### Export

| Command | Description |
|---------|-------------|
| `make export-parquet` | Export all events → `exports/charging_events.parquet` (Snappy, 200k row batches) |
| `--date YYYY-MM-DD` | Filter to a single day |
| `--out path/to/file` | Custom output path |

### Testing & Benchmarking

| Command | Description |
|---------|-------------|
| `make test` | Run all 25 unit tests inside Docker |
| `make benchmark` | Measure generator + Kafka throughput |
| `python scripts/scale_test.py` | Incremental scale test: 1k → 10k → 50k → 100k eps |

### Reset

```bash
make down && make up         # wipe everything and start fresh
make down && make build && make up   # also rebuild the image (after code changes)
```

---

## Analytics Report

`make report` covers 14 dimensions across the full dataset:

| # | Analysis | What it shows |
|---|---------|---------------|
| 1 | Revenue by Network | CPO league table — sessions, total and avg revenue |
| 2 | Sessions per Hour | Intra-day demand curve with avg power |
| 3 | Charger Utilisation | Yield and efficiency by charger class (AC L1/L2, DC Fast, Ultra-Fast) |
| 4 | Top Cities by Energy | Geographic demand — kWh and revenue per city |
| 5 | Fault Diagnostics | Error code frequency and affected stations |
| 6 | Vehicle Type Breakdown | BEV vs PHEV charging behaviour |
| 7 | Peak Hours Analysis | Hour-of-day traffic heatmap |
| 8 | Network Reliability | Fault rate per CPO |
| 9 | Tariff Performance | Revenue and €/kWh by tariff tier |
| A1 | Hourly Energy (7 days) | kWh trend with PEAK / LOW labels |
| A2 | Station Uptime Ratio | Worst-offender stations by downtime |
| A3 | Tesla vs Other Brands | Duration, energy, revenue delta |
| A4 | Peak vs Off-Peak Revenue | Morning Peak / Evening Peak / Off-Peak split |
| A5 | Fault Geographic Distribution | Fault density per city |
| A6 | Anomaly Detection | Sessions deviating >2σ from mean power (Z-score) |

---

## Data Model

### Event types

| Event | Share | Description |
|-------|------:|-------------|
| `meter_update` | 58% | Energy and SoC reading mid-session |
| `session_stop` | 18% | Session ended — final revenue and energy |
| `session_start` | 12% | EV connected |
| `status_change` | 9% | Connector availability change |
| `fault_alert` | 3% | OCPP-style fault |

### Event schema (sample)

```json
{
  "event_id":         "uuid4",
  "event_type":       "session_stop",
  "timestamp":        "2024-06-15T14:30:00.123+00:00",
  "station_id":       "IONITY_S00042",
  "network_id":       "IONITY",
  "city":             "Berlin",
  "country":          "DE",
  "charger_type":     "DC_FAST",
  "power_kw":         150.0,
  "energy_kwh":       45.2,
  "duration_minutes": 30,
  "revenue_eur":      35.71,
  "vehicle_brand":    "BMW",
  "tariff_id":        "ionity-standard",
  "error_code":       null
}
```

### ClickHouse schema

**Main table:** `charging_events` — `ReplacingMergeTree`, partitioned by month, 90-day TTL.

**Materialized views** (pre-aggregated for sub-second queries):

| View | Aggregation |
|------|-------------|
| `sessions_hourly_mv` | Per-hour, per-network, per-city |
| `station_daily_mv` | Per-day, per-station |
| `errors_daily_mv` | Per-day, per-error-code |
| `network_revenue_daily_mv` | Per-day, per-network |

### Redis key patterns

| Key | Type | Content |
|-----|------|---------|
| `session:{id}` | Hash | Live session state |
| `station:{id}:status` | Hash | Connector status |
| `global:active_sessions` | String | Platform-wide counter |
| `global:events:total` | String | Total events processed |
| `leaderboard:station_revenue:{date}` | Sorted Set | Daily revenue top-N |

---

## Configuration

All settings are in `config/settings.py` and can be overridden with environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka broker address |
| `REDIS_HOST` | `localhost` | Redis host |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `TARGET_EPS` | `10000` | Simulator target events/sec |
| `NUM_PRODUCERS` | `4` | Producer worker processes |
| `CH_BATCH_SIZE` | `10000` | ClickHouse insert batch size |
| `WATERMARK_MAX_LATENESS_S` | `300` | Late event tolerance in seconds (`999999` to disable for backfill) |
| `SIM_FAULT_RATE_PCT` | `0.3` | % of events that are fault alerts |
| `SIM_PEAK_MULT` | `3.0` | Session rate multiplier during peak hours |
| `SIM_SESSION_FRAC` | `0.08` | Fraction of off-peak events that open a new session |
| `SIM_NETWORKS` | `10` | Number of CPO networks |
| `SIM_STATIONS_PER_NET` | `100` | Stations per network |

---

## Tests

```bash
make test
```

25 unit tests covering event generation (shapes, field types, distributions, serialisation) and ClickHouse row conversion. No external services required — runs fully inside Docker.

---

## Project Structure

```
chargesquare/
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── config/settings.py              env-var driven configuration
├── simulator/
│   ├── models.py                   ChargingEvent dataclass
│   ├── generators.py               EventGenerator + StationRegistry
│   ├── session_manager.py          Session lifecycle (START → UPDATEs → STOP)
│   └── producer.py                 Multi-process Kafka producer
├── consumers/
│   ├── redis_consumer.py           Kafka → Redis real-time state
│   ├── clickhouse_consumer.py      Kafka → ClickHouse analytics
│   └── watermark.py                Late event filtering
├── analytics/
│   ├── queries.py                  14 analytical query methods
│   └── dashboard.py                Rich terminal UI (live + report mode)
├── scripts/
│   ├── init_clickhouse.sql         Schema DDL + materialized views
│   ├── backfill.py                 Inject 24h of historical data
│   ├── benchmark.py                Throughput benchmarking
│   └── scale_test.py               Incremental scale test (1k → 100k eps)
├── grafana/
│   ├── provisioning/               Auto-provisioned datasource + dashboard
│   └── dashboards/chargesquare.json
└── tests/
    ├── test_generators.py
    └── test_clickhouse_consumer.py
```
