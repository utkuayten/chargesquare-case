# ChargeSquare — EV Charging Data Pipeline

A real-time event streaming and analytics pipeline built for a data engineering case study. It simulates a network of 1,000 EV charging stations across Europe, processes up to 100,000 events/sec through Kafka, and stores data in both Redis (real-time state) and ClickHouse (analytics). Comes with a live terminal dashboard, a Grafana UI, and a full analytics report.

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
# 1. Build the app image
make build

# 2. Start all services (Kafka, Redis, ClickHouse, Grafana, Kafka UI)
make up
```

Web UIs once running:
- **Grafana** → http://localhost:3000 &nbsp;`admin / chargesquare`
- **Kafka UI** → http://localhost:8080

---

## Running the Pipeline

Open three terminals:

**Terminal 1 — Redis consumer (real-time state)**
```bash
make consumer-redis
```

**Terminal 2 — ClickHouse consumer (analytics)**
```bash
make consumer-ch
```

**Terminal 3 — Producer** (wait for both consumers to show `Assigned 12 partitions` first)
```bash
make producer          # 10,000 eps, 4 workers
make producer-fast     # 50,000 eps, 8 workers
make producer-max      # 100,000 eps, 16 workers
```

---

## Viewing Analytics

```bash
make report       # one-shot analytics report (printed to terminal)
make dashboard    # live dashboard, refreshes every 30s
```

Or open Grafana at http://localhost:3000 — the dashboard loads automatically.

---

## Common Workflows

### Reset everything
```bash
make down && make up
```
Wipes all data in Kafka, Redis, and ClickHouse and starts clean.

### Populate historical data (backfill)

Backfill injects 24 hours of yesterday's events so all time buckets (Morning Peak, Evening Peak, Off-Peak) show up in reports.

```bash
make backfill-full    # starts CH consumer + runs backfill in one step
make report           # run after consumer finishes
```

Or manually in two terminals:
```bash
make consumer-ch-backfill   # terminal 1 — CH consumer with watermark disabled
make backfill               # terminal 2
```

### Scale test (1k → 10k → 50k → 100k eps)
```bash
python scripts/scale_test.py             # 30s per tier
python scripts/scale_test.py --duration 60
```

Runs all 4 tiers back-to-back and prints a summary table with actual throughput vs target.

### Benchmark (no consumers needed)
```bash
make benchmark                        # generator throughput only
make benchmark -- --kafka             # also measures Kafka delivery
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
| `WATERMARK_MAX_LATENESS_S` | `300` | Late event tolerance in seconds |

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
