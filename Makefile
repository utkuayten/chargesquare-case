.PHONY: help build up down logs status producer producer-fast producer-max \
        consumer-redis consumer-ch consumer-ch-backfill pipeline dashboard report benchmark backfill backfill-7d backfill-30d backfill-full export-parquet pdf-report bench-redis test clean

RUN = docker-compose run --rm app

# ─────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  ChargeSquare — EV Charging Data Pipeline"
	@echo "  ──────────────────────────────────────────"
	@echo ""
	@echo "  Infrastructure"
	@echo "    make build           Build the app Docker image"
	@echo "    make up              Start all services (Kafka, Redis, ClickHouse, Grafana)"
	@echo "    make down            Stop and remove all containers + volumes"
	@echo "    make logs            Stream all service logs"
	@echo "    make status          Show container health status"
	@echo ""
	@echo "  Simulator"
	@echo "    make producer        Run simulator at 10,000 events/sec (4 workers)"
	@echo "    make producer-fast   Run simulator at 50,000 events/sec (8 workers)"
	@echo "    make producer-max    Run simulator at 100,000 events/sec (16 workers)"
	@echo ""
	@echo "  Consumers"
	@echo "    make consumer-redis  Start Redis real-time writer"
	@echo "    make consumer-ch     Start ClickHouse analytics writer"
	@echo "    make pipeline        Start both consumers in the background"
	@echo ""
	@echo "  Analytics"
	@echo "    make dashboard       Live terminal dashboard (refreshes every 30s)"
	@echo "    make report          Print a one-shot analytics report"
	@echo "    make benchmark       Run throughput benchmark"
	@echo "    make backfill        Inject yesterday's 24 h of data"
	@echo "    make backfill-7d     Inject last 7 days of data"
	@echo "    make backfill-30d    Inject last 30 days of data"
	@echo "    make export-parquet  Export all events from ClickHouse to Parquet"
	@echo "    make pdf-report      Generate PDF analytics report → exports/chargesquare_report.pdf"
	@echo "    make bench-redis     Benchmark Redis consumer at 1k/10k/100k eps → exports/redis_benchmark.pdf"
	@echo ""
	@echo "  Dev"
	@echo "    make test            Run unit tests"
	@echo ""

# ─────────────────────────────────────────────────────────
build:
	docker-compose build app

up:
	docker-compose up -d
	@echo "Waiting for services to become healthy..."
	@sleep 20
	@docker-compose ps

down:
	docker-compose down -v --remove-orphans

logs:
	docker-compose logs -f --tail=100

status:
	docker-compose ps

# ─────────────────────────────────────────────────────────
producer:
	$(RUN) python -m simulator.producer --eps 10000 --workers 4

producer-fast:
	$(RUN) python -m simulator.producer --eps 50000 --workers 8

producer-max:
	$(RUN) python -m simulator.producer --eps 100000 --workers 16

# ─────────────────────────────────────────────────────────
consumer-redis:
	$(RUN) python -m consumers.redis_consumer

consumer-ch:
	$(RUN) python -m consumers.clickhouse_consumer

pipeline:
	@echo "Starting consumers in background..."
	docker-compose run -d --rm --name cs-redis-consumer app python -m consumers.redis_consumer
	docker-compose run -d --rm --name cs-ch-consumer    app python -m consumers.clickhouse_consumer
	@echo "Consumers started. Logs: docker logs cs-redis-consumer / cs-ch-consumer"

# ─────────────────────────────────────────────────────────
dashboard:
	$(RUN) python -m analytics.dashboard --mode dashboard

report:
	$(RUN) python -m analytics.dashboard --mode report

benchmark:
	$(RUN) python scripts/benchmark.py

backfill:
	$(RUN) python scripts/backfill.py --days 1

backfill-7d:
	$(RUN) python scripts/backfill.py --days 7

backfill-30d:
	$(RUN) python scripts/backfill.py --days 30

backfill-direct:
	$(RUN) python scripts/backfill_direct.py

consumer-ch-backfill:
	docker-compose run --rm -e WATERMARK_MAX_LATENESS_S=999999 app python -m consumers.clickhouse_consumer

backfill-full:
	@echo "Starting CH consumer with watermark disabled..."
	docker-compose run -d --rm --name cs-ch-backfill -e WATERMARK_MAX_LATENESS_S=999999 app python -m consumers.clickhouse_consumer
	@echo "Running backfill..."
	$(RUN) python scripts/backfill.py
	@echo "Done. Run 'make report' when consumer finishes processing."

export-parquet:
	$(RUN) python scripts/export_parquet.py

pdf-report:
	$(RUN) python scripts/generate_report.py

bench-redis:
	$(RUN) python scripts/bench_redis.py

# ─────────────────────────────────────────────────────────
test:
	$(RUN) python -m pytest tests/ -v --tb=short

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; \
	find . -name "*.pyc" -delete 2>/dev/null; \
	rm -f dead_letter.jsonl
	@echo "Cleaned."
