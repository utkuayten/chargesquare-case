.PHONY: help install up down logs status producer producer-fast producer-max \
        consumer-redis consumer-ch pipeline dashboard report benchmark test

# ─────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  ChargeSquare — EV Charging Data Pipeline"
	@echo "  ──────────────────────────────────────────"
	@echo ""
	@echo "  Infrastructure"
	@echo "    make up              Start Kafka, Redis, ClickHouse, Kafka-UI"
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
	@echo "    make backfill        Inject 24 h of historical data (all time periods)"
	@echo ""
	@echo "  Dev"
	@echo "    make install         Install Python dependencies"
	@echo "    make test            Run unit tests"
	@echo ""

# ─────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt

# ─────────────────────────────────────────────────────────
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
	python -m simulator.producer --eps 10000 --workers 4

producer-fast:
	python -m simulator.producer --eps 50000 --workers 8

producer-max:
	python -m simulator.producer --eps 100000 --workers 16

# ─────────────────────────────────────────────────────────
consumer-redis:
	python -m consumers.redis_consumer

consumer-ch:
	python -m consumers.clickhouse_consumer

pipeline:
	@echo "Starting consumers in background..."
	nohup python -m consumers.redis_consumer > /tmp/redis_consumer.log 2>&1 &
	nohup python -m consumers.clickhouse_consumer > /tmp/ch_consumer.log 2>&1 &
	@echo "Consumers started (logs: /tmp/redis_consumer.log, /tmp/ch_consumer.log)"

# ─────────────────────────────────────────────────────────
dashboard:
	python -m analytics.dashboard --mode dashboard

report:
	python -m analytics.dashboard --mode report

benchmark:
	python scripts/benchmark.py

backfill:
	python scripts/backfill.py

backfill-direct:
	python scripts/backfill_direct.py

# ─────────────────────────────────────────────────────────
test:
	python -m pytest tests/ -v --tb=short
