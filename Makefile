.PHONY: help up down clean pull-models reset dev-reset logs

COMPOSE := docker compose

-include .env
export


OLLAMA_MODEL     ?= qwen2.5:3b
POSTGRES_USER    ?= rico
POSTGRES_DB      ?= rico
MINIO_ACCESS_KEY ?= minioadmin
MINIO_SECRET_KEY ?= minioadmin
MINIO_BUCKET     ?= rico-raw

help:
	@echo "Targets:"
	@echo "  up           start all services (Postgres, MinIO, Ollama, Airflow)"
	@echo "  pull-models  pull qwen2.5:3b into the Ollama container (run once)"
	@echo "  down         stop services (volumes preserved)"
	@echo "  clean        stop services and wipe volumes (full reset)"
	@echo "  reset        truncate destination tables + clear MinIO bucket"
	@echo "  dev-reset    reset + truncate tracking tables (pipeline_runs, audit_results, pipeline_metrics)"
	@echo "  logs         tail compose logs"

up:
	$(COMPOSE) up -d --wait postgres minio ollama
	$(COMPOSE) up -d minio-init ollama-init
	$(COMPOSE) up -d airflow-init
	$(COMPOSE) up -d --wait airflow-webserver airflow-scheduler

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

pull-models:
	$(COMPOSE) exec ollama ollama pull $(OLLAMA_MODEL)

reset:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	  "TRUNCATE TABLE screens_metadata, screens_embeddings, screens_review_queue RESTART IDENTITY CASCADE;"
	$(COMPOSE) exec minio mc alias set local http://minio:9000 $(MINIO_ACCESS_KEY) $(MINIO_SECRET_KEY) >/dev/null
	$(COMPOSE) exec minio mc rm --recursive --force local/$(MINIO_BUCKET)/ >/dev/null 2>&1 || true
	@echo "destination tables truncated, MinIO bucket cleared"

dev-reset: reset
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	  "TRUNCATE TABLE pipeline_runs RESTART IDENTITY CASCADE;"
	@echo "tracking tables truncated (pipeline_runs, audit_results, pipeline_metrics via cascade)"

logs:
	$(COMPOSE) logs -f --tail=100
