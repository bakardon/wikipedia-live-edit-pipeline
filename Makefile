.PHONY: help up down ps logs stream batch dbt dbt-test dashboard psql kafka-topics download-clickstream test lint clean nuke

SHELL := /bin/bash
COMPOSE := docker compose

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

up: ## Start kafka, postgres, producer, streaming, dashboard
	$(COMPOSE) up -d kafka postgres producer streaming dashboard

down: ## Stop all services (keep volumes)
	$(COMPOSE) down

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

stream: ## Force-recreate the producer + streaming pair
	$(COMPOSE) up -d --force-recreate producer streaming

batch: ## Run the Clickstream batch job once
	$(COMPOSE) --profile batch run --rm batch

download-clickstream: ## Download the latest English Clickstream dump (~1.5 GB)
	@mkdir -p data/clickstream
	@bash batch/download_clickstream.sh

dbt: ## Run dbt models against postgres
	cd dbt && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd dbt && dbt test --profiles-dir .

dashboard: ## Print Streamlit URL (service runs in compose)
	@echo "Streamlit: http://localhost:8501"

psql: ## Open a psql shell inside the postgres container
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-wiki} -d $${POSTGRES_DB:-wiki}

kafka-topics: ## List Kafka topics
	$(COMPOSE) exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list

test: ## Run pytest
	pytest -q

lint: ## Run ruff
	ruff check .

clean: ## Stop services (data volumes preserved)
	$(COMPOSE) down

nuke: ## DANGEROUS: drop all volumes including postgres data
	@read -p "This deletes all postgres + kafka data. Continue? [y/N] " ans && [ "$$ans" = "y" ]
	$(COMPOSE) down -v
