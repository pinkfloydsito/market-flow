COMPOSE_FILE := docker-compose.airflow.yml
AIRFLOW_CONTAINER := airflow-worker
AIRFLOW_SCHEDULER := airflow-scheduler

DBT_CONTAINER := airflow-worker

DBT_PROJECT_DIR := /opt/dbt_market_flow
DBT_PROFILE := dbt_market_flow
DBT_TARGET := dev

.PHONY: help
help:
	@echo "Makefile targets:"
	@echo "  up           - Start the Docker Compose project"
	@echo "  down         - Stop the Docker Compose project"
	@echo "  bash         - Open bash in the Airflow scheduler container"
	@echo "  dags         - Trigger the Airflow DAGs"
	@echo "  logs         - View logs from the scheduler container"
	@echo "  dbt-run              - Run DBT models"
	@echo "  dbt-test             - Run DBT tests"
	@echo "  dbt-clean            - Clean DBT target artifacts"
	@echo "  dbt-docs-generate    - Generate DBT documentation"
	@echo "  dbt-docs-serve       - Serve DBT documentation"

.PHONY: all up down exec start_dags

up:
	docker-compose -f $(COMPOSE_FILE) up -d
	@echo "Docker Compose is up and running."

build-up:
	docker-compose -f $(COMPOSE_FILE) up -d --build
	@echo "Docker Compose is up built, and running."

down:
	docker-compose -f $(COMPOSE_FILE) down
	@echo "Docker Compose stopped and cleaned up."

.PHONY: bash
bash:
	docker-compose -f $(COMPOSE_FILE) exec airflow-scheduler bash

dag-create-db:
	docker-compose -f $(COMPOSE_FILE) exec $(AIRFLOW_CONTAINER) airflow dags trigger load_schema_from_file
	@echo "Airflow DAG triggered: load_schema_from_file."

dag-load-raw-data:
	docker-compose -f $(COMPOSE_FILE) exec $(AIRFLOW_CONTAINER) airflow dags trigger csv_to_multiple_tables
	@echo "Airflow DAG triggered: csv_to_multiple_tables."

start_project: up start_dags
	@echo "Project setup completed and DAGs triggered."

.PHONY: logs
logs:
	docker compose -f $(COMPOSE_FILE) logs $(AIRFLOW_CONTAINER) -f

dbt-run:
	docker-compose -f $(COMPOSE_FILE) exec $(DBT_CONTAINER) dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)/profiles --target $(DBT_TARGET)
	@echo "DBT models executed successfully."

dbt-test:
	docker-compose -f $(COMPOSE_FILE) exec $(DBT_CONTAINER) dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)/profiles --target $(DBT_TARGET)
	@echo "DBT tests executed successfully."

dbt-clean:
	docker-compose -f $(COMPOSE_FILE) exec $(DBT_CONTAINER) dbt clean --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)/profiles
	@echo "DBT target artifacts cleaned."

dbt-docs-generate:
	docker-compose -f $(COMPOSE_FILE) exec $(DBT_CONTAINER) dbt docs generate --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)/profiles --target $(DBT_TARGET)
	@echo "DBT documentation generated."

dbt-docs-serve:
	docker-compose -f $(COMPOSE_FILE) exec $(DBT_CONTAINER) dbt docs serve --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)/profiles --target $(DBT_TARGET)
	@echo "DBT documentation server is running."
