COMPOSE_FILE := docker-compose.airflow.yml
AIRFLOW_CONTAINER := airflow-worker
AIRFLOW_SCHEDULER := airflow-scheduler

AIRFLOW_HOST ?= localhost:8080

DBT_CONTAINER := airflow-worker

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

DOCKER_AIRFLOW_CMD = docker compose -f $(COMPOSE_FILE) exec airflow-webserver airflow

DOCKER_DBT_CMD = docker compose -f $(COMPOSE_FILE) exec airflow-worker dbt 

list-dags:
	$(DOCKER_AIRFLOW_CMD) dags list

trigger-dag:
	@if [ -z "$(DAG_ID)" ]; then \
		echo "Error: DAG_ID is required. Usage: make trigger-dag DAG_ID=your_dag_id"; \
		exit 1; \
	fi
	$(DOCKER_AIRFLOW_CMD) dags trigger $(DAG_ID)

# Unpause all project DAGs
unpause-all:
	$(DOCKER_AIRFLOW_CMD) dags unpause coordinates_imputation
	$(DOCKER_AIRFLOW_CMD) dags unpause create_csv_for_prophet_dag
	$(DOCKER_AIRFLOW_CMD) dags unpause currency_imputation
	$(DOCKER_AIRFLOW_CMD) dags unpause raw_additional_tables
	$(DOCKER_AIRFLOW_CMD) dags unpause raw_data_ingestion
	$(DOCKER_AIRFLOW_CMD) dags unpause raw_fetch_currencies

pause-all:
	$(DOCKER_AIRFLOW_CMD) dags pause coordinates_imputation
	$(DOCKER_AIRFLOW_CMD) dags pause create_csv_for_prophet_dag
	$(DOCKER_AIRFLOW_CMD) dags pause currency_imputation
	$(DOCKER_AIRFLOW_CMD) dags pause raw_additional_tables
	$(DOCKER_AIRFLOW_CMD) dags pause raw_data_ingestion
	$(DOCKER_AIRFLOW_CMD) dags pause raw_fetch_currencies

trigger-coordinates:
	$(DOCKER_AIRFLOW_CMD) dags trigger --conf '{"execute_now": true}' coordinates_imputation

trigger-prophet:
	$(DOCKER_AIRFLOW_CMD) dags trigger --conf '{"execute_now": true}' create_csv_for_prophet_dag

trigger-currency:
	$(DOCKER_AIRFLOW_CMD) dags trigger --conf '{"execute_now": true}' currency_imputation

trigger-raw-tables:
	$(DOCKER_AIRFLOW_CMD) dags trigger --conf '{"execute_now": true}' raw_additional_tables

make unpause-ingestion:
	$(DOCKER_AIRFLOW_CMD) dags unpause raw_data_ingestion

trigger-ingestion:
	$(DOCKER_AIRFLOW_CMD) dags trigger --conf '{"execute_now": true}' raw_data_ingestion

trigger-fetch-currencies:
	$(DOCKER_AIRFLOW_CMD) dags trigger --conf '{"execute_now": true}' raw_fetch_currencies

run-full-pipeline:
	@echo "Unpausing all DAGs..."
	make unpause-all
	@echo "Starting full pipeline execution..."
	make trigger-ingestion
	@echo "Waiting for ingestion to complete (2 minutes)..."
	sleep 120
	make trigger-fetch-currencies
	@echo "Waiting for currency fetch to start (30s)..."
	sleep 30
	make trigger-raw-tables
	@echo "Waiting for raw tables to start (30s)..."
	sleep 30
	make dbt-run
	@echo "Waiting for dbt to complete (2 minutes)... (If the lock is not released, please run again this command (make dbt-run), maybe stop the DAGs in the UI and run again, you can re-run the whole stuff later after dbt has been completed)"
	sleep 120
	make trigger-coordinates
	make trigger-currency
	@echo "Waiting for imputations to complete (30s)..."
	sleep 30
	make trigger-prophet
	@echo "Pipeline triggers completed"

dbt-run:
	$(DOCKER_DBT_CMD) run

dbt-test:
	$(DOCKER_DBT_CMD) test

dbt-docs-generate:
	$(DOCKER_DBT_CMD) docs generate

dbt-docs-serve:
	$(DOCKER_DBT_CMD) docs serve

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

.PHONY: logs
logs:
	docker compose -f $(COMPOSE_FILE) logs $(AIRFLOW_CONTAINER) -f
