COMPOSE_FILE := docker-compose.airflow.yml
AIRFLOW_CONTAINER := airflow-worker
AIRFLOW_SCHEDULER := airflow-scheduler

.PHONY: help
help:
	@echo "Makefile targets:"
	@echo "  up           - Start the Docker Compose project"
	@echo "  down         - Stop the Docker Compose project"
	@echo "  bash         - Open bash in the Airflow scheduler container"
	@echo "  dags         - Trigger the Airflow DAGs"
	@echo "  logs         - View logs from the scheduler container"

.PHONY: all up down exec start_dags

up:
	docker-compose -f $(COMPOSE_FILE) up -d
	@echo "Docker Compose is up and running."

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
