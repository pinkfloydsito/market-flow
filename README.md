# Market Flow - Supermarket Price Analysis Pipeline

This repository contains a project designed to analyze supermarket price trends across seasons and assess their relationship with external factors such as weather, currency exchange rates, and socio-economic indicators.

## Project Overview

### Objectives

- **Identify seasonal pricing patterns:** Analyze how product prices vary by season and market.
- **Impact of weather variations:** Measure the influence of seasonal weather changes (temperature, precipitation) on pricing.
- **Currency fluctuations:** Assess the impact of exchange rate changes on commodity prices.
- **Relationship between HDI and price volatility:** Explore how socio-economic factors, such as the Human Development Index (HDI), affect seasonal price changes.

### Dataset Sources

1. [Global Food Prices Dataset](https://www.kaggle.com/datasets/jboysen/global-food-prices)
2. [Human Development Index Dataset](https://www.kaggle.com/datasets/iamsouravbanerjee/human-development-index-dataset/data)
3. [OpenWeather API](https://openweathermap.org/api) - Weather and seasonal data
4. [ExchangeRate API](https://www.exchangerate-api.com/) - Currency exchange data

---

## Project Structure

### Database Setup

- **PostgreSQL:** Used as the primary relational database for structured data.
- **DuckDB:** Enables in-memory data analysis for rapid querying and lightweight aggregation.

### ETL Pipeline

The ETL pipeline is managed using **Apache Airflow** with the following key DAGs:
1. **`load_schema_from_file`:** Automates schema creation based on SQL files.
2. **`csv_to_multiple_tables`:** Parses and loads CSV data into normalized tables.

### Makefile

The Makefile includes commands for setting up and running the project:
- Start the Docker environment with Airflow and the database.
- Trigger Airflow DAGs directly from the CLI.
- Example commands:
  - `make up`: Start the Docker environment.
  - `make dags-trigger`: Trigger DAGs for schema loading and data insertion.
  - `make down`: Stop and clean up the Docker containers.

### Usage

1. Ensure Docker and Docker Compose are installed on your machine.
2. Run the following commands to set up the project:
   ```bash
   make up
   make dags-trigger
