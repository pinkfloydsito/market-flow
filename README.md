# Market Flow - Supermarket Price Analysis Pipeline

This repository contains a project designed to analyze supermarket price trends across seasons and assess their relationship with external factors such as weather, currency exchange rates, and socio-economic indicators.

## Project Overview

### Objectives

- **Identify seasonal pricing patterns:** Analyze how product prices vary by season and market.
- **Impact of weather variations:** Measure the influence of seasonal weather changes (temperature, precipitation) on pricing.
- **Currency fluctuations:** Assess the impact of exchange rate changes on commodity prices.
- **Relationship between HDI and price volatility:** Explore how socio-economic factors, such as the Human Development Index (HDI), affect seasonal price changes. [NOT DONE YET]

### Dataset Sources

1. [Global Food Prices Dataset](https://www.kaggle.com/datasets/jboysen/global-food-prices)
2. [Human Development Index Dataset](https://www.kaggle.com/datasets/iamsouravbanerjee/human-development-index-dataset/data)

### APIs used to enrich data
1. [OpenWeather API](https://openweathermap.org/api) - Weather and seasonal data
2. [Currency API](https://currencyapi.com/) - Currency API
3. [Nominatim API](https://nominatim.openstreetmap.org/search) - Nominatim API

---

## Tools

### Database

- **DuckDB:** Enables in-memory data analysis for rapid querying and lightweight aggregation.

### Orchestrator
- Apache Airflow: Manages the ETL pipeline and automates data loading and transformation tasks.

### Visualization - Upstream Output
- Streamlit: Interactive data visualization and exploration.
- Prophet: Time series forecasting tool for analyzing seasonal trends.

### Makefile

The Makefile includes commands for setting up and running the project:
- Start the Docker environment with Airflow and the database.
- Trigger Airflow DAGs directly from the CLI.
- Example commands:
  - `make up`: Start the Docker environment.
  - `make dags-trigger`: Trigger DAGs for schema loading and data insertion.
  - `make down`: Stop and clean up the Docker containers.

### Usage
1. Run the command `cp .env.example .env` to create a new .env file.
The setup is done using docker, then we need to add the directories this way:
   ```bash
    AIRFLOW_CONN_MARKET_FLOW=postgresql://airflow:airflow@postgres:5432/market_flow
    PYTHONPATH=/opt/airflow
    CURRENCY_API_KEY=[currencyapi.com key]
    DUCKDB_FILE=/opt/airflow/db/analytics.duckdb
    ML_DATA_FILE=/opt/airflow/db/ml_data.csv
    RAW_DATA_PATH=/opt/airflow/data/raw
   ```
2. Ensure Docker and Docker Compose are installed on your machine.
3. Run the following commands to launch the docker containers:
   ```bash
   make up
   ```
4. We need to download the data from the [Global Food Prices Dataset](https://www.kaggle.com/datasets/jboysen/global-food-prices) and [Human Development Index Dataset](https://www.kaggle.com/datasets/iamsouravbanerjee/human-development-index-dataset/data)
5. Rename the Human Development Index Dataset to `hdi_(any pattern you prefer).csv` and the Global Food Prices Dataset to `wfp_(any pattern you prefer).csv` in the directory `data/raw`.

6. First we will unpause our DAGs in Airflow
   ```bash
    make unpause-all
   ```

7. Enter in the Airflow UI and create a connection:
Need to create the default fs connection. I used the following configurations:
  ![Connection Image](screenshots/streamlit-1.png)

8. Now we will run the full pipeline. It might take a few minutes
```bash
  make run-full-pipeline
```
9. You can check the results of each task in the Airflow UI. The weather, and currency take a bit in get fully processed however after running the pipeline there should be enough information for the next step

10. Now run `trigger-prophet` generate the csv based on the data we have until now.

11. Open the browser and go to `localhost:8501` to see the Streamlit dashboard. You can train your models and check the actual data and forecast by yourself in any product, country, locality you choose. There are several features there available
