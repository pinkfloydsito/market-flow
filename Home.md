# Product Price Analysis Project Documentation

## Project Overview
This project analyzes product pricing patterns across different markets using Airflow for data processing, Streamlit for visualization and analysis, and Prophet for price forecasting. The analysis focuses on three main aspects: seasonal pricing patterns, weather impacts, and currency fluctuations.

## Technical Stack & Architecture
- **Data Pipeline**: Apache Airflow
  - Handles data ingestion
  - Performs data transformations
  - Outputs processed data for analysis
- **Analysis & Visualization**: Streamlit
  - Interactive dashboard interface
  - Integration with Prophet for forecasting
  - Real-time data analysis
- **Forecasting**: Prophet
  - Called directly from Streamlit
  - Time series forecasting
  - Seasonal pattern detection

## Pipeline Structure

### 1. Airflow DAG
```mermaid
graph LR
    A[Raw Market Data] --> B[Ingestion DAG]
    C[Weather Data] --> B
    D[Currency Data] --> B
    B --> E[Transformation DAG]
    E --> F[Processed Dataset]
```

#### Ingestion Process
- Raw data collection from various sources
- Data validation and cleaning
- Initial formatting and structuring

#### Transformation Process
- Data aggregation
- Feature engineering (standardize units, imputation of data)
- Output preparation for Streamlit

### 2. Streamlit Application

#### Application Structure
```mermaid
graph TD
    A[Processed Data] --> B[Streamlit App]
    B --> C[Data Loading]
    C --> D[Prophet Integration]
    D --> E[Analysis Modules]
    E --> F[Visualization of prices of products between different countries, markets]
```

#### Prophet Integration
- Direct calls to Prophet from Streamlit
- Real-time forecasting based on selected filters (country, locality, product)
- Regressors integration with the temperature and the precipitation fields.

## Research Questions & Implementation

### 1. Seasonal Price Variations by Market

#### Analysis Implementation
- Data loading from Airflow output
- Prophet model configuration in Streamlit
- Seasonal component extraction
- Interactive visualization of patterns

#### Key Visualizations
- Seasonal trend decomposition
<img width="898" alt="image" src="https://github.com/user-attachments/assets/d4ec2122-78bf-449e-b2ac-675a1a0a0070" />

- Actual pricing variability per product
<img width="871" alt="image" src="https://github.com/user-attachments/assets/9c52ca71-0e56-4d07-ab94-4f6c0659f7fa" />

- Market comparison on prices per product (price in USD)
<img width="799" alt="image" src="https://github.com/user-attachments/assets/e4950b28-210e-4fa1-9313-517d8488f5f4" />
<img width="430" alt="image" src="https://github.com/user-attachments/assets/f9e694fa-3537-4694-a73f-9cf1d2a53cca" />


### 3. Currency Fluctuation Impact
The currency was changed for every data point to USD using the historical equivalent, and the prices were adjusted in the staging layer.