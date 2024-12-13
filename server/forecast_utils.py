import os
import datetime
import joblib
import hashlib
from typing import List, Tuple

import concurrent.futures

import pandas as pd
from prophet import Prophet
import streamlit as st


def get_model_path(key):
    model_id = hashlib.md5(f"{key}".encode()).hexdigest()
    models_dir = "cached_models"
    os.makedirs(models_dir, exist_ok=True)
    return os.path.join(models_dir, f"prophet_{model_id}.joblib")


def prepare_prophet_data(df):
    prophet_df = df[["constructed_date", "price_per_kg_usd"]].rename(
        columns={"constructed_date": "ds", "price_per_kg_usd": "y"}
    )
    return prophet_df


def create_prophet_model():
    return Prophet(
        weekly_seasonality=True,  # type: ignore
        yearly_seasonality=True,  # type: ignore
        daily_seasonality=False,  # type: ignore
        # growth="logistic",
    )


@st.cache_data(ttl=60)  # cache for 1 minute
def calculate_forecast_periods(df):
    max_date = df["constructed_date"].max()
    today = datetime.datetime.today()
    periods = (today.date() - max_date.date()).days + 365
    return max(periods, 365)


def process_market(market, filtered_df, country, product, periods):
    temp_df = filtered_df[filtered_df["market"] == market]
    if len(temp_df) == 0:
        return None

    prophet_df = prepare_prophet_data(temp_df)
    prophet_df.to_csv("prophet_df.csv")
    model = load_or_train_model(
        prophet_df, country=country, product=product, market=market
    )

    future = model.make_future_dataframe(periods=periods)

    forecast = model.predict(future)
    avg_forecast_price = forecast["yhat"].mean()

    return (market, avg_forecast_price)


@st.cache_data(ttl=300)  # Cache for 5 minutes (300 seconds)
def forecast_market_prices(filtered_df, periods, country, product):
    all_markets = filtered_df["market"].unique()
    market_prices: List[Tuple[str, float]] = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                process_market, market, filtered_df, country, product, periods
            ): market
            for market in all_markets
        }

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                market_prices.append(result)  # type: ignore

    return market_prices


@st.cache_data(ttl=600)
def forecast_best_products(df, country, locality=None, top_n=10):
    """
    Forecast prices for all products in a given country/locality and return the best ones.
    """
    # Filter by country and locality if provided
    filtered_df = df[df["country"] == country]
    currency_name = filtered_df["currency_name"].iloc[0]

    if locality:
        filtered_df = filtered_df[filtered_df["locality"] == locality]

    if len(filtered_df) == 0:
        return None, None

    all_products = filtered_df["product_name"].unique()
    product_prices: List[Tuple[str, float]] = []

    for product in all_products:
        temp_df = filtered_df[filtered_df["product_name"] == product]

        if len(temp_df) > 0:
            prophet_df = prepare_prophet_data(temp_df)

            # Load or train model
            model = load_or_train_model(
                prophet_df, country=country, product=product, locality=locality or "all"
            )

            # Calculate periods
            periods = calculate_forecast_periods(temp_df)

            future = model.make_future_dataframe(periods=periods)

            forecast = model.predict(future, vectorized=False)
            avg_price = float(forecast["yhat"].mean())
            product_prices.append((product, avg_price))

    product_prices = sorted(product_prices, key=lambda x: x[1])
    best_products_df = pd.DataFrame(
        product_prices,
        columns=[  # type: ignore
            "Product",
            f"Avg_Forecasted_Price({currency_name})",
        ],
    )

    # XXX: Sometimes predictions can go below (0.0),
    # need to check this further,
    # for the moment I filter out those rows
    filtered_df = best_products_df[
        best_products_df[f"Avg_Forecasted_Price({currency_name})"] > 0
    ]

    return filtered_df, filtered_df.head(top_n), currency_name


def load_or_train_model(
    prophet_df, locality=None, country=None, product=None, market=None
):
    model_path = get_model_path(f"{country}_{locality}_{product}_{market}")

    # Check if we have a cached model
    if os.path.exists(model_path):
        try:
            model = joblib.load(model_path)
            # Verify the model is still valid for our data
            if model.history_dates.max() >= prophet_df["ds"].max():
                print("Loaded cached model")
                return model
        except Exception as e:
            print(f"Error loading cached model: {e}")

    # Train a new model
    model = create_prophet_model()
    model.fit(prophet_df)

    # Cache the model
    try:
        joblib.dump(model, model_path)
    except Exception as e:
        print(f"Error saving model to cache: {e}")

    return model
