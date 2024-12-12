import streamlit as st
import pandas as pd
from prophet import Prophet
from prophet.plot import plot_components_plotly
import seaborn as sns
from datetime import datetime

import matplotlib.pyplot as plt

from constants import CSV_PATH


@st.cache_data
def load_data():
    df = pd.read_csv(f"../{CSV_PATH}")
    df["constructed_date"] = pd.to_datetime(df["constructed_date"])
    return df


df = load_data()
df["cap"] = 100
df["floor"] = 0

# Create country selector
country_list = df["country"].unique().tolist()
selected_country = st.selectbox("Select Country", country_list)

# Create product selector (Assuming 'price' or 'price_per_kg' represents a product measure)
# You might have a product column; if not, consider 'price' or 'price_per_kg' as the target

# Update df based on selected country
filtered_df = df[df["country"] == selected_country]

product_list = filtered_df["product_name"].unique().tolist()
selected_product = st.selectbox("Select Product", product_list)

filtered_df = df[
    (df["country"] == selected_country) & (df["product_name"] == selected_product)
]

max_date = filtered_df["constructed_date"].max()
max_year = max_date.year
st.write(f"Max year in data: {max_year}")

today = datetime.today()

periods = (today.date() - max_date.date()).days + 365
if periods < 0:
    periods = 365

# Store selection in session state if needed
st.session_state["country"] = selected_country
st.session_state["product"] = selected_product

# Button to run Prophet
if st.button("Run Forecast"):
    # Prophet expects columns: ds (datetime) and v (target)
    prophet_df = filtered_df[["constructed_date", "price_per_kg"]].rename(
        columns={"constructed_date": "ds", "price_per_kg": "y"}
    )

    # Initialize and fit Prophet model
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        growth="logistic",
    )
    model.fit(prophet_df)

    # Create future dataframe and forecast
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    # Display forecast and seasonality
    st.write("Forecast Data")
    st.write(forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(200))

    # Display forecast components (seasonality)
    fig_components = plot_components_plotly(model, forecast)
    st.plotly_chart(fig_components)

if st.button("Run Multivariate Forecast"):
    prophet_df = filtered_df[
        ["constructed_date", "price_per_kg", "temperature", "precipitation"]
    ].rename(columns={"constructed_date": "ds", "price_per_kg": "y"})

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        growth="logistic",
    )

    model.add_regressor("temperature")
    model.add_regressor("precipitation")
    model.fit(prophet_df)

    last_temp = filtered_df["temperature"].iloc[-1]
    last_prec = filtered_df["precipitation"].iloc[-1]

    future = model.make_future_dataframe(periods=periods)
    future["temperature"] = last_temp
    future["precipitation"] = last_prec

    forecast = model.predict(future)
    st.write("Forecast with Weather Regressors")
    st.write(forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(30))

    fig_components = plot_components_plotly(model, forecast)
    st.plotly_chart(fig_components)

# We will consider "best" as lowest forecasted price.
# We will run a forecast for each product in the selected country and compare average predicted prices.
if st.button("Show Best Products"):
    # Get all products in this country
    all_products = df[df["country"] == selected_country]["product_name"].unique()

    product_prices = []

    for prod in all_products:
        # Filter data for each product
        temp_df = df[(df["country"] == selected_country) & (df["product_name"] == prod)]

        if len(temp_df) > 0:
            # Prepare data
            temp_df = temp_df[["constructed_date", "price_per_kg"]].rename(
                columns={"constructed_date": "ds", "price_per_kg": "y"}
            )
            temp_model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                growth="logistic",
            )
            temp_model.fit(temp_df)

            # Calculate periods again for this product
            temp_max_date = temp_df["ds"].max()
            temp_periods = (today.date() - temp_max_date.date()).days + 365
            if temp_periods < 0:
                temp_periods = 365

            temp_future = temp_model.make_future_dataframe(periods=temp_periods)
            temp_forecast = temp_model.predict(temp_future)

            # Use mean predicted price as a metric
            avg_price = temp_forecast["yhat"].mean()
            product_prices.append((prod, avg_price))

    # Sort products by avg forecasted price (ascending)
    product_prices = sorted(product_prices, key=lambda x: x[1])
    best_products_df = pd.DataFrame(
        product_prices, columns=["Product", "Avg_Forecasted_Price"]
    )

    # Plot the top 10 (or all if less than 10)
    top_df = best_products_df.head(10)
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.barplot(data=top_df, x="Avg_Forecasted_Price", y="Product", ax=ax)
    ax.set_title("Top 10 Products with Lowest Forecasted Prices")
    st.pyplot(fig)
