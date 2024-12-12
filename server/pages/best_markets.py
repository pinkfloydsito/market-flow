import streamlit as st
from data_loader import get_filtered_data_by_country_and_product, load_data
from forecast_utils import calculate_forecast_periods, forecast_market_prices
from visualization import plot_top_markets

st.title("Markets Overview")

# Load data
df = load_data()

# Select Country
country_list = df["country"].unique().tolist()
selected_country = st.selectbox("Select Country", country_list)

# Filter and get product list
country_df = df[df["country"] == selected_country]
product_list = country_df["product_name"].unique().tolist()
selected_product = st.selectbox("Select Product", product_list)

# Get filtered data
filtered_df = get_filtered_data_by_country_and_product(
    df, selected_country, selected_product
)

if st.button("Show Best Markets"):
    periods = calculate_forecast_periods(filtered_df)
    market_prices = forecast_market_prices(
        filtered_df, periods, selected_country, selected_product
    )
    plot_top_markets(market_prices, selected_product, selected_country)
