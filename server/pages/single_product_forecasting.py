from typing import Optional
import streamlit as st
import plotly.express as px
import pandas as pd
from forecast_utils import calculate_forecast_periods, load_or_train_model
from data_loader import load_data
from prophet import Prophet
from prophet.plot import plot_components_plotly


def create_forecast_view(
    df, product_name: str, country: Optional[str] = None, locality: Optional[str] = None
):
    """
    Create forecast for a specific product and location
    """

    location_name = country if locality is None else locality
    with st.spinner(f"Generating forecast for {product_name} in {location_name}..."):
        prophet_df = df[["constructed_date", "price_per_kg_usd"]].rename(
            columns={"constructed_date": "ds", "price_per_kg_usd": "y"}
        )

        model = load_or_train_model(
            prophet_df, product=product_name, country=country, locality=locality
        )

        periods = calculate_forecast_periods(df)
        future = model.make_future_dataframe(periods=periods)
        forecast = model.predict(future)

        return model, forecast


def plot_price_comparison(df, product_name, locations, location_type="country"):
    """
    Plot historical price comparison across locations with improved visualization
    """
    # Calculate moving averages to smooth the lines
    df = df.copy()
    df["MA30"] = df.groupby(location_type)["price_per_kg_usd"].transform(
        lambda x: x.rolling(window=30, min_periods=1).mean()
    )

    # Create base figure
    fig = px.line(
        df,
        x="constructed_date",
        y="MA30",
        color=location_type,
        title=f"{product_name} Price Comparison by {location_type.title()} (30-day Moving Average)",
    )

    # Improve layout
    fig.update_layout(
        height=600,
        xaxis_title="Date",
        yaxis_title="Price per kg (USD)",
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=1.01,
            bgcolor="rgba(255, 255, 255, 0.8)",
        ),
        template="plotly_white",
    )

    # Update traces
    fig.update_traces(line=dict(width=2), mode="lines")

    return fig


def main():
    st.title("Product Price Forecast Analysis")

    df = load_data()

    with st.sidebar:
        st.header("Analysis Controls")

        product_list = sorted(df["product_name"].unique().tolist())
        selected_product = st.selectbox("Select Product", product_list)

        analysis_type = st.radio(
            "Select Analysis Type",
            ["Single Location", "Multi-Location Comparison", "All Countries Overview"],
            index=2,
        )

        # Set location type and get selected locations
        if analysis_type != "All Countries Overview":
            location_type = st.radio(
                "Select Location Level", ["Country", "Market"]
            ).lower()
        else:
            location_type = "country"

        # Handle location selection based on analysis type
        if analysis_type == "Single Location":
            selected_locations = None
            if location_type == "country":
                locations = sorted(df["country"].unique().tolist())
                selected_location = st.selectbox("Select Country", locations)
                filtered_df = df[
                    (df["country"] == selected_location)
                    & (df["product_name"] == selected_product)
                ]
            else:
                country = st.selectbox("Select Country", sorted(df["country"].unique()))
                markets = sorted(df[df["country"] == country]["market"].unique())
                selected_location = st.selectbox("Select Market", markets)
                filtered_df = df[
                    (df["market"] == selected_location)
                    & (df["product_name"] == selected_product)
                ]
        elif analysis_type == "Multi-Location Comparison":
            selected_location = None
            if location_type == "country":
                available_locations = sorted(df["country"].unique().tolist())
            else:
                country = st.selectbox("Select Country", sorted(df["country"].unique()))
                available_locations = sorted(
                    df[df["country"] == country]["market"].unique()
                )

            max_locations = st.slider(
                "Maximum number of locations to compare", 2, 10, 5
            )
            selected_locations = st.multiselect(
                f"Select {location_type.title()}s to Compare (max {max_locations})",
                available_locations,
                default=[available_locations[0]] if available_locations else None,
            )

            if len(selected_locations) > max_locations:
                selected_locations = selected_locations[:max_locations]
                st.warning(
                    f"Limiting selection to {max_locations} locations for better visualization"
                )

            filtered_df = df[
                (df["product_name"] == selected_product)
                & (df[location_type].isin(selected_locations))
            ]
        else:  # All Countries Overview
            selected_location = None
            selected_locations = None
            filtered_df = df[df["product_name"] == selected_product]

    # Main content area - display based on analysis type
    if analysis_type == "Single Location":
        if len(filtered_df) == 0:
            st.error("No data available for the selected criteria.")
            return

        # Historical data visualization
        st.subheader("Historical Price Data")
        hist_fig = px.line(
            filtered_df,
            x="constructed_date",
            y="price_per_kg_usd",
            title=f"{selected_product} Historical Prices in {selected_location}",
        )
        st.plotly_chart(hist_fig)

        # Generate forecast
        if st.button("Generate Forecast"):
            model, forecast = create_forecast_view(
                filtered_df, selected_product, selected_location, location_type
            )

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Forecast Summary")
                summary_df = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(
                    400
                )
                summary_df.columns = ["Date", "Predicted", "Lower Bound", "Upper Bound"]
                st.dataframe(summary_df)

            with col2:
                st.subheader("Download Forecast")
                csv = summary_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download Forecast Data",
                    csv,
                    f"forecast_{selected_product}_{selected_location}.csv",
                    "text/csv",
                )

            st.subheader("Forecast Components")
            fig_components = plot_components_plotly(model, forecast)
            st.plotly_chart(fig_components)

    elif analysis_type == "Multi-Location Comparison":
        if not selected_locations:
            st.warning("Please select at least one location to analyze.")
            return

        if len(filtered_df) == 0:
            st.error("No data available for the selected criteria.")
            return

        st.subheader("Historical Price Comparison")
        comparison_fig = plot_price_comparison(
            filtered_df, selected_product, selected_locations, location_type
        )
        st.plotly_chart(comparison_fig)

        if st.button("Generate Forecasts"):
            st.subheader("Individual Location Forecasts")

            for location in selected_locations:
                location_df = filtered_df[filtered_df[location_type] == location]
                if len(location_df) > 0:
                    with st.expander(f"Forecast for {location}"):
                        model, forecast = create_forecast_view(
                            location_df, selected_product, location, location_type
                        )

                        summary_df = forecast[
                            ["ds", "yhat", "yhat_lower", "yhat_upper"]
                        ].tail(400)
                        summary_df.columns = [
                            "Date",
                            "Predicted",
                            "Lower Bound",
                            "Upper Bound",
                        ]
                        st.dataframe(summary_df)

                        fig_components = plot_components_plotly(model, forecast)
                        st.plotly_chart(fig_components)

    else:  # All Countries Overview
        st.subheader(f"Price Overview - {selected_product}")

        # Calculate average prices per country
        country_prices = (
            filtered_df.groupby(["country", "constructed_date"])["price_per_kg_usd"]
            .mean()
            .reset_index()
        )

        tab_price_trends, tab_summary, tab_forecasting = st.tabs(
            ["Price Trends", "Summary Statistics", "Forecasting"]
        )

        with tab_price_trends:
            st.plotly_chart(
                plot_price_comparison(
                    country_prices,
                    selected_product,
                    country_prices["country"].unique(),
                    "country",
                ),
                use_container_width=True,
            )

        with tab_summary:
            summary_stats = (
                filtered_df.groupby("country")
                .agg({"price_per_kg_usd": ["mean", "std", "min", "max"]})
                .round(2)
            )
            summary_stats.columns = [
                "Average Price",
                "Std Dev",
                "Min Price",
                "Max Price",
            ]
            summary_stats = summary_stats.sort_values("Average Price")  # type: ignore

            st.dataframe(summary_stats, use_container_width=True)

            csv = summary_stats.to_csv().encode("utf-8")
            st.download_button(
                "Download Summary Statistics",
                csv,
                f"summary_stats_{selected_product}.csv",
                "text/csv",
            )

        with tab_forecasting:
            if st.button("Generate Forecast"):
                print(filtered_df.head())
                model, forecast = create_forecast_view(
                    filtered_df, product_name=selected_product
                )
                st.subheader("Forecast Summary")
                summary_df = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(
                    400
                )
                summary_df.columns = ["Date", "Predicted", "Lower Bound", "Upper Bound"]
                st.dataframe(summary_df)
                st.subheader("Forecast Components")
                fig_components = plot_components_plotly(model, forecast)
                st.plotly_chart(fig_components)


main()
