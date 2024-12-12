import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd


def plot_top_markets(market_prices, selected_product, selected_country):
    best_markets_df = pd.DataFrame(
        market_prices, columns=["Market", "Avg_Forecasted_Price"]
    )

    # XXX: Sometimes predictions can go below (0.0),
    # need to check this further,
    # for the moment I filter out those rows
    filtered_df = best_markets_df[best_markets_df["Avg_Forecasted_Price"] > 0]
    top_df = filtered_df.head(10)

    fig, ax = plt.subplots(figsize=(8, 6))
    sns.barplot(data=top_df, x="Avg_Forecasted_Price", y="Market", ax=ax)
    ax.set_title(
        f"Top 10 Markets with Lowest Forecasted {selected_product} Prices in {selected_country}"
    )

    st.pyplot(fig)
    st.write("All Markets Sorted by Forecasted Price:")
    st.write(best_markets_df)
