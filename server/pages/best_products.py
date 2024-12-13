import streamlit as st
from data_loader import load_data
from forecast_utils import forecast_best_products

# from visualization import plot_top_products
import seaborn as sns
import matplotlib.pyplot as plt

st.title("Best Products Overview")

df = load_data()

country_list = df["country"].unique().tolist()
selected_country = st.selectbox("Select Country", country_list)

show_locality = st.checkbox("Filter by specific locality")
selected_locality = None

if show_locality:
    locality_list = df[df["country"] == selected_country]["locality"].unique().tolist()
    selected_locality = st.selectbox("Select Locality", locality_list)

if st.button("Show Best Products"):
    with st.spinner("Analyzing products..."):
        best_products_df, top_df, currency_name = forecast_best_products(
            df, selected_country, selected_locality
        )

        if best_products_df is None:
            st.error("No data available for the selected criteria.")
        else:
            # Create visualization
            fig, ax = plt.subplots(figsize=(10, 6))
            sns.barplot(
                data=top_df,  # type: ignore
                x=f"Avg_Forecasted_Price({currency_name})",
                y="Product",
                ax=ax,
            )
            title = f"Top 10 Products with Lowest Forecasted Prices({currency_name}) in {selected_country}"
            if selected_locality:
                title += f" ({selected_locality})"
            ax.set_title(title)
            st.pyplot(fig)

            with st.expander("View All Products"):
                st.write(f"All Products Sorted by Forecasted Price({currency_name}):")
                st.dataframe(best_products_df)

            csv = best_products_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download Full Analysis",
                csv,
                "best_products_analysis.csv",
                "text/csv",
                key="download-csv",
            )
