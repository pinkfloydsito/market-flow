import streamlit as st
import pandas as pd


st.set_page_config(
    page_title="Market Flow",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://www.github.com/pinkfloydsito",
        "Report a bug": "https://www.github.com/market_flow/issues",
        "About": "# This is an *extremely* great app!",
    },
)

pg = st.navigation(
    [
        st.Page("./pages/dataset_viewer.py", title="Dataset Viewer"),
        st.Page("./pages/best_products.py", title="Best Products Overview"),
        st.Page(
            "./pages/single_product_forecasting.py", title="Forecasting for product"
        ),
        st.Page("./pages/best_markets.py", title="Best Markets Overview"),
    ]
)
pg.run()
