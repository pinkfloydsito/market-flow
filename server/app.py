import streamlit as st
import pandas as pd


from constants import CSV_PATH


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
        st.Page("./pages/best_products.py"),
        st.Page("./pages/forecasting.py"),
        st.Page("./pages/markets.py"),
    ]
)
pg.run()


@st.cache_resource
def load_csv(db_path=CSV_PATH):
    return pd.read_csv(db_path)
