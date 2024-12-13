import pandas as pd
import streamlit as st
from constants import CSV_PATH, CacheTTL


@st.cache_data(ttl=CacheTTL.LONG.value)
def load_data() -> pd.DataFrame:
    df = pd.read_csv(f"{CSV_PATH}")
    df["constructed_date"] = pd.to_datetime(df["constructed_date"])
    return df


def get_filtered_data_by_country_and_product(df, country, product) -> pd.DataFrame:
    country_df = df[df["country"] == country]
    filtered_df = country_df[country_df["product_name"] == product]
    return filtered_df
