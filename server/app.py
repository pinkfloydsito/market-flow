import streamlit as st
import duckdb

DUCKDB_PATH = "/app/db/analytics.duckdb.bak7"


@st.cache_resource
def get_duckdb_connection(db_path=DUCKDB_PATH):
    return duckdb.connect(database=db_path, read_only=True)


# Query the fact_transactions table
def fetch_transactions(con):
    query = "SELECT * FROM public.fact_transaction LIMIT 100"
    return con.execute(query).fetchdf()


def fetch_weather(con):
    query = "SELECT * FROM public.dim_weather LIMIT 100"
    return con.execute(query).fetchdf()


st.title("Fact Transactions Viewer")
st.sidebar.header("Options")
db_path = st.sidebar.text_input("DuckDB Path", DUCKDB_PATH)

if db_path:
    con = get_duckdb_connection(db_path)
    st.write("Connected to:", db_path)
    transactions = fetch_transactions(con)
    st.write(transactions)

    weather = fetch_weather(con)
    st.write(weather)
