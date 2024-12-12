import streamlit as st
import pandas as pd
import plotly.express as px
from data_loader import load_data


def initialize_session_state():
    if "page_number" not in st.session_state:
        st.session_state.page_number = 0
    if "rows_per_page" not in st.session_state:
        st.session_state.rows_per_page = 100
    if "filter_column" not in st.session_state:
        st.session_state.filter_column = None
    if "filter_value" not in st.session_state:
        st.session_state.filter_value = None


def apply_filters(df, column, value):
    if column and value:
        if pd.api.types.is_numeric_dtype(df[column]):
            try:
                value = float(value)
                return df[df[column] == value]
            except ValueError:
                st.warning("Please enter a valid number for numeric filtering")
                return df
        else:
            return df[df[column].astype(str).str.contains(str(value), case=False)]
    return df


def show_data_statistics(df):
    st.subheader("Dataset Statistics")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Records", len(df))
    with col2:
        st.metric("Total Countries", df["country"].nunique())
    with col3:
        st.metric("Total Products", df["product_name"].nunique())

    with st.expander("View Column Information"):
        col_info = pd.DataFrame(
            {
                "Data Type": df.dtypes,
                "Non-Null Count": df.count(),
                "Null Count": df.isnull().sum(),
                "Unique Values": df.nunique(),
            }
        )
        st.dataframe(col_info)


def show_quick_visualizations(df):
    st.subheader("Quick Visualizations")

    viz_type = st.selectbox(
        "Select Visualization",
        ["Products per Country", "Price Distribution", "Time Series Overview"],
    )

    if viz_type == "Products per Country":
        fig = px.bar(
            df.groupby("country")["product_name"].nunique().reset_index(),
            x="country",
            y="product_name",
            labels={"product_name": "Number of Unique Products"},
            title="Number of Products per Country",
        )
        st.plotly_chart(fig)

    elif viz_type == "Price Distribution":
        fig = px.box(
            df,
            x="product_name",
            y="price_per_kg_usd",
            title="Price Distribution(USD) by Product",
        )
        fig.update_layout(xaxis={"tickangle": 45}, yaxis=dict(tickformat=".2f"))
        st.plotly_chart(fig)

    elif viz_type == "Time Series Overview":
        fig = px.line(
            df.groupby("constructed_date")["price_per_kg_usd"].mean().reset_index(),
            x="constructed_date",
            y="price_per_kg_usd",
            title="Average Price Over Time",
        )
        st.plotly_chart(fig)


def main():
    st.title("Dataset Viewer")
    initialize_session_state()

    # Load data
    df = load_data()

    # Sidebar controls
    with st.sidebar:
        st.header("Controls")

        # Rows per page selector
        st.session_state.rows_per_page = st.select_slider(
            "Rows per page",
            options=[50, 100, 200, 500],
            value=st.session_state.rows_per_page,
        )

        # Filtering options
        st.subheader("Filter Data")
        filter_column = st.selectbox(
            "Select column to filter",
            ["None"] + list(df.columns),
            key="filter_column_select",
        )

        if filter_column != "None":
            filter_value = st.text_input(
                f"Filter value for {filter_column}", key="filter_value_input"
            )
            st.session_state.filter_column = filter_column
            st.session_state.filter_value = filter_value

        # Download button
        st.download_button(
            "Download Full Dataset",
            df.to_csv(index=False).encode("utf-8"),
            "market_flow_data.csv",
            "text/csv",
            key="download-csv",
        )

    # Apply filters
    filtered_df = apply_filters(
        df, st.session_state.filter_column, st.session_state.filter_value
    )

    # Show data statistics
    show_data_statistics(filtered_df)

    # Pagination
    N = st.session_state.rows_per_page
    last_page = len(filtered_df) // N

    # Pagination controls
    col1, col2, col3, col4 = st.columns([2, 1, 1, 2])

    with col1:
        if st.button("◀◀ First"):
            st.session_state.page_number = 0

    with col2:
        if st.button("◀ Previous"):
            if st.session_state.page_number > 0:
                st.session_state.page_number -= 1
            else:
                st.session_state.page_number = last_page

    with col3:
        if st.button("Next ▶"):
            if st.session_state.page_number < last_page:
                st.session_state.page_number += 1
            else:
                st.session_state.page_number = 0

    with col4:
        if st.button("Last ▶▶"):
            st.session_state.page_number = last_page

    # Page number indicator
    st.markdown(f"**Page {st.session_state.page_number + 1} of {last_page + 1}**")

    # Display dataframe
    start_idx = st.session_state.page_number * N
    end_idx = (1 + st.session_state.page_number) * N
    sub_df = filtered_df.iloc[start_idx:end_idx]
    st.dataframe(sub_df)

    # Show visualizations in expandable section
    with st.expander("View Quick Visualizations"):
        show_quick_visualizations(filtered_df)


main()
