import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient

# --------------------------------------------------
# MongoDB
# --------------------------------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_daywise_sales"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# --------------------------------------------------
# Page Setup
# --------------------------------------------------
st.set_page_config(
    page_title="Daywise Sales Dashboard",
    layout="wide"
)

st.title("📊 Tally Daywise Sales Dashboard")

# --------------------------------------------------
# Load Data
# --------------------------------------------------
@st.cache_data(ttl=60)
def load_data():

    records = list(
        collection.find(
            {},
            {"_id": 0}
        )
    )

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    df["voucher_date"] = pd.to_datetime(
        df["voucher_date"],
        errors="coerce"
    )

    if "uploaded_at" in df.columns:
        df["uploaded_at"] = pd.to_datetime(
            df["uploaded_at"],
            errors="coerce"
        )

    return df


df = load_data()

if df.empty:
    st.warning("No records found.")
    st.stop()

# --------------------------------------------------
# Sidebar Filters
# --------------------------------------------------
st.sidebar.header("Filters")

min_date = df["voucher_date"].min().date()
max_date = df["voucher_date"].max().date()

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date)
)

brand_filter = st.sidebar.multiselect(
    "Brand",
    sorted(df["brand"].dropna().unique())
)

size_filter = st.sidebar.multiselect(
    "Size ML",
    sorted(df["size_ml"].dropna().unique())
)

godown_filter = st.sidebar.multiselect(
    "Godown",
    sorted(df["godown"].dropna().unique())
)

party_filter = st.sidebar.multiselect(
    "Party Name",
    sorted(df["party_name"].dropna().unique())
)

voucher_filter = st.sidebar.text_input(
    "Voucher Number"
)

stock_search = st.sidebar.text_input(
    "Stock Item Search"
)

# --------------------------------------------------
# Apply Filters
# --------------------------------------------------
filtered_df = df.copy()

if len(date_range) == 2:
    start_date, end_date = date_range

    filtered_df = filtered_df[
        (filtered_df["voucher_date"].dt.date >= start_date) &
        (filtered_df["voucher_date"].dt.date <= end_date)
    ]

if brand_filter:
    filtered_df = filtered_df[
        filtered_df["brand"].isin(brand_filter)
    ]

if size_filter:
    filtered_df = filtered_df[
        filtered_df["size_ml"].isin(size_filter)
    ]

if godown_filter:
    filtered_df = filtered_df[
        filtered_df["godown"].isin(godown_filter)
    ]

if party_filter:
    filtered_df = filtered_df[
        filtered_df["party_name"].isin(party_filter)
    ]

if voucher_filter:
    filtered_df = filtered_df[
        filtered_df["voucher_number"]
        .astype(str)
        .str.contains(
            voucher_filter,
            case=False,
            na=False
        )
    ]

if stock_search:
    filtered_df = filtered_df[
        filtered_df["stock_item_name"]
        .str.contains(
            stock_search,
            case=False,
            na=False
        )
    ]

# --------------------------------------------------
# Metrics
# --------------------------------------------------
total_sales = filtered_df["amount"].sum()
total_qty = filtered_df["quantity"].sum()
total_records = len(filtered_df)
unique_brands = filtered_df["brand"].nunique()

c1, c2, c3, c4 = st.columns(4)

c1.metric("Sales Value", f"₹{total_sales:,.2f}")
c2.metric("Quantity Sold", f"{total_qty:,.0f}")
c3.metric("Transactions", total_records)
c4.metric("Brands Sold", unique_brands)

# --------------------------------------------------
# Daily Trend
# --------------------------------------------------
st.subheader("Daily Sales Trend")

daily_sales = (
    filtered_df
    .groupby("voucher_date", as_index=False)
    .agg(
        amount=("amount", "sum"),
        quantity=("quantity", "sum")
    )
)

fig = px.line(
    daily_sales,
    x="voucher_date",
    y="amount",
    markers=True,
    title="Daily Sales Amount"
)

st.plotly_chart(
    fig,
    use_container_width=True
)

# --------------------------------------------------
# Top Brands
# --------------------------------------------------
st.subheader("Top Brands By Sales")

brand_sales = (
    filtered_df
    .groupby("brand", as_index=False)
    .agg(
        amount=("amount", "sum")
    )
    .sort_values(
        "amount",
        ascending=False
    )
    .head(20)
)

fig = px.bar(
    brand_sales,
    x="brand",
    y="amount",
    title="Top 20 Brands"
)

st.plotly_chart(
    fig,
    use_container_width=True
)

# --------------------------------------------------
# Size Wise Sales
# --------------------------------------------------
st.subheader("Size Wise Sales")

size_sales = (
    filtered_df
    .groupby("size_ml", as_index=False)
    .agg(
        amount=("amount", "sum"),
        quantity=("quantity", "sum")
    )
)

fig = px.bar(
    size_sales,
    x="size_ml",
    y="quantity",
    title="Quantity Sold by Size"
)

st.plotly_chart(
    fig,
    use_container_width=True
)

# --------------------------------------------------
# Godown Wise Sales
# --------------------------------------------------
st.subheader("Godown Wise Sales")

godown_sales = (
    filtered_df
    .groupby("godown", as_index=False)
    .agg(
        amount=("amount", "sum")
    )
)

fig = px.pie(
    godown_sales,
    names="godown",
    values="amount",
    title="Sales by Godown"
)

st.plotly_chart(
    fig,
    use_container_width=True
)

# --------------------------------------------------
# Detailed Results
# --------------------------------------------------
st.subheader("Sales Transactions")

display_columns = [
    "voucher_date",
    "voucher_number",
    "party_name",
    "stock_item_name",
    "brand",
    "size_ml",
    "quantity",
    "rate",
    "amount",
    "godown",
    "batch_name",
    "price_level",
    "source"
]

display_columns = [
    c for c in display_columns
    if c in filtered_df.columns
]

st.dataframe(
    filtered_df[display_columns]
    .sort_values(
        ["voucher_date", "voucher_number"]
    ),
    hide_index=True,
    use_container_width=True
)

# --------------------------------------------------
# Brand Summary
# --------------------------------------------------
st.subheader("Brand Summary")

brand_summary = (
    filtered_df
    .groupby(
        ["brand", "size_ml"],
        as_index=False
    )
    .agg(
        Quantity=("quantity", "sum"),
        Sales_Value=("amount", "sum")
    )
    .sort_values(
        "Sales_Value",
        ascending=False
    )
)

st.dataframe(
    brand_summary,
    hide_index=True,
    use_container_width=True
)

# --------------------------------------------------
# CSV Download
# --------------------------------------------------
csv = filtered_df[
    display_columns
].to_csv(
    index=False
).encode("utf-8")

st.download_button(
    label="⬇ Download Filtered Sales CSV",
    data=csv,
    file_name="daywise_sales.csv",
    mime="text/csv"
)