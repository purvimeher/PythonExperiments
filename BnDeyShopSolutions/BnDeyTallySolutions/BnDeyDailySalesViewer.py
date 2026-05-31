# app.py

import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import date

# -----------------------------
# MongoDB Config
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_daily_sales"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

st.set_page_config(
    page_title="Daily Sales Viewer",
    layout="wide"
)

st.title("Daily Sales Viewer")

# -----------------------------
# Load Data
# -----------------------------
@st.cache_data(ttl=60)
def load_sales():
    records = list(collection.find({}, {"_id": 0}))
    return pd.DataFrame(records)

df = load_sales()

if df.empty:
    st.warning("No daily sales records found.")
    st.stop()

# -----------------------------
# Clean Dates
# -----------------------------
df["voucher_date"] = pd.to_datetime(df["voucher_date"], errors="coerce")
df["uploaded_at"] = pd.to_datetime(df["uploaded_at"], errors="coerce")

# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.header("Filters")

min_date = df["voucher_date"].min().date()
max_date = df["voucher_date"].max().date()

date_range = st.sidebar.date_input(
    "Voucher Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

brand_filter = st.sidebar.multiselect(
    "Brand",
    sorted(df["brand"].dropna().unique())
)

size_filter = st.sidebar.multiselect(
    "Size ML",
    sorted(df["size_ml"].dropna().unique())
)

party_filter = st.sidebar.multiselect(
    "Party Name",
    sorted(df["party_name"].dropna().unique())
)

voucher_filter = st.sidebar.text_input(
    "Voucher Number"
)

stock_search = st.sidebar.text_input(
    "Search Stock Item"
)

# -----------------------------
# Apply Filters
# -----------------------------
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

if party_filter:
    filtered_df = filtered_df[
        filtered_df["party_name"].isin(party_filter)
    ]

if voucher_filter:
    filtered_df = filtered_df[
        filtered_df["voucher_number"].astype(str).str.contains(
            voucher_filter,
            case=False,
            na=False
        )
    ]

if stock_search:
    filtered_df = filtered_df[
        filtered_df["stock_item_name"].astype(str).str.contains(
            stock_search,
            case=False,
            na=False
        )
    ]

# -----------------------------
# Summary Cards
# -----------------------------
total_qty = filtered_df["quantity"].sum()
total_amount = filtered_df["amount"].sum()
total_items = len(filtered_df)
unique_brands = filtered_df["brand"].nunique()

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Sales Lines", total_items)
c2.metric("Total Quantity Sold", int(total_qty))
c3.metric("Total Sales Amount", f"₹{total_amount:,.2f}")
c4.metric("Unique Brands", unique_brands)

# -----------------------------
# Display Result
# -----------------------------
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
    "unit",
    "source",
    "uploaded_at"
]

existing_columns = [
    col for col in display_columns if col in filtered_df.columns
]

st.subheader("Daily Sales Results")

st.dataframe(
    filtered_df[existing_columns].sort_values(
        by=["voucher_date", "voucher_number", "stock_item_name"],
        ascending=True
    ),
    use_container_width=True,
    hide_index=True
)

# -----------------------------
# Brand Summary
# -----------------------------
st.subheader("Brand Wise Sales Summary")

brand_summary = (
    filtered_df
    .groupby(["brand", "size_ml"], as_index=False)
    .agg(
        total_quantity=("quantity", "sum"),
        total_amount=("amount", "sum"),
        avg_rate=("rate", "mean")
    )
    .sort_values(by=["brand", "size_ml"])
)

st.dataframe(
    brand_summary,
    use_container_width=True,
    hide_index=True
)

# -----------------------------
# CSV Download
# -----------------------------
csv = filtered_df[existing_columns].to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download Filtered Sales CSV",
    data=csv,
    file_name="filtered_daily_sales.csv",
    mime="text/csv"
)