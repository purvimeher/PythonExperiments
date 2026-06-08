# app.py

import streamlit as st
import pandas as pd
from pymongo import MongoClient

# --------------------------------------------------
# MongoDB Config
# --------------------------------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_purchase"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# --------------------------------------------------
# Page Config
# --------------------------------------------------
st.set_page_config(
    page_title="Tally Purchase Viewer",
    layout="wide"
)

st.title("Tally Purchase Viewer")

# --------------------------------------------------
# Load Data
# --------------------------------------------------
@st.cache_data(ttl=60)
def load_purchase_data():
    records = list(collection.find({}, {"_id": 0}))

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    if "voucher_date" in df.columns:
        df["voucher_date"] = pd.to_datetime(df["voucher_date"], errors="coerce")

    if "uploaded_at" in df.columns:
        df["uploaded_at"] = pd.to_datetime(df["uploaded_at"], errors="coerce")

    return df


df = load_purchase_data()

if df.empty:
    st.warning("No purchase records found in tally_purchase collection.")
    st.stop()

# --------------------------------------------------
# Sidebar Filters
# --------------------------------------------------
st.sidebar.header("Filters")

min_date = df["voucher_date"].min().date()
max_date = df["voucher_date"].max().date()

date_range = st.sidebar.date_input(
    "Voucher Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

supplier_filter = st.sidebar.multiselect(
    "Supplier",
    sorted(df["supplier_name"].dropna().unique())
)

brand_filter = st.sidebar.multiselect(
    "Brand",
    sorted(df["brand"].dropna().unique())
)

size_filter = st.sidebar.multiselect(
    "Size ML",
    sorted(df["size_ml"].dropna().unique())
)

voucher_filter = st.sidebar.text_input("Voucher Number")

stock_search = st.sidebar.text_input("Search Stock Item")

# --------------------------------------------------
# Apply Filters
# --------------------------------------------------
filtered_df = df.copy()

if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = filtered_df[
        (filtered_df["voucher_date"].dt.date >= start_date)
        & (filtered_df["voucher_date"].dt.date <= end_date)
    ]

if supplier_filter:
    filtered_df = filtered_df[
        filtered_df["supplier_name"].isin(supplier_filter)
    ]

if brand_filter:
    filtered_df = filtered_df[
        filtered_df["brand"].isin(brand_filter)
    ]

if size_filter:
    filtered_df = filtered_df[
        filtered_df["size_ml"].isin(size_filter)
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

# --------------------------------------------------
# Summary Metrics
# --------------------------------------------------
total_records = len(filtered_df)
total_qty = filtered_df["quantity"].sum()
total_amount = filtered_df["amount"].sum()
unique_items = filtered_df["stock_item_name"].nunique()
unique_brands = filtered_df["brand"].nunique()

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Purchase Lines", f"{total_records:,}")
c2.metric("Total Quantity", f"{total_qty:,.0f}")
c3.metric("Total Purchase", f"₹{total_amount:,.2f}")
c4.metric("Unique Items", f"{unique_items:,}")
c5.metric("Unique Brands", f"{unique_brands:,}")

# --------------------------------------------------
# Graphs
# --------------------------------------------------
st.divider()
st.subheader("Purchase Graphs")

filtered_df["amount"] = pd.to_numeric(
    filtered_df["amount"],
    errors="coerce"
).fillna(0)

filtered_df["quantity"] = pd.to_numeric(
    filtered_df["quantity"],
    errors="coerce"
).fillna(0)

daily_purchase = (
    filtered_df
    .groupby(filtered_df["voucher_date"].dt.date)
    .agg(
        total_amount=("amount", "sum"),
        total_quantity=("quantity", "sum")
    )
    .reset_index()
    .rename(columns={"voucher_date": "date"})
)

st.subheader("Daily Purchase Amount Trend")
st.line_chart(
    daily_purchase,
    x="date",
    y="total_amount",
    use_container_width=True
)

st.subheader("Daily Purchase Quantity Trend")
st.line_chart(
    daily_purchase,
    x="date",
    y="total_quantity",
    use_container_width=True
)

brand_amount = (
    filtered_df
    .groupby("brand", as_index=False)
    .agg(total_amount=("amount", "sum"))
    .sort_values("total_amount", ascending=False)
    .head(20)
)

st.subheader("Top 20 Brands by Purchase Amount")
st.bar_chart(
    brand_amount,
    x="brand",
    y="total_amount",
    use_container_width=True
)

brand_qty = (
    filtered_df
    .groupby("brand", as_index=False)
    .agg(total_quantity=("quantity", "sum"))
    .sort_values("total_quantity", ascending=False)
    .head(20)
)

st.subheader("Top 20 Brands by Quantity Purchased")
st.bar_chart(
    brand_qty,
    x="brand",
    y="total_quantity",
    use_container_width=True
)

size_qty = (
    filtered_df
    .groupby("size_ml", as_index=False)
    .agg(total_quantity=("quantity", "sum"))
    .sort_values("size_ml")
)

st.subheader("Size Wise Quantity Purchased")
st.bar_chart(
    size_qty,
    x="size_ml",
    y="total_quantity",
    use_container_width=True
)

supplier_amount = (
    filtered_df
    .groupby("supplier_name", as_index=False)
    .agg(total_amount=("amount", "sum"))
    .sort_values("total_amount", ascending=False)
    .head(20)
)

st.subheader("Top Suppliers by Purchase Amount")
st.bar_chart(
    supplier_amount,
    x="supplier_name",
    y="total_amount",
    use_container_width=True
)

# --------------------------------------------------
# Result Table
# --------------------------------------------------
st.divider()
st.subheader("Purchase Transactions")

display_columns = [
    "voucher_date",
    "voucher_number",
    "supplier_name",
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
    col for col in display_columns
    if col in filtered_df.columns
]

result_df = filtered_df[existing_columns].sort_values(
    by=["voucher_date", "voucher_number", "stock_item_name"],
    ascending=True
)

st.dataframe(
    result_df,
    use_container_width=True,
    hide_index=True
)

# --------------------------------------------------
# Supplier Summary
# --------------------------------------------------
st.subheader("Supplier Summary")

supplier_summary = (
    filtered_df
    .groupby("supplier_name", as_index=False)
    .agg(
        total_quantity=("quantity", "sum"),
        total_amount=("amount", "sum"),
        unique_items=("stock_item_name", "nunique")
    )
    .sort_values("total_amount", ascending=False)
)

st.dataframe(
    supplier_summary,
    use_container_width=True,
    hide_index=True
)

# --------------------------------------------------
# Brand Summary
# --------------------------------------------------
st.subheader("Brand Summary")

brand_summary = (
    filtered_df
    .groupby(["brand", "size_ml"], as_index=False)
    .agg(
        total_quantity=("quantity", "sum"),
        total_amount=("amount", "sum"),
        average_rate=("rate", "mean")
    )
    .sort_values("total_amount", ascending=False)
)

st.dataframe(
    brand_summary,
    use_container_width=True,
    hide_index=True
)

# --------------------------------------------------
# CSV Download
# --------------------------------------------------
csv = result_df.to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download Filtered Purchase CSV",
    data=csv,
    file_name="filtered_tally_purchase.csv",
    mime="text/csv"
)