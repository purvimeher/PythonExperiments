import streamlit as st
import pandas as pd
from pymongo import MongoClient

# MongoDB Config
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_stock_items"

# Streamlit Page
st.set_page_config(
    page_title="Tally Stock Viewer",
    layout="wide"
)

st.title("Tally Stock Items Viewer")

# Mongo Connection
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Load MongoDB Data
docs = list(collection.find({}))

if not docs:
    st.warning("No stock items found.")
    st.stop()

df = pd.DataFrame(docs)

# REMOVE OID COLUMN
if "_id" in df.columns:
    df.drop(columns=["_id"], inplace=True)

# Convert dates
if "uploaded_at" in df.columns:
    df["uploaded_at"] = pd.to_datetime(
        df["uploaded_at"],
        errors="coerce"
    )

# Sidebar Filters
st.sidebar.header("Filters")

# Search
search_text = st.sidebar.text_input(
    "Search Stock Item / Brand"
)

# Brand Filter
brands = sorted(
    df["brand"].dropna().unique().tolist()
)

selected_brands = st.sidebar.multiselect(
    "Filter Brands",
    brands
)

# Size Filter
sizes = sorted(
    df["size_ml"].dropna().unique().tolist()
)

selected_sizes = st.sidebar.multiselect(
    "Filter Size ML",
    sizes
)

# Quantity Range
min_qty = int(df["quantity"].min())
max_qty = int(df["quantity"].max())

qty_range = st.sidebar.slider(
    "Quantity Range",
    min_value=min_qty,
    max_value=max_qty,
    value=(min_qty, max_qty)
)

# Rate Range
min_rate = float(df["rate"].min())
max_rate = float(df["rate"].max())

rate_range = st.sidebar.slider(
    "Rate Range",
    min_value=min_rate,
    max_value=max_rate,
    value=(min_rate, max_rate)
)

# Apply Filters
filtered_df = df.copy()

# Search Filter
if search_text:
    filtered_df = filtered_df[
        filtered_df["stock_item_name"].str.contains(
            search_text,
            case=False,
            na=False
        )
        |
        filtered_df["brand"].str.contains(
            search_text,
            case=False,
            na=False
        )
    ]

# Brand Filter
if selected_brands:
    filtered_df = filtered_df[
        filtered_df["brand"].isin(selected_brands)
    ]

# Size Filter
if selected_sizes:
    filtered_df = filtered_df[
        filtered_df["size_ml"].isin(selected_sizes)
    ]

# Quantity Filter
filtered_df = filtered_df[
    (filtered_df["quantity"] >= qty_range[0]) &
    (filtered_df["quantity"] <= qty_range[1])
]

# Rate Filter
filtered_df = filtered_df[
    (filtered_df["rate"] >= rate_range[0]) &
    (filtered_df["rate"] <= rate_range[1])
]

# Summary Cards
st.subheader("Summary")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total Items",
    len(filtered_df)
)

col2.metric(
    "Total Quantity",
    int(filtered_df["quantity"].sum())
)

col3.metric(
    "Total Stock Value",
    int(filtered_df["amount"].sum())
)

col4.metric(
    "Unique Brands",
    filtered_df["brand"].nunique()
)

# Sort Option
sort_column = st.selectbox(
    "Sort By",
    [
        "stock_item_name",
        "brand",
        "size_ml",
        "quantity",
        "rate",
        "amount"
    ]
)

sort_order = st.radio(
    "Order",
    ["Ascending", "Descending"],
    horizontal=True
)

filtered_df = filtered_df.sort_values(
    by=sort_column,
    ascending=(sort_order == "Ascending")
)

# Display Table
st.subheader("Stock Items")

columns_to_show = [
    "stock_item_name",
    "brand",
    "size_ml",
    "quantity",
    "unit",
    "rate",
    "amount",
    "source",
    "uploaded_at"
]

existing_columns = [
    col for col in columns_to_show
    if col in filtered_df.columns
]

st.dataframe(
    filtered_df[existing_columns],
    use_container_width=True,
    hide_index=True
)

# Download CSV
csv = filtered_df[existing_columns].to_csv(
    index=False
).encode("utf-8")

st.download_button(
    label="Download Filtered CSV",
    data=csv,
    file_name="filtered_tally_stock_items.csv",
    mime="text/csv"
)