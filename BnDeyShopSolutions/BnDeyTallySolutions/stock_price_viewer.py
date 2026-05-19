import streamlit as st
import pandas as pd
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "stock_prices_tally"

st.set_page_config(page_title="Stock Price Viewer", layout="wide")

st.title("Tally Stock Price Viewer")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

docs = list(collection.find({}))

if not docs:
    st.warning("No stock price records found.")
    st.stop()

df = pd.DataFrame(docs)

# Remove MongoDB _id from result set
if "_id" in df.columns:
    df = df.drop(columns=["_id"])

# Convert date columns
if "price_date" in df.columns:
    df["price_date"] = pd.to_datetime(df["price_date"], errors="coerce")

if "uploaded_at" in df.columns:
    df["uploaded_at"] = pd.to_datetime(df["uploaded_at"], errors="coerce")

st.sidebar.header("Filters")

search_text = st.sidebar.text_input("Search stock item or brand")

price_levels = sorted(df["price_level"].dropna().unique().tolist())
selected_price_levels = st.sidebar.multiselect(
    "Price Level",
    price_levels,
    default=price_levels
)

sizes = sorted(df["size_ml"].dropna().unique().tolist())
selected_sizes = st.sidebar.multiselect("Size ML", sizes)

min_rate = float(df["rate"].min())
max_rate = float(df["rate"].max())

rate_range = st.sidebar.slider(
    "Rate Range",
    min_value=min_rate,
    max_value=max_rate,
    value=(min_rate, max_rate)
)

filtered_df = df.copy()

if search_text:
    filtered_df = filtered_df[
        filtered_df["stock_item_name"].str.contains(search_text, case=False, na=False)
        | filtered_df["brand"].str.contains(search_text, case=False, na=False)
    ]

if selected_price_levels:
    filtered_df = filtered_df[
        filtered_df["price_level"].isin(selected_price_levels)
    ]

if selected_sizes:
    filtered_df = filtered_df[
        filtered_df["size_ml"].isin(selected_sizes)
    ]

filtered_df = filtered_df[
    (filtered_df["rate"] >= rate_range[0])
    & (filtered_df["rate"] <= rate_range[1])
]

st.subheader("Summary")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Price Records", len(filtered_df))
col2.metric("Unique Brands", filtered_df["brand"].nunique())
col3.metric("Unique Sizes", filtered_df["size_ml"].nunique())
col4.metric("Average Rate", round(filtered_df["rate"].mean(), 2) if len(filtered_df) else 0)

st.subheader("Stock Price List")

columns_to_show = [
    "stock_item_name",
    "brand",
    "size_ml",
    "price_level",
    "rate",
    "unit",
    "price_date_text",
    "starting_from",
    "ending_at",
    "discount",
    "source",
    "uploaded_at"
]

existing_columns = [col for col in columns_to_show if col in filtered_df.columns]

st.dataframe(
    filtered_df[existing_columns],
    use_container_width=True,
    hide_index=True
)

csv = filtered_df[existing_columns].to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download Filtered Price List CSV",
    data=csv,
    file_name="filtered_stock_prices.csv",
    mime="text/csv"
)