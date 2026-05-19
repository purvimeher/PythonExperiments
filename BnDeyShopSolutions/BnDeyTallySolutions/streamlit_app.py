import streamlit as st
import pandas as pd
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_stock_items"

st.set_page_config(page_title="Tally Stock Viewer", layout="wide")

st.title("Tally Stock Items Viewer")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

docs = list(collection.find({}))

if not docs:
    st.warning("No stock items found in MongoDB.")
    st.stop()

df = pd.DataFrame(docs)

# Convert _id to string so it displays properly
df["_id"] = df["_id"].astype(str)

# Convert uploaded_at if present
if "uploaded_at" in df.columns:
    df["uploaded_at"] = pd.to_datetime(df["uploaded_at"], errors="coerce")

st.sidebar.header("Filters")

search_text = st.sidebar.text_input("Search stock item / brand")

size_options = sorted(df["size_ml"].dropna().unique().tolist())
selected_sizes = st.sidebar.multiselect("Filter by Size ML", size_options)

min_qty = int(df["quantity"].min())
max_qty = int(df["quantity"].max())

qty_range = st.sidebar.slider(
    "Quantity Range",
    min_value=min_qty,
    max_value=max_qty,
    value=(min_qty, max_qty)
)

amount_filter = st.sidebar.selectbox(
    "Amount Type",
    ["All", "Positive Amount", "Negative Amount", "Zero Amount"]
)

filtered_df = df.copy()

if search_text:
    filtered_df = filtered_df[
        filtered_df["stock_item_name"].str.contains(search_text, case=False, na=False)
        | filtered_df["brand"].str.contains(search_text, case=False, na=False)
    ]

if selected_sizes:
    filtered_df = filtered_df[filtered_df["size_ml"].isin(selected_sizes)]

filtered_df = filtered_df[
    (filtered_df["quantity"] >= qty_range[0])
    & (filtered_df["quantity"] <= qty_range[1])
]

if amount_filter == "Positive Amount":
    filtered_df = filtered_df[filtered_df["amount"] > 0]
elif amount_filter == "Negative Amount":
    filtered_df = filtered_df[filtered_df["amount"] < 0]
elif amount_filter == "Zero Amount":
    filtered_df = filtered_df[filtered_df["amount"] == 0]

st.subheader("Summary")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Items", len(filtered_df))
col2.metric("Total Quantity", int(filtered_df["quantity"].sum()))
col3.metric("Total Amount", int(filtered_df["amount"].sum()))
col4.metric("Unique Brands", filtered_df["brand"].nunique())

st.subheader("Stock Items")

columns_to_show = [
    "_id",
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

existing_columns = [col for col in columns_to_show if col in filtered_df.columns]

st.dataframe(
    filtered_df[existing_columns],
    use_container_width=True,
    hide_index=True
)

csv = filtered_df[existing_columns].to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download Filtered CSV",
    data=csv,
    file_name="filtered_tally_stock_items.csv",
    mime="text/csv"
)