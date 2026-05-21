import streamlit as st
import pandas as pd
from pymongo import MongoClient

st.set_page_config(page_title="Stock Journal Viewer", layout="wide")

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "stock_journal"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

st.title("Stock Journal Viewer")

@st.cache_data(ttl=30)
def load_data():
    records = list(collection.find({}, {"_id": 0}))
    df = pd.DataFrame(records)

    if df.empty:
        return df

    if "voucher_date" in df.columns:
        df["voucher_date"] = pd.to_datetime(df["voucher_date"]).dt.date

    if "uploaded_at" in df.columns:
        df["uploaded_at" ] = pd.to_datetime(df["uploaded_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    return df

df = load_data()

if df.empty:
    st.warning("No records found in stock_journal collection.")
    st.stop()

st.sidebar.header("Filters")

voucher_numbers = sorted(df["voucher_number"].dropna().unique()) if "voucher_number" in df else []
godowns = sorted(df["godown"].dropna().unique()) if "godown" in df else []
items = sorted(df["stock_item_name"].dropna().unique()) if "stock_item_name" in df else []
sizes = sorted(df["size_ml"].dropna().unique()) if "size_ml" in df else []

selected_voucher = st.sidebar.multiselect("Voucher Number", voucher_numbers)
selected_godown = st.sidebar.multiselect("Godown", godowns)
selected_item = st.sidebar.multiselect("Stock Item", items)
selected_size = st.sidebar.multiselect("Size ML", sizes)

search_text = st.sidebar.text_input("Search stock item")

filtered_df = df.copy()

if selected_voucher:
    filtered_df = filtered_df[filtered_df["voucher_number"].isin(selected_voucher)]

if selected_godown:
    filtered_df = filtered_df[filtered_df["godown"].isin(selected_godown)]

if selected_item:
    filtered_df = filtered_df[filtered_df["stock_item_name"].isin(selected_item)]

if selected_size:
    filtered_df = filtered_df[filtered_df["size_ml"].isin(selected_size)]

if search_text:
    filtered_df = filtered_df[
        filtered_df["stock_item_name"]
        .str.contains(search_text, case=False, na=False)
    ]

st.subheader("Summary")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Records", len(filtered_df))
col2.metric("Total Quantity", int(filtered_df["quantity"].sum()))
col3.metric("Total Value", f"{filtered_df['amount'].sum():,.2f}")
col4.metric("Unique Items", filtered_df["stock_item_name"].nunique())

st.subheader("Stock Journal Records")

display_columns = [
    "voucher_date",
    "voucher_number",
    "voucher_type",
    "stock_item_name",
    "size_ml",
    "godown",
    "quantity",
    "billed_quantity",
    "rate",
    "amount",
    "unit",
    "batch_name",
    "source",
    "uploaded_at"
]

available_columns = [col for col in display_columns if col in filtered_df.columns]

st.dataframe(
    filtered_df[available_columns],
    use_container_width=True,
    hide_index=True
)

csv = filtered_df[available_columns].to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download Filtered CSV",
    data=csv,
    file_name="stock_journal_filtered.csv",
    mime="text/csv"
)