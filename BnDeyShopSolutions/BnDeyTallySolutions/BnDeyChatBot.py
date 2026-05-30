# app.py

import re
import streamlit as st
from pymongo import MongoClient

# ---------------- CONFIG ----------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_stock_items"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

st.set_page_config(page_title="BN Dey Inventory Chatbot", layout="wide")

st.title("BN Dey Inventory Chatbot")
st.caption("Strict filter on stock_item_name")


# ---------------- NORMALIZE ----------------

def normalize(text):
    if not text:
        return ""

    text = str(text).upper()

    text = text.replace("-", " ")
    text = text.replace("_", " ")
    text = text.replace(".", "")
    text = text.replace(",", " ")
    text = text.replace("'", "")
    text = text.replace('"', "")

    text = text.replace("MILLILITERS", "ML")
    text = text.replace("MILLILITRES", "ML")
    text = text.replace("MILLILITER", "ML")
    text = text.replace("MILLILITRE", "ML")

    text = re.sub(r"\s+", " ", text)
    return text.strip()


# ---------------- ONE-TIME NORMALIZED FIELD UPDATE ----------------

def update_normalized_names():
    docs = collection.find({}, {"stock_item_name": 1})

    updated = 0

    for doc in docs:
        stock_item_name = doc.get("stock_item_name", "")

        normalized_name = normalize(stock_item_name)

        collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "stock_item_name_normalized": normalized_name
                }
            }
        )

        updated += 1

    collection.create_index("stock_item_name_normalized")

    return updated


# ---------------- SEARCH ----------------

def search_item_strict(user_input):
    normalized_query = normalize(user_input)

    result = collection.find_one(
        {
            "stock_item_name_normalized": normalized_query
        },
        {
            "_id": 0,
            "stock_item_name": 1,
            "brand": 1,
            "size_ml": 1,
            "quantity": 1,
            "rate": 1,
            "amount": 1,
            "unit": 1,
            "source": 1
        }
    )

    return result


def format_result(item):
    if not item:
        return """
No exact stock item found.

Please enter the full stock item name exactly like Tally.

Example:

`BLACK BY BACARDI CLASSIC ORIGINAL PREMIUM CRAFTED RUM 180 - ML`
"""

    quantity = item.get("quantity", 0)
    rate = item.get("rate", 0)

    response = f"""
### {item.get("stock_item_name", "")}

Brand: **{item.get("brand", "")}**  
Size: **{item.get("size_ml", "")} ml**  
Price: **₹{rate}**  
Stock: **{quantity} {item.get("unit", "NOS")}**  
Source: **{item.get("source", "")}**
"""

    if quantity <= 0:
        response += "\nStatus: **Out of stock**"
    elif quantity <= 5:
        response += "\nStatus: **Low stock**"
    else:
        response += "\nStatus: **Available**"

    return response


# ---------------- SIDEBAR ----------------

with st.sidebar:
    st.header("Admin")

    if st.button("Create / Refresh Normalized Names"):
        count = update_normalized_names()
        st.success(f"Updated {count} stock items")

    st.info(
        "Click this once after importing new Tally stock data."
    )


# ---------------- CHAT UI ----------------

if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": """
Enter the exact stock item name from Tally.

Example:

`BLACK BY BACARDI CLASSIC ORIGINAL PREMIUM CRAFTED RUM 180 - ML`
"""
        }
    ]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

user_input = st.chat_input("Enter exact stock item name...")

if user_input:
    st.session_state.messages.append({
        "role": "user",
        "content": user_input
    })

    with st.chat_message("user"):
        st.markdown(user_input)

    item = search_item_strict(user_input)
    answer = format_result(item)

    st.session_state.messages.append({
        "role": "assistant",
        "content": answer
    })

    with st.chat_message("assistant"):
        st.markdown(answer)