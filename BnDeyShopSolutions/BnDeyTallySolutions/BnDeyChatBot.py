# app.py

import re
from datetime import datetime
import streamlit as st
from pymongo import MongoClient

# ---------------- PAGE CONFIG ----------------

st.set_page_config(
    page_title="BN Dey Inventory Chatbot",
    page_icon="🥃",
    layout="wide"
)

# ---------------- BLACK THEME ----------------

st.markdown("""
<style>
.stApp {
    background-color: #000000;
    color: white;
}

.main .block-container {
    background-color: #000000;
    padding-top: 1rem;
}

section[data-testid="stSidebar"] {
    background-color: #111111;
}

h1,h2,h3,h4,h5,h6,p,label,span,div {
    color: white !important;
}

[data-testid="stChatMessage"] {
    background-color: #111111 !important;
    border-radius: 12px;
    padding: 10px;
}

.stButton button {
    background-color: #D4AF37;
    color: black;
    border-radius: 8px;
    border: none;
}

hr {
    border-color: #333333;
}
</style>
""", unsafe_allow_html=True)


# ---------------- HEADER WITH LOGO ----------------

LOGO_FILE = "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyTallySolutions/bbndeylogo.png"

col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    try:
        st.image(LOGO_FILE, use_container_width=True)
    except Exception:
        st.markdown(
            """
            <h1 style='text-align:center;color:#D4AF37 !important;'>
                B.N. DEY
            </h1>
            """,
            unsafe_allow_html=True
        )

st.markdown(
    """
    <h1 style='text-align:center;color:#D4AF37 !important;margin-top:0px;'>
        BN Dey Inventory Chatbot
    </h1>

    <h3 style='text-align:center;color:#D4AF37 !important;'>
        Serving Since 1861
    </h3>

    <p style='text-align:center;font-size:18px;color:#CCCCCC !important;'>
        Stock Availability • Price Lookup • Daily Sales Analytics
    </p>
    """,
    unsafe_allow_html=True
)

st.divider()


# ---------------- MONGODB CONFIG ----------------

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"

STOCK_COLLECTION = "tally_stock_items"
SALES_COLLECTION = "tally_daily_sales"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

stock_collection = db[STOCK_COLLECTION]
sales_collection = db[SALES_COLLECTION]


# ---------------- HELPERS ----------------

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


# ---------------- STOCK NORMALIZATION ----------------

def update_normalized_stock_names():
    docs = stock_collection.find({}, {"stock_item_name": 1})
    updated = 0

    for doc in docs:
        stock_item_name = doc.get("stock_item_name", "")

        stock_collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "stock_item_name_normalized": normalize(stock_item_name)
                }
            }
        )

        updated += 1

    stock_collection.create_index("stock_item_name_normalized")
    return updated


# ---------------- STOCK SEARCH ----------------

def search_stock_item(user_input):
    normalized_query = normalize(user_input)

    item = stock_collection.find_one(
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

    return item


def format_stock_result(item):
    if not item:
        return """
### No exact stock item found

Please enter the exact Tally stock item name.

Example:

`OFFICERS CHOICE PRESTIGE WHISKY 375 - ML`
"""

    quantity = float(item.get("quantity", 0))
    rate = float(item.get("rate", 0))

    if quantity <= 0:
        stock_status = f"❌ NOT AVAILABLE — {quantity:.0f} {item.get('unit', 'NOS')}"
    elif quantity <= 5:
        stock_status = f"⚠️ LOW STOCK — {quantity:.0f} {item.get('unit', 'NOS')} available"
    else:
        stock_status = f"✅ AVAILABLE — {quantity:.0f} {item.get('unit', 'NOS')} available"

    price_status = "Price not configured" if rate <= 0 else f"₹{rate:,.2f}"

    return f"""
### {item.get("stock_item_name", "")}

**Brand:** {item.get("brand", "")}  
**Size:** {item.get("size_ml", "")} ML  
**Price:** {price_status}  
**Stock Quantity:** {quantity:.0f} {item.get("unit", "NOS")}  
**Stock Status:** {stock_status}  
**Source:** {item.get("source", "")}
"""


# ---------------- DAILY SALES ----------------

def extract_date_from_query(query):
    query = query.lower()

    if "today" in query:
        return datetime.today().strftime("%Y-%m-%d")

    match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", query)
    if match:
        return match.group(1)

    match = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", query)
    if match:
        dt = datetime.strptime(match.group(1), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")

    return datetime.today().strftime("%Y-%m-%d")


def get_daily_sales(date_text):
    pipeline = [
        {
            "$match": {
                "voucher_date": date_text
            }
        },
        {
            "$group": {
                "_id": {
                    "stock_item_name": "$stock_item_name",
                    "brand": "$brand",
                    "size_ml": "$size_ml",
                    "rate": "$rate",
                    "unit": "$unit"
                },
                "quantity": {
                    "$sum": "$quantity"
                },
                "amount": {
                    "$sum": "$amount"
                },
                "voucher_count": {
                    "$sum": 1
                }
            }
        },
        {
            "$sort": {
                "quantity": -1
            }
        }
    ]

    item_sales = list(sales_collection.aggregate(pipeline))

    total_quantity = sum(float(item.get("quantity", 0)) for item in item_sales)
    total_amount = sum(float(item.get("amount", 0)) for item in item_sales)
    total_vouchers = sum(int(item.get("voucher_count", 0)) for item in item_sales)

    return {
        "date": date_text,
        "total_quantity": total_quantity,
        "total_amount": total_amount,
        "total_vouchers": total_vouchers,
        "items": item_sales
    }


def render_daily_sales(query):
    date_text = extract_date_from_query(query)
    sales = get_daily_sales(date_text)

    st.markdown(f"""
## Daily Sales Summary

📅 Date: **{sales["date"]}**

🍾 Total Quantity Sold: **{sales["total_quantity"]:,.0f} NOS**

💰 Total Sales Amount: **₹{sales["total_amount"]:,.2f}**

🧾 Total Voucher Lines: **{sales["total_vouchers"]:,}**
""")

    if not sales["items"]:
        st.info("No sales found for this date.")
        return

    with st.expander("View Item-wise Voucher Lines", expanded=False):
        for item in sales["items"]:
            data = item["_id"]

            qty = float(item.get("quantity", 0))
            amount = float(item.get("amount", 0))
            rate = float(data.get("rate", 0))
            calculated_amount = qty * rate

            st.markdown(f"""
### {data.get("stock_item_name", "")}

Brand: **{data.get("brand", "")}**  
Size: **{data.get("size_ml", "")} ML**  
Qty Sold: **{qty:,.0f} {data.get("unit", "NOS")}**  
Rate: **₹{rate:,.2f}**  
Amount from Tally: **₹{amount:,.2f}**  
Calculated Amount: **₹{calculated_amount:,.2f}**

---
""")


# ---------------- CHAT ROUTER ----------------

def is_sales_query(query):
    query = query.lower()

    keywords = [
        "daily sale",
        "daily sales",
        "sales today",
        "today sale",
        "today sales",
        "sale figure",
        "sales figure",
        "total sale",
        "total sales",
        "quantity sold",
        "bottles sold",
        "voucher sales"
    ]

    return any(keyword in query for keyword in keywords)


def handle_chat(user_input):
    item = search_stock_item(user_input)
    return format_stock_result(item)


# ---------------- SIDEBAR ----------------

with st.sidebar:
    st.header("Admin")

    if st.button("Create / Refresh Normalized Stock Names"):
        count = update_normalized_stock_names()
        st.success(f"Updated {count} stock items")

    st.info("Click this after importing new Tally stock data.")


# ---------------- CHAT UI ----------------

if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": """
Ask me stock or sales questions.

**Stock example:**

`OFFICERS CHOICE PRESTIGE WHISKY 375 - ML`

**Sales examples:**

`daily sales 2026-04-07`

`total sales 07/04/2026`

`quantity sold today`
"""
        }
    ]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

user_input = st.chat_input("Ask stock or sales question...")

if user_input:
    st.session_state.messages.append({
        "role": "user",
        "content": user_input
    })

    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        if is_sales_query(user_input):
            render_daily_sales(user_input)

            st.session_state.messages.append({
                "role": "assistant",
                "content": f"Daily sales summary displayed for {extract_date_from_query(user_input)}."
            })
        else:
            answer = handle_chat(user_input)

            st.markdown(answer)

            st.session_state.messages.append({
                "role": "assistant",
                "content": answer
            })