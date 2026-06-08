# streamlit_sales_forecast.py

import streamlit as st
import pandas as pd
from pymongo import MongoClient
from sklearn.ensemble import RandomForestRegressor
from datetime import timedelta

# -----------------------------
# Page Config
# -----------------------------
st.set_page_config(
    page_title="Next 14 Days Sales Forecast",
    layout="wide"
)

st.title("Next 14 Days Sales Forecast")

# -----------------------------
# MongoDB Config
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bndey_db"
COLLECTION_NAME = "tally_daywise_sales"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# -----------------------------
# Load Data
# -----------------------------
@st.cache_data(ttl=60)
def load_sales_data():
    records = list(
        collection.find(
            {},
            {
                "_id": 0,
                "voucher_date": 1,
                "quantity": 1,
                "amount": 1
            }
        )
    )

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    df["voucher_date"] = pd.to_datetime(
        df["voucher_date"],
        errors="coerce"
    )

    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    ).fillna(0)

    df["amount"] = pd.to_numeric(
        df["amount"],
        errors="coerce"
    ).fillna(0)

    df = df.dropna(subset=["voucher_date"])

    return df


df = load_sales_data()

if df.empty:
    st.warning("No records found in tally_daywise_sales collection.")
    st.stop()

# -----------------------------
# Aggregate Daily Sales
# -----------------------------
daily_df = (
    df.groupby("voucher_date", as_index=False)
    .agg(
        total_quantity=("quantity", "sum"),
        total_sales_amount=("amount", "sum")
    )
    .sort_values("voucher_date")
)

full_dates = pd.date_range(
    daily_df["voucher_date"].min(),
    daily_df["voucher_date"].max(),
    freq="D"
)

daily_df = (
    daily_df
    .set_index("voucher_date")
    .reindex(full_dates)
    .fillna(0)
    .rename_axis("voucher_date")
    .reset_index()
)

# -----------------------------
# Basic Checks
# -----------------------------
if len(daily_df) < 7:
    st.error("At least 7 days of sales data is recommended for forecasting.")
    st.stop()

# -----------------------------
# Feature Engineering
# -----------------------------
def add_features(data):
    data = data.copy()

    data["day_of_week"] = data["voucher_date"].dt.dayofweek
    data["day"] = data["voucher_date"].dt.day
    data["month"] = data["voucher_date"].dt.month
    data["is_weekend"] = data["day_of_week"].isin([5, 6]).astype(int)

    data["sales_lag_1"] = data["total_sales_amount"].shift(1)
    data["sales_lag_7"] = data["total_sales_amount"].shift(7)

    data["qty_lag_1"] = data["total_quantity"].shift(1)
    data["qty_lag_7"] = data["total_quantity"].shift(7)

    data["sales_rolling_7"] = (
        data["total_sales_amount"]
        .rolling(window=7)
        .mean()
    )

    data["qty_rolling_7"] = (
        data["total_quantity"]
        .rolling(window=7)
        .mean()
    )

    return data.fillna(0)


daily_df = add_features(daily_df)

features = [
    "day_of_week",
    "day",
    "month",
    "is_weekend",
    "sales_lag_1",
    "sales_lag_7",
    "qty_lag_1",
    "qty_lag_7",
    "sales_rolling_7",
    "qty_rolling_7"
]

# -----------------------------
# Train Models
# -----------------------------
X = daily_df[features]
y_sales = daily_df["total_sales_amount"]
y_qty = daily_df["total_quantity"]

sales_model = RandomForestRegressor(
    n_estimators=300,
    random_state=42
)

qty_model = RandomForestRegressor(
    n_estimators=300,
    random_state=42
)

sales_model.fit(X, y_sales)
qty_model.fit(X, y_qty)

# -----------------------------
# Forecast Function
# -----------------------------
def forecast_next_days(history_df, days=14):
    future_rows = []
    history = history_df.copy()
    last_date = history["voucher_date"].max()

    for i in range(1, days + 1):

        next_date = last_date + timedelta(days=i)

        last_row = history.iloc[-1]
        lag_7_row = history.iloc[-7] if len(history) >= 7 else last_row

        future_row = {
            "voucher_date": next_date,
            "day_of_week": next_date.dayofweek,
            "day": next_date.day,
            "month": next_date.month,
            "is_weekend": int(next_date.dayofweek in [5, 6]),
            "sales_lag_1": last_row["total_sales_amount"],
            "sales_lag_7": lag_7_row["total_sales_amount"],
            "qty_lag_1": last_row["total_quantity"],
            "qty_lag_7": lag_7_row["total_quantity"],
            "sales_rolling_7": history["total_sales_amount"].tail(7).mean(),
            "qty_rolling_7": history["total_quantity"].tail(7).mean()
        }

        X_future = pd.DataFrame([future_row])[features]

        predicted_sales = sales_model.predict(X_future)[0]
        predicted_qty = qty_model.predict(X_future)[0]

        predicted_sales = max(0, round(predicted_sales, 2))
        predicted_qty = max(0, round(predicted_qty, 0))

        future_rows.append({
            "forecast_date": next_date.date(),
            "predicted_quantity": int(predicted_qty),
            "predicted_sales_amount": predicted_sales
        })

        history = pd.concat(
            [
                history,
                pd.DataFrame([{
                    "voucher_date": next_date,
                    "total_quantity": predicted_qty,
                    "total_sales_amount": predicted_sales,
                    **future_row
                }])
            ],
            ignore_index=True
        )

    return pd.DataFrame(future_rows)


forecast_df = forecast_next_days(daily_df, days=14)

# -----------------------------
# Summary Metrics
# -----------------------------
total_forecast_sales = forecast_df["predicted_sales_amount"].sum()
total_forecast_qty = forecast_df["predicted_quantity"].sum()
avg_daily_sales = forecast_df["predicted_sales_amount"].mean()
avg_daily_qty = forecast_df["predicted_quantity"].mean()

c1, c2, c3, c4 = st.columns(4)

c1.metric("Forecast Sales 14 Days", f"₹{total_forecast_sales:,.2f}")
c2.metric("Forecast Quantity 14 Days", f"{total_forecast_qty:,.0f}")
c3.metric("Avg Daily Sales", f"₹{avg_daily_sales:,.2f}")
c4.metric("Avg Daily Quantity", f"{avg_daily_qty:,.0f}")

# -----------------------------
# Historical Sales View
# -----------------------------
st.subheader("Historical Daily Sales")

historical_display = daily_df[
    [
        "voucher_date",
        "total_quantity",
        "total_sales_amount"
    ]
].copy()

historical_display["voucher_date"] = historical_display[
    "voucher_date"
].dt.date

st.dataframe(
    historical_display.sort_values(
        "voucher_date",
        ascending=False
    ),
    use_container_width=True,
    hide_index=True
)

# -----------------------------
# Forecast Table
# -----------------------------
st.subheader("Next 14 Days Forecast")

st.dataframe(
    forecast_df,
    use_container_width=True,
    hide_index=True
)

# -----------------------------
# Charts
# -----------------------------
st.subheader("Forecast Sales Amount Trend")

st.line_chart(
    forecast_df,
    x="forecast_date",
    y="predicted_sales_amount",
    use_container_width=True
)

st.subheader("Forecast Quantity Trend")

st.line_chart(
    forecast_df,
    x="forecast_date",
    y="predicted_quantity",
    use_container_width=True
)

# -----------------------------
# Combined Historical + Forecast Chart
# -----------------------------
st.subheader("Historical vs Forecast Sales")

historical_chart = historical_display.rename(
    columns={
        "voucher_date": "date",
        "total_sales_amount": "sales_amount"
    }
)

historical_chart["type"] = "Historical"

forecast_chart = forecast_df.rename(
    columns={
        "forecast_date": "date",
        "predicted_sales_amount": "sales_amount"
    }
)

forecast_chart["type"] = "Forecast"

combined_chart = pd.concat(
    [
        historical_chart[["date", "sales_amount", "type"]],
        forecast_chart[["date", "sales_amount", "type"]]
    ],
    ignore_index=True
)

st.line_chart(
    combined_chart,
    x="date",
    y="sales_amount",
    color="type",
    use_container_width=True
)

# -----------------------------
# Download Forecast CSV
# -----------------------------
csv = forecast_df.to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download Forecast CSV",
    data=csv,
    file_name="next_14_days_sales_forecast.csv",
    mime="text/csv"
)