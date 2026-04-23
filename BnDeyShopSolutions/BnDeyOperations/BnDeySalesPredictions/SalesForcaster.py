# =========================
# 1. Import libraries
# =========================
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

# =========================
# 2. Load data
# =========================
# Example CSV columns expected:
# Date, Brand_Category, Brand, Size_ML, Qty

df = pd.read_csv("/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyOperations/data/Daily_Sales/Daily_sales_07022026.csv")

# Preview
print(df.head())

# =========================
# 3. Clean and prepare data
# =========================
df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
df = df.dropna(subset=["Date"])

# If Qty is missing or invalid, clean it
df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce")
df = df.dropna(subset=["Qty"])

# Aggregate total sales quantity per day
daily_sales = df.groupby("Date", as_index=False)["Qty"].sum()
daily_sales = daily_sales.sort_values("Date")

print(daily_sales.head())

# =========================
# 4. Create time-based features
# =========================
daily_sales["day"] = daily_sales["Date"].dt.day
daily_sales["month"] = daily_sales["Date"].dt.month
daily_sales["year"] = daily_sales["Date"].dt.year
daily_sales["dayofweek"] = daily_sales["Date"].dt.dayofweek
daily_sales["weekofyear"] = daily_sales["Date"].dt.isocalendar().week.astype(int)

# Lag features
daily_sales["lag_1"] = daily_sales["Qty"].shift(1)
daily_sales["lag_2"] = daily_sales["Qty"].shift(2)
daily_sales["lag_3"] = daily_sales["Qty"].shift(3)
daily_sales["rolling_mean_7"] = daily_sales["Qty"].rolling(window=7).mean()
daily_sales["rolling_mean_14"] = daily_sales["Qty"].rolling(window=14).mean()

daily_sales = daily_sales.dropna().reset_index(drop=True)

print(daily_sales.head())

# =========================
# 5. Define X and y
# =========================
feature_cols = [
    "day", "month", "year", "dayofweek", "weekofyear",
    "lag_1", "lag_2", "lag_3",
    "rolling_mean_7", "rolling_mean_14"
]

X = daily_sales[feature_cols]
y = daily_sales["Qty"]

# Keep time order, do not shuffle
split_index = int(len(daily_sales) * 0.8)

X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]

# =========================
# 6. Train model
# =========================
model = RandomForestRegressor(
    n_estimators=200,
    max_depth=10,
    random_state=42
)
model.fit(X_train, y_train)

# =========================
# 7. Evaluate model
# =========================
y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print(f"MAE: {mae:.2f}")
print(f"RMSE: {rmse:.2f}")

# =========================
# 8. Plot actual vs predicted
# =========================
results = daily_sales.iloc[split_index:].copy()
results["Predicted_Qty"] = y_pred

plt.figure(figsize=(12, 6))
plt.plot(results["Date"], results["Qty"], label="Actual Sales")
plt.plot(results["Date"], results["Predicted_Qty"], label="Predicted Sales")
plt.xlabel("Date")
plt.ylabel("Qty Sold")
plt.title("Daily Sales Prediction")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# =========================
# 9. Forecast next 7 days
# =========================
forecast_days = 7
forecast_data = daily_sales.copy()

future_predictions = []

for i in range(forecast_days):
    last_date = forecast_data["Date"].max()
    next_date = last_date + pd.Timedelta(days=1)

    lag_1 = forecast_data["Qty"].iloc[-1]
    lag_2 = forecast_data["Qty"].iloc[-2]
    lag_3 = forecast_data["Qty"].iloc[-3]
    rolling_mean_7 = forecast_data["Qty"].iloc[-7:].mean()
    rolling_mean_14 = forecast_data["Qty"].iloc[-14:].mean()

    next_row = pd.DataFrame([{
        "Date": next_date,
        "day": next_date.day,
        "month": next_date.month,
        "year": next_date.year,
        "dayofweek": next_date.dayofweek,
        "weekofyear": int(next_date.isocalendar().week),
        "lag_1": lag_1,
        "lag_2": lag_2,
        "lag_3": lag_3,
        "rolling_mean_7": rolling_mean_7,
        "rolling_mean_14": rolling_mean_14
    }])

    pred_qty = model.predict(next_row[feature_cols])[0]

    future_predictions.append({
        "Date": next_date,
        "Predicted_Qty": pred_qty
    })

    # append predicted result back for recursive forecasting
    new_hist_row = next_row.copy()
    new_hist_row["Qty"] = pred_qty
    forecast_data = pd.concat([forecast_data, new_hist_row], ignore_index=True)

future_df = pd.DataFrame(future_predictions)
print(future_df)

# Plot forecast
plt.figure(figsize=(10, 5))
plt.plot(future_df["Date"], future_df["Predicted_Qty"], marker="o")
plt.xlabel("Date")
plt.ylabel("Predicted Qty")
plt.title("Next 7 Days Sales Forecast")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

