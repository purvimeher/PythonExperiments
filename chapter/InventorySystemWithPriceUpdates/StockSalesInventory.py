import pandas as pd
from datetime import datetime, timedelta


class StockSalesInventory():

    def __init__(self):
        pass

    # -------------------------------
    # FUNCTION: UPDATE INVENTORY DAILY
    # -------------------------------
    def update_inventory(self, inventory, sales, stock_in):
        inventory = inventory.copy()

        # Aggregate sales and stock_in per day/product
        sales_agg = sales.groupby(["date", "product_id"])["quantity_sold"].sum().reset_index()
        stock_agg = stock_in.groupby(["date", "product_id"])["quantity_added"].sum().reset_index()

        # Merge everything
        df = pd.merge(inventory, sales_agg, on=["date", "product_id"], how="left")
        df = pd.merge(df, stock_agg, on=["date", "product_id"], how="left")

        df["quantity_sold"] = df["quantity_sold"].fillna(0)
        df["quantity_added"] = df["quantity_added"].fillna(0)

        # Update stock
        df["stock"] = df["stock"] - df["quantity_sold"] + df["quantity_added"]

        return df[["date", "product_id", "stock"]]

    # -------------------------------
    # FUNCTION: PRICE UPDATE (EVERY 6 MONTHS)
    # -------------------------------
    def update_prices(products, current_date):
        products = products.copy()

        for i, row in products.iterrows():
            last_update = row["last_price_update"]

            if (current_date - last_update).days >= 180:
                # Example: Random increase/decrease
                change_factor = 1.05 if i % 2 == 0 else 0.95  # alternate up/down
                new_price = round(row["price"] * change_factor, 2)

                products.at[i, "price"] = new_price
                products.at[i, "last_price_update"] = current_date

        return products

    # -------------------------------
    # FUNCTION: SIMULATE DAILY PROCESS
    # -------------------------------
    def run_daily_simulation(self, inventory, products, sales, stock_in, days=5):
        current_inventory = inventory.copy()
        current_products = products.copy()

        start_date = current_inventory["date"].min()

        all_inventory = []

        for day in range(days):
            current_date = start_date + timedelta(days=day)

            # Filter today's transactions
            daily_sales = sales[sales["date"] == current_date]
            daily_stock = stock_in[stock_in["date"] == current_date]

            # Prepare today's inventory snapshot
            if day > 0:
                prev_day = all_inventory[-1]
                today_inventory = prev_day.copy()
                today_inventory["date"] = current_date
            else:
                today_inventory = current_inventory.copy()

            # Update inventory
            today_inventory = self.update_inventory(today_inventory, daily_sales, daily_stock)

            # Update prices every 6 months
            current_products = self.update_prices(current_products, current_date)

            all_inventory.append(today_inventory)

        final_inventory = pd.concat(all_inventory, ignore_index=True)
        return final_inventory, current_products

    # -------------------------------
    # FUNCTION: GENERATE REPORT
    # -------------------------------
    def generate_report(self, inventory, sales, products):
        # Merge sales with price
        sales_report = pd.merge(sales, products, on="product_id", how="left")

        sales_report["revenue"] = sales_report["quantity_sold"] * sales_report["price"]

        summary = sales_report.groupby("product_name").agg({
            "quantity_sold": "sum",
            "revenue": "sum"
        }).reset_index()

        return summary





    # -------------------------------
    # RUN SIMULATION
    # -------------------------------

    # -------------------------------
    # 1. INITIAL PRODUCT DATA
    # ------------------------------
products = pd.DataFrame({
    "product_id": [101, 102, 103],
    "product_name": ["Laptop", "Mouse", "Keyboard"],
    "price": [1000, 25, 50],
    "last_price_update": [
        pd.Timestamp("2025-01-01"),
        pd.Timestamp("2025-01-01"),
        pd.Timestamp("2025-01-01")
    ]
})

# -------------------------------
# 2. INITIAL INVENTORY
# -------------------------------
inventory = pd.DataFrame({
    "date": [pd.Timestamp("2025-01-01")] * 3,
    "product_id": [101, 102, 103],
    "stock": [50, 200, 150]
})

# -------------------------------
# 3. SALES DATA (DAILY)
# -------------------------------
sales = pd.DataFrame({
    "date": [
        pd.Timestamp("2025-01-02"),
        pd.Timestamp("2025-01-02"),
        pd.Timestamp("2025-01-03")
    ],
    "product_id": [101, 102, 101],
    "quantity_sold": [2, 10, 3]
})

# -------------------------------
# 4. STOCK IN (RESTOCK)
# -------------------------------
stock_in = pd.DataFrame({
    "date": [
        pd.Timestamp("2025-01-02"),
        pd.Timestamp("2025-01-03")
    ],
    "product_id": [101, 103],
    "quantity_added": [5, 20]
})
stockSalesInventory = StockSalesInventory()
updated_inventory = stockSalesInventory.update_inventory(inventory, sales, stock_in)
final_inventory, updated_products = stockSalesInventory.run_daily_simulation(inventory, products, sales, stock_in, days=5)
# report = stockSalesInventory.generate_report(final_inventory, sales, updated_products)

print(updated_inventory)
# -------------------------------
# OUTPUT
# -------------------------------
print("Final Inventory:")
print(final_inventory)

print("\nUpdated Product Prices:")
print(updated_products)

print("\nSales Report:")
# print(report)
