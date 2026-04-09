import pandas as pd
from datetime import datetime
import random


class DataLoader:
    def __init__(self, product_file, sales_file, stock_file):
        self.product_file = product_file
        self.sales_file = sales_file
        self.stock_file = stock_file

    def load_products(self):
        return pd.read_csv(self.product_file, parse_dates=["last_price_update"])

    def load_sales(self):
        return pd.read_csv(self.sales_file, parse_dates=["date"])

    def load_stock(self):
        return pd.read_csv(self.stock_file, parse_dates=["date"])


class InventoryManager:
    def __init__(self, products_df):
        self.products = products_df

    def update_inventory(self, sales_df, stock_df):
        # Aggregate sales
        sales_agg = sales_df.groupby("product_id")["quantity"].sum().reset_index()
        sales_agg.rename(columns={"quantity": "sold"}, inplace=True)

        # Aggregate stock additions
        stock_agg = stock_df.groupby("product_id")["quantity"].sum().reset_index()
        stock_agg.rename(columns={"quantity": "added"}, inplace=True)

        # Merge
        df = self.products.merge(sales_agg, on="product_id", how="left")
        df = df.merge(stock_agg, on="product_id", how="left")

        df["sold"] = df["sold"].fillna(0)
        df["added"] = df["added"].fillna(0)

        # Update inventory
        df["inventory"] = df["inventory"] + df["added"] - df["sold"]

        self.products = df.drop(columns=["sold", "added"])
        return self.products


class PriceManager:
    def __init__(self, products_df):
        self.products = products_df

    def update_prices(self):
        today = pd.Timestamp(datetime.today())

        def adjust(row):
            months = (today.year - row["last_price_update"].year) * 12 + \
                     (today.month - row["last_price_update"].month)

            if months >= 6:
                change_pct = random.uniform(-0.1, 0.1)  # ±10%
                row["price"] = round(row["price"] * (1 + change_pct), 2)
                row["last_price_update"] = today

            return row

        self.products = self.products.apply(adjust, axis=1)
        return self.products


class InventorySystem:
    def __init__(self, product_file, sales_file, stock_file):
        self.loader = DataLoader(product_file, sales_file, stock_file)

    def run(self):
        products = self.loader.load_products()
        sales = self.loader.load_sales()
        stock = self.loader.load_stock()

        # Inventory update
        inventory_mgr = InventoryManager(products)
        products = inventory_mgr.update_inventory(sales, stock)

        # Price update
        price_mgr = PriceManager(products)
        products = price_mgr.update_prices()

        # Save updated data
        products.to_csv(self.loader.product_file, index=False)

        print("Updated Inventory:")
        print(products)


if __name__ == "__main__":
    system = InventorySystem(
        "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/data/products.csv",
        "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/data/sales.csv",
        "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/data/stock_in.csv"
    )
    system.run()
