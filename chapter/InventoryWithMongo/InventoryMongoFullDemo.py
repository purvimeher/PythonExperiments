from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import random


class InventoryMongoFullDemo:
    # -----------------------------
    # DB CONNECTION
    # -----------------------------
    def __init__(self):

        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["inventory_db"]

        self.products_col = self.db["products"]
        self.sales_col = self.db["sales"]
        self.inventory_logs_col = self.db["inventory_logs"]

    # -----------------------------
    # INITIAL PRODUCT SETUP
    # -----------------------------
    def seed_products(self):
        products = [
            {"product_id": 1, "name": "Laptop", "price": 1000, "stock": 50, "last_price_update": datetime.now()},
            {"product_id": 2, "name": "Phone", "price": 500, "stock": 100, "last_price_update": datetime.now()},
            {"product_id": 3, "name": "Headphones", "price": 100, "stock": 200, "last_price_update": datetime.now()},
        ]

        if self.products_col.count_documents({}) == 0:
            self.products_col.insert_many(products)
            print("Products seeded!")

    # -----------------------------
    # RECORD DAILY SALES
    # -----------------------------
    def record_sale(self, product_id, quantity):
        product = self.products_col.find_one({"product_id": product_id})

        if not product:
            print("Product not found")
            return

        if product["stock"] < quantity:
            print("Not enough stock")
            return

        # Update stock
        new_stock = product["stock"] - quantity
        self.products_col.update_one(
            {"product_id": product_id},
            {"$set": {"stock": new_stock}}
        )

        # Record sale
        self.sales_col.insert_one({
            "product_id": product_id,
            "quantity": quantity,
            "date": datetime.now()
        })

        # Log inventory change
        self.inventory_logs_col.insert_one({
            "product_id": product_id,
            "change": -quantity,
            "type": "sale",
            "date": datetime.now()
        })

    # -----------------------------
    # STOCK REPLENISHMENT
    # -----------------------------
    def restock(self, product_id, quantity):
        product = self.products_col.find_one({"product_id": product_id})

        if not product:
            print("Product not found")
            return

        new_stock = product["stock"] + quantity

        self.products_col.update_one(
            {"product_id": self.product_id},
            {"$set": {"stock": new_stock}}
        )

        self.inventory_logs_col.insert_one({
            "product_id": product_id,
            "change": quantity,
            "type": "restock",
            "date": datetime.now()
        })

    # -----------------------------
    # PRICE UPDATE EVERY 6 MONTHS
    # -----------------------------
    def update_prices(self):
        products = list(self.products_col.find({}))

        for product in products:
            last_update = product.get("last_price_update", datetime.now())

        if datetime.now() >= last_update + relativedelta(months=6):
            # Random price fluctuation (-10% to +10%)
            change_factor = random.uniform(0.9, 1.1)
            new_price = round(product["price"] * change_factor, 2)

            self.products_col.update_one(
                {"product_id": product["product_id"]},
                {
                    "$set": {
                        "price": new_price,
                        "last_price_update": datetime.now()
                    }
                }
            )
            print(f"Updated price for {product['name']} -> {new_price}")

    # -----------------------------
    # DAILY BATCH PROCESS
    # -----------------------------
    def daily_job(self):
        print("Running daily job...")

        # Simulate random sales
        for product in self.products_col.find():
            qty = random.randint(0, 5)
            if qty > 0:
                self.record_sale(product["product_id"], qty)

        # Auto restock if low
        for product in self.products_col.find():
            if product["stock"] < 20:
                self.restock(product["product_id"], 50)



    # -----------------------------
    # PANDAS ANALYTICS DASHBOARD
    # -----------------------------
    def generate_reports(self):
        sales_data = list(self.sales_col.find({}))

        if not sales_data:
            print("No sales data")
            return

        df = pd.DataFrame(self.sales_data)

        # Convert date
        df["date"] = pd.to_datetime(df["date"])

        # Daily sales aggregation
        daily_sales = df.groupby(["date", "product_id"])["quantity"].sum().reset_index()

        # Total sales per product
        total_sales = df.groupby("product_id")["quantity"].sum().reset_index()

        print("\n📊 Daily Sales:")
        print(daily_sales.tail())

        print("\n📊 Total Sales per Product:")
        print(total_sales)


# -----------------------------
# CSV INGESTION (OPTIONAL)
# -----------------------------
def load_sales_from_csv(self, file_path):
    df = pd.read_csv(file_path)

    for _, row in df.iterrows():
        self.sales_col.insert_one({
            "product_id": int(row["product_id"]),
            "quantity": int(row["quantity"]),
            "date": pd.to_datetime(row["date"])
        })
        print("CSV data loaded!")


# -----------------------------
# MAIN EXECUTION
# -----------------------------
inventory = InventoryMongoFullDemo()
inventory.seed_products()
# Price updates
inventory.update_prices()

        # Simulate 10 days of operations
for i in range(10):
    inventory.daily_job()

inventory.generate_reports()
