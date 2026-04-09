import pandas as pd
import numpy as np
from datetime import datetime
import plotly.express as px
from pymongo import MongoClient
from fpdf import FPDF

# -----------------------------
# CONFIG
# -----------------------------
SIZES = [100, 180, 375, 600, 700]

# -----------------------------
# INVENTORY CLASS
# -----------------------------
class InventorySystem:

    def __init__(self, product_file, sales_file, stock_file):
        self.products = pd.read_csv(product_file)
        self.sales = pd.read_csv(sales_file, parse_dates=['date'])
        self.stock_in = pd.read_csv(stock_file, parse_dates=['date'])

        self.inventory = pd.DataFrame(columns=[
            'product_id', 'size_ml', 'stock'
        ])

        self.price_history = {}

    # -----------------------------
    # INITIALIZE INVENTORY
    # -----------------------------
    def initialize_inventory(self):
        rows = []
        for _, p in self.products.iterrows():
            for size in SIZES:
                rows.append({
                    'product_id': p['product_id'],
                    'size_ml': size,
                    'stock': 0
                })
        self.inventory = pd.DataFrame(rows)

    # -----------------------------
    # APPLY STOCK IN
    # -----------------------------
    def update_stock(self):
        grouped = self.stock_in.groupby(['product_id', 'size_ml'])['quantity'].sum().reset_index()

        for _, row in grouped.iterrows():
            mask = (
                (self.inventory['product_id'] == row['product_id']) &
                (self.inventory['size_ml'] == row['size_ml'])
            )
            self.inventory.loc[mask, 'stock'] += row['quantity']

    # -----------------------------
    # APPLY SALES
    # -----------------------------
    def apply_sales(self):
        grouped = self.sales.groupby(['product_id', 'size_ml'])['quantity'].sum().reset_index()

        for _, row in grouped.iterrows():
            mask = (
                (self.inventory['product_id'] == row['product_id']) &
                (self.inventory['size_ml'] == row['size_ml'])
            )
            self.inventory.loc[mask, 'stock'] -= row['quantity']

    # -----------------------------
    # PRICE ADJUSTMENT (6 MONTHS)
    # -----------------------------
    def adjust_prices(self):
        self.products['date'] = pd.to_datetime('today')

        self.products['price'] = self.products['base_price'] * (
            1 + 0.1 * (self.products['date'].dt.month // 6)
        )

    # -----------------------------
    # WEEKLY SALES
    # -----------------------------
    def weekly_sales(self):
        self.sales['week'] = self.sales['date'].dt.isocalendar().week
        weekly = self.sales.groupby(['week', 'product_id'])['quantity'].sum().reset_index()
        return weekly

    # -----------------------------
    # MONTHLY SALES
    # -----------------------------
    def monthly_sales(self):
        self.sales['month'] = self.sales['date'].dt.to_period('M')
        monthly = self.sales.groupby(['month', 'product_id'])['quantity'].sum().reset_index()
        return monthly

    # -----------------------------
    # END OF MONTH STOCK
    # -----------------------------
    def end_of_month_stock(self):
        return self.inventory.copy()

    # -----------------------------
    # PLOTLY VISUALIZATION
    # -----------------------------
    def generate_plots(self):
        weekly = self.weekly_sales()
        monthly = self.monthly_sales()

        fig1 = px.bar(weekly, x='week', y='quantity', color='product_id',
                      title="Weekly Sales")
        fig1.show()

        fig2 = px.bar(monthly, x='month', y='quantity', color='product_id',
                      title="Monthly Sales")
        fig2.show()

    # -----------------------------
    # EXPORT CSV
    # -----------------------------
    def export_csv(self):
        self.inventory.to_csv("/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/output/end_month_inventory.csv", index=False)

    # -----------------------------
    # GENERATE PDF REPORT
    # -----------------------------
    def generate_pdf(self):
        pdf = FPDF()
        pdf.add_page()

        pdf.set_font("Arial", size=12)
        pdf.cell(200, 10, txt="Inventory Report", ln=True)

        for _, row in self.inventory.iterrows():
            line = f"{row['product_id']} | {row['size_ml']}ml | Stock: {row['stock']}"
            pdf.cell(200, 10, txt=line, ln=True)

        pdf.output("/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/output/inventory_report.pdf")

    # -----------------------------
    # STORE IN MONGODB
    # -----------------------------
    def store_mongodb(self):
        client = MongoClient("mongodb://localhost:27017/")
        db = client["inventory_db"]

        inventory_collection = db["inventory"]
        sales_collection = db["sales"]

        inventory_collection.delete_many({})
        sales_collection.delete_many({})

        inventory_collection.insert_many(self.inventory.to_dict("records"))
        sales_collection.insert_many(self.sales.to_dict("records"))

    # -----------------------------
    # RUN ALL
    # -----------------------------
    def run_pipeline(self):
        self.initialize_inventory()
        self.update_stock()
        self.apply_sales()
        self.adjust_prices()
        self.store_mongodb()

        print("Final Inventory:\n", self.inventory)

        self.generate_plots()
        self.export_csv()
        self.generate_pdf()
        self.store_mongodb()


# -----------------------------
# RUN SYSTEM
# -----------------------------
if __name__ == "__main__":
    system = InventorySystem(
        product_file="/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/liquor_data_two/products.csv",
        sales_file="/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/liquor_data_two/sales.csv",
        stock_file="/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/liquor_data_two/stock_in.csv"
    )

    system.run_pipeline()