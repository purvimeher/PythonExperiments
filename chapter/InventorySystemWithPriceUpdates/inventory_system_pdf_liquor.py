import pandas as pd
import os
from datetime import datetime, timedelta
import plotly.express as px
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet

class InventorySystem:

    def __init__(self, data_dir="/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/liquor_data"):
        self.data_dir = data_dir
        self.products_file = os.path.join(data_dir, "products.csv")
        self.inventory_file = os.path.join(data_dir, "inventory.csv")
        self.sales_file = os.path.join(data_dir, "sales.csv")

        self.products = pd.read_csv(self.products_file, parse_dates=["last_price_update"])
        self.inventory = pd.read_csv(self.inventory_file, parse_dates=["date"])
        self.sales = pd.read_csv(self.sales_file, parse_dates=["date"])

    # -----------------------------
    # 📦 Add Stock
    # -----------------------------
    def add_stock(self, product_id, quantity, date):
        date = pd.to_datetime(date)

        last_stock = self.get_latest_stock(product_id)

        new_row = {
            "date": date,
            "product_id": product_id,
            "stock_in": quantity,
            "stock_out": 0,
            "closing_stock": last_stock + quantity
        }

        self.inventory = pd.concat([self.inventory, pd.DataFrame([new_row])])
        self.save_inventory()

    # -----------------------------
    # 💰 Record Sale
    # -----------------------------
    def record_sale(self, product_id, quantity, date):
        date = pd.to_datetime(date)

        last_stock = self.get_latest_stock(product_id)

        if quantity > last_stock:
            raise ValueError("Not enough stock!")

        # Update sales
        sale_row = {
            "date": date,
            "product_id": product_id,
            "quantity_sold": quantity
        }

        self.sales = pd.concat([self.sales, pd.DataFrame([sale_row])])

        # Update inventory
        inv_row = {
            "date": date,
            "product_id": product_id,
            "stock_in": 0,
            "stock_out": quantity,
            "closing_stock": last_stock - quantity
        }

        self.inventory = pd.concat([self.inventory, pd.DataFrame([inv_row])])

        self.save_all()

    # -----------------------------
    # 📊 Get Latest Stock
    # -----------------------------
    def get_latest_stock(self, product_id):
        df = self.inventory[self.inventory["product_id"] == product_id]

        if df.empty:
            return 0

        return df.sort_values("date").iloc[-1]["closing_stock"]

    # -----------------------------
    # 💲 Price Update (6 months)
    # -----------------------------
    def update_prices(self):
        today = pd.to_datetime(datetime.today())

        for idx, row in self.products.iterrows():
            last_update = row["last_price_update"]

            if (today - last_update).days >= 180:
                # Example: 5% increase or decrease randomly
                change = 1.05 if idx % 2 == 0 else 0.95
                self.products.at[idx, "price"] *= change
                self.products.at[idx, "last_price_update"] = today

        self.products.to_csv(self.products_file, index=False)

    # -----------------------------
    # 📅 Weekly Sales Report
    # -----------------------------
    def weekly_sales_report(self):
        self.sales["week"] = self.sales["date"].dt.to_period("W")
        weekly = self.sales.groupby(["week", "product_id"])["quantity_sold"].sum().reset_index()

        df = weekly.merge(self.products, on="product_id")

        fig = px.bar(df,
                     x="week",
                     y="quantity_sold",
                     color="product_name",
                     title="Weekly Sales")

        fig.show()

    # -----------------------------
    # 📆 Monthly Sales Report
    # -----------------------------
    def monthly_sales_report(self):
        self.sales["month"] = self.sales["date"].dt.to_period("M")
        monthly = self.sales.groupby(["month", "product_id"])["quantity_sold"].sum().reset_index()

        df = monthly.merge(self.products, on="product_id")

        fig = px.line(df,
                      x="month",
                      y="quantity_sold",
                      color="product_name",
                      title="Monthly Sales")

        fig.show()

    # -----------------------------
    # 📦 End-of-Month Stock Export
    # -----------------------------
    def export_month_end_stock(self):
        self.inventory["month"] = self.inventory["date"].dt.to_period("M")

        month_end = self.inventory.sort_values("date").groupby(
            ["product_id", "month"]
        ).tail(1)

        output_file = os.path.join(self.data_dir, "month_end_stock.csv")
        month_end.to_csv(output_file, index=False)

        print("Month-end stock saved:", output_file)

    # -----------------------------
    # 📊 Stock Visualization
    # -----------------------------
    def stock_visualization(self):
        df = self.inventory.merge(self.products, on="product_id")

        fig = px.line(df,
                      x="date",
                      y="closing_stock",
                      color="product_name",
                      title="Stock Levels Over Time")

        fig.show()

    # -----------------------------
    # 📄 PDF Report
    # -----------------------------
    def generate_pdf_report(self):
        doc = SimpleDocTemplate("reports/monthly_report.pdf")
        styles = getSampleStyleSheet()

        total_sales = self.sales["quantity_sold"].sum()
        total_products = self.products.shape[0]

        content = [
            Paragraph("Inventory Monthly Report", styles["Title"]),
            Paragraph(f"Total Sales: {total_sales}", styles["Normal"]),
            Paragraph(f"Total Products: {total_products}", styles["Normal"]),
        ]

        doc.build(content)

    # -----------------------------
    # 💾 Save Helpers
    # -----------------------------
    def save_inventory(self):
        self.inventory.to_csv(self.inventory_file, index=False)

    def save_all(self):
        self.inventory.to_csv(self.inventory_file, index=False)
        self.sales.to_csv(self.sales_file, index=False)

# -----------------------------------
# 🚀 Example Usage
# -----------------------------------

if __name__ == "__main__":
    ims = InventorySystem()

    # Add stock
    ims.add_stock(product_id=1, quantity=50, date="2025-02-01")

    # Record sale
    ims.record_sale(product_id=1, quantity=10, date="2025-02-02")

    # Update prices (6 months rule)
    ims.update_prices()

    # Reports
    ims.weekly_sales_report()
    ims.monthly_sales_report()

    # Export
    ims.export_month_end_stock()

    # Visuals
    ims.stock_visualization()

    # PDF
    ims.generate_pdf_report()