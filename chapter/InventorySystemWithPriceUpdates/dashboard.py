import pandas as pd
import plotly.express as px

class Dashboard:
    def __init__(self, product_file, sales_file):
        self.products = pd.read_csv(product_file)
        self.sales = pd.read_csv(sales_file, parse_dates=["date"])

    def sales_trend(self):
        df = self.sales.groupby(["date", "product_id"])["quantity"].sum().reset_index()

        fig = px.line(
            df,
            x="date",
            y="quantity",
            color="product_id",
            title="Daily Sales Trend"
        )
        fig.show()

    def inventory_levels(self):
        fig = px.bar(
            self.products,
            x="product_name",
            y="inventory",
            title="Inventory Levels"
        )
        fig.show()

    def revenue_analysis(self):
        merged = self.sales.merge(
            self.products[["product_id", "price"]],
            on="product_id"
        )
        merged["revenue"] = merged["quantity"] * merged["price"]

        df = merged.groupby("product_id")["revenue"].sum().reset_index()

        fig = px.pie(
            df,
            names="product_id",
            values="revenue",
            title="Revenue Distribution"
        )
        fig.show()


if __name__ == "__main__":
    dashboard = Dashboard(
        "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/data/products.csv",
        "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/InventorySystemWithPriceUpdates/data/sales.csv"
    )

    dashboard.sales_trend()
    dashboard.inventory_levels()
    dashboard.revenue_analysis()