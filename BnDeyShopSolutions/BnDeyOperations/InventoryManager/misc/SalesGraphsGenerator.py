from pathlib import Path
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from BnDeyShopSolutions.BnDeyOperations.configs.ConfigLoader import ConfigLoader


class SalesGraphsGenerator:
    def __init__(self,db="bdndey_db"):
        self.db = db
        config = ConfigLoader.load_config()
        self.mongodDbConnectionUrl = config["dailyinfomongodb"]["uri"]


    def generatePlotyForTotalMonthlySales(self):
        myclient = MongoClient(self.mongodDbConnectionUrl)
        db = myclient[self.db]
        collection = db["total_monthly_sales"]

        # Fetch data
        data = list(collection.find())

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Clean fields
        df["YearMonth"] = pd.to_datetime(df["YearMonth"])
        df["Total_Sales_Amount"] = df["Total_Sales_Amount"].astype(float)
        df["Qty"] = df["Qty"].astype(int)

        # Sort by date
        df = df.sort_values("YearMonth")

        fig = px.line(
            df,
            x="YearMonth",
            y="Total_Sales_Amount",
            markers=True,
            title="Monthly Sales Performance",
        )

        fig.update_layout(
            xaxis_title="Month",
            yaxis_title="Total Sales Amount",
            template="plotly_white"
        )

        fig.show()

    def generateMonthlySalesPieCharts(self):
        myclient = MongoClient(self.mongodDbConnectionUrl)
        db = myclient[self.db]
        collection = db["monthly_sales"]

        data = list(collection.find())
        df = pd.DataFrame(data)

        # Clean fields
        df["YearMonth"] = pd.to_datetime(df["YearMonth"])
        df["Qty"] = df["Qty"].astype(int)
        df["Sales_Amount"] = df["Sales_Amount"].astype(float)
        df["Size_ML"] = df["Size_ML"].astype(str)

        # Sort
        df = df.sort_values("YearMonth")

        import plotly.express as px

        # Get unique months
        months = df["YearMonth"].dt.strftime("%Y-%m").unique()

        for month in months:
            month_df = df[df["YearMonth"].dt.strftime("%Y-%m") == month]

            # --- Brand Pie ---
            brand_df = month_df.groupby("Brand")["Sales_Amount"].sum().reset_index()
            fig_brand = px.pie(
                brand_df,
                names="Brand",
                values="Sales_Amount",
                title=f"{month} - Sales by Brand"
            )
            fig_brand.update_traces(textinfo='percent+label')
            # fig_brand.show()

            # --- Size Pie ---
            size_df = month_df.groupby("Size_ML")["Sales_Amount"].sum().reset_index()
            fig_size = px.pie(
                size_df,
                names="Size_ML",
                values="Sales_Amount",
                title=f"{month} - Sales by Size (ML)"
            )
            fig_size.update_traces(textinfo='percent+label')
            # fig_size.show()

            # --- Quantity Pie ---
            qty_df = month_df.groupby("Brand")["Qty"].sum().reset_index()
            fig_qty = px.pie(
                qty_df,
                names="Brand",
                values="Qty",
                title=f"{month} - Quantity by Brand"
            )
            fig_qty.update_traces(textinfo='percent+label')
            # fig_qty.show()

            fig_brand.write_html(
                f"/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/BnDeyOperations/output/Monthly_Sales/Monthly_htmls/{month}_brand_pie.html")
            fig_size.write_html(
                f"/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/BnDeyOperations/output/Monthly_Sales/Monthly_htmls/{month}_size_pie.html")
            fig_qty.write_html(
                f"/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/BnDeyOperations/output/Monthly_Sales/Monthly_htmls/{month}_qty_pie.html")

    def generateMonthlySalesPieReportHtml(self):
        myclient = MongoClient(self.mongodDbConnectionUrl)
        db = myclient[self.db]
        collection = db["monthly_sales"]

        data = list(collection.find())
        df = pd.DataFrame(data)

        # Clean fields
        df["YearMonth"] = pd.to_datetime(df["YearMonth"])
        df["Qty"] = df["Qty"].astype(int)
        df["Sales_Amount"] = df["Sales_Amount"].astype(float)
        df["Size_ML"] = df["Size_ML"].astype(str)

        df = df.sort_values("YearMonth")

        # -----------------------------
        # Prepare Months
        # -----------------------------
        months = df["YearMonth"].dt.strftime("%Y-%m").unique()

        # Create subplot grid (3 charts per month)
        rows = len(months)
        # Correctly generate subplot_titles: 3 per month
        subplot_titles = [title for m in months for title in (f"{m} - Brand", f"{m} - Size", f"{m} - Qty")]

        fig = make_subplots(
            rows=rows,
            cols=3,
            specs=[[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}] for _ in range(rows)],
            subplot_titles=subplot_titles
        )

        # -----------------------------
        # Build Charts
        # -----------------------------
        for i, month in enumerate(months, start=1):
            month_df = df[df["YearMonth"].dt.strftime("%Y-%m") == month]

            # Brand Pie
            brand_df = month_df.groupby("Brand")["Sales_Amount"].sum().reset_index()
            fig.add_trace(
                go.Pie(labels=brand_df["Brand"], values=brand_df["Sales_Amount"], name=f"{month} Brand"),
                row=i, col=1
            )

            # Size Pie
            size_df = month_df.groupby("Size_ML")["Sales_Amount"].sum().reset_index()
            fig.add_trace(
                go.Pie(labels=size_df["Size_ML"], values=size_df["Sales_Amount"], name=f"{month} Size"),
                row=i, col=2
            )

            # Qty Pie
            qty_df = month_df.groupby("Brand")["Qty"].sum().reset_index()
            fig.add_trace(
                go.Pie(labels=qty_df["Brand"], values=qty_df["Qty"], name=f"{month} Qty"),
                row=i, col=3
            )

        # -----------------------------
        # Layout
        # -----------------------------
        fig.update_layout(
            height=400 * rows,
            title_text="Monthly Sales Distribution (Brand, Size, Quantity)",
            showlegend=False
        )

        # -----------------------------
        # Save to ONE HTML file
        # -----------------------------
        output_path = Path(
            "/chapter/BnDeyOperations/output/Monthly_Sales/Monthly_htmls/monthly_pie_dashboard.html")
        fig.write_html(output_path, include_plotlyjs='cdn', auto_open=True)

        print(f"Saved to {output_path}")

    def generateMonthlySalesBarChartReports(self):
        myclient = MongoClient(self.mongodDbConnectionUrl)
        db = myclient[self.db]
        collection = db["monthly_sales"]

        data = list(collection.find())
        df = pd.DataFrame(data)

        # Clean fields
        df["YearMonth"] = pd.to_datetime(df["YearMonth"])
        df["Qty"] = df["Qty"].astype(int)
        df["Sales_Amount"] = df["Sales_Amount"].astype(float)
        df["Size_ML"] = df["Size_ML"].astype(str)

        df = df.sort_values("YearMonth")

        # -----------------------------
        # Prepare unique months
        # -----------------------------
        months = df["YearMonth"].dt.strftime("%Y-%m").unique()
        rows = len(months)

        # -----------------------------
        # Create Subplots (3 charts per month)
        # -----------------------------
        subplot_titles = [
            title for m in months for title in
            (f"{m} - Sales by Brand", f"{m} - Sales by Size", f"{m} - Quantity by Brand")
        ]

        fig = make_subplots(
            rows=rows,
            cols=3,
            specs=[[{"type": "xy"}, {"type": "xy"}, {"type": "xy"}] for _ in range(rows)],
            subplot_titles=subplot_titles
        )

        # -----------------------------
        # Add Bar Charts
        # -----------------------------
        for i, month in enumerate(months, start=1):
            month_df = df[df["YearMonth"].dt.strftime("%Y-%m") == month]

            # --- Sales by Brand ---
            brand_df = month_df.groupby("Brand")["Sales_Amount"].sum().reset_index()
            fig.add_trace(
                go.Bar(x=brand_df["Brand"], y=brand_df["Sales_Amount"], name=f"{month} Brand"),
                row=i, col=1
            )

            # --- Sales by Size ---
            size_df = month_df.groupby("Size_ML")["Sales_Amount"].sum().reset_index()
            fig.add_trace(
                go.Bar(x=size_df["Size_ML"], y=size_df["Sales_Amount"], name=f"{month} Size"),
                row=i, col=2
            )

            # --- Quantity by Brand ---
            qty_df = month_df.groupby("Brand")["Qty"].sum().reset_index()
            fig.add_trace(
                go.Bar(x=qty_df["Brand"], y=qty_df["Qty"], name=f"{month} Qty"),
                row=i, col=3
            )

        # -----------------------------
        # Layout Settings
        # -----------------------------
        fig.update_layout(
            height=400 * rows,  # adjust height for all rows
            title_text="Monthly Sales & Quantity Bar Charts",
            showlegend=False
        )

        # -----------------------------
        # Save to HTML
        # -----------------------------
        output_file = Path(
            "/chapter/BnDeyOperations/output/Monthly_Sales/Monthly_htmls/monthly_bar_dashboard.html")
        fig.write_html(output_file, include_plotlyjs='cdn', auto_open=True)
        fig.write_image(
            "/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/BnDeyOperations/output/Monthly_Sales/Monthly_htmls/monthly_sales.pdf")

        print(f"Saved dashboard to {output_file}")
