import os
from datetime import datetime

import pandas as pd
from pymongo import MongoClient


class SalesReportsGenerator(object):
    def __init__(self, dailySalesCsvFileName='Daily_sales_test' , db="inventory_db"):
        self.stock_prices_csv_data = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/BnDeyOperations/data/Prices_List/Stock_prices.csv'
        self.daily_sales_csv_data = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/BnDeyOperations/data/Daily_Sales/' + dailySalesCsvFileName + '.csv'
        self.stock_prices_json_data = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/BnDeyOperations/output/Intermediate_jsons/stock_price.json'
        self.daily_sales_json_data = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/BnDeyOperations/output/Intermediate_jsons/' + dailySalesCsvFileName + '.json'
        self.mongodDbConnectionUrl = 'mongodb://localhost:27017/'
        self.output_dir = '/chapter/BnDeyOperations/output/Daily_Sales'
        self.output_dir_monthly = '/chapter/BnDeyOperations/output/Monthly_Sales'
        self.db=db

    def calculateAndPrintDailyTotalSalesToConsole(self):
        # Making Connection
        myclient = MongoClient(self.mongodDbConnectionUrl)

        # database
        db = myclient[self.db]

        daily_sales_col = db["daily_sales"]
        stock_prices_col = db["stock_prices"]

        # Load data into DataFrames
        daily_sales_df = pd.DataFrame(list(daily_sales_col.find()))
        stock_prices_df = pd.DataFrame(list(stock_prices_col.find()))

        # Clean and prepare data
        daily_sales_df["Date"] = pd.to_datetime(daily_sales_df["Date"], format="%d/%m/%Y")

        # Merge on Brand + Size_ML
        merged_df = pd.merge(
            daily_sales_df,
            stock_prices_df,
            on=["Brand", "Size_ML"],
            how="left"
        )

        # Calculate sales value
        merged_df["Sales_Value"] = (
                merged_df["Qty"] * merged_df["Maximum_Retail_Price_per_bottle"]
        )

        # -------------------------------
        # 1. Daily Sales (aggregated)
        # -------------------------------
        daily_sales_summary = merged_df.groupby("Date").agg({
            "Qty": "sum",
            "Sales_Value": "sum"
        }).reset_index()

        print("\n=== Daily Sales Summary ===")
        print(daily_sales_summary)

        # -------------------------------
        # 2. Total Sales
        # -------------------------------
        total_sales = merged_df["Sales_Value"].sum()
        total_qty = merged_df["Qty"].sum()

        print("\n=== Total Sales ===")
        print(f"Total Quantity Sold: {total_qty}")
        print(f"Total Quantity Sold: {total_qty}")
        print(f"Total Revenue: {total_sales}")

        # -------------------------------
        # 3. (Optional) Product-wise Daily Sales
        # -------------------------------
        product_daily_sales = merged_df.groupby(
            ["Date", "Brand", "Size_ML"]
        ).agg({
            "Qty": "sum",
            "Sales_Value": "sum"
        }).reset_index()

        print("\n=== Product-wise Daily Sales ===")
        print(product_daily_sales)

    def calculateDailyTotalSalesAndSaveIntoCsvFile(self):
        # Making Connection
        myclient = MongoClient(self.mongodDbConnectionUrl)

        # database
        db = myclient[self.db]

        daily_sales_col = db["daily_sales"]
        stock_prices_col = db["stock_prices"]

        # Load data
        daily_sales_df = pd.DataFrame(list(daily_sales_col.find()))
        stock_prices_df = pd.DataFrame(list(stock_prices_col.find()))

        # Prepare data
        daily_sales_df["Date"] = pd.to_datetime(daily_sales_df["Date"], format="%d/%m/%Y")

        # Merge
        merged_df = pd.merge(
            daily_sales_df,
            stock_prices_df,
            on=["Brand", "Size_ML"],
            how="left"
        )

        # Calculate Sales
        merged_df["Sales_Value"] = (
                merged_df["Qty"] * merged_df["Maximum_Retail_Price_per_bottle"]
        )

        # -------------------------------
        # Aggregations
        # -------------------------------

        # Daily Sales Summary
        daily_sales_summary = merged_df.groupby("Date").agg({
            "Qty": "sum",
            "Sales_Value": "sum"
        }).reset_index()

        # Product-wise Daily Sales
        product_daily_sales = merged_df.groupby(
            ["Date", "Brand", "Size_ML"]
        ).agg({
            "Qty": "sum",
            "Sales_Value": "sum"
        }).reset_index()

        # Total Sales
        total_sales_df = pd.DataFrame({
            "Total_Qty": [merged_df["Qty"].sum()],
            "Total_Revenue": [merged_df["Sales_Value"].sum()]
        })

        # -------------------------------
        # Save to CSV
        # -------------------------------

        # output_dir = "output_reports"
        os.makedirs(self.output_dir, exist_ok=True)

        daily_sales_summary.to_csv(f"{self.output_dir}/daily_sales_summary.csv", index=False)
        product_daily_sales.to_csv(f"{self.output_dir}/product_daily_sales.csv", index=False)
        total_sales_df.to_csv(f"{self.output_dir}/total_sales.csv", index=False)

        print("✅ CSV files saved successfully in 'output_reports/' folder")

    def generateMonthlySalesReportintoCsvAndDb(self):
        myclient = MongoClient(self.mongodDbConnectionUrl)
        db = myclient[self.db]
        daily_sales_col = db['daily_sales']
        monthly_sales_col = db['monthly_sales']

        # --- Fetch all daily sales ---
        daily_sales = list(daily_sales_col.find({}))

        if not daily_sales:
            print("No daily sales data found!")
            exit()

        # --- Convert to DataFrame ---
        df = pd.DataFrame(daily_sales)

        # Ensure Date column is datetime
        df['Date'] = pd.to_datetime(df['Date'], format="%d/%m/%Y")

        # Merge with stock_prices to get price information
        stock_prices_col = db['stock_prices']
        stock_prices = list(stock_prices_col.find({}))
        df_prices = pd.DataFrame(stock_prices)

        # Merge on Brand, Size_ML, Brand_Category
        df = pd.merge(df, df_prices[['Brand', 'Size_ML', 'Brand_Category', 'Maximum_Retail_Price_per_bottle']],
                      on=['Brand', 'Size_ML', 'Brand_Category'], how='left')

        # Calculate sales amount
        df['Sales_Amount'] = df['Qty'] * df['Maximum_Retail_Price_per_bottle']

        # Extract Year-Month for aggregation
        df['YearMonth'] = df['Date'].dt.strftime('%Y-%m')

        # --- Aggregate monthly sales ---
        monthly_agg = df.groupby(['YearMonth', 'Brand', 'Size_ML', 'Brand_Category']).agg({
            'Qty': 'sum',
            'Sales_Amount': 'sum'
        }).reset_index()

        for month, month_df in monthly_agg.groupby('YearMonth'):
            filename = os.path.join(self.output_dir_monthly, f"{month}.csv")
            month_df.to_csv(filename, index=False)
            print(f"Saved CSV for {month}: {filename}")

        # --- Insert aggregated data into MongoDB monthly_sales collection ---
        # Optionally, you can add timestamp or month field explicitly
        monthly_records = monthly_agg.to_dict('records')
        for record in monthly_records:
            record['Inserted_At'] = datetime.now()  # optional timestamp

        # Upsert to avoid duplicates per month per product
        for record in monthly_records:
            monthly_sales_col.update_one(
                {
                    'YearMonth': record['YearMonth'],
                    'Brand': record['Brand'],
                    'Size_ML': record['Size_ML'],
                    'Brand_Category': record['Brand_Category']
                },
                {'$set': record},
                upsert=True
            )

        print("Monthly sales aggregation completed and saved to MongoDB!")

    def calculateTotalMontlySalesAndSaveIntoDb(self):
        myclient = MongoClient(self.mongodDbConnectionUrl)
        db = myclient[self.db]
        daily_sales_col = db['daily_sales']
        stock_prices_col = db['stock_prices']
        total_monthly_sales_col = db['total_monthly_sales']

        # --- Fetch data from MongoDB ---
        daily_sales = list(daily_sales_col.find({}))
        stock_prices = list(stock_prices_col.find({}))

        if not daily_sales:
            print("No daily sales data found!")
            exit()

        # --- Convert to DataFrame ---
        df_sales = pd.DataFrame(daily_sales)
        df_prices = pd.DataFrame(stock_prices)

        # Ensure 'Date' is datetime
        df_sales['Date'] = pd.to_datetime(df_sales['Date'], format="%d/%m/%Y")

        # Merge daily sales with stock prices to get per bottle price
        df = pd.merge(df_sales,
                      df_prices[['Brand', 'Size_ML', 'Brand_Category', 'Maximum_Retail_Price_per_bottle']],
                      on=['Brand', 'Size_ML', 'Brand_Category'],
                      how='left')

        # Compute total sales amount
        df['Total_Sales_Amount'] = df['Qty'] * df['Maximum_Retail_Price_per_bottle']

        # Extract Year-Month for grouping
        df['YearMonth'] = df['Date'].dt.strftime('%Y-%m')

        # --- Aggregate total monthly sales ---
        monthly_total = df.groupby('YearMonth').agg({
            'Qty': 'sum',
            'Total_Sales_Amount': 'sum'
        }).reset_index()

        # # --- Save CSV files per month ---
        # output_dir = 'total_monthly_sales_csv'
        # os.makedirs(output_dir, exist_ok=True)

        for month, month_df in monthly_total.groupby('YearMonth'):
            filename = os.path.join(self.output_dir_monthly, f"Total_montly_{month}.csv")
            month_df.to_csv(filename, index=False)
            print(f"Saved CSV for {month}: {filename}")

        # --- Insert aggregated total monthly sales into MongoDB ---
        monthly_records = monthly_total.to_dict('records')
        for record in monthly_records:
            record['Inserted_At'] = datetime.now()  # optional timestamp
            # Upsert to avoid duplicates
            total_monthly_sales_col.update_one(
                {'YearMonth': record['YearMonth']},
                {'$set': record},
                upsert=True
            )

        print("Total monthly sales aggregation completed and saved to MongoDB!")
