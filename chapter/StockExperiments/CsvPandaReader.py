from datetime import datetime
import os
import pandas as pd
import json
import plotly.express as px
from pygments.lexers import go
from pymongo import MongoClient, ASCENDING, UpdateOne


class CsvPandaReader:
    def __init__(self, dailySalesCsvFileName='Daily_sales_test'):
        self.stock_prices_csv_data = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/StockExperiments/data/Prices_List/Stock_prices.csv'
        self.daily_sales_csv_data = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/StockExperiments/data/Daily_Sales/' + dailySalesCsvFileName + '.csv'
        self.stock_prices_json_data = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/StockExperiments/output/Intermediate_jsons/stock_price.json'
        self.daily_sales_json_data = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/StockExperiments/output/Intermediate_jsons/' + dailySalesCsvFileName + '.json'
        self.mongodDbConnectionUrl = 'mongodb://localhost:27017/'
        self.output_dir = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/StockExperiments/output/Daily_Sales'
        self.output_dir_monthly = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/StockExperiments/output/Monthly_Sales'

    def readCsvIntoDataFrameIntoJson(self):
        stock_prices_data = pd.read_csv(
            self.stock_prices_csv_data)

        dailySales_data = pd.read_csv(
            self.daily_sales_csv_data)

        stock_prices_data.to_json(self.stock_prices_json_data, orient='records')

        dailySales_data.to_json(self.daily_sales_json_data, orient='records')

        # daily_sales_json_data = pd.read_json(self.daily_sales_json_data, orient='records')
        # print(daily_sales_json_data)

    def loadStockPricesJsonIntoMongoDb(self):

        # Making Connection
        myclient = MongoClient(self.mongodDbConnectionUrl)

        # database
        db = myclient["inventory_db"]

        # Created or Switched to collection
        # names: GeeksForGeeks
        Collection = db["stock_prices"]

        # Loading or Opening the json file
        with open(
                self.stock_prices_json_data) as file:
            file_data = json.load(file)

        # Step 1: Clean
        clean_data = []
        for doc in file_data:
            doc = {k.strip(): v for k, v in doc.items()}
            doc.pop("_id", None)
            clean_data.append(doc)

        # Step 2: Ensure uniqueness
        Collection.create_index(
            [("Brand", 1), ("Size_ML", 1)],
            unique=True
        )

        # Step 3: Upsert
        for doc in clean_data:
            Collection.update_one(
                {"Brand": doc["Brand"], "Size_ML": doc["Size_ML"]},
                {"$set": doc},
                upsert=True
            )

    # def loadDailySalesJsonIntoMongoDb(self):
    #
    #     # Making Connection
    #     myclient = MongoClient(self.mongodDbConnectionUrl)
    #
    #     # database
    #     db = myclient["inventory_db"]
    #
    #     # Created or Switched to collection
    #     # names: GeeksForGeeks
    #     Collection = db["daily_sales"]
    #
    #     # Loading or Opening the json file
    #     with open(
    #             self.daily_sales_json_data) as file:
    #         file_data = json.load(file)
    #
    #     # Step 1: Clean
    #     clean_data = []
    #     for doc in file_data:
    #         doc = {k.strip(): v for k, v in doc.items()}
    #         doc.pop("_id", None)
    #         clean_data.append(doc)
    #
    #     # Step 2: Ensure uniqueness
    #     Collection.create_index(
    #         [("Brand", 1), ("Size_ML", 1), ("Date", 1)],
    #         unique=True
    #     )
    #
    #     # Step 3: Upsert
    #     for doc in clean_data:
    #         Collection.update_one(
    #             {"Brand": doc["Brand"], "Size_ML": doc["Size_ML"]},
    #             {"$set": doc},
    #             upsert=True
    #         )

    def calculateDailyTotalSalesToConsole(self):
        # Making Connection
        myclient = MongoClient(self.mongodDbConnectionUrl)

        # database
        db = myclient["inventory_db"]

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

    def calculateDailyTotalSalesIntoCsv(self):
        # Making Connection
        myclient = MongoClient(self.mongodDbConnectionUrl)

        # database
        db = myclient["inventory_db"]

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

    def loadDailySalesJsonIntoMongoDBBulk(self):

        # Connect to MongoDB
        # Making Connection
        myclient = MongoClient(self.mongodDbConnectionUrl) # Update if needed
        db = myclient["inventory_db"]
        daily_sales = db['daily_sales']

        # Ensure unique index on Brand + Size_ML + Date
        daily_sales.create_index(
            [("Brand", ASCENDING), ("Size_ML", ASCENDING), ("Date", ASCENDING)],
            unique=True
        )

        # Sample multiple records
        # Loading or Opening the json file
        with open(
                self.daily_sales_json_data) as file:
            file_data = json.load(file)

        # Prepare bulk operations
        operations = [
            UpdateOne(
                {"Brand": r["Brand"], "Size_ML": r["Size_ML"], "Date": r["Date"]},  # Filter
                {"$set": r},  # Update or insert
                upsert=True
            )
            for r in file_data
        ]

        # Execute bulk write
        result = daily_sales.bulk_write(operations)

        print(f"Inserted: {result.upserted_count}, Modified: {result.modified_count}")

    def generateDailySalesReportintoCsv(self):
        myclient = MongoClient(self.mongodDbConnectionUrl)
        db = myclient["inventory_db"]

        daily_sales_col = db["daily_sales"]
        stock_prices_col = db["stock_prices"]

        # -------------------------
        # Load Data
        # -------------------------
        daily_sales_df = pd.DataFrame(list(daily_sales_col.find()))
        stock_prices_df = pd.DataFrame(list(stock_prices_col.find()))

        # Convert Date
        daily_sales_df["Date"] = pd.to_datetime(daily_sales_df["Date"], format="%d/%m/%Y")

        # -------------------------
        # Merge Sales with Prices
        # -------------------------
        merged_df = pd.merge(
            daily_sales_df,
            stock_prices_df,
            on=["Brand", "Size_ML"],
            how="left"
        )

        # Calculate Sales Value
        merged_df["Sales_Value"] = merged_df["Qty"] * merged_df["Maximum_Retail_Price_per_bottle"]

        # -------------------------
        # Create Output Folder
        # -------------------------
        output_dir = "daily_reports"
        os.makedirs(output_dir, exist_ok=True)

        # -------------------------
        # Generate Daily Reports
        # -------------------------
        for date, group in merged_df.groupby("Date"):
            date_str = date.strftime("%Y-%m-%d")

            # --- Total Sales for this date ---
            total_sales = pd.DataFrame([{
                "Date": date,
                "Total_Qty": group["Qty"].sum(),
                "Total_Revenue": group["Sales_Value"].sum()
            }])

            # --- Product-wise Sales for this date ---
            product_sales = group.groupby(["Brand", "Size_ML", "Date"]).agg({
                "Qty": "sum",
                "Sales_Value": "sum"
            }).reset_index()

            # --- Save CSVs ---
            total_sales.to_csv(f"{self.output_dir}/total_sales_{date_str}.csv", index=False)
            product_sales.to_csv(f"{self.output_dir}/product_sales_{date_str}.csv", index=False)

            print(f"✅ Saved daily report for {date_str}")

        print("🎉 All daily reports generated successfully!")

    def generateMonthlySalesReportintoCsvAndDb(self):
        myclient = MongoClient(self.mongodDbConnectionUrl)
        db = myclient["inventory_db"]
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

    def calculateTotalMontlySales(self):
        myclient = MongoClient(self.mongodDbConnectionUrl)
        db = myclient["inventory_db"]
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

    def generatePlotyForTotalMonthlySales(self):
        myclient = MongoClient(self.mongodDbConnectionUrl)
        db = myclient["inventory_db"]
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
        db = myclient["inventory_db"]
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

            fig_brand.write_html(f"/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/StockExperiments/output/Monthly_Sales/Monthly_htmls/{month}_brand_pie.html")
            fig_size.write_html(f"/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/StockExperiments/output/Monthly_Sales/Monthly_htmls/{month}_size_pie.html")
            fig_qty.write_html(f"/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/StockExperiments/output/Monthly_Sales/Monthly_htmls/{month}_qty_pie.html")

# dailySales_CsvfileName ='Daily_sales_07022026'
# csvpanda_reader = CsvPandaReader(dailySales_CsvfileName)
csvpanda_reader = CsvPandaReader()
csvpanda_reader.readCsvIntoDataFrameIntoJson()
csvpanda_reader.loadStockPricesJsonIntoMongoDb()
csvpanda_reader.loadDailySalesJsonIntoMongoDBBulk()
csvpanda_reader.calculateDailyTotalSalesToConsole()
csvpanda_reader.calculateDailyTotalSalesIntoCsv()
csvpanda_reader.generateMonthlySalesReportintoCsvAndDb()
csvpanda_reader.calculateTotalMontlySales()
# csvpanda_reader.generatePlotyForTotalMonthlySales()
csvpanda_reader.generateMonthlySalesPieCharts()