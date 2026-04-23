from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import os

from BnDeyShopSolutions.BnDeyOperations.configs.ConfigLoader import ConfigLoader


class CurrentInventoryExporter:
    def __init__(
        self,
        mongo_uri="mongodb://localhost:27017/",
        db_name="your_database_name",
        collection_name="current_inventory",
        output_folder="output"
    ):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)

    def fetch_total_stock(self):
        """
        Aggregate total stock from current_inventory collection.
        Groups by Brand, Brand_Category, Size_ML, and Date.
        """
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "Brand": "$Brand",
                        "Brand_Category": "$Brand_Category",
                        "Size_ML": "$Size_ML",
                        "Date": "$Date"
                    },
                    "Total_Qty": {"$sum": "$Qty"},
                    "Last_Updated": {"$max": "$Last_Updated"},
                    "updated_at": {"$max": "$updated_at"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "Brand": "$_id.Brand",
                    "Brand_Category": "$_id.Brand_Category",
                    "Size_ML": "$_id.Size_ML",
                    "Date": "$_id.Date",
                    "Total_Qty": 1,
                    "Last_Updated": 1,
                    "updated_at": 1
                }
            },
            {
                "$sort": {
                    "Date": 1,
                    "Brand_Category": 1,
                    "Brand": 1,
                    "Size_ML": 1
                }
            }
        ]

        results = list(self.collection.aggregate(pipeline))
        return results

    def export_to_csv_and_html(self):
        data = self.fetch_total_stock()

        if not data:
            print("No data found in current_inventory collection.")
            return

        df = pd.DataFrame(data)

        # Convert datetime columns safely
        for col in ["Last_Updated", "updated_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = os.path.join(self.output_folder, f"total_stock_{timestamp}.csv")
        html_file = os.path.join(self.output_folder, f"total_stock_{timestamp}.html")

        # Save CSV
        df.to_csv(csv_file, index=False)

        # Save HTML
        html_content = f"""
        <html>
        <head>
            <title>Total Stock Report</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                }}
                h1 {{
                    color: #333;
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    margin-top: 20px;
                }}
                th, td {{
                    border: 1px solid #ccc;
                    padding: 8px;
                    text-align: left;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
                tr:nth-child(even) {{
                    background-color: #fafafa;
                }}
            </style>
        </head>
        <body>
            <h1>Total Stock Report</h1>
            <p>Generated at: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}</p>
            {df.to_html(index=False, border=0)}
        </body>
        </html>
        """

        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"CSV file created: {csv_file}")
        print(f"HTML file created: {html_file}")


if __name__ == "__main__":
    config = ConfigLoader.load_config()
    output_folder = config["paths"]["output_total_stock_fdr"]
    db_name = config["dailyinfomongodb"]["database"]

    exporter = CurrentInventoryExporter(
        mongo_uri="mongodb://localhost:27017/",
        db_name=db_name,   # change this
        collection_name="current_inventory",
        output_folder=output_folder
    )
    exporter.export_to_csv_and_html()