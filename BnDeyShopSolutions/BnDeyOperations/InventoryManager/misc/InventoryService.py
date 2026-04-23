import csv

import pandas as pd
from pymongo import MongoClient, ASCENDING, UpdateOne
import json
from datetime import datetime
from collections import defaultdict
from pymongo.errors import BulkWriteError

from BnDeyShopSolutions.BnDeyOperations.configs.ConfigLoader import ConfigLoader


class InventoryService:
    def __init__(self, db="inventory_db"):
        config = ConfigLoader.load_config()

        self.__INCOMING_STOCK_CSV_FILE = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyOperations/data/Monthly_Stock/Incoming_Stock.csv'
        self.__INITIAL_STOCK_IN_HAND_CSV_FILE = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/BnDeyShopSolutions/BnDeyOperations/data/Monthly_Stock/Stock_In_Hand.csv'
        self.mongodDbConnectionUrl = config["mongodb"]["uri"]
        self.db = config["mongodb"]["database"]


    def readCsvIntoDataFrameIntoJson(self):
        incoming_stock_data = pd.read_csv(
            self.__INCOMING_STOCK_CSV_FILE)

        monthly_stock_data = pd.read_csv(
            self.__INITIAL_STOCK_IN_HAND_CSV_FILE)

        incoming_stock_data.to_json(self.__INCOMING_STOCK_CSV_FILE, orient='records')

        monthly_stock_data.to_json(self.__INITIAL_STOCK_IN_HAND_CSV_FILE, orient='records')

        print(monthly_stock_data)


    def loadMonthlyStockIntoMongoDbBulk(self):

        # Connect to MongoDB
        # Making Connection
        myclient = MongoClient(self.mongodDbConnectionUrl)  # Update if needed
        db = myclient[self.db]
        monthly_stock = db['monthly_stock']

        # Ensure unique index on Brand + Size_ML + Date
        monthly_stock.create_index(
            [("Brand", ASCENDING), ("Size_ML", ASCENDING), ("Date", ASCENDING)],
            unique=True
        )

        # Sample multiple records
        # Loading or Opening the json file
        with open(
                self.__INITIAL_STOCK_IN_HAND_CSV_FILE) as file:
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
        result = monthly_stock.bulk_write(operations)

        print(f"Inserted: {result.upserted_count}, Modified: {result.modified_count}")

    def loadIncomingStockIntoMongoDbBulk(self):

        # Connect to MongoDB
        # Making Connection
        myclient = MongoClient(self.mongodDbConnectionUrl)  # Update if needed
        db = myclient[self.db]
        incoming_stock = db['incoming_stock']

        # Ensure unique index on Brand + Size_ML + Date
        incoming_stock.create_index(
            [("Brand", ASCENDING), ("Size_ML", ASCENDING), ("Date", ASCENDING)],
            unique=True
        )

        # Sample multiple records
        # Loading or Opening the json file
        with open(
                self.__INCOMING_STOCK_CSV_FILE) as file:
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
        result = incoming_stock.bulk_write(operations)

        print(f"Inserted: {result.upserted_count}, Modified: {result.modified_count}")

    def normalize_record(self,record):
        """
        Normalize fields and ensure Qty is integer.
        """
        return {
            "Date": record.get("Date"),
            "Brand": record.get("Brand").strip(),
            "Size_ML": int(record.get("Size_ML")),
            "Brand_Category": record.get("Brand_Category").strip(),
            "Qty": int(record.get("Qty", 0))
        }

    def aggregate_inventory(self):

        # MongoDB connection
        client = MongoClient("mongodb://localhost:27017/")
        db = client[self.db]

        monthly_collection = db["monthly_stock"]
        incoming_collection = db["incoming_stock"]
        current_inventory_collection = db["current_inventory"]

        inventory_map = defaultdict(int)

        # Read monthly stock
        for rec in monthly_collection.find():
            r = self.normalize_record(rec)

            key = (
                r["Date"],
                r["Brand"],
                r["Size_ML"],
                r["Brand_Category"]
            )

            inventory_map[key] += r["Qty"]

        # Read incoming stock
        for rec in incoming_collection.find():
            r = self.normalize_record(rec)

            key = (
                r["Date"],
                r["Brand"],
                r["Size_ML"],
                r["Brand_Category"]
            )

            inventory_map[key] += r["Qty"]

        operations = []

        for key, qty in inventory_map.items():
            date, brand, size, category = key

            filter_query = {
                "Date": date,
                "Brand": brand,
                "Size_ML": size,
                "Brand_Category": category
            }

            update_query = {
                "$set": {
                    "Qty": qty,
                    "Last_Updated": datetime.utcnow()
                }
            }

            operations.append(
                UpdateOne(
                    filter_query,
                    update_query,
                    upsert=True
                )
            )

        if operations:
            result = current_inventory_collection.bulk_write(operations)

            print("Inventory aggregation completed")
            print("Inserted:", result.upserted_count)
            print("Modified:", result.modified_count)

    def build_daily_inventory(self):

        # MongoDB connection
        client = MongoClient("mongodb://localhost:27017/")
        db = client[self.db]
        current_inventory_col = db["current_inventory"]
        daily_sales_col = db["daily_sales"]
        daily_inventory_col = db["daily_inventory"]
        stock_map = defaultdict(int)
        sales_map = defaultdict(int)

        # Load current inventory
        for rec in current_inventory_col.find():
            r = self.normalize_record(rec)
            key = (
                r["Date"],
                r["Brand"],
                r["Size_ML"],
                r["Brand_Category"]
            )
            stock_map[key] += r["Qty"]

        # Load daily sales
        for rec in daily_sales_col.find():
            r = self.normalize_record(rec)
            key = (
                r["Date"],
                r["Brand"],
                r["Size_ML"],
                r["Brand_Category"]
            )
            sales_map[key] += r["Qty"]

        all_keys = set(stock_map.keys()) | set(sales_map.keys())
        operations = []

        for key in all_keys:
            date, brand, size_ml, category = key
            opening_stock = stock_map.get(key, 0)
            sold_qty = sales_map.get(key, 0)
            closing_stock = opening_stock - sold_qty

            # optional safety: prevent negative stock
            if closing_stock < 0:
                closing_stock = 0

            filter_query = {
                "Date": date,
                "Brand": brand,
                "Size_ML": size_ml,
                "Brand_Category": category
            }

            update_query = {
                "$set": {
                    "Opening_Stock": opening_stock,
                    "Sold_Qty": sold_qty,
                    "Closing_Stock": closing_stock,
                    "Last_Updated": datetime.utcnow()
                }
            }

            operations.append(
                UpdateOne(filter_query, update_query, upsert=True)
            )

        if operations:
            result = daily_inventory_col.bulk_write(operations)
            print("Daily inventory calculation completed")
            print("Inserted:", result.upserted_count)
            print("Modified:", result.modified_count)
        else:
            print("No records found to process")

    def export_daily_inventory_to_csv(self):
        # MongoDB connection
        client = MongoClient("mongodb://localhost:27017/")
        db = client[self.db]
        daily_inventory_col = db["daily_inventory"]

        cursor = daily_inventory_col.find().sort("Date", 1)

        filename = f"/chapter/BnDeyOperations/output/end_of_day_inventory/daily_inventory_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            # CSV Header
            writer.writerow([
                "Date",
                "Brand",
                "Size_ML",
                "Brand_Category",
                "Monthly_Stock_Qty",
                "Incoming_Stock_Qty",
                "Opening_Stock",
                "Sold_Qty",
                "Closing_Stock",
                "Last_Updated"
            ])

            for record in cursor:
                writer.writerow([
                    record.get("Date"),
                    record.get("Brand"),
                    record.get("Size_ML"),
                    record.get("Brand_Category"),
                    record.get("Monthly_Stock_Qty", 0),
                    record.get("Incoming_Stock_Qty", 0),
                    record.get("Opening_Stock", 0),
                    record.get("Sold_Qty", 0),
                    record.get("Closing_Stock", 0),
                    record.get("Last_Updated")
                ])

        print("CSV Export Completed")
        print("File:", filename)

    def parse_date(self, date_str):
        return datetime.strptime(date_str, "%d/%m/%Y")

    def update_current_inventory_from_daily_inventory(self,process_date=None):
        client = MongoClient("mongodb://localhost:27017/")
        db = client[self.db]

        daily_inventory_col = db["daily_inventory"]
        current_inventory_col = db["current_inventory"]
        # If date not passed, pick latest date properly
        if process_date is None:
            all_dates = daily_inventory_col.distinct("Date")
            if not all_dates:
                print("No records found in daily_inventory")
                return

            process_date = max(all_dates, key=self.parse_date)

        print(f"Updating current_inventory for date: {process_date}")

        records = daily_inventory_col.find({"Date": process_date})

        operations = []

        for rec in records:
            brand = str(rec.get("Brand", "")).strip()
            size_ml = int(rec.get("Size_ML", 0))
            category = str(rec.get("Brand_Category", "")).strip()
            closing_stock = int(rec.get("Closing_Stock", 0))

            filter_query = {
                "Brand": brand,
                "Size_ML": size_ml,
                "Brand_Category": category
            }

            update_doc = {
                "$set": {
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "Brand_Category": category,
                    "Qty": closing_stock,
                    "Stock_Date": process_date,
                    "Last_Updated": datetime.utcnow()
                }
            }

            operations.append(UpdateOne(filter_query, update_doc, upsert=True))

        if not operations:
            print("No matching records found for update")
            return

        result = current_inventory_col.bulk_write(operations)

        print("current_inventory updated successfully")
        print("Inserted:", result.upserted_count)
        print("Modified:", result.modified_count)


    def aggregate_same_products(self):
        client = MongoClient("mongodb://localhost:27017/")
        db = client[self.db]

        current_inventory_col = db["current_inventory"]
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "Brand": "$Brand",
                        "Size_ML": "$Size_ML",
                        "Brand_Category": "$Brand_Category"
                    },
                    "Qty": {
                        "$sum": {
                            "$toInt": "$Qty"
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "Brand": "$_id.Brand",
                    "Size_ML": "$_id.Size_ML",
                    "Brand_Category": "$_id.Brand_Category",
                    "Qty": 1
                }
            }
        ]

        aggregated = list(current_inventory_col.aggregate(pipeline))

        if not aggregated:
            print("No records found")
            return

        # Replace collection safely
        current_inventory_col.delete_many({})
        current_inventory_col.insert_many(aggregated)

        print("Duplicate products merged and quantities summed")

    def create_indexes(self):
        """
        Unique index prevents duplicate records for same product/date/category.
        """
        self.collection.create_index(
            [
                ("Brand", 1),
                ("Size_ML", 1),
                ("Date", 1),
                ("Brand_Category", 1),
            ],
            unique=True,
            name="uniq_incoming_stock_record"
        )

    def loadIncomingStockCsvIntoMongoDb(self, add_qty_on_duplicate: bool = True):

        # Connect to MongoDB
        # Making Connection
        myclient = MongoClient(self.mongodDbConnectionUrl)  # Update if needed
        db = myclient[self.db]
        incoming_stock = db['incoming_stock']
        """
        Reads CSV and inserts/updates incoming_stock collection.

        add_qty_on_duplicate=True:
            If duplicate exists, Qty will be added to existing Qty.

        add_qty_on_duplicate=False:
            If duplicate exists, Qty will be overwritten.
        """
        df = pd.read_csv(self.__INCOMING_STOCK_CSV_FILE)

        # Clean column names
        df.columns = df.columns.str.strip()

        required_cols = ["Brand", "Size_ML", "Date", "Brand_Category", "Qty"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Clean values
        df["Brand"] = df["Brand"].astype(str).str.strip()
        df["Brand_Category"] = df["Brand_Category"].astype(str).str.strip()
        df["Date"] = df["Date"].astype(str).str.strip()
        df["Size_ML"] = pd.to_numeric(df["Size_ML"], errors="coerce").fillna(0).astype(int)
        df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0).astype(int)

        # Remove bad rows
        df = df[
            (df["Brand"] != "") &
            (df["Brand_Category"] != "") &
            (df["Date"] != "") &
            (df["Size_ML"] > 0) &
            (df["Qty"] > 0)
        ].copy()

        if df.empty:
            print("No valid rows found in CSV.")
            return

        # Optional: combine duplicates already inside the CSV before writing to MongoDB
        df = (
            df.groupby(["Brand", "Size_ML", "Date", "Brand_Category"], as_index=False)["Qty"]
            .sum()
        )

        operations = []

        for _, row in df.iterrows():
            filter_doc = {
                "Brand": row["Brand"],
                "Size_ML": int(row["Size_ML"]),
                "Date": row["Date"],
                "Brand_Category": row["Brand_Category"],
            }

            if add_qty_on_duplicate:
                update_doc = {
                    "$inc": {"Qty": int(row["Qty"])},
                    "$setOnInsert": {
                        "created_at": datetime.utcnow()
                    },
                    "$set": {
                        "updated_at": datetime.utcnow()
                    }
                }
            else:
                update_doc = {
                    "$set": {
                        "Qty": int(row["Qty"]),
                        "updated_at": datetime.utcnow()
                    },
                    "$setOnInsert": {
                        "created_at": datetime.utcnow()
                    }
                }

            operations.append(
                UpdateOne(
                    filter_doc,
                    update_doc,
                    upsert=True
                )
            )

        if not operations:
            print("No operations to perform.")
            return

        try:
            result = incoming_stock.bulk_write(operations, ordered=False)
            print("CSV processed successfully.")
            print(f"Inserted: {result.upserted_count}")
            print(f"Modified: {result.modified_count}")
        except BulkWriteError as e:
            print("Bulk write error occurred.")
            print(e.details)

    def loadMonthlyStockCsvIntoMongoDb(self, add_qty_on_duplicate: bool = True):

        # Connect to MongoDB
        # Making Connection
        myclient = MongoClient(self.mongodDbConnectionUrl)  # Update if needed
        db = myclient[self.db]
        monthly_stock = db['monthly_stock']
        """
        Reads CSV and inserts/updates incoming_stock collection.

        add_qty_on_duplicate=True:
            If duplicate exists, Qty will be added to existing Qty.

        add_qty_on_duplicate=False:
            If duplicate exists, Qty will be overwritten.
        """
        df = pd.read_csv(self.__INITIAL_STOCK_IN_HAND_CSV_FILE)

        # Clean column names
        df.columns = df.columns.str.strip()

        required_cols = ["Brand", "Size_ML", "Date", "Brand_Category", "Qty"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Clean values
        df["Brand"] = df["Brand"].astype(str).str.strip()
        df["Brand_Category"] = df["Brand_Category"].astype(str).str.strip()
        df["Date"] = df["Date"].astype(str).str.strip()
        df["Size_ML"] = pd.to_numeric(df["Size_ML"], errors="coerce").fillna(0).astype(int)
        df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0).astype(int)

        # Remove bad rows
        df = df[
            (df["Brand"] != "") &
            (df["Brand_Category"] != "") &
            (df["Date"] != "") &
            (df["Size_ML"] > 0) &
            (df["Qty"] > 0)
        ].copy()

        if df.empty:
            print("No valid rows found in CSV.")
            return

        # Optional: combine duplicates already inside the CSV before writing to MongoDB
        df = (
            df.groupby(["Brand", "Size_ML", "Date", "Brand_Category"], as_index=False)["Qty"]
            .sum()
        )

        operations = []

        for _, row in df.iterrows():
            filter_doc = {
                "Brand": row["Brand"],
                "Size_ML": int(row["Size_ML"]),
                "Date": row["Date"],
                "Brand_Category": row["Brand_Category"],
            }

            if add_qty_on_duplicate:
                update_doc = {
                    "$inc": {"Qty": int(row["Qty"])},
                    "$setOnInsert": {
                        "created_at": datetime.utcnow()
                    },
                    "$set": {
                        "updated_at": datetime.utcnow()
                    }
                }
            else:
                update_doc = {
                    "$set": {
                        "Qty": int(row["Qty"]),
                        "updated_at": datetime.utcnow()
                    },
                    "$setOnInsert": {
                        "created_at": datetime.utcnow()
                    }
                }

            operations.append(
                UpdateOne(
                    filter_doc,
                    update_doc,
                    upsert=True
                )
            )

        if not operations:
            print("No operations to perform.")
            return

        try:
            result = monthly_stock.bulk_write(operations, ordered=False)
            print("CSV processed successfully.")
            print(f"Inserted: {result.upserted_count}")
            print(f"Modified: {result.modified_count}")
        except BulkWriteError as e:
            print("Bulk write error occurred.")
            print(e.details)


    def update_current_inventory_daily(self, process_date=datetime.now().strftime("%d/%m/%Y")):

        client = MongoClient(self.mongodDbConnectionUrl)
        db = client[self.db]


        current_inventory = db["current_inventory"]
        daily_sales = db["daily_sales"]
        incoming_stock = db['incoming_stock']
        """
        Updates current_inventory using incoming_stock and daily_sales
        """

        print(f"Processing inventory for date: {process_date}")

        stock_changes = defaultdict(int)

        # -----------------------------
        # 1. ADD incoming stock
        # -----------------------------
        incoming_records = incoming_stock.aggregate([
            {
                "$match": {
                    "Date": process_date
                }
            },
            {
                "$group": {
                    "_id": {
                        "Brand": "$Brand",
                        "Size_ML": "$Size_ML",
                        "Brand_Category": "$Brand_Category"
                    },
                    "total_qty": {
                        "$sum": "$Qty"
                    }
                }
            }
        ])

        for record in incoming_records:
            key = (
                record["_id"]["Brand"],
                record["_id"]["Size_ML"],
                record["_id"]["Brand_Category"]
            )

            stock_changes[key] += record["total_qty"]

        # -----------------------------
        # 2. SUBTRACT daily sales
        # -----------------------------
        sales_records = daily_sales.aggregate([
            {
                "$match": {
                    "Date": process_date
                }
            },
            {
                "$group": {
                    "_id": {
                        "Brand": "$Brand",
                        "Size_ML": "$Size_ML",
                        "Brand_Category": "$Brand_Category"
                    },
                    "total_qty": {
                        "$sum": "$Qty"
                    }
                }
            }
        ])

        for record in sales_records:
            key = (
                record["_id"]["Brand"],
                record["_id"]["Size_ML"],
                record["_id"]["Brand_Category"]
            )

            stock_changes[key] -= record["total_qty"]

        # -----------------------------
        # 3. APPLY updates to inventory
        # -----------------------------
        for key, qty_change in stock_changes.items():

            brand, size_ml, category = key

            existing = current_inventory.find_one({
                "Brand": brand,
                "Size_ML": size_ml,
                "Brand_Category": category
            })

            if existing:

                new_qty = existing["Qty"] + qty_change

                if new_qty < 0:
                    new_qty = 0

                current_inventory.update_one(
                    {
                        "_id": existing["_id"]
                    },
                    {
                        "$set": {
                            "Qty": new_qty,
                            "updated_at": datetime.utcnow()
                        }
                    }
                )

            else:
                # Insert new product if not exists
                if qty_change > 0:

                    current_inventory.insert_one(
                        {
                            "Brand": brand,
                            "Size_ML": size_ml,
                            "Brand_Category": category,
                            "Qty": qty_change,
                            "created_at": datetime.utcnow(),
                            "updated_at": datetime.utcnow()
                        }
                    )

        print("Current inventory updated successfully.")




# MAIN EXECUTIONS STARTS FROM HERE
# THIS SHOULD BE ONLY RUN ONCE TO INSITIALISE STOCK LEVELS
inventory_service = InventoryService()
inventory_service.loadMonthlyStockCsvIntoMongoDb(False)

# THIS ONE SHOULD RUN AFTER DAILY SALES HAS BEEN RECORDED
# inventory_service = InventoryService()
# inventory_service.loadIncomingStockCsvIntoMongoDb(False)
inventory_service.aggregate_inventory()
inventory_service.update_current_inventory_daily()






# BELOW I THINK WE CAN REMOVE
# inventory_service.loadMonthlyStockIntoMongoDbBulk()

# inventory_service.build_daily_inventory()
# inventory_service.export_daily_inventory_to_csv()
# Option 1 — auto latest date
# inventory_service.update_current_inventory_from_daily_inventory()
# inventory_service.aggregate_same_products()

# Option 2 — specific date
# update_current_inventory_from_daily_inventory("09/04/2026")


