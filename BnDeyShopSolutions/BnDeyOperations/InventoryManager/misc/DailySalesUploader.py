import pandas as pd
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime
from collections import defaultdict

class DailySalesUploader:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="inventory_db"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

        self.current_inventory = self.db["current_inventory"]
        self.incoming_stock = self.db["incoming_stock"]
        self.daily_sales = self.db["daily_sales"]
        self.inventory_update_log = self.db["inventory_update_log"]

        self._ensure_indexes()

    def _ensure_indexes(self):
        # Unique product in current inventory
        self.current_inventory.create_index(
            [("Brand", 1), ("Size_ML", 1), ("Brand_Category", 1)],
            unique=True
        )

        # Helpful indexes
        self.incoming_stock.create_index(
            [("Date", 1), ("Brand", 1), ("Size_ML", 1), ("Brand_Category", 1)]
        )
        self.daily_sales.create_index(
            [("Date", 1), ("Brand", 1), ("Size_ML", 1), ("Brand_Category", 1)]
        )

        # Idempotent daily processing log
        self.inventory_update_log.create_index("process_date", unique=True)
    # ---------------------------------------------------------
    # 1. CHECK AVAILABLE STOCK BEFORE INSERTING A DAILY SALE
    # ---------------------------------------------------------
    def get_available_stock_for_sale(self, brand, size_ml, brand_category, sale_date):
        """
        Available stock for a sale on a given date:
        current_inventory + today's incoming_stock - today's already booked sales
        """

        current_doc = self.current_inventory.find_one({
            "Brand": brand,
            "Size_ML": size_ml,
            "Brand_Category": brand_category
        })

        current_qty = int(current_doc["Qty"]) if current_doc else 0

        incoming_pipeline = [
            {
                "$match": {
                    "Date": sale_date,
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "Brand_Category": brand_category
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_incoming": {"$sum": "$Qty"}
                }
            }
        ]

        incoming_result = list(self.incoming_stock.aggregate(incoming_pipeline))
        incoming_qty = int(incoming_result[0]["total_incoming"]) if incoming_result else 0

        sales_pipeline = [
            {
                "$match": {
                    "Date": sale_date,
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "Brand_Category": brand_category
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_sales": {"$sum": "$Qty"}
                }
            }
        ]

        sales_result = list(self.daily_sales.aggregate(sales_pipeline))
        sold_qty = int(sales_result[0]["total_sales"]) if sales_result else 0

        available_qty = current_qty + incoming_qty - sold_qty
        return available_qty

    def create_daily_sales_from_csv(self, csv_file_path):
        """
        Read sales from CSV and insert only when enough stock is available.

        Expected CSV columns:
        Brand, Size_ML, Brand_Category, Date, Qty
        """

        df = pd.read_csv(csv_file_path)

        required_columns = ["Brand", "Size_ML", "Brand_Category", "Date", "Qty"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in CSV: {missing_columns}")

        inserted_count = 0
        failed_count = 0
        results = []

        for index, row in df.iterrows():
            try:
                brand = str(row["Brand"]).strip()
                size_ml = int(row["Size_ML"])
                brand_category = str(row["Brand_Category"]).strip()
                sale_date = str(row["Date"]).strip()
                qty = int(row["Qty"])

                if qty <= 0:
                    raise ValueError("Sale quantity must be greater than 0")

                available_qty = self.get_available_stock_for_sale(
                    brand=brand,
                    size_ml=size_ml,
                    brand_category=brand_category,
                    sale_date=sale_date
                )

                if available_qty < qty:
                    raise ValueError(
                        f"Insufficient stock. Available={available_qty}, Requested={qty}"
                    )

                sale_doc = {
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "Brand_Category": brand_category,
                    "Date": sale_date,
                    "Qty": qty,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }

                result = self.daily_sales.insert_one(sale_doc)

                inserted_count += 1
                results.append({
                    "row": index + 1,
                    "status": "success",
                    "sale_id": str(result.inserted_id),
                    "brand": brand,
                    "size_ml": size_ml,
                    "qty": qty,
                    "available_before_sale": available_qty,
                    "remaining_after_sale": available_qty - qty
                })

            except Exception as e:
                failed_count += 1
                results.append({
                    "row": index + 1,
                    "status": "failed",
                    "error": str(e),
                    "data": row.to_dict()
                })

        return {
            "status": "completed",
            "inserted_count": inserted_count,
            "failed_count": failed_count,
            "results": results
        }

    # ---------------------------------------------------------
    # 2. DAILY BATCH UPDATE OF CURRENT INVENTORY
    # ---------------------------------------------------------
    def _acquire_process_lock(self, process_date):
        try:
            self.inventory_update_log.insert_one({
                "process_date": process_date,
                "status": "in_progress",
                "started_at": datetime.utcnow(),
                "completed_at": None,
                "message": None
            })
            return True
        except DuplicateKeyError:
            return False

    def _mark_process_completed(self, process_date, message="Success"):
        self.inventory_update_log.update_one(
            {"process_date": process_date},
            {
                "$set": {
                    "status": "completed",
                    "completed_at": datetime.utcnow(),
                    "message": message
                }
            }
        )

    def _mark_process_failed(self, process_date, message):
        self.inventory_update_log.update_one(
            {"process_date": process_date},
            {
                "$set": {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "message": message
                }
            }
        )

    def update_current_inventory_daily(self, process_date):
        """
        Update current_inventory for one date only once:
        new_qty = old_qty + incoming_stock(date) - daily_sales(date)

        This is idempotent because of inventory_update_log.
        """

        locked = self._acquire_process_lock(process_date)
        if not locked:
            return {
                "status": "skipped",
                "message": f"Inventory already processed for {process_date}"
            }

        try:
            stock_changes = defaultdict(int)

            # Aggregate incoming stock for the date
            incoming_records = self.incoming_stock.aggregate([
                {"$match": {"Date": process_date}},
                {
                    "$group": {
                        "_id": {
                            "Brand": "$Brand",
                            "Size_ML": "$Size_ML",
                            "Brand_Category": "$Brand_Category"
                        },
                        "total_qty": {"$sum": "$Qty"}
                    }
                }
            ])

            for record in incoming_records:
                key = (
                    record["_id"]["Brand"],
                    int(record["_id"]["Size_ML"]),
                    record["_id"]["Brand_Category"]
                )
                stock_changes[key] += int(record["total_qty"])

            # Aggregate daily sales for the date
            sales_records = self.daily_sales.aggregate([
                {"$match": {"Date": process_date}},
                {
                    "$group": {
                        "_id": {
                            "Brand": "$Brand",
                            "Size_ML": "$Size_ML",
                            "Brand_Category": "$Brand_Category"
                        },
                        "total_qty": {"$sum": "$Qty"}
                    }
                }
            ])

            for record in sales_records:
                key = (
                    record["_id"]["Brand"],
                    int(record["_id"]["Size_ML"]),
                    record["_id"]["Brand_Category"]
                )
                stock_changes[key] -= int(record["total_qty"])

            # Apply net changes to current_inventory
            for key, qty_change in stock_changes.items():
                brand, size_ml, brand_category = key

                existing = self.current_inventory.find_one({
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "Brand_Category": brand_category
                })

                if existing:
                    old_qty = int(existing.get("Qty", 0))
                    new_qty = old_qty + qty_change

                    # Safety check: do not allow negative inventory
                    if new_qty < 0:
                        raise ValueError(
                            f"Negative inventory would occur for "
                            f"{brand} {size_ml}ML {brand_category}. "
                            f"Old={old_qty}, Change={qty_change}, New={new_qty}"
                        )

                    self.current_inventory.update_one(
                        {"_id": existing["_id"]},
                        {
                            "$set": {
                                "Qty": new_qty,
                                "updated_at": datetime.utcnow()
                            }
                        }
                    )
                else:
                    # Product not present in current_inventory
                    if qty_change < 0:
                        raise ValueError(
                            f"Sales exceed stock for product not found in current_inventory: "
                            f"{brand} {size_ml}ML {brand_category}"
                        )

                    self.current_inventory.insert_one({
                        "Brand": brand,
                        "Size_ML": size_ml,
                        "Brand_Category": brand_category,
                        "Qty": qty_change,
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    })

            self._mark_process_completed(process_date)

            return {
                "status": "success",
                "message": f"Current inventory updated successfully for {process_date}"
            }

        except Exception as e:
            self._mark_process_failed(process_date, str(e))
            raise