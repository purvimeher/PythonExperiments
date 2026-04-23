import os
import hashlib
from datetime import datetime
from typing import Dict, Tuple

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError


class UploadIncomingStockInventoryService:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

        self.current_inventory = self.db["current_inventory"]

        # Optional: used only to prevent same CSV upload from being processed twice.
        # This does NOT modify incoming_stock.
        self.upload_audit = self.db["inventory_upload_audit"]

        self._ensure_indexes()

    def _ensure_indexes(self):
        # Unique product/date key in current_inventory
        self.current_inventory.create_index(
            [
                ("Brand_Category", 1),
                ("Brand", 1),
                ("Size_ML", 1),
                ("Date", 1),
            ],
            unique=True,
            name="uniq_current_inventory_product_date",
        )

        # Optional: prevent same file from being processed twice
        self.upload_audit.create_index(
            [("file_hash", 1)],
            unique=True,
            name="uniq_file_hash",
        )

    @staticmethod
    def _normalize_text(value) -> str:
        if pd.isna(value):
            return ""
        return str(value).strip()

    @staticmethod
    def _normalize_int(value) -> int:
        if pd.isna(value):
            return 0
        return int(float(value))

    @staticmethod
    def _normalize_date(value) -> str:
        """
        Converts date into DD/MM/YYYY string.
        """
        if pd.isna(value):
            raise ValueError("Date cannot be empty")

        # dayfirst=True because your sample is 09/04/2026
        dt = pd.to_datetime(value, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            raise ValueError(f"Invalid date value: {value}")

        return dt.strftime("%d/%m/%Y")

    @staticmethod
    def _file_hash(file_path: str) -> str:
        sha = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha.update(chunk)
        return sha.hexdigest()

    def _check_and_register_file(self, file_path: str) -> str:
        """
        Optional idempotency protection:
        register CSV hash so same upload is not applied twice.
        """
        file_hash = self._file_hash(file_path)

        existing = self.upload_audit.find_one({"file_hash": file_hash})
        if existing:
            raise ValueError(
                f"This CSV upload was already processed earlier: {os.path.basename(file_path)}"
            )

        self.upload_audit.insert_one(
            {
                "file_name": os.path.basename(file_path),
                "file_hash": file_hash,
                "processed_at": datetime.utcnow(),
            }
        )
        return file_hash

    def update_current_inventory_from_csv(self, csv_path: str, prevent_duplicate_upload: bool = True) -> Dict:
        """
        Reads uploaded CSV and updates ONLY current_inventory.

        Expected CSV columns:
        - Brand_Category
        - Brand
        - Size_ML
        - Date
        - Qty

        Rules:
        - incoming_stock collection is not modified
        - duplicate rows inside CSV are aggregated
        - existing current_inventory Qty is incremented
        - missing current_inventory rows are inserted via upsert
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Optional protection against applying same file twice
        file_hash = None
        if prevent_duplicate_upload:
            file_hash = self._check_and_register_file(csv_path)

        df = pd.read_csv(csv_path)

        required_columns = ["Brand_Category", "Brand", "Size_ML", "Date", "Qty"]
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns in CSV: {missing}")

        # Normalize data
        df["Brand_Category"] = df["Brand_Category"].apply(self._normalize_text)
        df["Brand"] = df["Brand"].apply(self._normalize_text)
        df["Size_ML"] = df["Size_ML"].apply(self._normalize_int)
        df["Date"] = df["Date"].apply(self._normalize_date)
        df["Qty"] = df["Qty"].apply(self._normalize_int)

        # Remove invalid rows
        df = df[
            (df["Brand_Category"] != "") &
            (df["Brand"] != "") &
            (df["Size_ML"] > 0) &
            (df["Qty"] > 0)
            ].copy()

        if df.empty:
            return {
                "status": "success",
                "message": "No valid rows found in CSV",
                "updated_records": 0,
                "inserted_or_upserted": 0,
                "file_hash": file_hash,
            }

        # Aggregate duplicates within the uploaded CSV
        grouped = (
            df.groupby(["Brand_Category", "Brand", "Size_ML", "Date"], as_index=False)["Qty"]
            .sum()
        )

        now = datetime.utcnow()
        operations = []

        for _, row in grouped.iterrows():
            brand_category = row["Brand_Category"]
            brand = row["Brand"]
            size_ml = int(row["Size_ML"])
            date_str = row["Date"]
            qty = int(row["Qty"])

            filter_doc = {
                "Brand_Category": brand_category,
                "Brand": brand,
                "Size_ML": size_ml,
                "Date": date_str,
            }

            update_doc = {
                "$inc": {
                    "Qty": qty
                },
                "$set": {
                    "Last_Updated": now,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "Brand_Category": brand_category,
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "Date": date_str,
                }
            }

            operations.append(
                UpdateOne(filter_doc, update_doc, upsert=True)
            )

        if not operations:
            return {
                "status": "success",
                "message": "No operations generated",
                "updated_records": 0,
                "inserted_or_upserted": 0,
                "file_hash": file_hash,
            }

        try:
            result = self.current_inventory.bulk_write(operations, ordered=False)

            return {
                "status": "success",
                "message": "current_inventory updated successfully from CSV",
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_count": len(result.upserted_ids) if result.upserted_ids else 0,
                "processed_grouped_rows": len(grouped),
                "file_hash": file_hash,
            }

        except BulkWriteError as e:
            raise ValueError(f"Bulk write failed: {e.details}")

    def close(self):
        self.client.close()

# if __name__ == "__main__":
#     MONGO_URI = "mongodb://localhost:27017/"
#     DB_NAME = "your_database_name"
#     CSV_PATH = "/path/to/incoming_stock_upload.csv"
#
#     updater = CurrentInventoryUpdater(MONGO_URI, DB_NAME)
#
#     try:
#         result = updater.update_current_inventory_from_csv(
#             csv_path=CSV_PATH,
#             prevent_duplicate_upload=True
#         )
#         print(result)
#     except Exception as e:
#         print(f"Update failed: {e}")
#     finally:
#         updater.close()
