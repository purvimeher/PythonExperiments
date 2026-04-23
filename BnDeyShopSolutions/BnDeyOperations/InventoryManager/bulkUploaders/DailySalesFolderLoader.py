import csv
import hashlib
from pathlib import Path
from datetime import datetime
from pymongo import MongoClient, UpdateOne, ASCENDING


class DailySalesFolderLoader:
    def __init__(
        self,
        mongo_uri="mongodb://localhost:27017/",
        db_name="bndey_db",
        sales_collection_name="daily_sales",
        file_log_collection_name="daily_sales_file_log",
    ):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.sales_collection = self.db[sales_collection_name]
        self.file_log_collection = self.db[file_log_collection_name]

        # Unique sales business key
        self.sales_collection.create_index(
            [
                ("Date", ASCENDING),
                ("Brand_Category", ASCENDING),
                ("Brand", ASCENDING),
                ("Size_ML", ASCENDING),
            ],
            unique=True,
            name="uniq_daily_sales_business_key",
        )

        # Unique file hash log for duplicate-run safety
        self.file_log_collection.create_index(
            [("file_hash", ASCENDING)],
            unique=True,
            name="uniq_file_hash",
        )

    @staticmethod
    def _compute_file_hash(file_path):
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    @staticmethod
    def _normalize_text(value):
        return str(value).strip()

    def _clean_row(self, row, file_path, line_number):
        """
        Expected columns:
        Date,Brand_Category,Brand,Size_ML,Qty
        """
        try:
            date_value = self._normalize_text(row["Date"])
            brand_category = self._normalize_text(row["Brand_Category"])
            brand = self._normalize_text(row["Brand"])
            size_ml = int(self._normalize_text(row["Size_ML"]))
            qty = int(self._normalize_text(row["Qty"]))

            if not date_value:
                raise ValueError("Date is required")
            if not brand_category:
                raise ValueError("Brand_Category is required")
            if not brand:
                raise ValueError("Brand is required")
            if size_ml <= 0:
                raise ValueError("Size_ML must be greater than 0")
            if qty < 0:
                raise ValueError("Qty cannot be negative")

            return {
                "Date": date_value,
                "Brand_Category": brand_category,
                "Brand": brand,
                "Size_ML": size_ml,
                "Qty": qty,
            }

        except Exception as e:
            raise ValueError(
                f"Invalid row in file '{file_path}', line {line_number}: {row} | Error: {e}"
            )

    def _is_file_already_processed(self, file_hash):
        return self.file_log_collection.find_one({"file_hash": file_hash}) is not None

    def _mark_file_as_processed(self, file_path, file_hash, processed_rows):
        self.file_log_collection.insert_one({
            "file_name": Path(file_path).name,
            "file_path": str(file_path),
            "file_hash": file_hash,
            "processed_rows": processed_rows,
            "processed_at": datetime.utcnow(),
        })

    def load_folder(self, folder_path):
        folder = Path(folder_path)

        if not folder.exists() or not folder.is_dir():
            raise ValueError(f"Folder does not exist or is not a directory: {folder_path}")

        csv_files = sorted(folder.glob("*.csv"))

        if not csv_files:
            return {
                "status": "success",
                "message": "No CSV files found in folder",
                "files_found": 0,
                "files_processed": 0,
                "files_skipped": 0,
                "processed_rows": 0,
                "upserted_count": 0,
                "modified_count": 0,
            }

        now = datetime.utcnow()
        aggregated_rows = {}
        files_processed = 0
        files_skipped = 0
        file_summaries = []

        for file_path in csv_files:
            file_hash = self._compute_file_hash(file_path)

            if self._is_file_already_processed(file_hash):
                files_skipped += 1
                file_summaries.append({
                    "file": file_path.name,
                    "status": "skipped",
                    "reason": "already processed",
                })
                continue

            valid_rows_in_file = 0

            with open(file_path, mode="r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)

                expected_columns = {"Date", "Brand_Category", "Brand", "Size_ML", "Qty"}
                actual_columns = set(reader.fieldnames or [])

                if not expected_columns.issubset(actual_columns):
                    file_summaries.append({
                        "file": file_path.name,
                        "status": "failed",
                        "reason": f"Missing required columns. Found: {sorted(actual_columns)}",
                    })
                    continue

                for line_number, row in enumerate(reader, start=2):
                    try:
                        clean = self._clean_row(row, file_path.name, line_number)

                        key = (
                            clean["Date"],
                            clean["Brand_Category"],
                            clean["Brand"],
                            clean["Size_ML"],
                        )

                        if key not in aggregated_rows:
                            aggregated_rows[key] = clean
                        else:
                            aggregated_rows[key]["Qty"] += clean["Qty"]

                        valid_rows_in_file += 1

                    except ValueError as e:
                        print(e)

            self._mark_file_as_processed(file_path, file_hash, valid_rows_in_file)
            files_processed += 1
            file_summaries.append({
                "file": file_path.name,
                "status": "processed",
                "valid_rows": valid_rows_in_file,
            })

        if not aggregated_rows:
            return {
                "status": "success",
                "message": "No new valid rows to load",
                "files_found": len(csv_files),
                "files_processed": files_processed,
                "files_skipped": files_skipped,
                "processed_rows": 0,
                "upserted_count": 0,
                "modified_count": 0,
                "file_summaries": file_summaries,
            }

        operations = []

        for _, doc in aggregated_rows.items():
            filter_query = {
                "Date": doc["Date"],
                "Brand_Category": doc["Brand_Category"],
                "Brand": doc["Brand"],
                "Size_ML": doc["Size_ML"],
            }

            update_query = {
                "$set": {
                    "Qty": doc["Qty"],
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                },
            }

            operations.append(UpdateOne(filter_query, update_query, upsert=True))

        result = self.sales_collection.bulk_write(operations, ordered=False)

        return {
            "status": "success",
            "message": "Folder CSV load completed",
            "files_found": len(csv_files),
            "files_processed": files_processed,
            "files_skipped": files_skipped,
            "processed_rows": len(aggregated_rows),
            "upserted_count": result.upserted_count,
            "modified_count": result.modified_count,
            "matched_count": result.matched_count,
            "file_summaries": file_summaries,
        }


if __name__ == "__main__":
    loader = DailySalesFolderLoader(
        mongo_uri="mongodb://localhost:27017/",
        db_name="bndey_db",
        sales_collection_name="daily_sales",   # change to "daiy_sales" only if your real collection name is spelled that way
        file_log_collection_name="daily_sales_file_log",
    )

    folder_path = "/chapter/BnDeyOperations/data/Daily_Sales"

    result = loader.load_folder(folder_path)
    print(result)