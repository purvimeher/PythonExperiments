import csv
from datetime import datetime
from pymongo import MongoClient, UpdateOne, ASCENDING


class PlainDailySalesCsvLoader:
    def __init__(
        self,
        client,
        db_name="inventory_db",
        collection_name="daily_sales",
    ):
        self.client = client
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

        # Unique index prevents duplicate records for same product on same date
        self.collection.create_index(
            [
                ("Date", ASCENDING),
                ("Brand_Category", ASCENDING),
                ("Brand", ASCENDING),
                ("Size_ML", ASCENDING),
            ],
            unique=True,
            name="uniq_daily_sales_business_key",
        )

    @staticmethod
    def _clean_row(row):
        """
        Normalize and validate one CSV row.
        Expected columns:
        Date,Brand_Category,Brand,Size_ML,Qty
        """
        date_value = row["Date"].strip()
        brand_category = row["Brand_Category"].strip()
        brand = row["Brand"].strip()
        size_ml = int(str(row["Size_ML"]).strip())
        qty = int(str(row["Qty"]).strip())

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

    def load_csv(self, csv_file_path):
        """
        Loads CSV into daily_sales using bulk upsert.
        Duplicate rows in file are consolidated before Mongo write.
        Duplicate runs are safe because same business key is upserted, not inserted again.
        """
        now = datetime.utcnow()

        # Consolidate duplicates inside the same CSV
        aggregated_rows = {}

        with open(csv_file_path, mode="r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)

            expected_columns = {"Date", "Brand_Category", "Brand", "Size_ML", "Qty"}
            actual_columns = set(reader.fieldnames or [])

            if not expected_columns.issubset(actual_columns):
                raise ValueError(
                    f"CSV must contain columns: {sorted(expected_columns)}. "
                    f"Found: {sorted(actual_columns)}"
                )

            for line_number, row in enumerate(reader, start=2):
                try:
                    clean = self._clean_row(row)
                    key = (
                        clean["Date"],
                        clean["Brand_Category"],
                        clean["Brand"],
                        clean["Size_ML"],
                    )

                    if key not in aggregated_rows:
                        aggregated_rows[key] = clean
                    else:
                        # Sum duplicate rows inside same CSV
                        aggregated_rows[key]["Qty"] += clean["Qty"]

                except Exception as e:
                    print(f"Skipping invalid row at line {line_number}: {row} | Error: {e}")

        if not aggregated_rows:
            return {
                "status": "success",
                "message": "No valid rows found in CSV",
                "processed_rows": 0,
                "upserted_count": 0,
                "modified_count": 0,
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

            operations.append(
                UpdateOne(filter_query, update_query, upsert=True)
            )

        result = self.collection.bulk_write(operations, ordered=False)

        return {
            "status": "success",
            "message": "CSV loaded into daily_sales successfully",
            "processed_rows": len(aggregated_rows),
            "upserted_count": result.upserted_count,
            "modified_count": result.modified_count,
            "matched_count": result.matched_count,
        }


if __name__ == "__main__":
    loader = PlainDailySalesCsvLoader(
        mongo_uri="mongodb://localhost:27017/",
        db_name="inventory_db",
        collection_name="daily_sales",   # change to "daiy_sales" only if that typo is your real collection name
    )

    csv_path = "/path/to/daily_sales.csv"

    result = loader.load_csv(csv_path)
    print(result)