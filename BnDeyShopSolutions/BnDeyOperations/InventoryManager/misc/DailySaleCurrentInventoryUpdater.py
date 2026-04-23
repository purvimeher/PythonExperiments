from datetime import datetime, timezone
from pathlib import Path
import hashlib
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError


class DailySaleCurrentInventoryUpdater:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="bndey_db_Experimental"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

        self.current_inventory = self.db["current_inventory"]
        self.processed_uploads = self.db["processed_uploads"]

        # Prevent same uploaded file from being applied twice
        self.processed_uploads.create_index("file_hash", unique=True)

        # Helpful index for inventory matching
        self.current_inventory.create_index(
            [("Brand_Category", 1), ("Brand", 1), ("Size_ML", 1), ("Date", 1)],
            unique=False
        )

    def _utc_now(self):
        return datetime.now(timezone.utc)

    def _file_hash(self, file_path: str) -> str:
        """
        Create a stable hash for the uploaded CSV file.
        Used to prevent duplicate processing of the same upload.
        """
        sha = hashlib.sha256()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                sha.update(chunk)
        return sha.hexdigest()

    def _normalize_sales_csv(self, csv_path: str) -> pd.DataFrame:
        """
        Read and normalize CSV columns.
        Expected CSV columns:
        Brand_Category, Brand, Size_ML, Date, Qty
        """
        df = pd.read_csv(csv_path)

        # Clean column names
        df.columns = [c.strip() for c in df.columns]

        required_cols = {"Brand_Category", "Brand", "Size_ML", "Date", "Qty"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns in CSV: {sorted(missing)}")

        # Normalize values
        df["Brand_Category"] = df["Brand_Category"].astype(str).str.strip()
        df["Brand"] = df["Brand"].astype(str).str.strip()
        df["Size_ML"] = pd.to_numeric(df["Size_ML"], errors="raise").astype(int)
        df["Date"] = df["Date"].astype(str).str.strip()
        df["Qty"] = pd.to_numeric(df["Qty"], errors="raise").astype(int)

        # Keep only positive sale quantities
        if (df["Qty"] <= 0).any():
            bad_rows = df[df["Qty"] <= 0]
            raise ValueError(
                f"CSV contains invalid Qty <= 0 rows:\n{bad_rows.to_dict(orient='records')}"
            )

        return df

    def _aggregate_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate duplicate sales rows in the uploaded CSV.
        """
        grouped = (
            df.groupby(["Brand_Category", "Brand", "Size_ML", "Date"], as_index=False)["Qty"]
            .sum()
        )
        return grouped

    def apply_sales_csv_to_current_inventory(self, csv_path: str):
        """
        Decrease current_inventory based on sales CSV.
        - daily_sales collection is NOT touched
        - current_inventory only is updated
        - duplicate rows in CSV are aggregated
        - duplicate processing of same upload is prevented
        """
        csv_path = str(Path(csv_path).expanduser().resolve())

        # 1. Prevent same file upload from being processed twice
        file_hash = self._file_hash(csv_path)

        try:
            self.processed_uploads.insert_one({
                "file_hash": file_hash,
                "file_name": Path(csv_path).name,
                "file_path": csv_path,
                "processed_at": self._utc_now()
            })
        except Exception:
            return {
                "status": "skipped",
                "message": f"This CSV upload was already processed earlier: {Path(csv_path).name}"
            }

        # 2. Read + normalize CSV
        df = self._normalize_sales_csv(csv_path)

        # 3. Aggregate duplicate sales rows inside the CSV
        sales_df = self._aggregate_duplicates(df)

        # 4. Validate stock before bulk update
        insufficient_stock = []
        not_found = []

        for _, row in sales_df.iterrows():
            flt = {
                "Brand_Category": row["Brand_Category"],
                "Brand": row["Brand"],
                "Size_ML": int(row["Size_ML"]),
                "Date": row["Date"]
            }

            inv_doc = self.current_inventory.find_one(flt)

            if not inv_doc:
                not_found.append({
                    "Brand_Category": row["Brand_Category"],
                    "Brand": row["Brand"],
                    "Size_ML": int(row["Size_ML"]),
                    "Date": row["Date"],
                    "Sale_Qty": int(row["Qty"])
                })
                continue

            current_qty = int(inv_doc.get("Qty", 0))
            sale_qty = int(row["Qty"])

            if current_qty < sale_qty:
                insufficient_stock.append({
                    "Brand_Category": row["Brand_Category"],
                    "Brand": row["Brand"],
                    "Size_ML": int(row["Size_ML"]),
                    "Date": row["Date"],
                    "Current_Qty": current_qty,
                    "Sale_Qty": sale_qty
                })

        # If any issues, do not update inventory
        if not_found or insufficient_stock:
            # Roll back the processed_upload marker so user can fix and retry
            self.processed_uploads.delete_one({"file_hash": file_hash})

            return {
                "status": "failed",
                "message": "Inventory update aborted due to missing inventory rows or insufficient stock.",
                "not_found": not_found,
                "insufficient_stock": insufficient_stock
            }

        # 5. Build bulk inventory updates
        now = self._utc_now()
        operations = []

        for _, row in sales_df.iterrows():
            flt = {
                "Brand_Category": row["Brand_Category"],
                "Brand": row["Brand"],
                "Size_ML": int(row["Size_ML"]),
                "Date": row["Date"]
            }

            sale_qty = int(row["Qty"])

            operations.append(
                UpdateOne(
                    flt,
                    {
                        "$inc": {"Qty": -sale_qty},
                        "$set": {
                            "Last_Updated": now,
                            "updated_at": now
                        }
                    }
                )
            )

        # 6. Execute bulk update
        try:
            result = self.current_inventory.bulk_write(operations, ordered=False)
        except BulkWriteError as e:
            # Roll back processed file record if bulk update fails
            self.processed_uploads.delete_one({"file_hash": file_hash})
            raise RuntimeError(f"Bulk inventory update failed: {e.details}")

        return {
            "status": "success",
            "message": "current_inventory updated successfully based on sales CSV.",
            "file_name": Path(csv_path).name,
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "processed_groups": len(sales_df)
        }




# if __name__ == "__main__":
#     updater = DailySaleCurrentInventoryUpdater(
#         mongo_uri="mongodb://localhost:27017/",
#         db_name="inventory_db"
#     )
#
#     csv_file = "/path/to/Daily_sales_09042026.csv"
#     response = updater.apply_sales_csv_to_current_inventory(csv_file)
#     print(response)