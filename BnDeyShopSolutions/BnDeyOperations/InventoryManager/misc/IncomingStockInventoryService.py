from datetime import datetime
from pymongo import MongoClient, UpdateOne
import pandas as pd


class IncomingStockInventoryService:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="inventory_db"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

        self.monthly_stock = self.db["monthly_stock"]
        self.incoming_stock = self.db["incoming_stock"]
        self.current_inventory = self.db["current_inventory"]

    def load_incoming_stock_csv_and_update_inventory(self, csv_file_path):
        """
        1. Read incoming stock CSV
        2. Insert rows into incoming_stock
        3. Aggregate totals per product/date
        4. Recalculate and bulk update current_inventory
        """

        df = pd.read_csv(csv_file_path)

        # Normalize column names if needed
        df.columns = [col.strip() for col in df.columns]

        required_columns = ["Brand", "Size_ML", "Brand_Category", "Date", "Qty"]
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns in CSV: {missing}")

        # Clean data
        df["Brand"] = df["Brand"].astype(str).str.strip()
        df["Brand_Category"] = df["Brand_Category"].astype(str).str.strip()
        df["Date"] = df["Date"].astype(str).str.strip()
        df["Size_ML"] = pd.to_numeric(df["Size_ML"], errors="coerce").fillna(0).astype(int)
        df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0).astype(int)

        # Remove invalid rows
        df = df[(df["Qty"] > 0) & (df["Size_ML"] > 0)].copy()

        if df.empty:
            return {
                "status": "warning",
                "message": "No valid incoming stock rows found in CSV"
            }

        now = datetime.utcnow()

        # Insert each CSV row into incoming_stock
        incoming_docs = []
        for _, row in df.iterrows():
            incoming_docs.append({
                "Brand": row["Brand"],
                "Size_ML": int(row["Size_ML"]),
                "Brand_Category": row["Brand_Category"],
                "Date": row["Date"],
                "Qty": int(row["Qty"]),
                "created_at": now,
                "updated_at": now
            })

        if incoming_docs:
            self.incoming_stock.insert_many(incoming_docs)

        # Group CSV data so we only recalculate affected products/dates
        grouped_df = (
            df.groupby(["Date", "Brand", "Size_ML", "Brand_Category"], as_index=False)["Qty"]
            .sum()
        )

        bulk_updates = []

        for _, row in grouped_df.iterrows():
            match_filter = {
                "Date": row["Date"],
                "Brand": row["Brand"],
                "Size_ML": int(row["Size_ML"]),
                "Brand_Category": row["Brand_Category"]
            }

            # Get monthly stock total for this key
            monthly_result = list(self.monthly_stock.aggregate([
                {"$match": match_filter},
                {"$group": {"_id": None, "total_qty": {"$sum": "$Qty"}}}
            ]))
            monthly_qty = monthly_result[0]["total_qty"] if monthly_result else 0

            # Get incoming stock total for this key
            incoming_result = list(self.incoming_stock.aggregate([
                {"$match": match_filter},
                {"$group": {"_id": None, "total_qty": {"$sum": "$Qty"}}}
            ]))
            incoming_qty = incoming_result[0]["total_qty"] if incoming_result else 0

            final_qty = monthly_qty + incoming_qty

            bulk_updates.append(
                UpdateOne(
                    match_filter,
                    {
                        "$set": {
                            "Date": row["Date"],
                            "Brand": row["Brand"],
                            "Size_ML": int(row["Size_ML"]),
                            "Brand_Category": row["Brand_Category"],
                            "Qty": int(final_qty),
                            "Last_Updated": now,
                            "updated_at": now
                        }
                    },
                    upsert=True
                )
            )

        if bulk_updates:
            result = self.current_inventory.bulk_write(bulk_updates, ordered=False)
            return {
                "status": "success",
                "csv_rows_processed": len(df),
                "incoming_rows_inserted": len(incoming_docs),
                "inventory_rows_updated": len(bulk_updates),
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_count": len(result.upserted_ids)
            }

        return {
            "status": "warning",
            "message": "No inventory updates generated"
        }


if __name__ == "__main__":
    service = IncomingStockInventoryService(
        mongo_uri="mongodb://localhost:27017/",
        db_name="inventory_db"
    )

    result = service.load_incoming_stock_csv_and_update_inventory(
        "/path/to/incoming_stock.csv"
    )
    print(result)