from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from collections import defaultdict


class EndOfTheDayInventoryUpdateService:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="inventory_db"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

        self.current_inventory = self.db["current_inventory"]
        self.daily_sales = self.db["daily_sales"]
        self.incoming_stock = self.db["incoming_stock"]

    @staticmethod
    def parse_date(date_str: str) -> datetime:
        return datetime.strptime(date_str, "%d/%m/%Y")

    @staticmethod
    def format_date(date_obj: datetime) -> str:
        return date_obj.strftime("%d/%m/%Y")

    def _aggregate_qty_by_product(self, collection, target_date: str):
        """
        Aggregate quantity by Brand + Size_ML + Brand_Category for a given date.
        Returns:
            {
                (brand, size_ml, brand_category): total_qty
            }
        """
        pipeline = [
            {
                "$match": {
                    "Date": target_date
                }
            },
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
        ]

        result = {}
        for doc in collection.aggregate(pipeline):
            key = (
                doc["_id"]["Brand"],
                doc["_id"]["Size_ML"],
                doc["_id"]["Brand_Category"]
            )
            result[key] = int(doc["total_qty"])
        return result

    def _get_previous_day_inventory_map(self, previous_date: str):
        """
        Read previous day's closing inventory from current_inventory.
        Returns:
            {
                (brand, size_ml, brand_category): qty
            }
        """
        inventory_map = {}

        cursor = self.current_inventory.find({"Date": previous_date})
        for doc in cursor:
            key = (
                doc["Brand"],
                doc["Size_ML"],
                doc["Brand_Category"]
            )
            inventory_map[key] = int(doc.get("Qty", 0))

        return inventory_map

    def update_end_of_day_inventory(self, target_date: str):
        """
        End-of-day update:
            closing_qty = previous_day_closing_qty + incoming_qty - sales_qty

        Then upserts records into current_inventory for target_date.
        """
        target_dt = self.parse_date(target_date)
        previous_dt = target_dt - timedelta(days=1)
        previous_date = self.format_date(previous_dt)

        # Previous day closing stock
        previous_inventory = self._get_previous_day_inventory_map(previous_date)

        # Today's incoming and sales
        incoming_map = self._aggregate_qty_by_product(self.incoming_stock, target_date)
        sales_map = self._aggregate_qty_by_product(self.daily_sales, target_date)

        # All products affected today or carried from previous day
        all_keys = set(previous_inventory.keys()) | set(incoming_map.keys()) | set(sales_map.keys())

        if not all_keys:
            return {
                "status": "warning",
                "message": f"No inventory, incoming stock, or sales found for {target_date}",
                "updated_count": 0
            }

        bulk_ops = []
        summary = []

        now = datetime.utcnow()

        for key in all_keys:
            brand, size_ml, brand_category = key

            opening_qty = int(previous_inventory.get(key, 0))
            incoming_qty = int(incoming_map.get(key, 0))
            sales_qty = int(sales_map.get(key, 0))

            closing_qty = opening_qty + incoming_qty - sales_qty

            if closing_qty < 0:
                raise ValueError(
                    f"Negative closing stock for {brand} {size_ml}ML on {target_date}. "
                    f"Opening={opening_qty}, Incoming={incoming_qty}, Sales={sales_qty}, Closing={closing_qty}"
                )

            filter_query = {
                "Brand": brand,
                "Size_ML": size_ml,
                "Brand_Category": brand_category,
                "Date": target_date
            }

            update_query = {
                "$set": {
                    "Brand": brand,
                    "Size_ML": size_ml,
                    "Brand_Category": brand_category,
                    "Date": target_date,
                    "Qty": closing_qty,
                    "Last_Updated": now,
                    "updated_at": now
                },
                "$setOnInsert": {
                    "created_at": now
                }
            }

            bulk_ops.append(UpdateOne(filter_query, update_query, upsert=True))

            summary.append({
                "Brand": brand,
                "Size_ML": size_ml,
                "Brand_Category": brand_category,
                "Opening_Qty": opening_qty,
                "Incoming_Qty": incoming_qty,
                "Sales_Qty": sales_qty,
                "Closing_Qty": closing_qty,
                "Date": target_date
            })

        result = self.current_inventory.bulk_write(bulk_ops)

        return {
            "status": "success",
            "message": f"End-of-day inventory updated for {target_date}",
            "updated_count": len(bulk_ops),
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted_count": len(result.upserted_ids) if result.upserted_ids else 0,
            "summary": summary
        }


if __name__ == "__main__":
    inventory_service = EndOfTheDayInventoryUpdateService(
        mongo_uri="mongodb://localhost:27017/",
        db_name="your_database"
    )

    result = inventory_service.update_end_of_day_inventory("09/04/2026")
    print(result)