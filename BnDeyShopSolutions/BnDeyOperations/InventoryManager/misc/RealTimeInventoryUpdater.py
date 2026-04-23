from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime
import time


class RealTimeInventoryUpdater:
    def __init__(self, mongo_uri="mongodb://localhost:27017/?replicaSet=rs0", db_name="inventory_db"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

        self.current_inventory = self.db["current_inventory"]
        self.daily_sales = self.db["daily_sales"]
        self.incoming_stock = self.db["incoming_stock"]

    def _build_inventory_filter(self, doc):
        return {
            "Brand": doc["Brand"],
            "Brand_Category": doc["Brand_Category"],
            "Size_ML": int(doc["Size_ML"]),
            "Date": doc["Date"]
        }

    def _ensure_int(self, value):
        return int(value) if value is not None else 0

    def _update_inventory_qty(self, source_doc, qty_change):
        """
        qty_change:
            +ve => incoming stock
            -ve => sales
        """
        inventory_filter = self._build_inventory_filter(source_doc)

        existing = self.current_inventory.find_one(inventory_filter)

        if existing:
            new_qty = self._ensure_int(existing.get("Qty", 0)) + qty_change
        else:
            new_qty = qty_change

        # Optional protection: never allow negative inventory
        if new_qty < 0:
            raise ValueError(
                f"Inventory would become negative for "
                f"{source_doc['Brand']} {source_doc['Size_ML']}ML on {source_doc['Date']}. "
                f"Computed Qty={new_qty}"
            )

        update_doc = {
            "$set": {
                "Brand": source_doc["Brand"],
                "Brand_Category": source_doc["Brand_Category"],
                "Size_ML": int(source_doc["Size_ML"]),
                "Date": source_doc["Date"],
                "Qty": new_qty,
                "Last_Updated": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
        }

        self.current_inventory.update_one(inventory_filter, update_doc, upsert=True)

        print(
            f"Updated current_inventory | "
            f"{source_doc['Brand']} | {source_doc['Size_ML']}ML | "
            f"{source_doc['Date']} | Qty change={qty_change} | New Qty={new_qty}"
        )

    def process_incoming_insert(self, doc):
        qty = self._ensure_int(doc.get("Qty", 0))
        if qty <= 0:
            print("Skipping incoming_stock record with invalid Qty")
            return
        self._update_inventory_qty(doc, qty_change=qty)

    def process_sale_insert(self, doc):
        qty = self._ensure_int(doc.get("Qty", 0))
        if qty <= 0:
            print("Skipping daily_sales record with invalid Qty")
            return
        self._update_inventory_qty(doc, qty_change=-qty)

    def watch_incoming_stock(self):
        pipeline = [
            {
                "$match": {
                    "operationType": "insert"
                }
            }
        ]

        with self.incoming_stock.watch(pipeline, full_document='updateLookup') as stream:
            print("Watching incoming_stock for inserts...")
            for change in stream:
                try:
                    full_doc = change["fullDocument"]
                    self.process_incoming_insert(full_doc)
                except Exception as e:
                    print(f"Error processing incoming_stock change: {e}")

    def watch_daily_sales(self):
        pipeline = [
            {
                "$match": {
                    "operationType": "insert"
                }
            }
        ]

        with self.daily_sales.watch(pipeline, full_document='updateLookup') as stream:
            print("Watching daily_sales for inserts...")
            for change in stream:
                try:
                    full_doc = change["fullDocument"]
                    self.process_sale_insert(full_doc)
                except Exception as e:
                    print(f"Error processing daily_sales change: {e}")

    def start(self):
        """
        Start both watchers.
        Run each watcher in a separate thread.
        """
        import threading

        t1 = threading.Thread(target=self.watch_incoming_stock, daemon=True)
        t2 = threading.Thread(target=self.watch_daily_sales, daemon=True)

        t1.start()
        t2.start()

        print("Real-time inventory updater started.")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopped by user.")