from bson import ObjectId
from datetime import datetime
from database import MongoDBConnection
from pymongo import UpdateOne


class StockPriceService:

    def __init__(self):
        db = MongoDBConnection()
        self.collection = db.get_collection("stock_prices")

    def serialize(self, doc):

        if doc:
            doc["_id"] = str(doc["_id"])

        return doc

    def get_all_stock_prices(self):

        records = list(self.collection.find())

        return [
            self.serialize(record)
            for record in records
        ]

    def get_stock_price_by_id(self, record_id):

        record = self.collection.find_one(
            {
                "_id": ObjectId(record_id)
            }
        )

        if not record:
            return None

        return self.serialize(record)

    def get_stock_price(self, brand, size_ml):

        record = self.collection.find_one(
            {
                "Brand": brand,
                "Size_ML": size_ml
            }
        )

        if not record:
            return None

        return self.serialize(record)

    def serialize(self, doc):
        if doc:
            doc["_id"] = str(doc["_id"])
        return doc

    def upsert_stock_price(self, data: dict):
        # Business key to identify unique stock price record
        filter_query = {
            "Brand": data["Brand"],
            "Size_ML": data["Size_ML"],
            "Brand_Category": data["Brand_Category"]
        }

        # Fields to update on every upsert
        update_fields = {
            "LookColumn": data["LookColumn"],
            "Maximum_Retail_Price_per_bottle": data["Maximum_Retail_Price_per_bottle"],
            "Maximum_Retail_Price_per_bottle_OLD": data["Maximum_Retail_Price_per_bottle_OLD"],
            "Maximum_Retail_Price_per_case": data["Maximum_Retail_Price_per_case"],
            "Sl_No": data["Sl_No"],
            "updated_at": datetime.utcnow()
        }

        # Insert-only fields
        insert_fields = {
            "created_at": datetime.utcnow()
        }

        result = self.collection.update_one(
            filter_query,
            {
                "$set": update_fields,
                "$setOnInsert": insert_fields
            },
            upsert=True
        )

        # Detect inserted vs updated
        if result.upserted_id:
            doc = self.collection.find_one({"_id": result.upserted_id})
            return {
                "message": "Record inserted",
                "action": "inserted",
                "data": self.serialize(doc)
            }

        doc = self.collection.find_one(filter_query)
        return {
            "message": "Record updated",
            "action": "updated",
            "data": self.serialize(doc)
        }

    # POST — Create new stock price
    def create_stock_price(self, data):

        # Optional duplicate check
        existing = self.collection.find_one({
            "Brand": data["Brand"],
            "Size_ML": data["Size_ML"]
        })

        if existing:
            return {
                "message": "Record already exists"
            }

        data["created_at"] = datetime.utcnow()

        result = self.collection.insert_one(data)

        return {
            "message": "Stock price created",
            "inserted_id": str(result.inserted_id)
        }

    # PUT — Update stock price
    def update_stock_price(self, record_id, data):

        data["updated_at"] = datetime.utcnow()

        result = self.collection.update_one(
            {
                "_id": ObjectId(record_id)
            },
            {
                "$set": data
            }
        )

        if result.matched_count == 0:
            return None

        return {
            "message": "Stock price updated"
        }

    def create_indexes(self):
        self.collection.create_index(
            [("Brand", 1), ("Size_ML", 1), ("Brand_Category", 1)],
            unique=True
        )

    def bulk_upsert_stock_prices(self, records: list):
        if not records:
            return {
                "message": "No records provided",
                "inserted_count": 0,
                "updated_count": 0,
                "matched_count": 0
            }

        operations = []
        inserted_candidates = 0
        updated_candidates = 0

        for row in records:
            filter_query = {
                "Brand": row["Brand"],
                "Size_ML": row["Size_ML"],
                "Brand_Category": row["Brand_Category"]
            }

            existing = self.collection.find_one(filter_query)

            if existing:
                old_current_price = existing.get("Maximum_Retail_Price_per_bottle")
                new_price = row.get("Maximum_Retail_Price_per_bottle")

                if old_current_price != new_price:
                    row["Maximum_Retail_Price_per_bottle_OLD"] = old_current_price

                updated_candidates += 1
            else:
                inserted_candidates += 1

            row["updated_at"] = datetime.utcnow()

            operations.append(
                UpdateOne(
                    filter_query,
                    {
                        "$set": row,
                        "$setOnInsert": {
                            "created_at": datetime.utcnow()
                        }
                    },
                    upsert=True
                )
            )

        result = self.collection.bulk_write(operations, ordered=False)

        return {
            "message": "Bulk upsert completed",
            "inserted_count": result.upserted_count,
            "updated_count": result.modified_count,
            "matched_count": result.matched_count,
            "expected_inserts": inserted_candidates,
            "expected_updates": updated_candidates
        }
