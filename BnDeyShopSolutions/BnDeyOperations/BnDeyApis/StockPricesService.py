from BnDeyShopSolutions.BnDeyOperations.BnDeyApis.Database import Database


class StockPricesService:

    def __init__(self):
        db = Database()
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