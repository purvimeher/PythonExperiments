from pymongo import MongoClient, UpdateOne
from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.InventoryService import InventoryService
from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.PlainDailySalesCsvLoader import PlainDailySalesCsvLoader
from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.StockPricesLoader import StockPricesLoader
from BnDeyShopSolutions.BnDeyOperations.configs.ConfigLoader import ConfigLoader


class BndeyDataUploaderService(object):
    def __init__(self, dailySalesCsvFile="Daily_sales_test"):
        config = ConfigLoader.load_config()
        mongo_uri = config["mongodb"]["uri"]

        self.db_name = config["mongodb"]["database"]
        self.inventory_db_name = config["mongodb"]["database"]
        self.daily_sales_csv_file = dailySalesCsvFile
        self.incoming_sales_csv_file = config["csvFileNames"]["incoming_stock_csv"]
        self.daily_sales_csv_path = config["paths"]["daily_sales_csv_path"]
        self.incoming_stock_csv_path = config["paths"]["incoming_stock_csv_path"]
        self.daily_sales_csv_ext = config["fileExt"]["csv"]

        self.client = MongoClient(mongo_uri)
        self.db = self.client[self.db_name]

        self.source_collection = self.client[self.inventory_db_name]["current_inventory"]
        self.target_collection = self.client[self.db_name]["current_inventory"]

    def load_stock_prices(self):
        stockPricesLoader = StockPricesLoader(self.db_name)
        stockPricesLoader.LoadStockPricesJsonIntoMongoDb_AllInOneStep()

    def load_monthly_stock(self):
        inventory_service = InventoryService(self.db_name)
        inventory_service.loadMonthlyStockCsvIntoMongoDb(False)

    def bulk_copy_with_upsert(self):

        operations = []

        for doc in self.source_collection.find():
            operations.append(
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": doc},
                    upsert=True
                )
            )

            if len(operations) == 1000:
                self.target_collection.bulk_write(operations)
                operations = []

        if operations:
            self.target_collection.bulk_write(operations)

        print("Bulk copy with upsert completed.")

    def load_incoming_stock(self):
        loader = PlainDailySalesCsvLoader(
            self.client,
            db_name=self.db_name,
            collection_name="incoming_stock",
        )
        csv_path = self.incoming_stock_csv_path + self.incoming_sales_csv_file + self.daily_sales_csv_ext

        result = loader.load_csv(csv_path)
        print(result)

    def load_daily_sales(self):
        loader = PlainDailySalesCsvLoader(
            self.client,
            db_name=self.db_name,
            collection_name="daily_sales",
        )
        csv_path = f'{self.daily_sales_csv_path}{self.daily_sales_csv_file}{self.daily_sales_csv_ext}'

        result = loader.load_csv(csv_path)
        print(result)

########################################################### MAIN EXECUTIONS FROM HERE ###########################################################

bndeyDataUploaderService = BndeyDataUploaderService()
bndeyDataUploaderService.load_stock_prices()
bndeyDataUploaderService.load_monthly_stock()
bndeyDataUploaderService.bulk_copy_with_upsert()
bndeyDataUploaderService.load_daily_sales()
bndeyDataUploaderService.load_incoming_stock()

