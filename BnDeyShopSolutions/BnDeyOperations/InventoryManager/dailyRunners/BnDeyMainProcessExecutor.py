from BnDeyShopSolutions.BnDeyOperations.BnDeyDataProcessor.BndeyDataUploaderService import BndeyDataUploaderService
from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.DailySaleCurrentInventoryUpdater import DailySaleCurrentInventoryUpdater
from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.DailySalesUploader import DailySalesUploader
from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.EndOfTheDayInventoryUpdateService import \
    EndOfTheDayInventoryUpdateService
from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.InventoryService import InventoryService
from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.StockPricesLoader import StockPricesLoader
from BnDeyShopSolutions.BnDeyOperations.InventoryManager.misc.UploadIncomingStockInventoryService import \
    UploadIncomingStockInventoryService
from BnDeyShopSolutions.BnDeyOperations.configs.ConfigLoader import ConfigLoader


class BnDeyMainProcessExecutor():
    def __init__(self):
        self.config = ConfigLoader.load_config()



    def Load_Stock_Prices(self):
        stockPricesLoader = StockPricesLoader()
        stockPricesLoader.LoadStockPricesJsonIntoMongoDb_AllInOneStep()

    def Load_Initial_Stock_Into_Database(self):
        inventory_service = InventoryService()
        inventory_service.loadMonthlyStockCsvIntoMongoDb(False)

    def Load_Incoming_Stock_Into_Db_and_Update_Current_Inventory(self):
        __mongodDbConnectionUrl = self.config["mongodb"]["uri"]
        __db =  self.config["mongodb"]["database"]
        uploadIncomingStockInventoryService = UploadIncomingStockInventoryService(__mongodDbConnectionUrl, __db)
        try:
            filename = f'{self.config["paths"]["incoming_stock_csv_path"]}Incoming_Stock.csv'
            result = uploadIncomingStockInventoryService.update_current_inventory_from_csv(filename)
            print(result)
        except Exception as e:
            print(f"Update failed: {e}")
        finally:
            uploadIncomingStockInventoryService.close()

    def Update_Inventory_State(self):
        self.__inventoryService.aggregate_inventory()
        self.__inventoryService.update_current_inventory_daily()

    @staticmethod
    def Load_Daily_Sales_Data_Into_Database(dailysalescsvfilename):
        config = ConfigLoader.load_config()
        bndeyDataUploaderService = BndeyDataUploaderService(dailySalesCsvFile=dailysalescsvfilename)
        bndeyDataUploaderService.load_daily_sales()
        dailysalesuploader = DailySaleCurrentInventoryUpdater(db_name=config["mongodb"]["database"])
        csv_file_path = f'{config["paths"]["daily_sales_csv_path"]}/{dailysalescsvfilename}.csv'
        dailysalesuploader.apply_sales_csv_to_current_inventory(csv_file_path)

    def update_current_inventory_daily(self, process_date):
        try:
            dailysalesuploader = DailySalesUploader()
            update_result = dailysalesuploader.update_current_inventory_daily(process_date)
            print(update_result)
        except Exception as e:
            print(f"Inventory update failed: {e}")

    def RunEndOfTheDayInventoryUpdate(self, process_date):
        _runEndOfTheDayInventoryService = EndOfTheDayInventoryUpdateService()
        result = _runEndOfTheDayInventoryService.update_end_of_day_inventory(process_date)
        print(result)


# MAIN EXECUTIONS STARTS FROM HERE
bndeyMainProcessExecutor = BnDeyMainProcessExecutor()
# THIS SHOULD BE ONLY RUN ONCE TO INSITIALISE STOCK LEVELS
# THIS SHOULD BE ONLY RUN ONCE TO INSITIALISE STOCK PRICES
# bndeyMainProcessExecutor.Load_Stock_Prices()
# bndeyMainProcessExecutor.Load_Initial_Stock_Into_Database()
#
# BELOW CAN BE USED TO INSERT INCOMING STOCK AND UPDATE CURRENT INVENTORY
# bndeyMainProcessExecutor.Load_Incoming_Stock_Into_Db_and_Update_Current_Inventory()


# BELOW CAN BE DAILY USED TO RECORD DAILY SALE AND UPDATE CURRENT INVENTORY --Run this twice
# daily_sales_files =['Daily_sales_09042026','Daily_sales_10042026','Daily_sales_11042026','Daily_sales_19042026']
# for filename in daily_sales_files:
#     BnDeyMainProcessExecutor.Load_Daily_Sales_Data_Into_Database(filename)
#
# process_dates =['09/04/2026','10/04/2026','11/04/2026','19/04/2026']
# for d in process_dates:
#     bndeyMainProcessExecutor.update_current_inventory_daily(process_date=d)