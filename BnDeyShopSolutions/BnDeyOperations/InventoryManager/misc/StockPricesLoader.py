import json

import pandas as pd
from pymongo import MongoClient

from BnDeyShopSolutions.BnDeyOperations.configs.ConfigLoader import ConfigLoader


class StockPricesLoader():
    def __init__(self,db="inventory_db"):
        config = ConfigLoader.load_config()
        stock_prices_csv_file = config["paths"]["stock_prices_csv_path"]+config["csvFileNames"]["stock_prices_csv"]+config["fileExt"]["csv"]
        # stock_prices_csv_file = config["paths"]["incoming_stock_csv_path"]+config["csvFileNames"]["stock_prices_csv"]+config["fileExt"]["csv"]
        self.__stock_prices_csv_data = stock_prices_csv_file
        self.__stock_prices_json_data = f'{config["paths"]["output_intermediate_fdr"]}stock_price.json'
        self.__mongodDbConnectionUrl = 'mongodb://localhost:27017/'
        self.db = db


    def __readCsvIntoDataFrameIntoJson(self):
        stock_prices_data = pd.read_csv(
            self.__stock_prices_csv_data)

        stock_prices_data.to_json(self.__stock_prices_json_data, orient='records')


    def __loadStockPricesJsonIntoMongoDb(self):

        # Making Connection
        myclient = MongoClient(self.__mongodDbConnectionUrl)

        # database
        db = myclient[self.db]

        # Created or Switched to collection
        # names: GeeksForGeeks
        Collection = db["stock_prices"]

        # Loading or Opening the json file
        with open(
                self.__stock_prices_json_data) as file:
            file_data = json.load(file)

        # Step 1: Clean
        clean_data = []
        for doc in file_data:
            doc = {k.strip(): v for k, v in doc.items()}
            doc.pop("_id", None)
            clean_data.append(doc)

        # Step 2: Ensure uniqueness
        Collection.create_index(
            [("Brand", 1), ("Size_ML", 1)],
            unique=True
        )

        # Step 3: Upsert
        for doc in clean_data:
            Collection.update_one(
                {"Brand": doc["Brand"], "Size_ML": doc["Size_ML"]},
                {"$set": doc},
                upsert=True
            )

    def LoadStockPricesJsonIntoMongoDb_AllInOneStep(self):
        self.__readCsvIntoDataFrameIntoJson()
        self.__loadStockPricesJsonIntoMongoDb()