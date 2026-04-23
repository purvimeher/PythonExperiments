from pymongo import MongoClient
from config import MONGO_URI, DB_NAME


@staticmethod
def _noop():
    return None


client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def get_collection(collection_name: str):
    return db[collection_name]