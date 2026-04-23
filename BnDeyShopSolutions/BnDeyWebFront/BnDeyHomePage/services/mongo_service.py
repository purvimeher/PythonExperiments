from extensions import mongo_db


def get_collection(collection_name):
    return mongo_db[collection_name]