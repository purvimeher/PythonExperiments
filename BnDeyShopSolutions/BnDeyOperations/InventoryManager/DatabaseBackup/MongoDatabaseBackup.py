from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import PyMongoError


class MongoDatabaseBackup:
    def __init__(self, mongo_uri: str, source_db_name: str):
        self.client = MongoClient(mongo_uri)
        self.source_db_name = source_db_name
        self.source_db = self.client[source_db_name]

        date_str = datetime.now().strftime("%Y_%m_%d")
        self.backup_db_name = f"{source_db_name}_backup_{date_str}"
        self.backup_db = self.client[self.backup_db_name]

    def get_all_collection_names(self):
        return self.source_db.list_collection_names()

    def copy_collection(self, collection_name: str):
        source_collection = self.source_db[collection_name]
        backup_collection = self.backup_db[collection_name]

        documents = list(source_collection.find({}))

        # Clear target collection before copy, in case script is re-run
        backup_collection.delete_many({})

        if documents:
            backup_collection.insert_many(documents)

        return len(documents)

    def backup_all_collections(self):
        collection_names = self.get_all_collection_names()

        if not collection_names:
            print(f"No collections found in database '{self.source_db_name}'.")
            return

        total_documents = 0

        print(f"Starting backup of database '{self.source_db_name}'")
        print(f"Backup database: '{self.backup_db_name}'")
        print("-" * 50)

        for collection_name in collection_names:
            copied_count = self.copy_collection(collection_name)
            total_documents += copied_count
            print(f"Copied collection '{collection_name}' -> {copied_count} documents")

        print("-" * 50)
        print("Backup completed successfully")
        print(f"Total collections copied: {len(collection_names)}")
        print(f"Total documents copied: {total_documents}")

    def close(self):
        self.client.close()


if __name__ == "__main__":
    MONGO_URI = "mongodb://localhost:27017/"
    SOURCE_DB = "bndey_db"

    backup_service = MongoDatabaseBackup(
        mongo_uri=MONGO_URI,
        source_db_name=SOURCE_DB
    )

    try:
        backup_service.backup_all_collections()
    except PyMongoError as e:
        print(f"MongoDB error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        backup_service.close()