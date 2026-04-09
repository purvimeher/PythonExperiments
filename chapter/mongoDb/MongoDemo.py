from pymongo import MongoClient


class MongoDbInteractions():

    def __init__(self, dbUrl, dbname, dbCollectionName):
        self.dbUrl = dbUrl
        self.dbname = dbname
        self.dbCollectionName = dbCollectionName

    def findEmployeeById(self,columnName, id):
        client = MongoClient(self.dbUrl)
        db = client[self.dbname]
        collection = db[self.dbCollectionName]
        cursor = collection.find({columnName : id})
        print(f"The data having {columnName} {id} is:")
        for doc in cursor:
            print(doc)

    def insertEmployee(self, data):
        client = MongoClient(self.dbUrl)
        db = client[self.dbname]
        collection = db[self.dbCollectionName]
        collection.insert_one(data)


mongo = MongoDbInteractions("mongodb://localhost:27017/", "MyFirstDb", "employees")
mongo.findEmployeeById("vendorId", 1)

testData = {'vendorId': 5, 'name': 'Hema', 'location': 'Chelmsford',
            'createdOn': '2024-06-19T15:36:02.324032'}

# mongo.insertEmployee(testData)
mongo.findEmployeeById("vendorId", 5)

