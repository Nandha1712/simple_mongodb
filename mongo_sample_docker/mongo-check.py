import pymongo
client = pymongo.MongoClient("mongodb://qualdoadmin:12345678@mongodb-service:27017")
qualdodb=client["qualdo"]
print(qualdodb)

collections = qualdodb.list_collection_names()
print(collections)
