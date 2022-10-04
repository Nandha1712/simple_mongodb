import pymongo
from datetime import datetime

client = pymongo.MongoClient("mongodb://qualdoadmin:12345678@mongodb-service:27017")
qualdodb = client["qualdo"]
print(qualdodb)

collections = qualdodb.list_collection_names()
print(collections)

sampleCol = qualdodb.get_collection("sample")

print(" ============== Insert queries section starts ============")
new_data = {
    "date": datetime.utcnow(),
    "id_dict": {"data_set_id": 1, "metric_id": 2, "meta_data_id": 3, "symbol": "Custom"},
    "value": 60.357498,
    "drift_value": 80.144997
}

cr = sampleCol.insert_one(new_data)
print(cr)
print(cr.inserted_id)
print(cr.acknowledged)
