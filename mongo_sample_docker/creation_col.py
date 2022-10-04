import pymongo
from datetime import datetime

client = pymongo.MongoClient("mongodb://qualdoadmin:12345678@mongodb-service:27017")
qualdodb = client["qualdo"]
print(qualdodb)

collections = qualdodb.list_collection_names()
print(collections)

print(" ============== Insert queries section ends ============")


# ============ Creating time series collections =======
create_res = qualdodb.create_collection(
    "sample",
    timeseries={"timeField": "date", "metaField": "", "granularity": "seconds"},
)
