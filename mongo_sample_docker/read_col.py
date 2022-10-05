import pymongo
from datetime import datetime

client = pymongo.MongoClient("mongodb://qualdoadmin:12345678@mongodb-service:27017")
qualdodb = client["qualdo"]
print(qualdodb)

collections = qualdodb.list_collection_names()
print(collections)

sampleCol = qualdodb.get_collection("sample")

print(sampleCol)

# avg_hour_close = sampleCol.aggregate(
#     [
#         # stage 1
#         {
#             "$group": {
#                 "_id": {
#                     "timeunit": {"$dateTrunc": {"date": "$date", "unit": "hour"}},
#                     "id_dict": "$id_dict",
#                 },
#                 "avgHourValue": {"$avg": "$value"},
#                 "avgHourDriftValue": {"$avg": "$drift_value"},
#             }
#         },
#     ]
# )

avg_hour_close = sampleCol.find({})


print(avg_hour_close)

for i in avg_hour_close:
    print(i)
