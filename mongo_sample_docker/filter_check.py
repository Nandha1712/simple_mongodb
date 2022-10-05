import pymongo
from datetime import datetime

client = pymongo.MongoClient("mongodb://qualdoadmin:12345678@mongodb-service:27017")
qualdodb = client["qualdo"]
print(qualdodb)

collections = qualdodb.list_collection_names()
print(collections)

sampleCol = qualdodb.get_collection("sample")

max_month_close = sampleCol.aggregate(
    [
        # stage 1
        {
            "$match": {
                "$and": [
                    {
                        "date": {"$gt": datetime(2022, 10, 3, 20, 0), "$lt": datetime(2022, 10, 6, 20, 0)},
                         "id_dict.data_set_id": 1
                    }
                ]
            }
        },

        # stage 2
        {
            "$group": {
                "_id": {
                    "timeunit": {"$dateTrunc": {"date": "$date", "unit": "hour"}},
                    "id_dict": "$id_dict",
                },
                "avgHourValue": {"$avg": "$value"},
                "avgHourDriftValue": {"$avg": "$drift_value"},
            }
        },
    ]
)

print(max_month_close)

for i in max_month_close:
    print(i)
