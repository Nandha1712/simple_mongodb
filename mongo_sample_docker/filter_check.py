import pymongo
from datetime import datetime

client = pymongo.MongoClient("mongodb://qualdoadmin:12345678@mongodb-service:27017")
qualdodb = client["qualdo"]
print(qualdodb)

collections = qualdodb.list_collection_names()
print(collections)

sampleCol = qualdodb.get_collection("sample")
# Possible units - "hour", "second", "minute"

max_month_close = sampleCol.aggregate(
    [
        # stage 1
        {
            "$match": {
                "$and": [
                    {
                        "date": {"$gt": datetime(1970, 1, 3, 0, 0), "$lt": datetime(1970, 1, 20, 20, 0)},
                         "id_dict.data_set_id": 1,
                         "id_dict.meta_data_id": 41
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
                "avgValue": {"$avg": "$value"},
                "maxDriftValue": {"$max": "$drift_value"},
                "minEpoch": {"$min": "$epoch"}
            }
        },
    ],
allowDiskUse=True
)

print(max_month_close)

for i in max_month_close:
    print(i)
