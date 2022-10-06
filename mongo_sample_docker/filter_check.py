import pymongo
from datetime import datetime

start_time0 = datetime.utcnow()
client = pymongo.MongoClient("mongodb://qualdoadmin:12345678@mongodb-service:27017")
qualdodb = client["qualdo"]
print(qualdodb)
end_time0 = datetime.utcnow()
time_taken0 = (end_time0 - start_time0).total_seconds()
print(f"time taken0: {time_taken0} seconds")

start_time01 = datetime.utcnow()
collections = qualdodb.list_collection_names()
print(collections)
end_time01 = datetime.utcnow()
time_taken01 = (end_time01 - start_time01).total_seconds()
print(f"time taken01: {time_taken01} seconds")

start_time02 = datetime.utcnow()
sampleCol = qualdodb.get_collection("sample")
# Possible units - "hour", "second", "minute", "month"

end_time02 = datetime.utcnow()
time_taken02 = (end_time02 - start_time02).total_seconds()
print(f"time taken02: {time_taken02} seconds")


start_time = datetime.utcnow()
max_month_close = sampleCol.aggregate(
    [
        # stage 1
        {
            "$match": {
                "$and": [
                    {
                        "date": {"$gt": datetime(1970, 1, 3, 0, 0), "$lt": datetime(1971, 1, 6, 1, 3)},
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
                    "timeunit": {"$dateTrunc": {"date": "$date", "unit": "month"}},
                    "id_dict": "$id_dict",
                },
                "avgValue": {"$avg": "$value"},
                "maxDriftValue": {"$max": "$drift_value"},
                "minEpoch": {"$min": "$epoch"},
            }
        },

        # stage 3 - sorting results
        { "$sort" : { "_id.timeunit" : 1} },

        # Stage 4 - limit
        { "$limit" : 5 }
    ],
allowDiskUse=True
)

print(max_month_close)

end_time = datetime.utcnow()
time_taken = (end_time - start_time).total_seconds()
print(f"time taken: {time_taken} seconds")

start_time2 = datetime.utcnow()
for i in max_month_close:
    print(i)
end_time2 = datetime.utcnow()
time_taken2 = (end_time2 - start_time2).total_seconds()
print(f"time taken2 for loop: {time_taken2} seconds")


time_taken_total = (end_time2 - start_time0).total_seconds()
print(f"Total time taken: {time_taken_total} seconds")