import pymongo
from datetime import datetime

client = pymongo.MongoClient("mongodb://qualdoadmin:12345678@mongodb-service:27017")
qualdodb=client["qualdo"]
print(qualdodb)

collections = qualdodb.list_collection_names()
print(collections)

dowJonesTickerData = qualdodb.get_collection("dowJonesTickerData")

max_month_close = dowJonesTickerData.aggregate([
    ## stage 1
    {
        "$group" : 
                 {
				 "_id" : {"timeunit" : { "$dateTrunc": { "date": "$date", "unit": "month" } }, "symbol": "$symbol" },
				 "maxMonthClose": { "$max": "$close" }
				 }
    },

])


print(max_month_close)

for i in max_month_close:
    print(i)

print("\n\n\n\n========Next set of queries============\n\n\n\n\n\n\n")

avg_hour_close = dowJonesTickerData.aggregate([
    ## stage 1
    {
        "$group" : 
                 {
				 "_id" : {"timeunit" : { "$dateTrunc": { "date": "$date", "unit": "hour" } }, "symbol": "$symbol" },
				 "avgHourClose": { "$avg": "$close" }
				 }
    },

])


print(avg_hour_close)

for i in avg_hour_close:
    print(i)

# ============== Insert queries section ============

print(" ============== Insert queries section starts ============")
new_data =   {
    "date": datetime.utcnow(),
    "symbol": 'Custom',
    "close": 68.357498,
    "high": 84.144997,
    "adjClose": 75.486023,
    "open": 65.287498,
    "volume": 146322800,
    "low": 72.125
  }

dowJonesTickerData.insert_one(new_data)

print(" ============== Insert queries section ends ============")