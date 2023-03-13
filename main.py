import pymongo
from bson.json_util import dumps


conn = pymongo.MongoClient('mete tu url')
db = conn['tfg']
coll = db['users']

pipeline = [
    {"$match": {"numReviewsEnBD": {"$gte": 20}}},
    {"$group": {"_id": "null", "usersIds": {"$push": "$reviewerId"}}}
]
indices = 1
user = coll.aggregate(pipeline).next()
users = coll.find({"reviewerId": {"$in": user.get("usersIds")}}, {"reviewerId": 1, "id": {"$sum": 1}, "_id": 0})
list_cur1 = list(users)

coll = db['reviews']
reviews = coll.find({"reviewerId": {"$in": user.get("usersIds")}},
                    {"reviewerId": 1, "placeId": 1, "stars": 1, "_id": 0, "idUsuario": {"$sum": 1},
                     "idRestaurante": {"$sum": 1}})
list_cur2 = list(reviews)

for e in list_cur1:
    e["id"] = indices
    indices = indices + 1
    for b in list_cur2:
        if b["reviewerId"] == e["reviewerId"]:
            b["idUsuario"] = e["id"]

restaurantsIds = reviews.distinct("placeId")
coll = db['restaurants']
restaurants = coll.find({"place_id": {"$in": restaurantsIds}}, {"place_id": 1, "name": 1, "id": {"$sum": 1}, "_id": 0})
list_cur3 = list(restaurants)
indices = 1
for e in list_cur3:
    e["id"] = indices
    indices = indices + 1
    for b in list_cur2:
        if b["placeId"] == e["place_id"]:
            b["idRestaurante"] = e["id"]

json_users = dumps(list_cur1)
json_restaurants = dumps(list_cur3)
json_reviews = dumps(list_cur2)

with open('restaurants.json', 'w') as f:
    f.write(json_restaurants)
with open('reviews.json', 'w') as f:
    f.write(json_reviews)
with open('users.json', 'w') as f:
    f.write(json_users)
