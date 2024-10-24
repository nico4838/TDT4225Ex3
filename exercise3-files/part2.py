from pprint import pprint
from DbConnector import DbConnector
from datetime import datetime
from haversine import haversine
from collections import defaultdict


def get_labeled_ids():
    """
    :return: A set of Integers correspoding to the users which have Labeled their activities
    """
    file = open("../labeled_ids.txt")
    return set(file.read().splitlines())


class Part2:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def task1(self):
        documents = self.db.Users.aggregate([
            {"$count": "Users count"}
        ])
        for doc in documents:
            pprint(doc)
        documents = self.db.Activities.aggregate([
            {"$count": "Activity count"}
        ])
        for doc in documents:
            pprint(doc)
        documents = self.db.TrackPoints.aggregate([
            {"$count": "TrackPoint count"}
        ])
        for doc in documents:
            pprint(doc)

    def task2(self):
        documents = self.db.Users.aggregate([
            {
                "$group": {
                    "_id": "$_id",
                    "highestActivityCount": {"$max": {"$size": ["$Activities"]}}
                }
            },
            {"$sort": {"highestActivityCount": -1}},
            {"$limit": 1}
        ])
        for doc in documents:
            pprint(doc)

        documents = self.db.Users.aggregate([
            {
                "$group": {
                    "_id": "$_id",
                    "lowestActivityCount": {"$max": {"$size": ["$Activities"]}}
                }
            },
            {"$sort": {"lowestActivityCount": 1}},
            {"$limit": 1}
        ])
        for doc in documents:
            pprint(doc)

        documents = self.db.Users.aggregate([
            {
                "$group": {
                    "_id": 1,
                    "averageActivities": {"$avg": {"$size": ["$Activities"]}}}
            }
        ])

        for doc in documents:
            pprint(doc)

    def task3(self):
        documents = self.db.Users.aggregate([
            {
                "$group": {
                    "_id": "$_id",
                    "activity_count": {"$max": {"$size": ["$Activities"]}}
                }
            },
            {"$sort": {"activity_count": -1}},
            {"$limit": 10}
        ])
        for doc in documents:
            pprint(doc)

    def task4(self):
        documents = self.db.Activities.aggregate([{
            "$match": {
                "$expr": {
                    "$and": [
                        {"$eq": [{"$year": "$start_date_time"}, {"$year": "$end_date_time"}]},
                        {"$eq": [{"$dayOfYear": "$start_date_time"},
                                 {"$subtract": [{"$dayOfYear": "$end_date_time"}, 1]}]}
                    ]
                }
            }
        }, {
            "$group": {
                "_id": "$user_id",
            }},
            {"$count": "distinct_user_id"}
        ])

        for doc in documents:
            pprint(doc)

    def task5(self):
        documents = self.db.Activities.aggregate([
            {"$group": {"_id": {"user_id": "$user_id", "start": "$start_date_time", "end": "$end_date_time"},
                        "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$project": {"_id": 1}},
            {"$group": {"_id": "null", "duplicateActivities": {"$push": "$_id"}}},
            {"$project": {"_id": 0, "duplicateActivities": 1}}
        ])
        for doc in documents:
            pprint(doc)

    def task6(self):
        documents = self.db.TrackPoints.aggregate([
            {
                "$match": {
                    "date_time": {"$gte": datetime.strptime("2008-08-24 15:37:00", "%Y-%m-%d %H:%M:%S"),
                                  "$lte": datetime.strptime("2008-08-24 15:39:00", "%Y-%m-%d %H:%M:%S")}
                }
            }
        ])
        close_activities = set()
        target_pos = (39.97548, 116.33031)
        for doc in documents:
            if doc["activity_id"] in close_activities:
                continue
            distance = haversine(target_pos, (doc["lat"], doc["lon"]), unit="m")
            if distance < 100:
                close_activities.add(doc["activity_id"])
        close_users = []
        for activity_id in close_activities:
            doc = self.db.Activities.find({"_id": activity_id})
            for d in doc:
                close_users.append(d["user_id"])
        print("Close users: {}".format(close_users))

    def task7(self):
        documents = self.db.Activities.aggregate([
            {"$group": {"_id": "$user_id", "transport_mode_list": {"$addToSet": "$transportation_mode"}}},
            {"$match":
                {
                    "$expr":
                        {"$not": {"$in": ["taxi", "$transport_mode_list"]}}}
            },
            {"$project": {"_id": "$_id"}}
        ])
        labeled_ids = get_labeled_ids()
        no_taxi_labeled_ids = [doc["_id"] for doc in documents if doc["_id"] in labeled_ids]
        print(no_taxi_labeled_ids)

    def task8(self):
        documents = self.db.Activities.aggregate([
            {"$match": {"$expr": {"$not": {"$eq": ["$transportation_mode", " "]}}}},
            {"$group": {"_id": "$transportation_mode", "user_id_list": {"$addToSet": "$user_id"}}},
            {"$project": {"_id": 1, "distinct_count": {"$size": "$user_id_list"}}}
        ])
        for doc in documents:
            pprint(doc)

    def task9(self):
        documents = self.db.Activities.aggregate([
            {"$group": {"_id": {"$year": "$start_date_time"}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ])
        for doc in documents:
            pprint(doc)
        documents = self.db.Activities.aggregate([
            {"$group": {"_id": {"year": {"$year": "$start_date_time"}, "month": {"$month": "$start_date_time"}},
                        "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ])
        for doc in documents:
            pprint(doc)
        documents = self.db.Activities.aggregate([
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": [{"$year": "$start_date_time"}, 2008]},
                            {"$eq": [{"$month": "$start_date_time"}, 11]},
                        ]
                    }
                }
            },
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 2}
        ])
        for doc in documents:
            pprint(doc)

    def task10(self):
        documents = self.db.Activities.aggregate([
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$user_id", "112"]},
                            {"$eq": [{"$year": "$start_date_time"}, 2008]},
                            {"$eq": ["$transportation_mode", "walk"]}
                        ]
                    }
                }
            },
            {
                "$lookup": {
                    "from": "TrackPoints",
                    "localField": "trackpoints",
                    "foreignField": "_id",
                    "as": "tp"
                }
            },
            {"$project": {"_id": "$tp"}},
        ])
        km_walked = 0
        for doc in documents:
            temp_sum = 0
            for index in range(0, len(doc['_id']) - 1):
                point1 = (doc['_id'][index]['lat'], doc['_id'][index]['lon'])
                point2 = (doc['_id'][index + 1]['lat'], doc['_id'][index + 1]['lon'])
                temp_sum += haversine(point1, point2)
            km_walked += temp_sum
        print("distance walked by user 112 in 2008: {}".format(km_walked))

    def task11(self):
        documents = self.db.Activities.aggregate([
            {
                "$lookup": {
                    "from": "TrackPoints",
                    "localField": "trackpoints",
                    "foreignField": "_id",
                    "as": "tp"
                }
            },
            {"$project": {"_id": "$user_id", "alt": "$tp.altitude"}},
        ])
        users = defaultdict(int)
        for doc in documents:
            for index in range(1, len(doc["alt"]) - 1):
                alt1 = doc["alt"][index]
                alt2 = doc["alt"][index + 1]
                if alt1 == -777 or alt2 == -777:
                    continue
                if alt1 > alt2:
                    continue
                if doc["_id"] in users:
                    users[doc["_id"]] += alt2 - alt1
                else:
                    users[doc["_id"]] = alt2 - alt1
        pprint(list(sorted(users.items(), key=lambda x: round(x[1], 2), reverse=True)[:20]))

    def task12(self):
        users = defaultdict(int)
        documents = self.db.Activities.aggregate([
            {
                "$lookup": {
                    "from": "TrackPoints",
                    "localField": "trackpoints",
                    "foreignField": "_id",
                    "as": "tp"
                }
            },
            {"$project": {"_id": "$user_id", "date_time": "$tp.date_time"}},
        ])

        for doc in documents:
            for index in range(1, len(doc["date_time"]) - 1):
                time1 = doc["date_time"][index]
                time2 = doc["date_time"][index + 1]
                diff = time2-time1
                if diff.total_seconds() > 300:
                    if doc["_id"] in users:
                        users[doc["_id"]] += 1
                    else:
                        users[doc["_id"]] = 1
                    break
        pprint(dict(sorted(users.items(), key=lambda x: x[1], reverse=True)))
