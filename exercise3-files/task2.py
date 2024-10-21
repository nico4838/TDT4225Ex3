from pymongo import MongoClient
from DbConnector import DbConnector
from pprint import pprint
from bson.son import SON
from datetime import datetime

class Task2:

    def __init__(self):
        self.db_connector = DbConnector()
        self.db = self.db_connector.db

    def close_connection(self):
        self.db_connector.close_connection()

    def task_1(self):
        # Question 1: How many users, activities and trackpoints are there in the dataset
        user_count = self.db.User.count_documents({})
        activity_count = self.db.Activity.count_documents({})
        trackpoint_count = self.db.TrackPoint.count_documents({})

        print(f"Number of users: {user_count}")
        print(f"Number of activities: {activity_count}")
        print(f"Number of trackpoints: {trackpoint_count}")

    def task_2(self):
        # Question 2: Find the average number of activities per user.
        user_count = self.db.User.count_documents({})
        activity_count = self.db.Activity.count_documents({})
        avg_activities = activity_count / user_count if user_count != 0 else 0
        print(f"Average number of activities per user: {avg_activities}")

    def task_3(self):
        # Question 3: Find the top 20 users with the highest number of activities
        top_users = self.db.Activity.aggregate([
            {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
            {"$sort": {"activity_count": -1}},
            {"$limit": 20}
        ])
        pprint(list(top_users))

    def task_4(self):
        # Question 4: Find all users who have taken a taxi
        taxi_users = self.db.Activity.distinct("user_id", {"transportation_mode": "taxi"})
        print("Users who have taken a taxi:")
        pprint(taxi_users)

    def task_5(self):
        # Question 5: Find all types of transportation modes and count activities for each mode
        transport_modes = self.db.Activity.aggregate([
            {"$match": {"transportation_mode": {"$ne": ""}}},
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}}
        ])
        print("Transportation modes and activity counts:")
        pprint(list(transport_modes))

    def task_6(self):
        # Question 6a: Find the year with the most activities
        year_activities = self.db.Activity.aggregate([
            {"$group": {"_id": {"$year": "$start_date_time"}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ])
        most_active_year = list(year_activities)
        print("Year with the most activities:")
        pprint(most_active_year)

        # Question 6b: Is this also the year with the most recorded hours?
        year_hours = self.db.Activity.aggregate([
            {"$project": {
                "year": {"$year": "$start_date_time"},
                "duration_hours": {"$divide": [{"$subtract": ["$end_date_time", "$start_date_time"]}, 3600000]}
            }},
            {"$group": {"_id": "$year", "total_hours": {"$sum": "$duration_hours"}}},
            {"$sort": {"total_hours": -1}},
            {"$limit": 1}
        ])
        most_recorded_hours_year = list(year_hours)
        print("Year with the most recorded hours:")
        pprint(most_recorded_hours_year)

    def task_7(self):
        # Question 7: Total distance walked in 2008 by user with id=112
        user_id = "112"
        activities = self.db.Activity.find({"user_id": user_id, "transportation_mode": "walk", "$expr": {"$eq": [{"$year": "$start_date_time"}, 2008]}})
        
        total_distance = 0
        for activity in activities:
            trackpoints = list(self.db.TrackPoint.find({"activity_id": activity["_id"]}).sort("date_time"))
            for i in range(1, len(trackpoints)):
                lat1, lon1 = trackpoints[i-1]["lat"], trackpoints[i-1]["lon"]
                lat2, lon2 = trackpoints[i]["lat"], trackpoints[i]["lon"]
                total_distance += self.haversine(lat1, lon1, lat2, lon2)

        print(f"Total distance walked by user {user_id} in 2008: {total_distance} km")

    def task_8(self):
        # Question 8: Top 20 users with the most altitude gain
        altitude_gain = self.db.TrackPoint.aggregate([
            {"$group": {
                "_id": "$user_id",
                "total_altitude_gain": {
                    "$sum": {
                        "$cond": [
                            {"$gt": [{"$subtract": ["$altitude", {"$ifNull": ["$previous_altitude", 0]}]}, 0]},
                            {"$subtract": ["$altitude", "$previous_altitude"]},
                            0
                        ]
                    }
                }
            }},
            {"$sort": {"total_altitude_gain": -1}},
            {"$limit": 20}
        ])
        print("Top 20 users by altitude gain:")
        pprint(list(altitude_gain))

    def task_9(self):
        # Question 9: Users with invalid activities
        invalid_activities = self.db.Activity.aggregate([
            {"$lookup": {
                "from": "TrackPoint",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "trackpoints"
            }},
            {"$unwind": "$trackpoints"},
            {"$group": {
                "_id": {"user_id": "$user_id", "activity_id": "$_id"},
                "max_timestamp_diff": {"$max": {"$subtract": [{"$millisecond": "$trackpoints.date_time"}, {"$millisecond": "$previous_trackpoint.date_time"}]}},
            }},
            {"$match": {"max_timestamp_diff": {"$gt": 300000}}},
            {"$group": {"_id": "$_id.user_id", "invalid_count": {"$sum": 1}}}
        ])
        print("Users with invalid activities:")
        pprint(list(invalid_activities))

    def task_10(self):
        # Question 10: Users with activity in Forbidden City
        forbidden_city_activities = self.db.TrackPoint.aggregate([
            {"$match": {
                "lat": {"$eq": 39.916},
                "lon": {"$eq": 116.397}
            }},
            {"$lookup": {
                "from": "Activity",
                "localField": "activity_id",
                "foreignField": "_id",
                "as": "activity"
            }},
            {"$unwind": "$activity"},
            {"$group": {"_id": "$activity.user_id"}}
        ])
        print("Users with activity in the Forbidden City:")
        pprint(list(forbidden_city_activities))

    def task_11(self):
        # Question 11: Users with registered transportation modes and their most used mode
        most_used_modes = self.db.Activity.aggregate([
            {"$match": {"transportation_mode": {"$ne": ""}}},
            {"$group": {"_id": {"user_id": "$user_id", "mode": "$transportation_mode"}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$group": {"_id": "$_id.user_id", "most_used_mode": {"$first": "$_id.mode"}}},
            {"$sort": {"_id": 1}}
        ])
        print("Most used transportation mode per user:")
        pprint(list(most_used_modes))

    def haversine(self, lat1, lon1, lat2, lon2):
        # Calculate distance between two lat/lon points in km using Haversine formula
        from math import radians, cos, sin, sqrt, atan2

        R = 6371.0  # Earth radius in km

        lat1, lon1, lat2, lon2 = radians(lat1), radians(lon1), radians(lat2), radians(lon2)
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return R * c

if __name__ == '__main__':
    task = Task2()

    try:
        task.task_1()
        task.task_2()
        task.task_3()
        task.task_4()
        task.task_5()
        task.task_6()
        task.task_7()
        task.task_8()
        task.task_9()
        task.task_10()
        task.task_11()

    finally:
        task.close_connection()
