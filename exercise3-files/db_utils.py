from src.DbConnector import DbConnector
from datetime import datetime


class DbUtils:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def create_all_colls(self):
        self.drop_all_colls()
        try:
            self.create_coll("Users")
            self.create_coll("Activities")
            self.create_coll("TrackPoints")
        except Exception as e:
            print("ERROR while accessing database", e)

    def drop_all_colls(self):
        try:
            self.drop_coll("Users")
            self.drop_coll("Activities")
            self.drop_coll("TrackPoints")
        except Exception as e:
            print("ERROR while accessing database", e)

    def insert_users(self, user, activities):
        collection = self.db["Users"]
        user["Activities"] = activities
        collection.insert_one(user)

    def insert_activity(self, activity_id, label, start_time, end_time, trackpoint_ids, user_id):
        activity = {
            "_id": activity_id,
            "user_id": user_id,
            "transportation_mode": label,
            "start_date_time": datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S"),
            "end_date_time": datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S"),
            "trackpoints": trackpoint_ids
        }
        collection = self.db["Activities"]
        try:
            collection.insert_one(activity)
            return True
        except Exception as E:
            return False

    def insert_trackpoints(self, df):
        trackpoints = df.to_dict("records")
        collection = self.db["TrackPoints"]
        collection.insert_many(trackpoints)
