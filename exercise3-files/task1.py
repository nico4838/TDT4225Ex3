import os
from pymongo import MongoClient
from DbConnector import DbConnector
from pprint import pprint
from datetime import datetime
import pandas as pd
from bson.objectid import ObjectId

class GeolifeInserter:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.labels_cache = {}

    def create_collections(self):
        self.db.create_collection("User", validator={"$jsonSchema": {"bsonType": "object"}})
        self.db.create_collection("Activity", validator={"$jsonSchema": {"bsonType": "object"}})
        self.db.create_collection("TrackPoint", validator={"$jsonSchema": {"bsonType": "object"}})
        print('Collections created: User, Activity, TrackPoint')

    def insert_geolife_data(self, dataset_path):
        count = 0
        for user_dir in os.listdir(dataset_path):
            user_path = os.path.join(dataset_path, user_dir)
            if os.path.isdir(user_path): 
                user_id = user_dir  
                self.insert_user(user_id)
                count += 1
                print(f'User {count} inserted')
                trajectory_path = os.path.join(user_path, 'Trajectory')
                for plt_file in os.listdir(trajectory_path):
                    if plt_file.endswith('.plt'):
                        plt_file_path = os.path.join(trajectory_path, plt_file)
                        self.insert_activity(user_id, plt_file_path)
                        
    
    def insert_user(self, user_id):
        user_doc = {"_id": user_id, "has_labels": False} 
        labels_path = f"./dataset/dataset/Data/{user_id}/labels.txt"
        if os.path.exists(labels_path):
            user_doc["has_labels"] = True
        self.db["User"].insert_one(user_doc)


    def load_labels(self, user_id):
        self.labels_cache = {}

        labels_path = f"./dataset/dataset/Data/{user_id}/labels.txt"
        if os.path.exists(labels_path):
            with open(labels_path, 'r') as file:
                next(file)
                for line in file:
                    parts = line.strip().split('\t')
                    if len(parts) == 3:
                        start_time = parts[0].strip().replace('/','-')  
                        end_time = parts[1].strip().replace('/','-')  
                        transportation_mode = parts[2].strip()  

                        self.labels_cache[(start_time, end_time)] = transportation_mode

    def find_transportation_label(self, start_date_time, end_date_time):
        return self.labels_cache.get((start_date_time, end_date_time), None)
    
    def get_data_from_plt(self, plt_file_path):


        plt_column_names = ["lat", "lon", "altitude", "date_days", "date", "time"]
        column_numbers = [0, 1, 3, 4, 5, 6]
        return pd.read_csv(plt_file_path,
                        skiprows=6,
                        names=plt_column_names,
                        usecols=column_numbers
                        )
        
    def insert_activity(self, user_id, plt_file_path):
        self.load_labels(user_id)
        
        trackpoints = []

        with open(plt_file_path, 'r') as file:
            plt_column_names = ["lat", "lon", "altitude", "date_days", "date", "time"]
            column_numbers = [0, 1, 3, 4, 5, 6]
            tp = pd.read_csv(plt_file_path,skiprows=6, names=plt_column_names, usecols=column_numbers)
            tp_size = tp.shape[0]
            if tp_size <= 2500:

                first_point = tp.iloc[0]
                last_point = tp.iloc[-1]
                first_time = first_point['date'] + ' ' + first_point['time']
                last_time = last_point['date'] + ' ' + last_point['time']
                transportation_mode = self.find_transportation_label(first_time, last_time)
                tp['date_time'] = tp['date'] + ' ' + tp['time']
                tp['date_days']
                del tp['date']
                del tp['time']
       
    
                tp['_id'] = [str(ObjectId()) for _ in range(tp_size)] 
                activity_id = ObjectId() 
                tp['activity_id'] = activity_id 

                trackpoint_ids = tp['_id'].tolist()

                trackpoint_ids = tp['_id'].tolist()

                activity_doc = {
                    "_id": activity_id,
                    "user_id": user_id,
                    "start_date_time": first_time,
                    "end_date_time": last_time,
                    "transportation_mode": transportation_mode,
                    "trackpoints": trackpoint_ids
                }
                
                self.db["Activity"].insert_one(activity_doc)
                print('Activity inserted')
                tf_dict = tp.to_dict("records")
                collection = self.db["TrackPoint"]
                collection.insert_many(tf_dict)
                print('Trackpoints inserted')


    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        
    def drop_collection(self, collection_name):
        self.db[collection_name].drop()
        print(f"Dropped collection: {collection_name}")

    def close_connection(self):
        self.client.close()

def main():
    try:

        dataset_path = "./dataset/dataset/Data" 

        inserter = GeolifeInserter()
        inserter.create_collections()
        print('Collections created')
        inserter.insert_geolife_data(dataset_path)
        print('Data inserted')
        
        #inserter.fetch_documents("User")
        #inserter.fetch_documents("Activity")

        # Drop collections if needed (optional)
        # inserter.drop_collection("User")
        # inserter.drop_collection("Activity")
        # inserter.drop_collection("TrackPoint")

    except Exception as e:
        print(f"ERROR: Failed to use database: {e}")
    finally:
        inserter.close_connection()

if __name__ == '__main__':
    main()