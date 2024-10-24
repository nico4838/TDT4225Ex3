import os
from pymongo import MongoClient
from DbConnector import DbConnector
from pprint import pprint
from datetime import datetime
import pandas as pd
from bson.objectid import ObjectId

class GeolifeInserter:

    def __init__(self):
        # Connect to MongoDB
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.labels_cache = {}  # Cache for labels.txt files

    def create_collections(self):
        # Create collections if they don't already exist
        self.db.create_collection("User", validator={"$jsonSchema": {"bsonType": "object"}})
        self.db.create_collection("Activity", validator={"$jsonSchema": {"bsonType": "object"}})
        self.db.create_collection("TrackPoint", validator={"$jsonSchema": {"bsonType": "object"}})
        print('Collections created: User, Activity, TrackPoint')

    def insert_geolife_data(self, dataset_path):
        # Iterate through the users
        count = 0
        for user_dir in os.listdir(dataset_path):
            user_path = os.path.join(dataset_path, user_dir)
            if os.path.isdir(user_path):  # Ensure it's a directory
                user_id = user_dir  # This is the user ID (e.g., "000")
                self.insert_user(user_id)
                count += 1
                print(f'User {count} inserted')
                # Now go into the Trajectory subfolder for each user
                trajectory_path = os.path.join(user_path, 'Trajectory')
                for plt_file in os.listdir(trajectory_path):
                    if plt_file.endswith('.plt'):
                        plt_file_path = os.path.join(trajectory_path, plt_file)
                        self.insert_activity(user_id, plt_file_path)
                        
    
    def insert_user(self, user_id):
        # Insert a new User document in the MongoDB User collection
        user_doc = {"_id": user_id, "has_labels": False}  # Modify this as needed
        # If labels.txt exists, set has_labels to True
        labels_path = f"./dataset/dataset/Data/{user_id}/labels.txt"
        if os.path.exists(labels_path):
            user_doc["has_labels"] = True
        self.db["User"].insert_one(user_doc)


    def load_labels(self, user_id):
        # Clear the cache for each new user
        self.labels_cache = {}

        labels_path = f"./dataset/dataset/Data/{user_id}/labels.txt"
        if os.path.exists(labels_path):
            with open(labels_path, 'r') as file:
                # Skip header if there's any
                next(file)
                # Populate the cache (a dictionary with (start_time, end_time) as key and mode as value)
                for line in file:
                    parts = line.strip().split('\t')
                    if len(parts) == 3:
                        start_time = parts[0].strip().replace('/','-')  # Start Time
                        end_time = parts[1].strip().replace('/','-')    # End Time
                        transportation_mode = parts[2].strip()  # Transportation Mode
                        # Store a tuple (start_time, end_time) as the key in the cache
                        self.labels_cache[(start_time, end_time)] = transportation_mode
                        #print(f"Loaded label: {start_time} - {end_time} -> {transportation_mode}")

    def find_transportation_label(self, start_date_time, end_date_time):
        # Check for an exact match in the (start_time, end_time) key
        return self.labels_cache.get((start_date_time, end_date_time), None)
    
    def get_data_from_plt(self, plt_file_path):
        #param file_path:
        # return: Pandas dataframe containing the data of the given file path

        plt_column_names = ["lat", "lon", "altitude", "date_days", "date", "time"]
        column_numbers = [0, 1, 3, 4, 5, 6]
        return pd.read_csv(plt_file_path,
                        skiprows=6,
                        names=plt_column_names,
                        usecols=column_numbers
                        )
        
    def insert_activity(self, user_id, plt_file_path):
        # Load the labels file into memory for the new user (cache is cleared here)
        self.load_labels(user_id)
        
        trackpoints = []

        with open(plt_file_path, 'r') as file:
            #lines = file.readlines()[6:]  # Skip the first 6 metadata lines
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
                # Trackpoint id
                
                # Generate unique _id for each trackpoint and activity_id for reference
                tp['_id'] = [str(ObjectId()) for _ in range(tp_size)]  # Generate unique IDs for trackpoints
                activity_id = ObjectId()  # Generate a unique ObjectId for the activity
                tp['activity_id'] = user_id  # Set activity_id for all trackpoints

                trackpoint_ids = tp['_id'].tolist()



                # tp['_id'] = user_id + '_' + tp['date_time'] 
                # tp['activity_id'] = user_id 
                
                trackpoint_ids = tp['_id'].tolist()

                activity_doc = {
                    "user_id": user_id,
                    "activity_id": activity_id,
                    "start_date_time": first_time,
                    "end_date_time": last_time,
                    "transportation_mode": transportation_mode,
                    "trackpoints": trackpoint_ids
                }
                
                # Insert activity
                self.db["Activity"].insert_one(activity_doc)
                print('Activity inserted')
                # Insert trackpoints
                tf_dict = tp.to_dict("records")
                collection = self.db["TrackPoint"]
                collection.insert_many(tf_dict)
                print('Trackpoints inserted')

            # if len(lines) <= 2500:  # Only process activities with <= 2500 trackpoints
            #     for line in lines:
            #         lat, lon, _, altitude, date_days, date_str, time_str = line.strip().split(',')
                    
            #         # Create a dictionary for each trackpoint
            #         trackpoint = {
            #             "lat": float(lat),
            #             "lon": float(lon),
            #             "altitude":float(altitude),
            #             "date_days": float(date_days),
            #             "date_time": f"{date_str} {time_str}"  # Combine date and time
            #         }
            #         trackpoints.append(trackpoint)

            #     # Determine the transportation mode for the activity
            #     print('Start date time: '+ trackpoints[0]["date_time"])
            #     print('End date time: '+ trackpoints[-1]["date_time"])

            #     transportation_mode = self.find_transportation_label(trackpoints[0]["date_time"], trackpoints[-1]["date_time"])
            #     if transportation_mode != None:
            #         print('Transportation mode: '+ transportation_mode)
            #     # Create and insert the Activity document
            #     activity_doc = {
            #         "user_id": user_id,
            #         "start_date_time": trackpoints[0]["date_time"],
            #         "end_date_time": trackpoints[-1]["date_time"],
            #         "transportation_mode": transportation_mode,
            #         "trackpoints": trackpoints  # Store all trackpoints within the activity
            #     }

                #self.db["Activity"].insert_one(activity_doc)

                # Insert trackpoints as separate documents in the TrackPoint collection
                #self.db["TrackPoint"].insert_many(trackpoints)  # Bulk insert

    def fetch_documents(self, collection_name):
        # Fetch and print all documents from a collection
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        
    def drop_collection(self, collection_name):
        # Drop a collection if needed
        self.db[collection_name].drop()
        print(f"Dropped collection: {collection_name}")

    def close_connection(self):
        # Close the MongoDB connection
        self.client.close()

def main():
    try:

        dataset_path = "./dataset/dataset/Data" 

        inserter = GeolifeInserter()
        inserter.create_collections()
        print('Collections created')
        inserter.insert_geolife_data(dataset_path)
        print('Data inserted')

        # Optionally, you can fetch and display the documents to verify insertion
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