import pymongo
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from tabulate import tabulate

class DbConnector:
    """
    Connects to the MongoDB server.
    """
    def __init__(self, HOST="localhost", PORT=27017, DATABASE="my_database"):
        try:
            # Connect to the MongoDB server
            self.client = MongoClient(HOST, PORT)
            self.db = self.client[DATABASE]
            print("Connected to MongoDB")

        except Exception as e:
            print("ERROR: Failed to connect to db:", e)
            self.db = None

    def close_connection(self):
        if self.client:
            self.client.close()
            print("Connection to MongoDB closed.")
        else:
            print("No connection to close.")

    def count_entries(self):
        # Collect results in an array
        results = []

        # Query to count users
        user_count = self.db['User'].count_documents({})
        results.append(("User Count", user_count))

        # Query to count activities
        activity_count = self.db['Activity'].count_documents({})
        results.append(("Activity Count", activity_count))

        # Query to count trackpoints
        trackpoint_count = self.db['TrackPoint'].count_documents({})
        results.append(("TrackPoint Count", trackpoint_count))

        # Print the results using tabulate for a clean output
        print(tabulate(results, headers=["Type", "Count"], tablefmt="fancy_grid"))

    def print_csv_samples(self):
        # Paths to your CSV files
        activity_csv_path = '/path/to/activity.csv'
        user_csv_path = '/path/to/user.csv'
        trackpoint_csv_path = '/path/to/trackpoint2.csv'

        # Load the CSV files using pandas
        activity_df = pd.read_csv(activity_csv_path)
        user_df = pd.read_csv(user_csv_path)
        trackpoint_df = pd.read_csv(trackpoint_csv_path)

        # Print the first 10 rows of each CSV file
        print("First 10 rows from activity.csv:")
        print(activity_df.head(10))
        print("\n")

        print("First 10 rows from user.csv:")
        print(user_df.head(10))
        print("\n")

        print("First 10 rows from trackpoint2.csv:")
        print(trackpoint_df.head(10))

    def create_collections(self):
        if self.db is None:
            print("Cannot create collections because the connection to the database failed.")
            return

        # MongoDB collections are created automatically upon the first insert, so no need for explicit table creation
        print("Collections ready to be used in MongoDB.")

    def clear_collections(self):
        if self.db is None:
            print("Cannot clear collections because the connection to the database failed.")
            return

        self.db['TrackPoint'].delete_many({})
        self.db['Activity'].delete_many({})
        self.db['User'].delete_many({})
        print("Collections cleared successfully.")


def load_data(db_connector):
    # Load users
    user_df = pd.read_csv('dataset/user.csv')
    user_data = user_df.to_dict(orient='records')
    db_connector.db['User'].insert_many(user_data)
    print("Inserted users.")

    # Load activities
    activity_df = pd.read_csv('dataset/activity.csv')
    activity_data = []
    for _, row in activity_df.iterrows():
        activity_data.append({
            "id": row['id'],
            "user_id": row['user_id'],
            "transportation_mode": row['transportation_mode'] if pd.notnull(row['transportation_mode']) else None,
            "start_date_time": datetime.strptime(row['start_date_time'], '%Y/%m/%d %H:%M:%S'),
            "end_date_time": datetime.strptime(row['end_date_time'], '%Y/%m/%d %H:%M:%S')
        })
    db_connector.db['Activity'].insert_many(activity_data)
    print("Inserted activities.")

    # Load trackpoints
    trackpoint_df = pd.read_csv('dataset/trackpoint2.csv')
    trackpoint_data = []
    for _, row in trackpoint_df.iterrows():
        trackpoint_data.append({
            "activity_id": row['activity_id'],
            "lat": row['lat'],
            "lon": row['lon'],
            "altitude": row['altitude'],
            "date_days": row['date_days'],
            "date_time": datetime.strptime(row['date_time'], '%Y-%m-%d %H:%M:%S')
        })
    db_connector.db['TrackPoint'].insert_many(trackpoint_data)
    print("Inserted trackpoints.")

def main():
    db_connector = DbConnector()
    db_connector.print_csv_samples()
    db_connector.count_entries()

    # Example queries in MongoDB
    print("Top 20 users with the highest number of activities:")
    top_users = db_connector.db['Activity'].aggregate([
        {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
        {"$sort": {"activity_count": -1}},
        {"$limit": 20}
    ])
    print(tabulate([(x['_id'], x['activity_count']) for x in top_users], headers=["User ID", "Activity Count"], tablefmt="fancy_grid"))

if __name__ == "__main__":
    main()
