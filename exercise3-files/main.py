import os
import pandas as pd
from db_utils import DbUtils
from part2 import Part2

db = DbUtils()


def main():
    #db.create_all_colls()
    #walk_dataset()
    p = Part2()
    p.task1()
    p.task2()
    p.task3()
    p.task4()
    p.task5()
    p.task6()
    p.task7()
    p.task8()
    p.task9()
    p.task10()
    p.task11()
    p.task12()


def get_data_from_plt(file_path):
    """
    :param file_path:
    :return: Pandas dataframe containing the data of the given file path
    """
    plt_column_names = ["lat", "lon", "altitude", "date", "time"]
    column_numbers = [0, 1, 3, 5, 6]
    return pd.read_csv(file_path,
                       skiprows=6,
                       names=plt_column_names,
                       usecols=column_numbers
                       )


def get_labeled_ids():
    """
    :return: A set of Integers correspoding to the users which have Labeled their activities
    """
    file = open("labeled_ids.txt")
    return set(file.read().splitlines())


def insert_user(user_id):
    """
    Checks if the given user_id is labeled and inserts it into the database
    :param user_id:
    :return: none
    """
    has_labels = 1 if (user_id in get_labeled_ids()) else 0
    db.insert_users([(user_id, has_labels)])


def get_label_match(user_id):
    """
    :param user_id:
    :return: Returns a dataframe of every label belonging to one user_id.
    """
    label_column_names = ["start_time", "end_time", "label"]

    df = pd.read_csv(
        "Data/" + user_id + "/labels.txt",
        skiprows=1,
        names=label_column_names,
        sep="\t"
    )
    df["timestamp"] = df["start_time"] + " " + df["end_time"]
    del df["start_time"]
    del df["end_time"]
    df["timestamp"] = df["timestamp"].str.replace('/', '-')
    df_condensed = df.set_index(["timestamp"])
    return df_condensed


def walk_dataset():
    """
    Walks through the dataset folder, parses the data and inserts it into the database.
    :return: None
    """
    db = DbUtils()
    labeled_ids = get_labeled_ids()
    user_id = 0
    activity_id = 0
    trackpoint_id = 0
    labels = None
    activities = []
    for root, dirs, files in os.walk("Data/", topdown=True):
        print(root)
        last_3_chars_of_root = root[-3:]
        if last_3_chars_of_root.isdigit():
            labels = None
            user_id = last_3_chars_of_root
            user = {"_id": int(user_id), "has_labels": False}
            if user_id in labeled_ids:
                labels = get_label_match(user_id)
                user["has_labels"] = True

        for file in files:
            label = " "
            file_path = os.path.join(root, file)
            if file[-3:] == "txt":
                continue
            if file[0] == ".":
                continue
            df = get_data_from_plt(file_path)
            df_size = df.shape[0]
            if df_size > 2500:
                continue
            first_point = df.iloc[0]
            last_point = df.iloc[-1]
            first_timestamp = first_point["date"] + " " + first_point["time"]
            last_timestamp = last_point["date"] + " " + last_point["time"]
            if labels is not None:
                x = first_timestamp + " " + last_timestamp
                try:
                    label = labels.loc[x, 'label']
                except KeyError:
                    label = " "
            df["date_time"] = df["date"] + " " + df["time"]
            del df["date"]
            del df["time"]
            df['date_time'] = pd.to_datetime(df['date_time'])

            trackpoint_ids = [*range(trackpoint_id, trackpoint_id+df_size)]
            trackpoint_id += df_size
            df["_id"] = trackpoint_ids
            if label != " ":
                print(label)
                print(activity_id)
            success = db.insert_activity(activity_id, label, first_timestamp, last_timestamp, trackpoint_ids, user_id)
            if success:
                activities.append(activity_id)
                activity_id += 1
                df["activity_id"] = activity_id
                db.insert_trackpoints(df)

        if not last_3_chars_of_root.isdigit() and root != "Data/":
            db.insert_users(user, activities)
            activities = []


if __name__ == '__main__':
    main()
