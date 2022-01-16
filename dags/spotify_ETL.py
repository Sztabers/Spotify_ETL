from urllib import request
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3




def check_id_data_validate(df: pd.DataFrame) -> bool:
    # check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Primary Key check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")
    
    # check for nulls 
    if df.isnull().values.any():
        raise Exception("Null Value Exception")

    #check that all the timestamps are of yesterday's date
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0,minute=0,second=0, microsecond=0)

    # timestamps = df["timestamp"].tolist()
    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
    #         raise Exception("at least one of returned songs does not come from the last 24 hours ")
    return True



def run_spotify_etl():
    DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
    USER_ID = "Sztaber"
    TOKEN = "BQBqEDo984gI2OOPHwZJKgeX7C7_-wxFBZdwAbwLaqca6F2HtPf4OzGPRJs2ESo3BQsrwxHV-XsddHNh8UTw2GB6JbdDBGZM9WeiccSFCwVoyxdc9aA32yZoERcQwq5RS9SPX8joGUTa8-IeoE9RKxsXa4yrtzLW39c_9lfX"
    # Extract part of the ETL process
 
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    
    # Convert time to Unix timestamp in miliseconds      
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    # today_unix_timestamp = int(today.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours      
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)


    data = r.json()

  

   

    songs_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        songs_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])


    song_dict = {
        "song_name" : songs_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps}

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    print(song_df)


    # Validate:

    if check_id_data_validate(song_df):
        print("Data is valid. Proceed to Load stage")


    # Load:

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()


    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        ) """



    cursor.execute(sql_query)
    print("Opened data successfully")


    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")