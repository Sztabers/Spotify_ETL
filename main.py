import sqlalchemy
import pandas as pd
import sqlite3
import requests
import json
import datetime
import pandas as pd

from os import access


DB_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "Sztaber"
TOKEN = 'BQC2BaRBT6d4rH0NIh-SN2H6wz12g-7pmV1PGYVRrMtuAjYgsQzHu57ZjnUK-KaQw-hn6G0i7rPhoLlHrn17Mv_LB2zF-H7V0VcKvFhn1mePXDm-Yi07hDgncORDzzjMZg5bacqmwB9Ko3MXdDrUrJ-bVA2nJzXg94GbR5H2'

if __name__ == '__main__':

#Extract the data from Spotify API

    headers = {
        'Accept' : 'application/json',
        'Content-Type' : 'application/json',
        'Authorization' : f'Bearer {TOKEN}'
    }

    today = datetime.datetime.now()
    time_range = today - datetime.timedelta(days=7)
    time_range_unix = int(time_range.timestamp()) * 1000

    # Get played songs from the last 7 days
    r = requests.get(f'https://api.spotify.com/v1/me/player/recently-played?after={time_range_unix}', headers=headers)

    data = r.json()




    # Put the data into dataframe

    songs_names = []
    artists_names = []
    played_at_list = []
    timestamps_list = []

    for song in data['items']:
        songs_names.append(song['track']['name'])
        artists_names.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps_list.append(song['played_at'][0:10])

    song_dict = {
        'song_name' : songs_names,
        'artist_name' : artists_names,
        'played_at' : played_at_list,
        'timestamps' : timestamps_list 
     }

    song_df = pd.DataFrame(song_dict, columns=['song_name', 'artist_name', 'played_at', 'timestamps'])

    print(song_df)



    





    engine = sqlalchemy.create_engine(DB_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks (
        song_name VARCHAR(200),
        artist_name VARCAHR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Database opened successfully!")
        





    