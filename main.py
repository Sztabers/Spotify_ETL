import sqlalchemy
import pandas as pd
import sqlite3

from os import access


DB_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "ji3emja5ktfuue1z1j6l1jyt9"
TOKEN = 'BQAxzXhupu8Aq68qnXd6XGkhmZ3oKFF5R6AbkiwaZWjfWS8wka4WaKl_H'

if __name__ == '__main__':

    headers = {
        'Accept' : 'application/json',
        'Content-Type' : 'application/json',
        'Authorization' : f'Bearer {TOKEN}'
    }


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
    





    