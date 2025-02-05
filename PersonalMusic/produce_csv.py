import json 
import pandas as pd
import glob
import os

path = os.getcwd()

# Load all StreamingHistory JSON files into a single DataFrame
files = glob.glob(path +"/PersonalMusic/data/streaming_history/*Streaming_History*.json")
dfs = []

for file in files:
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
        dfs.append(pd.DataFrame(data))
        

df = pd.concat(dfs, ignore_index=True)


# Data additions/changes
df = df.rename(
    columns={
        'master_metadata_track_name': 'track_name',
        'master_metadata_album_artist_name': 'artist_name', 
        'master_metadata_album_album_name': 'album_name'
        })

df['minutes_played'] = df['ms_played']/60000
df['yearlyquarter'] = pd.PeriodIndex(df.ts, freq='Q').astype(str)
df['year'] = df['yearlyquarter'].str[:4]  # Extract the first 4 characters for the year
df['quarter'] = df['yearlyquarter'].str[-1]  # Extract the last character for the quarter
df['album_with_artist'] = df['album_name'] + ' | ' + df['artist_name']

df.to_csv(path + '/PersonalMusic/data/compiled_data.csv', index=False)