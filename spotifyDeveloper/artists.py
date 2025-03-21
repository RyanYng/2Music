import os
from dotenv import load_dotenv
import base64 
import json 
from requests import post,get

load_dotenv()
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

def get_token():
    auth_string = client_id + ':' + client_secret 
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes),"utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = post(url,headers = headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token 

def get_auth_header(token):
    return {"Authorization": "Bearer " + token}

def search_for_artist(token,artist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={artist_name}&type=artist&limit=1"

    query_url = url + query
    result = get(query_url,headers = headers)
    json_result = json.loads(result.content)["artists"]["items"]

    if len(json_result) == 0:
        print("No Artist Found")
        return None
    return json_result[0]

def get_artist_top_songs(token,artist_id):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=UK"
    headers = get_auth_header(token)

    result = get(url, headers = headers)
    json_result = json.loads(result.content)["tracks"]
    return json_result


token = get_token()
artist = search_for_artist(token,'Billy Brag')
artist_id = artist['id']
songs = get_artist_top_songs(token,artist_id)

for idx,song in enumerate(songs):
    # print('-'*500)
    print(f"{idx + 1} . {song["name"]} -> {song["track_number"]} song on album {song["album"]["name"]} | released {song["album"]["release_date"]}")
    # print(song)
