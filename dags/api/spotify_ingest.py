import os
from dotenv import load_dotenv
import base64
import json
from requests import post, get

load_dotenv()

client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

def get_token():
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token

def get_auth_header(token):
    return {"Authorization": "Bearer " + token}

def search_for_artist(token, artist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"q={artist_name}&type=artist&limit=1"

    query_url = url + "?" + query
    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)["artists"]["items"]

    return json_result[0]

def get_artist_albums(token, artist_id):
    headers = get_auth_header(token)
    limit = 10
    offset = 0
    albums = []
    while True:
        url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?include_groups=album&limit={limit}&offset={offset}"
        result = get(url, headers=headers)

        if result.status_code != 200:
            print("HTTP ERROR:", result.status_code)
            print(result.text)
            break

        data = result.json()["items"]

        if len(data) == 0:
            break
        
        albums.extend(data)

        if len(data) < limit:
            break

        offset += limit
    return albums

def get_album_tracks(token, albums):
    headers = get_auth_header(token)
    limit = 10
    tracks = []
    for album in albums:
        offset = 0
        while True:
            url = f"https://api.spotify.com/v1/albums/{album["id"]}/tracks?limit={limit}&offset={offset}"
            result = get(url, headers=headers)

            if result.status_code != 200:
                print("HTTP ERROR:", result.status_code)
                print(result.text)
                break

            data = result.json()["items"]

            if len(data) == 0:
                break
            
            tracks.extend(data)

            if len(data) < limit:
                break

            offset += limit
    return tracks


token = get_token()
result = search_for_artist(token,"skillet")
artist_id = result["id"]
albums = get_artist_albums(token, artist_id)
tracks = get_album_tracks(token, albums)

for track in tracks:
    print(track['name'])