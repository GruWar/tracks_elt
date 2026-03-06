import os
from dotenv import load_dotenv
import base64
import json
from requests import post, get
import time

load_dotenv()

client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

# -------------------------
# AUTH
# -------------------------

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


# -------------------------
# Universal request
# -------------------------

def spotify_get(url, headers):
    while True:

        result = get(url, headers=headers)

        if result.status_code == 429:
            retry = int(result.headers.get("Retry-After", 1))
            print(f"Rate limit hit, waiting {retry} sec")
            time.sleep(retry)
            continue

        if result.status_code != 200:
            print("HTTP ERROR:", result.status_code)
            print(result.text)
            return None

        return result.json()

# -------------------------
# Artist search
# -------------------------

def search_for_artist(token, artist_name):
    headers = get_auth_header(token)
    url = f"https://api.spotify.com/v1/search?q={artist_name}&type=artist&limit=1"

    data = spotify_get(url, headers=headers)

    return data["artists"]["items"][0]

# -------------------------
# Artist top tracks
# -------------------------

def get_artist_top_tracks(token, artist_id, market):
    headers = get_auth_header(token)
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?market=US"
    result = get(url, headers=headers)
    json_res = json.loads(result.content)["tracks"]
    #data = spotify_get(url, headers=headers)

    return json_res

# -------------------------
# Artist albums
# -------------------------

def get_artist_albums(token, artist_id):
    headers = get_auth_header(token)
    limit = 10
    offset = 0
    albums = []

    while True:
        url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?include_groups=album&limit={limit}&offset={offset}"

        result = spotify_get(url, headers=headers)
        print(result)
        data = result["items"]

        if len(data) == 0:
            break
        
        albums.extend(data)

        if len(data) < limit:
            break

        offset += limit
    return albums

# -------------------------
# Album tracks
# -------------------------

def get_album_tracks(token, albums):
    headers = get_auth_header(token)
    limit = 10
    tracks = []
    for album in albums:
        offset = 0
        while True:
            url = f"https://api.spotify.com/v1/albums/{album["id"]}/tracks?limit={limit}&offset={offset}"
            result = spotify_get(url, headers=headers)

            data = result.json()["items"]

            if len(data) == 0:
                break
            
            tracks.extend(data)

            if len(data) < limit:
                break

            offset += limit
    return tracks

# -------------------------
# Tracks details 
# -------------------------

def get_tracks_details(token, tracks, market):
    headers = get_auth_header(token)
    all_tracks = []
    batch_size = 20

    for i in range(0, len(tracks), batch_size):
        batch = tracks[i:i+batch_size]
        ids = ",".join([t["id"] for t in batch])
        url = f"https://api.spotify.com/v1/tracks?{ids}&market={market}"
        result = spotify_get(url, headers=headers)

        all_tracks.extend(result["tracks"])
    return all_tracks




token = get_token()
artist = search_for_artist(token, "ACDC")
artist_id = artist["id"]
print(artist_id)
tracks = get_artist_top_tracks(token, artist_id, "US")

for idx, song in enumerate(tracks):
    print(f"{idx + 1}. {song['name']}")