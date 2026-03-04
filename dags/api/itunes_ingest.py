import requests

def search_artist(artist_name):
    url = "https://itunes.apple.com/search"
    params = {
        "term": artist_name,
        "entity": "musicArtist",
        "limit": 1
    }

    response = requests.get(url, params=params)
    data = response.json()

    if data["resultCount"] == 0:
        return None

    return data["results"][0]

def get_artist_albums(artist_id):
    url = "https://itunes.apple.com/lookup"
    params = {
        "id": artist_id,
        "entity": "album",
        "limit": 200
    }

    response = requests.get(url, params=params)
    data = response.json()

    # první položka je samotný artist → přeskočíme
    albums = data["results"][1:]

    return albums

def get_album_tracks(collection_id):
    url = "https://itunes.apple.com/lookup"
    params = {
        "id": collection_id,
        "entity": "song"
    }

    response = requests.get(url, params=params)
    data = response.json()

    # první položka je album info
    tracks = data["results"][1:]

    return tracks

artist = search_artist("skillet")
albums = get_artist_albums(artist["artistId"])

for album in albums:
    print("\nAlbum:", album["collectionName"])
    tracks = get_album_tracks(album["collectionId"])
    for track in tracks:
        print("  -", track["trackName"])