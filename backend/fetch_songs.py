import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
import os
import random
import requests
import time
import genres
from collections import defaultdict


load_dotenv()  # load variables from .env
random.seed()  # seeding rng with the current time

# client credentials flow
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

# setting up spotipy client
auth_manager = SpotifyClientCredentials(
    client_id=client_id, client_secret=client_secret
)
sp = spotipy.Spotify(auth_manager=auth_manager)

# connect to mongo db database
uri = os.getenv("MONGODB_URL")
client = MongoClient(
    uri,
    server_api=pymongo.server_api.ServerApi(
        version="1", strict=True, deprecation_errors=True
    ),
)
db = client["musicdb"]
tracks_collection = db["tracks"]

# const variables
NUM_SONGS_REQUESTED_PER_PLAYLIST = 3  # we are currently only getting 1 playlist per genre, and this many songs per playlist (max # of songs: 100)
VALID_SONGS = 5
# global variables
db_count = 0


# track the counts
def fetch_tracks_per_genre():
    global db_count
    for genre in genres.GENRES:
        track_map = {}
        db_count = get_docs_from_mongo(track_map, genre)
        if db_count >= VALID_SONGS:
            print(f"Already have {db_count} {genre} songs in database")
            continue
        
        playlists = get_playlists(genre)
        prev_ids = set()  # keeps track of playlists we've already tried to extract songs from
        retry_count = len(playlists)

        while len(track_map) < (VALID_SONGS - db_count) and retry_count > 0:
            chosen_pl = choose_playlist(playlists, prev_ids)  # randomly choose one playlist
            playlist_id = chosen_pl["id"]
            tracks = get_playlist_tracks(playlist_id)  # get tracks for that playlist
            audio_features = get_audio_features(tracks)
            create_track_map(
                track_map, tracks, audio_features, playlist_id
            )  # modifies the track_map

            retry_count -= 1
            prev_ids.add(playlist_id)

        # store to mongodb
        count = store_tracks_in_mongo(track_map, genre)
        print(
            "##################################################################################################################################################################"
        )
        print(
            f"Retried {len(playlists) - retry_count} times out of {len(playlists)} possible retries"
        )
        print(f"Number of new tracks stored for {genre}: {count}")
        print(f"Number of {genre} already in database: {db_count}")


def remove_nones(list):
    return [p for p in list if p]


def get_playlists(genre):
    result = spotify_retry_request(
        sp.search, q=genre, limit=10, offset=0, type="playlist"
    )  # in case of rate limiting
    playlists = result["playlists"]["items"]  # getting the playlists
    playlists = remove_nones(playlists)  # remove none values
    return playlists


# randomly chooses a single playlist given a list of playlists
def choose_playlist(playlists, prev_ids):
    index = random.randint(0, len(playlists) - 1)
    chosen = playlists[index]
    while (chosen["id"] in prev_ids):  # makes sure chosen playlist is different from previous ones
        index = random.randint(0, len(playlists) - 1)
        chosen = playlists[index]
    return chosen


# gets n tracks for a specific playlist id
def get_playlist_tracks(playlist_id):
    tracks = spotify_retry_request(
        sp.playlist_items,
        playlist_id=playlist_id,
        additional_types=["track"],
        limit=NUM_SONGS_REQUESTED_PER_PLAYLIST,
    )
    return tracks["items"]


# gets ids of tracks
def get_track_ids(tracks):
    ids = []
    for track in tracks:
        if track and track.get("track") and track["track"].get("id"):
            ids.append(track["track"]["id"])
        else:
            print(f"This track doesn't have an id: {track}")
    return ids


# calls recco beat's api to get audio features
def get_audio_features(tracks):
    track_ids = get_track_ids(tracks)
    features = []
    url = "https://api.reccobeats.com/v1/audio-features"
    headers = {"Accept": "application/json"}
    for id in track_ids:
        if id:
            params = {"ids": id}
            response = recco_retry_request(
                url=url, headers=headers, params=params
            )  # in case of rate limiting
            features.append(response.json()["content"])
    return features


def create_track_map(track_map, tracks, audio_features, playlist_id):
    global db_count
    for track, features in zip(tracks, audio_features):
        if not track or not track.get("track") or not features:
            continue
        if len(track_map) >= (
            VALID_SONGS - db_count
        ):  # once we have reached the valid songs to add we can break out of this function, we don't need to add anything else to track_map
            break

        t_id = track["track"]["id"]
        if t_id not in track_map:
            track_map[t_id] = [track["track"], features, playlist_id]


def store_tracks_in_mongo(track_map, genre):
    count = 0
    for t_id, data in track_map.items():  # this only contains valid tracks and features
        track = data[0]  # this is already track["track"]
        features = data[1]
        playlist_id = data[2]

        if track == "": #skipping empty entries - these empty entries come from reading from the database
            continue

        doc = {
            "_id": t_id,  # track id is the main ID
            "metadata": {
                "name": track["name"],
                "artists": [artist["name"] for artist in track["artists"]],
                "album": track["album"]["name"],
                "images": track["album"].get("images", []),
                "track_link": track["external_urls"]["spotify"],
            },
            "audio_features": features[0],
            "playlist_id": playlist_id,
            "genre": genre,
            "emb": {"triplet64": None, "ae32": None},  # leaving blank for now
        }

        result = tracks_collection.update_one(
            {"_id": t_id}, {"$setOnInsert": doc}, upsert=True
        )  # set on insert: insert if _id doesn't exist, otherwise skip

        if (result.upserted_id is not None):  # inserted a brand new track - no previous ids matched and an upsert is a new insert
            count += 1

    return count


def get_docs_from_mongo(track_map, genre):
    cursor = tracks_collection.find({"genre": genre})
    count = tracks_collection.count_documents({"genre": genre})
    
    for doc in cursor:
        track_map[doc["_id"]] = ["", "", ""] #info stored is irrelevant because we're gonna skip over these tracks, they're already in the database
    return count

# wrapper to handle spotify rate limiting; can accept any function
def spotify_retry_request(func, *args, **kwargs):
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(
                    e.headers.get("Retry-After", 5)
                )  # default to 5 sec if retry-after header is missing
                print(f"Rate limited by Spotify. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                raise e


# wrapper to handle reccobeats rate limiting
def recco_retry_request(url, headers, params):
    while True:
        response = requests.request("GET", url=url, headers=headers, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited by Reccobeats. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            response.raise_for_status()  # raise an error if present
            return response


# calling first function
fetch_tracks_per_genre()
client.close()  # close mongodb connection
